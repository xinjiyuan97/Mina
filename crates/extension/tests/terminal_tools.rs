#![cfg(feature = "builtin-tools")]

use std::{sync::Arc, time::Duration};

use agent_core::{
    harness::{RunCancellation, RunId, Tool, ToolCallRequest, ToolPort, ToolRiskLevel},
    sandbox::{
        ProcessSandbox, ProcessSandboxSessionRequest, ProcessSandboxWriteRequest, SandboxErrorKind,
    },
};
use agent_extension::{
    sandbox::HostProcessSandbox,
    tool::{ApplyPatchTool, BuiltinToolCatalog, ExecCommandTool, ShellCommandTool, WriteStdinTool},
};
use serde_json::{Value, json};
use tempfile::tempdir;

fn request(name: &str, arguments: Value) -> ToolCallRequest {
    ToolCallRequest {
        run_id: RunId::new(),
        call_id: format!("{name}-call"),
        name: name.into(),
        arguments,
        cancellation: RunCancellation::new(),
    }
}

#[test]
fn terminal_definitions_match_runtime_boundaries() {
    let directory = tempdir().expect("temporary workspace");
    let shell = ShellCommandTool::new(directory.path()).expect("shell tool");
    let exec = ExecCommandTool::new(directory.path()).expect("exec tool");
    let write = WriteStdinTool::new(Arc::new(HostProcessSandbox::default()));
    let patch = ApplyPatchTool::new(directory.path()).expect("patch tool");

    let shell_definition = shell.definition();
    assert_eq!(shell_definition.name, "shell_command");
    assert_eq!(shell_definition.risk_level, ToolRiskLevel::High);
    assert_eq!(
        shell_definition.input_schema["required"],
        json!(["command"])
    );
    assert_eq!(shell_definition.input_schema["additionalProperties"], false);

    let exec_definition = exec.definition();
    assert_eq!(exec_definition.name, "exec_command");
    assert_eq!(exec_definition.risk_level, ToolRiskLevel::High);
    assert_eq!(exec_definition.input_schema["required"], json!(["cmd"]));

    let write_definition = write.definition();
    assert_eq!(write_definition.name, "write_stdin");
    assert_eq!(write_definition.risk_level, ToolRiskLevel::High);
    assert_eq!(
        write_definition.input_schema["required"],
        json!(["session_id"])
    );
    assert!(write_definition.input_schema["properties"]["close_stdin"].is_object());
    assert!(write_definition.input_schema["properties"]["terminate"].is_object());

    assert_eq!(patch.definition().name, "apply_patch");
    assert_eq!(patch.definition().risk_level, ToolRiskLevel::Medium);
    assert_eq!(
        patch.definition().input_schema["required"],
        json!(["patch"])
    );
}

#[tokio::test]
async fn shell_command_executes_with_timeout_and_bounded_output() {
    let directory = tempdir().expect("temporary workspace");
    let tool = ShellCommandTool::new(directory.path())
        .expect("shell tool")
        .with_max_output_bytes(32);
    let output = tool
        .call(request(
            "shell_command",
            json!({"command": "printf 'abcdefghijklmnopqrstuvwxyz0123456789'", "timeout_ms": 1000}),
        ))
        .await
        .expect("command succeeds");
    let value: Value = serde_json::from_str(&output.content).expect("JSON output");
    assert_eq!(value["exit_code"], 0);
    assert_eq!(value["stdout_truncated"], true);
    assert!(value["stdout"].as_str().expect("stdout").len() <= 32);

    let error = tool
        .call(request(
            "shell_command",
            json!({"command": "sleep 2", "timeout_ms": 20}),
        ))
        .await
        .expect_err("command times out");
    assert_eq!(error.code(), "sandbox_timeout");
}

#[tokio::test]
async fn terminal_environment_is_allowlisted_and_timeout_kills_descendants() {
    let directory = tempdir().expect("temporary workspace");
    let tool = ShellCommandTool::new(directory.path()).expect("shell tool");
    let output = tool
        .call(request(
            "shell_command",
            json!({"command": "env", "timeout_ms": 1000}),
        ))
        .await
        .expect("environment command");
    let output: Value = serde_json::from_str(&output.content).expect("JSON output");
    let stdout = output["stdout"].as_str().expect("stdout");
    assert!(!stdout.contains("HOME="));
    assert!(!stdout.contains("CARGO_REGISTRY_TOKEN="));
    assert!(!stdout.contains("AWS_SECRET_ACCESS_KEY="));

    let error = tool
        .call(request(
            "shell_command",
            json!({"command": "sleep 60 & echo $! > child.pid; wait", "timeout_ms": 100}),
        ))
        .await
        .expect_err("process group times out");
    assert_eq!(error.code(), "sandbox_timeout");
    let pid = tokio::fs::read_to_string(directory.path().join("child.pid"))
        .await
        .expect("child pid was recorded");
    let status = std::process::Command::new("/bin/kill")
        .args(["-0", pid.trim()])
        .stderr(std::process::Stdio::null())
        .status()
        .expect("kill probe");
    assert!(!status.success(), "descendant process survived timeout");
}

#[tokio::test]
async fn session_timeout_and_invalid_write_are_structured() {
    let directory = tempdir().expect("temporary workspace");
    let sandbox: Arc<dyn ProcessSandbox> = Arc::new(HostProcessSandbox::default());
    let exec =
        ExecCommandTool::from_workspace_root_with_sandbox(directory.path(), Arc::clone(&sandbox))
            .expect("exec tool");
    let write = WriteStdinTool::new(sandbox);
    let error = exec
        .call(request(
            "exec_command",
            json!({"cmd": "sleep 60", "timeout_ms": 20, "yield_time_ms": 1000}),
        ))
        .await
        .expect_err("session timeout");
    assert_eq!(error.code(), "sandbox_timeout");

    let error = write
        .call(request(
            "write_stdin",
            json!({"session_id": "missing", "chars": "x", "terminate": true}),
        ))
        .await
        .expect_err("invalid combination");
    assert_eq!(error.code(), "invalid_tool_arguments");
}

#[tokio::test]
async fn session_supports_write_poll_close_and_structured_lifecycle_errors() {
    let directory = tempdir().expect("temporary workspace");
    let sandbox: Arc<dyn ProcessSandbox> = Arc::new(HostProcessSandbox::default());
    let exec =
        ExecCommandTool::from_workspace_root_with_sandbox(directory.path(), Arc::clone(&sandbox))
            .expect("exec tool");
    let write = WriteStdinTool::new(Arc::clone(&sandbox));

    let started = exec
        .call(request(
            "exec_command",
            json!({"cmd": "while IFS= read -r line; do printf 'got:%s\\n' \"$line\"; done", "yield_time_ms": 20}),
        ))
        .await
        .expect("session starts");
    let started: Value = serde_json::from_str(&started.content).expect("JSON output");
    let session_id = started["session_id"].as_str().expect("live session id");

    let written = write
        .call(request(
            "write_stdin",
            json!({"session_id": session_id, "chars": "hello\n", "yield_time_ms": 1000}),
        ))
        .await
        .expect("write succeeds");
    let written: Value = serde_json::from_str(&written.content).expect("JSON output");
    assert!(
        written["output"]
            .as_str()
            .expect("output")
            .contains("got:hello")
    );
    assert_eq!(written["session_id"], session_id);

    let closed = write
        .call(request(
            "write_stdin",
            json!({"session_id": session_id, "close_stdin": true, "yield_time_ms": 1000}),
        ))
        .await
        .expect("stdin close succeeds");
    let closed: Value = serde_json::from_str(&closed.content).expect("JSON output");
    assert_eq!(closed["exit_code"], 0);
    assert!(closed.get("session_id").is_none());

    let error = write
        .call(request(
            "write_stdin",
            json!({"session_id": session_id, "chars": "late"}),
        ))
        .await
        .expect_err("finished session is unavailable");
    assert_eq!(error.code(), "sandbox_session_not_found");
}

#[tokio::test]
async fn sandbox_session_termination_and_cancellation_are_real() {
    let directory = tempdir().expect("temporary workspace");
    let sandbox = HostProcessSandbox::default();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let started = sandbox
        .start(ProcessSandboxSessionRequest {
            execution_id: "session-test".into(),
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 60".into()],
            workspace_root: directory.path().to_owned(),
            cancellation: cancellation.clone(),
            timeout: None,
            yield_time: Duration::from_millis(20),
            max_output_bytes: 1024,
        })
        .await
        .expect("session starts");
    let session_id = started.session_id.expect("live session");
    sandbox
        .write(ProcessSandboxWriteRequest {
            session_id: session_id.clone(),
            input: String::new(),
            close_stdin: false,
            terminate: true,
            yield_time: Duration::from_secs(1),
            max_output_bytes: 1024,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .expect("termination returns process result");
    let error = sandbox
        .write(ProcessSandboxWriteRequest {
            session_id,
            input: String::new(),
            close_stdin: false,
            terminate: false,
            yield_time: Duration::ZERO,
            max_output_bytes: 1024,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .expect_err("terminated session is removed");
    assert_eq!(error.kind(), SandboxErrorKind::InvalidRequest);

    let started = sandbox
        .start(ProcessSandboxSessionRequest {
            execution_id: "cancel-test".into(),
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 60".into()],
            workspace_root: directory.path().to_owned(),
            cancellation: cancellation.clone(),
            timeout: None,
            yield_time: Duration::from_millis(20),
            max_output_bytes: 1024,
        })
        .await
        .expect("session starts");
    cancellation.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let error = sandbox
        .write(ProcessSandboxWriteRequest {
            session_id: started.session_id.expect("live session"),
            input: String::new(),
            close_stdin: false,
            terminate: false,
            yield_time: Duration::from_millis(20),
            max_output_bytes: 1024,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .expect_err("cancelled session reports cancellation");
    assert_eq!(error.kind(), SandboxErrorKind::Cancelled);
}

#[tokio::test]
async fn apply_patch_mutates_workspace_and_rejects_unsafe_paths() {
    let directory = tempdir().expect("temporary workspace");
    tokio::fs::write(directory.path().join("note.txt"), "alpha\nbeta\n")
        .await
        .expect("fixture");
    let tool = ApplyPatchTool::new(directory.path()).expect("patch tool");
    let output = tool
        .call(request(
            "apply_patch",
            json!({"patch": "*** Begin Patch\n*** Update File: note.txt\n@@\n-alpha\n+gamma\n beta\n*** Add File: added.txt\n+new\n*** End Patch"}),
        ))
        .await
        .expect("patch applies");
    let value: Value = serde_json::from_str(&output.content).expect("JSON output");
    assert_eq!(value["files_changed"], 2);
    assert_eq!(
        tokio::fs::read_to_string(directory.path().join("note.txt"))
            .await
            .expect("note"),
        "gamma\nbeta\n"
    );

    for patch in [
        "*** Begin Patch\n*** Add File: ../escape.txt\n+bad\n*** End Patch",
        "*** Begin Patch\n*** Add File: .env\n+SECRET=bad\n*** End Patch",
    ] {
        let error = tool
            .call(request("apply_patch", json!({"patch": patch})))
            .await
            .expect_err("unsafe patch rejected");
        assert!(matches!(
            error.code(),
            "path_outside_workspace" | "path_protected"
        ));
    }

    let error = tool
        .call(request(
            "apply_patch",
            json!({"patch": "*** Begin Patch\n*** Update File: note.txt\n@@\n\n*** End Patch"}),
        ))
        .await
        .expect_err("malformed empty hunk is a structured error");
    assert_eq!(error.code(), "invalid_patch");
}

#[cfg(unix)]
#[tokio::test]
async fn apply_patch_rejects_symlink_destinations() {
    use std::os::unix::fs::symlink;

    let directory = tempdir().expect("temporary workspace");
    let outside = tempdir().expect("outside directory");
    let outside_file = outside.path().join("outside.txt");
    tokio::fs::write(&outside_file, "safe")
        .await
        .expect("fixture");
    symlink(&outside_file, directory.path().join("link.txt")).expect("symlink");
    let tool = ApplyPatchTool::new(directory.path()).expect("patch tool");
    let error = tool
        .call(request(
            "apply_patch",
            json!({"patch": "*** Begin Patch\n*** Update File: link.txt\n@@\n-safe\n+unsafe\n*** End Patch"}),
        ))
        .await
        .expect_err("symlink rejected");
    assert_eq!(error.code(), "path_is_symlink");
    assert_eq!(
        tokio::fs::read_to_string(outside_file)
            .await
            .expect("outside"),
        "safe"
    );
}

#[test]
fn catalog_registers_terminal_tools_without_request_permissions_bypass() {
    let directory = tempdir().expect("temporary workspace");
    let registry = BuiltinToolCatalog::new(directory.path())
        .enable_terminal_tools()
        .build()
        .expect("catalog");
    let definitions = registry.definitions();
    for name in [
        "shell_command",
        "exec_command",
        "write_stdin",
        "apply_patch",
    ] {
        assert!(
            definitions.iter().any(|definition| definition.name == name),
            "missing {name}"
        );
    }
    assert!(
        !definitions
            .iter()
            .any(|definition| definition.name == "request_permissions")
    );
    assert!(
        !definitions
            .iter()
            .any(|definition| definition.name == "run_command")
    );
    for name in ["shell_command", "exec_command", "write_stdin"] {
        assert_eq!(
            definitions
                .iter()
                .find(|definition| definition.name == name)
                .map(|definition| definition.risk_level),
            Some(ToolRiskLevel::High)
        );
    }
}
