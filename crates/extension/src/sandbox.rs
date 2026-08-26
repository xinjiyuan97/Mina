//! Host process adapter for the Core sandbox contract.

use std::{
    ffi::OsString,
    process::Stdio,
    time::{Duration, Instant},
};

pub use agent_core::sandbox::*;
use tokio::{io::AsyncReadExt, process::Command};

const DEFAULT_MAX_CAPTURE_BYTES: usize = 256 * 1024;

/// Development adapter. It constrains cwd, inherited environment and captured
/// output, but intentionally reports `IsolationStrength::None`: it is not a
/// kernel security boundary.
#[derive(Debug, Clone)]
pub struct HostProcessSandbox {
    path: Option<OsString>,
    max_capture_bytes: usize,
}

impl Default for HostProcessSandbox {
    fn default() -> Self {
        Self {
            path: std::env::var_os("PATH"),
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
        }
    }
}

impl HostProcessSandbox {
    #[must_use]
    pub fn with_max_capture_bytes(mut self, max_capture_bytes: usize) -> Self {
        self.max_capture_bytes = max_capture_bytes.max(1);
        self
    }
}

impl ProcessSandbox for HostProcessSandbox {
    fn descriptor(&self) -> ProcessSandboxDescriptor {
        ProcessSandboxDescriptor {
            identity: "process:host-development".into(),
            kind: "host_process".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            isolation: IsolationStrength::None,
            network_isolated: false,
            filesystem_isolated: false,
            resource_limited: false,
        }
    }

    fn execute(&self, request: ProcessSandboxRequest) -> ProcessSandboxFuture {
        let path = self.path.clone();
        let max_capture_bytes = self.max_capture_bytes;
        Box::pin(async move {
            if request.execution_id.trim().is_empty() || request.program.trim().is_empty() {
                return Err(SandboxError::new(
                    SandboxErrorKind::InvalidRequest,
                    "sandbox_invalid_request",
                    "sandbox execution requires an id and executable",
                    false,
                ));
            }
            let started = Instant::now();
            let mut command = Command::new(request.program);
            command
                .args(request.args)
                .current_dir(request.workspace_root)
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            if let Some(path) = path {
                command.env("PATH", path);
            }

            let mut child = command.spawn().map_err(|_| spawn_failed())?;
            let stdout = child.stdout.take().ok_or_else(command_io_failed)?;
            let stderr = child.stderr.take().ok_or_else(command_io_failed)?;
            let stdout_task = tokio::spawn(read_capped(stdout, max_capture_bytes));
            let stderr_task = tokio::spawn(read_capped(stderr, max_capture_bytes));

            let status = tokio::select! {
                result = child.wait() => result.map_err(|_| command_io_failed())?,
                () = request.cancellation.cancelled() => {
                    let _ = child.kill().await;
                    return Err(cancelled());
                }
            };
            let (stdout, stdout_truncated) = stdout_task
                .await
                .map_err(|_| command_io_failed())?
                .map_err(|_| command_io_failed())?;
            let (stderr, stderr_truncated) = stderr_task
                .await
                .map_err(|_| command_io_failed())?
                .map_err(|_| command_io_failed())?;

            Ok(ProcessSandboxOutput {
                exit_code: status.code(),
                success: status.success(),
                stdout,
                stderr,
                stdout_truncated,
                stderr_truncated,
                duration_ms: duration_ms(started.elapsed()),
            })
        })
    }
}

async fn read_capped<R>(
    mut reader: R,
    max_capture_bytes: usize,
) -> Result<(String, bool), std::io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = max_capture_bytes.saturating_sub(captured.len());
        let retained = remaining.min(read);
        captured.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }
    Ok((String::from_utf8_lossy(&captured).into_owned(), truncated))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn spawn_failed() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::SpawnFailed,
        "command_spawn_failed",
        "the requested executable could not be started",
        false,
    )
}

fn command_io_failed() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::Io,
        "command_io_failed",
        "command output could not be collected",
        false,
    )
}

fn cancelled() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::Cancelled,
        "sandbox_cancelled",
        "sandbox execution was cancelled",
        false,
    )
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[tokio::test]
    async fn host_adapter_reports_no_isolation_and_executes_directly() {
        let directory = tempdir().expect("temporary workspace should be created");
        let executable = std::env::current_exe().expect("test executable should be available");
        let sandbox = HostProcessSandbox::default();
        let output = sandbox
            .execute(ProcessSandboxRequest {
                execution_id: "sandbox-test".into(),
                program: executable.display().to_string(),
                args: vec!["--list".into()],
                workspace_root: directory.path().to_owned(),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("host process should execute");

        assert!(output.success);
        assert_eq!(sandbox.descriptor().isolation, IsolationStrength::None);
    }
}
