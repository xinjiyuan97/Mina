//! Host process adapter for the Core sandbox contract.

use std::{
    collections::HashMap,
    ffi::OsString,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

pub use agent_core::sandbox::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_CAPTURE_BYTES: usize = 256 * 1024;

/// Development adapter. It constrains cwd, inherited environment and captured
/// output, but intentionally reports `IsolationStrength::None`: it is not a
/// kernel security boundary.
#[derive(Clone)]
pub struct HostProcessSandbox {
    path: Option<OsString>,
    max_capture_bytes: usize,
    sessions: Arc<Mutex<HashMap<String, Arc<SessionHandle>>>>,
}

impl std::fmt::Debug for HostProcessSandbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostProcessSandbox")
            .field("max_capture_bytes", &self.max_capture_bytes)
            .finish_non_exhaustive()
    }
}

impl Default for HostProcessSandbox {
    fn default() -> Self {
        Self {
            path: std::env::var_os("PATH"),
            max_capture_bytes: DEFAULT_MAX_CAPTURE_BYTES,
            sessions: Arc::new(Mutex::new(HashMap::new())),
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
            #[cfg(unix)]
            command.process_group(0);
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
                    terminate_child(&mut child).await;
                    return Err(cancelled());
                },
                () = wait_for_timeout(request.timeout) => {
                    terminate_child(&mut child).await;
                    return Err(timed_out());
                },
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

    fn start(&self, request: ProcessSandboxSessionRequest) -> ProcessSandboxSessionFuture {
        let path = self.path.clone();
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            validate_session_request(&request)?;
            let session_id = request.execution_id;
            let mut command = Command::new(request.program);
            command
                .args(request.args)
                .current_dir(request.workspace_root)
                .env_clear()
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            command.process_group(0);
            if let Some(path) = path {
                command.env("PATH", path);
            }
            let mut child = command.spawn().map_err(|_| spawn_failed())?;
            let stdin = child.stdin.take().ok_or_else(command_io_failed)?;
            let stdout = child.stdout.take().ok_or_else(command_io_failed)?;
            let stderr = child.stderr.take().ok_or_else(command_io_failed)?;
            let (output_tx, output_rx) = mpsc::channel(64);
            tokio::spawn(stream_output(stdout, output_tx.clone()));
            tokio::spawn(stream_output(stderr, output_tx));
            let (command_tx, command_rx) = mpsc::channel(8);
            let (status_tx, status_rx) = watch::channel(SessionStatus::Running);
            let handle = Arc::new(SessionHandle {
                commands: command_tx,
                output: Mutex::new(output_rx),
                status: status_rx,
                started: Instant::now(),
            });
            {
                let mut store = sessions.lock().await;
                if store.contains_key(&session_id) {
                    terminate_child(&mut child).await;
                    return Err(SandboxError::new(
                        SandboxErrorKind::InvalidRequest,
                        "sandbox_session_exists",
                        "the requested sandbox session identifier is already active",
                        false,
                    ));
                }
                store.insert(session_id.clone(), Arc::clone(&handle));
            }
            tokio::spawn(supervise_session(
                child,
                stdin,
                command_rx,
                status_tx,
                request.cancellation,
                request.timeout,
            ));
            collect_session_output(
                sessions,
                session_id,
                handle,
                request.yield_time,
                request.max_output_bytes,
                CancellationToken::new(),
            )
            .await
        })
    }

    fn write(&self, request: ProcessSandboxWriteRequest) -> ProcessSandboxSessionFuture {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            if request.session_id.trim().is_empty() || request.max_output_bytes == 0 {
                return Err(invalid_session_arguments());
            }
            if request.terminate && (request.close_stdin || !request.input.is_empty()) {
                return Err(invalid_session_arguments());
            }
            let handle = sessions
                .lock()
                .await
                .get(&request.session_id)
                .cloned()
                .ok_or_else(session_not_found)?;
            if request.terminate {
                send_session_command(&handle, SessionCommandKind::Terminate).await?;
            } else {
                if !request.input.is_empty() {
                    send_session_command(
                        &handle,
                        SessionCommandKind::Write(request.input.into_bytes()),
                    )
                    .await?;
                }
                if request.close_stdin {
                    send_session_command(&handle, SessionCommandKind::CloseStdin).await?;
                }
            }
            collect_session_output(
                sessions,
                request.session_id,
                handle,
                request.yield_time,
                request.max_output_bytes,
                request.cancellation,
            )
            .await
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionStatus {
    Running,
    Exited(Option<i32>),
    Cancelled,
    TimedOut,
    IoFailed,
}

struct SessionHandle {
    commands: mpsc::Sender<SessionCommand>,
    output: Mutex<mpsc::Receiver<Vec<u8>>>,
    status: watch::Receiver<SessionStatus>,
    started: Instant,
}

enum SessionCommandKind {
    Write(Vec<u8>),
    CloseStdin,
    Terminate,
}

struct SessionCommand {
    kind: SessionCommandKind,
    acknowledged: oneshot::Sender<Result<(), SandboxError>>,
}

async fn send_session_command(
    handle: &SessionHandle,
    kind: SessionCommandKind,
) -> Result<(), SandboxError> {
    let (acknowledged, response) = oneshot::channel();
    handle
        .commands
        .send(SessionCommand { kind, acknowledged })
        .await
        .map_err(|_| session_not_running())?;
    response.await.map_err(|_| session_not_running())?
}

async fn supervise_session(
    mut child: Child,
    stdin: tokio::process::ChildStdin,
    mut commands: mpsc::Receiver<SessionCommand>,
    status: watch::Sender<SessionStatus>,
    cancellation: CancellationToken,
    timeout: Option<Duration>,
) {
    let mut stdin = Some(stdin);
    let timeout = wait_for_timeout(timeout);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            result = child.wait() => {
                let next = result.map_or(SessionStatus::IoFailed, |value| SessionStatus::Exited(value.code()));
                status.send_replace(next);
                break;
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    terminate_child(&mut child).await;
                    let _ = child.wait().await;
                    status.send_replace(SessionStatus::Cancelled);
                    break;
                };
                match command.kind {
                    SessionCommandKind::Write(bytes) => {
                        let result = match stdin.as_mut() {
                            Some(stdin) => stdin.write_all(&bytes).await.map_err(|_| session_stdin_closed()),
                            None => Err(session_stdin_closed()),
                        };
                        let _ = command.acknowledged.send(result);
                    }
                    SessionCommandKind::CloseStdin => {
                        let result = match stdin.take() {
                            Some(mut stdin) => stdin.shutdown().await.map_err(|_| session_stdin_closed()),
                            None => Err(session_stdin_closed()),
                        };
                        let _ = command.acknowledged.send(result);
                    }
                    SessionCommandKind::Terminate => {
                        terminate_child(&mut child).await;
                        let _ = command.acknowledged.send(Ok(()));
                    }
                }
            }
            () = cancellation.cancelled() => {
                terminate_child(&mut child).await;
                let _ = child.wait().await;
                status.send_replace(SessionStatus::Cancelled);
                break;
            }
            () = &mut timeout => {
                terminate_child(&mut child).await;
                let _ = child.wait().await;
                status.send_replace(SessionStatus::TimedOut);
                break;
            }
        }
    }
}

async fn stream_output<R>(mut reader: R, output: mpsc::Sender<Vec<u8>>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; 8192];
    loop {
        let Ok(read) = reader.read(&mut buffer).await else {
            break;
        };
        if read == 0 || output.send(buffer[..read].to_vec()).await.is_err() {
            break;
        }
    }
}

async fn collect_session_output(
    sessions: Arc<Mutex<HashMap<String, Arc<SessionHandle>>>>,
    session_id: String,
    handle: Arc<SessionHandle>,
    yield_time: Duration,
    max_output_bytes: usize,
    cancellation: CancellationToken,
) -> Result<ProcessSandboxSessionOutput, SandboxError> {
    let deadline = tokio::time::Instant::now() + yield_time;
    let mut status = handle.status.clone();
    let mut receiver = handle.output.lock().await;
    let mut captured = Vec::new();
    let mut truncated = false;
    loop {
        if *status.borrow() != SessionStatus::Running {
            while let Some(chunk) = receiver.recv().await {
                append_capped(&mut captured, &chunk, max_output_bytes, &mut truncated);
            }
            break;
        }
        tokio::select! {
            chunk = receiver.recv() => {
                if let Some(chunk) = chunk {
                    append_capped(&mut captured, &chunk, max_output_bytes, &mut truncated);
                }
            }
            changed = status.changed() => {
                if changed.is_err() { break; }
            }
            () = tokio::time::sleep_until(deadline) => break,
            () = cancellation.cancelled() => return Err(cancelled()),
        }
    }
    drop(receiver);
    let final_status = status.borrow().clone();
    let (live_session, exit_code) = match final_status {
        SessionStatus::Running => (Some(session_id.clone()), None),
        SessionStatus::Exited(code) => (None, code),
        SessionStatus::Cancelled => {
            sessions.lock().await.remove(&session_id);
            return Err(cancelled());
        }
        SessionStatus::TimedOut => {
            sessions.lock().await.remove(&session_id);
            return Err(timed_out());
        }
        SessionStatus::IoFailed => {
            sessions.lock().await.remove(&session_id);
            return Err(command_io_failed());
        }
    };
    if live_session.is_none() {
        sessions.lock().await.remove(&session_id);
    }
    Ok(ProcessSandboxSessionOutput {
        session_id: live_session,
        exit_code,
        output: String::from_utf8_lossy(&captured).into_owned(),
        output_truncated: truncated,
        duration_ms: duration_ms(handle.started.elapsed()),
    })
}

fn append_capped(target: &mut Vec<u8>, chunk: &[u8], cap: usize, truncated: &mut bool) {
    let retained = cap.saturating_sub(target.len()).min(chunk.len());
    target.extend_from_slice(&chunk[..retained]);
    *truncated |= retained < chunk.len();
}

fn validate_session_request(request: &ProcessSandboxSessionRequest) -> Result<(), SandboxError> {
    if request.execution_id.trim().is_empty()
        || request.program.trim().is_empty()
        || request.max_output_bytes == 0
    {
        return Err(invalid_session_arguments());
    }
    Ok(())
}

async fn wait_for_timeout(timeout: Option<Duration>) {
    match timeout {
        Some(timeout) => tokio::time::sleep(timeout).await,
        None => std::future::pending().await,
    }
}

async fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    if let Some(process_id) = child.id() {
        let _ = Command::new("/bin/kill")
            .args(["-KILL", "--", &format!("-{process_id}")])
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    let _ = child.kill().await;
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

fn timed_out() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::Timeout,
        "sandbox_timeout",
        "sandbox execution exceeded its time limit",
        false,
    )
}

fn invalid_session_arguments() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::InvalidRequest,
        "sandbox_invalid_session_request",
        "sandbox session arguments do not match the declared contract",
        false,
    )
}

fn session_not_found() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::InvalidRequest,
        "sandbox_session_not_found",
        "the requested sandbox session does not exist",
        false,
    )
}

fn session_not_running() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::InvalidRequest,
        "sandbox_session_not_running",
        "the requested sandbox session is no longer running",
        false,
    )
}

fn session_stdin_closed() -> SandboxError {
    SandboxError::new(
        SandboxErrorKind::Io,
        "sandbox_session_stdin_closed",
        "the sandbox session standard input is closed",
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
                timeout: None,
            })
            .await
            .expect("host process should execute");

        assert!(output.success);
        assert_eq!(sandbox.descriptor().isolation, IsolationStrength::None);
    }
}
