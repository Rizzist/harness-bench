//! Harness operations over exec, stdin-RPC, socket JSON-RPC, or HTTP transports.
//!
//! All transports use the same small JSON-RPC-like envelope.  The envelope keeps the
//! semantic driver independent from process and wire framing, while the monotonically
//! increasing request ID makes recordings deterministic.

use crate::events::{EventNormalizer, EventVocab, NormalizedEvent, rule_matches};
use crate::manifest::{EventMapping, ExitContract, Probe};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
#[cfg(unix)]
use std::ffi::CString;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// An opaque harness session identifier.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SessionId(pub String);

/// An opaque replay cursor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Cursor(pub u64);

/// A transport request independent of framing.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransportRequest {
    /// Stable operation name.
    pub operation: String,
    /// JSON parameters.
    pub params: Value,
}

/// A transport response independent of framing.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransportResponse {
    /// Stable request ID.
    pub id: String,
    /// JSON result.
    pub result: Value,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: String,
    method: &'a str,
    params: &'a Value,
}

#[derive(Deserialize)]
struct RpcResponse {
    id: Value,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// Future returned by object-safe asynchronous driver traits.
pub type DriverFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// A framing transport used by the generic harness driver.
pub trait Transport: Send {
    /// Start any persistent transport resources.
    fn start(&mut self) -> DriverFuture<'_, ()>;
    /// Send one operation and receive its response.
    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse>;
    /// Stop persistent transport resources.
    fn stop(&mut self) -> DriverFuture<'_, ()>;
    /// Launcher/controller PIDs directly owned by this transport.
    fn owned_pids(&self) -> Vec<u32> {
        Vec::new()
    }
}

static DAEMON_LOG_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Lifecycle settings for a daemon that is separate from its socket/HTTP client.
#[derive(Clone, Debug)]
pub struct ManagedDaemonConfig {
    /// Fully rendered cold-start argv.
    pub command: Vec<String>,
    /// Isolated environment inherited by the daemon.
    pub environment: BTreeMap<String, String>,
    /// Readiness condition evaluated before the inner client starts.
    pub readiness: Probe,
    /// Grace period before a process-group SIGKILL.
    pub grace: Duration,
    /// Fresh run directory that receives bounded daemon stdout/stderr files.
    pub log_directory: PathBuf,
}

/// A lifecycle wrapper for daemons reached through a separate client transport.
pub struct ManagedDaemonTransport<T: Transport> {
    inner: T,
    config: ManagedDaemonConfig,
    child: Option<Child>,
}

impl<T: Transport> ManagedDaemonTransport<T> {
    /// Wrap a socket or HTTP client with a cold, owned daemon process.
    pub fn new(inner: T, config: ManagedDaemonConfig) -> Self {
        Self {
            inner,
            config,
            child: None,
        }
    }

    async fn readiness_satisfied(probe: &Probe) -> Result<bool> {
        match probe.kind.as_str() {
            "" | "process" => Ok(true),
            "file" => Ok(Path::new(&probe.target).is_file()),
            "socket" => {
                #[cfg(unix)]
                {
                    match tokio::net::UnixStream::connect(&probe.target).await {
                        Ok(stream) => {
                            drop(stream);
                            Ok(true)
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::NotFound
                                    | std::io::ErrorKind::ConnectionRefused
                                    | std::io::ErrorKind::ConnectionReset
                            ) =>
                        {
                            Ok(false)
                        }
                        Err(error) => Err(error.into()),
                    }
                }
                #[cfg(not(unix))]
                {
                    Err(AhrbError::Unsupported(
                        "Unix socket daemon readiness requires Unix".to_owned(),
                    ))
                }
            }
            "http" => {
                let parsed = ParsedHttpUrl::parse(&probe.target)?;
                match tokio::net::TcpStream::connect((parsed.host.as_str(), parsed.port)).await {
                    Ok(stream) => {
                        drop(stream);
                        Ok(true)
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionRefused
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::TimedOut
                        ) =>
                    {
                        Ok(false)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            other => Err(AhrbError::Validation(format!(
                "unsupported daemon readiness probe {other:?}"
            ))),
        }
    }

    #[cfg(unix)]
    fn signal_process_group(pid: u32, signal: i32) -> Result<()> {
        let process_group = i32::try_from(pid).map_err(|_| {
            AhrbError::Protocol(format!("daemon PID {pid} does not fit process-group API"))
        })?;
        // SAFETY: managed daemons are launched as process-group leaders, and
        // negative PIDs address exactly that owned group.
        if unsafe { libc::kill(-process_group, signal) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }

    async fn stop_child(child: &mut Child, grace: Duration) -> Result<()> {
        let Some(pid) = child.id() else {
            let _ = child.wait().await?;
            return Ok(());
        };
        #[cfg(unix)]
        Self::signal_process_group(pid, libc::SIGTERM)?;
        #[cfg(not(unix))]
        child.start_kill()?;

        match tokio::time::timeout(grace, child.wait()).await {
            Ok(status) => {
                let _ = status?;
            }
            Err(_) => {
                #[cfg(unix)]
                Self::signal_process_group(pid, libc::SIGKILL)?;
                #[cfg(not(unix))]
                child.start_kill()?;
                let _ = child.wait().await?;
            }
        }

        // A daemon leader can exit before workers in its process group. Sweep the
        // still-owned group after reaping the leader so a child cannot outlive the run.
        #[cfg(unix)]
        Self::signal_process_group(pid, libc::SIGKILL)?;
        Ok(())
    }
}

impl<T: Transport> Transport for ManagedDaemonTransport<T> {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            if self.child.is_some() {
                return Ok(());
            }
            std::fs::create_dir_all(&self.config.log_directory)?;
            let sequence = DAEMON_LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let stdout = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(
                    self.config
                        .log_directory
                        .join(format!("daemon-{sequence:04}.stdout")),
                )?;
            let stderr = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(
                    self.config
                        .log_directory
                        .join(format!("daemon-{sequence:04}.stderr")),
                )?;
            let mut command = command_from_argv(&self.config.command)?;
            command
                .envs(&self.config.environment)
                .stdin(Stdio::null())
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr));
            self.child = Some(command.spawn()?);

            let timeout = Duration::from_millis(self.config.readiness.timeout_ms.max(1));
            let started = std::time::Instant::now();
            loop {
                let child = self.child.as_mut().ok_or_else(|| {
                    AhrbError::Protocol("managed daemon child disappeared".to_owned())
                })?;
                let daemon_pid = child.id();
                if let Some(status) = child.try_wait()? {
                    self.child.take();
                    #[cfg(unix)]
                    if let Some(pid) = daemon_pid {
                        Self::signal_process_group(pid, libc::SIGKILL)?;
                    }
                    return Err(AhrbError::Protocol(format!(
                        "managed daemon exited before readiness with {status}"
                    )));
                }
                match Self::readiness_satisfied(&self.config.readiness).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => {
                        if let Some(mut child) = self.child.take() {
                            Self::stop_child(&mut child, self.config.grace).await?;
                        }
                        return Err(error);
                    }
                }
                if started.elapsed() >= timeout {
                    if let Some(mut child) = self.child.take() {
                        Self::stop_child(&mut child, self.config.grace).await?;
                    }
                    return Err(AhrbError::Timeout(format!(
                        "daemon readiness {:?} at {:?}",
                        self.config.readiness.kind, self.config.readiness.target
                    )));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if let Err(error) = self.inner.start().await {
                if let Some(mut child) = self.child.take() {
                    Self::stop_child(&mut child, self.config.grace).await?;
                }
                return Err(error);
            }
            Ok(())
        })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        self.inner.request(request)
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            let inner_result = self.inner.stop().await;
            let daemon_result = if let Some(mut child) = self.child.take() {
                Self::stop_child(&mut child, self.config.grace).await
            } else {
                Ok(())
            };
            inner_result.and(daemon_result)
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        let mut pids = self.inner.owned_pids();
        if let Some(pid) = self.child.as_ref().and_then(Child::id) {
            pids.push(pid);
        }
        pids.sort_unstable();
        pids.dedup();
        pids
    }
}

impl<T: Transport> Drop for ManagedDaemonTransport<T> {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.as_ref().and_then(Child::id) {
            // Drop cannot wait asynchronously, but it must prevent an error path
            // from leaving daemon workers resident. Tokio's kill-on-drop handles
            // the leader; this signal covers every descendant in the owned group.
            let _ = Self::signal_process_group(pid, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

impl<T: Transport + ?Sized> Transport for Box<T> {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        (**self).start()
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        (**self).request(request)
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        (**self).stop()
    }

    fn owned_pids(&self) -> Vec<u32> {
        (**self).owned_pids()
    }
}

/// The semantic harness lifecycle used by workflows.
pub trait Driver: Send {
    /// Start and await readiness.
    fn start(&mut self) -> DriverFuture<'_, ()>;
    /// Create an isolated session.
    fn create_session(&mut self, marker: &str) -> DriverFuture<'_, SessionId>;
    /// Submit a prompt with an idempotency key.
    fn submit(&mut self, session: &SessionId, prompt: &str, key: &str) -> DriverFuture<'_, ()>;
    /// Attach strictly after a durable cursor.
    fn attach(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>>;
    /// Reopen harness-owned durable storage and replay strictly after a cursor.
    fn replay_persisted(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>>;
    /// Resume a previously accepted turn.
    fn resume(&mut self, session: &SessionId) -> DriverFuture<'_, ()>;
    /// Inject input at the next safe boundary.
    fn steer(&mut self, session: &SessionId, prompt: &str) -> DriverFuture<'_, ()>;
    /// Inject input before a pending tool is allowed to run.
    fn subturn(&mut self, session: &SessionId, prompt: &str) -> DriverFuture<'_, ()>;
    /// Queue a distinct next turn.
    fn queue(&mut self, session: &SessionId, prompt: &str, key: &str) -> DriverFuture<'_, ()>;
    /// Release a state barrier previously reported by the session.
    fn release_checkpoint(
        &mut self,
        session: &SessionId,
        release_token: &str,
    ) -> DriverFuture<'_, ()>;
    /// Create a native child actor and return its stable session ID.
    fn spawn_agent(
        &mut self,
        parent: &SessionId,
        marker: &str,
        prompt: Option<&str>,
    ) -> DriverFuture<'_, SessionId>;
    /// Cancel active work.
    fn cancel(&mut self, session: &SessionId) -> DriverFuture<'_, ()>;
    /// Close and delete a session.
    fn close(&mut self, session: &SessionId) -> DriverFuture<'_, ()>;
    /// Shut down and clean up the harness.
    fn shutdown(&mut self) -> DriverFuture<'_, ()>;
    /// Release benchmark launch gates after resource membership is armed.
    fn release_invocations(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Currently live launcher/controller PIDs owned by this driver.
    fn owned_pids(&self) -> Vec<u32> {
        Vec::new()
    }
    /// Currently live launcher PIDs for one logical session, when separable.
    fn session_pids(&self, _session: &SessionId) -> Vec<u32> {
        Vec::new()
    }
}

/// Generic data-driven driver backed by one transport.
pub struct GenericDriver<T: Transport> {
    /// Selected transport.
    pub transport: T,
    /// Manifest-selected operation names.
    pub operations: DriverOperations,
}

/// Semantic operation names used by a transport-backed driver.
///
/// Manifests may map these operations to any JSON-RPC/HTTP method names. The
/// defaults describe the built-in reference harness only.
#[derive(Clone, Debug)]
pub struct DriverOperations {
    /// Create an isolated session.
    pub create_session: String,
    /// Submit a turn.
    pub submit: String,
    /// Attach after an optional cursor.
    pub attach: String,
    /// Resume an accepted turn.
    pub resume: String,
    /// Inject at the next safe boundary.
    pub steer: String,
    /// Inject before the next tool effect.
    pub subturn: String,
    /// Queue a distinct next turn.
    pub queue: String,
    /// Release a durable state checkpoint.
    pub release_checkpoint: String,
    /// Spawn a native child.
    pub spawn_agent: String,
    /// Cancel active work.
    pub cancel: String,
    /// Close and delete a session.
    pub close: String,
    /// Shut down the harness.
    pub shutdown: String,
}

impl Default for DriverOperations {
    fn default() -> Self {
        Self {
            create_session: "session.create".to_owned(),
            submit: "session.submit".to_owned(),
            attach: "session.attach".to_owned(),
            resume: "session.resume".to_owned(),
            steer: "session.steer".to_owned(),
            subturn: "session.subturn".to_owned(),
            queue: "session.queue".to_owned(),
            release_checkpoint: "checkpoint.release".to_owned(),
            spawn_agent: "agent.spawn".to_owned(),
            cancel: "session.cancel".to_owned(),
            close: "session.close".to_owned(),
            shutdown: "harness.shutdown".to_owned(),
        }
    }
}

impl<T: Transport> GenericDriver<T> {
    /// Construct a semantic driver around a framing transport.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            operations: DriverOperations::default(),
        }
    }

    /// Override semantic operation names from an adapter manifest.
    pub fn with_operations(mut self, operations: DriverOperations) -> Self {
        self.operations = operations;
        self
    }

    async fn call(&mut self, operation: &str, params: Value) -> Result<Value> {
        if operation.trim().is_empty() {
            return Err(AhrbError::Unsupported(
                "adapter operation is not declared".to_owned(),
            ));
        }
        let response = self
            .transport
            .request(TransportRequest {
                operation: operation.to_owned(),
                params,
            })
            .await?;
        Ok(response.result)
    }
}

impl<T: Transport> Driver for GenericDriver<T> {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        self.transport.start()
    }

    fn create_session(&mut self, marker: &str) -> DriverFuture<'_, SessionId> {
        let marker = marker.to_owned();
        let operation = self.operations.create_session.clone();
        Box::pin(async move {
            let value = self.call(&operation, json!({ "marker": marker })).await?;
            extract_session_id(&value)
        })
    }

    fn submit(&mut self, session: &SessionId, prompt: &str, key: &str) -> DriverFuture<'_, ()> {
        let session = session.0.clone();
        let prompt = prompt.to_owned();
        let key = key.to_owned();
        let operation = self.operations.submit.clone();
        Box::pin(async move {
            self.call(
                &operation,
                json!({ "session_id": session, "prompt": prompt, "key": key }),
            )
            .await?;
            Ok(())
        })
    }

    fn attach(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>> {
        let session = session.0.clone();
        let operation = self.operations.attach.clone();
        Box::pin(async move {
            let value = self
                .call(
                    &operation,
                    json!({ "session_id": session, "after": after.map(|cursor| cursor.0) }),
                )
                .await?;
            let events = value.get("events").cloned().unwrap_or(value);
            Ok(serde_json::from_value(events)?)
        })
    }

    fn replay_persisted(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>> {
        let session = session.clone();
        Box::pin(async move { self.attach(&session, after).await })
    }

    fn resume(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let operation = self.operations.resume.clone();
        unit_call(self, operation, json!({ "session_id": session.0 }))
    }

    fn steer(&mut self, session: &SessionId, prompt: &str) -> DriverFuture<'_, ()> {
        let operation = self.operations.steer.clone();
        unit_call(
            self,
            operation,
            json!({ "session_id": session.0, "prompt": prompt }),
        )
    }

    fn subturn(&mut self, session: &SessionId, prompt: &str) -> DriverFuture<'_, ()> {
        let operation = self.operations.subturn.clone();
        unit_call(
            self,
            operation,
            json!({ "session_id": session.0, "prompt": prompt }),
        )
    }

    fn queue(&mut self, session: &SessionId, prompt: &str, key: &str) -> DriverFuture<'_, ()> {
        let operation = self.operations.queue.clone();
        unit_call(
            self,
            operation,
            json!({ "session_id": session.0, "prompt": prompt, "key": key }),
        )
    }

    fn release_checkpoint(
        &mut self,
        session: &SessionId,
        release_token: &str,
    ) -> DriverFuture<'_, ()> {
        let operation = self.operations.release_checkpoint.clone();
        unit_call(
            self,
            operation,
            json!({ "session_id": session.0, "release_token": release_token }),
        )
    }

    fn spawn_agent(
        &mut self,
        parent: &SessionId,
        marker: &str,
        prompt: Option<&str>,
    ) -> DriverFuture<'_, SessionId> {
        let parent = parent.0.clone();
        let marker = marker.to_owned();
        let prompt = prompt.map(str::to_owned);
        let operation = self.operations.spawn_agent.clone();
        Box::pin(async move {
            let value = self
                .call(
                    &operation,
                    json!({ "parent_session_id": parent, "marker": marker, "prompt": prompt }),
                )
                .await?;
            extract_session_id(&value)
        })
    }

    fn cancel(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let operation = self.operations.cancel.clone();
        unit_call(self, operation, json!({ "session_id": session.0 }))
    }

    fn close(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let operation = self.operations.close.clone();
        unit_call(self, operation, json!({ "session_id": session.0 }))
    }

    fn shutdown(&mut self) -> DriverFuture<'_, ()> {
        let operation = self.operations.shutdown.clone();
        Box::pin(async move {
            if !operation.is_empty() {
                let _ = self.call(&operation, json!({})).await;
            }
            self.transport.stop().await
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        self.transport.owned_pids()
    }
}

fn unit_call<'a, T: Transport>(
    driver: &'a mut GenericDriver<T>,
    operation: String,
    params: Value,
) -> DriverFuture<'a, ()> {
    Box::pin(async move {
        driver.call(&operation, params).await?;
        Ok(())
    })
}

fn extract_session_id(value: &Value) -> Result<SessionId> {
    value
        .get("session_id")
        .and_then(Value::as_str)
        .or_else(|| value.as_str())
        .map(|id| SessionId(id.to_owned()))
        .ok_or_else(|| AhrbError::Protocol("response omitted session_id".to_owned()))
}

fn command_from_argv(argv: &[String]) -> Result<Command> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| AhrbError::Validation("transport command is empty".to_owned()))?;
    let mut command = Command::new(program);
    command.args(args);
    #[cfg(unix)]
    command.process_group(0);
    command.kill_on_drop(true);
    Ok(command)
}

fn rpc_bytes(id: u64, request: &TransportRequest) -> Result<(String, Vec<u8>)> {
    let id = id.to_string();
    let bytes = serde_json::to_vec(&RpcRequest {
        jsonrpc: "2.0",
        id: id.clone(),
        method: &request.operation,
        params: &request.params,
    })?;
    Ok((id, bytes))
}

fn parse_rpc(expected_id: &str, bytes: &[u8]) -> Result<TransportResponse> {
    let response: RpcResponse = serde_json::from_slice(bytes)?;
    let received_id = match response.id {
        Value::String(id) => id,
        Value::Number(id) => id.to_string(),
        _ => {
            return Err(AhrbError::Protocol(
                "RPC response ID is not scalar".to_owned(),
            ));
        }
    };
    if received_id != expected_id {
        return Err(AhrbError::Protocol(format!(
            "RPC response ID {received_id:?} did not match {expected_id:?}"
        )));
    }
    if let Some(error) = response.error {
        return Err(AhrbError::Protocol(format!(
            "RPC error {}: {}",
            error.code, error.message
        )));
    }
    let result = response
        .result
        .ok_or_else(|| AhrbError::Protocol("RPC response omitted result".to_owned()))?;
    Ok(TransportResponse {
        id: received_id,
        result,
    })
}

/// Configuration for a fresh-process-per-turn harness CLI.
#[derive(Clone, Debug)]
pub struct PerInvocationConfig {
    /// First-turn argv. `{{prompt}}`, `{{session_id}}`, `{{marker}}`,
    /// `{{turn_key}}`, `{{workspace}}`, and `{{profile}}` are rendered directly.
    pub command: Vec<String>,
    /// Subsequent-turn argv used to reopen a harness-owned session from disk.
    pub resume_command: Vec<String>,
    /// Out-of-process command used to release a durable checkpoint token.
    pub release_command: Vec<String>,
    /// Out-of-process command used after terminating an active invocation.
    pub cancel_command: Vec<String>,
    /// Out-of-process command that strictly reopens and emits the durable journal.
    pub replay_command: Vec<String>,
    /// Isolated environment inherited by every invocation.
    pub environment: BTreeMap<String, String>,
    /// Fully populated manifest-level template variables. Per-session and
    /// per-turn values are layered over this foundation for every argv and
    /// event-path render.
    pub base_variables: BTreeMap<String, String>,
    /// Fresh run profile containing AHRB bookkeeping and harness state roots.
    pub profile_root: PathBuf,
    /// Harness-owned event source and normalization table.
    pub events: EventMapping,
    /// Exit-code and stdout terminal contract.
    pub exit: ExitContract,
    /// Pointer used to learn the harness's persisted session/thread identifier.
    pub session_id_pointer: String,
    /// Client-side upper bound for a single invocation.
    pub timeout: Duration,
    /// Maximum source bytes parsed per invocation.
    pub max_output_bytes: usize,
    /// Hold fresh processes behind a launch gate until samplers establish
    /// durable ownership of their unique process groups.
    pub gate_launch: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedExecSession {
    local_id: String,
    marker: String,
    harness_id: String,
    turns: u64,
    #[serde(default)]
    invocations: u64,
    next_cursor: u64,
    closed: bool,
}

#[derive(Debug)]
struct ActiveInvocation {
    child: Child,
    stdout_path: PathBuf,
    gate_path: Option<PathBuf>,
    started: std::time::Instant,
    turn: u64,
}

#[derive(Debug)]
struct ExecSession {
    persisted: PersistedExecSession,
    active: Option<ActiveInvocation>,
}

/// Driver for ordinary CLIs that start a new OS process for each workflow turn.
///
/// Unlike RPC transports, this driver never invents `session.create` or
/// `session.attach` method calls. AHRB's local session handle only indexes the
/// harness's own stdout/journal evidence and the external session ID needed by a
/// later resume invocation.
pub struct PerInvocationDriver {
    config: PerInvocationConfig,
    state_root: PathBuf,
    sessions: BTreeMap<String, ExecSession>,
}

impl PerInvocationDriver {
    /// Construct a per-invocation CLI driver. Disk state is loaded by `start`.
    pub fn new(config: PerInvocationConfig) -> Self {
        let state_root = config.profile_root.join("ahrb-exec-sessions");
        Self {
            config,
            state_root,
            sessions: BTreeMap::new(),
        }
    }

    fn session_directory(&self, id: &str) -> PathBuf {
        self.state_root.join(id)
    }

    fn metadata_path(&self, id: &str) -> PathBuf {
        self.session_directory(id).join("session.json")
    }

    fn events_path(&self, id: &str) -> PathBuf {
        self.session_directory(id).join("events.jsonl")
    }

    fn persist_session_at(path: &Path, session: &PersistedExecSession) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("json.tmp");
        std::fs::write(&temporary, serde_json::to_vec_pretty(session)?)?;
        let file = std::fs::OpenOptions::new().write(true).open(&temporary)?;
        file.sync_all()?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }

    fn load_sessions(&mut self) -> Result<()> {
        std::fs::create_dir_all(&self.state_root)?;
        let mut entries =
            std::fs::read_dir(&self.state_root)?.collect::<std::result::Result<Vec<_>, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path().join("session.json");
            if !path.is_file() {
                continue;
            }
            let mut persisted: PersistedExecSession =
                serde_json::from_slice(&std::fs::read(&path)?)?;
            if self.config.events.source == "journal-file" {
                // The harness journal is authoritative. Rebuild AHRB's normalized
                // cache on every driver start so crash/replay evidence cannot be
                // satisfied by AHRB's own prior cache.
                match std::fs::remove_file(self.events_path(&persisted.local_id)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                persisted.next_cursor = 1;
            }
            self.sessions.insert(
                persisted.local_id.clone(),
                ExecSession {
                    persisted,
                    active: None,
                },
            );
        }
        Ok(())
    }

    fn stable_local_id(marker: &str) -> String {
        let digest = Sha256::digest(marker.as_bytes());
        let encoded = format!("{digest:x}");
        // Some CLIs require a caller-supplied UUID before their first event can
        // reveal a harness-native session ID. Keep this deterministic while
        // setting RFC 4122 version/variant nibbles.
        format!(
            "{}-{}-4{}-8{}-{}",
            &encoded[..8],
            &encoded[8..12],
            &encoded[13..16],
            &encoded[17..20],
            &encoded[20..32]
        )
    }

    #[cfg(unix)]
    fn create_launch_fifo(path: &Path) -> Result<()> {
        let encoded = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            AhrbError::Validation("launch-gate path contains a NUL byte".to_owned())
        })?;
        // SAFETY: `encoded` is a NUL-terminated path inside the isolated
        // invocation directory and mode 0600 grants access only to this user.
        if unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    fn invocation_variables(
        &self,
        session: &PersistedExecSession,
        prompt: &str,
        key: &str,
    ) -> BTreeMap<String, String> {
        let directory = self.session_directory(&session.local_id);
        let workspace = directory.join("workspace");
        let journal = directory.join("harness-journal.jsonl");
        let mut variables = self.config.base_variables.clone();
        variables.extend([
            (
                "profile".to_owned(),
                self.config.profile_root.to_string_lossy().into_owned(),
            ),
            ("prompt".to_owned(), prompt.to_owned()),
            (
                "session_id".to_owned(),
                if session.harness_id.is_empty() {
                    session.local_id.clone()
                } else {
                    session.harness_id.clone()
                },
            ),
            ("marker".to_owned(), session.marker.clone()),
            ("actor".to_owned(), session.marker.clone()),
            ("turn_key".to_owned(), key.to_owned()),
            (
                "workspace".to_owned(),
                workspace.to_string_lossy().into_owned(),
            ),
            ("journal".to_owned(), journal.to_string_lossy().into_owned()),
        ]);
        variables
    }

    fn render_invocation(
        &self,
        template: &[String],
        variables: &BTreeMap<String, String>,
        prompt: &str,
    ) -> Result<Vec<String>> {
        let contains_prompt = template
            .iter()
            .any(|argument| argument.contains("{{prompt}}"));
        let mut argv = template
            .iter()
            .map(|argument| crate::manifest::render_template(argument, variables))
            .collect::<Result<Vec<_>>>()?;
        if !contains_prompt {
            argv.push(prompt.to_owned());
        }
        if argv.is_empty() {
            return Err(AhrbError::Validation(
                "per-invocation command is empty".to_owned(),
            ));
        }
        Ok(argv)
    }

    fn bounded_read(path: &Path, maximum: usize, tail_only: bool) -> Result<Vec<u8>> {
        use std::io::{Read as _, Seek as _};
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let length = file.metadata()?.len();
        let maximum = u64::try_from(maximum).unwrap_or(u64::MAX);
        if maximum == 0 || length <= maximum {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            return Ok(bytes);
        }
        if tail_only {
            file.seek(std::io::SeekFrom::Start(length.saturating_sub(maximum)))?;
            let mut bytes = Vec::with_capacity(usize::try_from(maximum).unwrap_or(usize::MAX));
            file.read_to_end(&mut bytes)?;
            return Ok(bytes);
        }
        let prefix_len = maximum / 2;
        let tail_len = maximum.saturating_sub(prefix_len);
        let mut prefix = vec![0_u8; usize::try_from(prefix_len).unwrap_or(usize::MAX)];
        file.read_exact(&mut prefix)?;
        file.seek(std::io::SeekFrom::Start(length.saturating_sub(tail_len)))?;
        let mut tail = Vec::with_capacity(usize::try_from(tail_len).unwrap_or(usize::MAX));
        file.read_to_end(&mut tail)?;
        prefix.push(b'\n');
        prefix.extend(tail);
        Ok(prefix)
    }

    fn source_records(
        &self,
        session: &PersistedExecSession,
        stdout_path: Option<&Path>,
    ) -> Result<(Vec<Value>, Vec<u8>)> {
        let stdout = match stdout_path {
            Some(path) => Self::bounded_read(path, self.config.max_output_bytes, false)?,
            None => Vec::new(),
        };
        let bytes = match self.config.events.source.as_str() {
            "stdout" => stdout.clone(),
            "journal-file" => {
                let variables = self.invocation_variables(session, "", "");
                let path = crate::manifest::render_template(&self.config.events.path, &variables)?;
                // Earlier records are already present in AHRB's normalized cache;
                // retain the newest bounded tail so a growing journal cannot hide
                // later turns beyond a permanently capped prefix.
                Self::bounded_read(Path::new(&path), self.config.max_output_bytes, true)?
            }
            other => {
                return Err(AhrbError::Validation(format!(
                    "per-invocation events.source must be stdout or journal-file, not {other:?}"
                )));
            }
        };
        let records = if self.config.events.source == "journal-file"
            && matches!(self.config.events.framing.as_str(), "jsonl" | "json-seq")
        {
            parse_journal_records(&bytes)?
        } else {
            parse_event_records(&bytes, &self.config.events.framing)?
        };
        Ok((records, stdout))
    }

    fn read_cached_events(path: &Path) -> Result<Vec<NormalizedEvent>> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut events = Vec::new();
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            events.push(serde_json::from_slice(line)?);
        }
        Ok(events)
    }

    fn append_cached_events(path: &Path, events: &[NormalizedEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        for event in events {
            serde_json::to_writer(&mut file, event)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        Ok(())
    }

    fn normalize_records(
        mapping: &EventMapping,
        session: &mut PersistedExecSession,
        records: &[Value],
        existing: &BTreeMap<String, NormalizedEvent>,
        namespace: &str,
        allow_terminal: bool,
    ) -> Result<Vec<NormalizedEvent>> {
        let mut output = Vec::new();
        let mut normalizer = EventNormalizer::default();
        let mut effective = mapping.clone();
        effective.id_pointer = "/_ahrb_id".to_owned();
        effective.cursor_pointer = "/_ahrb_cursor".to_owned();
        for (index, raw) in records.iter().enumerate() {
            let event_type = raw
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| raw.get("event").and_then(Value::as_str));
            let Some(event_type) = event_type else {
                continue;
            };
            for (rule_index, rule) in mapping
                .rules
                .iter()
                .enumerate()
                .filter(|(_, rule)| rule.matches == event_type)
            {
                if !allow_terminal
                    && matches!(
                        rule.event.as_str(),
                        "terminal-success" | "terminal-failure" | "terminal-cancelled"
                    )
                {
                    continue;
                }
                let expanded = if rule.expand_pointer.is_empty() {
                    vec![None]
                } else {
                    raw.pointer(&rule.expand_pointer)
                        .and_then(Value::as_array)
                        .map(|values| values.iter().cloned().map(Some).collect())
                        .unwrap_or_default()
                };
                for (expand_index, expanded_value) in expanded.into_iter().enumerate() {
                    let mut prepared = raw.clone();
                    {
                        let object = prepared.as_object_mut().ok_or_else(|| {
                            AhrbError::Protocol("exec event record is not a JSON object".to_owned())
                        })?;
                        if let Some(value) = expanded_value {
                            object.insert("_ahrb_expanded".to_owned(), value);
                        }
                    }
                    if !rule_matches(&prepared, rule) {
                        continue;
                    }
                    let source_id = if mapping.id_pointer.is_empty() {
                        None
                    } else {
                        raw.pointer(&mapping.id_pointer)
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    };
                    let source_id = source_id.unwrap_or_else(|| {
                        let mut digest = Sha256::new();
                        if let Ok(bytes) = serde_json::to_vec(raw) {
                            digest.update(bytes);
                        }
                        digest.update(index.to_le_bytes());
                        format!("{:x}", digest.finalize())
                    });
                    let id = format!(
                        "{}:{namespace}:{source_id}:r{rule_index}:x{expand_index}",
                        session.local_id
                    );
                    if existing.contains_key(&id)
                        || output.iter().any(|event: &NormalizedEvent| event.id == id)
                    {
                        continue;
                    }
                    let object = prepared.as_object_mut().ok_or_else(|| {
                        AhrbError::Protocol("exec event record is not a JSON object".to_owned())
                    })?;
                    object.insert("_ahrb_id".to_owned(), Value::String(id));
                    object.insert("_ahrb_cursor".to_owned(), Value::from(session.next_cursor));
                    object
                        .entry("session_id".to_owned())
                        .or_insert_with(|| Value::String(session.local_id.clone()));
                    object
                        .entry("actor".to_owned())
                        .or_insert_with(|| Value::String(session.marker.clone()));
                    effective.rules.clear();
                    effective.rules.push(rule.clone());
                    if let Some(event) = normalizer.normalize(&prepared, &effective)? {
                        session.next_cursor = session.next_cursor.saturating_add(1);
                        output.push(event);
                    }
                }
            }
        }
        Ok(output)
    }

    fn learn_harness_id(&self, session: &mut PersistedExecSession, records: &[Value]) {
        if self.config.session_id_pointer.is_empty() || !session.harness_id.is_empty() {
            return;
        }
        if let Some(id) = records.iter().find_map(|record| {
            record
                .pointer(&self.config.session_id_pointer)
                .and_then(Value::as_str)
        }) {
            session.harness_id = id.to_owned();
        }
    }

    fn terminal_contract(
        &self,
        status: std::process::ExitStatus,
        stdout: &[u8],
    ) -> (EventVocab, Value) {
        let text = String::from_utf8_lossy(stdout);
        let code = status.code();
        let failure_marker = self
            .config
            .exit
            .failure_stdout
            .iter()
            .find(|marker| text.contains(marker.as_str()))
            .cloned();
        let success_marker_ok = self.config.exit.success_stdout.is_empty()
            || self
                .config
                .exit
                .success_stdout
                .iter()
                .any(|marker| text.contains(marker));
        let success =
            code == Some(self.config.exit.success) && failure_marker.is_none() && success_marker_ok;
        if success {
            (
                EventVocab::TerminalSuccess,
                json!({"status":"success", "exit_code":code}),
            )
        } else {
            let category = code
                .and_then(|value| {
                    self.config
                        .exit
                        .failures
                        .iter()
                        .find_map(|(category, expected)| {
                            (*expected == value).then(|| category.clone())
                        })
                })
                .unwrap_or_else(|| {
                    if failure_marker.is_some() {
                        "stdout-failure".to_owned()
                    } else if code == Some(self.config.exit.success) {
                        "stdout-contract".to_owned()
                    } else {
                        "unmapped-exit".to_owned()
                    }
                });
            (
                EventVocab::TerminalFailure,
                json!({
                    "status":"failure",
                    "category":category,
                    "exit_code":code,
                    "failure_marker":failure_marker
                }),
            )
        }
    }

    fn refresh_source(
        &self,
        session: &mut PersistedExecSession,
        active: &ActiveInvocation,
        completed: Option<std::process::ExitStatus>,
    ) -> Result<()> {
        let cache_path = self.events_path(&session.local_id);
        let cached = Self::read_cached_events(&cache_path)?;
        let by_id: BTreeMap<String, NormalizedEvent> = cached
            .iter()
            .cloned()
            .map(|event| (event.id.clone(), event))
            .collect();
        let (records, stdout) = self.source_records(session, Some(&active.stdout_path))?;
        self.learn_harness_id(session, &records);
        let namespace = if self.config.events.source == "stdout" {
            format!("turn-{}", active.turn)
        } else {
            "journal".to_owned()
        };
        let mut additions = Self::normalize_records(
            &self.config.events,
            session,
            &records,
            &by_id,
            &namespace,
            completed.is_some(),
        )?;
        if let Some(status) = completed {
            let (expected, payload) = self.terminal_contract(status, &stdout);
            let turn_prefix = format!("{}:turn-{}", session.local_id, active.turn);
            let mapped_terminal = additions.iter().chain(cached.iter()).rev().find(|event| {
                matches!(
                    event.event,
                    EventVocab::TerminalSuccess | EventVocab::TerminalFailure
                )
            });
            if mapped_terminal.is_none_or(|event| event.event != expected) {
                additions.push(NormalizedEvent {
                    id: format!("{turn_prefix}:terminal"),
                    cursor: session.next_cursor,
                    session_id: session.local_id.clone(),
                    actor: session.marker.clone(),
                    event: expected,
                    payload,
                });
                session.next_cursor = session.next_cursor.saturating_add(1);
            }
        }
        Self::append_cached_events(&cache_path, &additions)
    }

    fn refresh_inactive_journal(&self, session: &mut PersistedExecSession) -> Result<()> {
        if self.config.events.source != "journal-file" {
            return Ok(());
        }
        let cache_path = self.events_path(&session.local_id);
        let cached = Self::read_cached_events(&cache_path)?;
        let by_id: BTreeMap<String, NormalizedEvent> = cached
            .iter()
            .cloned()
            .map(|event| (event.id.clone(), event))
            .collect();
        let (records, _) = self.source_records(session, None)?;
        self.learn_harness_id(session, &records);
        let additions = Self::normalize_records(
            &self.config.events,
            session,
            &records,
            &by_id,
            "journal",
            true,
        )?;
        Self::append_cached_events(&cache_path, &additions)
    }

    async fn run_control_command(
        &self,
        template: &[String],
        session: &PersistedExecSession,
        release_token: Option<&str>,
    ) -> Result<()> {
        if template.is_empty() {
            return Err(AhrbError::Unsupported(
                "per-invocation control command is absent".to_owned(),
            ));
        }
        let mut variables = self.invocation_variables(session, "", "");
        variables.insert(
            "release_token".to_owned(),
            release_token.unwrap_or_default().to_owned(),
        );
        let argv = template
            .iter()
            .map(|argument| crate::manifest::render_template(argument, &variables))
            .collect::<Result<Vec<_>>>()?;
        let (program, arguments) = argv.split_first().ok_or_else(|| {
            AhrbError::Validation("per-invocation control command is empty".to_owned())
        })?;
        let mut command = Command::new(program);
        command
            .args(arguments)
            .envs(&self.config.environment)
            .current_dir(self.session_directory(&session.local_id).join("workspace"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let status = tokio::time::timeout(self.config.timeout, command.status())
            .await
            .map_err(|_| AhrbError::Timeout("per-invocation control command".to_owned()))??;
        if !status.success() {
            return Err(AhrbError::Protocol(format!(
                "per-invocation control command exited with {status}"
            )));
        }
        Ok(())
    }

    fn cached_after(&self, id: &str, after: Option<Cursor>) -> Result<Vec<NormalizedEvent>> {
        let mut events = Self::read_cached_events(&self.events_path(id))?;
        events.retain(|event| after.is_none_or(|cursor| event.cursor > cursor.0));
        events.sort_by_key(|event| event.cursor);
        Ok(events)
    }
}

impl Driver for PerInvocationDriver {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move { self.load_sessions() })
    }

    fn create_session(&mut self, marker: &str) -> DriverFuture<'_, SessionId> {
        let marker = marker.to_owned();
        Box::pin(async move {
            if marker.trim().is_empty() {
                return Err(AhrbError::Validation(
                    "exec session marker is empty".to_owned(),
                ));
            }
            let id = Self::stable_local_id(&marker);
            if let Some(session) = self.sessions.get_mut(&id) {
                session.persisted.closed = false;
                let persisted = session.persisted.clone();
                Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
                return Ok(SessionId(id));
            }
            let persisted = PersistedExecSession {
                local_id: id.clone(),
                marker,
                harness_id: String::new(),
                turns: 0,
                invocations: 0,
                next_cursor: 1,
                closed: false,
            };
            let directory = self.session_directory(&id);
            std::fs::create_dir_all(directory.join("workspace"))?;
            Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
            self.sessions.insert(
                id.clone(),
                ExecSession {
                    persisted,
                    active: None,
                },
            );
            Ok(SessionId(id))
        })
    }

    fn submit(&mut self, session: &SessionId, prompt: &str, key: &str) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        let prompt = prompt.to_owned();
        let key = key.to_owned();
        Box::pin(async move {
            let persisted = self
                .sessions
                .get(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .persisted
                .clone();
            if self
                .sessions
                .get(&id)
                .is_some_and(|item| item.active.is_some())
            {
                return Err(AhrbError::Protocol(format!(
                    "exec session {id:?} already has an active invocation"
                )));
            }
            if persisted.closed {
                return Err(AhrbError::Protocol(format!(
                    "exec session {id:?} is closed"
                )));
            }
            let template = if persisted.turns > 0 && !self.config.resume_command.is_empty() {
                &self.config.resume_command
            } else {
                &self.config.command
            };
            let variables = self.invocation_variables(&persisted, &prompt, &key);
            let argv = self.render_invocation(template, &variables, &prompt)?;
            let directory = self.session_directory(&id);
            let turn = persisted.invocations.saturating_add(1);
            let mut launched = persisted.clone();
            launched.invocations = turn;
            Self::persist_session_at(&self.metadata_path(&id), &launched)?;
            if let Some(item) = self.sessions.get_mut(&id) {
                item.persisted = launched;
            }
            let stdout_path = directory.join(format!("turn-{turn:06}.stdout"));
            let stderr_path = directory.join(format!("turn-{turn:06}.stderr"));
            let stdout = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&stdout_path)?;
            let stderr = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&stderr_path)?;
            let (program, arguments) = argv.split_first().ok_or_else(|| {
                AhrbError::Validation("per-invocation command is empty".to_owned())
            })?;
            let gate_path = self
                .config
                .gate_launch
                .then(|| directory.join(format!("turn-{turn:06}.launch")));
            let mut command = if let Some(gate) = &gate_path {
                Self::create_launch_fifo(gate)?;
                let mut gated = Command::new("/bin/sh");
                gated
                    .arg("-c")
                    .arg("gate=$1; shift; IFS= read -r ahrb_release < \"$gate\"; exec \"$@\"")
                    .arg("ahrb-exec-launch-gate")
                    .arg(gate)
                    .arg(program)
                    .args(arguments);
                gated
            } else {
                let mut direct = Command::new(program);
                direct.args(arguments);
                direct
            };
            #[cfg(unix)]
            command.process_group(0);
            command
                .envs(&self.config.environment)
                .current_dir(directory.join("workspace"))
                .stdin(Stdio::null())
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            let child = command.spawn()?;
            let item = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?;
            item.active = Some(ActiveInvocation {
                child,
                stdout_path,
                gate_path,
                started: std::time::Instant::now(),
                turn,
            });
            Ok(())
        })
    }

    fn attach(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>> {
        let id = session.0.clone();
        Box::pin(async move {
            let mut active = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .active
                .take();
            if let Some(mut invocation) = active.take() {
                let mut status = invocation.child.try_wait()?;
                if status.is_none() && invocation.started.elapsed() >= self.config.timeout {
                    invocation.child.kill().await?;
                    status = Some(invocation.child.wait().await?);
                }
                let mut persisted = self
                    .sessions
                    .get(&id)
                    .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?
                    .persisted
                    .clone();
                self.refresh_source(&mut persisted, &invocation, status)?;
                if status.is_some() {
                    persisted.turns = persisted.turns.saturating_add(1);
                    Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
                    let item = self.sessions.get_mut(&id).ok_or_else(|| {
                        AhrbError::Protocol(format!("exec session {id:?} disappeared"))
                    })?;
                    item.persisted = persisted;
                    item.active = None;
                } else {
                    let item = self.sessions.get_mut(&id).ok_or_else(|| {
                        AhrbError::Protocol(format!("exec session {id:?} disappeared"))
                    })?;
                    item.persisted = persisted;
                    item.active = Some(invocation);
                }
            } else if self.config.events.source == "journal-file" {
                let mut persisted = self
                    .sessions
                    .get(&id)
                    .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?
                    .persisted
                    .clone();
                self.refresh_inactive_journal(&mut persisted)?;
                Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
                if let Some(item) = self.sessions.get_mut(&id) {
                    item.persisted = persisted;
                }
            }
            self.cached_after(&id, after)
        })
    }

    fn replay_persisted(
        &mut self,
        session: &SessionId,
        after: Option<Cursor>,
    ) -> DriverFuture<'_, Vec<NormalizedEvent>> {
        let id = session.0.clone();
        Box::pin(async move {
            let persisted = self
                .sessions
                .get(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .persisted
                .clone();
            if self.config.replay_command.is_empty() {
                return Err(AhrbError::Unsupported(
                    "per-invocation durable replay command is absent".to_owned(),
                ));
            }
            let mut variables = self.invocation_variables(&persisted, "", "");
            variables.insert(
                "cursor".to_owned(),
                after.map_or_else(|| "0".to_owned(), |cursor| cursor.0.to_string()),
            );
            let argv = self
                .config
                .replay_command
                .iter()
                .map(|argument| crate::manifest::render_template(argument, &variables))
                .collect::<Result<Vec<_>>>()?;
            let (program, arguments) = argv.split_first().ok_or_else(|| {
                AhrbError::Validation("per-invocation replay command is empty".to_owned())
            })?;
            let mut command = Command::new(program);
            command
                .args(arguments)
                .envs(&self.config.environment)
                .current_dir(self.session_directory(&id).join("workspace"))
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let output = tokio::time::timeout(self.config.timeout, command.output())
                .await
                .map_err(|_| AhrbError::Timeout("per-invocation durable replay".to_owned()))??;
            if !output.status.success() {
                return Err(AhrbError::Protocol(format!(
                    "per-invocation durable replay exited with {}",
                    output.status
                )));
            }
            if output.stdout.len() > self.config.max_output_bytes {
                return Err(AhrbError::Protocol(
                    "per-invocation durable replay exceeded capture bound".to_owned(),
                ));
            }
            if !output.stdout.is_empty() && !output.stdout.ends_with(b"\n") {
                return Err(AhrbError::Protocol(
                    "durable replay output ended with a torn record".to_owned(),
                ));
            }
            let records = parse_journal_records(&output.stdout)?;
            for record in &records {
                let cursor = record
                    .pointer(&self.config.events.cursor_pointer)
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "durable replay output omitted its source cursor".to_owned(),
                        )
                    })?;
                if after.is_some_and(|after| cursor <= after.0) {
                    return Err(AhrbError::Protocol(
                        "durable replay returned an event at or before its cursor".to_owned(),
                    ));
                }
            }
            let mut replayed = persisted;
            replayed.next_cursor = after.map_or(1, |cursor| cursor.0.saturating_add(1));
            Self::normalize_records(
                &self.config.events,
                &mut replayed,
                &records,
                &BTreeMap::new(),
                "journal",
                true,
            )
        })
    }

    fn resume(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        Box::pin(async move {
            if self
                .sessions
                .get(&id)
                .is_some_and(|session| session.active.is_some())
            {
                return Err(AhrbError::Protocol(format!(
                    "cannot replay active exec session {id:?}"
                )));
            }
            let path = self.metadata_path(&id);
            let mut persisted: PersistedExecSession =
                serde_json::from_slice(&std::fs::read(&path).map_err(|error| {
                    AhrbError::Protocol(format!(
                        "could not reopen exec session {id:?} metadata: {error}"
                    ))
                })?)?;
            if self.config.events.source == "journal-file" {
                match std::fs::remove_file(self.events_path(&id)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                persisted.next_cursor = 1;
                self.refresh_inactive_journal(&mut persisted)?;
            }
            Self::persist_session_at(&path, &persisted)?;
            self.sessions.insert(
                id,
                ExecSession {
                    persisted,
                    active: None,
                },
            );
            Ok(())
        })
    }

    fn steer(&mut self, _session: &SessionId, _prompt: &str) -> DriverFuture<'_, ()> {
        Box::pin(async {
            Err(AhrbError::Unsupported(
                "per-invocation CLI does not declare safe-boundary steer".to_owned(),
            ))
        })
    }

    fn subturn(&mut self, _session: &SessionId, _prompt: &str) -> DriverFuture<'_, ()> {
        Box::pin(async {
            Err(AhrbError::Unsupported(
                "per-invocation CLI does not declare pre-tool subturn".to_owned(),
            ))
        })
    }

    fn queue(&mut self, _session: &SessionId, _prompt: &str, _key: &str) -> DriverFuture<'_, ()> {
        Box::pin(async {
            Err(AhrbError::Unsupported(
                "per-invocation CLI does not declare queued input".to_owned(),
            ))
        })
    }

    fn release_checkpoint(
        &mut self,
        session: &SessionId,
        release_token: &str,
    ) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        let release_token = release_token.to_owned();
        Box::pin(async move {
            let persisted = self
                .sessions
                .get(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .persisted
                .clone();
            let command = self.config.release_command.clone();
            self.run_control_command(&command, &persisted, Some(&release_token))
                .await
        })
    }

    fn spawn_agent(
        &mut self,
        _parent: &SessionId,
        _marker: &str,
        _prompt: Option<&str>,
    ) -> DriverFuture<'_, SessionId> {
        Box::pin(async {
            Err(AhrbError::Unsupported(
                "per-invocation CLI does not declare native child spawn".to_owned(),
            ))
        })
    }

    fn cancel(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        Box::pin(async move {
            let active = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .active
                .take();
            if let Some(mut invocation) = active {
                invocation.child.kill().await?;
                let _status = invocation.child.wait().await?;
                let mut persisted = self
                    .sessions
                    .get(&id)
                    .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?
                    .persisted
                    .clone();
                if self.config.cancel_command.is_empty() {
                    let event = NormalizedEvent {
                        id: format!("{}:turn-{}:cancelled", id, invocation.turn),
                        cursor: persisted.next_cursor,
                        session_id: id.clone(),
                        actor: persisted.marker.clone(),
                        event: EventVocab::TerminalCancelled,
                        payload: json!({"status":"cancelled"}),
                    };
                    persisted.next_cursor = persisted.next_cursor.saturating_add(1);
                    Self::append_cached_events(&self.events_path(&id), &[event])?;
                } else {
                    let command = self.config.cancel_command.clone();
                    self.run_control_command(&command, &persisted, None).await?;
                    self.refresh_inactive_journal(&mut persisted)?;
                }
                persisted.turns = persisted.turns.saturating_add(1);
                Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
                let workspace = self.session_directory(&id).join("workspace");
                if workspace.exists() {
                    std::fs::remove_dir_all(&workspace)?;
                }
                std::fs::create_dir(&workspace)?;
                if let Some(item) = self.sessions.get_mut(&id) {
                    item.persisted = persisted;
                }
            }
            Ok(())
        })
    }

    fn close(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        Box::pin(async move {
            if self
                .sessions
                .get(&id)
                .is_some_and(|item| item.active.is_some())
            {
                self.cancel(&SessionId(id.clone())).await?;
            }
            let item = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?;
            item.persisted.closed = true;
            let persisted = item.persisted.clone();
            Self::persist_session_at(&self.metadata_path(&id), &persisted)
        })
    }

    fn shutdown(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            let ids: Vec<String> = self
                .sessions
                .iter()
                .filter(|(_, session)| session.active.is_some())
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                self.cancel(&SessionId(id)).await?;
            }
            Ok(())
        })
    }

    fn release_invocations(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            use std::io::Write as _;
            for session in self.sessions.values() {
                let Some(path) = session
                    .active
                    .as_ref()
                    .and_then(|invocation| invocation.gate_path.as_ref())
                else {
                    continue;
                };
                let mut gate = std::fs::OpenOptions::new().write(true).open(path)?;
                gate.write_all(b"release\n")?;
                drop(gate);
                std::fs::remove_file(path)?;
            }
            Ok(())
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        self.sessions
            .values()
            .filter_map(|session| session.active.as_ref())
            .filter_map(|active| active.child.id())
            .collect()
    }

    fn session_pids(&self, session: &SessionId) -> Vec<u32> {
        self.sessions
            .get(&session.0)
            .and_then(|item| item.active.as_ref())
            .and_then(|active| active.child.id())
            .into_iter()
            .collect()
    }
}

fn parse_event_records(bytes: &[u8], framing: &str) -> Result<Vec<Value>> {
    match framing {
        "jsonl" | "json-seq" => {
            let mut records = Vec::new();
            for line in bytes.split(|byte| *byte == b'\n') {
                let line = trim_ascii(line);
                if line.is_empty() {
                    continue;
                }
                if let Ok(value) = serde_json::from_slice(line) {
                    records.push(value);
                }
            }
            Ok(records)
        }
        "json" => {
            let bytes = trim_ascii(bytes);
            if bytes.is_empty() {
                Ok(Vec::new())
            } else {
                let value: Value = serde_json::from_slice(bytes)?;
                Ok(value.as_array().cloned().unwrap_or_else(|| vec![value]))
            }
        }
        "sse" => {
            let mut records = Vec::new();
            for line in bytes.split(|byte| *byte == b'\n') {
                let line = trim_ascii(line);
                let Some(data) = line.strip_prefix(b"data:") else {
                    continue;
                };
                let data = trim_ascii(data);
                if data == b"[DONE]" || data.is_empty() {
                    continue;
                }
                if let Ok(value) = serde_json::from_slice(data) {
                    records.push(value);
                }
            }
            Ok(records)
        }
        other => Err(AhrbError::Validation(format!(
            "unsupported event framing {other:?}"
        ))),
    }
}

fn parse_journal_records(bytes: &[u8]) -> Result<Vec<Value>> {
    let mut records = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if line.last() != Some(&b'\n') {
            // Only a non-newline-terminated final record is a permitted torn tail.
            break;
        }
        let line = trim_ascii(&line[..line.len().saturating_sub(1)]);
        if line.is_empty() {
            continue;
        }
        records.push(serde_json::from_slice(line).map_err(|error| {
            AhrbError::Protocol(format!("corrupt durable journal record: {error}"))
        })?);
    }
    Ok(records)
}

/// One-process-per-operation transport using JSON on stdin and stdout.
pub struct ExecTransport {
    command: Vec<String>,
    timeout: Duration,
    sequence: u64,
    environment: BTreeMap<String, String>,
}

impl ExecTransport {
    /// Construct an exec transport. The argv is executed directly, never via a shell.
    pub fn new(command: Vec<String>, timeout: Duration) -> Self {
        Self {
            command,
            timeout,
            sequence: 0,
            environment: BTreeMap::new(),
        }
    }

    /// Add deterministic per-process environment bindings.
    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }
}

impl Transport for ExecTransport {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        Box::pin(async move {
            self.sequence = self.sequence.saturating_add(1);
            let (id, mut bytes) = rpc_bytes(self.sequence, &request)?;
            bytes.push(b'\n');
            let mut command = command_from_argv(&self.command)?;
            command
                .envs(&self.environment)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut child = command.spawn()?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| AhrbError::Protocol("exec transport has no stdin".to_owned()))?;
            stdin.write_all(&bytes).await?;
            stdin.shutdown().await?;
            let output = tokio::time::timeout(self.timeout, child.wait_with_output())
                .await
                .map_err(|_| AhrbError::Timeout("exec transport request".to_owned()))??;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(AhrbError::Protocol(format!(
                    "exec transport exited with {}: {}",
                    output.status,
                    stderr.trim()
                )));
            }
            parse_rpc(&id, trim_ascii(&output.stdout))
        })
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Persistent child transport using newline-delimited JSON-RPC.
pub struct StdinRpcTransport {
    command: Vec<String>,
    timeout: Duration,
    sequence: u64,
    environment: BTreeMap<String, String>,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<tokio::io::Lines<BufReader<ChildStdout>>>,
}

impl StdinRpcTransport {
    /// Construct a persistent stdin-RPC transport.
    pub fn new(command: Vec<String>, timeout: Duration) -> Self {
        Self {
            command,
            timeout,
            sequence: 0,
            environment: BTreeMap::new(),
            child: None,
            stdin: None,
            stdout: None,
        }
    }

    /// Add deterministic per-process environment bindings.
    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }

    /// Return the persistent child's PID after startup for process-tree ownership.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }
}

impl Transport for StdinRpcTransport {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            if self.child.is_some() {
                return Ok(());
            }
            let mut command = command_from_argv(&self.command)?;
            command
                .envs(&self.environment)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            let mut child = command.spawn()?;
            let stdin = child
                .stdin
                .take()
                .ok_or_else(|| AhrbError::Protocol("stdin-RPC child has no stdin".to_owned()))?;
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| AhrbError::Protocol("stdin-RPC child has no stdout".to_owned()))?;
            self.stdin = Some(stdin);
            self.stdout = Some(BufReader::new(stdout).lines());
            self.child = Some(child);
            Ok(())
        })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        Box::pin(async move {
            if self.child.is_none() {
                return Err(AhrbError::Protocol(
                    "stdin-RPC transport was not started".to_owned(),
                ));
            }
            self.sequence = self.sequence.saturating_add(1);
            let (id, mut bytes) = rpc_bytes(self.sequence, &request)?;
            bytes.push(b'\n');
            let stdin = self
                .stdin
                .as_mut()
                .ok_or_else(|| AhrbError::Protocol("stdin-RPC stdin is closed".to_owned()))?;
            stdin.write_all(&bytes).await?;
            stdin.flush().await?;
            let stdout = self
                .stdout
                .as_mut()
                .ok_or_else(|| AhrbError::Protocol("stdin-RPC stdout is closed".to_owned()))?;
            let line = tokio::time::timeout(self.timeout, stdout.next_line())
                .await
                .map_err(|_| AhrbError::Timeout("stdin-RPC request".to_owned()))??
                .ok_or_else(|| AhrbError::Protocol("stdin-RPC peer closed stdout".to_owned()))?;
            parse_rpc(&id, line.as_bytes())
        })
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            self.stdin.take();
            self.stdout.take();
            if let Some(mut child) = self.child.take() {
                match tokio::time::timeout(self.timeout, child.wait()).await {
                    Ok(status) => {
                        status?;
                    }
                    Err(_) => {
                        child.kill().await?;
                        child.wait().await?;
                    }
                }
            }
            Ok(())
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        self.pid().into_iter().collect()
    }
}

/// Unix-domain socket JSON-RPC transport.
pub struct SocketJsonRpcTransport {
    endpoint: PathBuf,
    timeout: Duration,
    sequence: u64,
}

impl SocketJsonRpcTransport {
    /// Construct a newline-delimited JSON-RPC Unix socket transport.
    pub fn new(endpoint: PathBuf, timeout: Duration) -> Self {
        Self {
            endpoint,
            timeout,
            sequence: 0,
        }
    }
}

impl Transport for SocketJsonRpcTransport {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        Box::pin(async move {
            self.sequence = self.sequence.saturating_add(1);
            let (id, mut bytes) = rpc_bytes(self.sequence, &request)?;
            bytes.push(b'\n');
            #[cfg(unix)]
            {
                let stream = tokio::time::timeout(
                    self.timeout,
                    tokio::net::UnixStream::connect(&self.endpoint),
                )
                .await
                .map_err(|_| AhrbError::Timeout("socket JSON-RPC connect".to_owned()))??;
                let (read, mut write) = stream.into_split();
                write.write_all(&bytes).await?;
                write.shutdown().await?;
                let mut lines = BufReader::new(read).lines();
                let line = tokio::time::timeout(self.timeout, lines.next_line())
                    .await
                    .map_err(|_| AhrbError::Timeout("socket JSON-RPC request".to_owned()))??
                    .ok_or_else(|| AhrbError::Protocol("socket JSON-RPC peer closed".to_owned()))?;
                parse_rpc(&id, line.as_bytes())
            }
            #[cfg(not(unix))]
            {
                let _ = bytes;
                Err(AhrbError::Unsupported(
                    "Unix socket transport requires Unix".to_owned(),
                ))
            }
        })
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Plain-loopback HTTP JSON-RPC transport.
pub struct HttpTransport {
    endpoint: String,
    timeout: Duration,
    sequence: u64,
    headers: BTreeMap<String, String>,
}

impl HttpTransport {
    /// Construct an HTTP transport. Only plain HTTP to loopback is accepted.
    pub fn new(endpoint: String, timeout: Duration) -> Self {
        Self {
            endpoint,
            timeout,
            sequence: 0,
            headers: BTreeMap::new(),
        }
    }

    /// Add request headers, typically a run-local bearer credential.
    pub fn with_headers(mut self, headers: BTreeMap<String, String>) -> Self {
        self.headers = headers;
        self
    }
}

impl Transport for HttpTransport {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        Box::pin(async move {
            self.sequence = self.sequence.saturating_add(1);
            let (id, bytes) = rpc_bytes(self.sequence, &request)?;
            let response = http_post(&self.endpoint, &self.headers, &bytes, self.timeout).await?;
            if !(200..300).contains(&response.status) {
                return Err(AhrbError::Protocol(format!(
                    "HTTP transport returned status {}",
                    response.status
                )));
            }
            parse_rpc(&id, &response.body)
        })
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Minimal HTTP response used by the mock model client and HTTP transport.
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

/// POST a bounded body over plain HTTP to a loopback endpoint.
pub(crate) async fn http_post(
    endpoint: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
    timeout: Duration,
) -> Result<HttpResponse> {
    let endpoint = ParsedHttpUrl::parse(endpoint)?;
    let future = async {
        let mut stream =
            tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await?;
        let mut request = format!(
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            endpoint.path,
            endpoint.host,
            endpoint.port,
            body.len()
        )
        .into_bytes();
        for (name, value) in headers {
            if !valid_header(name) || value.contains(['\r', '\n']) {
                return Err(AhrbError::Validation(format!(
                    "invalid HTTP header {name:?}"
                )));
            }
            request.extend_from_slice(name.as_bytes());
            request.extend_from_slice(b": ");
            request.extend_from_slice(value.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        stream.write_all(&request).await?;
        let mut response = Vec::new();
        stream
            .take(16 * 1024 * 1024)
            .read_to_end(&mut response)
            .await?;
        parse_http_response(&response)
    };
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| AhrbError::Timeout("HTTP request idle deadline".to_owned()))?
}

/// POST the same bounded HTTP request over a Unix-domain socket.
#[cfg(unix)]
pub(crate) async fn unix_http_post(
    socket_path: &std::path::Path,
    path: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
    timeout: Duration,
) -> Result<HttpResponse> {
    if !path.starts_with('/') || path.contains(['\r', '\n']) {
        return Err(AhrbError::Validation(
            "Unix HTTP request path must be absolute".to_owned(),
        ));
    }
    let future = async {
        let mut stream = tokio::net::UnixStream::connect(socket_path).await?;
        let request = http_request_bytes(path, "localhost", headers, body)?;
        stream.write_all(&request).await?;
        let mut response = Vec::new();
        stream
            .take(16 * 1024 * 1024)
            .read_to_end(&mut response)
            .await?;
        parse_http_response(&response)
    };
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| AhrbError::Timeout("Unix HTTP request idle deadline".to_owned()))?
}

#[cfg(unix)]
fn http_request_bytes(
    path: &str,
    host: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> Result<Vec<u8>> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .into_bytes();
    append_http_headers(&mut request, headers)?;
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    Ok(request)
}

#[cfg(unix)]
fn append_http_headers(request: &mut Vec<u8>, headers: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in headers {
        if !valid_header(name) || value.contains(['\r', '\n']) {
            return Err(AhrbError::Validation(format!(
                "invalid HTTP header {name:?}"
            )));
        }
        request.extend_from_slice(name.as_bytes());
        request.extend_from_slice(b": ");
        request.extend_from_slice(value.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    Ok(())
}

fn valid_header(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

struct ParsedHttpUrl {
    host: String,
    port: u16,
    path: String,
}

impl ParsedHttpUrl {
    fn parse(url: &str) -> Result<Self> {
        let rest = url.strip_prefix("http://").ok_or_else(|| {
            AhrbError::Unsupported("only plain loopback HTTP is supported".to_owned())
        })?;
        let (authority, path) = rest
            .split_once('/')
            .map(|(authority, path)| (authority, format!("/{path}")))
            .unwrap_or((rest, "/".to_owned()));
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| AhrbError::Validation("HTTP endpoint must include a port".to_owned()))?;
        let host = host.trim_matches(['[', ']']);
        let loopback = host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .map(|address| address.is_loopback())
                .unwrap_or(false);
        if !loopback {
            return Err(AhrbError::Validation(
                "HTTP transport endpoint must be loopback".to_owned(),
            ));
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| AhrbError::Validation("invalid HTTP endpoint port".to_owned()))?;
        Ok(Self {
            host: host.to_owned(),
            port,
            path,
        })
    }
}

fn parse_http_response(response: &[u8]) -> Result<HttpResponse> {
    let split = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| AhrbError::Protocol("malformed HTTP response".to_owned()))?;
    let head = std::str::from_utf8(&response[..split])
        .map_err(|_| AhrbError::Protocol("non-UTF-8 HTTP response headers".to_owned()))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| AhrbError::Protocol("malformed HTTP status line".to_owned()))?;
    let body = response[split + 4..].to_vec();
    Ok(HttpResponse { status, body })
}

fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|position| position + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{EventMapping, EventRule};

    struct NoopTransport;

    impl Transport for NoopTransport {
        fn start(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn request(&mut self, _request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
            Box::pin(async {
                Err(AhrbError::Unsupported(
                    "noop transport has no requests".to_owned(),
                ))
            })
        }

        fn stop(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn rejects_non_loopback_http() {
        assert!(ParsedHttpUrl::parse("http://example.com:80/rpc").is_err());
    }

    #[test]
    fn accepts_ipv4_loopback_http() {
        let parsed = ParsedHttpUrl::parse("http://127.0.0.1:8080/rpc").expect("parse");
        assert_eq!(parsed.port, 8080);
        assert_eq!(parsed.path, "/rpc");
    }

    #[test]
    fn rpc_error_is_typed() {
        let error = parse_rpc(
            "1",
            br#"{"jsonrpc":"2.0","id":"1","error":{"code":-1,"message":"bad"}}"#,
        )
        .expect_err("RPC error");
        assert!(error.to_string().contains("RPC error -1"));
    }

    #[test]
    fn exec_rules_support_nested_predicates_and_array_expansion() {
        let mapping = EventMapping {
            source: "stdout".to_owned(),
            path: String::new(),
            framing: "jsonl".to_owned(),
            id_pointer: String::new(),
            cursor_pointer: String::new(),
            replay_command: Vec::new(),
            rules: vec![
                EventRule {
                    matches: "message".to_owned(),
                    match_fields: BTreeMap::from([(
                        "/_ahrb_expanded/type".to_owned(),
                        "tool_use".to_owned(),
                    )]),
                    expand_pointer: "/content".to_owned(),
                    event: "tool-call".to_owned(),
                    payload_pointer: "/_ahrb_expanded".to_owned(),
                },
                EventRule {
                    matches: "message".to_owned(),
                    match_fields: BTreeMap::from([(
                        "/_ahrb_expanded/type".to_owned(),
                        "tool_result".to_owned(),
                    )]),
                    expand_pointer: "/content".to_owned(),
                    event: "tool-result".to_owned(),
                    payload_pointer: "/_ahrb_expanded".to_owned(),
                },
            ],
        };
        let mut session = PersistedExecSession {
            local_id: "00000000-0000-4000-8000-000000000000".to_owned(),
            marker: "actor".to_owned(),
            harness_id: String::new(),
            turns: 1,
            invocations: 1,
            next_cursor: 1,
            closed: false,
        };
        let records = vec![json!({
            "type":"message",
            "content":[
                {"type":"tool_use","id":"call-1"},
                {"type":"tool_result","tool_use_id":"call-1"}
            ]
        })];
        let events = PerInvocationDriver::normalize_records(
            &mapping,
            &mut session,
            &records,
            &BTreeMap::new(),
            "turn-1",
            true,
        )
        .expect("normalize expanded records");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, EventVocab::ToolCall);
        assert_eq!(events[1].event, EventVocab::ToolResult);
        assert_ne!(events[0].id, events[1].id);
        assert_eq!(events[0].cursor, 1);
        assert_eq!(events[1].cursor, 2);
    }

    #[tokio::test]
    async fn managed_daemon_is_cold_owned_and_reaped() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-managed-daemon-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if logs.exists() {
            std::fs::remove_dir_all(&logs).expect("remove stale daemon logs");
        }
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec!["/bin/sleep".to_owned(), "30".to_owned()],
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "process".to_owned(),
                    target: String::new(),
                    timeout_ms: 1_000,
                },
                grace: Duration::from_millis(100),
                log_directory: logs.clone(),
            },
        );
        transport.start().await.expect("start managed daemon");
        let pids = transport.owned_pids();
        assert_eq!(pids.len(), 1);
        let pid = pids[0];
        transport.stop().await.expect("stop managed daemon");
        assert!(transport.owned_pids().is_empty());
        #[cfg(unix)]
        {
            let pid = i32::try_from(pid).expect("test PID fits i32");
            // SAFETY: signal zero performs a read-only liveness probe.
            let result = unsafe { libc::kill(pid, 0) };
            assert_eq!(result, -1);
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ESRCH)
            );
        }
        std::fs::remove_dir_all(logs).expect("remove daemon logs");
    }

    #[cfg(unix)]
    async fn wait_until_pid_is_dead(pid: u32) -> bool {
        let pid = i32::try_from(pid).expect("test PID fits i32");
        for _ in 0..200 {
            // SAFETY: signal zero performs a read-only liveness probe.
            if unsafe { libc::kill(pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[cfg(unix)]
    async fn worker_pid(path: &Path) -> u32 {
        for _ in 0..200 {
            if let Ok(contents) = std::fs::read_to_string(path) {
                if let Ok(pid) = contents.trim().parse() {
                    return pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("worker PID did not become parseable: {}", path.display());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_daemon_sweeps_worker_when_leader_exits_before_readiness() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-managed-early-exit-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        let worker_file = logs.join("worker.pid");
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "sleep 30 & worker=$!; echo \"$worker\" > \"$1\"; exit 9".to_owned(),
                    "ahrb-managed-daemon".to_owned(),
                    worker_file.to_string_lossy().into_owned(),
                ],
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: logs.join("never-ready").to_string_lossy().into_owned(),
                    timeout_ms: 1_000,
                },
                grace: Duration::from_millis(100),
                log_directory: logs.clone(),
            },
        );
        let error = transport
            .start()
            .await
            .expect_err("leader exit must fail readiness");
        assert!(error.to_string().contains("exited before readiness"));
        let worker = worker_pid(&worker_file).await;
        assert!(wait_until_pid_is_dead(worker).await);
        std::fs::remove_dir_all(logs).expect("remove daemon logs");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_daemon_drop_sweeps_worker_after_later_error() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-managed-drop-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        let worker_file = logs.join("worker.pid");
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "sleep 30 & worker=$!; echo \"$worker\" > \"$1\"; wait".to_owned(),
                    "ahrb-managed-daemon".to_owned(),
                    worker_file.to_string_lossy().into_owned(),
                ],
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: worker_file.to_string_lossy().into_owned(),
                    timeout_ms: 1_000,
                },
                grace: Duration::from_millis(100),
                log_directory: logs.clone(),
            },
        );
        transport.start().await.expect("start managed daemon");
        let worker = worker_pid(&worker_file).await;
        drop(transport);
        assert!(wait_until_pid_is_dead(worker).await);
        std::fs::remove_dir_all(logs).expect("remove daemon logs");
    }
}
