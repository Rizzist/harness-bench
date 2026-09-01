//! Harness operations over exec, stdin-RPC, socket JSON-RPC, or HTTP transports.
//!
//! All transports use the same small JSON-RPC-like envelope.  The envelope keeps the
//! semantic driver independent from process and wire framing, while the monotonically
//! increasing request ID makes recordings deterministic.

use crate::events::{
    EventNormalizer, EventVocab, NATIVE_FIXTURE_METADATA_PREFIX, NormalizedEvent, rule_matches,
};
use crate::manifest::{EventMapping, ExitContract, Probe, ShutdownResult};
use crate::process::ProcIdentity;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
    /// Await a readiness probe after the owned process has been launched.
    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Send one operation and receive its response.
    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse>;
    /// Stop persistent transport resources.
    fn stop(&mut self) -> DriverFuture<'_, ()>;
    /// Launcher/controller PIDs directly owned by this transport.
    fn owned_pids(&self) -> Vec<u32> {
        Vec::new()
    }
    /// Daemon root PID captured from the readiness contract, when distinct
    /// from the client transport.
    fn daemon_pid(&self) -> Option<u32> {
        None
    }
    /// Lifecycle notes emitted while reclaiming owned daemon processes.
    fn lifecycle_notes(&self) -> Vec<String> {
        Vec::new()
    }
}

static DAEMON_LOG_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Lifecycle settings for a daemon that is separate from its socket/HTTP client.
#[derive(Clone, Debug)]
pub struct ManagedDaemonConfig {
    /// Fully rendered cold-start argv.
    pub command: Vec<String>,
    /// The start command is a finite launcher for a double-forked daemon.
    pub launcher_exits: bool,
    /// Fully rendered one-time setup argv run after readiness.
    pub initialize_command: Vec<String>,
    /// Profile-local success marker for the initialization command.
    pub initialize_marker: PathBuf,
    /// Isolated environment inherited by the daemon.
    pub environment: BTreeMap<String, String>,
    /// Readiness condition evaluated before the inner client starts.
    pub readiness: Probe,
    /// Fully rendered graceful shutdown argv.
    pub shutdown_command: Vec<String>,
    /// Typed shutdown-result contract.
    pub shutdown_result: ShutdownResult,
    /// Grace period before a process-group SIGKILL.
    pub grace: Duration,
    /// Fresh run directory that receives bounded daemon stdout/stderr files.
    pub log_directory: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ReadinessObservation {
    pub metadata: Value,
    pub pid: Option<u32>,
}

fn readiness_json_observation(probe: &Probe, value: Value) -> Result<Option<ReadinessObservation>> {
    if !probe.ready_pointer.is_empty() {
        let ready = value
            .pointer(&probe.ready_pointer)
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "daemon readiness JSON omitted boolean at {}",
                    probe.ready_pointer
                ))
            })?;
        if !ready {
            return Ok(None);
        }
    }
    let pid = readiness_json_pid(probe, &value)?;
    Ok(Some(ReadinessObservation {
        metadata: value,
        pid,
    }))
}

fn readiness_json_pid(probe: &Probe, value: &Value) -> Result<Option<u32>> {
    if probe.pid_pointer.is_empty() {
        Ok(None)
    } else {
        let raw = value.pointer(&probe.pid_pointer).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "daemon readiness JSON omitted PID at {}",
                probe.pid_pointer
            ))
        })?;
        let pid = raw.as_u64().and_then(|pid| u32::try_from(pid).ok());
        match pid {
            Some(pid) if pid > 0 => Ok(Some(pid)),
            _ => Err(AhrbError::Protocol(format!(
                "daemon readiness JSON PID at {} is not a positive u32",
                probe.pid_pointer
            ))),
        }
    }
}

fn validate_readiness_json_roots(
    probe: &Probe,
    environment: &BTreeMap<String, String>,
    value: &Value,
) -> Result<()> {
    for (pointer, root_name) in &probe.json_pointer_roots {
        let returned = value
            .pointer(pointer)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "daemon readiness JSON omitted string path at {pointer}"
                ))
            })?;
        let root = environment.get(root_name).ok_or_else(|| {
            AhrbError::Validation(format!(
                "daemon readiness root environment {root_name:?} is absent"
            ))
        })?;
        let returned_path = Path::new(returned);
        let root_path = Path::new(root);
        let canonical_returned =
            std::fs::canonicalize(returned_path).unwrap_or_else(|_| returned_path.to_path_buf());
        let canonical_root =
            std::fs::canonicalize(root_path).unwrap_or_else(|_| root_path.to_path_buf());
        if !canonical_returned.starts_with(&canonical_root) {
            return Err(AhrbError::Protocol(format!(
                "daemon readiness JSON path {pointer}={returned:?} is outside isolated {root_name}={root:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) async fn managed_daemon_readiness_satisfied(
    probe: &Probe,
    environment: &BTreeMap<String, String>,
) -> Result<Option<ReadinessObservation>> {
    match probe.kind.as_str() {
        "" | "process" => Ok(Some(ReadinessObservation {
            metadata: Value::Null,
            pid: None,
        })),
        "file" => Ok(Path::new(&probe.target)
            .is_file()
            .then_some(ReadinessObservation {
                metadata: Value::Null,
                pid: None,
            })),
        "socket" => {
            #[cfg(unix)]
            {
                match tokio::net::UnixStream::connect(&probe.target).await {
                    Ok(stream) => {
                        drop(stream);
                        Ok(Some(ReadinessObservation {
                            metadata: Value::Null,
                            pid: None,
                        }))
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound
                                | std::io::ErrorKind::ConnectionRefused
                                | std::io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        Ok(None)
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
                    Ok(Some(ReadinessObservation {
                        metadata: Value::Null,
                        pid: None,
                    }))
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(None)
                }
                Err(error) => Err(error.into()),
            }
        }
        "command" => {
            let mut command = command_from_argv(&probe.command)?;
            command
                .envs(environment)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let attempt_timeout = Duration::from_millis(probe.timeout_ms.clamp(1, 1_000));
            match run_owned_output(&mut command, attempt_timeout, "daemon readiness command").await
            {
                Ok(output) => Ok(output.status.success().then_some(ReadinessObservation {
                    metadata: Value::Null,
                    pid: None,
                })),
                Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(None)
                }
                Err(AhrbError::Timeout(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
        "command-json" => {
            let mut command = command_from_argv(&probe.command)?;
            command
                .envs(environment)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            let attempt_timeout = Duration::from_millis(probe.timeout_ms.clamp(1, 5_000));
            let output = match run_owned_output(
                &mut command,
                attempt_timeout,
                "daemon readiness JSON command",
            )
            .await
            {
                Ok(output) => output,
                Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(AhrbError::Timeout(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            if !output.status.success() {
                return Ok(None);
            }
            let value: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
                AhrbError::Protocol(format!(
                    "daemon readiness command returned invalid JSON: {error}"
                ))
            })?;
            validate_readiness_json_roots(probe, environment, &value)?;
            readiness_json_observation(probe, value)
        }
        other => Err(AhrbError::Validation(format!(
            "unsupported daemon readiness probe {other:?}"
        ))),
    }
}

#[cfg(unix)]
fn signal_managed_process_group(pid: u32, signal: i32) -> Result<()> {
    let process_group = i32::try_from(pid).map_err(|_| {
        AhrbError::Protocol(format!("daemon PID {pid} does not fit process-group API"))
    })?;
    // SAFETY: managed daemons are launched as process-group leaders, and
    // negative PIDs address exactly that owned group.
    if unsafe { libc::kill(-process_group, signal) } != 0 {
        let error = std::io::Error::last_os_error();
        // ESRCH: the owned group is already empty. EPERM: the group id was
        // recycled by another user's process after the owned tree exited;
        // nothing of ours remains to signal, so neither is a stop failure.
        if !matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::EPERM)) {
            return Err(error.into());
        }
    }
    Ok(())
}

async fn stop_managed_child(child: &mut Child, grace: Duration) -> Result<()> {
    let Some(pid) = child.id() else {
        let _ = child.wait().await?;
        return Ok(());
    };
    #[cfg(unix)]
    signal_managed_process_group(pid, libc::SIGTERM)?;
    #[cfg(not(unix))]
    child.start_kill()?;

    match tokio::time::timeout(grace, child.wait()).await {
        Ok(status) => {
            let _ = status?;
        }
        Err(_) => {
            #[cfg(unix)]
            signal_managed_process_group(pid, libc::SIGKILL)?;
            #[cfg(not(unix))]
            child.start_kill()?;
            let _ = child.wait().await?;
        }
    }

    // A daemon leader can exit before workers in its process group. Sweep the
    // still-owned group after reaping the leader so a child cannot outlive the run.
    #[cfg(unix)]
    signal_managed_process_group(pid, libc::SIGKILL)?;
    crate::process::retire_process(pid)?;
    Ok(())
}

async fn read_owned_pipe<R>(pipe: Option<R>) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_end(&mut bytes).await?;
    }
    Ok(bytes)
}

async fn wait_owned_output(
    child: &mut Child,
    timeout: Duration,
    label: &str,
) -> Result<std::process::Output> {
    let child_pid = child.id();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let captured = tokio::time::timeout(timeout, async {
        let (status, stdout, stderr) = tokio::try_join!(
            child.wait(),
            read_owned_pipe(stdout),
            read_owned_pipe(stderr)
        )?;
        Ok::<_, std::io::Error>(std::process::Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match captured {
        Ok(output) => {
            let output = output?;
            if let Some(pid) = child_pid {
                crate::process::retire_process(pid)?;
            }
            Ok(output)
        }
        Err(_) => {
            stop_managed_child(child, Duration::from_millis(100)).await?;
            Err(AhrbError::Timeout(label.to_owned()))
        }
    }
}

async fn run_owned_output(
    command: &mut Command,
    timeout: Duration,
    label: &str,
) -> Result<std::process::Output> {
    let mut child = command.spawn()?;
    crate::process::register_child(&child)?;
    wait_owned_output(&mut child, timeout, label).await
}

#[derive(Debug)]
struct ManagedDaemonProcess {
    child: Option<Child>,
    launch_identity: Option<ProcIdentity>,
    external_identity: Option<ProcIdentity>,
    lifecycle_notes: Vec<String>,
}

impl ManagedDaemonProcess {
    fn pid(&self) -> Option<u32> {
        self.external_identity
            .map(|identity| identity.pid)
            .or_else(|| self.child.as_ref().and_then(Child::id))
    }
}

impl Drop for ManagedDaemonProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(identity) = self.external_identity.or(self.launch_identity) {
            let _ = crate::process::signal_registered_tree(identity, libc::SIGKILL);
        }
        #[cfg(not(unix))]
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn reap_managed_daemon_child(process: &mut ManagedDaemonProcess) -> Result<()> {
    let Some(child) = process.child.as_mut() else {
        return Ok(());
    };
    let pid = child.id();
    if child.try_wait()?.is_some() {
        if let Some(pid) = pid {
            crate::process::retire_process(pid)?;
        }
        process.child = None;
    }
    Ok(())
}

#[cfg(unix)]
fn signal_external_process(identity: ProcIdentity, signal: i32) -> Result<()> {
    crate::process::signal_registered_tree(identity, signal)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownDisposition {
    Clean,
    Escalate,
}

fn shutdown_disposition(contract: &ShutdownResult, value: &Value) -> Result<ShutdownDisposition> {
    if contract.outcome_pointer.is_empty() {
        return Ok(ShutdownDisposition::Clean);
    }
    let outcome = value
        .pointer(&contract.outcome_pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "daemon shutdown JSON omitted string outcome at {}",
                contract.outcome_pointer
            ))
        })?;
    if contract.clean_outcomes.iter().any(|item| item == outcome) {
        Ok(ShutdownDisposition::Clean)
    } else if contract
        .escalate_outcomes
        .iter()
        .any(|item| item == outcome)
    {
        Ok(ShutdownDisposition::Escalate)
    } else {
        Err(AhrbError::Protocol(format!(
            "daemon shutdown returned undeclared outcome {outcome:?}"
        )))
    }
}

async fn run_daemon_shutdown(config: &ManagedDaemonConfig) -> Result<ShutdownDisposition> {
    if config.shutdown_command.is_empty() {
        return Ok(ShutdownDisposition::Escalate);
    }
    let mut command = command_from_argv(&config.shutdown_command)?;
    command
        .envs(&config.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = run_owned_output(&mut command, config.grace, "daemon shutdown command").await?;
    if !output.status.success() {
        return Err(AhrbError::Protocol(format!(
            "daemon shutdown command exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    if config.shutdown_result.outcome_pointer.is_empty() {
        return Ok(ShutdownDisposition::Clean);
    }
    let value: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        AhrbError::Protocol(format!(
            "daemon shutdown command returned invalid JSON: {error}"
        ))
    })?;
    shutdown_disposition(&config.shutdown_result, &value)
}

#[cfg(unix)]
async fn reacquire_daemon_identity_for_cleanup(
    config: &ManagedDaemonConfig,
) -> Result<Option<ProcIdentity>> {
    let probe = &config.readiness;
    if probe.kind != "command-json" || probe.command.is_empty() || probe.pid_pointer.is_empty() {
        return Ok(None);
    }
    let mut command = command_from_argv(&probe.command)?;
    command
        .envs(&config.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let timeout = Duration::from_millis(probe.timeout_ms.clamp(1, 5_000));
    let output = match run_owned_output(
        &mut command,
        timeout,
        "daemon cleanup readiness JSON command",
    )
    .await
    {
        Ok(output) if output.status.success() => output,
        Ok(_) | Err(AhrbError::Timeout(_)) => return Ok(None),
        Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let value: Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        AhrbError::Protocol(format!(
            "daemon cleanup readiness command returned invalid JSON: {error}"
        ))
    })?;
    validate_readiness_json_roots(probe, &config.environment, &value)?;
    let Some(pid) = readiness_json_pid(probe, &value)? else {
        return Ok(None);
    };
    crate::process::register_external_process(pid).map(Some)
}

async fn stop_managed_daemon(
    process: &mut ManagedDaemonProcess,
    config: &ManagedDaemonConfig,
) -> Result<()> {
    let identity = process.external_identity.or(process.launch_identity);
    #[cfg(unix)]
    {
        let mut identity = identity;
        let mut tree_is_live = match identity {
            Some(identity) => crate::process::registered_tree_is_live(identity)?,
            None => false,
        };
        if !tree_is_live && config.shutdown_command.is_empty() {
            if let Some(child) = process.child.as_mut() {
                stop_managed_child(child, Duration::from_millis(100)).await?;
                process.child = None;
            }
            process.external_identity = None;
            process.launch_identity = None;
            return Ok(());
        }
        let disposition = match run_daemon_shutdown(config).await {
            Ok(disposition) => disposition,
            Err(error) => {
                let note = format!(
                    "graceful daemon shutdown failed ({error}); escalating the registered owned tree"
                );
                eprintln!("ahrb: {note}");
                process.lifecycle_notes.push(note);
                ShutdownDisposition::Escalate
            }
        };
        if disposition == ShutdownDisposition::Escalate && !tree_is_live {
            identity = reacquire_daemon_identity_for_cleanup(config).await?;
            if let Some(reacquired) = identity {
                process.external_identity = Some(reacquired);
                tree_is_live = crate::process::registered_tree_is_live(reacquired)?;
                let note = format!(
                    "daemon shutdown required escalation; reacquired readiness PID {} for the owned-tree sweep",
                    reacquired.pid
                );
                eprintln!("ahrb: {note}");
                process.lifecycle_notes.push(note);
            }
        }
        if disposition == ShutdownDisposition::Clean {
            if let Some(identity) = identity {
                let deadline = std::time::Instant::now() + config.grace;
                while crate::process::registered_tree_is_live(identity)?
                    && std::time::Instant::now() < deadline
                {
                    reap_managed_daemon_child(process)?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                tree_is_live = crate::process::registered_tree_is_live(identity)?;
            }
        }
        if !tree_is_live {
            if disposition == ShutdownDisposition::Escalate {
                let note = "daemon shutdown requested escalation, but containment-checked readiness reported no live owned PID".to_owned();
                eprintln!("ahrb: {note}");
                process.lifecycle_notes.push(note);
                return Err(AhrbError::Protocol(
                    "daemon shutdown requested escalation, but no containment-validated owned PID was available for the required SIGTERM/SIGKILL sweep"
                        .to_owned(),
                ));
            }
            if let Some(child) = process.child.as_mut() {
                stop_managed_child(child, Duration::from_millis(100)).await?;
                process.child = None;
            }
            process.external_identity = None;
            process.launch_identity = None;
            return Ok(());
        }
        let identity = identity.ok_or_else(|| {
            AhrbError::Protocol(
                "live managed daemon tree lost its registered PID identity".to_owned(),
            )
        })?;
        if disposition == ShutdownDisposition::Escalate {
            let note = format!(
                "daemon shutdown outcome requested owned-tree SIGTERM/SIGKILL escalation for PID {}",
                identity.pid
            );
            eprintln!("ahrb: {note}");
            process.lifecycle_notes.push(note);
        } else {
            let note = format!(
                "daemon reported a clean shutdown but PID {} remained live; escalating the registered owned tree",
                identity.pid
            );
            eprintln!("ahrb: {note}");
            process.lifecycle_notes.push(note);
        }
        signal_external_process(identity, libc::SIGTERM)?;
        let deadline = std::time::Instant::now() + config.grace;
        while crate::process::registered_tree_is_live(identity)?
            && std::time::Instant::now() < deadline
        {
            reap_managed_daemon_child(process)?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if crate::process::registered_tree_is_live(identity)? {
            signal_external_process(identity, libc::SIGKILL)?;
            let kill_deadline = std::time::Instant::now() + Duration::from_millis(250);
            while crate::process::registered_tree_is_live(identity)?
                && std::time::Instant::now() < kill_deadline
            {
                reap_managed_daemon_child(process)?;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if crate::process::registered_tree_is_live(identity)? {
                return Err(AhrbError::Protocol(format!(
                    "owned daemon tree rooted at PID {} survived SIGKILL",
                    identity.pid
                )));
            }
        }
        if let Some(child) = process.child.as_mut() {
            let pid = child.id();
            let _ = child.wait().await?;
            if let Some(pid) = pid {
                crate::process::retire_process(pid)?;
            }
            process.child = None;
        }
        process.external_identity = None;
        process.launch_identity = None;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = identity;
        if let Some(child) = process.child.as_mut() {
            stop_managed_child(child, config.grace).await?;
            process.child = None;
        }
        process.external_identity = None;
        process.launch_identity = None;
        Ok(())
    }
}

async fn start_managed_daemon_inner(
    config: &ManagedDaemonConfig,
    launch_pid: Option<tokio::sync::oneshot::Sender<u32>>,
) -> Result<ManagedDaemonProcess> {
    std::fs::create_dir_all(&config.log_directory)?;
    let sequence = DAEMON_LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let stdout = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(
            config
                .log_directory
                .join(format!("daemon-{sequence:04}.stdout")),
        )?;
    let stderr = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(
            config
                .log_directory
                .join(format!("daemon-{sequence:04}.stderr")),
        )?;
    let mut command = command_from_argv(&config.command)?;
    command
        .envs(&config.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let child = command.spawn()?;
    let child_pid = child
        .id()
        .ok_or_else(|| AhrbError::Protocol("managed daemon launcher has no PID".to_owned()))?;
    let launch_identity = crate::process::register_external_process(child_pid)?;
    if let Some(sender) = launch_pid {
        let _ = sender.send(child_pid);
    }
    let mut process = ManagedDaemonProcess {
        child: Some(child),
        launch_identity: Some(launch_identity),
        external_identity: None,
        lifecycle_notes: Vec::new(),
    };

    let readiness_timeout = Duration::from_millis(config.readiness.timeout_ms.max(1));
    let started = std::time::Instant::now();
    let readiness_deadline = started + readiness_timeout;
    if config.launcher_exits {
        let launcher = process.child.as_mut().ok_or_else(|| {
            AhrbError::Protocol("detached daemon launcher disappeared".to_owned())
        })?;
        let status = match tokio::time::timeout(readiness_timeout, launcher.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                stop_managed_daemon(&mut process, config).await?;
                return Err(error.into());
            }
            Err(_) => {
                stop_managed_daemon(&mut process, config).await?;
                return Err(AhrbError::Timeout("detached daemon launcher".to_owned()));
            }
        };
        process.child = None;
        crate::process::retire_process(child_pid)?;
        if !status.success() {
            stop_managed_daemon(&mut process, config).await?;
            return Err(AhrbError::Protocol(format!(
                "detached daemon launcher exited with {status}"
            )));
        }
    }
    let readiness = loop {
        if let Some(child) = process.child.as_mut() {
            let daemon_pid = child.id();
            let status = match child.try_wait() {
                Ok(status) => status,
                Err(error) => {
                    stop_managed_daemon(&mut process, config).await?;
                    return Err(error.into());
                }
            };
            if let Some(status) = status {
                #[cfg(unix)]
                if let Some(pid) = daemon_pid {
                    signal_managed_process_group(pid, libc::SIGKILL)?;
                }
                return Err(AhrbError::Protocol(format!(
                    "managed daemon exited before readiness with {status}"
                )));
            }
        }
        match managed_daemon_readiness_satisfied(&config.readiness, &config.environment).await {
            Ok(Some(observation)) => break observation,
            Ok(None) => {}
            Err(error) => {
                stop_managed_daemon(&mut process, config).await?;
                return Err(error);
            }
        }
        if std::time::Instant::now() >= readiness_deadline {
            stop_managed_daemon(&mut process, config).await?;
            return Err(AhrbError::Timeout(format!(
                "daemon readiness {:?} at {:?}",
                config.readiness.kind, config.readiness.target
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    if config.launcher_exits {
        let Some(pid) = readiness.pid else {
            stop_managed_daemon(&mut process, config).await?;
            return Err(AhrbError::Protocol(
                "detached daemon readiness completed without a declared PID".to_owned(),
            ));
        };
        match crate::process::register_external_process(pid) {
            Ok(identity) => process.external_identity = Some(identity),
            Err(error) => {
                stop_managed_daemon(&mut process, config).await?;
                return Err(error);
            }
        }
    }

    if readiness.metadata != Value::Null {
        let status_path = config
            .log_directory
            .join(format!("daemon-{sequence:04}.readiness.json"));
        let metadata = match serde_json::to_vec_pretty(&readiness.metadata) {
            Ok(metadata) => metadata,
            Err(error) => {
                stop_managed_daemon(&mut process, config).await?;
                return Err(error.into());
            }
        };
        if let Err(error) = std::fs::write(&status_path, metadata) {
            stop_managed_daemon(&mut process, config).await?;
            return Err(AhrbError::Protocol(format!(
                "write parsed daemon readiness metadata {}: {error}",
                status_path.display()
            )));
        }
    }

    if !config.initialize_command.is_empty() && !config.initialize_marker.is_file() {
        let initialize_result = async {
            if let Some(parent) = config.initialize_marker.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut initialize = command_from_argv(&config.initialize_command)?;
            initialize
                .envs(&config.environment)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let output = run_owned_output(
                &mut initialize,
                readiness_timeout,
                "managed daemon initialization command",
            )
            .await?;
            if !output.status.success() {
                return Err(AhrbError::Protocol(format!(
                    "managed daemon initialization exited with {}",
                    output.status
                )));
            }
            std::fs::write(&config.initialize_marker, b"initialized\n")?;
            Ok(())
        }
        .await;
        if let Err(error) = initialize_result {
            stop_managed_daemon(&mut process, config).await?;
            return Err(error);
        }
    }
    Ok(process)
}

async fn start_managed_daemon(config: &ManagedDaemonConfig) -> Result<ManagedDaemonProcess> {
    start_managed_daemon_inner(config, None).await
}

/// A lifecycle wrapper for daemons reached through a separate client transport.
pub struct ManagedDaemonTransport<T: Transport> {
    inner: T,
    config: ManagedDaemonConfig,
    daemon: Option<ManagedDaemonProcess>,
    lifecycle_notes: Vec<String>,
}

impl<T: Transport> ManagedDaemonTransport<T> {
    /// Wrap a socket or HTTP client with a cold, owned daemon process.
    pub fn new(inner: T, config: ManagedDaemonConfig) -> Self {
        Self {
            inner,
            config,
            daemon: None,
            lifecycle_notes: Vec::new(),
        }
    }
}

impl<T: Transport> Transport for ManagedDaemonTransport<T> {
    fn start(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            if self.daemon.is_some() {
                return Ok(());
            }
            self.daemon = Some(start_managed_daemon(&self.config).await?);
            if let Err(error) = self.inner.start().await {
                if let Some(mut daemon) = self.daemon.take() {
                    let stopped = stop_managed_daemon(&mut daemon, &self.config).await;
                    self.lifecycle_notes.append(&mut daemon.lifecycle_notes);
                    stopped?;
                }
                return Err(error);
            }
            Ok(())
        })
    }

    fn request(&mut self, request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
        self.inner.request(request)
    }

    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        self.inner.await_readiness()
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            let inner_result = self.inner.stop().await;
            let daemon_result = if let Some(mut daemon) = self.daemon.take() {
                let result = stop_managed_daemon(&mut daemon, &self.config).await;
                self.lifecycle_notes.append(&mut daemon.lifecycle_notes);
                result
            } else {
                Ok(())
            };
            inner_result.and(daemon_result)
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        let mut pids = self.inner.owned_pids();
        if let Some(pid) = self.daemon.as_ref().and_then(ManagedDaemonProcess::pid) {
            pids.push(pid);
        }
        pids.sort_unstable();
        pids.dedup();
        pids
    }

    fn daemon_pid(&self) -> Option<u32> {
        self.daemon.as_ref().and_then(ManagedDaemonProcess::pid)
    }

    fn lifecycle_notes(&self) -> Vec<String> {
        let mut notes = self.inner.lifecycle_notes();
        notes.extend(self.lifecycle_notes.clone());
        notes
    }
}

impl<T: Transport> Drop for ManagedDaemonTransport<T> {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.daemon.as_ref().and_then(ManagedDaemonProcess::pid) {
            // Drop cannot wait asynchronously, but it must prevent an error path
            // from leaving daemon workers resident. Tokio's kill-on-drop handles
            // the leader; this signal covers every descendant in the owned group.
            if self.config.launcher_exits {
                if let Some(identity) = self
                    .daemon
                    .as_ref()
                    .and_then(|daemon| daemon.external_identity)
                {
                    let _ = signal_external_process(identity, libc::SIGKILL);
                }
            } else {
                let _ = signal_managed_process_group(pid, libc::SIGKILL);
            }
        }
        #[cfg(not(unix))]
        if let Some(child) = self
            .daemon
            .as_mut()
            .and_then(|daemon| daemon.child.as_mut())
        {
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

    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        (**self).await_readiness()
    }

    fn stop(&mut self) -> DriverFuture<'_, ()> {
        (**self).stop()
    }

    fn owned_pids(&self) -> Vec<u32> {
        (**self).owned_pids()
    }

    fn daemon_pid(&self) -> Option<u32> {
        (**self).daemon_pid()
    }

    fn lifecycle_notes(&self) -> Vec<String> {
        (**self).lifecycle_notes()
    }
}

/// The semantic harness lifecycle used by workflows.
pub trait Driver: Send {
    /// Start and await readiness.
    fn start(&mut self) -> DriverFuture<'_, ()>;
    /// Await readiness after launch. Resource workflows call this while their
    /// owned-tree samplers are already armed.
    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
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
    /// Probe and reconcile a session after daemon restart.
    fn recover_probe(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        self.resume(session)
    }
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
    /// Reap launcher handles after AHRB has externally killed the owned tree.
    ///
    /// This must not send a graceful control request or mutate harness state.
    fn reap_after_external_kill(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Observe a cohort with an adapter-declared readiness command. This is
    /// advisory only: resource PASS fencing uses durable terminal state and
    /// thin-client exit status.
    fn wait_ready(
        &mut self,
        _sessions: &[SessionId],
        _timeout: Duration,
    ) -> DriverFuture<'_, Option<Value>> {
        Box::pin(async { Ok(None) })
    }
    /// Release benchmark launch gates after resource membership is armed.
    fn release_invocations(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    /// Currently live launcher/controller PIDs owned by this driver.
    fn owned_pids(&self) -> Vec<u32> {
        Vec::new()
    }
    /// Root PID captured by the declared daemon readiness probe.
    fn daemon_pid(&self) -> Option<u32> {
        None
    }
    /// Currently live launcher PIDs for one logical session, when separable.
    fn session_pids(&self, _session: &SessionId) -> Vec<u32> {
        Vec::new()
    }
    /// Completed external process-lifetime clocks for one-process-per-turn drivers.
    fn completed_turn_wall_ns(&self) -> Vec<u64> {
        Vec::new()
    }
    /// Completed one-shot launch/exit boundaries on the shared monotonic clock.
    fn completed_turn_boundaries(&self) -> Vec<CompletedTurnBoundary> {
        Vec::new()
    }
    /// Last observed thin-client process state for a logical session.
    fn client_exit(&self, _session: &SessionId) -> ClientExit {
        ClientExit::NotApplicable
    }
    /// Parsed JSON returned by headless control commands for this session.
    fn control_evidence(&self, _session: &SessionId) -> Vec<Value> {
        Vec::new()
    }
    /// Notes from typed shutdown and owned-tree escalation.
    fn lifecycle_notes(&self) -> Vec<String> {
        Vec::new()
    }
    /// Unique source payload kinds observed at a supported schema version but
    /// left unmapped by the adapter's normalization table.
    fn unmapped_payload_kinds(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Exact external lifecycle boundaries for one completed per-invocation turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedTurnBoundary {
    /// Boundary immediately before spawning the one-shot child.
    pub launch_ns: u64,
    /// Boundary immediately after observing child exit.
    pub exit_ns: u64,
}

/// Explicit process-exit evidence used by daemon-topology resource fencing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientExit {
    /// The transport has no per-session thin client.
    NotApplicable,
    /// A thin client is still running or has not yet been reaped.
    Running,
    /// The thin client exited; signals have no numeric exit code.
    Exited(Option<i32>),
}

/// Generic data-driven driver backed by one transport.
pub struct GenericDriver<T: Transport> {
    /// Selected transport.
    pub transport: T,
    /// Manifest-selected operation names.
    pub operations: DriverOperations,
    lifecycle_notes: Vec<String>,
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
    /// Informational cohort readiness operation.
    pub wait_ready: String,
    /// Typed shutdown response contract.
    pub shutdown_result: ShutdownResult,
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
            wait_ready: String::new(),
            shutdown_result: ShutdownResult::default(),
        }
    }
}

impl<T: Transport> GenericDriver<T> {
    /// Construct a semantic driver around a framing transport.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            operations: DriverOperations::default(),
            lifecycle_notes: Vec::new(),
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

    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        self.transport.await_readiness()
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
        let contract = self.operations.shutdown_result.clone();
        Box::pin(async move {
            let mut escalate = false;
            if !operation.is_empty() {
                let value = self.call(&operation, json!({})).await?;
                if !contract.outcome_pointer.is_empty()
                    && shutdown_disposition(&contract, &value)? == ShutdownDisposition::Escalate
                {
                    escalate = true;
                    self.lifecycle_notes
                        .push("daemon shutdown outcome requested owned-tree escalation".to_owned());
                }
            }
            let stopped = self.transport.stop().await;
            self.lifecycle_notes
                .extend(self.transport.lifecycle_notes());
            if escalate && stopped.is_err() {
                return Err(AhrbError::Protocol(
                    "daemon requested escalation and the owned-tree sweep failed".to_owned(),
                ));
            }
            stopped
        })
    }

    fn wait_ready(
        &mut self,
        sessions: &[SessionId],
        timeout: Duration,
    ) -> DriverFuture<'_, Option<Value>> {
        let operation = self.operations.wait_ready.clone();
        let session_ids = sessions
            .iter()
            .map(|session| session.0.clone())
            .collect::<Vec<_>>();
        Box::pin(async move {
            if operation.is_empty() {
                return Ok(None);
            }
            let value = self
                .call(
                    &operation,
                    json!({
                        "count": session_ids.len(),
                        "session_ids": session_ids,
                        "timeout_ms": u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                    }),
                )
                .await?;
            Ok(Some(value))
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        self.transport.owned_pids()
    }

    fn daemon_pid(&self) -> Option<u32> {
        self.transport.daemon_pid()
    }

    fn lifecycle_notes(&self) -> Vec<String> {
        self.lifecycle_notes.clone()
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
    /// Optional resident daemon owned for the lifetime of this thin-client
    /// driver. Each turn is still submitted through the declared CLI argv.
    pub daemon: Option<ManagedDaemonConfig>,
    /// First-turn argv. `{{prompt}}`, `{{session_id}}`, `{{marker}}`,
    /// `{{turn_key}}`, `{{workspace}}`, and `{{profile}}` are rendered directly.
    pub command: Vec<String>,
    /// Subsequent-turn argv used to reopen a harness-owned session from disk.
    pub resume_command: Vec<String>,
    /// Headless resume/reconciliation control argv.
    pub resume_control_command: Vec<String>,
    /// Headless post-restart recovery probe argv.
    pub recover_probe_command: Vec<String>,
    /// Out-of-process command used to release a durable checkpoint token.
    pub release_command: Vec<String>,
    /// Out-of-process command used after terminating an active invocation.
    pub cancel_command: Vec<String>,
    /// Out-of-process command that strictly reopens and emits the durable journal.
    pub replay_command: Vec<String>,
    /// Informational command used to observe a resource cohort settling.
    pub wait_ready_command: Vec<String>,
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
    /// Pointer used to learn the harness's persistent run identifier.
    pub run_id_pointer: String,
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
    #[serde(default)]
    run_id: String,
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
    launch_ns: u64,
    wall_prefix_ns: u64,
    turn: u64,
}

#[derive(Debug)]
struct ExecSession {
    persisted: PersistedExecSession,
    active: Option<ActiveInvocation>,
    client_exit: ClientExit,
    control_evidence: Vec<Value>,
}

#[derive(Clone, Debug)]
struct AbstractFixtureCall {
    call_id: String,
    name: String,
    arguments: Value,
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_fixture_metadata(text: &str) -> Option<Value> {
    let token = text
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_hexdigit())
        .copied()
        .collect::<Vec<_>>();
    if token.is_empty() || token.len() % 2 != 0 {
        return None;
    }
    let mut decoded = Vec::with_capacity(token.len() / 2);
    for pair in token.chunks_exact(2) {
        decoded.push(hex_nibble(pair[0])?.checked_mul(16)? + hex_nibble(pair[1])?);
    }
    serde_json::from_slice(&decoded).ok()
}

fn embedded_fixture_call(value: &Value) -> Option<AbstractFixtureCall> {
    match value {
        Value::String(text) => {
            if matches!(text.as_bytes().first(), Some(b'{') | Some(b'[')) {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    if let Some(call) = embedded_fixture_call(&parsed) {
                        return Some(call);
                    }
                }
            }
            let mut offsets = text
                .match_indices(NATIVE_FIXTURE_METADATA_PREFIX)
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>();
            while let Some(offset) = offsets.pop() {
                let encoded = &text[offset + NATIVE_FIXTURE_METADATA_PREFIX.len()..];
                let Some(metadata) = decode_fixture_metadata(encoded) else {
                    continue;
                };
                let Some(call_id) = metadata.get("call_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(name) = metadata.get("name").and_then(Value::as_str) else {
                    continue;
                };
                let Some(arguments) = metadata.get("arguments").cloned() else {
                    continue;
                };
                if !arguments.is_object() {
                    continue;
                }
                return Some(AbstractFixtureCall {
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                    arguments,
                });
            }
            None
        }
        Value::Array(values) => values.iter().find_map(embedded_fixture_call),
        Value::Object(object) => object.values().find_map(embedded_fixture_call),
        Value::Null | Value::Bool(_) | Value::Number(_) => None,
    }
}

fn nested_value<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    match value {
        Value::Object(object) => object
            .get(field)
            .or_else(|| object.values().find_map(|value| nested_value(value, field))),
        Value::Array(values) => values.iter().find_map(|value| nested_value(value, field)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

fn canonical_native_result(native: Value) -> Value {
    let mut result = match native {
        Value::Object(object) => object,
        value => BTreeMap::from([("native".to_owned(), value)])
            .into_iter()
            .collect(),
    };
    // Some harnesses (verified: haider 0.0.967) carry the structured execution
    // record as a JSON *string* under `preview`; surface it as `preview_record`
    // so exit_code/status/output inside it are visible to `ok` derivation AND to
    // downstream oracles (e.g. reading tool stdout back for a dependency check).
    if !result.contains_key("preview_record")
        && let Some(parsed) = result
            .get("preview")
            .and_then(Value::as_str)
            .and_then(|preview| serde_json::from_str::<Value>(preview.trim()).ok())
            .filter(Value::is_object)
    {
        result.insert("preview_record".to_owned(), parsed);
    }
    if !result.contains_key("ok") {
        let value = Value::Object(result.clone());
        let is_error = nested_value(&value, "is_error")
            .or_else(|| nested_value(&value, "isError"))
            .and_then(Value::as_bool);
        let exit_code = nested_value(&value, "exit_code").and_then(Value::as_i64);
        let status = nested_value(&value, "status").and_then(Value::as_str);
        let has_output = nested_value(&value, "output").is_some();
        let ok = is_error.map(|is_error| !is_error).or_else(|| {
            exit_code.map(|code| code == 0).or_else(|| {
                status
                    .and_then(|status| match status {
                        "completed" | "success" | "succeeded" => Some(true),
                        "failed" | "failure" | "error" | "cancelled" => Some(false),
                        _ => None,
                    })
                    .or(has_output.then_some(true))
            })
        });
        if let Some(ok) = ok {
            result.insert("ok".to_owned(), Value::Bool(ok));
        }
    }
    Value::Object(result)
}

fn canonicalize_native_fixture_event(
    event: &mut NormalizedEvent,
    calls_by_native_id: &BTreeMap<String, AbstractFixtureCall>,
) -> Option<(String, AbstractFixtureCall)> {
    if !matches!(event.event, EventVocab::ToolCall | EventVocab::ToolResult) {
        return None;
    }
    let native_call_id = event
        .payload
        .get("call_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let call = embedded_fixture_call(&event.payload).or_else(|| {
        native_call_id
            .as_ref()
            .and_then(|call_id| calls_by_native_id.get(call_id))
            .cloned()
    })?;
    let payload = event.payload.as_object_mut()?;
    if let Some(native_call_id) = &native_call_id {
        payload.insert(
            "native_call_id".to_owned(),
            Value::String(native_call_id.clone()),
        );
    }
    if let Some(native_name) = payload.get("name").cloned() {
        payload.insert("native_name".to_owned(), native_name);
    }
    payload.insert("call_id".to_owned(), Value::String(call.call_id.clone()));
    payload.insert("name".to_owned(), Value::String(call.name.clone()));
    match event.event {
        EventVocab::ToolCall => {
            if let Some(native_arguments) = payload.get("arguments").cloned() {
                payload.insert("native_arguments".to_owned(), native_arguments);
            }
            payload.insert("arguments".to_owned(), call.arguments.clone());
        }
        EventVocab::ToolResult => {
            payload.insert("arguments".to_owned(), call.arguments.clone());
            let native_result = payload
                .get("result")
                .cloned()
                .unwrap_or_else(|| Value::Object(payload.clone()));
            payload.insert("native_result".to_owned(), native_result.clone());
            payload.insert("result".to_owned(), canonical_native_result(native_result));
        }
        _ => {}
    }
    native_call_id.map(|native_call_id| (native_call_id, call))
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
    daemon_process: Option<ManagedDaemonProcess>,
    daemon_start: Option<tokio::task::JoinHandle<Result<ManagedDaemonProcess>>>,
    daemon_launch_pid: Option<u32>,
    completed_turn_wall_ns: Vec<u64>,
    completed_turn_boundaries: Vec<CompletedTurnBoundary>,
    lifecycle_notes: Vec<String>,
    unmapped_payload_kinds: BTreeSet<String>,
}

impl PerInvocationDriver {
    /// Construct a per-invocation CLI driver. Disk state is loaded by `start`.
    pub fn new(config: PerInvocationConfig) -> Self {
        let state_root = config.profile_root.join("ahrb-exec-sessions");
        Self {
            config,
            state_root,
            sessions: BTreeMap::new(),
            daemon_process: None,
            daemon_start: None,
            daemon_launch_pid: None,
            completed_turn_wall_ns: Vec::new(),
            completed_turn_boundaries: Vec::new(),
            lifecycle_notes: Vec::new(),
            unmapped_payload_kinds: BTreeSet::new(),
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
                    client_exit: ClientExit::Exited(Some(0)),
                    control_evidence: Vec::new(),
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
            ("run_id".to_owned(), session.run_id.clone()),
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

    fn raw_live_replay_records(&self, session: &PersistedExecSession) -> Result<Vec<Value>> {
        let mut records = Vec::new();
        for turn in 1..=session.invocations {
            let path = self
                .session_directory(&session.local_id)
                .join(format!("turn-{turn:06}.stdout"));
            let bytes = Self::bounded_read(&path, self.config.max_output_bytes, false)?;
            for record in parse_event_records(&bytes, &self.config.events.framing)? {
                let has_cursor = record
                    .pointer(&self.config.events.cursor_pointer)
                    .and_then(Value::as_u64)
                    .is_some();
                let same_run = self.config.run_id_pointer.is_empty()
                    || record
                        .pointer(&self.config.run_id_pointer)
                        .and_then(Value::as_str)
                        == Some(session.run_id.as_str());
                if has_cursor && same_run {
                    records.push(record);
                }
            }
        }
        Ok(records)
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
        let mut fixture_calls = existing
            .values()
            .filter(|event| event.event == EventVocab::ToolCall)
            .filter_map(|event| {
                let native_call_id = event
                    .payload
                    .get("native_call_id")
                    .or_else(|| event.payload.get("call_id"))
                    .and_then(Value::as_str)?;
                let call_id = event.payload.get("call_id").and_then(Value::as_str)?;
                let name = event.payload.get("name").and_then(Value::as_str)?;
                let arguments = event.payload.get("arguments")?.clone();
                Some((
                    native_call_id.to_owned(),
                    AbstractFixtureCall {
                        call_id: call_id.to_owned(),
                        name: name.to_owned(),
                        arguments,
                    },
                ))
            })
            .collect::<BTreeMap<_, _>>();
        // Some harnesses (verified: haider 0.0.967) issue the tool call as a
        // `started` item with EMPTY args and only populate the full arguments —
        // including the AHRB fixture marker — on the later `completed` item, which
        // the journal commits AFTER the tool_result. AHRB anchors the tool-call
        // event on the `started` item so it precedes the result in cursor order;
        // pre-scan the `completed` items so that started call's abstract identity
        // and arguments are backfilled by native call_id during canonicalization.
        for raw in records {
            let is_completed_tool_call = raw.pointer("/payload/event").and_then(Value::as_str)
                == Some("completed")
                && raw.pointer("/payload/item/item").and_then(Value::as_str) == Some("tool_call");
            if !is_completed_tool_call {
                continue;
            }
            let Some(item) = raw.pointer("/payload/item") else {
                continue;
            };
            let Some(native_call_id) = item.get("call_id").and_then(Value::as_str) else {
                continue;
            };
            if fixture_calls.contains_key(native_call_id) {
                continue;
            }
            if let Some(call) = embedded_fixture_call(item) {
                fixture_calls.insert(native_call_id.to_owned(), call);
            }
        }
        let mut effective = mapping.clone();
        effective.id_pointer = "/_ahrb_id".to_owned();
        effective.cursor_pointer = "/_ahrb_cursor".to_owned();
        for (index, raw) in records.iter().enumerate() {
            let event_type = (!mapping.type_pointer.is_empty())
                .then(|| raw.pointer(&mapping.type_pointer))
                .flatten()
                .and_then(Value::as_str)
                .or_else(|| raw.get("type").and_then(Value::as_str))
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
                        "terminal-success"
                            | "terminal-failure"
                            | "terminal-cancelled"
                            | "terminal-timeout"
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
                    if let Some(mut event) = normalizer.normalize(&prepared, &effective)? {
                        if let Some((native_call_id, call)) =
                            canonicalize_native_fixture_event(&mut event, &fixture_calls)
                        {
                            fixture_calls.insert(native_call_id, call);
                        }
                        session.next_cursor = session.next_cursor.saturating_add(1);
                        output.push(event);
                    }
                }
            }
        }
        Ok(output)
    }

    fn learn_harness_identifiers(&self, session: &mut PersistedExecSession, records: &[Value]) {
        if !self.config.session_id_pointer.is_empty() && session.harness_id.is_empty() {
            if let Some(id) = records.iter().find_map(|record| {
                record
                    .pointer(&self.config.session_id_pointer)
                    .and_then(Value::as_str)
            }) {
                session.harness_id = id.to_owned();
            }
        }
        if !self.config.run_id_pointer.is_empty() && session.run_id.is_empty() {
            if let Some(id) = records.iter().find_map(|record| {
                record
                    .pointer(&self.config.run_id_pointer)
                    .and_then(Value::as_str)
            }) {
                session.run_id = id.to_owned();
            }
        }
    }

    fn terminal_contract(
        &self,
        status: std::process::ExitStatus,
        stdout: &[u8],
    ) -> (EventVocab, Value) {
        classify_exit_contract(&self.config.exit, status.code(), stdout)
    }

    fn refresh_source(
        &mut self,
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
        self.record_event_contract(&records)?;
        self.learn_harness_identifiers(session, &records);
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
            let (expected, mut payload) = self.terminal_contract(status, &stdout);
            if payload.get("category").and_then(Value::as_str) == Some("idle-timeout")
                && let Some(object) = payload.as_object_mut()
            {
                object.insert(
                    "client_turn_wall_ms".to_owned(),
                    Value::from(
                        u64::try_from(active.started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    ),
                );
            }
            let turn_prefix = format!("{}:turn-{}", session.local_id, active.turn);
            if let Some(mapped) = additions.iter_mut().rev().find(|event| {
                matches!(
                    event.event,
                    EventVocab::TerminalSuccess
                        | EventVocab::TerminalFailure
                        | EventVocab::TerminalTimeout
                )
            }) && mapped.event == expected
                && let (Some(mapped_payload), Some(exit_payload)) =
                    (mapped.payload.as_object_mut(), payload.as_object())
            {
                for (key, value) in exit_payload {
                    mapped_payload
                        .entry(key.clone())
                        .or_insert_with(|| value.clone());
                }
            }
            let mapped_terminal = additions.iter().chain(cached.iter()).rev().find(|event| {
                matches!(
                    event.event,
                    EventVocab::TerminalSuccess
                        | EventVocab::TerminalFailure
                        | EventVocab::TerminalCancelled
                        | EventVocab::TerminalTimeout
                )
            });
            if mapped_terminal.is_none_or(|event| {
                event.event != expected
                    && !matches!(
                        event.event,
                        EventVocab::TerminalCancelled | EventVocab::TerminalTimeout
                    )
            }) {
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

    fn refresh_inactive_journal(&mut self, session: &mut PersistedExecSession) -> Result<()> {
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
        self.record_event_contract(&records)?;
        self.learn_harness_identifiers(session, &records);
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

    fn record_event_contract(&mut self, records: &[Value]) -> Result<()> {
        for kind in inspect_event_contract(&self.config.events, records)? {
            if self.unmapped_payload_kinds.insert(kind.clone()) {
                eprintln!(
                    "warning: supported source schema contained unmapped payload kind {kind:?}"
                );
            }
        }
        Ok(())
    }

    async fn run_control_command(
        &self,
        template: &[String],
        session: &PersistedExecSession,
        release_token: Option<&str>,
    ) -> Result<Option<Value>> {
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
        if template
            .iter()
            .any(|argument| argument.contains("{{run_id}}"))
            && session.run_id.is_empty()
        {
            return Err(AhrbError::Protocol(format!(
                "control command requires a harness run ID, but session {:?} never exposed {}",
                session.local_id, self.config.run_id_pointer
            )));
        }
        let argv = template
            .iter()
            .map(|argument| crate::manifest::render_template(argument, &variables))
            .collect::<Result<Vec<_>>>()?;
        let (program, arguments) = argv.split_first().ok_or_else(|| {
            AhrbError::Validation("per-invocation control command is empty".to_owned())
        })?;
        let mut command = Command::new(program);
        #[cfg(unix)]
        command.process_group(0);
        command
            .args(arguments)
            .envs(&self.config.environment)
            .current_dir(self.session_directory(&session.local_id).join("workspace"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = run_owned_output(
            &mut command,
            self.config.timeout,
            "per-invocation control command",
        )
        .await?;
        // Control commands may print a human-readable preamble line before the
        // JSON document on the same stream (e.g. haider's recover --probe emits
        // "no crash window to reconcile" ahead of its haider.session_recovery.v1
        // document), so extract the LAST parseable JSON line rather than parsing
        // the whole buffer.
        let stdout_text = String::from_utf8_lossy(&output.stdout);
        let last_json = stdout_text
            .lines()
            .rev()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .find_map(|line| serde_json::from_str::<Value>(line).ok());
        if !output.status.success() {
            // A structured JSON control response with a nonzero exit is still
            // evidence (e.g. a typed "no_recovery" probe answer); let the row
            // oracle judge it instead of masking it behind a protocol error.
            if let Some(value) = last_json {
                return Ok(Some(value));
            }
            return Err(AhrbError::Protocol(format!(
                "per-invocation control command exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        if output.stdout.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        let value = last_json.ok_or_else(|| {
            AhrbError::Protocol(
                "per-invocation control command returned no parseable JSON line".to_owned(),
            )
        })?;
        Ok(Some(value))
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
        Box::pin(async move {
            self.load_sessions()?;
            if self.daemon_process.is_none()
                && self.daemon_start.is_none()
                && let Some(config) = self.config.daemon.clone()
            {
                let (sender, receiver) = tokio::sync::oneshot::channel();
                self.daemon_start = Some(tokio::spawn(async move {
                    start_managed_daemon_inner(&config, Some(sender)).await
                }));
                match receiver.await {
                    Ok(pid) => self.daemon_launch_pid = Some(pid),
                    Err(_) => {
                        let task = self.daemon_start.take().ok_or_else(|| {
                            AhrbError::Protocol(
                                "managed daemon startup task disappeared".to_owned(),
                            )
                        })?;
                        let result = task.await.map_err(|error| {
                            AhrbError::Protocol(format!(
                                "managed daemon startup task failed: {error}"
                            ))
                        })?;
                        return match result {
                            Ok(process) => {
                                self.daemon_process = Some(process);
                                Ok(())
                            }
                            Err(error) => Err(error),
                        };
                    }
                }
            }
            Ok(())
        })
    }

    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            let Some(task) = self.daemon_start.take() else {
                return Ok(());
            };
            let result = task.await.map_err(|error| {
                AhrbError::Protocol(format!("managed daemon startup task failed: {error}"))
            })?;
            self.daemon_launch_pid = None;
            self.daemon_process = Some(result?);
            Ok(())
        })
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
                run_id: String::new(),
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
                    client_exit: ClientExit::Exited(Some(0)),
                    control_evidence: Vec::new(),
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
            // Reuse the invocation deadline clock as the external wall clock.
            // Gated resource trials retain only actual spawn plus post-release
            // execution time, excluding the observer-arming wait.
            let started = std::time::Instant::now();
            let launch_ns = crate::fake_model::monotonic_timestamp_ns();
            let child = command.spawn()?;
            crate::process::register_child(&child)?;
            let wall_prefix_ns = if gate_path.is_some() {
                duration_ns(started.elapsed())
            } else {
                0
            };
            let item = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?;
            item.active = Some(ActiveInvocation {
                child,
                stdout_path,
                gate_path,
                started,
                launch_ns,
                wall_prefix_ns,
                turn,
            });
            item.client_exit = ClientExit::Running;
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
                let invocation_pid = invocation.child.id();
                let mut status = invocation.child.try_wait()?;
                if status.is_none() && invocation.started.elapsed() >= self.config.timeout {
                    if let Some(pid) = invocation.child.id() {
                        #[cfg(unix)]
                        signal_managed_process_group(pid, libc::SIGKILL)?;
                        #[cfg(not(unix))]
                        invocation.child.start_kill()?;
                    }
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
                    let exit_ns = crate::fake_model::monotonic_timestamp_ns();
                    if let Some(pid) = invocation_pid {
                        crate::process::retire_process(pid)?;
                    }
                    let wall_ns = invocation
                        .wall_prefix_ns
                        .saturating_add(duration_ns(invocation.started.elapsed()));
                    self.completed_turn_wall_ns.push(wall_ns);
                    self.completed_turn_boundaries.push(CompletedTurnBoundary {
                        launch_ns: invocation.launch_ns,
                        exit_ns,
                    });
                    persisted.turns = persisted.turns.saturating_add(1);
                    Self::persist_session_at(&self.metadata_path(&id), &persisted)?;
                    let item = self.sessions.get_mut(&id).ok_or_else(|| {
                        AhrbError::Protocol(format!("exec session {id:?} disappeared"))
                    })?;
                    item.persisted = persisted;
                    item.active = None;
                    item.client_exit = ClientExit::Exited(status.and_then(|value| value.code()));
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
            if self
                .config
                .replay_command
                .iter()
                .any(|argument| argument.contains("{{run_id}}"))
                && persisted.run_id.is_empty()
            {
                return Err(AhrbError::Protocol(format!(
                    "durable replay requires a harness run ID, but session {id:?} never exposed {}",
                    self.config.run_id_pointer
                )));
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
            let working_directory = self.session_directory(&id).join("workspace");
            let state_argv = self
                .config
                .events
                .replay_state_command
                .iter()
                .map(|argument| crate::manifest::render_template(argument, &variables))
                .collect::<Result<Vec<_>>>()?;
            let before_state = if state_argv.is_empty() {
                None
            } else {
                Some(
                    run_json_output_command(
                        &state_argv,
                        &self.config.environment,
                        &working_directory,
                        self.config.timeout,
                        self.config.max_output_bytes,
                        "pre-replay durable state",
                    )
                    .await?,
                )
            };
            let mut command = Command::new(program);
            #[cfg(unix)]
            command.process_group(0);
            command
                .args(arguments)
                .envs(&self.config.environment)
                .current_dir(&working_directory)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let output = run_owned_output(
                &mut command,
                self.config.timeout,
                "per-invocation durable replay",
            )
            .await?;
            validate_replay_output(&output, self.config.max_output_bytes)?;
            let mut records = extract_replay_records(&self.config.events, &output.stdout)?;
            self.record_event_contract(&records)?;
            if self.config.events.replay_compare_live_records {
                let live = self.raw_live_replay_records(&persisted)?;
                // The durable journal stores the bare envelope payload, while the
                // live stream may enrich it with documented client augmentation
                // (verified on haider 0.0.967: the terminal run_state gains
                // terminal_kind/error_code on the live carrier only). Envelope
                // fields must match exactly; the replayed payload must be an
                // exact subset of the live payload.
                let replay_matches_live = records.len() == live.len()
                    && records.iter().zip(live.iter()).all(|(replayed, lived)| {
                        if replayed == lived {
                            return true;
                        }
                        let (Some(replay_object), Some(live_object)) =
                            (replayed.as_object(), lived.as_object())
                        else {
                            return false;
                        };
                        if replay_object.len() != live_object.len() {
                            return false;
                        }
                        replay_object.iter().all(|(key, replay_value)| {
                            match (key.as_str(), live_object.get(key)) {
                                ("payload", Some(live_value)) => {
                                    match (replay_value.as_object(), live_value.as_object()) {
                                        (Some(replay_payload), Some(live_payload)) => {
                                            replay_payload.iter().all(|(field, value)| {
                                                live_payload.get(field) == Some(value)
                                            })
                                        }
                                        _ => replay_value == live_value,
                                    }
                                }
                                (_, Some(live_value)) => replay_value == live_value,
                                (_, None) => false,
                            }
                        })
                    });
                if !replay_matches_live {
                    return Err(AhrbError::Protocol(format!(
                        "durable replay records differed from raw live run projection beyond documented client augmentation: live={}, replay={}",
                        live.len(),
                        records.len()
                    )));
                }
            }
            if let Some(before) = before_state {
                let after = run_json_output_command(
                    &state_argv,
                    &self.config.environment,
                    &working_directory,
                    self.config.timeout,
                    self.config.max_output_bytes,
                    "post-replay durable state",
                )
                .await?;
                validate_scalar_assertions(
                    &before,
                    &self.config.events.replay_state_assertions,
                    "pre-replay durable state",
                )?;
                validate_scalar_assertions(
                    &after,
                    &self.config.events.replay_state_assertions,
                    "post-replay durable state",
                )?;
                for pointer in &self.config.events.replay_state_pointers {
                    let before_value = before.pointer(pointer).ok_or_else(|| {
                        AhrbError::Protocol(format!(
                            "pre-replay durable state omitted comparison field at {pointer}"
                        ))
                    })?;
                    let after_value = after.pointer(pointer).ok_or_else(|| {
                        AhrbError::Protocol(format!(
                            "post-replay durable state omitted comparison field at {pointer}"
                        ))
                    })?;
                    if before_value != after_value {
                        return Err(AhrbError::Protocol(format!(
                            "durable replay mutated state at {pointer}: before={}, after={}",
                            before_value, after_value
                        )));
                    }
                }
            }
            let mut replayed = persisted;
            if self.config.events.source == "stdout" {
                // Normalize the replay stream into AHRB cursors before applying
                // the caller's AHRB cursor. An adapter-declared replay envelope
                // has already been unwrapped above.
                replayed.next_cursor = self.config.events.replay_cursor_start.unwrap_or(1);
                let mut normalized = Self::normalize_records(
                    &self.config.events,
                    &mut replayed,
                    &records,
                    &BTreeMap::new(),
                    "turn-1",
                    true,
                )?;
                normalized.retain(|event| after.is_none_or(|cursor| event.cursor > cursor.0));
                Ok(normalized)
            } else {
                for record in &records {
                    if record
                        .pointer(&self.config.events.cursor_pointer)
                        .and_then(Value::as_u64)
                        .is_none()
                    {
                        return Err(AhrbError::Protocol(
                            "durable journal replay omitted its source cursor".to_owned(),
                        ));
                    }
                }
                records.retain(|record| {
                    let cursor = record
                        .pointer(&self.config.events.cursor_pointer)
                        .and_then(Value::as_u64);
                    after.is_none_or(|after| cursor.is_some_and(|cursor| cursor > after.0))
                });
                replayed.next_cursor = after.map_or(1, |cursor| cursor.0.saturating_add(1));
                Self::normalize_records(
                    &self.config.events,
                    &mut replayed,
                    &records,
                    &BTreeMap::new(),
                    "journal",
                    true,
                )
            }
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
            if !self.config.resume_control_command.is_empty() {
                let command = self.config.resume_control_command.clone();
                let evidence = self
                    .run_control_command(&command, &persisted, None)
                    .await?
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "declared resume control command returned no JSON evidence".to_owned(),
                        )
                    })?;
                let mut evidence_log = self
                    .sessions
                    .get(&id)
                    .map(|session| session.control_evidence.clone())
                    .unwrap_or_default();
                evidence_log.push(evidence);
                Self::persist_session_at(&path, &persisted)?;
                self.sessions.insert(
                    id,
                    ExecSession {
                        persisted,
                        active: None,
                        client_exit: ClientExit::Exited(Some(0)),
                        control_evidence: evidence_log,
                    },
                );
                return Ok(());
            } else if self.config.events.source == "journal-file" {
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
                    client_exit: ClientExit::Exited(Some(0)),
                    control_evidence: Vec::new(),
                },
            );
            Ok(())
        })
    }

    fn recover_probe(&mut self, session: &SessionId) -> DriverFuture<'_, ()> {
        let id = session.0.clone();
        Box::pin(async move {
            let persisted = self
                .sessions
                .get(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?
                .persisted
                .clone();
            if self.config.recover_probe_command.is_empty() {
                return self.resume(&SessionId(id)).await;
            }
            let command = self.config.recover_probe_command.clone();
            let evidence = self
                .run_control_command(&command, &persisted, None)
                .await?
                .ok_or_else(|| {
                    AhrbError::Protocol(
                        "declared recovery probe returned no JSON evidence".to_owned(),
                    )
                })?;
            let item = self
                .sessions
                .get_mut(&id)
                .ok_or_else(|| AhrbError::Protocol(format!("unknown exec session {id:?}")))?;
            item.control_evidence.push(evidence);
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
                .map(|_| ())
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
                let invocation_pid = invocation.child.id();
                let mut persisted = self
                    .sessions
                    .get(&id)
                    .ok_or_else(|| AhrbError::Protocol(format!("exec session {id:?} disappeared")))?
                    .persisted
                    .clone();
                if self.config.cancel_command.is_empty() {
                    stop_managed_child(&mut invocation.child, Duration::from_millis(100)).await?;
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
                    let _ = self.run_control_command(&command, &persisted, None).await?;
                    let deadline = std::time::Instant::now() + Duration::from_secs(5);
                    let status = loop {
                        if let Some(status) = invocation.child.try_wait()? {
                            break status;
                        }
                        if std::time::Instant::now() >= deadline {
                            stop_managed_child(&mut invocation.child, Duration::from_millis(100))
                                .await?;
                            break invocation.child.try_wait()?.ok_or_else(|| {
                                AhrbError::Protocol(
                                    "cancelled thin client could not be reaped".to_owned(),
                                )
                            })?;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    };
                    self.refresh_source(&mut persisted, &invocation, Some(status))?;
                    if self.config.events.source == "journal-file" {
                        self.refresh_inactive_journal(&mut persisted)?;
                    }
                }
                if let Some(pid) = invocation_pid {
                    crate::process::retire_process(pid)?;
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
                    item.client_exit = ClientExit::Exited(
                        invocation
                            .child
                            .try_wait()?
                            .and_then(|status| status.code()),
                    );
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
            self.await_readiness().await?;
            let ids: Vec<String> = self
                .sessions
                .iter()
                .filter(|(_, session)| session.active.is_some())
                .map(|(id, _)| id.clone())
                .collect();
            let mut invocation_result = Ok(());
            for id in ids {
                if let Err(error) = self.cancel(&SessionId(id)).await {
                    invocation_result = Err(error);
                    break;
                }
            }
            let daemon_result = if let (Some(mut daemon), Some(config)) =
                (self.daemon_process.take(), self.config.daemon.as_ref())
            {
                let result = stop_managed_daemon(&mut daemon, config).await;
                self.lifecycle_notes.append(&mut daemon.lifecycle_notes);
                result
            } else {
                Ok(())
            };
            invocation_result.and(daemon_result)
        })
    }

    fn reap_after_external_kill(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            for session in self.sessions.values_mut() {
                if let Some(invocation) = session.active.as_mut() {
                    let pid = invocation.child.id();
                    invocation.child.wait().await?;
                    if let Some(pid) = pid {
                        crate::process::retire_process(pid)?;
                    }
                }
            }
            Ok(())
        })
    }

    fn release_invocations(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            use std::io::Write as _;
            for session in self.sessions.values_mut() {
                let Some(invocation) = session.active.as_mut() else {
                    continue;
                };
                let Some(path) = invocation.gate_path.as_ref() else {
                    continue;
                };
                // The same deadline clock is restarted at the release boundary,
                // excluding only AHRB's deliberate sampler-arming hold.
                invocation.started = std::time::Instant::now();
                let mut gate = std::fs::OpenOptions::new().write(true).open(path)?;
                gate.write_all(b"release\n")?;
                drop(gate);
                std::fs::remove_file(path)?;
            }
            Ok(())
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        let mut pids = self
            .sessions
            .values()
            .filter_map(|session| session.active.as_ref())
            .filter_map(|active| active.child.id())
            .collect::<Vec<_>>();
        if let Some(pid) = self
            .daemon_process
            .as_ref()
            .and_then(ManagedDaemonProcess::pid)
        {
            pids.push(pid);
        }
        if let Some(pid) = self.daemon_launch_pid {
            pids.push(pid);
        }
        pids.sort_unstable();
        pids.dedup();
        pids
    }

    fn daemon_pid(&self) -> Option<u32> {
        self.daemon_process
            .as_ref()
            .and_then(ManagedDaemonProcess::pid)
    }

    fn session_pids(&self, session: &SessionId) -> Vec<u32> {
        self.sessions
            .get(&session.0)
            .and_then(|item| item.active.as_ref())
            .and_then(|active| active.child.id())
            .into_iter()
            .collect()
    }

    fn completed_turn_wall_ns(&self) -> Vec<u64> {
        self.completed_turn_wall_ns.clone()
    }

    fn completed_turn_boundaries(&self) -> Vec<CompletedTurnBoundary> {
        self.completed_turn_boundaries.clone()
    }

    fn client_exit(&self, session: &SessionId) -> ClientExit {
        self.sessions
            .get(&session.0)
            .map_or(ClientExit::NotApplicable, |item| item.client_exit)
    }

    fn control_evidence(&self, session: &SessionId) -> Vec<Value> {
        self.sessions
            .get(&session.0)
            .map(|item| item.control_evidence.clone())
            .unwrap_or_default()
    }

    fn lifecycle_notes(&self) -> Vec<String> {
        self.lifecycle_notes.clone()
    }

    fn unmapped_payload_kinds(&self) -> Vec<String> {
        self.unmapped_payload_kinds.iter().cloned().collect()
    }

    fn wait_ready(
        &mut self,
        sessions: &[SessionId],
        timeout: Duration,
    ) -> DriverFuture<'_, Option<Value>> {
        let count = sessions.len();
        let session = sessions
            .first()
            .and_then(|session| self.sessions.get(&session.0))
            .map(|session| session.persisted.clone());
        let template = self.config.wait_ready_command.clone();
        Box::pin(async move {
            if template.is_empty() {
                return Ok(None);
            }
            let session = session.ok_or_else(|| {
                AhrbError::Protocol("wait-ready cohort omitted its sessions".to_owned())
            })?;
            let mut variables = self.invocation_variables(&session, "", "");
            variables.insert("count".to_owned(), count.to_string());
            variables.insert(
                "timeout".to_owned(),
                format!(
                    "{}ms",
                    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
                ),
            );
            let argv = template
                .iter()
                .map(|argument| crate::manifest::render_template(argument, &variables))
                .collect::<Result<Vec<_>>>()?;
            let (program, arguments) = argv.split_first().ok_or_else(|| {
                AhrbError::Validation("per-invocation wait-ready command is empty".to_owned())
            })?;
            let mut command = Command::new(program);
            #[cfg(unix)]
            command.process_group(0);
            command
                .args(arguments)
                .envs(&self.config.environment)
                .current_dir(self.session_directory(&session.local_id).join("workspace"))
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let output =
                run_owned_output(&mut command, timeout, "per-invocation wait-ready").await?;
            if !output.status.success() {
                return Err(AhrbError::Protocol(format!(
                    "per-invocation wait-ready exited with {}",
                    output.status
                )));
            }
            let value = serde_json::from_slice(&output.stdout).map_err(|error| {
                AhrbError::Protocol(format!("wait-ready emitted invalid JSON: {error}"))
            })?;
            Ok(Some(value))
        })
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl Drop for PerInvocationDriver {
    fn drop(&mut self) {
        if let Some(task) = self.daemon_start.take() {
            task.abort();
        }
        #[cfg(unix)]
        if let Some(pid) = self
            .daemon_process
            .as_ref()
            .and_then(ManagedDaemonProcess::pid)
        {
            if self
                .config
                .daemon
                .as_ref()
                .is_some_and(|config| config.launcher_exits)
            {
                if let Some(identity) = self
                    .daemon_process
                    .as_ref()
                    .and_then(|daemon| daemon.external_identity)
                {
                    let _ = signal_external_process(identity, libc::SIGKILL);
                }
            } else {
                let _ = signal_managed_process_group(pid, libc::SIGKILL);
            }
        }
        #[cfg(not(unix))]
        if let Some(child) = self
            .daemon_process
            .as_mut()
            .and_then(|daemon| daemon.child.as_mut())
        {
            let _ = child.start_kill();
        }
    }
}

fn classify_exit_contract(
    exit: &ExitContract,
    code: Option<i32>,
    stdout: &[u8],
) -> (EventVocab, Value) {
    let text = String::from_utf8_lossy(stdout);
    let failure_marker = exit
        .failure_stdout
        .iter()
        .find(|marker| text.contains(marker.as_str()))
        .cloned()
        .or_else(|| stdout_has_top_level_error(stdout).then(|| "top-level JSONL error".to_owned()));
    let success_marker_ok = exit.success_stdout.is_empty()
        || exit
            .success_stdout
            .iter()
            .any(|marker| text.contains(marker));
    let success = code == Some(exit.success) && failure_marker.is_none() && success_marker_ok;
    if success {
        (
            EventVocab::TerminalSuccess,
            json!({"status":"success", "exit_code":code}),
        )
    } else {
        let category = code
            .and_then(|value| {
                exit.failures
                    .iter()
                    .find_map(|(category, expected)| (*expected == value).then(|| category.clone()))
            })
            .unwrap_or_else(|| {
                if failure_marker.is_some() {
                    "stdout-failure".to_owned()
                } else if code == Some(exit.success) {
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

fn stdout_has_top_level_error(stdout: &[u8]) -> bool {
    stdout.split(|byte| *byte == b'\n').any(|line| {
        let line = trim_ascii(line);
        serde_json::from_slice::<Value>(line)
            .ok()
            .is_some_and(|value| {
                let event_type = value.get("type").and_then(Value::as_str);
                event_type == Some("error")
                    || (event_type == Some("result")
                        && value.get("is_error").and_then(Value::as_bool) == Some(true))
            })
    })
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

fn validate_replay_output(output: &std::process::Output, maximum: usize) -> Result<()> {
    if !output.status.success() {
        return Err(AhrbError::Protocol(format!(
            "per-invocation durable replay exited with {}",
            output.status
        )));
    }
    if output.stdout.len() > maximum {
        return Err(AhrbError::Protocol(
            "per-invocation durable replay exceeded capture bound".to_owned(),
        ));
    }
    if !output.stdout.is_empty() && !output.stdout.ends_with(b"\n") {
        return Err(AhrbError::Protocol(
            "durable replay output ended with a torn record".to_owned(),
        ));
    }
    Ok(())
}

fn extract_replay_records(mapping: &EventMapping, bytes: &[u8]) -> Result<Vec<Value>> {
    let records = parse_journal_records(bytes)?;
    if mapping.replay_mode == "document" {
        let [document] = records.as_slice() else {
            return Err(AhrbError::Protocol(format!(
                "durable replay document mode expected one JSON document, received {}",
                records.len()
            )));
        };
        validate_scalar_assertions(
            document,
            &mapping.replay_assertions,
            "durable replay document",
        )?;
        return document
            .pointer(&mapping.replay_records_pointer)
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "durable replay document omitted event array at {}",
                    mapping.replay_records_pointer
                ))
            });
    }
    if mapping.replay_envelope_pointer.is_empty() {
        return Ok(records);
    }
    records
        .iter()
        .map(|record| {
            record
                .pointer(&mapping.replay_envelope_pointer)
                .filter(|value| value.is_object())
                .cloned()
                .ok_or_else(|| {
                    AhrbError::Protocol(format!(
                        "durable replay record omitted object envelope at {}",
                        mapping.replay_envelope_pointer
                    ))
                })
        })
        .collect()
}

fn validate_scalar_assertions(
    document: &Value,
    assertions: &BTreeMap<String, String>,
    label: &str,
) -> Result<()> {
    for (pointer, expected) in assertions {
        let actual = document.pointer(pointer).ok_or_else(|| {
            AhrbError::Protocol(format!("{label} omitted asserted field at {pointer}"))
        })?;
        if !scalar_matches(actual, expected) {
            return Err(AhrbError::Protocol(format!(
                "{label} assertion at {pointer} expected {expected:?}, received {actual}"
            )));
        }
    }
    Ok(())
}

async fn run_json_output_command(
    argv: &[String],
    environment: &BTreeMap<String, String>,
    working_directory: &Path,
    timeout: Duration,
    maximum: usize,
    label: &str,
) -> Result<Value> {
    let (program, arguments) = argv
        .split_first()
        .ok_or_else(|| AhrbError::Validation(format!("{label} command is empty")))?;
    let mut command = Command::new(program);
    #[cfg(unix)]
    command.process_group(0);
    command
        .args(arguments)
        .envs(environment)
        .current_dir(working_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = run_owned_output(&mut command, timeout, label).await?;
    if !output.status.success() {
        return Err(AhrbError::Protocol(format!(
            "{label} exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    if output.stdout.len() > maximum {
        return Err(AhrbError::Protocol(format!(
            "{label} exceeded capture bound"
        )));
    }
    parse_last_json_value(&output.stdout)
        .ok_or_else(|| AhrbError::Protocol(format!("{label} emitted no JSON document")))
}

fn parse_last_json_value(bytes: &[u8]) -> Option<Value> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice(trim_ascii(line)).ok())
        .next_back()
}

fn scalar_matches(actual: &Value, expected: &str) -> bool {
    match actual {
        Value::String(value) => value == expected,
        Value::Bool(value) => value.to_string() == expected,
        Value::Number(value) => value.to_string() == expected,
        _ => false,
    }
}

fn inspect_event_contract(mapping: &EventMapping, records: &[Value]) -> Result<BTreeSet<String>> {
    if mapping.schema_version_pointer.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut unmapped = BTreeSet::new();
    for record in records {
        let Some(kind) = record
            .pointer(&mapping.type_pointer)
            .and_then(Value::as_str)
        else {
            // Mixed stdout streams can contain transport announcements which
            // are not durable envelopes and therefore have no schema version.
            continue;
        };
        let version = record
            .pointer(&mapping.schema_version_pointer)
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "source payload kind {kind:?} omitted u64 schema version at {}",
                    mapping.schema_version_pointer
                ))
            })?;
        if !mapping.schema_versions.contains(&version) {
            return Err(AhrbError::Protocol(format!(
                "unsupported source schema version {version} for payload kind {kind:?}; supported versions are {:?}",
                mapping.schema_versions
            )));
        }
        if mapping.warn_unmapped_payload_kinds
            && !mapping.rules.iter().any(|rule| rule.matches == kind)
        {
            unmapped.insert(kind.to_owned());
        }
    }
    Ok(unmapped)
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
            crate::process::register_child(&child)?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| AhrbError::Protocol("exec transport has no stdin".to_owned()))?;
            stdin.write_all(&bytes).await?;
            stdin.shutdown().await?;
            let output =
                wait_owned_output(&mut child, self.timeout, "exec transport request").await?;
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
    readiness: Option<Probe>,
    readiness_pid: Option<u32>,
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
            readiness: None,
            readiness_pid: None,
        }
    }

    /// Add deterministic per-process environment bindings.
    pub fn with_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.environment = environment;
        self
    }

    /// Require the persistent peer to satisfy a manifest-declared readiness
    /// probe and retain the PID returned by its JSON contract.
    pub fn with_readiness(mut self, readiness: Probe) -> Self {
        self.readiness = Some(readiness);
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
            crate::process::register_child(&child)?;
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

    fn await_readiness(&mut self) -> DriverFuture<'_, ()> {
        Box::pin(async move {
            if self.readiness_pid.is_some() || self.readiness.is_none() {
                return Ok(());
            }
            if let Some(readiness) = self.readiness.clone() {
                let deadline =
                    std::time::Instant::now() + Duration::from_millis(readiness.timeout_ms.max(1));
                let launched = self.pid().ok_or_else(|| {
                    AhrbError::Protocol("stdin-RPC daemon disappeared during readiness".to_owned())
                })?;
                let observation = loop {
                    match managed_daemon_readiness_satisfied(&readiness, &self.environment).await {
                        Ok(Some(observation))
                            if observation.pid.is_none_or(|pid| pid == launched) =>
                        {
                            break observation;
                        }
                        Ok(Some(_)) if std::time::Instant::now() < deadline => {
                            // A cold restart may briefly expose the previous
                            // daemon's atomically published PID. It is not
                            // readiness for this owned process identity.
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Ok(Some(_)) => {
                            if let Some(mut child) = self.child.take() {
                                stop_managed_child(&mut child, Duration::from_millis(100)).await?;
                            }
                            self.stdin.take();
                            self.stdout.take();
                            return Err(AhrbError::Timeout(format!(
                                "stdin-RPC readiness never published owned PID {launched}"
                            )));
                        }
                        Ok(None) if std::time::Instant::now() < deadline => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Ok(None) => {
                            if let Some(mut child) = self.child.take() {
                                stop_managed_child(&mut child, Duration::from_millis(100)).await?;
                            }
                            self.stdin.take();
                            self.stdout.take();
                            return Err(AhrbError::Timeout(
                                "stdin-RPC daemon readiness".to_owned(),
                            ));
                        }
                        Err(error) => {
                            if let Some(mut child) = self.child.take() {
                                stop_managed_child(&mut child, Duration::from_millis(100)).await?;
                            }
                            self.stdin.take();
                            self.stdout.take();
                            return Err(error);
                        }
                    }
                };
                if let Some(pid) = observation.pid {
                    if pid != launched {
                        if let Some(mut child) = self.child.take() {
                            stop_managed_child(&mut child, Duration::from_millis(100)).await?;
                        }
                        self.stdin.take();
                        self.stdout.take();
                        return Err(AhrbError::Protocol(format!(
                            "readiness reported daemon PID {pid}, but stdin-RPC owns PID {launched}"
                        )));
                    }
                }
                self.readiness_pid = observation.pid;
            }
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
                let child_pid = child.id();
                match tokio::time::timeout(self.timeout, child.wait()).await {
                    Ok(status) => {
                        status?;
                        if let Some(pid) = child_pid {
                            crate::process::retire_process(pid)?;
                        }
                    }
                    Err(_) => {
                        stop_managed_child(&mut child, Duration::from_millis(100)).await?;
                    }
                }
            }
            self.readiness_pid = None;
            Ok(())
        })
    }

    fn owned_pids(&self) -> Vec<u32> {
        self.pid().into_iter().collect()
    }

    fn daemon_pid(&self) -> Option<u32> {
        self.readiness_pid.or_else(|| self.pid())
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

    struct TypedEscalationTransport {
        stopped: bool,
    }

    impl Transport for TypedEscalationTransport {
        fn start(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn request(&mut self, _request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
            Box::pin(async {
                Ok(TransportResponse {
                    id: "shutdown".to_owned(),
                    result: json!({"outcome":"did_not_stop"}),
                })
            })
        }

        fn stop(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async move {
                self.stopped = true;
                Ok(())
            })
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
            type_pointer: String::new(),
            id_pointer: String::new(),
            cursor_pointer: String::new(),
            schema_version_pointer: String::new(),
            schema_versions: Vec::new(),
            warn_unmapped_payload_kinds: false,
            replay_command: Vec::new(),
            replay_mode: "lines".to_owned(),
            replay_records_pointer: String::new(),
            replay_assertions: BTreeMap::new(),
            replay_state_command: Vec::new(),
            replay_state_assertions: BTreeMap::new(),
            replay_state_pointers: Vec::new(),
            replay_compare_live_records: false,
            replay_envelope_pointer: String::new(),
            replay_cursor_start: None,
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
                    payload_bindings: BTreeMap::new(),
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
                    payload_bindings: BTreeMap::new(),
                },
            ],
        };
        let mut session = PersistedExecSession {
            local_id: "00000000-0000-4000-8000-000000000000".to_owned(),
            marker: "actor".to_owned(),
            harness_id: String::new(),
            run_id: String::new(),
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

    #[test]
    fn codex_exec_jsonl_rules_normalize_known_tool_and_terminal_items() {
        let manifest = crate::manifest::load(Path::new("adapters/codex/manifest.toml"))
            .expect("load Codex manifest");
        let mut records = vec![
            json!({"type": "thread.started", "thread_id": "thread-1"}),
            json!({"type": "turn.started"}),
        ];
        for (index, item_type) in [
            "command_execution",
            "function_call",
            "local_shell_call",
            "file_change",
            "patch_apply",
        ]
        .iter()
        .enumerate()
        {
            let id = format!("tool-{index}");
            let command = if index == 0 {
                let metadata = serde_json::to_vec(&json!({
                    "call_id": "call-fail",
                    "name": "fail_fixture",
                    "arguments": {"message": "expected failure"}
                }))
                .expect("serialize test fixture metadata")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
                format!(
                    "/bin/zsh -lc '/bin/false # {}{}'",
                    NATIVE_FIXTURE_METADATA_PREFIX, metadata
                )
            } else {
                "/bin/false".to_owned()
            };
            records.push(json!({
                "type": "item.started",
                "item": {"id": id, "type": item_type, "command": command}
            }));
            records.push(json!({
                "type": "item.completed",
                "item": {"id": id, "type": item_type, "command": command, "exit_code": 1, "aggregated_output": "fixture failure"}
            }));
        }
        records.extend([
            json!({
                "type": "item.completed",
                "item": {"id": "message-1", "type": "agent_message", "text": "done"}
            }),
            json!({"type": "turn.completed", "usage": {"output_tokens": 1}}),
            json!({"type": "turn.failed", "error": {"message": "failed"}}),
            json!({"type": "error", "message": "fatal provider error"}),
        ]);
        let mut session = PersistedExecSession {
            local_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            marker: "root".to_owned(),
            harness_id: String::new(),
            run_id: String::new(),
            turns: 1,
            invocations: 1,
            next_cursor: 1,
            closed: false,
        };
        let events = PerInvocationDriver::normalize_records(
            &manifest.events,
            &mut session,
            &records,
            &BTreeMap::new(),
            "turn-1",
            true,
        )
        .expect("normalize Codex JSONL records");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == EventVocab::ToolCall)
                .count(),
            5
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count(),
            5
        );
        for event in events
            .iter()
            .filter(|event| matches!(event.event, EventVocab::ToolCall | EventVocab::ToolResult))
        {
            if event.payload.get("native_call_id").is_none() {
                assert_eq!(
                    event.payload.get("call_id").and_then(Value::as_str),
                    event.payload.get("id").and_then(Value::as_str)
                );
            }
            let bound = if event.event == EventVocab::ToolCall {
                event.payload.get("arguments")
            } else {
                event.payload.get("result")
            };
            assert!(bound.is_some(), "Codex tool payload was not correlated");
        }
        let failed_result = events
            .iter()
            .find(|event| {
                event.event == EventVocab::ToolResult
                    && event.payload.get("call_id").and_then(Value::as_str) == Some("call-fail")
            })
            .expect("correlated command result");
        assert_eq!(
            failed_result
                .payload
                .get("native_call_id")
                .and_then(Value::as_str),
            Some("tool-0")
        );
        assert_eq!(
            failed_result.payload.get("name").and_then(Value::as_str),
            Some("fail_fixture")
        );
        assert_eq!(
            failed_result
                .payload
                .pointer("/result/ok")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            failed_result
                .payload
                .pointer("/result/exit_code")
                .and_then(Value::as_i64),
            Some(1)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == EventVocab::ModelResponse)
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess)
        );
        assert!(
            events
                .iter()
                .any(|event| event.event == EventVocab::TerminalFailure)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == EventVocab::TerminalFailure)
                .count(),
            2
        );
        assert_eq!(events.len(), 16);
    }

    #[test]
    fn remaining_exec_adapter_rules_correlate_native_fixture_events() {
        let metadata = serde_json::to_vec(&json!({
            "call_id": "call-write",
            "name": "write_fixture",
            "arguments": {"path": "fixture.txt", "content": "fixture payload"}
        }))
        .expect("serialize fixture metadata")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
        let command = format!(
            "/usr/bin/true; : '{}{}'",
            NATIVE_FIXTURE_METADATA_PREFIX, metadata
        );
        let cases = [
            (
                "claude-code",
                vec![
                    json!({"type":"system","subtype":"init","session_id":"claude-session"}),
                    json!({
                        "type":"assistant",
                        "session_id":"claude-session",
                        "message":{"content":[{
                            "type":"tool_use","id":"toolu-1","name":"Bash",
                            "input":{"command":command}
                        }]}
                    }),
                    json!({
                        "type":"user",
                        "session_id":"claude-session",
                        "message":{"content":[{
                            "type":"tool_result","tool_use_id":"toolu-1",
                            "content":"ok","is_error":false
                        }]}
                    }),
                    json!({
                        "type":"assistant","session_id":"claude-session",
                        "message":{"content":[{"type":"text","text":"done"}]}
                    }),
                    json!({
                        "type":"result","subtype":"success","is_error":false,
                        "session_id":"claude-session"
                    }),
                ],
            ),
            (
                "pi",
                vec![
                    json!({"type":"session","version":3,"id":"pi-session","cwd":"/tmp"}),
                    json!({"type":"agent_start"}),
                    json!({"type":"turn_start"}),
                    json!({
                        "type":"tool_execution_start","toolCallId":"pi-tool-1",
                        "toolName":"bash","args":{"command":command}
                    }),
                    json!({
                        "type":"tool_execution_end","toolCallId":"pi-tool-1",
                        "toolName":"bash","result":{"content":"ok"},"isError":false
                    }),
                    json!({
                        "type":"message_end",
                        "message":{"role":"assistant","content":[],"stopReason":"stop"}
                    }),
                    json!({"type":"agent_settled"}),
                ],
            ),
            (
                "rick",
                vec![
                    json!({
                        "type":"tool_start",
                        "tool":{"name":"bash","input":{"command":command}}
                    }),
                    json!({
                        "type":"tool_end",
                        "tool":{
                            "name":"bash","input":{"command":command},
                            "output":"ok","is_error":false
                        }
                    }),
                    json!({"type":"text","text":"done"}),
                    json!({"type":"done","session_id":"rick-session"}),
                ],
            ),
            (
                "opencode",
                vec![
                    json!({
                        "type":"step_start","sessionID":"opencode-session",
                        "part":{"id":"step-1","type":"step-start"}
                    }),
                    json!({
                        "type":"tool_use","sessionID":"opencode-session",
                        "part":{
                            "id":"part-tool-1","callID":"opencode-tool-1",
                            "tool":"bash","type":"tool",
                            "state":{
                                "status":"completed",
                                "input":{"command":command},
                                "output":"ok","metadata":{"exit":0}
                            }
                        }
                    }),
                    json!({
                        "type":"text","sessionID":"opencode-session",
                        "part":{"id":"text-1","type":"text","text":"done"}
                    }),
                    json!({
                        "type":"step_finish","sessionID":"opencode-session",
                        "part":{"id":"step-2","type":"step-finish","reason":"stop"}
                    }),
                ],
            ),
            (
                "haider-agent",
                vec![
                    json!({
                        "event":"accepted","session_id":"haider-session","head_seq":0
                    }),
                    json!({
                        "event_id":"evt-thinking","seq":1,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{"type":"run_state","state":"thinking"}
                    }),
                    // Real 0.0.967 order: the `started` tool_call item carries
                    // no args, the tool_result commits next, and the `completed`
                    // item (last) carries the full command with the fixture
                    // marker. AHRB anchors the call on `started` and backfills its
                    // arguments from `completed` so the call precedes the result.
                    json!({
                        "event_id":"evt-tool-start","seq":2,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{
                            "type":"item","event":"started","item_id":"item-1",
                            "item":{
                                "item":"tool_call","call_id":"haider-tool-1",
                                "name":"process_exec","args":{},
                                "status":"in_progress"
                            }
                        }
                    }),
                    json!({
                        "event_id":"evt-tool-result","seq":3,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{
                            "type":"tool_result","call_id":"haider-tool-1",
                            "result":{
                                "preview":"{\"exit_code\":0,\"status\":\"completed\",\"output\":\"ok\"}",
                                "truncated":false
                            }
                        }
                    }),
                    json!({
                        "event_id":"evt-tool-done","seq":4,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{
                            "type":"item","event":"completed","item_id":"item-1",
                            "item":{
                                "item":"tool_call","call_id":"haider-tool-1",
                                "name":"process_exec","args":{"command":command},
                                "status":"completed"
                            }
                        }
                    }),
                    json!({
                        "event_id":"evt-message","seq":5,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{
                            "type":"item","event":"completed","item_id":"item-2",
                            "item":{"item":"agent_message","text":"done"}
                        }
                    }),
                    json!({
                        "event_id":"evt-done","seq":6,
                        "session_id":"haider-session","run_id":"run-1",
                        "payload":{"type":"run_state","state":"done","terminal_kind":"success"}
                    }),
                ],
            ),
        ];

        for (index, (adapter, records)) in cases.into_iter().enumerate() {
            let manifest =
                crate::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))
                    .expect("load exec adapter manifest");
            let mut session = PersistedExecSession {
                local_id: format!("00000000-0000-4000-8000-00000000000{index}"),
                marker: "root".to_owned(),
                harness_id: String::new(),
                run_id: String::new(),
                turns: 1,
                invocations: 1,
                next_cursor: 1,
                closed: false,
            };
            let events = PerInvocationDriver::normalize_records(
                &manifest.events,
                &mut session,
                &records,
                &BTreeMap::new(),
                "turn-1",
                true,
            )
            .expect("normalize native exec events");
            let call = events
                .iter()
                .find(|event| event.event == EventVocab::ToolCall)
                .unwrap_or_else(|| {
                    panic!(
                        "normalized tool call missing for {adapter}; normalized kinds: {:?}",
                        events.iter().map(|event| &event.event).collect::<Vec<_>>()
                    )
                });
            let result = events
                .iter()
                .find(|event| event.event == EventVocab::ToolResult)
                .expect("normalized tool result");
            assert_eq!(
                call.payload.get("call_id").and_then(Value::as_str),
                Some("call-write"),
                "{adapter}"
            );
            assert_eq!(
                result.payload.get("call_id").and_then(Value::as_str),
                Some("call-write"),
                "{adapter}"
            );
            assert_eq!(
                result
                    .payload
                    .pointer("/result/ok")
                    .and_then(Value::as_bool),
                Some(true),
                "{adapter}"
            );
            assert!(
                events
                    .iter()
                    .any(|event| event.event == EventVocab::TerminalSuccess),
                "{adapter}"
            );
        }
    }

    #[test]
    fn native_result_status_supports_boolean_error_and_output_schemas() {
        for (native, expected) in [
            (json!({"is_error": false}), true),
            (json!({"is_error": true}), false),
            (json!({"isError": false}), true),
            (json!({"isError": true}), false),
            (json!({"output": "completed"}), true),
        ] {
            assert_eq!(
                canonical_native_result(native)
                    .get("ok")
                    .and_then(Value::as_bool),
                Some(expected)
            );
        }
    }

    #[test]
    fn codex_exit_contract_ignores_nested_metadata_warning_but_keeps_failures() {
        let manifest = crate::manifest::load(Path::new("adapters/codex/manifest.toml"))
            .expect("load Codex manifest");
        let metadata_warning_then_success = br#"{"type":"item.completed","item":{"type":"error","message":"Model metadata not found"}}
{"type":"turn.completed","usage":{"output_tokens":1}}
"#;
        let (event, _) =
            classify_exit_contract(&manifest.exit, Some(0), metadata_warning_then_success);
        assert_eq!(event, EventVocab::TerminalSuccess);

        for (code, stdout) in [
            (
                Some(0),
                br#"{"type":"error","message":"fatal"}
{"type":"turn.completed","usage":{"output_tokens":1}}
"#
                .as_slice(),
            ),
            (
                Some(0),
                br#"{"type":"turn.failed"}
"#
                .as_slice(),
            ),
            (
                Some(1),
                br#"{"type":"turn.completed","usage":{"output_tokens":1}}
"#
                .as_slice(),
            ),
            (
                Some(0),
                br#"{"type":"item.completed"}
"#
                .as_slice(),
            ),
        ] {
            let (event, _) = classify_exit_contract(&manifest.exit, code, stdout);
            assert_eq!(event, EventVocab::TerminalFailure);
        }
    }

    #[test]
    fn claude_exit_contract_distinguishes_tool_and_terminal_errors() {
        let manifest = crate::manifest::load(Path::new("adapters/claude-code/manifest.toml"))
            .expect("load Claude Code manifest");
        let failed_tool_then_success = br#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu-1","is_error":true}]}}
{"type":"result","subtype":"success","is_error":false}
"#;
        let (event, _) = classify_exit_contract(&manifest.exit, Some(0), failed_tool_then_success);
        assert_eq!(event, EventVocab::TerminalSuccess);

        let failed_terminal = br#"{"type":"result","subtype":"success","is_error":true}
"#;
        let (event, _) = classify_exit_contract(&manifest.exit, Some(0), failed_terminal);
        assert_eq!(event, EventVocab::TerminalFailure);
    }

    #[test]
    fn claude_error_subtype_is_terminal_even_when_is_error_is_false() {
        let manifest = crate::manifest::load(Path::new("adapters/claude-code/manifest.toml"))
            .expect("load Claude Code manifest");
        let mut session = PersistedExecSession {
            local_id: "00000000-0000-4000-8000-000000000000".to_owned(),
            marker: "root".to_owned(),
            harness_id: String::new(),
            run_id: String::new(),
            turns: 1,
            invocations: 1,
            next_cursor: 1,
            closed: false,
        };
        let records = [json!({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": false,
            "session_id": "claude-session"
        })];
        let events = PerInvocationDriver::normalize_records(
            &manifest.events,
            &mut session,
            &records,
            &BTreeMap::new(),
            "turn-1",
            true,
        )
        .expect("normalize Claude error subtype");
        assert!(
            events
                .iter()
                .any(|event| event.event == EventVocab::TerminalFailure)
        );
    }

    #[test]
    fn claude_success_subtype_is_failure_when_result_is_error() {
        let manifest = crate::manifest::load(Path::new("adapters/claude-code/manifest.toml"))
            .expect("load Claude Code manifest");
        let mut session = PersistedExecSession {
            local_id: "00000000-0000-4000-8000-000000000000".to_owned(),
            marker: "root".to_owned(),
            harness_id: String::new(),
            run_id: String::new(),
            turns: 1,
            invocations: 1,
            next_cursor: 1,
            closed: false,
        };
        let records = [json!({
            "type": "result",
            "subtype": "success",
            "is_error": true,
            "session_id": "claude-session"
        })];
        let events = PerInvocationDriver::normalize_records(
            &manifest.events,
            &mut session,
            &records,
            &BTreeMap::new(),
            "turn-1",
            true,
        )
        .expect("normalize Claude API error result");
        assert!(
            events
                .iter()
                .any(|event| event.event == EventVocab::TerminalFailure)
        );
        assert!(
            !events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess)
        );
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
                launcher_exits: false,
                initialize_command: Vec::new(),
                initialize_marker: logs.join("initialized"),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "process".to_owned(),
                    target: String::new(),
                    command: Vec::new(),
                    json_pointer_roots: BTreeMap::new(),
                    timeout_ms: 1_000,
                    ..Probe::default()
                },
                shutdown_command: Vec::new(),
                shutdown_result: ShutdownResult::default(),
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

    #[tokio::test]
    async fn managed_daemon_command_readiness_initializes_once_per_profile() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-managed-daemon-init-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if logs.exists() {
            std::fs::remove_dir_all(&logs).expect("remove stale daemon init logs");
        }
        let marker = logs.join("state/initialized");
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec!["/bin/sleep".to_owned(), "30".to_owned()],
                launcher_exits: false,
                initialize_command: vec!["/usr/bin/true".to_owned()],
                initialize_marker: marker.clone(),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "command".to_owned(),
                    target: String::new(),
                    command: vec!["/usr/bin/true".to_owned()],
                    json_pointer_roots: BTreeMap::new(),
                    timeout_ms: 1_000,
                    ..Probe::default()
                },
                shutdown_command: Vec::new(),
                shutdown_result: ShutdownResult::default(),
                grace: Duration::from_millis(100),
                log_directory: logs.join("logs"),
            },
        );
        transport.start().await.expect("start initialized daemon");
        assert!(marker.is_file());
        transport.stop().await.expect("stop initialized daemon");

        transport.config.initialize_command = vec!["/usr/bin/false".to_owned()];
        transport
            .start()
            .await
            .expect("existing marker skips repeated initialization");
        transport.stop().await.expect("stop restarted daemon");
        std::fs::remove_dir_all(logs).expect("remove daemon init logs");
    }

    #[tokio::test]
    async fn per_invocation_driver_owns_resident_daemon_separately_from_clients() {
        let profile = std::env::temp_dir().join(format!(
            "ahrb-thin-client-daemon-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if profile.exists() {
            std::fs::remove_dir_all(&profile).expect("remove stale thin-client profile");
        }
        let manifest = crate::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
            .expect("load exec reference manifest");
        let daemon = ManagedDaemonConfig {
            command: vec!["/bin/sleep".to_owned(), "30".to_owned()],
            launcher_exits: false,
            initialize_command: Vec::new(),
            initialize_marker: profile.join("initialized"),
            environment: BTreeMap::new(),
            readiness: Probe {
                kind: "process".to_owned(),
                target: String::new(),
                command: Vec::new(),
                json_pointer_roots: BTreeMap::new(),
                timeout_ms: 1_000,
                ..Probe::default()
            },
            shutdown_command: Vec::new(),
            shutdown_result: ShutdownResult::default(),
            grace: Duration::from_millis(100),
            log_directory: profile.join("daemon-logs"),
        };
        let mut driver = PerInvocationDriver::new(PerInvocationConfig {
            daemon: Some(daemon),
            command: vec!["/usr/bin/true".to_owned()],
            resume_command: Vec::new(),
            resume_control_command: Vec::new(),
            recover_probe_command: Vec::new(),
            release_command: Vec::new(),
            cancel_command: Vec::new(),
            replay_command: Vec::new(),
            wait_ready_command: Vec::new(),
            environment: BTreeMap::new(),
            base_variables: BTreeMap::new(),
            profile_root: profile.clone(),
            events: manifest.events,
            exit: manifest.exit,
            session_id_pointer: String::new(),
            run_id_pointer: String::new(),
            timeout: Duration::from_secs(1),
            max_output_bytes: 4_096,
            gate_launch: false,
        });
        driver.start().await.expect("start thin-client driver");
        assert_eq!(driver.owned_pids().len(), 1);
        driver.shutdown().await.expect("stop thin-client driver");
        assert!(driver.owned_pids().is_empty());
        std::fs::remove_dir_all(profile).expect("remove thin-client profile");
    }

    #[tokio::test]
    async fn command_json_readiness_extracts_pid_and_ready_state() {
        let probe = Probe {
            kind: "command-json".to_owned(),
            command: vec![
                "/usr/bin/printf".to_owned(),
                json!({"daemon":{"pid":std::process::id(),"ready":true}}).to_string(),
            ],
            pid_pointer: "/daemon/pid".to_owned(),
            ready_pointer: "/daemon/ready".to_owned(),
            timeout_ms: 1_000,
            ..Probe::default()
        };
        let observation = managed_daemon_readiness_satisfied(&probe, &BTreeMap::new())
            .await
            .expect("parse readiness JSON")
            .expect("ready=true");
        assert_eq!(observation.pid, Some(std::process::id()));

        let mut not_ready = probe;
        not_ready.command[1] =
            json!({"daemon":{"pid":std::process::id(),"ready":false}}).to_string();
        assert!(
            managed_daemon_readiness_satisfied(&not_ready, &BTreeMap::new())
                .await
                .expect("parse not-ready JSON")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn per_invocation_driver_reaps_externally_killed_launchers() {
        let profile = std::env::temp_dir().join(format!(
            "ahrb-external-kill-reap-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let manifest = crate::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
            .expect("load exec reference manifest");
        let mut driver = PerInvocationDriver::new(PerInvocationConfig {
            daemon: None,
            command: vec!["/bin/sleep".to_owned(), "30".to_owned()],
            resume_command: Vec::new(),
            resume_control_command: Vec::new(),
            recover_probe_command: Vec::new(),
            release_command: Vec::new(),
            cancel_command: Vec::new(),
            replay_command: Vec::new(),
            wait_ready_command: Vec::new(),
            environment: BTreeMap::new(),
            base_variables: BTreeMap::new(),
            profile_root: profile.clone(),
            events: manifest.events,
            exit: manifest.exit,
            session_id_pointer: String::new(),
            run_id_pointer: String::new(),
            timeout: Duration::from_secs(1),
            max_output_bytes: 4_096,
            gate_launch: false,
        });
        driver.start().await.expect("start per-invocation driver");
        let session = driver
            .create_session("external-kill")
            .await
            .expect("session");
        driver
            .submit(&session, "ignored", "turn-1")
            .await
            .expect("launch child");
        let pid = driver
            .session_pids(&session)
            .into_iter()
            .next()
            .expect("active launcher PID");
        let platform_pid = i32::try_from(pid).expect("PID fits platform range");
        // SAFETY: the PID belongs to the child just spawned by this test.
        assert_eq!(unsafe { libc::kill(platform_pid, libc::SIGKILL) }, 0);

        driver
            .reap_after_external_kill()
            .await
            .expect("reap externally killed launcher");
        let status = driver
            .sessions
            .get_mut(&session.0)
            .and_then(|session| session.active.as_mut())
            .expect("active invocation handle")
            .child
            .try_wait()
            .expect("query reaped launcher");
        assert!(status.is_some());

        drop(driver);
        std::fs::remove_dir_all(profile).expect("remove external-kill profile");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn detached_launcher_is_located_by_isolated_environment_and_reaped() {
        let profile = std::env::temp_dir().join(format!(
            "ahrb-detached-daemon-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if profile.exists() {
            std::fs::remove_dir_all(&profile).expect("remove stale detached-daemon profile");
        }
        let home = profile.join("home").to_string_lossy().into_owned();
        let temporary = profile.join("tmp").to_string_lossy().into_owned();
        let runtime = profile.join("run").to_string_lossy().into_owned();
        let pid_file = profile.join("daemon.pid");
        let readiness_script = format!(
            r#"pid=$(/bin/cat "$1"); /usr/bin/printf '{{"profile_path":"{home}/.haider/dev-profile","runtime_dir":"{runtime}/haider/abc123","daemon":{{"pid":%s,"ready":true,"pipe_dir":"{home}/.haider/dev-profile/pipe"}}}}' "$pid""#
        );
        let environment = BTreeMap::from([
            ("HOME".to_owned(), home),
            ("TMPDIR".to_owned(), temporary),
            ("XDG_RUNTIME_DIR".to_owned(), runtime),
            (
                "AHRB_PROFILE".to_owned(),
                profile.to_string_lossy().into_owned(),
            ),
        ]);
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "/bin/sleep 30 & /usr/bin/printf '%s' \"$!\" > \"$1\"".to_owned(),
                    "ahrb-detached-daemon".to_owned(),
                    pid_file.to_string_lossy().into_owned(),
                ],
                launcher_exits: true,
                initialize_command: Vec::new(),
                initialize_marker: profile.join("initialized"),
                environment,
                readiness: Probe {
                    kind: "command-json".to_owned(),
                    target: String::new(),
                    command: vec![
                        "/bin/sh".to_owned(),
                        "-c".to_owned(),
                        readiness_script,
                        "ahrb-readiness".to_owned(),
                        pid_file.to_string_lossy().into_owned(),
                    ],
                    pid_pointer: "/daemon/pid".to_owned(),
                    ready_pointer: "/daemon/ready".to_owned(),
                    json_pointer_roots: BTreeMap::from([
                        ("/profile_path".to_owned(), "HOME".to_owned()),
                        ("/runtime_dir".to_owned(), "XDG_RUNTIME_DIR".to_owned()),
                        ("/daemon/pipe_dir".to_owned(), "HOME".to_owned()),
                    ]),
                    timeout_ms: 2_000,
                },
                shutdown_command: Vec::new(),
                shutdown_result: ShutdownResult::default(),
                grace: Duration::from_millis(500),
                log_directory: profile.join("logs"),
            },
        );
        transport.start().await.expect("start detached daemon");
        let pids = transport.owned_pids();
        assert_eq!(pids.len(), 1);
        let pid = pids[0];
        assert!(
            profile
                .join("logs")
                .read_dir()
                .expect("read daemon logs")
                .filter_map(std::result::Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".readiness.json"))
        );
        transport.stop().await.expect("stop detached daemon");
        assert!(wait_until_pid_is_dead(pid).await);
        std::fs::remove_dir_all(profile).expect("remove detached daemon profile");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn detached_startup_failure_still_runs_typed_shutdown_after_launcher_exit() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-detached-start-failure-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if logs.exists() {
            std::fs::remove_dir_all(&logs).expect("remove stale detached-start logs");
        }
        let shutdown_marker = logs.join("typed-shutdown-ran");
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec!["/usr/bin/true".to_owned()],
                launcher_exits: true,
                initialize_command: Vec::new(),
                initialize_marker: logs.join("initialized"),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: logs.join("never-ready").to_string_lossy().into_owned(),
                    timeout_ms: 25,
                    ..Probe::default()
                },
                shutdown_command: vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "printf shutdown > \"$1\"; printf '%s\\n' '{\"outcome\":\"not_running\"}'"
                        .to_owned(),
                    "ahrb-shutdown".to_owned(),
                    shutdown_marker.to_string_lossy().into_owned(),
                ],
                shutdown_result: ShutdownResult {
                    outcome_pointer: "/outcome".to_owned(),
                    clean_outcomes: vec!["not_running".to_owned()],
                    escalate_outcomes: vec!["did_not_stop".to_owned()],
                },
                grace: Duration::from_millis(250),
                log_directory: logs.clone(),
            },
        );
        let error = transport
            .start()
            .await
            .expect_err("readiness timeout must fail detached startup");
        assert!(error.to_string().contains("daemon readiness"));
        assert_eq!(
            std::fs::read_to_string(&shutdown_marker).expect("typed shutdown marker"),
            "shutdown"
        );
        std::fs::remove_dir_all(logs).expect("remove detached-start logs");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn detached_startup_escalation_without_reacquired_pid_fails_closed() {
        let logs = std::env::temp_dir().join(format!(
            "ahrb-detached-no-cleanup-pid-{}-{}",
            std::process::id(),
            DAEMON_LOG_SEQUENCE.load(Ordering::Relaxed)
        ));
        if logs.exists() {
            std::fs::remove_dir_all(&logs).expect("remove stale escalation logs");
        }
        let mut transport = ManagedDaemonTransport::new(
            NoopTransport,
            ManagedDaemonConfig {
                command: vec!["/usr/bin/true".to_owned()],
                launcher_exits: true,
                initialize_command: Vec::new(),
                initialize_marker: logs.join("initialized"),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: logs.join("never-ready").to_string_lossy().into_owned(),
                    timeout_ms: 25,
                    ..Probe::default()
                },
                shutdown_command: vec![
                    "/usr/bin/printf".to_owned(),
                    r#"{"outcome":"did_not_stop"}"#.to_owned(),
                ],
                shutdown_result: ShutdownResult {
                    outcome_pointer: "/outcome".to_owned(),
                    clean_outcomes: vec!["not_running".to_owned()],
                    escalate_outcomes: vec!["did_not_stop".to_owned()],
                },
                grace: Duration::from_millis(250),
                log_directory: logs.clone(),
            },
        );
        let error = transport
            .start()
            .await
            .expect_err("missing cleanup PID must fail closed");
        assert!(
            error
                .to_string()
                .contains("containment-validated owned PID")
        );
        std::fs::remove_dir_all(logs).expect("remove escalation logs");
    }

    #[test]
    fn haider_replay_document_reuses_live_rules_and_cursor_origin() {
        let manifest = crate::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))
            .expect("load Haider manifest");
        let live_records = vec![
            json!({
                "schema_version":1,
                "event_id":"evt-thinking","seq":8,"session_id":"session-1","run_id":"run-1",
                "payload":{"type":"run_state","state":"thinking"}
            }),
            json!({
                "schema_version":1,
                "event_id":"evt-tool-call","seq":16,"session_id":"session-1","run_id":"run-1",
                "payload":{"type":"item","event":"started","item":{
                    "item":"tool_call","call_id":"provider-call-1","name":"process_exec",
                    "args":{"command":["true"]},"status":"in_progress"
                }}
            }),
            json!({
                "schema_version":1,
                "event_id":"evt-tool-result","seq":17,"session_id":"session-1","run_id":"run-1",
                "payload":{"type":"tool_result","call_id":"provider-call-1","result":{"ok":true}}
            }),
            json!({
                "schema_version":1,
                "event_id":"evt-done","seq":23,"session_id":"session-1","run_id":"run-1",
                "payload":{"type":"run_state","state":"done","terminal_kind":"success"}
            }),
        ];
        let document = json!({
            "schema":"haider.run.replay.v1",
            "mode":"durable_journal",
            "provider_requests":0,
            "events":live_records,
            "integrity":{
                "sequences_strictly_increasing":true,
                "run_id_stable":true,
                "exactly_one_typed_terminal":true,
                "terminal_seq_matches_status":true
            },
            "equivalence":{"equivalent":true}
        });
        let mut bytes = serde_json::to_vec(&document).expect("serialize replay document");
        bytes.push(b'\n');
        let records = extract_replay_records(&manifest.events, &bytes)
            .expect("extract replay document events");
        assert_eq!(records, live_records);
        assert_eq!(
            records
                .iter()
                .filter_map(|record| record.get("seq").and_then(Value::as_u64))
                .collect::<Vec<_>>(),
            [8, 16, 17, 23]
        );
        assert_eq!(
            records
                .iter()
                .filter_map(|record| record.pointer("/payload/call_id").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            ["provider-call-1"]
        );
        let mut session = PersistedExecSession {
            local_id: "local-1".to_owned(),
            marker: "r30".to_owned(),
            harness_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            turns: 1,
            invocations: 1,
            next_cursor: manifest.events.replay_cursor_start.unwrap_or(1),
            closed: false,
        };
        let normalized = PerInvocationDriver::normalize_records(
            &manifest.events,
            &mut session,
            &records,
            &BTreeMap::new(),
            "turn-1",
            true,
        )
        .expect("normalize replayed Haider envelopes");
        assert_eq!(normalized.len(), 4);
        assert_eq!(normalized[0].cursor, 2);
        assert_eq!(normalized[0].event, EventVocab::ModelRequest);
        assert_eq!(normalized[1].event, EventVocab::ToolCall);
        assert_eq!(
            normalized[1].payload.get("call_id").and_then(Value::as_str),
            Some("provider-call-1")
        );
        assert_eq!(normalized[2].event, EventVocab::ToolResult);
        assert_eq!(
            normalized[2].payload.get("call_id").and_then(Value::as_str),
            Some("provider-call-1")
        );
        assert_eq!(normalized[3].cursor, 5);
        assert_eq!(normalized[3].event, EventVocab::TerminalSuccess);
    }

    #[test]
    fn typed_shutdown_outcome_selects_clean_or_escalation() {
        let contract = ShutdownResult {
            outcome_pointer: "/outcome".to_owned(),
            clean_outcomes: vec!["stopped_cleanly".to_owned(), "not_running".to_owned()],
            escalate_outcomes: vec!["did_not_stop".to_owned()],
        };
        assert_eq!(
            shutdown_disposition(&contract, &json!({"outcome":"stopped_cleanly"}))
                .expect("clean outcome"),
            ShutdownDisposition::Clean
        );
        assert_eq!(
            shutdown_disposition(&contract, &json!({"outcome":"did_not_stop"}))
                .expect("escalation outcome"),
            ShutdownDisposition::Escalate
        );
        assert!(shutdown_disposition(&contract, &json!({"outcome":"mystery"})).is_err());
    }

    #[tokio::test]
    async fn typed_shutdown_escalation_still_runs_transport_sweep_and_records_note() {
        let transport = TypedEscalationTransport { stopped: false };
        let mut driver = GenericDriver::new(transport);
        driver.operations.shutdown_result = ShutdownResult {
            outcome_pointer: "/outcome".to_owned(),
            clean_outcomes: vec!["stopped_cleanly".to_owned()],
            escalate_outcomes: vec!["did_not_stop".to_owned()],
        };
        driver.shutdown().await.expect("owned-tree sweep succeeds");
        assert!(driver.transport.stopped);
        assert!(
            driver
                .lifecycle_notes()
                .iter()
                .any(|note| note.contains("owned-tree escalation"))
        );
    }

    #[tokio::test]
    async fn command_json_readiness_rejects_paths_outside_isolation() {
        let probe = Probe {
            kind: "command-json".to_owned(),
            target: String::new(),
            command: vec![
                "/usr/bin/printf".to_owned(),
                r#"{"runtime_dir":"/tmp/ambient/haider"}"#.to_owned(),
            ],
            json_pointer_roots: BTreeMap::from([("/runtime_dir".to_owned(), "TMPDIR".to_owned())]),
            timeout_ms: 1_000,
            ..Probe::default()
        };
        let environment =
            BTreeMap::from([("TMPDIR".to_owned(), "/tmp/isolated-profile/tmp".to_owned())]);
        let error = managed_daemon_readiness_satisfied(&probe, &environment)
            .await
            .expect_err("ambient runtime path must be rejected");
        assert!(error.to_string().contains("outside isolated TMPDIR"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn command_json_readiness_accepts_private_tmp_alias_inside_tmp_root() {
        let root = PathBuf::from(format!("/tmp/ahrb-readiness-alias-{}", std::process::id()));
        let returned = PathBuf::from(format!(
            "/private/tmp/ahrb-readiness-alias-{}/pipe",
            std::process::id()
        ));
        std::fs::create_dir_all(&returned).expect("create readiness path through /private/tmp");
        let probe = Probe {
            kind: "command-json".to_owned(),
            target: String::new(),
            command: vec![
                "/usr/bin/printf".to_owned(),
                json!({"pipe_dir": returned}).to_string(),
            ],
            json_pointer_roots: BTreeMap::from([("/pipe_dir".to_owned(), "TMPDIR".to_owned())]),
            timeout_ms: 1_000,
            ..Probe::default()
        };
        let environment =
            BTreeMap::from([("TMPDIR".to_owned(), root.to_string_lossy().into_owned())]);

        let ready = managed_daemon_readiness_satisfied(&probe, &environment)
            .await
            .expect("/private/tmp path must be contained by equivalent /tmp root");
        assert!(ready.is_some());
        std::fs::remove_dir_all(root).expect("remove readiness alias root");
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
                launcher_exits: false,
                initialize_command: Vec::new(),
                initialize_marker: logs.join("initialized"),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: logs.join("never-ready").to_string_lossy().into_owned(),
                    command: Vec::new(),
                    json_pointer_roots: BTreeMap::new(),
                    timeout_ms: 1_000,
                    ..Probe::default()
                },
                shutdown_command: Vec::new(),
                shutdown_result: ShutdownResult::default(),
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
                launcher_exits: false,
                initialize_command: Vec::new(),
                initialize_marker: logs.join("initialized"),
                environment: BTreeMap::new(),
                readiness: Probe {
                    kind: "file".to_owned(),
                    target: worker_file.to_string_lossy().into_owned(),
                    command: Vec::new(),
                    json_pointer_roots: BTreeMap::new(),
                    timeout_ms: 1_000,
                    ..Probe::default()
                },
                shutdown_command: Vec::new(),
                shutdown_result: ShutdownResult::default(),
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
