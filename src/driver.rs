//! Harness operations over exec, stdin-RPC, socket JSON-RPC, or HTTP transports.
//!
//! All transports use the same small JSON-RPC-like envelope.  The envelope keeps the
//! semantic driver independent from process and wire framing, while the monotonically
//! increasing request ID makes recordings deterministic.

use crate::events::NormalizedEvent;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
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
            return Err(AhrbError::Validation(
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
            let _ = self.call(&operation, json!({})).await;
            self.transport.stop().await
        })
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
}
