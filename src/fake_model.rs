//! Marker-routed deterministic fake-model engine and local HTTP protocol frontends.

use crate::workflow::{BarrierCoordinator, Fault, RouteMarker, Workflow, WorkflowMachine};
use crate::{AhrbError, Result};
use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

/// Maximum accepted fake-model request body. This keeps malformed peers bounded.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

// The managed source-build sandbox permits local listeners but may reject concurrent
// binds. Serialize listener-owning unit tests; production servers are unaffected.
#[cfg(test)]
pub(crate) static LOCAL_SERVER_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// A protocol-independent canonical model request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelRequest {
    /// HTTP protocol dialect.
    pub dialect: String,
    /// Validated endpoint path the peer addressed (e.g. `/v1/chat/completions`).
    pub endpoint: String,
    /// Requested model ID.
    pub model: String,
    /// Scenario route marker.
    pub scenario: String,
    /// Actor route marker.
    pub actor: String,
    /// Checkpoint derived from canonical conversation state.
    pub checkpoint: String,
    /// Canonical semantic request JSON with recursively sorted object keys.
    pub canonical: Value,
    /// Supplied credential fingerprint, never the secret itself.
    pub credential_fingerprint: String,
    /// Whether the peer requested a streaming response.
    pub stream: bool,
}

impl ModelRequest {
    /// Return the request route as a validated marker.
    pub fn marker(&self) -> Result<RouteMarker> {
        RouteMarker::new(&self.scenario, &self.actor, &self.checkpoint)
            .map_err(|error| AhrbError::Protocol(error.to_string()))
    }

    /// Compute the stable hash used for transition validation and retry identity.
    pub fn canonical_hash(&self) -> Result<String> {
        canonical_request_hash(&self.canonical)
    }
}

/// A protocol-independent response selected by the workflow engine.
#[derive(Clone, Debug)]
pub struct ModelResponse {
    /// Model ID supplied by the request.
    pub model: String,
    /// Scenario route marker.
    pub scenario: String,
    /// Actor route marker.
    pub actor: String,
    /// Accepted checkpoint.
    pub checkpoint: String,
    /// Canonical request hash.
    pub request_hash: String,
    /// Deterministic semantic response.
    pub value: Value,
    /// Optional transport fault.
    pub fault: Option<Fault>,
    /// Whether the request was an identical retry.
    pub retry: bool,
    /// Whether the peer requested streaming.
    pub stream: bool,
}

/// Deterministically rendered response bytes and headers.
#[derive(Clone, Debug)]
pub struct RenderedResponse {
    /// HTTP status code.
    pub status: u16,
    /// Stable response headers.
    pub headers: BTreeMap<String, String>,
    /// Exact response bytes before transport fault injection.
    pub body: Vec<u8>,
}

/// Transport-agnostic protocol adapter for parsing and rendering model traffic.
pub trait ProtocolFrontend: Send + Sync {
    /// Stable dialect name.
    fn dialect(&self) -> &'static str;
    /// Exact endpoint path accepted by this frontend.
    fn path(&self) -> &'static str;
    /// Parse request bytes into the shared canonical form.
    fn parse(
        &self,
        path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<ModelRequest>;
    /// Render a shared response to dialect-specific bytes.
    fn render(&self, response: &ModelResponse) -> Result<RenderedResponse>;
}

/// One stable request evidence record. Records are returned in route/hash order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelRequestRecord {
    /// Parsed canonical request.
    pub request: ModelRequest,
    /// SHA-256 of canonical semantic JSON.
    pub canonical_hash: String,
    /// Number of byte-equivalent attempts observed for this semantic request.
    pub attempts: u64,
    /// Whether the workflow state machine accepted the request.
    pub accepted: bool,
}

type RequestRecordKey = (String, String, String, String, String);

/// State for deterministic transition validation, barriers, and idempotent retries.
#[derive(Debug)]
pub struct FakeModelEngine {
    machine: WorkflowMachine,
    barriers: BarrierCoordinator,
    requests: Mutex<BTreeMap<RequestRecordKey, ModelRequestRecord>>,
}

impl FakeModelEngine {
    /// Build an engine from one validated declarative workflow.
    pub fn new(workflow: &Workflow) -> Result<Self> {
        Ok(Self {
            machine: WorkflowMachine::new(workflow)?,
            barriers: BarrierCoordinator::new(workflow)?,
            requests: Mutex::new(BTreeMap::new()),
        })
    }

    /// Validate, route, and await any named barrier for a canonical request.
    pub async fn handle(&self, request: ModelRequest) -> Result<ModelResponse> {
        let marker = request.marker()?;
        let request_hash = request.canonical_hash()?;
        let record_key = (
            request.scenario.clone(),
            request.actor.clone(),
            request.checkpoint.clone(),
            request_hash.clone(),
            request.dialect.clone(),
        );
        {
            let mut records = self.requests.lock().await;
            let record = records
                .entry(record_key.clone())
                .or_insert_with(|| ModelRequestRecord {
                    request: request.clone(),
                    canonical_hash: request_hash.clone(),
                    attempts: 0,
                    accepted: false,
                });
            if record.request != request {
                return Err(AhrbError::Protocol(
                    "request evidence key collision with different request".to_owned(),
                ));
            }
            record.attempts = record.attempts.checked_add(1).ok_or_else(|| {
                AhrbError::Protocol("request attempt counter overflow".to_owned())
            })?;
        }

        let accepted = self.machine.accept(&marker, &request_hash).await?;
        {
            let mut records = self.requests.lock().await;
            let Some(record) = records.get_mut(&record_key) else {
                return Err(AhrbError::Protocol(
                    "request evidence disappeared during transition".to_owned(),
                ));
            };
            record.accepted = true;
        }
        if let Some(barrier) = &accepted.response.barrier {
            self.barriers.arrive(barrier, &marker).await?;
            self.barriers.wait_for_release(barrier).await?;
        }

        Ok(ModelResponse {
            model: request.model,
            scenario: marker.scenario,
            actor: marker.actor,
            checkpoint: marker.checkpoint,
            request_hash,
            value: accepted.response.response,
            fault: accepted.response.fault,
            retry: accepted.retry,
            stream: request.stream,
        })
    }

    /// Return immutable access to state-based barrier coordination.
    pub fn barriers(&self) -> &BarrierCoordinator {
        &self.barriers
    }

    /// Return deterministic request evidence sorted independently of arrival order.
    pub async fn request_records(&self) -> Vec<ModelRequestRecord> {
        self.requests.lock().await.values().cloned().collect()
    }
}

/// OpenAI Chat Completions (`POST /v1/chat/completions`) frontend.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenAiChatFrontend;

impl ProtocolFrontend for OpenAiChatFrontend {
    fn dialect(&self) -> &'static str {
        "openai-chat-completions"
    }

    fn path(&self) -> &'static str {
        "/v1/chat/completions"
    }

    fn parse(
        &self,
        path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<ModelRequest> {
        parse_request(self.dialect(), self.path(), path, headers, body)
    }

    fn render(&self, response: &ModelResponse) -> Result<RenderedResponse> {
        let value = render_chat_value(response)?;
        render_json_or_sse(value, response.stream, "chat")
    }
}

/// OpenAI Responses (`POST /v1/responses`) frontend.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenAiResponsesFrontend;

impl ProtocolFrontend for OpenAiResponsesFrontend {
    fn dialect(&self) -> &'static str {
        "openai-responses"
    }

    fn path(&self) -> &'static str {
        "/v1/responses"
    }

    fn parse(
        &self,
        path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<ModelRequest> {
        parse_request(self.dialect(), self.path(), path, headers, body)
    }

    fn render(&self, response: &ModelResponse) -> Result<RenderedResponse> {
        let value = render_responses_value(response)?;
        render_json_or_sse(value, response.stream, "responses")
    }
}

/// Anthropic Messages (`POST /v1/messages`) frontend.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnthropicMessagesFrontend;

impl ProtocolFrontend for AnthropicMessagesFrontend {
    fn dialect(&self) -> &'static str {
        "anthropic-messages"
    }

    fn path(&self) -> &'static str {
        "/v1/messages"
    }

    fn parse(
        &self,
        path: &str,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<ModelRequest> {
        parse_request(self.dialect(), self.path(), path, headers, body)
    }

    fn render(&self, response: &ModelResponse) -> Result<RenderedResponse> {
        let value = render_anthropic_value(response)?;
        render_json_or_sse(value, response.stream, "anthropic")
    }
}

/// A running local fake-model HTTP server.
#[derive(Debug)]
pub struct FakeModelServer {
    local_addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    engine: Arc<FakeModelEngine>,
}

impl FakeModelServer {
    /// Bind a loopback/local address and start serving all built-in protocol frontends.
    pub async fn bind(addr: SocketAddr, engine: Arc<FakeModelEngine>) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let task_engine = Arc::clone(&engine);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let connection_engine = Arc::clone(&task_engine);
                        tokio::spawn(async move {
                            let service = service_fn(move |request| {
                                serve_request(request, Arc::clone(&connection_engine))
                            });
                            let result = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                            if result.is_err() {
                                // Mid-stream disconnect faults intentionally end a connection.
                            }
                        });
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            local_addr,
            shutdown: Some(shutdown_sender),
            task,
            engine,
        })
    }

    /// Bound socket address. Port zero bindings resolve to their allocated port here.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Loopback base URL suitable for adapter environment binding.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.local_addr)
    }

    /// Shared engine, including barrier controls and request evidence.
    pub fn engine(&self) -> &Arc<FakeModelEngine> {
        &self.engine
    }

    /// Gracefully stop accepting connections and wait for the listener task.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(sender) = self.shutdown.take() {
            let _sent = sender.send(());
        }
        self.task
            .await
            .map_err(|error| AhrbError::Protocol(format!("fake-model server task: {error}")))?
    }
}

/// A running fake-model HTTP server carried over a Unix-domain socket.
///
/// This is protocol-identical to [`FakeModelServer`]. It exists for restricted local
/// environments that prohibit `bind(2)` on even loopback TCP sockets.
#[cfg(unix)]
#[derive(Debug)]
pub struct FakeModelUnixServer {
    socket_path: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    engine: Arc<FakeModelEngine>,
}

#[cfg(unix)]
impl FakeModelUnixServer {
    /// Bind a new Unix socket and serve all built-in HTTP protocol frontends.
    ///
    /// The path must not already exist; the server never removes an unknown existing
    /// filesystem entry. Callers should place it in a fresh run-local directory.
    pub async fn bind(path: impl AsRef<Path>, engine: Arc<FakeModelEngine>) -> Result<Self> {
        let socket_path = path.as_ref().to_path_buf();
        if socket_path.as_os_str().is_empty() {
            return Err(AhrbError::Validation(
                "fake-model Unix socket path is empty".to_owned(),
            ));
        }
        if socket_path.exists() {
            return Err(AhrbError::Validation(format!(
                "fake-model Unix socket path already exists: {}",
                socket_path.display()
            )));
        }
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let listener = UnixListener::bind(&socket_path)?;
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let task_engine = Arc::clone(&engine);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let connection_engine = Arc::clone(&task_engine);
                        tokio::spawn(async move {
                            let service = service_fn(move |request| {
                                serve_request(request, Arc::clone(&connection_engine))
                            });
                            let result = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                            if result.is_err() {
                                // Scripted disconnect faults intentionally end a connection.
                            }
                        });
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            socket_path,
            shutdown: Some(shutdown_sender),
            task,
            engine,
        })
    }

    /// Filesystem path clients use for `connect(2)`.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Shared engine, including barrier controls and request evidence.
    pub fn engine(&self) -> &Arc<FakeModelEngine> {
        &self.engine
    }

    /// Stop accepting connections, wait for the task, and remove the socket entry.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(sender) = self.shutdown.take() {
            let _sent = sender.send(());
        }
        self.task
            .await
            .map_err(|error| AhrbError::Protocol(format!("fake-model server task: {error}")))??;
        match tokio::fs::remove_file(&self.socket_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

async fn serve_request(
    request: Request<Incoming>,
    engine: Arc<FakeModelEngine>,
) -> std::result::Result<Response<DeterministicBody>, Infallible> {
    let response = match handle_http(request, engine).await {
        Ok(response) => response,
        Err(error) => diagnostic_response(&error),
    };
    Ok(response)
}

async fn handle_http(
    request: Request<Incoming>,
    engine: Arc<FakeModelEngine>,
) -> Result<Response<DeterministicBody>> {
    let path = request.uri().path().to_owned();
    if request.method() == Method::GET && path == "/healthz" {
        return response_from_parts(
            200,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::from_static(b"{\"status\":\"ok\"}")),
        );
    }
    if request.method() != Method::POST {
        return Err(AhrbError::Protocol(format!(
            "fake model only accepts POST for {path:?}"
        )));
    }

    let frontend: &dyn ProtocolFrontend = match path.as_str() {
        "/v1/chat/completions" => &OpenAiChatFrontend,
        "/v1/responses" => &OpenAiResponsesFrontend,
        "/v1/messages" => &AnthropicMessagesFrontend,
        _ => {
            return Err(AhrbError::Protocol(format!(
                "unexpected fake-model path {path:?}"
            )));
        }
    };
    let headers = canonical_headers(request.headers())?;
    let body = collect_bounded(request.into_body()).await?;
    let parsed = frontend.parse(&path, &headers, &body)?;
    let selected = engine.handle(parsed).await?;
    if let Some(Fault::HttpStatus { status, body }) = &selected.fault {
        return response_from_parts(
            *status,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::copy_from_slice(body.as_bytes())),
        );
    }

    let rendered = frontend.render(&selected)?;
    let response_body = DeterministicBody::from_fault(rendered.body, selected.fault.as_ref())?;
    response_from_parts(rendered.status, &rendered.headers, response_body)
}

async fn collect_bounded(mut body: Incoming) -> Result<Vec<u8>> {
    use http_body_util::BodyExt;

    let mut output = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            AhrbError::Protocol(format!("could not read fake-model request body: {error}"))
        })?;
        if let Ok(data) = frame.into_data() {
            let next_len = output.len().checked_add(data.len()).ok_or_else(|| {
                AhrbError::Protocol("fake-model request size overflow".to_owned())
            })?;
            if next_len > MAX_REQUEST_BYTES {
                return Err(AhrbError::Protocol(format!(
                    "fake-model request exceeds {MAX_REQUEST_BYTES} bytes"
                )));
            }
            output.extend_from_slice(&data);
        }
    }
    Ok(output)
}

fn parse_request(
    dialect: &str,
    expected_path: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> Result<ModelRequest> {
    if path != expected_path {
        return Err(AhrbError::Protocol(format!(
            "{dialect} frontend received path {path:?}; expected {expected_path:?}"
        )));
    }
    let raw: Value = serde_json::from_slice(body)?;
    let object = raw.as_object().ok_or_else(|| {
        AhrbError::Protocol("fake-model request body must be a JSON object".to_owned())
    })?;
    let model = required_string(object, "model")?.to_owned();
    let header_marker = marker_from_headers(headers)?;
    let metadata_marker = marker_from_metadata(object)?;
    let text_marker = marker_from_text(&raw)?;
    let marker = header_marker
        .or(metadata_marker)
        .or(text_marker)
        .ok_or_else(|| {
            AhrbError::Protocol(
                "request lacks x-ahrb-* headers, metadata marker, or prompt marker".to_owned(),
            )
        })?;
    let canonical = canonicalize_json(&raw);
    Ok(ModelRequest {
        dialect: dialect.to_owned(),
        endpoint: expected_path.to_owned(),
        model,
        scenario: marker.scenario,
        actor: marker.actor,
        checkpoint: marker.checkpoint,
        canonical,
        credential_fingerprint: credential_fingerprint(headers),
        stream: object
            .get("stream")
            .and_then(Value::as_bool)
            .is_some_and(|stream| stream),
    })
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    object.get(key).and_then(Value::as_str).ok_or_else(|| {
        AhrbError::Protocol(format!("fake-model request lacks string field {key:?}"))
    })
}

fn marker_from_headers(headers: &BTreeMap<String, String>) -> Result<Option<RouteMarker>> {
    let values = (
        headers.get("x-ahrb-scenario"),
        headers.get("x-ahrb-actor"),
        headers.get("x-ahrb-checkpoint"),
    );
    match values {
        (None, None, None) => Ok(None),
        (Some(scenario), Some(actor), Some(checkpoint)) => {
            RouteMarker::new(scenario, actor, checkpoint)
                .map(Some)
                .map_err(|error| AhrbError::Protocol(error.to_string()))
        }
        _ => Err(AhrbError::Protocol(
            "x-ahrb-scenario, x-ahrb-actor, and x-ahrb-checkpoint must be supplied together"
                .to_owned(),
        )),
    }
}

fn marker_from_metadata(object: &Map<String, Value>) -> Result<Option<RouteMarker>> {
    let Some(metadata) = object.get("metadata").and_then(Value::as_object) else {
        return Ok(None);
    };
    let nested = metadata.get("ahrb").and_then(Value::as_object);
    let source = match nested {
        Some(source) => source,
        None => metadata,
    };
    let prefix = if nested.is_some() { "" } else { "ahrb_" };
    let scenario = source
        .get(&format!("{prefix}scenario"))
        .and_then(Value::as_str);
    let actor = source
        .get(&format!("{prefix}actor"))
        .and_then(Value::as_str);
    let checkpoint = source
        .get(&format!("{prefix}checkpoint"))
        .and_then(Value::as_str);
    match (scenario, actor, checkpoint) {
        (None, None, None) => Ok(None),
        (Some(scenario), Some(actor), Some(checkpoint)) => {
            RouteMarker::new(scenario, actor, checkpoint)
                .map(Some)
                .map_err(|error| AhrbError::Protocol(error.to_string()))
        }
        _ => Err(AhrbError::Protocol(
            "AHRB metadata route must include scenario, actor, and checkpoint".to_owned(),
        )),
    }
}

fn marker_from_text(value: &Value) -> Result<Option<RouteMarker>> {
    fn walk(value: &Value, last: &mut Option<RouteMarker>) -> Result<()> {
        match value {
            Value::String(text) => {
                if let Some(marker) = RouteMarker::extract(text)? {
                    *last = Some(marker);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, last)?;
                }
            }
            Value::Object(object) => {
                for (key, item) in object {
                    if key != "metadata" {
                        walk(item, last)?;
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
        Ok(())
    }

    let mut last = None;
    walk(value, &mut last)?;
    Ok(last)
}

fn canonical_headers(headers: &hyper::HeaderMap) -> Result<BTreeMap<String, String>> {
    let mut canonical = BTreeMap::new();
    for (name, value) in headers {
        let value = value.to_str().map_err(|_| {
            AhrbError::Protocol(format!("header {:?} is not visible ASCII", name.as_str()))
        })?;
        canonical.insert(name.as_str().to_ascii_lowercase(), value.to_owned());
    }
    Ok(canonical)
}

fn credential_fingerprint(headers: &BTreeMap<String, String>) -> String {
    let credential = headers
        .get("authorization")
        .or_else(|| headers.get("x-api-key"))
        .map(String::as_str);
    let credential = credential.map_or("", |credential| credential);
    if credential.is_empty() {
        "absent".to_owned()
    } else {
        sha256_hex(credential.as_bytes())
    }
}

/// Recursively sort JSON object keys without changing array order or scalar values.
pub fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect();
            let mut canonical = Map::new();
            for (key, value) in sorted {
                canonical.insert(key, value);
            }
            Value::Object(canonical)
        }
        _ => value.clone(),
    }
}

/// SHA-256 a canonical JSON request into lowercase hexadecimal.
pub fn canonical_request_hash(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(&canonicalize_json(value))?;
    Ok(sha256_hex(&bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn render_chat_value(response: &ModelResponse) -> Result<Value> {
    if response.value.get("choices").is_some() {
        return Ok(response.value.clone());
    }
    let (text, tool_calls) = semantic_parts(&response.value)?;
    let mut message = Map::new();
    message.insert("role".to_owned(), Value::String("assistant".to_owned()));
    message.insert(
        "content".to_owned(),
        match text {
            Some(text) => Value::String(text),
            None => Value::Null,
        },
    );
    if !tool_calls.is_empty() {
        message.insert(
            "tool_calls".to_owned(),
            Value::Array(
                tool_calls
                    .iter()
                    .enumerate()
                    .map(|(index, call)| chat_tool_call(call, index, response))
                    .collect::<Result<Vec<_>>>()?,
            ),
        );
    }
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    Ok(json!({
        "id": stable_id("chatcmpl", response),
        "object": "chat.completion",
        "created": 1_700_000_000_u64,
        "model": response.model,
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": finish_reason
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    }))
}

fn chat_tool_call(call: &Value, index: usize, response: &ModelResponse) -> Result<Value> {
    if call.get("function").is_some() {
        return Ok(call.clone());
    }
    let object = call.as_object().ok_or_else(|| {
        AhrbError::Protocol("semantic tool_calls entries must be objects".to_owned())
    })?;
    let name = required_string(object, "name")?;
    let id = match object.get("id").and_then(Value::as_str) {
        Some(id) => id.to_owned(),
        None => format!("{}_{}", stable_id("call", response), index),
    };
    let arguments = match object.get("arguments") {
        Some(Value::String(arguments)) => arguments.clone(),
        Some(arguments) => serde_json::to_string(&canonicalize_json(arguments))?,
        None => "{}".to_owned(),
    };
    Ok(json!({
        "id": id,
        "type": "function",
        "function": {"name": name, "arguments": arguments}
    }))
}

fn render_responses_value(response: &ModelResponse) -> Result<Value> {
    if response.value.get("object").and_then(Value::as_str) == Some("response") {
        return Ok(response.value.clone());
    }
    let (text, tool_calls) = semantic_parts(&response.value)?;
    let mut output = Vec::new();
    if let Some(text) = text {
        output.push(json!({
            "id": stable_id("msg", response),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}]
        }));
    }
    for (index, call) in tool_calls.iter().enumerate() {
        let object = call.as_object().ok_or_else(|| {
            AhrbError::Protocol("semantic tool_calls entries must be objects".to_owned())
        })?;
        let name = required_string(object, "name")?;
        let call_id = match object.get("id").and_then(Value::as_str) {
            Some(id) => id.to_owned(),
            None => format!("{}_{}", stable_id("call", response), index),
        };
        let arguments = match object.get("arguments") {
            Some(Value::String(arguments)) => arguments.clone(),
            Some(arguments) => serde_json::to_string(&canonicalize_json(arguments))?,
            None => "{}".to_owned(),
        };
        output.push(json!({
            "type": "function_call",
            "id": format!("{}_{}", stable_id("fc", response), index),
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
            "status": "completed"
        }));
    }
    Ok(json!({
        "id": stable_id("resp", response),
        "object": "response",
        "created_at": 1_700_000_000_u64,
        "status": "completed",
        "model": response.model,
        "output": output,
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    }))
}

fn render_anthropic_value(response: &ModelResponse) -> Result<Value> {
    if response.value.get("type").and_then(Value::as_str) == Some("message")
        && response.value.get("content").is_some()
    {
        return Ok(response.value.clone());
    }
    let (text, tool_calls) = semantic_parts(&response.value)?;
    let mut content = Vec::new();
    if let Some(text) = text {
        content.push(json!({"type": "text", "text": text}));
    }
    for (index, call) in tool_calls.iter().enumerate() {
        let object = call.as_object().ok_or_else(|| {
            AhrbError::Protocol("semantic tool_calls entries must be objects".to_owned())
        })?;
        let name = required_string(object, "name")?;
        let id = match object.get("id").and_then(Value::as_str) {
            Some(id) => id.to_owned(),
            None => format!("{}_{}", stable_id("toolu", response), index),
        };
        let input = match object.get("arguments") {
            Some(Value::String(arguments)) => serde_json::from_str(arguments).map_err(|error| {
                AhrbError::Protocol(format!("Anthropic tool input is invalid JSON: {error}"))
            })?,
            Some(arguments) => canonicalize_json(arguments),
            None => json!({}),
        };
        content.push(json!({"type": "tool_use", "id": id, "name": name, "input": input}));
    }
    Ok(json!({
        "id": stable_id("msg", response),
        "type": "message",
        "role": "assistant",
        "model": response.model,
        "content": content,
        "stop_reason": if tool_calls.is_empty() { "end_turn" } else { "tool_use" },
        "stop_sequence": null,
        "usage": {"input_tokens": 0, "output_tokens": 0}
    }))
}

fn semantic_parts(value: &Value) -> Result<(Option<String>, Vec<Value>)> {
    match value {
        Value::String(text) => Ok((Some(text.clone()), Vec::new())),
        Value::Object(object) => {
            let text = object
                .get("text")
                .or_else(|| object.get("content"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let tool_calls = object
                .get("tool_calls")
                .map(|calls| {
                    calls.as_array().cloned().ok_or_else(|| {
                        AhrbError::Protocol("semantic tool_calls must be an array".to_owned())
                    })
                })
                .transpose()?;
            let tool_calls = tool_calls.into_iter().flatten().collect();
            Ok((text, tool_calls))
        }
        _ => Err(AhrbError::Protocol(
            "semantic response must be a string or object".to_owned(),
        )),
    }
}

fn stable_id(prefix: &str, response: &ModelResponse) -> String {
    let material = format!(
        "{}\0{}\0{}\0{}\0{}",
        response.scenario, response.actor, response.checkpoint, response.request_hash, prefix
    );
    let digest = sha256_hex(material.as_bytes());
    format!("{prefix}_{}", &digest[..24])
}

fn render_json_or_sse(value: Value, stream: bool, dialect: &str) -> Result<RenderedResponse> {
    let mut headers = BTreeMap::new();
    let body = if stream {
        headers.insert("cache-control".to_owned(), "no-cache".to_owned());
        headers.insert("content-type".to_owned(), "text/event-stream".to_owned());
        let serialized = serde_json::to_string(&value)?;
        match dialect {
            "chat" => format!("data: {serialized}\n\ndata: [DONE]\n\n").into_bytes(),
            "responses" => format!(
                "event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{serialized}}}\n\n"
            )
            .into_bytes(),
            "anthropic" => format!(
                "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{serialized}}}\n\nevent: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
            )
            .into_bytes(),
            _ => {
                return Err(AhrbError::Protocol(format!(
                    "unknown streaming dialect {dialect:?}"
                )));
            }
        }
    } else {
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        serde_json::to_vec(&value)?
    };
    Ok(RenderedResponse {
        status: 200,
        headers,
        body,
    })
}

fn response_from_parts(
    status: u16,
    headers: &BTreeMap<String, String>,
    body: DeterministicBody,
) -> Result<Response<DeterministicBody>> {
    let status = StatusCode::from_u16(status)
        .map_err(|error| AhrbError::Protocol(format!("invalid response status: {error}")))?;
    let mut response = Response::new(body);
    *response.status_mut() = status;
    for (name, value) in headers {
        let name = hyper::header::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            AhrbError::Protocol(format!("invalid response header name {name:?}: {error}"))
        })?;
        let value = hyper::header::HeaderValue::from_str(value).map_err(|error| {
            AhrbError::Protocol(format!("invalid response header value: {error}"))
        })?;
        response.headers_mut().insert(name, value);
    }
    Ok(response)
}

fn diagnostic_response(error: &AhrbError) -> Response<DeterministicBody> {
    let serialized = serde_json::to_vec(&json!({
        "error": {"type": "ahrb_infrastructure_diagnostic", "message": error.to_string()}
    }));
    let body = match serialized {
        Ok(body) => body,
        Err(_) => b"{\"error\":{\"type\":\"ahrb_infrastructure_diagnostic\"}}".to_vec(),
    };
    let mut response = Response::new(DeterministicBody::full(Bytes::from(body)));
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response
}

#[derive(Debug)]
enum BodyState {
    Frames(VecDeque<Bytes>),
    Disconnect {
        prefix: Option<Bytes>,
        emitted_error: bool,
    },
    Stall,
}

/// HTTP body supporting deterministic fragmentation, disconnect, repetition, and stall.
#[derive(Debug)]
struct DeterministicBody {
    state: BodyState,
}

impl DeterministicBody {
    fn full(bytes: Bytes) -> Self {
        Self {
            state: BodyState::Frames(VecDeque::from([bytes])),
        }
    }

    fn from_fault(bytes: Vec<u8>, fault: Option<&Fault>) -> Result<Self> {
        match fault {
            None | Some(Fault::HttpStatus { .. }) => Ok(Self::full(Bytes::from(bytes))),
            Some(Fault::Stall) => Ok(Self {
                state: BodyState::Stall,
            }),
            Some(Fault::MidStreamDisconnect { after_bytes }) => {
                if *after_bytes > bytes.len() {
                    return Err(AhrbError::Protocol(format!(
                        "disconnect offset {after_bytes} exceeds response length {}",
                        bytes.len()
                    )));
                }
                Ok(Self {
                    state: BodyState::Disconnect {
                        prefix: (*after_bytes > 0)
                            .then(|| Bytes::copy_from_slice(&bytes[..*after_bytes])),
                        emitted_error: false,
                    },
                })
            }
            Some(Fault::Fragment { boundaries }) => {
                let mut start = 0;
                let mut frames = VecDeque::new();
                for boundary in boundaries {
                    if *boundary <= start || *boundary >= bytes.len() {
                        return Err(AhrbError::Protocol(format!(
                            "fragment boundary {boundary} is outside response body of {} bytes",
                            bytes.len()
                        )));
                    }
                    frames.push_back(Bytes::copy_from_slice(&bytes[start..*boundary]));
                    start = *boundary;
                }
                frames.push_back(Bytes::copy_from_slice(&bytes[start..]));
                Ok(Self {
                    state: BodyState::Frames(frames),
                })
            }
            Some(Fault::RepeatFrame { copies }) => {
                if *copies == 0 {
                    return Err(AhrbError::Protocol(
                        "repeat-frame copies must be nonzero".to_owned(),
                    ));
                }
                let bytes = Bytes::from(bytes);
                let frames = (0..*copies).map(|_| bytes.clone()).collect();
                Ok(Self {
                    state: BodyState::Frames(frames),
                })
            }
        }
    }
}

impl Body for DeterministicBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.state {
            BodyState::Frames(frames) => {
                Poll::Ready(frames.pop_front().map(|bytes| Ok(Frame::data(bytes))))
            }
            BodyState::Disconnect {
                prefix,
                emitted_error,
            } => {
                if let Some(bytes) = prefix.take() {
                    return Poll::Ready(Some(Ok(Frame::data(bytes))));
                }
                if !*emitted_error {
                    *emitted_error = true;
                    return Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "AHRB scripted mid-stream disconnect",
                    ))));
                }
                Poll::Ready(None)
            }
            BodyState::Stall => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.state, BodyState::Frames(frames) if frames.is_empty())
            || matches!(
                &self.state,
                BodyState::Disconnect {
                    prefix: None,
                    emitted_error: true
                }
            )
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        if let BodyState::Frames(frames) = &self.state {
            let total = frames
                .iter()
                .fold(0_u64, |sum, frame| sum.saturating_add(frame.len() as u64));
            hint.set_exact(total);
        }
        hint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{Actor, Barrier};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn simple_workflow() -> Workflow {
        Workflow {
            version: 1,
            scenario: "routing".to_owned(),
            actors: BTreeMap::from([(
                "root".to_owned(),
                Actor {
                    id: "root".to_owned(),
                    parent: None,
                    prompt: "route".to_owned(),
                    workspace: "root".to_owned(),
                },
            )]),
            barriers: BTreeMap::<String, Barrier>::new(),
            responses: vec![crate::workflow::ScriptedResponse {
                scenario: "routing".to_owned(),
                actor: "root".to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({"text": "SUCCESS"}),
                fault: None,
                barrier: None,
            }],
        }
    }

    fn request_body() -> Vec<u8> {
        br#"{"model":"ahrb-fake","messages":[{"role":"user","content":"go [[AHRB:scenario=routing;actor=root;checkpoint=start]]"}]}"#.to_vec()
    }

    #[tokio::test]
    async fn routes_by_marker_and_retries_idempotently() -> Result<()> {
        let engine = FakeModelEngine::new(&simple_workflow())?;
        let frontend = OpenAiChatFrontend;
        let request = frontend.parse(frontend.path(), &BTreeMap::new(), &request_body())?;
        let first = engine.handle(request.clone()).await?;
        let retry = engine.handle(request).await?;
        assert!(!first.retry);
        assert!(retry.retry);
        assert_eq!(first.value, retry.value);
        assert_eq!(engine.request_records().await[0].attempts, 2);
        Ok(())
    }

    #[test]
    fn canonical_hash_ignores_object_insertion_order() -> Result<()> {
        let left: Value = serde_json::from_str(r#"{"a":1,"b":{"c":2,"d":3}}"#)?;
        let right: Value = serde_json::from_str(r#"{"b":{"d":3,"c":2},"a":1}"#)?;
        assert_eq!(
            canonical_request_hash(&left)?,
            canonical_request_hash(&right)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_server_serves_chat_completions_and_records_evidence() -> Result<()> {
        let _listener_guard = LOCAL_SERVER_TEST_LOCK.lock().await;
        let engine = Arc::new(FakeModelEngine::new(&simple_workflow())?);
        let bound = FakeModelServer::bind(
            "127.0.0.1:0".parse().map_err(|error| {
                AhrbError::Validation(format!("invalid test socket address: {error}"))
            })?,
            Arc::clone(&engine),
        )
        .await;
        let server = match bound {
            Ok(server) => server,
            Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                // Some source-build sandboxes prohibit even loopback listeners.
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let mut stream = tokio::net::TcpStream::connect(server.local_addr()).await?;
        let body = request_body();
        let head = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer test-only\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            server.local_addr(),
            body.len()
        );
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(&body).await?;
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read_to_end(&mut response),
        )
        .await
        .map_err(|_| AhrbError::Timeout("test fake-model response".to_owned()))??;
        let response = String::from_utf8(response).map_err(|error| {
            AhrbError::Protocol(format!("test response was not UTF-8: {error}"))
        })?;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("SUCCESS"));
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert!(records[0].accepted);
        assert_ne!(records[0].request.credential_fingerprint, "absent");
        server.shutdown().await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_http_server_uses_the_same_frontend_and_removes_socket() -> Result<()> {
        let _listener_guard = LOCAL_SERVER_TEST_LOCK.lock().await;
        // macOS limits sockaddr_un paths to 104 bytes; use the short system temp root.
        #[cfg(target_os = "macos")]
        let temporary_root = Path::new("/private/tmp");
        #[cfg(not(target_os = "macos"))]
        let temporary_root = Path::new("/tmp");
        let directory = temporary_root.join(format!("ahrb-fmu-{}", std::process::id()));
        match std::fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        std::fs::create_dir(&directory)?;
        let socket_path = directory.join("model.sock");
        let engine = Arc::new(FakeModelEngine::new(&simple_workflow())?);
        let bound = FakeModelUnixServer::bind(&socket_path, Arc::clone(&engine)).await;
        let server = match bound {
            Ok(server) => server,
            Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                // Some managed sandboxes grant only one listener bind per test process.
                std::fs::remove_dir(&directory)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let headers = BTreeMap::from([(
            "Authorization".to_owned(),
            "Bearer unix-test-only".to_owned(),
        )]);
        let response = crate::driver::unix_http_post(
            server.socket_path(),
            "/v1/chat/completions",
            &headers,
            &request_body(),
            std::time::Duration::from_secs(2),
        )
        .await?;
        assert_eq!(response.status, 200);
        let value: Value = serde_json::from_slice(&response.body)?;
        assert_eq!(
            value
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str),
            Some("SUCCESS")
        );
        assert_eq!(engine.request_records().await.len(), 1);
        server.shutdown().await?;
        assert!(!socket_path.exists());
        std::fs::remove_dir(&directory)?;
        Ok(())
    }
}
