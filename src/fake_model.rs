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
use std::future::Future;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

/// Maximum accepted fake-model request body. This keeps malformed peers bounded.
pub const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

const BIND_MAX_ATTEMPTS: usize = 8;
const BIND_BACKOFF_MS: u64 = 10;

// The managed source-build sandbox permits local listeners but may reject concurrent
// binds. Serialize listener-owning unit tests; production servers are unaffected.
#[cfg(test)]
pub(crate) static LOCAL_SERVER_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

#[cfg(test)]
pub(crate) struct ProcessServerTestLock {
    #[cfg(unix)]
    _file: std::fs::File,
}

#[cfg(test)]
pub(crate) fn acquire_process_server_test_lock() -> Result<ProcessServerTestLock> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;

        let path = std::env::temp_dir().join("ahrb-test-server-subprocess.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        loop {
            // SAFETY: `file` owns this valid descriptor for the full lifetime of the lock.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result == 0 {
                return Ok(ProcessServerTestLock { _file: file });
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
    }
    #[cfg(not(unix))]
    {
        Ok(ProcessServerTestLock {})
    }
}

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
        canonical_request_hash(&retry_identity_canonical(&self.dialect, &self.canonical))
    }

    fn same_retry_identity(&self, other: &Self) -> bool {
        self.dialect == other.dialect
            && self.endpoint == other.endpoint
            && self.model == other.model
            && self.scenario == other.scenario
            && self.actor == other.actor
            && self.checkpoint == other.checkpoint
            && self.credential_fingerprint == other.credential_fingerprint
            && retry_identity_canonical(&self.dialect, &self.canonical)
                == retry_identity_canonical(&other.dialect, &other.canonical)
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
    /// Canonical retry-identity hash.
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
    /// SHA-256 of canonical retry-identity JSON.
    pub canonical_hash: String,
    /// Number of retry-equivalent attempts observed for this semantic request.
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
            if !record.request.same_retry_identity(&request) {
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
        let value = adapt_scripted_tool_calls(&accepted.response.response, &request.canonical)?;

        Ok(ModelResponse {
            model: request.model,
            scenario: marker.scenario,
            actor: marker.actor,
            checkpoint: marker.checkpoint,
            request_hash,
            value,
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

#[derive(Clone, Debug)]
struct DeclaredTool {
    name: String,
    schema: Value,
}

fn adapt_scripted_tool_calls(value: &Value, request: &Value) -> Result<Value> {
    let Some(calls) = value.get("tool_calls").and_then(Value::as_array) else {
        return Ok(value.clone());
    };
    if !calls.iter().any(|call| call.get("_ahrb_native").is_some()) {
        return Ok(value.clone());
    }
    let declared = declared_tools(request)?;
    let mut adapted = value.clone();
    let adapted_calls = adapted
        .get_mut("tool_calls")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| AhrbError::Protocol("semantic tool_calls must be an array".to_owned()))?;
    for call in adapted_calls {
        adapt_scripted_tool_call(call, &declared)?;
    }
    Ok(adapted)
}

fn declared_tools(request: &Value) -> Result<BTreeMap<String, DeclaredTool>> {
    let tools = request
        .get("tools")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut declared = BTreeMap::new();
    for tool in tools {
        let object = tool.as_object().ok_or_else(|| {
            AhrbError::Protocol("request tools entries must be objects".to_owned())
        })?;
        let function = object.get("function").and_then(Value::as_object);
        let source = function.unwrap_or(object);
        let name = source.get("name").and_then(Value::as_str).or_else(|| {
            object
                .get("type")
                .and_then(Value::as_str)
                .filter(|kind| *kind != "function")
        });
        let Some(name) = name else {
            continue;
        };
        let schema = source
            .get("parameters")
            .or_else(|| source.get("input_schema"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let declaration = DeclaredTool {
            name: name.to_owned(),
            schema,
        };
        if declared.insert(name.to_owned(), declaration).is_some() {
            return Err(AhrbError::Protocol(format!(
                "request declares native tool {name:?} more than once"
            )));
        }
    }
    Ok(declared)
}

fn adapt_scripted_tool_call(
    call: &mut Value,
    declared: &BTreeMap<String, DeclaredTool>,
) -> Result<()> {
    let object = call.as_object_mut().ok_or_else(|| {
        AhrbError::Protocol("semantic tool_calls entries must be objects".to_owned())
    })?;
    let Some(adapter) = object.remove("_ahrb_native") else {
        return Ok(());
    };
    let adapter = adapter.as_object().ok_or_else(|| {
        AhrbError::Protocol("_ahrb_native tool adapter must be an object".to_owned())
    })?;
    let aliases = adapter
        .get("aliases")
        .and_then(Value::as_array)
        .ok_or_else(|| AhrbError::Protocol("native tool adapter lacks aliases[]".to_owned()))?;
    let bindings = adapter
        .get("bindings")
        .and_then(Value::as_object)
        .ok_or_else(|| AhrbError::Protocol("native tool adapter lacks bindings".to_owned()))?;
    let argv = adapter
        .get("argv")
        .and_then(Value::as_array)
        .ok_or_else(|| AhrbError::Protocol("native tool adapter lacks argv[]".to_owned()))?
        .iter()
        .map(|argument| {
            argument.as_str().map(str::to_owned).ok_or_else(|| {
                AhrbError::Protocol("native tool adapter argv must contain strings".to_owned())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let abstract_call_id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| AhrbError::Protocol("abstract fixture call lacks id".to_owned()))?
        .to_owned();
    let abstract_name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| AhrbError::Protocol("abstract fixture call lacks name".to_owned()))?
        .to_owned();
    let semantic_arguments = object
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AhrbError::Protocol("abstract fixture arguments must be an object".to_owned())
        })?;

    let mut declared_candidates = Vec::new();
    let mut shape_errors = Vec::new();
    for alias in aliases {
        let name = alias.as_str().ok_or_else(|| {
            AhrbError::Protocol("native tool adapter aliases must be strings".to_owned())
        })?;
        let Some(tool) = declared.get(name) else {
            continue;
        };
        declared_candidates.push(name);
        match render_native_arguments(
            tool,
            &abstract_call_id,
            &abstract_name,
            semantic_arguments,
            &argv,
            bindings,
        ) {
            Ok(arguments) => {
                object.insert("name".to_owned(), Value::String(tool.name.clone()));
                object.insert("arguments".to_owned(), arguments);
                return Ok(());
            }
            Err(error) => shape_errors.push(format!("{name}: {error}")),
        }
    }
    if declared_candidates.is_empty() {
        let aliases = aliases
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let available = declared
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(AhrbError::Protocol(format!(
            "none of the adapter native tools [{aliases}] were declared by the harness; declared tools: [{available}]"
        )));
    }
    Err(AhrbError::Protocol(format!(
        "declared native tools did not match adapter bindings: {}",
        shape_errors.join("; ")
    )))
}

fn render_native_arguments(
    tool: &DeclaredTool,
    abstract_call_id: &str,
    abstract_name: &str,
    semantic_arguments: &Map<String, Value>,
    argv: &[String],
    bindings: &Map<String, Value>,
) -> Result<Value> {
    for (style, expected_type) in [("command", "string"), ("command_argv", "array")] {
        let Some(field) = binding_for(bindings, &tool.name, style)? else {
            continue;
        };
        require_schema_property_type(&tool.schema, &field, expected_type)?;
        let metadata =
            native_fixture_metadata(abstract_call_id, abstract_name, semantic_arguments)?;
        let script = shell_command(argv, semantic_arguments, &metadata);
        let command = if style == "command" {
            Value::String(script)
        } else {
            json!(["/bin/sh", "-c", script])
        };
        let mut arguments = Map::new();
        arguments.insert(field.clone(), command);
        copy_allowed_semantic_arguments(
            &mut arguments,
            semantic_arguments,
            &tool.name,
            bindings,
            &tool.schema,
            Some(&field),
        )?;
        return Ok(Value::Object(arguments));
    }

    let mut arguments = Map::new();
    copy_allowed_semantic_arguments(
        &mut arguments,
        semantic_arguments,
        &tool.name,
        bindings,
        &tool.schema,
        None,
    )?;
    if arguments.is_empty() && !semantic_arguments.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "native tool {:?} accepts none of the abstract fixture fields",
            tool.name
        )));
    }
    Ok(Value::Object(arguments))
}

fn binding_for(
    bindings: &Map<String, Value>,
    native_name: &str,
    semantic_field: &str,
) -> Result<Option<String>> {
    let qualified = format!("{native_name}.{semantic_field}");
    bindings
        .get(&qualified)
        .or_else(|| bindings.get(semantic_field))
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "native tool binding {qualified:?} must be a string"
                ))
            })
        })
        .transpose()
}

fn copy_allowed_semantic_arguments(
    output: &mut Map<String, Value>,
    semantic_arguments: &Map<String, Value>,
    native_name: &str,
    bindings: &Map<String, Value>,
    schema: &Value,
    command_field: Option<&str>,
) -> Result<()> {
    for (field, value) in semantic_arguments {
        let target = binding_for(bindings, native_name, field)?.unwrap_or_else(|| field.clone());
        if command_field == Some(target.as_str()) || !schema_allows_property(schema, &target) {
            continue;
        }
        output.insert(target, value.clone());
    }
    Ok(())
}

fn schema_allows_property(schema: &Value, field: &str) -> bool {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| properties.contains_key(field))
        || schema.get("additionalProperties").and_then(Value::as_bool) != Some(false)
}

fn require_schema_property_type(schema: &Value, field: &str, expected: &str) -> Result<()> {
    let property = schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| properties.get(field))
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "declared schema has no property {field:?} for the configured command binding"
            ))
        })?;
    let matches = match property.get("type") {
        Some(Value::String(kind)) => kind == expected,
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind.as_str() == Some(expected)),
        _ => false,
    };
    if !matches {
        return Err(AhrbError::Protocol(format!(
            "declared schema property {field:?} is not type {expected:?}"
        )));
    }
    Ok(())
}

fn native_fixture_metadata(
    abstract_call_id: &str,
    abstract_name: &str,
    semantic_arguments: &Map<String, Value>,
) -> Result<String> {
    let bytes = serde_json::to_vec(&json!({
        "call_id": abstract_call_id,
        "name": abstract_name,
        "arguments": semantic_arguments
    }))?;
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|error| {
            AhrbError::Protocol(format!("encode native fixture metadata: {error}"))
        })?;
    }
    Ok(format!(
        "{}{}",
        crate::events::NATIVE_FIXTURE_METADATA_PREFIX,
        encoded
    ))
}

fn shell_command(
    argv: &[String],
    semantic_arguments: &Map<String, Value>,
    metadata: &str,
) -> String {
    let mut command = argv
        .iter()
        .map(|argument| format!("'{}'", argument.replace('\'', "'\"'\"'")))
        .collect::<Vec<_>>()
        .join(" ");
    command.push_str(" # ");
    for context in semantic_arguments
        .values()
        .filter_map(Value::as_str)
        .filter(|value| value.contains(crate::workflow::MARKER_PREFIX))
    {
        command.push_str(&context.replace(['\n', '\r'], " "));
        command.push(' ');
    }
    command.push_str(metadata);
    command
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
        let listener = retry_transient_bind(|| TcpListener::bind(addr)).await?;
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
        let listener =
            retry_transient_bind(|| std::future::ready(UnixListener::bind(&socket_path))).await?;
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

async fn retry_transient_bind<T, F, Fut>(mut bind: F) -> std::io::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::io::Result<T>>,
{
    for attempt in 1..=BIND_MAX_ATTEMPTS {
        match bind().await {
            Ok(listener) => return Ok(listener),
            Err(error) if attempt < BIND_MAX_ATTEMPTS && is_transient_bind_error(&error) => {
                let delay_ms = BIND_BACKOFF_MS.saturating_mul(attempt as u64);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other(
        "fake-model bind retry loop exhausted without a result",
    ))
}

pub(crate) fn is_transient_bind_error(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::AddrInUse
            | std::io::ErrorKind::AddrNotAvailable
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
    ) {
        return true;
    }
    error.raw_os_error().is_some_and(|code| {
        matches!(
            code,
            libc::ENOBUFS | libc::ENOMEM | libc::EMFILE | libc::ENFILE
        )
    })
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
    if request.method() == Method::GET && path == "/v1/models" {
        return response_from_parts(
            200,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::from_static(
                b"{\"object\":\"list\",\"data\":[{\"id\":\"ahrb-fake-v1\",\"object\":\"model\"}]}",
            )),
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

fn retry_identity_canonical(dialect: &str, value: &Value) -> Value {
    let mut canonical = canonicalize_json(value);
    if dialect == "openai-chat-completions" {
        if let Some(object) = canonical.as_object_mut() {
            // Rick v0.1.18 probes an OpenAI-compatible model with the complete
            // conversation before sending the real streamed request. These
            // response-delivery controls vary between the probe and request,
            // but the model, conversation, and tool declaration do not.
            object.remove("max_completion_tokens");
            object.remove("stream");
            object.remove("stream_options");
        }
    }
    canonical
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
    let completion_tokens = nonzero_token_estimate(&Value::Object(message.clone()));
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
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": completion_tokens,
            "total_tokens": completion_tokens.saturating_add(1)
        }
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
    let output_tokens = nonzero_token_estimate(&Value::Array(output.clone()));
    Ok(json!({
        "id": stable_id("resp", response),
        "object": "response",
        "created_at": 1_700_000_000_u64,
        "status": "completed",
        "model": response.model,
        "output": output,
        "usage": {
            "input_tokens": 1,
            "output_tokens": output_tokens,
            "total_tokens": output_tokens.saturating_add(1)
        }
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
    let output_tokens = nonzero_token_estimate(&Value::Array(content.clone()));
    Ok(json!({
        "id": stable_id("msg", response),
        "type": "message",
        "role": "assistant",
        "model": response.model,
        "content": content,
        "stop_reason": if tool_calls.is_empty() { "end_turn" } else { "tool_use" },
        "stop_sequence": null,
        "usage": {"input_tokens": 1, "output_tokens": output_tokens}
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

fn nonzero_token_estimate(value: &Value) -> u64 {
    let bytes = value.to_string().len();
    u64::try_from(bytes.saturating_add(3) / 4)
        .unwrap_or(u64::MAX)
        .max(1)
}

fn render_json_or_sse(value: Value, stream: bool, dialect: &str) -> Result<RenderedResponse> {
    let mut headers = BTreeMap::new();
    let body = if stream {
        headers.insert("cache-control".to_owned(), "no-cache".to_owned());
        headers.insert("content-type".to_owned(), "text/event-stream".to_owned());
        match dialect {
            "chat" => render_chat_sse(&value)?,
            "responses" => render_responses_sse(&value)?,
            "anthropic" => render_anthropic_sse(&value)?,
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

fn push_sse_event(body: &mut Vec<u8>, event: Option<&str>, data: &Value) -> Result<()> {
    if let Some(event) = event {
        body.extend_from_slice(b"event: ");
        body.extend_from_slice(event.as_bytes());
        body.push(b'\n');
    }
    body.extend_from_slice(b"data: ");
    serde_json::to_writer(&mut *body, data)?;
    body.extend_from_slice(b"\n\n");
    Ok(())
}

fn positive_usage_token(usage: Option<&Map<String, Value>>, key: &str, fallback: u64) -> u64 {
    usage
        .and_then(|usage| usage.get(key))
        .and_then(Value::as_u64)
        .filter(|tokens| *tokens > 0)
        .unwrap_or(fallback.max(1))
}

fn render_responses_sse(value: &Value) -> Result<Vec<u8>> {
    let response = value.as_object().ok_or_else(|| {
        AhrbError::Protocol("OpenAI Responses value must be an object".to_owned())
    })?;
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| AhrbError::Protocol("OpenAI Responses value lacks output[]".to_owned()))?;
    let output_tokens = positive_usage_token(
        response.get("usage").and_then(Value::as_object),
        "output_tokens",
        nonzero_token_estimate(&Value::Array(output.clone())),
    );
    let input_tokens = positive_usage_token(
        response.get("usage").and_then(Value::as_object),
        "input_tokens",
        1,
    );

    let mut completed_response = value.clone();
    let completed = completed_response.as_object_mut().ok_or_else(|| {
        AhrbError::Protocol("OpenAI Responses value must be an object".to_owned())
    })?;
    completed.insert("status".to_owned(), Value::String("completed".to_owned()));
    completed.insert(
        "usage".to_owned(),
        json!({
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens.saturating_add(output_tokens)
        }),
    );

    let mut created_response = completed_response.clone();
    let created = created_response.as_object_mut().ok_or_else(|| {
        AhrbError::Protocol("OpenAI Responses value must be an object".to_owned())
    })?;
    created.insert("status".to_owned(), Value::String("in_progress".to_owned()));
    created.insert("output".to_owned(), Value::Array(Vec::new()));
    created.insert("usage".to_owned(), Value::Null);

    let mut body = Vec::new();
    let mut sequence_number = 0_u64;
    push_responses_event(
        &mut body,
        &mut sequence_number,
        "response.created",
        json!({"response": created_response}),
    )?;

    for (output_index, item) in output.iter().enumerate() {
        let item_object = item.as_object().ok_or_else(|| {
            AhrbError::Protocol("OpenAI Responses output items must be objects".to_owned())
        })?;
        let item_type = item_object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AhrbError::Protocol("OpenAI Responses output item lacks type".to_owned())
            })?;
        let item_id = item_object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AhrbError::Protocol("OpenAI Responses output item lacks id".to_owned())
            })?;

        let mut started_item = item.clone();
        let started = started_item.as_object_mut().ok_or_else(|| {
            AhrbError::Protocol("OpenAI Responses output items must be objects".to_owned())
        })?;
        started.insert("status".to_owned(), Value::String("in_progress".to_owned()));
        match item_type {
            "message" => {
                started.insert("content".to_owned(), Value::Array(Vec::new()));
            }
            "function_call" => {
                started.insert("arguments".to_owned(), Value::String(String::new()));
            }
            _ => {}
        }
        push_responses_event(
            &mut body,
            &mut sequence_number,
            "response.output_item.added",
            json!({"output_index": output_index, "item": started_item}),
        )?;

        match item_type {
            "message" => {
                let content = item_object
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "OpenAI Responses message item lacks content[]".to_owned(),
                        )
                    })?;
                for (content_index, part) in content.iter().enumerate() {
                    let part_type = part.get("type").and_then(Value::as_str).unwrap_or("");
                    let mut started_part = part.clone();
                    if part_type == "output_text" {
                        if let Some(started_part) = started_part.as_object_mut() {
                            started_part.insert("text".to_owned(), Value::String(String::new()));
                        }
                    }
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.content_part.added",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": content_index,
                            "part": started_part
                        }),
                    )?;
                    if part_type == "output_text" {
                        let text = part.get("text").and_then(Value::as_str).unwrap_or("");
                        push_responses_event(
                            &mut body,
                            &mut sequence_number,
                            "response.output_text.delta",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "content_index": content_index,
                                "delta": text,
                                "logprobs": []
                            }),
                        )?;
                        push_responses_event(
                            &mut body,
                            &mut sequence_number,
                            "response.output_text.done",
                            json!({
                                "item_id": item_id,
                                "output_index": output_index,
                                "content_index": content_index,
                                "text": text,
                                "logprobs": []
                            }),
                        )?;
                    }
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.content_part.done",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "content_index": content_index,
                            "part": part
                        }),
                    )?;
                }
            }
            "function_call" => {
                let arguments = item_object
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                push_responses_event(
                    &mut body,
                    &mut sequence_number,
                    "response.function_call_arguments.delta",
                    json!({
                        "item_id": item_id,
                        "output_index": output_index,
                        "delta": arguments
                    }),
                )?;
                push_responses_event(
                    &mut body,
                    &mut sequence_number,
                    "response.function_call_arguments.done",
                    json!({
                        "item_id": item_id,
                        "output_index": output_index,
                        "arguments": arguments
                    }),
                )?;
            }
            _ => {}
        }
        push_responses_event(
            &mut body,
            &mut sequence_number,
            "response.output_item.done",
            json!({"output_index": output_index, "item": item}),
        )?;
    }

    push_responses_event(
        &mut body,
        &mut sequence_number,
        "response.completed",
        json!({"response": completed_response}),
    )?;
    Ok(body)
}

fn push_responses_event(
    body: &mut Vec<u8>,
    sequence_number: &mut u64,
    event: &str,
    fields: Value,
) -> Result<()> {
    let mut data = fields.as_object().cloned().ok_or_else(|| {
        AhrbError::Protocol("OpenAI Responses event fields must be an object".to_owned())
    })?;
    data.insert("type".to_owned(), Value::String(event.to_owned()));
    data.insert(
        "sequence_number".to_owned(),
        Value::Number((*sequence_number).into()),
    );
    *sequence_number = sequence_number.saturating_add(1);
    push_sse_event(body, Some(event), &Value::Object(data))
}

fn render_chat_sse(value: &Value) -> Result<Vec<u8>> {
    let completion = value.as_object().ok_or_else(|| {
        AhrbError::Protocol("OpenAI Chat Completions value must be an object".to_owned())
    })?;
    let id = required_string(completion, "id")?;
    let model = required_string(completion, "model")?;
    let created = completion
        .get("created")
        .cloned()
        .unwrap_or_else(|| json!(1_700_000_000_u64));
    let choices = completion
        .get("choices")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AhrbError::Protocol("OpenAI Chat Completions value lacks choices[]".to_owned())
        })?;
    let fallback_tokens = nonzero_token_estimate(&Value::Array(choices.clone()));
    let usage = completion.get("usage").and_then(Value::as_object);
    let prompt_tokens = positive_usage_token(usage, "prompt_tokens", 1);
    let completion_tokens = positive_usage_token(usage, "completion_tokens", fallback_tokens);

    let mut body = Vec::new();
    for (choice_offset, choice) in choices.iter().enumerate() {
        let choice = choice.as_object().ok_or_else(|| {
            AhrbError::Protocol("OpenAI Chat Completions choices must be objects".to_owned())
        })?;
        let index = choice
            .get("index")
            .cloned()
            .unwrap_or_else(|| json!(choice_offset));
        let message = choice
            .get("message")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AhrbError::Protocol("OpenAI Chat Completions choice lacks message".to_owned())
            })?;
        let role = message
            .get("role")
            .cloned()
            .unwrap_or_else(|| Value::String("assistant".to_owned()));
        push_chat_chunk(
            &mut body,
            id,
            model,
            &created,
            json!([{"index": index, "delta": {"role": role}, "finish_reason": null}]),
            None,
        )?;

        if let Some(content) = message.get("content").and_then(Value::as_str) {
            push_chat_chunk(
                &mut body,
                id,
                model,
                &created,
                json!([{"index": index, "delta": {"content": content}, "finish_reason": null}]),
                None,
            )?;
        }
        if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
            for (tool_index, tool_call) in tool_calls.iter().enumerate() {
                let mut tool_call = tool_call.as_object().cloned().ok_or_else(|| {
                    AhrbError::Protocol(
                        "OpenAI Chat Completions tool calls must be objects".to_owned(),
                    )
                })?;
                tool_call.insert("index".to_owned(), json!(tool_index));
                push_chat_chunk(
                    &mut body,
                    id,
                    model,
                    &created,
                    json!([{
                        "index": index,
                        "delta": {"tool_calls": [Value::Object(tool_call)]},
                        "finish_reason": null
                    }]),
                    None,
                )?;
            }
        }
        let finish_reason = choice
            .get("finish_reason")
            .cloned()
            .unwrap_or_else(|| Value::String("stop".to_owned()));
        push_chat_chunk(
            &mut body,
            id,
            model,
            &created,
            json!([{"index": index, "delta": {}, "finish_reason": finish_reason}]),
            Some(json!({
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens.saturating_add(completion_tokens)
            })),
        )?;
    }
    body.extend_from_slice(b"data: [DONE]\n\n");
    Ok(body)
}

fn push_chat_chunk(
    body: &mut Vec<u8>,
    id: &str,
    model: &str,
    created: &Value,
    choices: Value,
    usage: Option<Value>,
) -> Result<()> {
    let mut chunk = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": choices
    });
    if let Some(usage) = usage {
        let object = chunk.as_object_mut().ok_or_else(|| {
            AhrbError::Protocol("OpenAI Chat Completions chunk must be an object".to_owned())
        })?;
        object.insert("usage".to_owned(), usage);
    }
    push_sse_event(body, None, &chunk)
}

fn render_anthropic_sse(value: &Value) -> Result<Vec<u8>> {
    let message = value.as_object().ok_or_else(|| {
        AhrbError::Protocol("Anthropic Messages value must be an object".to_owned())
    })?;
    let content = message
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AhrbError::Protocol("Anthropic Messages value lacks content[]".to_owned())
        })?;
    let usage = message.get("usage").and_then(Value::as_object);
    let input_tokens = positive_usage_token(usage, "input_tokens", 1);
    let output_tokens = positive_usage_token(
        usage,
        "output_tokens",
        nonzero_token_estimate(&Value::Array(content.clone())),
    );

    let mut started_message = value.clone();
    let started = started_message.as_object_mut().ok_or_else(|| {
        AhrbError::Protocol("Anthropic Messages value must be an object".to_owned())
    })?;
    started.insert("content".to_owned(), Value::Array(Vec::new()));
    started.insert("stop_reason".to_owned(), Value::Null);
    started.insert("stop_sequence".to_owned(), Value::Null);
    started.insert(
        "usage".to_owned(),
        json!({"input_tokens": input_tokens, "output_tokens": 0}),
    );

    let mut body = Vec::new();
    push_sse_event(
        &mut body,
        Some("message_start"),
        &json!({"type": "message_start", "message": started_message}),
    )?;
    for (index, block) in content.iter().enumerate() {
        let block_object = block.as_object().ok_or_else(|| {
            AhrbError::Protocol("Anthropic Messages content blocks must be objects".to_owned())
        })?;
        let block_type = block_object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AhrbError::Protocol("Anthropic Messages content block lacks type".to_owned())
            })?;
        let (started_block, delta) = match block_type {
            "text" => (
                json!({"type": "text", "text": ""}),
                json!({
                    "type": "text_delta",
                    "text": block_object.get("text").and_then(Value::as_str).unwrap_or("")
                }),
            ),
            "tool_use" => {
                let id = block_object
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol("Anthropic tool_use block lacks id".to_owned())
                    })?;
                let name = block_object
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol("Anthropic tool_use block lacks name".to_owned())
                    })?;
                let input = block_object
                    .get("input")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                (
                    json!({"type": "tool_use", "id": id, "name": name, "input": {}}),
                    json!({
                        "type": "input_json_delta",
                        "partial_json": serde_json::to_string(&input)?
                    }),
                )
            }
            _ => (block.clone(), json!({"type": "text_delta", "text": ""})),
        };
        push_sse_event(
            &mut body,
            Some("content_block_start"),
            &json!({
                "type": "content_block_start",
                "index": index,
                "content_block": started_block
            }),
        )?;
        push_sse_event(
            &mut body,
            Some("content_block_delta"),
            &json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )?;
        push_sse_event(
            &mut body,
            Some("content_block_stop"),
            &json!({"type": "content_block_stop", "index": index}),
        )?;
    }
    let stop_reason = message
        .get("stop_reason")
        .cloned()
        .unwrap_or_else(|| Value::String("end_turn".to_owned()));
    let stop_sequence = message.get("stop_sequence").cloned().unwrap_or(Value::Null);
    push_sse_event(
        &mut body,
        Some("message_delta"),
        &json!({
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": stop_sequence},
            "usage": {"output_tokens": output_tokens}
        }),
    )?;
    push_sse_event(
        &mut body,
        Some("message_stop"),
        &json!({"type": "message_stop"}),
    )?;
    Ok(body)
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
    use std::cell::Cell;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn transient_bind_errors_are_retried_but_permanent_errors_are_not() -> Result<()> {
        let transient_attempts = Cell::new(0_usize);
        let listener = retry_transient_bind(|| {
            let attempt = transient_attempts.get().saturating_add(1);
            transient_attempts.set(attempt);
            std::future::ready(if attempt < 3 {
                Err(std::io::Error::from(std::io::ErrorKind::AddrInUse))
            } else {
                Ok("bound")
            })
        })
        .await?;
        assert_eq!(listener, "bound");
        assert_eq!(transient_attempts.get(), 3);

        let permanent_attempts = Cell::new(0_usize);
        let error = retry_transient_bind(|| {
            permanent_attempts.set(permanent_attempts.get().saturating_add(1));
            std::future::ready(Err::<(), _>(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied,
            )))
        })
        .await
        .expect_err("permanent bind error must be returned");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(permanent_attempts.get(), 1);
        Ok(())
    }

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

    fn streamed_tool_response() -> ModelResponse {
        ModelResponse {
            model: "ahrb-fake-v1".to_owned(),
            scenario: "routing".to_owned(),
            actor: "root".to_owned(),
            checkpoint: "start".to_owned(),
            request_hash: "request-hash".to_owned(),
            value: json!({
                "tool_calls": [{
                    "id": "call_fixture",
                    "name": "shell",
                    "arguments": {"command": ["ahrb-fixture", "write"]}
                }]
            }),
            fault: None,
            retry: false,
            stream: true,
        }
    }

    fn sse_json_frames(body: &[u8]) -> Result<Vec<(Option<String>, Value)>> {
        let body = std::str::from_utf8(body).map_err(|error| {
            AhrbError::Protocol(format!("test SSE response was not UTF-8: {error}"))
        })?;
        let mut frames = Vec::new();
        for frame in body.split("\n\n").filter(|frame| !frame.is_empty()) {
            let mut event = None;
            let mut data = None;
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    event = Some(value.to_owned());
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = Some(value);
                }
            }
            let data = data.ok_or_else(|| {
                AhrbError::Protocol(format!("test SSE frame lacks data: {frame:?}"))
            })?;
            if data != "[DONE]" {
                frames.push((event, serde_json::from_str(data)?));
            }
        }
        Ok(frames)
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
    async fn openai_chat_retry_identity_ignores_rick_probe_delivery_controls() -> Result<()> {
        let frontend = OpenAiChatFrontend;
        let common = json!({
            "model": "ahrb-fake",
            "messages": [{
                "role": "user",
                "content": "go [[AHRB:scenario=routing;actor=root;checkpoint=start]]"
            }],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "bash",
                    "parameters": {
                        "type": "object",
                        "properties": {"command": {"type": "string"}},
                        "required": ["command"]
                    }
                }
            }]
        });
        let mut probe_body = common.clone();
        probe_body["max_completion_tokens"] = json!(1);
        probe_body["stream"] = json!(false);
        let probe_bytes = serde_json::to_vec(&probe_body)?;
        let probe = frontend.parse(frontend.path(), &BTreeMap::new(), &probe_bytes)?;

        let mut streamed_body = common.clone();
        streamed_body["max_completion_tokens"] = json!(16_384);
        streamed_body["stream"] = json!(true);
        streamed_body["stream_options"] = json!({"include_usage": true});
        let streamed_bytes = serde_json::to_vec(&streamed_body)?;
        let streamed = frontend.parse(frontend.path(), &BTreeMap::new(), &streamed_bytes)?;

        assert_eq!(probe.canonical_hash()?, streamed.canonical_hash()?);
        let engine = FakeModelEngine::new(&simple_workflow())?;
        assert!(!engine.handle(probe).await?.retry);
        assert!(engine.handle(streamed.clone()).await?.retry);
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].attempts, 2);

        let mut changed_body = streamed_body;
        changed_body["messages"][0]["content"] =
            json!("different [[AHRB:scenario=routing;actor=root;checkpoint=start]]");
        let changed_bytes = serde_json::to_vec(&changed_body)?;
        let changed = frontend.parse(frontend.path(), &BTreeMap::new(), &changed_bytes)?;
        assert_ne!(streamed.canonical_hash()?, changed.canonical_hash()?);
        assert!(engine.handle(changed).await.is_err());
        Ok(())
    }

    #[test]
    fn scripted_write_fixture_uses_the_shell_tool_declared_by_the_request() -> Result<()> {
        let scripted = json!({
            "tool_calls": [{
                "id": "call-write",
                "name": "write_fixture",
                "arguments": {
                    "path": "nested/fixture.txt",
                    "content": "fixture payload",
                    "route": "[[AHRB:scenario=native;actor=root;checkpoint=next]]"
                },
                "_ahrb_native": {
                    "semantic": "write",
                    "aliases": ["missing_shell", "request_shell"],
                    "bindings": {"request_shell.command": "command"},
                    "argv": [
                        "/tmp/ahrb-fixture",
                        "write",
                        "--path",
                        "nested/fixture.txt",
                        "--content",
                        "fixture payload"
                    ]
                }
            }]
        });
        let request = json!({
            "model": "ahrb-fake-v1",
            "tools": [{
                "type": "function",
                "name": "request_shell",
                "parameters": {
                    "type": "object",
                    "properties": {"command": {"type": "string"}},
                    "required": ["command"],
                    "additionalProperties": false
                }
            }]
        });
        let adapted = adapt_scripted_tool_calls(&scripted, &request)?;
        assert_eq!(
            adapted
                .pointer("/tool_calls/0/name")
                .and_then(Value::as_str),
            Some("request_shell")
        );
        let command = adapted
            .pointer("/tool_calls/0/arguments/command")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("adapted command is absent".to_owned()))?;
        assert!(command.contains("'/tmp/ahrb-fixture' 'write'"));
        assert!(command.contains("'nested/fixture.txt'"));
        assert!(command.contains("'fixture payload'"));
        assert!(command.contains("checkpoint=next"));
        assert!(adapted.pointer("/tool_calls/0/_ahrb_native").is_none());
        assert!(adapted.pointer("/tool_calls/0/arguments/path").is_none());
        let rendered = OpenAiResponsesFrontend.render(&ModelResponse {
            model: "ahrb-fake-v1".to_owned(),
            scenario: "native".to_owned(),
            actor: "root".to_owned(),
            checkpoint: "next".to_owned(),
            request_hash: "request-hash".to_owned(),
            value: adapted.clone(),
            fault: None,
            retry: false,
            stream: false,
        })?;
        let body: Value = serde_json::from_slice(&rendered.body)?;
        assert_eq!(
            body.pointer("/output/0/name").and_then(Value::as_str),
            Some("request_shell")
        );
        let rendered_arguments = body
            .pointer("/output/0/arguments")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("rendered arguments are absent".to_owned()))?;
        assert_eq!(
            serde_json::from_str::<Value>(rendered_arguments)?
                .get("command")
                .and_then(Value::as_str),
            Some(command)
        );
        Ok(())
    }

    #[test]
    fn native_tool_translation_honors_chat_and_anthropic_schema_locations() -> Result<()> {
        let directory =
            std::env::temp_dir().join(format!("ahrb-native-argv-{}", std::process::id()));
        if directory.exists() {
            std::fs::remove_dir_all(&directory)?;
        }
        std::fs::create_dir_all(&directory)?;
        for request in [
            json!({
                "tools": [{
                    "type": "function",
                    "function": {
                        "name": "native_exec",
                        "parameters": {
                            "type": "object",
                            "properties": {"command": {"type": "array"}},
                            "additionalProperties": false
                        }
                    }
                }]
            }),
            json!({
                "tools": [{
                    "name": "native_exec",
                    "input_schema": {
                        "type": "object",
                        "properties": {"command": {"type": "array"}},
                        "additionalProperties": false
                    }
                }]
            }),
        ] {
            let scripted = json!({
                "tool_calls": [{
                    "id": "call-read",
                    "name": "read_fixture",
                    "arguments": {"path": "fixture.txt"},
                    "_ahrb_native": {
                        "semantic": "read",
                        "aliases": ["native_exec"],
                        "bindings": {"native_exec.command_argv": "command"},
                        "argv": ["/bin/sh", "-c", "printf argv-ok > argv-effect.txt"]
                    }
                }]
            });
            let adapted = adapt_scripted_tool_calls(&scripted, &request)?;
            let command = adapted
                .pointer("/tool_calls/0/arguments/command")
                .and_then(Value::as_array)
                .ok_or_else(|| AhrbError::Protocol("native argv is absent".to_owned()))?;
            assert_eq!(command.first().and_then(Value::as_str), Some("/bin/sh"));
            assert_eq!(command.get(1).and_then(Value::as_str), Some("-c"));
            let script = command
                .get(2)
                .and_then(Value::as_str)
                .ok_or_else(|| AhrbError::Protocol("native shell script is absent".to_owned()))?;
            assert!(script.contains("printf argv-ok > argv-effect.txt"));
            assert!(script.contains(crate::events::NATIVE_FIXTURE_METADATA_PREFIX));
            let status = std::process::Command::new("/bin/sh")
                .args(["-c", script])
                .current_dir(&directory)
                .status()?;
            assert!(status.success());
        }
        assert_eq!(
            std::fs::read_to_string(directory.join("argv-effect.txt"))?,
            "argv-ok"
        );
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn remaining_adapter_writes_render_through_their_declared_native_shell() -> Result<()> {
        let directory =
            std::env::temp_dir().join(format!("ahrb-native-adapter-writes-{}", std::process::id()));
        if directory.exists() {
            std::fs::remove_dir_all(&directory)?;
        }
        std::fs::create_dir_all(&directory)?;

        for adapter in ["claude-code", "pi", "rick"] {
            let manifest =
                crate::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))?;
            let aliases = manifest.tools.aliases["write"].candidates();
            let native_name = aliases.first().ok_or_else(|| {
                AhrbError::Validation(format!("{adapter} has no native write alias"))
            })?;
            let effect = format!("{adapter}.txt");
            let scripted = json!({
                "tool_calls": [{
                    "id": format!("call-{adapter}"),
                    "name": "write_fixture",
                    "arguments": {"path": effect, "content": adapter},
                    "_ahrb_native": {
                        "semantic": "write",
                        "aliases": aliases,
                        "bindings": manifest.tools.bindings,
                        "argv": [
                            "/bin/sh",
                            "-c",
                            format!("printf '%s' '{adapter}' > '{effect}'")
                        ]
                    }
                }]
            });
            let schema = json!({
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
                "additionalProperties": false
            });
            let request = match manifest.fake_model.dialect {
                crate::manifest::ProtocolDialect::AnthropicMessages => json!({
                    "tools": [{
                        "name": native_name,
                        "input_schema": schema
                    }]
                }),
                crate::manifest::ProtocolDialect::OpenAiChatCompletions => json!({
                    "tools": [{
                        "type": "function",
                        "function": {
                            "name": native_name,
                            "parameters": schema
                        }
                    }]
                }),
                crate::manifest::ProtocolDialect::OpenAiResponses => json!({
                    "tools": [{
                        "type": "function",
                        "name": native_name,
                        "parameters": schema
                    }]
                }),
            };
            let adapted = adapt_scripted_tool_calls(&scripted, &request)?;
            assert_eq!(
                adapted
                    .pointer("/tool_calls/0/name")
                    .and_then(Value::as_str),
                Some(native_name.as_str()),
                "{adapter}"
            );
            let command = adapted
                .pointer("/tool_calls/0/arguments/command")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AhrbError::Protocol(format!("{adapter} native command is absent"))
                })?;
            assert!(
                command.contains(crate::events::NATIVE_FIXTURE_METADATA_PREFIX),
                "{adapter}"
            );
            if adapter == "claude-code" {
                let rendered = AnthropicMessagesFrontend.render(&ModelResponse {
                    model: manifest.fake_model.model.clone(),
                    scenario: "native".to_owned(),
                    actor: "claude".to_owned(),
                    checkpoint: "start".to_owned(),
                    request_hash: "claude-request-hash".to_owned(),
                    value: adapted.clone(),
                    fault: None,
                    retry: false,
                    stream: true,
                })?;
                let frames = sse_json_frames(&rendered.body)?;
                assert_eq!(
                    frames[1]
                        .1
                        .pointer("/content_block/name")
                        .and_then(Value::as_str),
                    Some("Bash")
                );
                let partial_input = frames[2]
                    .1
                    .pointer("/delta/partial_json")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "Claude Anthropic tool input delta is absent".to_owned(),
                        )
                    })?;
                let input: Value = serde_json::from_str(partial_input)?;
                assert!(input.is_object());
                assert_eq!(input.get("command").and_then(Value::as_str), Some(command));
            }
            let status = std::process::Command::new("/bin/sh")
                .args(["-c", command])
                .current_dir(&directory)
                .status()?;
            assert!(status.success(), "{adapter}");
            assert_eq!(
                std::fs::read_to_string(directory.join(&effect))?,
                adapter,
                "{adapter}"
            );
        }

        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn responses_sse_streams_function_call_lifecycle_and_nonzero_usage() -> Result<()> {
        let rendered = OpenAiResponsesFrontend.render(&streamed_tool_response())?;
        let frames = sse_json_frames(&rendered.body)?;
        let event_names: Vec<_> = frames
            .iter()
            .map(|(event, _)| event.as_deref().unwrap_or(""))
            .collect();
        assert_eq!(
            event_names,
            vec![
                "response.created",
                "response.output_item.added",
                "response.function_call_arguments.delta",
                "response.function_call_arguments.done",
                "response.output_item.done",
                "response.completed"
            ]
        );
        assert_eq!(
            frames[1].1.pointer("/item/type").and_then(Value::as_str),
            Some("function_call")
        );
        assert_eq!(
            frames[1].1.pointer("/item/status").and_then(Value::as_str),
            Some("in_progress")
        );
        assert!(
            frames[5]
                .1
                .pointer("/response/usage/output_tokens")
                .and_then(Value::as_u64)
                .is_some_and(|tokens| tokens > 0)
        );
        Ok(())
    }

    #[test]
    fn chat_sse_streams_role_tool_delta_finish_and_done() -> Result<()> {
        let rendered = OpenAiChatFrontend.render(&streamed_tool_response())?;
        let frames = sse_json_frames(&rendered.body)?;
        assert_eq!(frames.len(), 3);
        assert!(frames.iter().all(|(event, value)| {
            event.is_none()
                && value.get("object").and_then(Value::as_str) == Some("chat.completion.chunk")
        }));
        assert_eq!(
            frames[0]
                .1
                .pointer("/choices/0/delta/role")
                .and_then(Value::as_str),
            Some("assistant")
        );
        assert_eq!(
            frames[1]
                .1
                .pointer("/choices/0/delta/tool_calls/0/function/name")
                .and_then(Value::as_str),
            Some("shell")
        );
        assert_eq!(
            frames[2]
                .1
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str),
            Some("tool_calls")
        );
        assert!(
            frames[2]
                .1
                .pointer("/usage/completion_tokens")
                .and_then(Value::as_u64)
                .is_some_and(|tokens| tokens > 0)
        );
        assert!(rendered.body.ends_with(b"data: [DONE]\n\n"));
        Ok(())
    }

    #[test]
    fn anthropic_sse_streams_tool_block_then_terminal_message_events() -> Result<()> {
        let rendered = AnthropicMessagesFrontend.render(&streamed_tool_response())?;
        let frames = sse_json_frames(&rendered.body)?;
        let event_names: Vec<_> = frames
            .iter()
            .map(|(event, _)| event.as_deref().unwrap_or(""))
            .collect();
        assert_eq!(
            event_names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
        assert_eq!(
            frames[1]
                .1
                .pointer("/content_block/type")
                .and_then(Value::as_str),
            Some("tool_use")
        );
        assert_eq!(
            frames[2].1.pointer("/delta/type").and_then(Value::as_str),
            Some("input_json_delta")
        );
        assert_eq!(
            frames[4]
                .1
                .pointer("/delta/stop_reason")
                .and_then(Value::as_str),
            Some("tool_use")
        );
        assert!(
            frames[4]
                .1
                .pointer("/usage/output_tokens")
                .and_then(Value::as_u64)
                .is_some_and(|tokens| tokens > 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_server_serves_chat_completions_and_records_evidence() -> Result<()> {
        let _listener_guard = LOCAL_SERVER_TEST_LOCK.lock().await;
        let _process_guard = acquire_process_server_test_lock()?;
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
        let mut catalog_stream = tokio::net::TcpStream::connect(server.local_addr()).await?;
        let catalog_request = format!(
            "GET /v1/models HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            server.local_addr()
        );
        catalog_stream.write_all(catalog_request.as_bytes()).await?;
        let mut catalog_response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            catalog_stream.read_to_end(&mut catalog_response),
        )
        .await
        .map_err(|_| AhrbError::Timeout("test fake-model catalog response".to_owned()))??;
        let catalog_response = String::from_utf8(catalog_response).map_err(|error| {
            AhrbError::Protocol(format!("test catalog response was not UTF-8: {error}"))
        })?;
        assert!(catalog_response.starts_with("HTTP/1.1 200 OK"));
        assert!(catalog_response.contains("ahrb-fake-v1"));
        assert!(engine.request_records().await.is_empty());

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
        let _process_guard = acquire_process_server_test_lock()?;
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
