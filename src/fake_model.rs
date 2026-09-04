//! Marker-routed deterministic fake-model engine and local HTTP protocol frontends.

use crate::manifest::{ModelRole, RequestRoleRule, SideChannelKind};
use crate::workflow::{
    BarrierCoordinator, Fault, RouteMarker, TransitionRecognition, Workflow, WorkflowMachine,
};
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
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::{AsRawFd as _, RawFd};
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
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
const DETERMINISTIC_THREAD_TITLE: &str = "AHRB benchmark thread";
#[cfg(test)]
const TITLE_SYSTEM_PROMPT: &str = "You are a title generator. You output ONLY a thread title.";

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
}

/// A protocol-independent response selected by the workflow engine.
#[derive(Clone, Debug)]
pub struct ModelResponse {
    /// Protocol dialect used to locate physical-attempt evidence.
    pub dialect: String,
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
    /// Physical request attempt correlated to response evidence.
    pub attempt: u64,
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

/// One physical model-request attempt. Records are returned in semantic-key/attempt order.
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
    /// Stable ordinal within the scripted scenario/actor/checkpoint route.
    pub semantic_ordinal: u64,
    /// One-based physical attempt number for this semantic request.
    pub attempt: u64,
    /// Monotonic timestamp after the fake provider finished reading the request body.
    pub received_ns: u64,
    /// Exact raw HTTP request body length before JSON parsing.
    pub body_bytes: u64,
    /// Revision-2.3 fake-provider token count when a context window is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    /// `primary`, `side-channel`, or `unclassified`.
    pub role: String,
    /// Classified auxiliary kind; null for a primary request.
    pub side_channel_kind: Option<String>,
    /// HTTP response status when known at the provider boundary.
    pub response_status: Option<u16>,
    /// Monotonic boundary immediately before the provider returns response headers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_headers_ns: Option<u64>,
    /// Monotonic first response-frame yield boundary when observed.
    pub response_first_frame_yield_ns: Option<u64>,
    /// Monotonic last response-frame yield boundary when observed.
    pub response_last_frame_yield_ns: Option<u64>,
    /// Final physical-attempt total, repeated on every attempt record.
    pub semantic_attempts_total: u64,
}

/// One out-of-band fake-provider body-frame observation.
///
/// The scheduled boundary is anchored to the response-header boundary. The
/// yielded boundary is captured immediately before returning the frame to
/// Hyper; neither field claims kernel-write or peer-read timing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelFrameObservation {
    /// Scenario route marker.
    pub scenario: String,
    /// Actor route marker.
    pub actor: String,
    /// Accepted checkpoint.
    pub checkpoint: String,
    /// One-based physical request attempt.
    pub attempt: u64,
    /// One-based frame ordinal within this response.
    pub ordinal: u32,
    /// Anchored monotonic scheduled boundary.
    pub scheduled_ns: u64,
    /// Monotonic `Body::poll_frame` yield boundary.
    pub frame_yielded_ns: u64,
    /// Payload bytes in this frame.
    pub bytes: u64,
}

/// Complete row-42 aggregation and reference-envelope decision.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelRequestEfficiencyEvaluation {
    /// Exact numeric `metrics` entries required by the v2 schema.
    pub metrics: BTreeMap<String, f64>,
    /// Exact structured `details.model-request-efficiency` value.
    pub details: Value,
    /// Whether all mandatory observations were present.
    pub measurement_complete: bool,
    /// Reference-envelope decision when measurement is complete.
    pub reference_envelope_pass: bool,
}

/// Count physical retry attempts by semantic request using row 42's attempt-total evidence.
pub fn retry_attempt_count(records: &[ModelRequestRecord]) -> u64 {
    let mut semantic_attempts = BTreeMap::new();
    for record in records {
        semantic_attempts
            .entry((
                record.request.scenario.as_str(),
                record.request.actor.as_str(),
                record.request.checkpoint.as_str(),
                record.semantic_ordinal,
            ))
            .and_modify(|total: &mut u64| {
                *total = (*total).max(record.semantic_attempts_total);
            })
            .or_insert(record.semantic_attempts_total);
    }
    semantic_attempts.values().fold(0_u64, |total, attempts| {
        total.saturating_add(attempts.saturating_sub(1))
    })
}

/// Aggregate physical request attempts for row 42 without instrumenting the harness path.
pub fn evaluate_model_request_efficiency(
    records: &[ModelRequestRecord],
    completed_semantic_turns: u64,
    expected_semantic_turns: u64,
) -> ModelRequestEfficiencyEvaluation {
    evaluate_model_request_efficiency_repetitions(
        records,
        &BTreeMap::from([(1_u32, completed_semantic_turns)]),
        expected_semantic_turns,
    )
}

/// Aggregate the two row-42 fresh-profile repetitions and enforce every
/// per-repetition oracle before publishing pooled headline metrics.
pub fn evaluate_model_request_efficiency_repetitions(
    records: &[ModelRequestRecord],
    completed_turns_by_repetition: &BTreeMap<u32, u64>,
    expected_turns_per_repetition: u64,
) -> ModelRequestEfficiencyEvaluation {
    let physical_requests = records.len() as u64;
    let primary_requests = records
        .iter()
        .filter(|record| record.role == "primary")
        .count() as u64;
    let side_channel_requests = records
        .iter()
        .filter(|record| record.role != "primary")
        .count() as u64;
    let unclassified = records
        .iter()
        .filter(|record| {
            record.role == "unclassified"
                || record.side_channel_kind.as_deref() == Some("unknown-side-channel")
        })
        .collect::<Vec<_>>();
    let missing_role_evidence = records.iter().any(|record| {
        !matches!(
            record.role.as_str(),
            "primary" | "side-channel" | "unclassified"
        ) || (record.role == "side-channel"
            && !matches!(
                record.side_channel_kind.as_deref(),
                Some(
                    "title"
                        | "summary"
                        | "compaction"
                        | "reviewer"
                        | "child"
                        | "unknown-side-channel"
                )
            ))
    });
    let retry_attempts = retry_attempt_count(records);
    let body_sizes = records
        .iter()
        .map(|record| record.body_bytes)
        .collect::<Vec<_>>();
    let context_sizes = records
        .iter()
        .map(|record| {
            canonical_context_tax_bytes_for_dialect(
                &record.request.dialect,
                &record.request.canonical,
            )
        })
        .collect::<Vec<_>>();
    let repetition_for = |record: &ModelRequestRecord| {
        record
            .request
            .scenario
            .strip_prefix("ahrb-row42-r")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(1)
    };
    let mut context_slopes = Vec::new();
    let mut repetitions = Vec::new();
    let mut repetitions_pass = true;
    for (&repetition, &completed_turns) in completed_turns_by_repetition {
        let repetition_records = records
            .iter()
            .filter(|record| repetition_for(record) == repetition)
            .collect::<Vec<_>>();
        let mut primary_by_turn =
            BTreeMap::<(&str, &str, u64, &str), Vec<&ModelRequestRecord>>::new();
        for record in repetition_records
            .iter()
            .copied()
            .filter(|record| record.role == "primary")
        {
            primary_by_turn
                .entry((
                    record.request.scenario.as_str(),
                    record.request.actor.as_str(),
                    record.semantic_ordinal,
                    record.request.checkpoint.as_str(),
                ))
                .or_default()
                .push(record);
        }
        let one_primary_per_turn = primary_by_turn.len() as u64 == expected_turns_per_repetition
            && primary_by_turn.values().all(|attempts| attempts.len() == 1);
        let repetition_context = primary_by_turn
            .values()
            .filter_map(|attempts| attempts.first().copied())
            .map(|record| {
                canonical_context_tax_bytes_for_dialect(
                    &record.request.dialect,
                    &record.request.canonical,
                )
            })
            .collect::<Vec<_>>();
        let mut repetition_attempts = BTreeMap::new();
        for record in &repetition_records {
            repetition_attempts
                .entry((
                    record.request.scenario.as_str(),
                    record.request.actor.as_str(),
                    record.semantic_ordinal,
                    record.request.checkpoint.as_str(),
                ))
                .and_modify(|total: &mut u64| {
                    *total = (*total).max(record.semantic_attempts_total);
                })
                .or_insert(record.semantic_attempts_total);
        }
        let repetition_retries = repetition_attempts.values().fold(0_u64, |total, attempts| {
            total.saturating_add(attempts.saturating_sub(1))
        });
        let repetition_side_channels = repetition_records
            .iter()
            .filter(|record| record.role != "primary")
            .count() as u64;
        let repetition_unclassified = repetition_records.iter().any(|record| {
            record.role == "unclassified"
                || record.side_channel_kind.as_deref() == Some("unknown-side-channel")
        });
        let slope = theil_sen_by_index(&repetition_context);
        context_slopes.push(slope);
        let complete = completed_turns == expected_turns_per_repetition
            && !repetition_records.is_empty()
            && repetition_records
                .iter()
                .all(|record| record.body_bytes > 0)
            && !missing_role_evidence;
        let denominator = completed_turns.max(1) as f64;
        let passed = complete
            && one_primary_per_turn
            && !repetition_unclassified
            && repetition_side_channels as f64 / denominator <= 0.05
            && repetition_retries == 0
            && nearest_rank(&repetition_context, 95) <= 1_048_576
            && slope.abs() <= 1_024.0;
        repetitions_pass &= passed;
        repetitions.push(json!({
            "repetition": repetition,
            "completed_turns": completed_turns,
            "primary_turns": primary_by_turn.len(),
            "one_primary_per_turn": one_primary_per_turn,
            "side_channel_requests": repetition_side_channels,
            "retry_attempts": repetition_retries,
            "context_tax_slope_bytes_per_turn": slope,
            "measurement_complete": complete,
            "reference_envelope_pass": passed,
        }));
    }
    context_slopes.sort_by(f64::total_cmp);
    let completed_semantic_turns = completed_turns_by_repetition.values().copied().sum::<u64>();
    let denominator = completed_semantic_turns as f64;
    let rate = |count: u64| {
        if completed_semantic_turns == 0 {
            0.0
        } else {
            count as f64 / denominator
        }
    };
    let context_slope = median_f64(&context_slopes);
    let mut metrics = BTreeMap::from([
        (
            "model_request_efficiency.requests_per_semantic_turn".to_owned(),
            rate(physical_requests),
        ),
        (
            "model_request_efficiency.primary_requests_per_turn".to_owned(),
            rate(primary_requests),
        ),
        (
            "model_request_efficiency.side_channel_requests_per_turn".to_owned(),
            rate(side_channel_requests),
        ),
        (
            "model_request_efficiency.retry_attempts_per_turn".to_owned(),
            rate(retry_attempts),
        ),
        (
            "model_request_efficiency.request_body_bytes_p50".to_owned(),
            nearest_rank(&body_sizes, 50) as f64,
        ),
        (
            "model_request_efficiency.request_body_bytes_p95".to_owned(),
            nearest_rank(&body_sizes, 95) as f64,
        ),
        (
            "model_request_efficiency.request_body_bytes_max".to_owned(),
            body_sizes.iter().copied().max().unwrap_or(0) as f64,
        ),
        (
            "model_request_efficiency.context_tax_bytes_p50".to_owned(),
            nearest_rank(&context_sizes, 50) as f64,
        ),
        (
            "model_request_efficiency.context_tax_bytes_p95".to_owned(),
            nearest_rank(&context_sizes, 95) as f64,
        ),
        (
            "model_request_efficiency.context_tax_bytes_max".to_owned(),
            context_sizes.iter().copied().max().unwrap_or(0) as f64,
        ),
        (
            "model_request_efficiency.context_tax_slope_bytes_per_turn".to_owned(),
            context_slope,
        ),
    ]);
    // Normalize negative zero so serialized reports stay byte-stable.
    if metrics.get("model_request_efficiency.context_tax_slope_bytes_per_turn") == Some(&-0.0) {
        metrics.insert(
            "model_request_efficiency.context_tax_slope_bytes_per_turn".to_owned(),
            0.0,
        );
    }
    let mut side_channel_requests_by_role = BTreeMap::new();
    for kind in ["title", "summary", "compaction", "reviewer", "child"] {
        let count = records
            .iter()
            .filter(|record| record.side_channel_kind.as_deref() == Some(kind))
            .count() as u64;
        side_channel_requests_by_role.insert(kind, count);
    }
    let unclassified_requests = unclassified
        .iter()
        .map(|record| {
            json!({
                "scenario": record.request.scenario,
                "actor": record.request.actor,
                "checkpoint": record.request.checkpoint,
                "semantic_ordinal": record.semantic_ordinal,
                "attempt": record.attempt
            })
        })
        .collect::<Vec<_>>();
    let expected_total =
        expected_turns_per_repetition.saturating_mul(completed_turns_by_repetition.len() as u64);
    let measurement_complete = !completed_turns_by_repetition.is_empty()
        && completed_semantic_turns == expected_total
        && !records.is_empty()
        && records.iter().all(|record| record.body_bytes > 0)
        && !missing_role_evidence;
    let reference_envelope_pass = measurement_complete && repetitions_pass;
    ModelRequestEfficiencyEvaluation {
        metrics,
        details: json!({
            "side_channel_requests_by_role": side_channel_requests_by_role,
            "unclassified_requests": unclassified_requests,
            "repetitions": repetitions,
        }),
        measurement_complete,
        reference_envelope_pass,
    }
}

/// Canonical encoded system/developer instructions plus tool definitions.
pub fn canonical_context_tax_bytes(canonical: &Value) -> u64 {
    let dialect = if canonical.get("instructions").is_some() {
        "openai-responses"
    } else if canonical.get("system").is_some() && canonical.get("messages").is_some() {
        "anthropic-messages"
    } else {
        "openai-chat-completions"
    };
    canonical_context_tax_bytes_for_dialect(dialect, canonical)
}

/// Serialize exactly the dialect instruction/tool envelope used by row 42.
pub fn canonical_context_tax_bytes_for_dialect(dialect: &str, canonical: &Value) -> u64 {
    let tools = canonical.get("tools").cloned().unwrap_or_else(|| json!([]));
    let value = match dialect {
        "openai-responses" => json!({
            "instructions": canonical.get("instructions").cloned().unwrap_or(Value::Null),
            "tools": tools,
        }),
        "anthropic-messages" => json!({
            "system": canonical.get("system").cloned().unwrap_or(Value::Null),
            "tools": tools,
        }),
        _ => {
            let messages = canonical
                .get("messages")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            matches!(
                                item.get("role").and_then(Value::as_str),
                                Some("system" | "developer")
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            json!({"messages": messages, "tools": tools})
        }
    };
    serde_json::to_vec(&value).map_or(0, |encoded| encoded.len() as u64)
}

fn nearest_rank(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

fn theil_sen_by_index(values: &[u64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mut slopes = Vec::new();
    for left in 0..values.len() - 1 {
        for right in left + 1..values.len() {
            let delta = values[right] as f64 - values[left] as f64;
            slopes.push(delta / (right - left) as f64);
        }
    }
    slopes.sort_by(f64::total_cmp);
    let middle = slopes.len() / 2;
    if slopes.len() % 2 == 0 {
        (slopes[middle - 1] + slopes[middle]) / 2.0
    } else {
        slopes[middle]
    }
}

fn median_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

type RequestRecordKey = (String, String, String, String, String);
type SemanticRequestKey = (String, String, String, u64);
type FrameBoundaryKey = (String, String, String, u64);
type FrameBoundaryMap = BTreeMap<FrameBoundaryKey, (u64, u64)>;

/// State for deterministic transition validation, barriers, and idempotent retries.
#[derive(Debug)]
pub struct FakeModelEngine {
    machine: WorkflowMachine,
    barriers: BarrierCoordinator,
    requests: Mutex<BTreeMap<RequestRecordKey, Vec<ModelRequestRecord>>>,
    frame_observations: Arc<StdMutex<Vec<ModelFrameObservation>>>,
    frame_boundaries: Arc<StdMutex<FrameBoundaryMap>>,
    attempt_sequences: Mutex<BTreeMap<SemanticRequestKey, u64>>,
    request_role_rules: Vec<RequestRoleRule>,
    semantic_ordinals: BTreeMap<(String, String, String), u64>,
    context_window_tokens: Option<u64>,
    context_faulted_routes: Mutex<BTreeSet<(String, String, String)>>,
}

impl FakeModelEngine {
    /// Build an engine from one validated declarative workflow.
    pub fn new(workflow: &Workflow) -> Result<Self> {
        Self::with_request_roles(workflow, &BTreeMap::new(), &[])
    }

    /// Build an engine with manifest-declared auxiliary model roles.
    pub fn with_model_roles(
        workflow: &Workflow,
        model_roles: &BTreeMap<String, ModelRole>,
    ) -> Result<Self> {
        Self::with_request_roles(workflow, model_roles, &[])
    }

    /// Build an engine with model IDs and ordered non-primary request classifiers.
    pub fn with_request_roles(
        workflow: &Workflow,
        model_roles: &BTreeMap<String, ModelRole>,
        request_role_rules: &[RequestRoleRule],
    ) -> Result<Self> {
        Self::with_request_roles_and_context_window(workflow, model_roles, request_role_rules, None)
    }

    /// Build an engine with one advertised and enforced fake context window.
    pub fn with_request_roles_and_context_window(
        workflow: &Workflow,
        _model_roles: &BTreeMap<String, ModelRole>,
        request_role_rules: &[RequestRoleRule],
        context_window_tokens: Option<u64>,
    ) -> Result<Self> {
        if context_window_tokens == Some(0) {
            return Err(AhrbError::Validation(
                "fake context window must be positive".to_owned(),
            ));
        }
        let mut request_role_rules = request_role_rules.to_vec();
        request_role_rules.sort_by_key(|rule| rule.priority);
        let mut next_by_actor = BTreeMap::<String, u64>::new();
        let mut semantic_ordinals = BTreeMap::new();
        for response in &workflow.responses {
            let next = next_by_actor.entry(response.actor.clone()).or_default();
            *next = next.saturating_add(1);
            semantic_ordinals.insert(
                (
                    response.scenario.clone(),
                    response.actor.clone(),
                    response.checkpoint.clone(),
                ),
                *next,
            );
        }
        Ok(Self {
            machine: WorkflowMachine::new(workflow)?,
            barriers: BarrierCoordinator::new(workflow)?,
            requests: Mutex::new(BTreeMap::new()),
            frame_observations: Arc::new(StdMutex::new(Vec::new())),
            frame_boundaries: Arc::new(StdMutex::new(BTreeMap::new())),
            attempt_sequences: Mutex::new(BTreeMap::new()),
            request_role_rules,
            semantic_ordinals,
            context_window_tokens,
            context_faulted_routes: Mutex::new(BTreeSet::new()),
        })
    }

    /// Advertised row-51 provider context window, when configured.
    pub fn context_window_tokens(&self) -> Option<u64> {
        self.context_window_tokens
    }

    /// Validate, route, and await any named barrier for a canonical request.
    pub async fn handle(&self, request: ModelRequest) -> Result<ModelResponse> {
        let body_bytes = serde_json::to_vec(&request.canonical)?.len() as u64;
        self.handle_observed(request, body_bytes, monotonic_timestamp_ns())
            .await
    }

    async fn handle_observed(
        &self,
        request: ModelRequest,
        body_bytes: u64,
        received_ns: u64,
    ) -> Result<ModelResponse> {
        let marker = request.marker()?;
        let request_hash = request.canonical_hash()?;
        let record_key = (
            request.scenario.clone(),
            request.actor.clone(),
            request.checkpoint.clone(),
            request_hash.clone(),
            request.dialect.clone(),
        );
        let semantic_ordinal = self
            .semantic_ordinals
            .get(&(
                request.scenario.clone(),
                request.actor.clone(),
                request.checkpoint.clone(),
            ))
            .copied()
            .unwrap_or(1);
        let semantic_key = (
            request.scenario.clone(),
            request.actor.clone(),
            request.checkpoint.clone(),
            semantic_ordinal,
        );
        let attempt = {
            let mut sequences = self.attempt_sequences.lock().await;
            let attempt = sequences.entry(semantic_key).or_default();
            *attempt = attempt.checked_add(1).ok_or_else(|| {
                AhrbError::Protocol("request attempt counter overflow".to_owned())
            })?;
            *attempt
        };
        let input_tokens = self.context_window_tokens.map(|_| {
            crate::wave3_long_horizon::fake_context_input_tokens(
                &request.dialect,
                &request.canonical,
            )
        });
        {
            let mut records = self.requests.lock().await;
            let attempts = records.entry(record_key.clone()).or_default();
            attempts.push(ModelRequestRecord {
                request: request.clone(),
                canonical_hash: request_hash.clone(),
                attempts: attempt,
                accepted: false,
                semantic_ordinal,
                attempt,
                received_ns,
                body_bytes,
                input_tokens,
                role: "unclassified".to_owned(),
                side_channel_kind: Some("unknown-side-channel".to_owned()),
                response_status: None,
                response_headers_ns: None,
                response_first_frame_yield_ns: None,
                response_last_frame_yield_ns: None,
                semantic_attempts_total: attempt,
            });
        }

        let recognition = self.machine.recognize(&marker).await;
        let route = (
            marker.scenario.clone(),
            marker.actor.clone(),
            marker.checkpoint.clone(),
        );
        let context_faulted = self.context_faulted_routes.lock().await.contains(&route);
        if recognition == TransitionRecognition::Retry && context_faulted {
            let response = self
                .machine
                .response_for_checkpoint(&marker)
                .ok_or_else(|| {
                    AhrbError::Protocol(
                        "context recovery retry lacks its scripted checkpoint".to_owned(),
                    )
                })?;
            let window_tokens = match response.fault {
                Some(Fault::ContextLength { window_tokens }) => window_tokens,
                _ => {
                    return Err(AhrbError::Protocol(
                        "context recovery route no longer carries a context-length fixture"
                            .to_owned(),
                    ));
                }
            };
            let observed_tokens = input_tokens.ok_or_else(|| {
                AhrbError::Protocol(
                    "context recovery retry lacks provider token evidence".to_owned(),
                )
            })?;
            let accepted =
                observed_tokens <= window_tokens && body_bytes <= window_tokens.saturating_mul(8);
            self.mark_request_accepted(&record_key, "primary", None)
                .await?;
            let value = adapt_scripted_tool_calls(&response.response, &request.canonical)?;
            return Ok(ModelResponse {
                dialect: request.dialect,
                model: request.model,
                scenario: marker.scenario,
                actor: marker.actor,
                checkpoint: marker.checkpoint,
                request_hash,
                attempt,
                value,
                fault: (!accepted).then_some(Fault::ContextLength { window_tokens }),
                retry: true,
                stream: request.stream,
            });
        }

        if matches!(
            recognition,
            TransitionRecognition::Current | TransitionRecognition::Retry
        ) {
            let accepted = self.machine.accept(&marker, &request_hash).await?;
            self.mark_request_accepted(&record_key, "primary", None)
                .await?;
            if let Some(barrier) = &accepted.response.barrier {
                self.barriers.arrive(barrier, &marker).await?;
                self.barriers.wait_for_release(barrier).await?;
            }
            let value = adapt_scripted_tool_calls(&accepted.response.response, &request.canonical)?;
            if let Some(Fault::ContextLength { window_tokens }) = accepted.response.fault.as_ref() {
                if self.context_window_tokens != Some(*window_tokens) {
                    return Err(AhrbError::Protocol(format!(
                        "context-length fixture declares W={window_tokens}, provider advertises {:?}",
                        self.context_window_tokens
                    )));
                }
                let observed_tokens = input_tokens.ok_or_else(|| {
                    AhrbError::Protocol(
                        "context-length fixture lacks provider token evidence".to_owned(),
                    )
                })?;
                let expected_tokens = window_tokens.saturating_add(256);
                let expected_body = window_tokens
                    .checked_mul(8)
                    .and_then(|value| value.checked_add(1_024))
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "context-length fixture byte target overflow".to_owned(),
                        )
                    })?;
                if observed_tokens != expected_tokens || body_bytes != expected_body {
                    return Err(AhrbError::Protocol(format!(
                        "context-length pre-error request measured {observed_tokens} tokens/{body_bytes} bytes; expected {expected_tokens}/{expected_body}"
                    )));
                }
                self.context_faulted_routes.lock().await.insert(route);
                return Ok(ModelResponse {
                    dialect: request.dialect,
                    model: request.model,
                    scenario: marker.scenario,
                    actor: marker.actor,
                    checkpoint: marker.checkpoint,
                    request_hash,
                    attempt,
                    value,
                    fault: Some(Fault::ContextLength {
                        window_tokens: *window_tokens,
                    }),
                    retry: accepted.retry,
                    stream: request.stream,
                });
            }

            return Ok(ModelResponse {
                dialect: request.dialect,
                model: request.model,
                scenario: marker.scenario,
                actor: marker.actor,
                checkpoint: marker.checkpoint,
                request_hash,
                attempt,
                value,
                fault: accepted.response.fault,
                retry: accepted.retry,
                stream: request.stream,
            });
        }

        let classified_side_channel = self.classify_side_channel(&request)?;
        if let Some(kind) = classified_side_channel {
            self.mark_request_accepted(&record_key, "side-channel", Some(kind.as_str()))
                .await?;
            return Ok(ModelResponse {
                dialect: request.dialect,
                model: request.model,
                scenario: marker.scenario,
                actor: marker.actor,
                checkpoint: marker.checkpoint,
                request_hash,
                attempt,
                value: json!({"text": if kind == SideChannelKind::Title { DETERMINISTIC_THREAD_TITLE } else { "AHRB deterministic auxiliary response" }}),
                fault: None,
                retry: attempt > 1,
                stream: request.stream,
            });
        }

        self.mark_request_accepted(&record_key, "side-channel", Some("unknown-side-channel"))
            .await?;
        Ok(ModelResponse {
            dialect: request.dialect,
            model: request.model,
            scenario: marker.scenario,
            actor: marker.actor,
            checkpoint: marker.checkpoint,
            request_hash,
            attempt,
            value: json!({"text": "AHRB deterministic unclassified auxiliary response"}),
            fault: None,
            retry: attempt > 1,
            stream: request.stream,
        })
    }

    /// Return immutable access to state-based barrier coordination.
    pub fn barriers(&self) -> &BarrierCoordinator {
        &self.barriers
    }

    /// Return deterministic request evidence sorted independently of arrival order.
    pub async fn request_records(&self) -> Vec<ModelRequestRecord> {
        let records = self.requests.lock().await;
        let frame_boundaries = self
            .frame_boundaries
            .lock()
            .map(|boundaries| boundaries.clone())
            .unwrap_or_default();
        let mut output = Vec::new();
        for attempts in records.values() {
            for record in attempts {
                output.push(record.clone());
            }
        }
        output.sort_by(|left, right| {
            (
                &left.request.scenario,
                &left.request.actor,
                left.semantic_ordinal,
                &left.request.checkpoint,
                left.attempt,
            )
                .cmp(&(
                    &right.request.scenario,
                    &right.request.actor,
                    right.semantic_ordinal,
                    &right.request.checkpoint,
                    right.attempt,
                ))
        });
        let totals = output.iter().fold(
            BTreeMap::<SemanticRequestKey, u64>::new(),
            |mut totals, record| {
                let key = (
                    record.request.scenario.clone(),
                    record.request.actor.clone(),
                    record.request.checkpoint.clone(),
                    record.semantic_ordinal,
                );
                totals
                    .entry(key)
                    .and_modify(|total| *total = (*total).max(record.attempt))
                    .or_insert(record.attempt);
                totals
            },
        );
        for record in &mut output {
            if let Some((first, last)) = frame_boundaries.get(&(
                record.request.scenario.clone(),
                record.request.actor.clone(),
                record.request.checkpoint.clone(),
                record.attempt,
            )) {
                record.response_first_frame_yield_ns = Some(*first);
                record.response_last_frame_yield_ns = Some(*last);
            }
            let key = (
                record.request.scenario.clone(),
                record.request.actor.clone(),
                record.request.checkpoint.clone(),
                record.semantic_ordinal,
            );
            let total = totals.get(&key).copied().unwrap_or(record.attempt);
            record.attempts = total;
            record.semantic_attempts_total = total;
        }
        output
    }

    /// Return a stable snapshot of out-of-band provider frame-yield evidence.
    ///
    /// The synchronous lock is held only while cloning the short observation
    /// vector. This permits `Body::poll_frame` to record a boundary without
    /// blocking on an async task or instrumenting the harness data path.
    pub fn frame_observations(&self) -> Result<Vec<ModelFrameObservation>> {
        let observations = self.frame_observations.lock().map_err(|_| {
            AhrbError::Protocol("fake-model frame observation lock was poisoned".to_owned())
        })?;
        let mut output = observations.clone();
        output.sort_by(|left, right| {
            (
                &left.scenario,
                &left.actor,
                &left.checkpoint,
                left.attempt,
                left.ordinal,
            )
                .cmp(&(
                    &right.scenario,
                    &right.actor,
                    &right.checkpoint,
                    right.attempt,
                    right.ordinal,
                ))
        });
        Ok(output)
    }

    fn frame_observation_sink(&self, response: &ModelResponse) -> FrameObservationSink {
        FrameObservationSink {
            observations: Arc::clone(&self.frame_observations),
            boundaries: Arc::clone(&self.frame_boundaries),
            scenario: response.scenario.clone(),
            actor: response.actor.clone(),
            checkpoint: response.checkpoint.clone(),
            attempt: response.attempt,
        }
    }

    async fn mark_request_accepted(
        &self,
        record_key: &RequestRecordKey,
        role: &str,
        side_channel_kind: Option<&str>,
    ) -> Result<()> {
        let mut records = self.requests.lock().await;
        let Some(record) = records
            .get_mut(record_key)
            .and_then(|records| records.last_mut())
        else {
            return Err(AhrbError::Protocol(
                "request evidence disappeared during transition".to_owned(),
            ));
        };
        record.accepted = true;
        record.role = role.to_owned();
        record.side_channel_kind = side_channel_kind.map(str::to_owned);
        Ok(())
    }

    fn classify_side_channel(&self, request: &ModelRequest) -> Result<Option<SideChannelKind>> {
        for rule in &self.request_role_rules {
            if !rule.model_ids.is_empty()
                && !rule.model_ids.iter().any(|model| model == &request.model)
            {
                continue;
            }
            if !rule.json_pointer.is_empty() {
                let Some(value) = request.canonical.pointer(&rule.json_pointer) else {
                    continue;
                };
                let candidate = match value {
                    Value::String(value) => value.clone(),
                    _ => serde_json::to_string(value)?,
                };
                let expression = regex::Regex::new(&rule.regex).map_err(|error| {
                    AhrbError::Validation(format!(
                        "invalid request-role regex after manifest validation: {error}"
                    ))
                })?;
                if !expression.is_match(&candidate) {
                    continue;
                }
            }
            return Ok(Some(rule.kind));
        }
        Ok(None)
    }

    async fn mark_response_status(&self, response: &ModelResponse, status: u16) -> Result<u64> {
        let headers_ns = monotonic_timestamp_ns();
        let key = (
            response.scenario.clone(),
            response.actor.clone(),
            response.checkpoint.clone(),
            response.request_hash.clone(),
            response.dialect.clone(),
        );
        let mut records = self.requests.lock().await;
        let Some(record) = records.get_mut(&key).and_then(|records| {
            records
                .iter_mut()
                .find(|record| record.attempt == response.attempt)
        }) else {
            return Err(AhrbError::Protocol(
                "request evidence disappeared before response status recording".to_owned(),
            ));
        };
        record.response_status = Some(status);
        record.response_headers_ns = Some(headers_ns);
        Ok(headers_ns)
    }

    async fn mark_response_frame_yield(
        &self,
        response: &ModelResponse,
        yielded_ns: u64,
    ) -> Result<()> {
        let key = (
            response.scenario.clone(),
            response.actor.clone(),
            response.checkpoint.clone(),
            response.request_hash.clone(),
            response.dialect.clone(),
        );
        let mut records = self.requests.lock().await;
        let Some(record) = records.get_mut(&key).and_then(|records| {
            records
                .iter_mut()
                .find(|record| record.attempt == response.attempt)
        }) else {
            return Err(AhrbError::Protocol(
                "request evidence disappeared before response frame recording".to_owned(),
            ));
        };
        record
            .response_first_frame_yield_ns
            .get_or_insert(yielded_ns);
        record.response_last_frame_yield_ns = Some(yielded_ns);
        Ok(())
    }
}

/// System-wide monotonic nanoseconds suitable for cross-process evidence correlation.
pub fn monotonic_timestamp_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid writable timespec and CLOCK_MONOTONIC is system-wide.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
        return 0;
    }
    let seconds = u64::try_from(time.tv_sec).unwrap_or(0);
    let nanoseconds = u64::try_from(time.tv_nsec).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(nanoseconds)
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
    physical_requests: Arc<AtomicU64>,
    credential_rejections: Arc<AtomicU64>,
}

impl FakeModelServer {
    /// Bind a loopback/local address and start serving all built-in protocol frontends.
    pub async fn bind(addr: SocketAddr, engine: Arc<FakeModelEngine>) -> Result<Self> {
        Self::bind_with_credential_policy(addr, engine, None).await
    }

    /// Bind a fake provider that rejects every provider request whose API
    /// credential does not exactly match `credential`. This is reserved for
    /// row-65's active credential-carrier trap.
    pub(crate) async fn bind_requiring_credential(
        addr: SocketAddr,
        engine: Arc<FakeModelEngine>,
        credential: String,
    ) -> Result<Self> {
        Self::bind_with_credential_policy(addr, engine, Some(Arc::<str>::from(credential))).await
    }

    async fn bind_with_credential_policy(
        addr: SocketAddr,
        engine: Arc<FakeModelEngine>,
        required_credential: Option<Arc<str>>,
    ) -> Result<Self> {
        let listener = retry_transient_bind(|| TcpListener::bind(addr)).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let task_engine = Arc::clone(&engine);
        let physical_requests = Arc::new(AtomicU64::new(0));
        let credential_rejections = Arc::new(AtomicU64::new(0));
        let task_physical_requests = Arc::clone(&physical_requests);
        let task_credential_rejections = Arc::clone(&credential_rejections);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let connection_engine = Arc::clone(&task_engine);
                        let connection_physical_requests = Arc::clone(&task_physical_requests);
                        let connection_credential_rejections =
                            Arc::clone(&task_credential_rejections);
                        let connection_required_credential = required_credential.clone();
                        tokio::spawn(async move {
                            let service = service_fn(move |request| {
                                serve_counted_request(
                                    request,
                                    Arc::clone(&connection_engine),
                                    Arc::clone(&connection_physical_requests),
                                    Arc::clone(&connection_credential_rejections),
                                    connection_required_credential.clone(),
                                )
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
            physical_requests,
            credential_rejections,
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

    /// Number of physical HTTP requests received by this listener, including
    /// catalog probes, malformed requests, and authentication rejections.
    pub(crate) fn physical_request_count(&self) -> u64 {
        self.physical_requests.load(Ordering::Acquire)
    }

    /// Number of physical provider requests actively rejected by the row-65
    /// credential policy.
    pub(crate) fn credential_rejection_count(&self) -> u64 {
        self.credential_rejections.load(Ordering::Acquire)
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

/// One preconnected HTTP/1 fake-provider transport for hosts that deny bind(2).
#[cfg(unix)]
#[derive(Debug)]
pub struct FakeModelPreconnectedServer {
    peer: std::os::unix::net::UnixStream,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

#[cfg(unix)]
impl FakeModelPreconnectedServer {
    /// Create a connected Unix stream pair, serving Hyper on the runner-owned end.
    pub fn pair(engine: Arc<FakeModelEngine>) -> Result<Self> {
        let (provider, peer) = std::os::unix::net::UnixStream::pair()?;
        provider.set_nonblocking(true)?;
        clear_close_on_exec(peer.as_raw_fd())?;
        let provider = tokio::net::UnixStream::from_std(provider)?;
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let service = service_fn(move |request| serve_request(request, Arc::clone(&engine)));
            let connection =
                http1::Builder::new().serve_connection(TokioIo::new(provider), service);
            tokio::pin!(connection);
            tokio::select! {
                result = &mut connection => {
                    if result.is_err() {
                        // Scripted disconnect faults intentionally end a connection.
                    }
                }
                _ = &mut shutdown_receiver => {}
            }
            Ok(())
        });
        Ok(Self {
            peer,
            shutdown: Some(shutdown_sender),
            task,
        })
    }

    /// Inheritable connected peer descriptor passed only to the reference mocks.
    pub fn peer_fd(&self) -> RawFd {
        self.peer.as_raw_fd()
    }

    /// Stop the one-connection provider task.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(sender) = self.shutdown.take() {
            let _sent = sender.send(());
        }
        self.task.await.map_err(|error| {
            AhrbError::Protocol(format!("preconnected fake-model task: {error}"))
        })?
    }
}

#[cfg(unix)]
fn clear_close_on_exec(fd: RawFd) -> Result<()> {
    // SAFETY: fd is owned by `peer` and F_GETFD does not mutate memory.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: fd remains owned and valid; F_SETFD updates only descriptor flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Atomic raw HTTP envelope used when the host denies socket creation.
///
/// This is an actual provider transport, not telemetry: the harness receives no model
/// response until the runner-owned provider sidecar consumes this envelope.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderMailboxRequest {
    /// Transport correlation ID; never used for semantic workflow routing.
    pub id: String,
    /// Exact HTTP method.
    pub method: String,
    /// Exact HTTP request path.
    pub path: String,
    /// Exact request headers.
    pub headers: BTreeMap<String, String>,
    /// Exact raw body bytes before parsing.
    pub body: Vec<u8>,
}

/// Atomic provider response paired 1:1 with a mailbox request.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProviderMailboxResponse {
    /// Correlation ID copied from the request envelope.
    pub id: String,
    /// HTTP response status when a response was produced.
    pub status: u16,
    /// Exact rendered response body.
    pub body: Vec<u8>,
    /// Transport/protocol failure when no HTTP response can be represented.
    pub error: Option<String>,
}

/// Runner-owned fake-provider sidecar for restricted hosts without sockets.
#[derive(Debug)]
pub struct FakeModelMailboxServer {
    directory: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
    physical_requests: Arc<AtomicU64>,
    credential_rejections: Arc<AtomicU64>,
}

impl FakeModelMailboxServer {
    /// Create a fresh atomic-envelope provider transport.
    pub async fn bind(directory: PathBuf, engine: Arc<FakeModelEngine>) -> Result<Self> {
        Self::bind_with_credential_policy(directory, engine, None).await
    }

    /// Create a mailbox provider that actively rejects every provider request
    /// whose credential is not exactly `credential`.
    pub(crate) async fn bind_requiring_credential(
        directory: PathBuf,
        engine: Arc<FakeModelEngine>,
        credential: String,
    ) -> Result<Self> {
        Self::bind_with_credential_policy(directory, engine, Some(Arc::<str>::from(credential)))
            .await
    }

    async fn bind_with_credential_policy(
        directory: PathBuf,
        engine: Arc<FakeModelEngine>,
        required_credential: Option<Arc<str>>,
    ) -> Result<Self> {
        tokio::fs::create_dir(&directory).await?;
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let task_directory = directory.clone();
        let physical_requests = Arc::new(AtomicU64::new(0));
        let credential_rejections = Arc::new(AtomicU64::new(0));
        let task_physical_requests = Arc::clone(&physical_requests);
        let task_credential_rejections = Arc::clone(&credential_rejections);
        let task = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => {
                        requests.abort_all();
                        while requests.join_next().await.is_some() {}
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(2)) => {
                        let mut entries = tokio::fs::read_dir(&task_directory).await?;
                        let mut paths = Vec::new();
                        while let Some(entry) = entries.next_entry().await? {
                            let path = entry.path();
                            if path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.ends_with(".request.json")) {
                                paths.push(path);
                            }
                        }
                        paths.sort();
                        for path in paths {
                            let claimed = path.with_extension("claimed");
                            match tokio::fs::rename(&path, &claimed).await {
                                Ok(()) => {}
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                                Err(error) => return Err(error.into()),
                            }
                            let bytes = tokio::fs::read(&claimed).await?;
                            let received_ns = monotonic_timestamp_ns();
                            tokio::fs::remove_file(&claimed).await?;
                            let envelope: ProviderMailboxRequest = serde_json::from_slice(&bytes)?;
                            task_physical_requests.fetch_add(1, Ordering::AcqRel);
                            let rejected = required_credential.as_deref().is_some_and(|expected| {
                                !provider_headers_use_credential(&envelope.headers, expected)
                            });
                            if rejected {
                                task_credential_rejections.fetch_add(1, Ordering::AcqRel);
                            }
                            let response_directory = task_directory.clone();
                            let response_engine = Arc::clone(&engine);
                            requests.spawn(async move {
                                let response = if rejected {
                                    Ok(ProviderMailboxResponse {
                                        id: envelope.id.clone(),
                                        status: 401,
                                        body: b"{\"error\":{\"type\":\"authentication_error\"}}"
                                            .to_vec(),
                                        error: None,
                                    })
                                } else {
                                    handle_provider_mailbox_request(
                                        &envelope,
                                        received_ns,
                                        response_engine,
                                    )
                                    .await
                                };
                                let response = match response {
                                    Ok(response) => response,
                                    Err(error) => ProviderMailboxResponse {
                                        id: envelope.id.clone(),
                                        status: 0,
                                        body: Vec::new(),
                                        error: Some(error.to_string()),
                                    },
                                };
                                write_provider_mailbox_response(&response_directory, &response).await
                            });
                        }
                        while requests.try_join_next().is_some() {}
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            directory,
            shutdown: Some(shutdown_sender),
            task,
            physical_requests,
            credential_rejections,
        })
    }

    /// Provider transport directory passed to the reference harness.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Number of physical provider envelopes claimed before parsing.
    pub(crate) fn physical_request_count(&self) -> u64 {
        self.physical_requests.load(Ordering::Acquire)
    }

    /// Number of envelopes rejected by the active credential policy.
    pub(crate) fn credential_rejection_count(&self) -> u64 {
        self.credential_rejections.load(Ordering::Acquire)
    }

    /// Stop the sidecar and remove its fresh transport directory.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(sender) = self.shutdown.take() {
            let _sent = sender.send(());
        }
        self.task
            .await
            .map_err(|error| AhrbError::Protocol(format!("provider mailbox task: {error}")))??;
        tokio::fs::remove_dir_all(&self.directory).await?;
        Ok(())
    }
}

fn provider_headers_use_credential(headers: &BTreeMap<String, String>, expected: &str) -> bool {
    let value = headers.iter().find_map(|(name, value)| {
        (name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("x-api-key"))
            .then_some(value.as_str())
    });
    value.is_some_and(|value| {
        value == expected
            || value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
                == Some(expected)
    })
}

async fn handle_provider_mailbox_request(
    envelope: &ProviderMailboxRequest,
    received_ns: u64,
    engine: Arc<FakeModelEngine>,
) -> Result<ProviderMailboxResponse> {
    if envelope.method != "POST" {
        return Err(AhrbError::Protocol(format!(
            "provider mailbox only accepts POST, not {:?}",
            envelope.method
        )));
    }
    let frontend: &dyn ProtocolFrontend = match envelope.path.as_str() {
        "/v1/chat/completions" => &OpenAiChatFrontend,
        "/v1/responses" => &OpenAiResponsesFrontend,
        "/v1/messages" => &AnthropicMessagesFrontend,
        other => {
            return Err(AhrbError::Protocol(format!(
                "unexpected provider mailbox path {other:?}"
            )));
        }
    };
    let body_bytes = u64::try_from(envelope.body.len()).map_err(|_| {
        AhrbError::Protocol("provider mailbox body length does not fit u64".to_owned())
    })?;
    let headers = canonicalize_provider_headers(&envelope.headers)?;
    let parsed = frontend.parse(&envelope.path, &headers, &envelope.body)?;
    let selected = engine
        .handle_observed(parsed, body_bytes, received_ns)
        .await?;
    let (status, body) = match &selected.fault {
        Some(Fault::HttpStatus { status, body } | Fault::SustainedHttpStatus { status, body }) => {
            (*status, body.as_bytes().to_vec())
        }
        Some(Fault::ContextLength { window_tokens }) => (
            400,
            serde_json::to_vec(
                &json!({"error":{"type":"context_length_exceeded","code":"context_length_exceeded","context_window":window_tokens}}),
            )?,
        ),
        Some(Fault::Stall) => std::future::pending::<(u16, Vec<u8>)>().await,
        Some(Fault::MidStreamDisconnect { .. }) => {
            return Err(AhrbError::Protocol(
                "provider mailbox injected a mid-stream disconnect".to_owned(),
            ));
        }
        Some(Fault::Trickle { .. }) => {
            return Err(AhrbError::Protocol(
                "provider mailbox cannot represent timed trickle frames".to_owned(),
            ));
        }
        Some(Fault::Delay { delay_ms }) => {
            tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
            let rendered = frontend.render(&selected)?;
            (rendered.status, rendered.body)
        }
        None | Some(Fault::Fragment { .. }) | Some(Fault::RepeatFrame { .. }) => {
            let rendered = frontend.render(&selected)?;
            (rendered.status, rendered.body)
        }
    };
    let _headers_ns = engine.mark_response_status(&selected, status).await?;
    engine
        .mark_response_frame_yield(&selected, monotonic_timestamp_ns())
        .await?;
    Ok(ProviderMailboxResponse {
        id: envelope.id.clone(),
        status,
        body,
        error: None,
    })
}

async fn write_provider_mailbox_response(
    directory: &Path,
    response: &ProviderMailboxResponse,
) -> Result<()> {
    let final_path = directory.join(format!("{}.response.json", response.id));
    let temporary = directory.join(format!("{}.response.tmp", response.id));
    tokio::fs::write(&temporary, serde_json::to_vec(response)?).await?;
    tokio::fs::rename(temporary, final_path).await?;
    Ok(())
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

fn request_uses_credential(request: &Request<Incoming>, expected: &str) -> bool {
    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok());
    let api_key = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());
    authorization.is_some_and(|value| {
        value == expected
            || value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
                == Some(expected)
    }) || api_key == Some(expected)
}

async fn serve_counted_request(
    request: Request<Incoming>,
    engine: Arc<FakeModelEngine>,
    physical_requests: Arc<AtomicU64>,
    credential_rejections: Arc<AtomicU64>,
    required_credential: Option<Arc<str>>,
) -> std::result::Result<Response<DeterministicBody>, Infallible> {
    physical_requests.fetch_add(1, Ordering::AcqRel);
    let provider_path = request.uri().path() != "/healthz";
    let rejected = required_credential
        .as_deref()
        .is_some_and(|expected| provider_path && !request_uses_credential(&request, expected));
    if rejected {
        credential_rejections.fetch_add(1, Ordering::AcqRel);
        let response = match response_from_parts(
            401,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::from_static(
                b"{\"error\":{\"type\":\"authentication_error\"}}",
            )),
        ) {
            Ok(response) => response,
            Err(error) => diagnostic_response(&error),
        };
        return Ok(response);
    }
    serve_request(request, engine).await
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
        let model = match engine.context_window_tokens() {
            Some(tokens) => json!({
                "object":"list",
                "data":[{
                    "id":"ahrb-fake-v1",
                    "object":"model",
                    "context_window":tokens,
                    "context_length":tokens,
                }]
            }),
            None => json!({
                "object":"list",
                "data":[{"id":"ahrb-fake-v1","object":"model"}]
            }),
        };
        return response_from_parts(
            200,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::from(serde_json::to_vec(&model)?)),
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
    let received_ns = monotonic_timestamp_ns();
    let body_bytes = u64::try_from(body.len()).map_err(|_| {
        AhrbError::Protocol("fake-model request body length does not fit u64".to_owned())
    })?;
    let parsed = frontend.parse(&path, &headers, &body)?;
    let selected = engine
        .handle_observed(parsed, body_bytes, received_ns)
        .await?;
    if let Some(Fault::HttpStatus { status, body } | Fault::SustainedHttpStatus { status, body }) =
        &selected.fault
    {
        let _headers_ns = engine.mark_response_status(&selected, *status).await?;
        return response_from_parts(
            *status,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            DeterministicBody::full(Bytes::copy_from_slice(body.as_bytes())),
        );
    }
    if let Some(Fault::ContextLength { window_tokens }) = &selected.fault {
        let headers_ns = engine.mark_response_status(&selected, 400).await?;
        let body = serde_json::to_vec(&json!({
            "error": {
                "type": "context_length_exceeded",
                "code": "context_length_exceeded",
                "message": format!("maximum context length is {window_tokens} tokens"),
                "context_window": window_tokens,
            }
        }))?;
        let observation_sink = engine.frame_observation_sink(&selected);
        let response_body = DeterministicBody::from_fault(
            body,
            selected.fault.as_ref(),
            Some(observation_sink),
            Some(headers_ns),
        )?;
        return response_from_parts(
            400,
            &BTreeMap::from([("content-type".to_owned(), "application/json".to_owned())]),
            response_body,
        );
    }

    if let Some(Fault::Delay { delay_ms }) = selected.fault.as_ref() {
        tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
    }
    let mut rendered = frontend.render(&selected)?;
    if let Some(Fault::Trickle { count, .. }) = selected.fault.as_ref() {
        rendered.body = trickle_success_body(*count)?;
    }
    let headers_ns = engine
        .mark_response_status(&selected, rendered.status)
        .await?;
    let observation_sink = engine.frame_observation_sink(&selected);
    let response_body = DeterministicBody::from_fault(
        rendered.body,
        selected.fault.as_ref(),
        Some(observation_sink),
        Some(headers_ns),
    )?;
    response_from_parts(rendered.status, &rendered.headers, response_body)
}

fn trickle_success_body(count: u32) -> Result<Vec<u8>> {
    const MARKER: &[u8] = b"AHRB-TRICKLE-SUCCESS";
    let count = usize::try_from(count)
        .map_err(|_| AhrbError::Protocol("trickle count does not fit usize".to_owned()))?;
    Ok(MARKER.iter().copied().cycle().take(count).collect())
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

fn canonicalize_provider_headers(
    headers: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut canonical = BTreeMap::new();
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if canonical.insert(lower.clone(), value.clone()).is_some() {
            return Err(AhrbError::Protocol(format!(
                "provider mailbox repeats header {lower:?} case-insensitively"
            )));
        }
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
    if let Some(reasoning) = response.value.get("_ahrb_reasoning") {
        message.insert("reasoning_content".to_owned(), reasoning.clone());
    }
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
    let estimated_output = nonzero_token_estimate(&Value::Object(message.clone()));
    let (prompt_tokens, completion_tokens) = semantic_usage(&response.value, estimated_output)?;
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
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": completion_tokens.saturating_add(prompt_tokens)
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
    if let Some(reasoning) = response.value.get("_ahrb_reasoning") {
        output.push(json!({
            "id": stable_id("reasoning", response),
            "type": "reasoning",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": reasoning}]
        }));
    }
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
    let estimated_output = nonzero_token_estimate(&Value::Array(output.clone()));
    let (input_tokens, output_tokens) = semantic_usage(&response.value, estimated_output)?;
    Ok(json!({
        "id": stable_id("resp", response),
        "object": "response",
        "created_at": 1_700_000_000_u64,
        "status": "completed",
        "model": response.model,
        "output": output,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": output_tokens.saturating_add(input_tokens)
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
    if let Some(reasoning) = response.value.get("_ahrb_reasoning") {
        content.push(json!({
            "type": "thinking",
            "thinking": reasoning,
            "signature": stable_id("thinking", response)
        }));
    }
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
    let estimated_output = nonzero_token_estimate(&Value::Array(content.clone()));
    let (input_tokens, output_tokens) = semantic_usage(&response.value, estimated_output)?;
    Ok(json!({
        "id": stable_id("msg", response),
        "type": "message",
        "role": "assistant",
        "model": response.model,
        "content": content,
        "stop_reason": if tool_calls.is_empty() { "end_turn" } else { "tool_use" },
        "stop_sequence": null,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens}
    }))
}

fn semantic_usage(value: &Value, default_output_tokens: u64) -> Result<(u64, u64)> {
    let Some(usage) = value.get("_ahrb_usage") else {
        return Ok((1, default_output_tokens));
    };
    let usage = usage
        .as_object()
        .ok_or_else(|| AhrbError::Protocol("semantic _ahrb_usage must be an object".to_owned()))?;
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            AhrbError::Protocol("semantic _ahrb_usage.input_tokens must be u64".to_owned())
        })?;
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            AhrbError::Protocol("semantic _ahrb_usage.output_tokens must be u64".to_owned())
        })?;
    Ok((input_tokens, output_tokens))
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
            "reasoning" => {
                started.insert("summary".to_owned(), Value::Array(Vec::new()));
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
            "reasoning" => {
                let summary = item_object
                    .get("summary")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "OpenAI Responses reasoning item lacks summary[]".to_owned(),
                        )
                    })?;
                for (summary_index, part) in summary.iter().enumerate() {
                    let text = part.get("text").and_then(Value::as_str).ok_or_else(|| {
                        AhrbError::Protocol(
                            "OpenAI Responses reasoning summary lacks text".to_owned(),
                        )
                    })?;
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.reasoning_summary_part.added",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": summary_index,
                            "part": {"type":"summary_text","text":""}
                        }),
                    )?;
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.reasoning_summary_text.delta",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": summary_index,
                            "delta": text,
                        }),
                    )?;
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.reasoning_summary_text.done",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": summary_index,
                            "text": text,
                        }),
                    )?;
                    push_responses_event(
                        &mut body,
                        &mut sequence_number,
                        "response.reasoning_summary_part.done",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "summary_index": summary_index,
                            "part": part,
                        }),
                    )?;
                }
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

        if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
            push_chat_chunk(
                &mut body,
                id,
                model,
                &created,
                json!([{
                    "index": index,
                    "delta": {"reasoning_content": reasoning},
                    "finish_reason": null
                }]),
                None,
            )?;
        }
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
            "thinking" => (
                json!({"type": "thinking", "thinking": "", "signature": ""}),
                json!({
                    "type": "thinking_delta",
                    "thinking": block_object
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or("")
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
    ObservedFrames {
        frames: VecDeque<Bytes>,
        next_ordinal: u32,
        scheduled_ns: u64,
        observation_sink: FrameObservationSink,
    },
    Disconnect {
        prefix: Option<Bytes>,
        emitted_error: bool,
    },
    Stall,
    Trickle(TrickleBodyState),
}

/// HTTP body supporting deterministic fragmentation, disconnect, repetition, stall, and trickle.
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

    fn from_fault(
        bytes: Vec<u8>,
        fault: Option<&Fault>,
        observation_sink: Option<FrameObservationSink>,
        response_headers_ns: Option<u64>,
    ) -> Result<Self> {
        match fault {
            Some(Fault::ContextLength { .. }) => Ok(Self {
                state: BodyState::ObservedFrames {
                    frames: VecDeque::from([Bytes::from(bytes)]),
                    next_ordinal: 1,
                    scheduled_ns: response_headers_ns.unwrap_or_else(monotonic_timestamp_ns),
                    observation_sink: observation_sink.ok_or_else(|| {
                        AhrbError::Protocol(
                            "context-length body lacks frame observation sink".to_owned(),
                        )
                    })?,
                },
            }),
            None
            | Some(Fault::HttpStatus { .. })
            | Some(Fault::SustainedHttpStatus { .. })
            | Some(Fault::Delay { .. }) => Ok(Self::full(Bytes::from(bytes))),
            Some(Fault::Stall) => Ok(Self {
                state: BodyState::Stall,
            }),
            Some(Fault::Trickle { cadence_ms, count }) => {
                if *cadence_ms == 0 || *count == 0 {
                    return Err(AhrbError::Protocol(
                        "trickle cadence-ms and count must both be nonzero".to_owned(),
                    ));
                }
                let count_usize = usize::try_from(*count).map_err(|_| {
                    AhrbError::Protocol("trickle count does not fit usize".to_owned())
                })?;
                if count_usize != bytes.len() {
                    return Err(AhrbError::Protocol(format!(
                        "trickle response length {} does not equal declared frame count {count}",
                        bytes.len()
                    )));
                }
                let observation_sink = observation_sink.ok_or_else(|| {
                    AhrbError::Protocol("trickle body lacks frame observation sink".to_owned())
                })?;
                let origin_instant = response_headers_ns.map(|headers_ns| {
                    let observed_after_headers_ns = monotonic_timestamp_ns();
                    let elapsed =
                        Duration::from_nanos(observed_after_headers_ns.saturating_sub(headers_ns));
                    let now = tokio::time::Instant::now();
                    now.checked_sub(elapsed).unwrap_or(now)
                });
                Ok(Self {
                    state: BodyState::Trickle(TrickleBodyState {
                        payload: Bytes::from(bytes),
                        cadence_ms: *cadence_ms,
                        count: *count,
                        next_ordinal: 1,
                        origin_instant,
                        origin_ns: response_headers_ns,
                        sleep: None,
                        observation_sink,
                    }),
                })
            }
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
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.state {
            BodyState::Frames(frames) => {
                Poll::Ready(frames.pop_front().map(|bytes| Ok(Frame::data(bytes))))
            }
            BodyState::ObservedFrames {
                frames,
                next_ordinal,
                scheduled_ns,
                observation_sink,
            } => {
                let Some(bytes) = frames.pop_front() else {
                    return Poll::Ready(None);
                };
                let yielded_ns = monotonic_timestamp_ns();
                if let Err(error) = observation_sink.record(
                    *next_ordinal,
                    *scheduled_ns,
                    yielded_ns,
                    bytes.len() as u64,
                ) {
                    return Poll::Ready(Some(Err(error)));
                }
                *next_ordinal = next_ordinal.saturating_add(1);
                Poll::Ready(Some(Ok(Frame::data(bytes))))
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
            BodyState::Trickle(trickle) => trickle.poll_frame(context),
        }
    }

    fn is_end_stream(&self) -> bool {
        matches!(&self.state, BodyState::Frames(frames) if frames.is_empty())
            || matches!(&self.state, BodyState::ObservedFrames { frames, .. } if frames.is_empty())
            || matches!(
                &self.state,
                BodyState::Disconnect {
                    prefix: None,
                    emitted_error: true
                }
            )
            || matches!(&self.state, BodyState::Trickle(trickle) if trickle.is_end_stream())
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        if let BodyState::Frames(frames) | BodyState::ObservedFrames { frames, .. } = &self.state {
            let total = frames
                .iter()
                .fold(0_u64, |sum, frame| sum.saturating_add(frame.len() as u64));
            hint.set_exact(total);
        } else if let BodyState::Trickle(trickle) = &self.state {
            hint.set_exact(trickle.remaining_bytes());
        }
        hint
    }
}

#[derive(Clone, Debug)]
struct FrameObservationSink {
    observations: Arc<StdMutex<Vec<ModelFrameObservation>>>,
    boundaries: Arc<StdMutex<FrameBoundaryMap>>,
    scenario: String,
    actor: String,
    checkpoint: String,
    attempt: u64,
}

impl FrameObservationSink {
    fn record(
        &self,
        ordinal: u32,
        scheduled_ns: u64,
        frame_yielded_ns: u64,
        bytes: u64,
    ) -> std::io::Result<()> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| std::io::Error::other("fake-model frame observation lock was poisoned"))?;
        observations.push(ModelFrameObservation {
            scenario: self.scenario.clone(),
            actor: self.actor.clone(),
            checkpoint: self.checkpoint.clone(),
            attempt: self.attempt,
            ordinal,
            scheduled_ns,
            frame_yielded_ns,
            bytes,
        });
        drop(observations);
        let key = (
            self.scenario.clone(),
            self.actor.clone(),
            self.checkpoint.clone(),
            self.attempt,
        );
        let mut boundaries = self
            .boundaries
            .lock()
            .map_err(|_| std::io::Error::other("fake-model frame boundary lock was poisoned"))?;
        boundaries
            .entry(key)
            .and_modify(|boundary| boundary.1 = frame_yielded_ns)
            .or_insert((frame_yielded_ns, frame_yielded_ns));
        Ok(())
    }
}

#[derive(Debug)]
struct TrickleBodyState {
    payload: Bytes,
    cadence_ms: u64,
    count: u32,
    next_ordinal: u64,
    origin_instant: Option<tokio::time::Instant>,
    origin_ns: Option<u64>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    observation_sink: FrameObservationSink,
}

impl TrickleBodyState {
    fn poll_frame(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, std::io::Error>>> {
        if self.next_ordinal > u64::from(self.count) {
            return Poll::Ready(None);
        }
        if self.origin_instant.is_none() {
            self.origin_ns = Some(monotonic_timestamp_ns());
            self.origin_instant = Some(tokio::time::Instant::now());
        }
        if self.sleep.is_none() {
            let deadline = match self.scheduled_instant(self.next_ordinal) {
                Ok(deadline) => deadline,
                Err(error) => return Poll::Ready(Some(Err(error))),
            };
            self.sleep = Some(Box::pin(tokio::time::sleep_until(deadline)));
        }
        let Some(sleep) = self.sleep.as_mut() else {
            return Poll::Ready(Some(Err(std::io::Error::other(
                "trickle timer disappeared before polling",
            ))));
        };
        if sleep.as_mut().poll(context).is_pending() {
            return Poll::Pending;
        }

        let ordinal_u64 = self.next_ordinal;
        let ordinal = match u32::try_from(ordinal_u64) {
            Ok(ordinal) => ordinal,
            Err(_) => {
                return Poll::Ready(Some(Err(std::io::Error::other(
                    "trickle ordinal does not fit u32",
                ))));
            }
        };
        let scheduled_ns = match self.scheduled_ns(ordinal_u64) {
            Ok(scheduled_ns) => scheduled_ns,
            Err(error) => return Poll::Ready(Some(Err(error))),
        };
        let index = match usize::try_from(ordinal_u64.saturating_sub(1)) {
            Ok(index) => index,
            Err(_) => {
                return Poll::Ready(Some(Err(std::io::Error::other(
                    "trickle ordinal does not fit usize",
                ))));
            }
        };
        let Some(byte) = self.payload.get(index).copied() else {
            return Poll::Ready(Some(Err(std::io::Error::other(
                "trickle payload ended before its declared count",
            ))));
        };
        let frame_yielded_ns = monotonic_timestamp_ns();
        if let Err(error) = self
            .observation_sink
            .record(ordinal, scheduled_ns, frame_yielded_ns, 1)
        {
            return Poll::Ready(Some(Err(error)));
        }
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        self.sleep = None;
        Poll::Ready(Some(Ok(Frame::data(Bytes::copy_from_slice(&[byte])))))
    }

    fn scheduled_instant(&self, ordinal: u64) -> std::io::Result<tokio::time::Instant> {
        let origin = self
            .origin_instant
            .ok_or_else(|| std::io::Error::other("trickle timer lacks its monotonic origin"))?;
        let offset_ms = self
            .cadence_ms
            .checked_mul(ordinal)
            .ok_or_else(|| std::io::Error::other("trickle timer offset overflow"))?;
        origin
            .checked_add(Duration::from_millis(offset_ms))
            .ok_or_else(|| std::io::Error::other("trickle timer deadline overflow"))
    }

    fn scheduled_ns(&self, ordinal: u64) -> std::io::Result<u64> {
        let origin_ns = self
            .origin_ns
            .ok_or_else(|| std::io::Error::other("trickle evidence lacks its monotonic origin"))?;
        let offset_ns = self
            .cadence_ms
            .checked_mul(ordinal)
            .and_then(|millis| millis.checked_mul(1_000_000))
            .ok_or_else(|| std::io::Error::other("trickle evidence offset overflow"))?;
        origin_ns
            .checked_add(offset_ns)
            .ok_or_else(|| std::io::Error::other("trickle scheduled boundary overflow"))
    }

    fn remaining_bytes(&self) -> u64 {
        u64::from(self.count)
            .saturating_add(1)
            .saturating_sub(self.next_ordinal)
    }

    fn is_end_stream(&self) -> bool {
        self.next_ordinal > u64::from(self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{Actor, Barrier};
    use std::cell::Cell;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn semantic_usage_overrides_dialect_token_estimates() -> Result<()> {
        assert_eq!(
            semantic_usage(
                &json!({
                    "text": "SUCCESS",
                    "_ahrb_usage": {"input_tokens": 140, "output_tokens": 30}
                }),
                999,
            )?,
            (140, 30)
        );
        assert_eq!(semantic_usage(&json!({"text": "SUCCESS"}), 19)?, (1, 19));
        assert!(semantic_usage(&json!({"_ahrb_usage": {"input_tokens": "bad"}}), 1).is_err());
        Ok(())
    }

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
            dialect: "openai-chat-completions".to_owned(),
            model: "ahrb-fake-v1".to_owned(),
            scenario: "routing".to_owned(),
            actor: "root".to_owned(),
            checkpoint: "start".to_owned(),
            request_hash: "request-hash".to_owned(),
            attempt: 1,
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

    fn reasoning_response() -> ModelResponse {
        let mut response = streamed_tool_response();
        response.value = json!({
            "text": "answer",
            "_ahrb_reasoning": "provider reasoning"
        });
        response
    }

    #[tokio::test]
    async fn trickle_body_wakes_on_anchored_schedule_and_records_each_frame_once() -> Result<()> {
        use http_body_util::BodyExt as _;

        let mut workflow = simple_workflow();
        workflow.responses[0].fault = Some(Fault::Trickle {
            cadence_ms: 10,
            count: 3,
        });
        let engine = FakeModelEngine::new(&workflow)?;
        let frontend = OpenAiChatFrontend;
        let request = frontend.parse(frontend.path(), &BTreeMap::new(), &request_body())?;
        let response = engine.handle(request).await?;
        let response_headers_ns = monotonic_timestamp_ns();
        let mut body = DeterministicBody::from_fault(
            b"abc".to_vec(),
            response.fault.as_ref(),
            Some(engine.frame_observation_sink(&response)),
            Some(response_headers_ns),
        )?;
        assert_eq!(body.size_hint().exact(), Some(3));

        let mut yielded = Vec::new();
        for expected in b"abc" {
            let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
                .await
                .map_err(|_| AhrbError::Timeout("test trickle frame wake".to_owned()))?
                .ok_or_else(|| {
                    AhrbError::Protocol("test trickle ended before declared count".to_owned())
                })??;
            let data = frame.into_data().map_err(|_| {
                AhrbError::Protocol("test trickle yielded a non-data frame".to_owned())
            })?;
            assert_eq!(data.as_ref(), &[*expected]);
            yielded.push(data);
            if yielded.len() == 1 {
                // A delayed consumer must not shift later scheduled boundaries.
                tokio::time::sleep(Duration::from_millis(35)).await;
            }
        }
        assert!(body.frame().await.is_none());
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));

        let observations = engine.frame_observations()?;
        assert_eq!(observations.len(), 3);
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.ordinal)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(observations.iter().all(|observation| {
            observation.scenario == "routing"
                && observation.actor == "root"
                && observation.checkpoint == "start"
                && observation.attempt == 1
                && observation.bytes == 1
                && observation.frame_yielded_ns >= observation.scheduled_ns
        }));
        assert_eq!(
            observations[0].scheduled_ns,
            response_headers_ns.saturating_add(10_000_000)
        );
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].response_first_frame_yield_ns,
            Some(observations[0].frame_yielded_ns)
        );
        assert_eq!(
            records[0].response_last_frame_yield_ns,
            Some(observations[2].frame_yielded_ns)
        );
        assert_eq!(
            observations[1]
                .scheduled_ns
                .saturating_sub(observations[0].scheduled_ns),
            10_000_000
        );
        assert_eq!(
            observations[2]
                .scheduled_ns
                .saturating_sub(observations[1].scheduled_ns),
            10_000_000
        );
        Ok(())
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

    #[tokio::test]
    async fn current_checkpoint_precedes_ordered_side_channel_rules_and_unknown_is_retained()
    -> Result<()> {
        let workflow = simple_workflow();
        let rules = vec![RequestRoleRule {
            kind: SideChannelKind::Title,
            priority: 10,
            model_ids: vec!["ahrb-title-v1".to_owned()],
            json_pointer: String::new(),
            regex: String::new(),
        }];
        let engine = FakeModelEngine::with_request_roles(&workflow, &BTreeMap::new(), &rules)?;
        let frontend = OpenAiChatFrontend;
        let current_body = json!({
            "model": "ahrb-title-v1",
            "messages": [
                {"role": "system", "content": TITLE_SYSTEM_PROMPT},
                {"role": "user", "content": "go [[AHRB:scenario=routing;actor=root;checkpoint=start]]"}
            ],
            "tool_choice": null,
            "stream": false
        });
        let current = frontend.parse(
            frontend.path(),
            &BTreeMap::new(),
            &serde_json::to_vec(&current_body)?,
        )?;
        let selected = engine.handle(current).await?;
        assert_eq!(selected.value, json!({"text":"SUCCESS"}));

        let mut auxiliary_body = current_body.clone();
        auxiliary_body["messages"][1]["content"] =
            json!("title [[AHRB:scenario=routing;actor=root;checkpoint=aux]]");
        let auxiliary = frontend.parse(
            frontend.path(),
            &BTreeMap::new(),
            &serde_json::to_vec(&auxiliary_body)?,
        )?;
        assert_eq!(
            engine
                .handle(auxiliary)
                .await?
                .value
                .get("text")
                .and_then(Value::as_str),
            Some(DETERMINISTIC_THREAD_TITLE)
        );

        let mut unknown_body = auxiliary_body;
        unknown_body["model"] = json!("unmatched-model");
        unknown_body["messages"][1]["content"] =
            json!("other [[AHRB:scenario=routing;actor=root;checkpoint=other]]");
        let unknown = frontend.parse(
            frontend.path(),
            &BTreeMap::new(),
            &serde_json::to_vec(&unknown_body)?,
        )?;
        engine.handle(unknown).await?;

        let records = engine.request_records().await;
        assert_eq!(records.len(), 3);
        assert!(records.iter().any(|record| record.role == "primary"));
        assert!(
            records
                .iter()
                .any(|record| { record.side_channel_kind.as_deref() == Some("title") })
        );
        assert!(
            records.iter().any(|record| {
                record.side_channel_kind.as_deref() == Some("unknown-side-channel")
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn current_checkpoint_protocol_errors_never_fall_through_to_role_rules() -> Result<()> {
        let mut workflow = simple_workflow();
        workflow.responses[0].request_hash = "0".repeat(64);
        let rules = vec![RequestRoleRule {
            kind: SideChannelKind::Title,
            priority: 1,
            model_ids: vec!["ahrb-fake".to_owned()],
            json_pointer: String::new(),
            regex: String::new(),
        }];
        let engine = FakeModelEngine::with_request_roles(&workflow, &BTreeMap::new(), &rules)?;
        let frontend = OpenAiChatFrontend;
        let request = frontend.parse(frontend.path(), &BTreeMap::new(), &request_body())?;
        let error = engine
            .handle(request)
            .await
            .expect_err("a current-checkpoint hash mismatch must remain a protocol error");
        assert!(
            error
                .to_string()
                .contains("canonical request hash mismatch")
        );
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].role, "unclassified");
        Ok(())
    }

    #[tokio::test]
    async fn mailbox_normalizes_headers_and_captures_raw_body_before_provider_parsing() -> Result<()>
    {
        let body = request_body();
        let engine = Arc::new(FakeModelEngine::new(&simple_workflow())?);
        let response = handle_provider_mailbox_request(
            &ProviderMailboxRequest {
                id: "mailbox-test".to_owned(),
                method: "POST".to_owned(),
                path: "/v1/chat/completions".to_owned(),
                headers: BTreeMap::from([(
                    "Authorization".to_owned(),
                    "Bearer mailbox-secret".to_owned(),
                )]),
                body: body.clone(),
            },
            42,
            Arc::clone(&engine),
        )
        .await?;
        assert_eq!(response.status, 200);
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].received_ns, 42);
        assert_eq!(records[0].body_bytes, body.len() as u64);
        assert_eq!(
            records[0].request.credential_fingerprint,
            sha256_hex(b"Bearer mailbox-secret")
        );
        Ok(())
    }

    #[tokio::test]
    async fn records_sort_by_semantic_key_and_attempt_not_canonical_hash() -> Result<()> {
        let rules = vec![RequestRoleRule {
            kind: SideChannelKind::Title,
            priority: 1,
            model_ids: vec!["ahrb-aux".to_owned()],
            json_pointer: String::new(),
            regex: String::new(),
        }];
        let engine =
            FakeModelEngine::with_request_roles(&simple_workflow(), &BTreeMap::new(), &rules)?;
        let frontend = OpenAiChatFrontend;
        for content in ["z-body", "a-body"] {
            let body = serde_json::to_vec(&json!({
                "model": "ahrb-aux",
                "messages": [{
                    "role": "user",
                    "content": format!(
                        "{content} [[AHRB:scenario=routing;actor=root;checkpoint=aux]]"
                    )
                }]
            }))?;
            let request = frontend.parse(frontend.path(), &BTreeMap::new(), &body)?;
            engine.handle(request).await?;
        }
        let records = engine.request_records().await;
        assert_eq!(
            records
                .iter()
                .map(|record| record.attempt)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            records
                .iter()
                .all(|record| record.semantic_attempts_total == 2)
        );
        assert_eq!(
            records[0]
                .request
                .canonical
                .pointer("/messages/0/content")
                .and_then(Value::as_str),
            Some("z-body [[AHRB:scenario=routing;actor=root;checkpoint=aux]]")
        );
        Ok(())
    }

    #[tokio::test]
    async fn shared_title_model_non_title_request_reaches_scripted_barrier() -> Result<()> {
        let mut workflow = simple_workflow();
        workflow.responses[0].barrier = Some("steady".to_owned());
        workflow.barriers.insert(
            "steady".to_owned(),
            Barrier {
                name: "steady".to_owned(),
                actors: vec!["root".to_owned()],
                checkpoint: "start".to_owned(),
            },
        );
        let roles = BTreeMap::from([(
            "title".to_owned(),
            ModelRole {
                model: "ahrb-fake".to_owned(),
                required: false,
            },
        )]);
        let engine = Arc::new(FakeModelEngine::with_model_roles(&workflow, &roles)?);
        let frontend = OpenAiChatFrontend;
        let request = frontend.parse(frontend.path(), &BTreeMap::new(), &request_body())?;
        let request_engine = Arc::clone(&engine);
        let response_task = tokio::spawn(async move { request_engine.handle(request).await });

        tokio::time::timeout(
            Duration::from_secs(1),
            engine.barriers().wait_until_ready("steady"),
        )
        .await
        .map_err(|error| {
            AhrbError::Protocol(format!("scripted barrier was not reached: {error}"))
        })??;
        engine.barriers().release("steady").await?;
        let response = response_task
            .await
            .map_err(|error| AhrbError::Protocol(format!("request task failed: {error}")))??;

        assert_eq!(response.value, json!({"text": "SUCCESS"}));
        let records = engine.request_records().await;
        assert_eq!(records.len(), 1);
        assert!(records[0].accepted);
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
    async fn openai_chat_retry_identity_ignores_delivery_controls_but_rejects_changed_retry()
    -> Result<()> {
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
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.attempts == 2));
        assert_eq!(records[0].attempt, 1);
        assert_eq!(records[1].attempt, 2);

        let mut changed_body = streamed_body;
        changed_body["messages"][0]["content"] =
            json!("different [[AHRB:scenario=routing;actor=root;checkpoint=start]]");
        let changed_bytes = serde_json::to_vec(&changed_body)?;
        let changed = frontend.parse(frontend.path(), &BTreeMap::new(), &changed_bytes)?;
        assert_ne!(streamed.canonical_hash()?, changed.canonical_hash()?);
        let error = engine
            .handle(changed)
            .await
            .expect_err("a changed request at an accepted checkpoint is a protocol error");
        assert!(
            error
                .to_string()
                .contains("retried with different canonical request")
        );
        let records = engine.request_records().await;
        assert!(
            records.iter().any(|record| {
                record.side_channel_kind.as_deref() == Some("unknown-side-channel")
            })
        );
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
            dialect: "openai-responses".to_owned(),
            model: "ahrb-fake-v1".to_owned(),
            scenario: "native".to_owned(),
            actor: "root".to_owned(),
            checkpoint: "next".to_owned(),
            request_hash: "request-hash".to_owned(),
            attempt: 1,
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
                    dialect: "anthropic-messages".to_owned(),
                    model: manifest.fake_model.model.clone(),
                    scenario: "native".to_owned(),
                    actor: "claude".to_owned(),
                    checkpoint: "start".to_owned(),
                    request_hash: "claude-request-hash".to_owned(),
                    attempt: 1,
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
    fn every_frontend_preserves_fixture_reasoning_in_json_and_streams() -> Result<()> {
        let response = reasoning_response();
        let chat = render_chat_value(&response)?;
        assert_eq!(
            chat.pointer("/choices/0/message/reasoning_content")
                .and_then(Value::as_str),
            Some("provider reasoning")
        );
        let responses = render_responses_value(&response)?;
        assert_eq!(
            responses
                .pointer("/output/0/summary/0/text")
                .and_then(Value::as_str),
            Some("provider reasoning")
        );
        let anthropic = render_anthropic_value(&response)?;
        assert_eq!(
            anthropic
                .pointer("/content/0/thinking")
                .and_then(Value::as_str),
            Some("provider reasoning")
        );
        for (dialect, value) in [
            ("chat", chat),
            ("responses", responses),
            ("anthropic", anthropic),
        ] {
            let streamed = render_json_or_sse(value, true, dialect)?;
            assert!(
                streamed
                    .body
                    .windows(b"provider reasoning".len())
                    .any(|window| window == b"provider reasoning"),
                "{dialect} stream discarded provider reasoning"
            );
        }
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

    #[tokio::test]
    async fn credential_policy_actively_rejects_wrong_secret_and_counts_all_requests() -> Result<()>
    {
        let _listener_guard = LOCAL_SERVER_TEST_LOCK.lock().await;
        let _process_guard = acquire_process_server_test_lock()?;
        let engine = Arc::new(FakeModelEngine::new(&simple_workflow())?);
        let address = "127.0.0.1:0".parse().map_err(|error| {
            AhrbError::Validation(format!("invalid test socket address: {error}"))
        })?;
        let bound = FakeModelServer::bind_requiring_credential(
            address,
            Arc::clone(&engine),
            "credential-b".to_owned(),
        )
        .await;
        let server = match bound {
            Ok(server) => server,
            Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let mut rejected_stream = tokio::net::TcpStream::connect(server.local_addr()).await?;
        let rejected_request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer credential-a\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}",
            server.local_addr()
        );
        rejected_stream
            .write_all(rejected_request.as_bytes())
            .await?;
        let mut rejected_response = Vec::new();
        rejected_stream.read_to_end(&mut rejected_response).await?;
        assert!(rejected_response.starts_with(b"HTTP/1.1 401"));
        assert_eq!(server.physical_request_count(), 1);
        assert_eq!(server.credential_rejection_count(), 1);
        assert!(engine.request_records().await.is_empty());

        let mut accepted_stream = tokio::net::TcpStream::connect(server.local_addr()).await?;
        let body = request_body();
        let accepted_head = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer credential-b\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            server.local_addr(),
            body.len()
        );
        accepted_stream.write_all(accepted_head.as_bytes()).await?;
        accepted_stream.write_all(&body).await?;
        let mut accepted_response = Vec::new();
        accepted_stream.read_to_end(&mut accepted_response).await?;
        assert!(accepted_response.starts_with(b"HTTP/1.1 200"));
        assert_eq!(server.physical_request_count(), 2);
        assert_eq!(server.credential_rejection_count(), 1);
        assert_eq!(engine.request_records().await.len(), 1);
        server.shutdown().await?;
        let rebound_engine = Arc::new(FakeModelEngine::new(&simple_workflow())?);
        let rebound = FakeModelServer::bind(address, rebound_engine).await?;
        rebound.shutdown().await?;
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

    fn efficiency_record(turn: usize, role: &str, side_kind: Option<&str>) -> ModelRequestRecord {
        let canonical = json!({
            "model": "ahrb-fake-v1",
            "messages": [
                {"role":"system", "content":"fixed instructions"},
                {"role":"user", "content":format!("turn {turn}")}
            ],
            "tools": []
        });
        ModelRequestRecord {
            request: ModelRequest {
                dialect: "openai-chat-completions".to_owned(),
                endpoint: "/v1/chat/completions".to_owned(),
                model: "ahrb-fake-v1".to_owned(),
                scenario: "efficiency".to_owned(),
                actor: format!("r42t{turn}"),
                checkpoint: "start".to_owned(),
                canonical,
                credential_fingerprint: "redacted".to_owned(),
                stream: false,
            },
            canonical_hash: format!("hash-{turn}"),
            attempts: 1,
            accepted: true,
            semantic_ordinal: 1,
            attempt: 1,
            received_ns: turn as u64,
            body_bytes: 256,
            input_tokens: None,
            role: role.to_owned(),
            side_channel_kind: side_kind.map(str::to_owned),
            response_status: Some(200),
            response_headers_ns: Some(turn as u64),
            response_first_frame_yield_ns: None,
            response_last_frame_yield_ns: None,
            semantic_attempts_total: 1,
        }
    }

    #[test]
    fn model_request_efficiency_oracle_honors_side_channel_boundary_and_exact_keys() {
        let mut records = (1..=20)
            .map(|turn| efficiency_record(turn, "primary", None))
            .collect::<Vec<_>>();
        records.push(efficiency_record(21, "side-channel", Some("title")));
        let at_boundary = evaluate_model_request_efficiency(&records, 20, 20);
        assert!(at_boundary.measurement_complete);
        assert!(at_boundary.reference_envelope_pass);
        assert_eq!(at_boundary.metrics.len(), 11);
        assert_eq!(
            at_boundary.metrics["model_request_efficiency.side_channel_requests_per_turn"],
            0.05
        );
        assert_eq!(
            at_boundary.details["side_channel_requests_by_role"]["title"],
            1
        );

        records.push(efficiency_record(22, "side-channel", Some("summary")));
        let above_boundary = evaluate_model_request_efficiency(&records, 20, 20);
        assert!(!above_boundary.reference_envelope_pass);
        assert_eq!(
            above_boundary.metrics["model_request_efficiency.side_channel_requests_per_turn"],
            0.10
        );
    }

    #[test]
    fn model_request_efficiency_counts_retry_subset_once_and_rejects_unclassified() {
        let mut records = (1..=20)
            .map(|turn| efficiency_record(turn, "primary", None))
            .collect::<Vec<_>>();
        records[0].attempts = 2;
        records[0].semantic_attempts_total = 2;
        let mut retry = records[0].clone();
        retry.attempt = 2;
        records.push(retry);
        let retried = evaluate_model_request_efficiency(&records, 20, 20);
        assert_eq!(
            retried.metrics["model_request_efficiency.retry_attempts_per_turn"],
            0.05
        );
        assert!(!retried.reference_envelope_pass);

        let mut unclassified_records = (1..=20)
            .map(|turn| efficiency_record(turn, "primary", None))
            .collect::<Vec<_>>();
        unclassified_records.push(efficiency_record(21, "unclassified", None));
        let unclassified = evaluate_model_request_efficiency(&unclassified_records, 20, 20);
        assert!(unclassified.measurement_complete);
        assert!(!unclassified.reference_envelope_pass);
        assert_eq!(
            unclassified.metrics["model_request_efficiency.side_channel_requests_per_turn"],
            0.05
        );
        assert_eq!(
            unclassified.details["repetitions"][0]["side_channel_requests"],
            1
        );
        assert_eq!(
            unclassified.details["unclassified_requests"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn canonical_context_tax_includes_only_instructions_and_tools() {
        let base = json!({
            "messages": [
                {"role":"system", "content":"system"},
                {"role":"developer", "content":"developer"},
                {"role":"user", "content":"user-a"}
            ],
            "tools": [{"type":"function","function":{"name":"x"}}]
        });
        let mut changed_user = base.clone();
        changed_user["messages"][2]["content"] = json!("a much longer user message");
        assert_eq!(
            canonical_context_tax_bytes(&base),
            canonical_context_tax_bytes(&changed_user)
        );
        let mut changed_system = base.clone();
        changed_system["messages"][0]["content"] = json!("a much longer system instruction");
        assert!(canonical_context_tax_bytes(&changed_system) > canonical_context_tax_bytes(&base));
    }
}
