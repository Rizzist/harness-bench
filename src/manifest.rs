//! Data-only adapter manifest schema and validation.

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A complete harness adapter manifest.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Manifest {
    /// Schema and adapter identity.
    pub identity: Identity,
    /// Harness availability checks.
    pub availability: Availability,
    /// Fake-model routing configuration.
    pub fake_model: FakeModelBinding,
    /// Logical model roles.
    #[serde(default)]
    pub model_roles: BTreeMap<String, ModelRole>,
    /// Ordered predicates used to classify non-primary model requests.
    #[serde(default)]
    pub request_role_rules: Vec<RequestRoleRule>,
    /// Per-run profile isolation.
    pub isolation: Isolation,
    /// Daemon lifecycle operations.
    pub daemon: DaemonLifecycle,
    /// Automation transport.
    pub transport: TransportConfig,
    /// Session operations.
    pub sessions: SessionOps,
    /// Inputs injected into an active or queued turn.
    pub next_input: NextInputOps,
    /// Typed facts about the harness's prompt input surface.
    #[serde(default, skip_serializing_if = "InputConfig::is_absent")]
    pub input: InputConfig,
    /// Native agent operations.
    pub agents: AgentOps,
    /// Concurrency capabilities.
    pub concurrency: Concurrency,
    /// Tool schema and fixture commands.
    pub tools: ToolSemantics,
    /// Event stream mappings.
    pub events: EventMapping,
    /// Process exit contract.
    pub exit: ExitContract,
    /// Whole-tree ownership hints.
    pub process: ProcessOwnership,
    /// Resource control limits.
    pub resources: ResourceControls,
    /// Lifecycle hooks.
    #[serde(default)]
    pub hooks: Hooks,
    /// Cleanup operations.
    pub cleanup: Cleanup,
    /// Evidence capture policy.
    #[serde(default)]
    pub capture: CapturePolicy,
    /// Declared required and optional capabilities.
    pub capabilities: Capabilities,
}

/// Schema version and human-readable adapter identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Identity {
    /// Manifest schema version.
    pub schema: u32,
    /// Stable adapter identifier.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Optional adapter revision.
    #[serde(default)]
    pub revision: String,
}

/// Executable discovery and version probing.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Availability {
    /// Candidate executable paths.
    pub exec_paths: Vec<String>,
    /// Additional executables that the adapter requires at runtime.
    #[serde(default)]
    pub required_exec_paths: Vec<String>,
    /// Version probe argv.
    #[serde(default)]
    pub version_probe: Vec<String>,
    /// Required substring in the version output.
    #[serde(default)]
    pub version_pattern: String,
}

/// Supported fake-model HTTP dialect.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProtocolDialect {
    /// OpenAI `/v1/chat/completions`.
    OpenAiChatCompletions,
    /// OpenAI `/v1/responses`.
    OpenAiResponses,
    /// Anthropic `/v1/messages`.
    AnthropicMessages,
}

/// Fake-model endpoint, credential, and provider configuration binding.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FakeModelBinding {
    /// HTTP protocol dialect.
    pub dialect: ProtocolDialect,
    /// Environment variable receiving the base URL.
    pub base_url_env: String,
    /// Environment variable receiving the credential.
    pub credential_env: String,
    /// Whether row 1 requires an authentication header at the fake endpoint.
    #[serde(default = "default_true")]
    pub auth_required: bool,
    /// Model ID expected by the fake server.
    pub model: String,
    /// HTTP request paths the harness may use.
    #[serde(default)]
    pub allowed_paths: Vec<String>,
    /// Provider configuration templates.
    #[serde(default)]
    pub provider_templates: Vec<GeneratedFile>,
}

fn default_true() -> bool {
    true
}

/// A logical model role and its configured model ID.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ModelRole {
    /// Expected model ID.
    pub model: String,
    /// Whether the role must reach the fake server.
    #[serde(default)]
    pub required: bool,
}

/// Allowed auxiliary request classifications for model-efficiency evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SideChannelKind {
    /// Thread/title generation.
    Title,
    /// Conversation summary generation.
    Summary,
    /// Context compaction.
    Compaction,
    /// Reviewer or critic pass.
    Reviewer,
    /// Child-agent request.
    Child,
}

impl SideChannelKind {
    /// Stable report spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Title => "title",
            Self::Summary => "summary",
            Self::Compaction => "compaction",
            Self::Reviewer => "reviewer",
            Self::Child => "child",
        }
    }
}

/// One ordered request-role classifier. All specified predicates are ANDed.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequestRoleRule {
    /// Assigned auxiliary kind.
    pub kind: SideChannelKind,
    /// Unique ascending match priority.
    pub priority: u32,
    /// Optional exact provider model-ID allow-list.
    #[serde(default)]
    pub model_ids: Vec<String>,
    /// Optional JSON Pointer whose scalar/canonical value is matched by `regex`.
    #[serde(default)]
    pub json_pointer: String,
    /// Rust regular expression applied to the pointed value.
    #[serde(default)]
    pub regex: String,
}

/// Per-run environment roots, non-directory bindings, and generated files.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Isolation {
    /// Environment variables whose rendered values are directories AHRB creates.
    #[serde(default)]
    pub roots: BTreeMap<String, String>,
    /// Additional environment variables whose values AHRB must not create as
    /// directories (for example, a configuration-file path).
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Files generated inside isolated roots.
    #[serde(default)]
    pub generated_files: Vec<GeneratedFile>,
    /// Historical state locations that must remain untouched.
    #[serde(default)]
    pub forbidden_roots: Vec<String>,
    /// Relative Unix-socket suffixes that must fit beneath every rendered
    /// isolation root. Declaring these opts a socket-using adapter into the
    /// conservative cross-platform 100-byte path budget.
    #[serde(default)]
    pub socket_path_suffixes: Vec<String>,
}

/// A generated configuration file.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeneratedFile {
    /// Destination path template.
    pub path: String,
    /// File content template.
    pub content: String,
    /// Unix mode, conventionally written as an octal string.
    #[serde(default = "default_file_mode")]
    pub mode: String,
}

fn default_file_mode() -> String {
    "0600".to_owned()
}

/// Daemon topology and lifecycle commands.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DaemonLifecycle {
    /// Whether the controller persists between turns.
    pub persistent: bool,
    /// Start command argv.
    #[serde(default)]
    pub start: Vec<String>,
    /// Whether `start` is a finite launcher that leaves the actual daemon
    /// detached from AHRB's child process group.
    #[serde(default)]
    pub launcher_exits: bool,
    /// One-time initialization argv run after readiness. The driver records a
    /// profile-local marker only after this command succeeds.
    #[serde(default)]
    pub initialize: Vec<String>,
    /// Readiness probe.
    #[serde(default)]
    pub readiness: Probe,
    /// PID locator description or path.
    #[serde(default)]
    pub pid_locator: String,
    /// Graceful shutdown command argv.
    #[serde(default)]
    pub shutdown: Vec<String>,
    /// Typed interpretation of a JSON shutdown response.
    #[serde(default)]
    pub shutdown_result: ShutdownResult,
    /// Shutdown grace period.
    #[serde(default = "default_grace_ms")]
    pub grace_ms: u64,
}

/// Typed JSON contract returned by a graceful daemon shutdown operation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ShutdownResult {
    /// JSON pointer containing the typed outcome string.
    #[serde(default)]
    pub outcome_pointer: String,
    /// Outcomes proving graceful shutdown or an already-stopped daemon.
    #[serde(default)]
    pub clean_outcomes: Vec<String>,
    /// Outcomes requiring an owned-tree TERM/KILL escalation.
    #[serde(default)]
    pub escalate_outcomes: Vec<String>,
}

fn default_grace_ms() -> u64 {
    2_000
}

/// A readiness probe.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Probe {
    /// Probe kind: process, file, socket, HTTP, or command.
    #[serde(default)]
    pub kind: String,
    /// Probe target.
    #[serde(default)]
    pub target: String,
    /// Direct argv used by a command readiness probe.
    #[serde(default)]
    pub command: Vec<String>,
    /// For `command-json`, map required JSON pointers containing paths to the
    /// isolated environment root that must contain each returned path.
    #[serde(default)]
    pub json_pointer_roots: BTreeMap<String, String>,
    /// JSON pointer containing the detached daemon's root PID.
    #[serde(default)]
    pub pid_pointer: String,
    /// Optional JSON pointer containing the process-global readiness boolean.
    #[serde(default)]
    pub ready_pointer: String,
    /// Maximum wait.
    #[serde(default = "default_ready_ms")]
    pub timeout_ms: u64,
}

fn default_ready_ms() -> u64 {
    10_000
}

/// Transport protocol selected by the adapter.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    /// One process per operation.
    Exec,
    /// JSON records over child stdin/stdout.
    StdinRpc,
    /// JSON-RPC over a Unix-domain socket.
    SocketJsonrpc,
    /// HTTP request/response transport.
    Http,
}

/// Automation transport configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransportConfig {
    /// Transport kind.
    pub kind: TransportKind,
    /// Executable plus arguments for exec and stdin-RPC.
    #[serde(default)]
    pub command: Vec<String>,
    /// Socket path or HTTP URL template.
    #[serde(default)]
    pub endpoint: String,
    /// Request timeout.
    #[serde(default = "default_request_ms")]
    pub timeout_ms: u64,
}

fn default_request_ms() -> u64 {
    30_000
}

/// Commands and extractors for session lifecycle operations.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SessionOps {
    /// Create operation name or argv template.
    #[serde(default)]
    pub create: Vec<String>,
    /// Submit operation name or argv template.
    #[serde(default)]
    pub submit: Vec<String>,
    /// Attach operation name or argv template.
    #[serde(default)]
    pub attach: Vec<String>,
    /// Resume operation name or argv template.
    #[serde(default)]
    pub resume: Vec<String>,
    /// Subsequent-turn argv for EXEC adapters when it differs from resume.
    #[serde(default)]
    pub continue_turn: Vec<String>,
    /// Headless, idempotent resume/reconciliation command.
    #[serde(default)]
    pub resume_control: Vec<String>,
    /// Headless recovery probe run after daemon restart.
    #[serde(default)]
    pub recover_probe: Vec<String>,
    /// Close/delete operation name or argv template.
    #[serde(default)]
    pub close_delete: Vec<String>,
    /// List operation name or argv template.
    #[serde(default)]
    pub list: Vec<String>,
    /// Informational command/RPC used to observe a cohort settling. Resource
    /// certification never treats this process-global signal as its PASS fence.
    #[serde(default)]
    pub wait_ready: Vec<String>,
    /// JSON pointer used to extract a new session ID.
    #[serde(default)]
    pub id_pointer: String,
    /// JSON pointer used to learn a persistent run identifier.
    #[serde(default)]
    pub run_id_pointer: String,
}

/// Operations for steer, subturn, and queued input.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NextInputOps {
    /// Safe-boundary steer operation.
    #[serde(default)]
    pub steer: Vec<String>,
    /// Pre-tool intervention operation.
    #[serde(default)]
    pub subturn: Vec<String>,
    /// Queue-next-turn operation.
    #[serde(default)]
    pub queue: Vec<String>,
}

/// Typed facts about prompt input which cannot be inferred from transport alone.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct InputConfig {
    /// Whether prompts use the harness process's standard input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_uses_stdin: Option<bool>,
}

impl InputConfig {
    fn is_absent(&self) -> bool {
        self.prompt_uses_stdin.is_none()
    }
}

/// Native delegation operations.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AgentOps {
    /// Create operation.
    #[serde(default)]
    pub create: Vec<String>,
    /// Native child spawn operation.
    #[serde(default)]
    pub spawn: Vec<String>,
    /// Status operation.
    #[serde(default)]
    pub status: Vec<String>,
    /// Cancellation operation.
    #[serde(default)]
    pub cancel: Vec<String>,
    /// Result collection operation.
    #[serde(default)]
    pub collect: Vec<String>,
    /// Child ID JSON pointer.
    #[serde(default)]
    pub child_id_pointer: String,
    /// JSON pointer selecting the declared child-status result.
    #[serde(default)]
    pub status_result_pointer: String,
    /// JSON pointer selecting the normalized child-event array returned by collect.
    #[serde(default)]
    pub collect_events_pointer: String,
}

/// Harness concurrency topology and limits.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Concurrency {
    /// Reported topology label.
    pub topology: String,
    /// Maximum simultaneous agents.
    pub max_agents: usize,
    /// Fan-out mode.
    pub fanout_mode: String,
    /// Event evidence used to establish barrier presence.
    #[serde(default)]
    pub barrier_evidence: String,
    /// Operation used to release a durable state barrier token.
    #[serde(default)]
    pub release: Vec<String>,
}

/// Lifecycle family used by topology-relative certification policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyFamily {
    /// Every turn is owned by a fresh client or worker process.
    PerInvocation,
    /// Sessions share a persistent daemon or native sibling controller.
    SharedController,
}

/// Classify a normative topology label without erasing the original report label.
pub fn topology_family(topology: &str) -> Option<TopologyFamily> {
    match topology {
        "client-process-fanout" | "worker-processes" => Some(TopologyFamily::PerInvocation),
        "shared-daemon-sessions" | "native-sibling-fanout" => {
            Some(TopologyFamily::SharedController)
        }
        _ => None,
    }
}

/// Tool aliases, schema bindings, and safe fixture commands.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ToolSemantics {
    /// Semantic tool name to one or more harness-native aliases, in preference order.
    #[serde(default)]
    pub aliases: BTreeMap<String, ToolAlias>,
    /// Semantic field to harness schema binding.
    #[serde(default)]
    pub bindings: BTreeMap<String, String>,
    /// Safe argv fixture templates.
    #[serde(default)]
    pub fixtures: BTreeMap<String, Vec<String>>,
}

/// One preferred native tool name or an ordered set of version-compatible names.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ToolAlias {
    /// One exact native tool name.
    One(String),
    /// Ordered exact native tool names; the request declaration selects one.
    Candidates(Vec<String>),
}

impl ToolAlias {
    /// Return native tool names in adapter preference order.
    pub fn candidates(&self) -> &[String] {
        match self {
            Self::One(name) => std::slice::from_ref(name),
            Self::Candidates(names) => names,
        }
    }

    /// Return the preferred native tool name, when one was declared.
    pub fn primary(&self) -> Option<&str> {
        self.candidates().first().map(String::as_str)
    }
}

/// Event source/framing and table-driven extraction rules.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventMapping {
    /// stdout, stderr, journal, socket, or HTTP.
    pub source: String,
    /// Path template for file-backed event sources and durable-journal trials.
    #[serde(default)]
    pub path: String,
    /// jsonl, JSON sequence, or SSE.
    pub framing: String,
    /// Optional JSON pointer for an event discriminator nested in an envelope.
    /// Top-level `type`/`event` remain fallbacks for mixed streams.
    #[serde(default)]
    pub type_pointer: String,
    /// Stable event identity pointer.
    #[serde(default)]
    pub id_pointer: String,
    /// Resume cursor pointer.
    #[serde(default)]
    pub cursor_pointer: String,
    /// Envelope schema-version pointer. Records resolved through
    /// `type_pointer` must carry one of `schema_versions` when configured.
    #[serde(default)]
    pub schema_version_pointer: String,
    /// Envelope schema versions understood by this adapter revision.
    #[serde(default)]
    pub schema_versions: Vec<u64>,
    /// Warn and retain unique evidence for source payload kinds which have no
    /// normalization rule at an understood schema version.
    #[serde(default)]
    pub warn_unmapped_payload_kinds: bool,
    /// Optional argv that reopens the harness journal and emits normalized JSONL.
    #[serde(default)]
    pub replay_command: Vec<String>,
    /// Replay output shape: `lines` applies replay extraction to each record,
    /// while `document` extracts an array from one JSON document.
    #[serde(default = "default_replay_mode")]
    pub replay_mode: String,
    /// JSON pointer to the replay event array when `replay_mode = "document"`.
    #[serde(default)]
    pub replay_records_pointer: String,
    /// Scalar JSON-pointer assertions evaluated on a replay document before
    /// any contained events are normalized.
    #[serde(default)]
    pub replay_assertions: BTreeMap<String, String>,
    /// Optional argv that snapshots durable replay state before and after the
    /// replay command.
    #[serde(default)]
    pub replay_state_command: Vec<String>,
    /// Scalar assertions evaluated on both durable-state snapshots.
    #[serde(default)]
    pub replay_state_assertions: BTreeMap<String, String>,
    /// JSON pointers that must remain identical across the before/after
    /// durable-state snapshots.
    #[serde(default)]
    pub replay_state_pointers: Vec<String>,
    /// Require replay records to equal the raw, run-scoped live records before
    /// normalization. This retains source sequence gaps and nested call IDs.
    #[serde(default)]
    pub replay_compare_live_records: bool,
    /// Optional JSON pointer that unwraps each replay record before applying
    /// the live-stream normalization rules.
    #[serde(default)]
    pub replay_envelope_pointer: String,
    /// Optional first AHRB cursor assigned to replay when nondurable live
    /// announcements are omitted from the durable stream.
    #[serde(default)]
    pub replay_cursor_start: Option<u64>,
    /// Ordered normalization rules.
    #[serde(default)]
    pub rules: Vec<EventRule>,
}

fn default_replay_mode() -> String {
    "lines".to_owned()
}

/// One table-driven event normalization rule.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventRule {
    /// Source event type or predicate value.
    pub matches: String,
    /// Additional JSON-pointer predicates that must equal the declared strings.
    #[serde(default)]
    pub match_fields: BTreeMap<String, String>,
    /// Optional JSON pointer to an array whose elements are matched independently.
    #[serde(default)]
    pub expand_pointer: String,
    /// AHRB normalized event name.
    pub event: String,
    /// Optional JSON pointer for payload extraction.
    #[serde(default)]
    pub payload_pointer: String,
    /// Additional normalized payload fields sourced from the unmodified record.
    #[serde(default)]
    pub payload_bindings: BTreeMap<String, String>,
}

/// Stable exit-code mapping.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExitContract {
    /// Success exit code.
    pub success: i32,
    /// Optional stdout markers, one of which must be present for success.
    #[serde(default)]
    pub success_stdout: Vec<String>,
    /// Stdout markers that always classify the invocation as failure.
    #[serde(default)]
    pub failure_stdout: Vec<String>,
    /// Failure category to exit code.
    #[serde(default)]
    pub failures: BTreeMap<String, i32>,
}

/// Process roots and reparented-worker ownership hints.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProcessOwnership {
    /// Declared PID-file templates.
    #[serde(default)]
    pub pid_files: Vec<String>,
    /// Executable basenames that may be reparented.
    #[serde(default)]
    pub executable_names: Vec<String>,
}

/// Limits requested from the harness.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceControls {
    /// Maximum turn duration.
    pub turn_timeout_ms: u64,
    /// Client-side idle deadline.
    pub idle_timeout_ms: u64,
    /// Maximum captured output bytes.
    pub max_output_bytes: usize,
    /// Maximum physical provider requests, including the first request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_attempts: Option<u32>,
    /// Initial retry delay in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_base_delay_ms: Option<u64>,
    /// Capped retry delay in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_ms: Option<u64>,
    /// Harness log paths. `None` means omitted; `Some([])` is an explicit no-log claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_paths: Option<Vec<String>>,
    /// Additional journal paths; `events.path` is always implicit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_paths: Option<Vec<String>>,
}

/// Acceptance and completion hook argv templates.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Hooks {
    /// Acceptance hook.
    #[serde(default)]
    pub acceptance: Vec<String>,
    /// Completion hook.
    #[serde(default)]
    pub completion: Vec<String>,
}

/// Cleanup commands and paths.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Cleanup {
    /// Cleanup command argv.
    #[serde(default)]
    pub command: Vec<String>,
    /// Run-local paths eligible for deletion.
    #[serde(default)]
    pub paths: Vec<String>,
}

/// Redaction and evidence-size policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CapturePolicy {
    /// Environment names whose values are redacted.
    #[serde(default)]
    pub redact_env: Vec<String>,
    /// Explicitly permit `{{credential}}` in direct argv templates. This is
    /// disabled by default because process arguments can be externally visible.
    #[serde(default)]
    pub allow_credential_argv: bool,
    /// Maximum bytes captured per stream.
    #[serde(default)]
    pub max_bytes: usize,
    /// Structured marker used when model-visible tool output is truncated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation_marker: Option<TruncationMarker>,
}

/// Regex contract for a normalized, model-visible truncation marker.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TruncationMarker {
    /// Regex with the four named captures required by manifest schema 2.
    pub regex: String,
}

/// Required and optional capability declarations.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Capabilities {
    /// Required capability name to rationale.
    #[serde(default)]
    pub required: BTreeMap<String, String>,
    /// Optional capability name to rationale.
    #[serde(default)]
    pub optional: BTreeMap<String, String>,
}

/// Load and parse a manifest from disk.
pub fn load(path: &Path) -> Result<Manifest> {
    let text = std::fs::read_to_string(path)?;
    let manifest: Manifest = toml::from_str(&text)?;
    validate(&manifest)?;
    Ok(manifest)
}

/// Result of checking adapter availability on the current host.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DoctorReport {
    /// Stable adapter ID.
    pub adapter: String,
    /// Resolved executable, if one was found.
    pub executable: Option<PathBuf>,
    /// Captured version output, with surrounding whitespace removed.
    pub version: Option<String>,
    /// Canonical manifest SHA-256.
    pub manifest_sha256: String,
    /// Non-fatal diagnostics in deterministic order.
    pub diagnostics: Vec<String>,
    /// Whether all required checks passed.
    pub ready: bool,
}

/// Validate invariants that can be checked without starting a harness.
pub fn validate(manifest: &Manifest) -> Result<()> {
    if !matches!(manifest.identity.schema, 1 | 2) {
        return Err(AhrbError::Validation(format!(
            "unsupported manifest schema {} (expected 1 or 2)",
            manifest.identity.schema
        )));
    }
    validate_identifier("identity.id", &manifest.identity.id)?;
    if manifest.identity.name.trim().is_empty() {
        return Err(AhrbError::Validation(
            "identity.name must not be empty".to_owned(),
        ));
    }
    if manifest.availability.exec_paths.is_empty() {
        return Err(AhrbError::Validation(
            "availability.exec_paths must not be empty".to_owned(),
        ));
    }
    if manifest
        .availability
        .exec_paths
        .iter()
        .chain(manifest.availability.required_exec_paths.iter())
        .any(|candidate| candidate.trim().is_empty())
    {
        return Err(AhrbError::Validation(
            "availability executable paths must not be empty".to_owned(),
        ));
    }
    if manifest.fake_model.base_url_env.trim().is_empty()
        || manifest.fake_model.credential_env.trim().is_empty()
        || manifest.fake_model.model.trim().is_empty()
    {
        return Err(AhrbError::Validation(
            "fake-model URL env, credential env, and model are required".to_owned(),
        ));
    }
    if manifest.fake_model.allowed_paths.is_empty() {
        return Err(AhrbError::Validation(
            "fake_model.allowed_paths must not be empty".to_owned(),
        ));
    }
    let mut role_rule_priorities = std::collections::BTreeSet::new();
    for rule in &manifest.request_role_rules {
        if !role_rule_priorities.insert(rule.priority) {
            return Err(AhrbError::Validation(format!(
                "request_role_rules priority {} is duplicated",
                rule.priority
            )));
        }
        if rule.model_ids.iter().any(|model| model.trim().is_empty()) {
            return Err(AhrbError::Validation(format!(
                "request_role_rules priority {} contains an empty model ID",
                rule.priority
            )));
        }
        let has_pointer = !rule.json_pointer.is_empty();
        let has_regex = !rule.regex.is_empty();
        if has_pointer != has_regex {
            return Err(AhrbError::Validation(format!(
                "request_role_rules priority {} requires json_pointer and regex together",
                rule.priority
            )));
        }
        if has_pointer && (!rule.json_pointer.starts_with('/') || rule.json_pointer == "/") {
            return Err(AhrbError::Validation(format!(
                "request_role_rules priority {} has invalid JSON Pointer {:?}",
                rule.priority, rule.json_pointer
            )));
        }
        if has_regex {
            regex::Regex::new(&rule.regex).map_err(|error| {
                AhrbError::Validation(format!(
                    "request_role_rules priority {} has invalid regex: {error}",
                    rule.priority
                ))
            })?;
        }
        if rule.model_ids.is_empty() && !has_pointer {
            return Err(AhrbError::Validation(format!(
                "request_role_rules priority {} must declare at least one predicate",
                rule.priority
            )));
        }
    }
    if manifest.transport.command.is_empty()
        && matches!(
            manifest.transport.kind,
            TransportKind::Exec | TransportKind::StdinRpc
        )
    {
        return Err(AhrbError::Validation(
            "exec and stdin-rpc transports require an argv command".to_owned(),
        ));
    }
    if manifest.daemon.persistent
        && manifest.daemon.start.is_empty()
        && topology_family(&manifest.concurrency.topology) == Some(TopologyFamily::SharedController)
    {
        return Err(AhrbError::Validation(
            "persistent transports require daemon.start for a cold owned run".to_owned(),
        ));
    }
    if matches!(
        manifest.daemon.readiness.kind.as_str(),
        "command" | "command-json"
    ) && manifest.daemon.readiness.command.is_empty()
    {
        return Err(AhrbError::Validation(
            "command daemon readiness requires daemon.readiness.command".to_owned(),
        ));
    }
    if manifest.daemon.launcher_exits && manifest.daemon.readiness.pid_pointer.trim().is_empty() {
        return Err(AhrbError::Validation(
            "a detached daemon launcher requires daemon.readiness.pid_pointer".to_owned(),
        ));
    }
    if !manifest.daemon.readiness.json_pointer_roots.is_empty()
        && manifest.daemon.readiness.kind != "command-json"
    {
        return Err(AhrbError::Validation(
            "daemon.readiness.json_pointer_roots requires kind = command-json".to_owned(),
        ));
    }
    if (!manifest.daemon.readiness.pid_pointer.is_empty()
        || !manifest.daemon.readiness.ready_pointer.is_empty())
        && manifest.daemon.readiness.kind != "command-json"
    {
        return Err(AhrbError::Validation(
            "daemon readiness pid_pointer/ready_pointer require kind = command-json".to_owned(),
        ));
    }
    if !manifest.daemon.shutdown_result.outcome_pointer.is_empty()
        && manifest.daemon.shutdown.is_empty()
    {
        return Err(AhrbError::Validation(
            "daemon.shutdown_result requires daemon.shutdown".to_owned(),
        ));
    }
    if manifest.daemon.shutdown_result.outcome_pointer.is_empty()
        && (!manifest.daemon.shutdown_result.clean_outcomes.is_empty()
            || !manifest.daemon.shutdown_result.escalate_outcomes.is_empty())
    {
        return Err(AhrbError::Validation(
            "daemon.shutdown_result outcomes require outcome_pointer".to_owned(),
        ));
    }
    if manifest.transport.timeout_ms == 0
        || manifest.resources.turn_timeout_ms == 0
        || manifest.resources.idle_timeout_ms == 0
    {
        return Err(AhrbError::Validation(
            "transport, turn, and idle timeouts must be positive".to_owned(),
        ));
    }
    if manifest.resources.idle_timeout_ms >= manifest.resources.turn_timeout_ms {
        return Err(AhrbError::Validation(
            "idle timeout must be strictly less than turn timeout".to_owned(),
        ));
    }
    validate_wave_2_resources(manifest)?;
    if manifest.concurrency.max_agents == 0 {
        return Err(AhrbError::Validation(
            "concurrency.max_agents must be positive".to_owned(),
        ));
    }
    for (semantic, alias) in &manifest.tools.aliases {
        let candidates = alias.candidates();
        if candidates.is_empty() || candidates.iter().any(|name| name.trim().is_empty()) {
            return Err(AhrbError::Validation(format!(
                "tools.aliases.{semantic} must declare at least one non-empty native tool name"
            )));
        }
        let unique: std::collections::BTreeSet<_> = candidates.iter().collect();
        if unique.len() != candidates.len() {
            return Err(AhrbError::Validation(format!(
                "tools.aliases.{semantic} repeats a native tool name"
            )));
        }
    }
    if let Some(argv) = manifest.tools.fixtures.get("large_output") {
        let placeholder_count = argv
            .iter()
            .map(|argument| argument.match_indices("{{bytes}}").count())
            .sum::<usize>();
        if argv.is_empty() || placeholder_count == 0 {
            return Err(AhrbError::Validation(
                "tools.fixtures.large_output must be nonempty and contain {{bytes}}".to_owned(),
            ));
        }
        validate_wave_2_capture_limits(manifest)?;
    }
    let topology_family = topology_family(&manifest.concurrency.topology);
    let topology_matches_lifecycle = matches!(
        (manifest.daemon.persistent, topology_family),
        (false, Some(TopologyFamily::PerInvocation))
            | (true, Some(TopologyFamily::SharedController))
    );
    if !topology_matches_lifecycle {
        return Err(AhrbError::Validation(format!(
            "daemon.persistent={} conflicts with concurrency.topology {:?}",
            manifest.daemon.persistent, manifest.concurrency.topology
        )));
    }
    for required_root in ["HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"] {
        let Some(template) = manifest.isolation.roots.get(required_root) else {
            return Err(AhrbError::Validation(format!(
                "isolation.roots.{required_root} is required for a cold run"
            )));
        };
        if !profile_scoped_template(template) {
            return Err(AhrbError::Validation(format!(
                "isolation.roots.{required_root} must be lexically contained under {{{{profile}}}}"
            )));
        }
    }
    for (name, template) in &manifest.isolation.roots {
        if !profile_scoped_template(template) {
            return Err(AhrbError::Validation(format!(
                "isolation.roots.{name} must be lexically contained under {{{{profile}}}}"
            )));
        }
    }
    for (name, template) in &manifest.isolation.environment {
        if template.contains("{{profile}}") && !profile_scoped_template(template) {
            return Err(AhrbError::Validation(format!(
                "isolation.environment.{name} must be lexically contained under {{{{profile}}}}"
            )));
        }
        if manifest.isolation.roots.contains_key(name) {
            return Err(AhrbError::Validation(format!(
                "isolation environment variable {name:?} is declared as both a directory root and a non-directory binding"
            )));
        }
    }
    for (pointer, root_name) in &manifest.daemon.readiness.json_pointer_roots {
        if !pointer.starts_with('/') || pointer == "/" {
            return Err(AhrbError::Validation(format!(
                "daemon readiness JSON pointer {pointer:?} is invalid"
            )));
        }
        if !manifest.isolation.roots.contains_key(root_name) {
            return Err(AhrbError::Validation(format!(
                "daemon readiness JSON pointer {pointer:?} refers to undeclared isolation root {root_name:?}"
            )));
        }
    }
    for (label, pointer) in [
        (
            "pid_pointer",
            manifest.daemon.readiness.pid_pointer.as_str(),
        ),
        (
            "ready_pointer",
            manifest.daemon.readiness.ready_pointer.as_str(),
        ),
        (
            "shutdown outcome_pointer",
            manifest.daemon.shutdown_result.outcome_pointer.as_str(),
        ),
        (
            "sessions.run_id_pointer",
            manifest.sessions.run_id_pointer.as_str(),
        ),
        (
            "agents.child_id_pointer",
            manifest.agents.child_id_pointer.as_str(),
        ),
        (
            "agents.status_result_pointer",
            manifest.agents.status_result_pointer.as_str(),
        ),
        (
            "agents.collect_events_pointer",
            manifest.agents.collect_events_pointer.as_str(),
        ),
    ] {
        if !pointer.is_empty() && (!pointer.starts_with('/') || pointer == "/") {
            return Err(AhrbError::Validation(format!(
                "manifest {label} {pointer:?} is not a non-root JSON pointer"
            )));
        }
    }
    let clean: std::collections::BTreeSet<_> = manifest
        .daemon
        .shutdown_result
        .clean_outcomes
        .iter()
        .collect();
    if manifest
        .daemon
        .shutdown_result
        .escalate_outcomes
        .iter()
        .any(|outcome| clean.contains(outcome))
    {
        return Err(AhrbError::Validation(
            "daemon shutdown clean and escalation outcomes must be disjoint".to_owned(),
        ));
    }
    for suffix in &manifest.isolation.socket_path_suffixes {
        let path = Path::new(suffix);
        if suffix.trim().is_empty()
            || path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(AhrbError::Validation(format!(
                "isolation socket path suffix {suffix:?} must be a non-empty relative path"
            )));
        }
    }
    match manifest.events.source.as_str() {
        "stdout" | "journal" | "http" => {}
        "journal-file" if !manifest.events.path.trim().is_empty() => {}
        "journal-file" => {
            return Err(AhrbError::Validation(
                "events.source journal-file requires events.path".to_owned(),
            ));
        }
        other => {
            return Err(AhrbError::Validation(format!(
                "unsupported events.source {other:?}"
            )));
        }
    }
    if !manifest.events.path.is_empty() && !profile_scoped_template(&manifest.events.path) {
        return Err(AhrbError::Validation(
            "events.path must be lexically contained under {{profile}}".to_owned(),
        ));
    }
    for (label, paths) in [
        ("resources.log_paths", manifest.resources.log_paths.as_ref()),
        (
            "resources.journal_paths",
            manifest.resources.journal_paths.as_ref(),
        ),
    ] {
        if let Some(paths) = paths {
            for path in paths {
                if !profile_scoped_template(path) {
                    return Err(AhrbError::Validation(format!(
                        "{label} entry {path:?} must be lexically contained under {{{{profile}}}}"
                    )));
                }
            }
        }
    }
    if !manifest.events.replay_envelope_pointer.is_empty()
        && (!manifest.events.replay_envelope_pointer.starts_with('/')
            || manifest.events.replay_envelope_pointer == "/")
    {
        return Err(AhrbError::Validation(format!(
            "events.replay_envelope_pointer {:?} is not a non-root JSON pointer",
            manifest.events.replay_envelope_pointer
        )));
    }
    match manifest.events.replay_mode.as_str() {
        "lines" => {
            if !manifest.events.replay_records_pointer.is_empty() {
                return Err(AhrbError::Validation(
                    "events.replay_records_pointer requires replay_mode = \"document\"".to_owned(),
                ));
            }
        }
        "document" => {
            if manifest.events.replay_records_pointer.is_empty()
                || !manifest.events.replay_records_pointer.starts_with('/')
                || manifest.events.replay_records_pointer == "/"
            {
                return Err(AhrbError::Validation(format!(
                    "events.replay_records_pointer {:?} is not a non-root JSON pointer",
                    manifest.events.replay_records_pointer
                )));
            }
            if !manifest.events.replay_envelope_pointer.is_empty() {
                return Err(AhrbError::Validation(
                    "events.replay_envelope_pointer is only valid for replay_mode = \"lines\""
                        .to_owned(),
                ));
            }
        }
        other => {
            return Err(AhrbError::Validation(format!(
                "unsupported events.replay_mode {other:?}"
            )));
        }
    }
    for pointer in manifest.events.replay_assertions.keys() {
        if !pointer.starts_with('/') || pointer == "/" {
            return Err(AhrbError::Validation(format!(
                "events.replay_assertions pointer {pointer:?} is not a non-root JSON pointer"
            )));
        }
    }
    if (!manifest.events.replay_state_pointers.is_empty()
        || !manifest.events.replay_state_assertions.is_empty())
        && manifest.events.replay_state_command.is_empty()
    {
        return Err(AhrbError::Validation(
            "events replay state checks require replay_state_command".to_owned(),
        ));
    }
    for pointer in manifest
        .events
        .replay_state_pointers
        .iter()
        .chain(manifest.events.replay_state_assertions.keys())
    {
        if !pointer.starts_with('/') || pointer == "/" {
            return Err(AhrbError::Validation(format!(
                "events replay state pointer {pointer:?} is not a non-root JSON pointer"
            )));
        }
    }
    if manifest.events.replay_cursor_start == Some(0) {
        return Err(AhrbError::Validation(
            "events.replay_cursor_start must be greater than zero".to_owned(),
        ));
    }
    if manifest.events.schema_version_pointer.is_empty() {
        if !manifest.events.schema_versions.is_empty()
            || manifest.events.warn_unmapped_payload_kinds
        {
            return Err(AhrbError::Validation(
                "events schema versions/unmapped warnings require schema_version_pointer"
                    .to_owned(),
            ));
        }
    } else {
        if !manifest.events.schema_version_pointer.starts_with('/')
            || manifest.events.schema_version_pointer == "/"
        {
            return Err(AhrbError::Validation(format!(
                "events.schema_version_pointer {:?} is not a non-root JSON pointer",
                manifest.events.schema_version_pointer
            )));
        }
        if manifest.events.schema_versions.is_empty() {
            return Err(AhrbError::Validation(
                "events.schema_version_pointer requires at least one schema_versions entry"
                    .to_owned(),
            ));
        }
    }
    if manifest
        .exit
        .failures
        .values()
        .any(|code| *code == manifest.exit.success)
    {
        return Err(AhrbError::Validation(
            "failure exit codes must differ from success".to_owned(),
        ));
    }
    if let Some(marker) = &manifest.capture.truncation_marker {
        validate_wave_2_capture_limits(manifest)?;
        let regex = regex::Regex::new(&marker.regex).map_err(|error| {
            AhrbError::Validation(format!(
                "capture.truncation_marker.regex is invalid: {error}"
            ))
        })?;
        let names = regex
            .capture_names()
            .flatten()
            .collect::<std::collections::BTreeSet<_>>();
        for required in ["truncated", "original_bytes", "payload_bytes", "sha256"] {
            if !names.contains(required) {
                return Err(AhrbError::Validation(format!(
                    "capture.truncation_marker.regex requires named capture {required:?}"
                )));
            }
        }
        if marker.regex.contains("encoded_bytes") || names.contains("encoded_bytes") {
            return Err(AhrbError::Validation(
                "capture.truncation_marker.regex must not expose or render encoded_bytes"
                    .to_owned(),
            ));
        }
    }
    for (label, argv) in command_vectors(manifest) {
        if !manifest.capture.allow_credential_argv
            && argv.iter().any(|arg| arg.contains("{{credential}}"))
        {
            return Err(AhrbError::Validation(format!(
                "{label} embeds the credential value in argv"
            )));
        }
        if argv
            .first()
            .is_some_and(|arg| arg == "sh" || arg == "bash" || arg == "zsh")
        {
            return Err(AhrbError::Validation(format!(
                "{label} invokes a shell; commands must be direct argv arrays"
            )));
        }
    }
    for file in manifest
        .isolation
        .generated_files
        .iter()
        .chain(manifest.fake_model.provider_templates.iter())
    {
        if !profile_scoped_template(&file.path) {
            return Err(AhrbError::Validation(format!(
                "generated file {:?} must be lexically contained under {{{{profile}}}}",
                file.path
            )));
        }
        let mode = u32::from_str_radix(file.mode.trim_start_matches('0'), 8).map_err(|_| {
            AhrbError::Validation(format!("invalid generated-file mode {:?}", file.mode))
        })?;
        if mode & 0o077 != 0 {
            return Err(AhrbError::Validation(format!(
                "generated credential/config file {:?} must not be group/world accessible",
                file.path
            )));
        }
    }
    Ok(())
}

fn validate_wave_2_resources(manifest: &Manifest) -> Result<()> {
    match (
        manifest.resources.retry_max_attempts,
        manifest.resources.retry_base_delay_ms,
        manifest.resources.retry_max_delay_ms,
    ) {
        (None, None, None) => {}
        (Some(max_attempts), Some(base_delay_ms), Some(max_delay_ms)) => {
            if !(2..=6).contains(&max_attempts) {
                return Err(AhrbError::Validation(
                    "resources.retry_max_attempts must be in 2..=6".to_owned(),
                ));
            }
            if base_delay_ms < 50 {
                return Err(AhrbError::Validation(
                    "resources.retry_base_delay_ms must be at least 50".to_owned(),
                ));
            }
            if max_delay_ms == 0 {
                return Err(AhrbError::Validation(
                    "resources.retry_max_delay_ms must be positive".to_owned(),
                ));
            }
            if base_delay_ms > max_delay_ms {
                return Err(AhrbError::Validation(
                    "resources.retry_base_delay_ms must not exceed retry_max_delay_ms".to_owned(),
                ));
            }

            let delay_sum_ms = (0..max_attempts.saturating_sub(1))
                .map(|exponent| {
                    (u128::from(base_delay_ms) * (1_u128 << exponent)).min(u128::from(max_delay_ms))
                })
                .sum::<u128>();
            // Twice the specified `1.5 * sum(delays) + 1_000` formula avoids
            // losing a possible half millisecond to integer rounding.
            let worst_case_twice_ms = 3 * delay_sum_ms + 2_000;
            if worst_case_twice_ms > 20_000 {
                return Err(AhrbError::Validation(
                    "the declared retry worst case must not exceed 10,000 ms".to_owned(),
                ));
            }
            if worst_case_twice_ms > 2 * u128::from(manifest.resources.turn_timeout_ms) {
                return Err(AhrbError::Validation(
                    "the declared retry worst case must not exceed resources.turn_timeout_ms"
                        .to_owned(),
                ));
            }
        }
        _ => {
            return Err(AhrbError::Validation(
                "retry policy requires retry_max_attempts, retry_base_delay_ms, and retry_max_delay_ms together"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_wave_2_capture_limits(manifest: &Manifest) -> Result<()> {
    const MAX_CAPTURE_BYTES: usize = 1_048_576;
    if !(1..=MAX_CAPTURE_BYTES).contains(&manifest.resources.max_output_bytes) {
        return Err(AhrbError::Validation(
            "resources.max_output_bytes must be in 1..=1,048,576 for large output capture"
                .to_owned(),
        ));
    }
    if !(1..=MAX_CAPTURE_BYTES).contains(&manifest.capture.max_bytes) {
        return Err(AhrbError::Validation(
            "capture.max_bytes must be in 1..=1,048,576 for large output capture".to_owned(),
        ));
    }
    Ok(())
}

fn profile_scoped_template(template: &str) -> bool {
    let Some(suffix) = template.strip_prefix("{{profile}}") else {
        return false;
    };
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return false;
    }
    !Path::new(suffix).components().any(|component| {
        matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::Prefix(_)
        )
    })
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(AhrbError::Validation(format!(
            "{label} must contain only lowercase ASCII letters, digits, and hyphens"
        )));
    }
    Ok(())
}

fn command_vectors(manifest: &Manifest) -> Vec<(&'static str, &[String])> {
    vec![
        (
            "availability.version_probe",
            &manifest.availability.version_probe,
        ),
        ("daemon.start", &manifest.daemon.start),
        ("daemon.initialize", &manifest.daemon.initialize),
        (
            "daemon.readiness.command",
            &manifest.daemon.readiness.command,
        ),
        ("daemon.shutdown", &manifest.daemon.shutdown),
        ("transport.command", &manifest.transport.command),
        ("sessions.create", &manifest.sessions.create),
        ("sessions.submit", &manifest.sessions.submit),
        ("sessions.attach", &manifest.sessions.attach),
        ("sessions.resume", &manifest.sessions.resume),
        ("sessions.continue_turn", &manifest.sessions.continue_turn),
        ("sessions.resume_control", &manifest.sessions.resume_control),
        ("sessions.recover_probe", &manifest.sessions.recover_probe),
        ("sessions.close_delete", &manifest.sessions.close_delete),
        ("sessions.list", &manifest.sessions.list),
        ("sessions.wait_ready", &manifest.sessions.wait_ready),
        ("next_input.steer", &manifest.next_input.steer),
        ("next_input.subturn", &manifest.next_input.subturn),
        ("next_input.queue", &manifest.next_input.queue),
        ("agents.create", &manifest.agents.create),
        ("agents.spawn", &manifest.agents.spawn),
        ("agents.status", &manifest.agents.status),
        ("agents.cancel", &manifest.agents.cancel),
        ("agents.collect", &manifest.agents.collect),
        ("hooks.acceptance", &manifest.hooks.acceptance),
        ("hooks.completion", &manifest.hooks.completion),
        ("cleanup.command", &manifest.cleanup.command),
    ]
}

/// Canonical SHA-256 of a parsed manifest.
pub fn hash(manifest: &Manifest) -> Result<String> {
    let canonical = serde_json::to_vec(manifest)?;
    let digest = Sha256::digest(canonical);
    Ok(format!("{digest:x}"))
}

/// Render `{{name}}` tokens from a deterministic variable map.
pub fn render_template(template: &str, variables: &BTreeMap<String, String>) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find("{{") {
        output.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let close = after_open.find("}}").ok_or_else(|| {
            AhrbError::Validation(format!("unterminated template token in {template:?}"))
        })?;
        let name = after_open[..close].trim();
        let value = variables
            .get(name)
            .ok_or_else(|| AhrbError::Validation(format!("unknown template variable {name:?}")))?;
        output.push_str(value);
        rest = &after_open[close + 2..];
    }
    output.push_str(rest);
    Ok(output)
}

/// Run non-mutating manifest and executable availability checks.
pub fn doctor(path: &Path) -> Result<DoctorReport> {
    let manifest = load(path)?;
    let executable = manifest
        .availability
        .exec_paths
        .iter()
        .find_map(|candidate| resolve_executable(candidate));
    let mut diagnostics = Vec::new();
    if executable.is_none() {
        diagnostics.push("no candidate executable exists".to_owned());
    }
    for required in &manifest.availability.required_exec_paths {
        if resolve_executable(required).is_none() {
            diagnostics.push(format!("required executable {required:?} does not exist"));
        }
    }
    let version = match manifest.availability.version_probe.as_slice() {
        [first, rest @ ..] => {
            let program = resolve_executable(first).or_else(|| executable.clone());
            let Some(program) = program else {
                diagnostics.push("version probe executable does not exist".to_owned());
                diagnostics.sort();
                return Ok(DoctorReport {
                    adapter: manifest.identity.id.clone(),
                    executable,
                    version: None,
                    manifest_sha256: hash(&manifest)?,
                    ready: false,
                    diagnostics,
                });
            };
            let output = crate::process::owned_command_output(
                std::process::Command::new(program).args(rest),
            )?;
            let mut text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if text.is_empty() {
                text = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            }
            if !output.status.success() {
                diagnostics.push(format!("version probe exited with {}", output.status));
            } else if !manifest.availability.version_pattern.is_empty()
                && !text.contains(&manifest.availability.version_pattern)
            {
                diagnostics.push("version output does not match the required pattern".to_owned());
            }
            Some(concise_version(
                &text,
                &manifest.availability.version_pattern,
            ))
        }
        [] => None,
    };
    diagnostics.sort();
    Ok(DoctorReport {
        adapter: manifest.identity.id.clone(),
        executable,
        version,
        manifest_sha256: hash(&manifest)?,
        ready: diagnostics.is_empty(),
        diagnostics,
    })
}

fn concise_version(output: &str, version_pattern: &str) -> String {
    const MAX_CHARS: usize = 80;
    let mut nonempty = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let first = nonempty.next().unwrap_or_default();
    let selected = if version_pattern.is_empty() {
        first
    } else {
        output
            .lines()
            .map(str::trim)
            .find(|line| line.contains(version_pattern))
            .unwrap_or(first)
    };
    let mut characters = selected.chars();
    let prefix = characters.by_ref().take(MAX_CHARS).collect::<String>();
    if characters.next().is_some() {
        prefix.chars().take(MAX_CHARS - 1).collect::<String>() + "…"
    } else {
        prefix
    }
}

fn resolve_executable(candidate: &str) -> Option<PathBuf> {
    let path = PathBuf::from(candidate);
    if path.components().count() > 1 {
        return path.is_file().then_some(path);
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|directory| directory.join(candidate))
            .find(|possible| possible.is_file())
    })
}

#[cfg(test)]
mod version_tests {
    use super::{
        Manifest, RequestRoleRule, SideChannelKind, TruncationMarker, concise_version, validate,
    };

    fn wave_2_manifest() -> Manifest {
        let mut manifest = super::load(std::path::Path::new("adapters/aider/manifest.toml"))
            .expect("load schema-1 reference manifest");
        manifest.identity.schema = 2;
        manifest
    }

    #[test]
    fn version_probe_keeps_only_matching_line_or_first_line_and_caps_it() {
        let help = "usage: mock [OPTIONS]\nahrb-mock-harness 0.1.0\nmore help";
        assert_eq!(
            concise_version(help, "ahrb-mock-harness"),
            "ahrb-mock-harness 0.1.0"
        );
        assert_eq!(concise_version(help, ""), "usage: mock [OPTIONS]");
        let long = format!("version {}", "x".repeat(100));
        let value = concise_version(&long, "version");
        assert_eq!(value.chars().count(), 80);
        assert!(value.ends_with('…'));
    }

    #[test]
    fn request_role_rules_require_unique_priorities_and_complete_predicates() {
        let mut manifest = super::load(std::path::Path::new("adapters/mock/manifest.toml"))
            .expect("load mock manifest");
        manifest.request_role_rules = vec![
            RequestRoleRule {
                kind: SideChannelKind::Title,
                priority: 10,
                model_ids: vec!["title-model".to_owned()],
                json_pointer: String::new(),
                regex: String::new(),
            },
            RequestRoleRule {
                kind: SideChannelKind::Summary,
                priority: 20,
                model_ids: Vec::new(),
                json_pointer: "/messages/0/content".to_owned(),
                regex: "(?i)summary".to_owned(),
            },
        ];
        validate(&manifest).expect("valid ordered role rules");

        manifest.request_role_rules[1].priority = 10;
        assert!(validate(&manifest).is_err());
        manifest.request_role_rules[1].priority = 20;
        manifest.request_role_rules[1].regex.clear();
        assert!(validate(&manifest).is_err());
        manifest.request_role_rules[1].json_pointer.clear();
        assert!(validate(&manifest).is_err());
    }

    #[test]
    fn wave_2_optional_fields_preserve_absence_and_explicit_empty_paths() {
        let manifest = wave_2_manifest();
        assert!(manifest.resources.log_paths.is_none());
        assert!(manifest.resources.journal_paths.is_none());
        assert!(manifest.input.prompt_uses_stdin.is_none());
        assert!(manifest.capture.truncation_marker.is_none());

        let absent = serde_json::to_value(&manifest).expect("serialize absent fields");
        assert!(absent.get("input").is_none());
        assert!(absent["resources"].get("log_paths").is_none());
        assert!(absent["resources"].get("journal_paths").is_none());
        assert!(absent["capture"].get("truncation_marker").is_none());

        let mut explicit = manifest;
        explicit.resources.log_paths = Some(Vec::new());
        explicit.resources.journal_paths = Some(Vec::new());
        explicit.input.prompt_uses_stdin = Some(false);
        let encoded = toml::to_string(&explicit).expect("serialize explicit Wave-2 fields");
        let decoded: Manifest = toml::from_str(&encoded).expect("parse explicit Wave-2 fields");
        assert_eq!(decoded.resources.log_paths, Some(Vec::new()));
        assert_eq!(decoded.resources.journal_paths, Some(Vec::new()));
        assert_eq!(decoded.input.prompt_uses_stdin, Some(false));
    }

    #[test]
    fn wave_2_paths_must_be_profile_scoped() {
        let mut manifest = wave_2_manifest();
        manifest.resources.log_paths = Some(Vec::new());
        manifest.resources.journal_paths = Some(vec!["{{profile}}/state/extra.jsonl".to_owned()]);
        validate(&manifest).expect("explicit no-log and profile journal are valid");

        manifest.resources.log_paths = Some(vec!["state/harness.log".to_owned()]);
        let error = validate(&manifest).expect_err("relative log path must be rejected");
        assert!(error.to_string().contains("resources.log_paths"));

        manifest.resources.log_paths = Some(Vec::new());
        manifest.resources.journal_paths = Some(vec!["{{profile}}/../journal".to_owned()]);
        let error = validate(&manifest).expect_err("traversing journal path must be rejected");
        assert!(error.to_string().contains("resources.journal_paths"));
    }

    #[test]
    fn retry_policy_is_complete_bounded_and_has_a_certifiable_worst_case() {
        let mut manifest = wave_2_manifest();
        manifest.resources.retry_max_attempts = Some(3);
        let error = validate(&manifest).expect_err("partial retry policy must be rejected");
        assert!(error.to_string().contains("requires retry_max_attempts"));

        manifest.resources.retry_base_delay_ms = Some(50);
        manifest.resources.retry_max_delay_ms = Some(200);
        validate(&manifest).expect("minimum retry base is valid");

        manifest.resources.retry_max_attempts = Some(1);
        let error = validate(&manifest).expect_err("one attempt cannot prove bounded retry");
        assert!(error.to_string().contains("2..=6"));

        manifest.resources.retry_max_attempts = Some(3);
        manifest.resources.retry_base_delay_ms = Some(49);
        let error = validate(&manifest).expect_err("retry base below floor must be rejected");
        assert!(error.to_string().contains("at least 50"));

        manifest.resources.retry_base_delay_ms = Some(50);
        manifest.resources.retry_max_delay_ms = Some(0);
        let error = validate(&manifest).expect_err("zero retry cap must be rejected");
        assert!(error.to_string().contains("must be positive"));

        manifest.resources.retry_base_delay_ms = Some(200);
        manifest.resources.retry_max_delay_ms = Some(100);
        let error = validate(&manifest).expect_err("retry base above cap must be rejected");
        assert!(error.to_string().contains("must not exceed"));

        manifest.resources.retry_max_attempts = Some(6);
        manifest.resources.retry_base_delay_ms = Some(1_500);
        manifest.resources.retry_max_delay_ms = Some(1_500);
        let error = validate(&manifest).expect_err("retry envelope above 10 seconds");
        assert!(error.to_string().contains("10,000 ms"));

        manifest.resources.retry_max_attempts = Some(2);
        manifest.resources.retry_base_delay_ms = Some(100);
        manifest.resources.retry_max_delay_ms = Some(100);
        manifest.resources.turn_timeout_ms = 1_100;
        manifest.resources.idle_timeout_ms = 1_000;
        let error = validate(&manifest).expect_err("retry envelope above turn timeout");
        assert!(error.to_string().contains("turn_timeout_ms"));
    }

    #[test]
    fn large_output_fixture_and_truncation_marker_are_typed_and_bounded() {
        let mut manifest = wave_2_manifest();
        manifest.tools.fixtures.insert(
            "large_output".to_owned(),
            vec![
                "ahrb-fixture".to_owned(),
                "emit".to_owned(),
                "--bytes".to_owned(),
                "{{bytes}}".to_owned(),
            ],
        );
        manifest.capture.truncation_marker = Some(TruncationMarker {
            regex: r"TRUNCATED truncated=(?P<truncated>true) original=(?P<original_bytes>[0-9]+) payload=(?P<payload_bytes>[0-9]+) sha256=(?P<sha256>[0-9a-f]{64})"
                .to_owned(),
        });
        validate(&manifest).expect("valid large-output declarations");

        manifest.tools.fixtures.insert(
            "large_output".to_owned(),
            vec!["ahrb-fixture".to_owned(), "emit".to_owned()],
        );
        let error = validate(&manifest).expect_err("missing bytes placeholder");
        assert!(error.to_string().contains("contain {{bytes}}"));

        manifest.tools.fixtures.insert(
            "large_output".to_owned(),
            vec!["ahrb-fixture".to_owned(), "{{bytes}}{{bytes}}".to_owned()],
        );
        validate(&manifest).expect("the specification only requires the placeholder to occur");

        manifest.tools.fixtures.insert(
            "large_output".to_owned(),
            vec!["ahrb-fixture".to_owned(), "{{bytes}}".to_owned()],
        );
        manifest.capture.truncation_marker = Some(TruncationMarker {
            regex: r"(?P<truncated>true)-(?P<original_bytes>[0-9]+)-(?P<payload_bytes>[0-9]+)"
                .to_owned(),
        });
        let error = validate(&manifest).expect_err("missing sha256 capture");
        assert!(error.to_string().contains("sha256"));

        manifest.capture.truncation_marker = Some(TruncationMarker {
            regex: r"(?P<truncated>true)-(?P<original_bytes>[0-9]+)-(?P<payload_bytes>[0-9]+)-(?P<sha256>[0-9a-f]{64})-encoded_bytes"
                .to_owned(),
        });
        let error = validate(&manifest).expect_err("encoded byte field is self-referential");
        assert!(error.to_string().contains("encoded_bytes"));

        manifest.capture.truncation_marker = Some(TruncationMarker {
            regex: r"(?P<truncated>true)-(?P<original_bytes>[0-9]+)-(?P<payload_bytes>[0-9]+)-(?P<sha256>[0-9a-f]{64})"
                .to_owned(),
        });
        manifest.resources.max_output_bytes = 1_048_577;
        let error = validate(&manifest).expect_err("oversized harness output limit");
        assert!(error.to_string().contains("resources.max_output_bytes"));

        manifest.resources.max_output_bytes = 1_048_576;
        manifest.capture.max_bytes = 0;
        let error = validate(&manifest).expect_err("zero evidence capture limit");
        assert!(error.to_string().contains("capture.max_bytes"));
    }
}
