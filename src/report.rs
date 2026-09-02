//! Human-readable, machine-readable, and raw-evidence reports.

use crate::economy::EconomySummary;
use crate::evaluate::{Badge, TestOutcome, TestResult, badge_label};
use crate::manifest::Manifest;
use crate::process::{ProcIdentity, ProcOwnership, ProcessSample, Sample};
use crate::sampler::cadence_quality;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::ops::Deref;
use std::path::{Path, PathBuf};

/// Exhaustive schema-3 top-level numeric metric keys introduced by Wave 4.
///
/// Structured strings, lists, and observation records belong in the matching
/// `details.<row-id>` block and must not be synthesized into dynamic metrics.
pub const WAVE4_METRIC_KEYS: &[&str] = &[
    "injection_surface.provider_score",
    "injection_surface.base_url_score",
    "injection_surface.credential_score",
    "injection_surface.score",
    "injection_surface.verified_components",
    "budget_enforcement.token_limit",
    "budget_enforcement.token_observed",
    "budget_enforcement.cost_limit_microusd",
    "budget_enforcement.cost_observed_microusd",
    "budget_enforcement.time_limit_ms",
    "budget_enforcement.time_observed_ms",
    "budget_enforcement.overrun_count",
    "budget_enforcement.structured_failures",
    "budget_enforcement.score",
    "usage_reporting.input_tokens",
    "usage_reporting.output_tokens",
    "usage_reporting.total_tokens",
    "usage_reporting.cost_microusd",
    "usage_reporting.turns",
    "usage_reporting.crosscheck_errors",
    "usage_reporting.score",
    "session_ops_cli.create_ok",
    "session_ops_cli.list_ok",
    "session_ops_cli.resume_ok",
    "session_ops_cli.fork_ok",
    "session_ops_cli.delete_ok",
    "session_ops_cli.score",
    "event_stream_completeness.tool_call_id",
    "event_stream_completeness.correlated_result",
    "event_stream_completeness.timestamps",
    "event_stream_completeness.usage",
    "event_stream_completeness.terminal_typing",
    "event_stream_completeness.schema_version",
    "event_stream_completeness.score",
    "headless_permission_model.score",
    "headless_permission_model.tty_prompts",
    "headless_permission_model.allowed_effects",
    "headless_permission_model.denied_filesystem_effects",
    "headless_permission_model.denied_network_effects",
    "headless_permission_model.scope_violations",
    "secrets_hygiene_on_disk.files_scanned",
    "secrets_hygiene_on_disk.bytes_scanned",
    "secrets_hygiene_on_disk.stdout_matches",
    "secrets_hygiene_on_disk.stderr_matches",
    "secrets_hygiene_on_disk.journal_matches",
    "secrets_hygiene_on_disk.session_matches",
    "secrets_hygiene_on_disk.log_matches",
    "secrets_hygiene_on_disk.declared_carrier_files",
    "tool_result_role_fidelity.checks",
    "tool_result_role_fidelity.violations",
    "tool_result_role_fidelity.plain_user_text_violations",
    "tool_result_role_fidelity.missing_results",
    "tool_result_role_fidelity.duplicate_results",
];

/// One auditable recursive process-membership refresh boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipSample {
    /// Monotonic nanoseconds since resource collection began.
    pub elapsed_ns: u64,
    /// Resource phase active at this refresh.
    pub phase: String,
    /// Wall time consumed by recursive discovery.
    pub discovery_wall_ns: u64,
    /// Calling-thread CPU consumed by recursive discovery.
    pub discovery_cpu_ns: u64,
    /// Deterministic staggered sampler lane.
    pub lane: u32,
}

/// Reproducibility fingerprint fields.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Fingerprint {
    /// Harness artifact or executable digest.
    pub harness: String,
    /// Harness version.
    pub harness_version: String,
    /// Manifest digest.
    pub manifest: String,
    /// Workflow set digest.
    pub workflows: String,
    /// Fake-model engine version.
    pub fake_model: String,
    /// Event normalizer version.
    pub normalizer: String,
    /// AHRB source revision.
    pub ahrb_revision: String,
    /// OS/kernel/architecture summary.
    pub platform: String,
    /// Host physical memory bytes.
    pub host_memory_bytes: u64,
    /// Quick or certification profile.
    pub profile: String,
}

/// One numeric resource metric with its mandatory topology comparison scope.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TopologyMetric {
    /// Numeric observation in the unit encoded by the metric name.
    pub value: f64,
    /// Quick or certification profile under which the value was measured.
    #[serde(default)]
    pub profile: String,
    /// Architecture topology under which the observation was measured.
    pub topology: String,
    /// Normative comparison guard. Resource classes and marginal beta values
    /// may only be compared when this topology label is identical.
    pub comparison_scope: String,
}

/// Schema-validated named report detail blocks. Unknown future block names are
/// retained verbatim; every Wave-1 block is type-checked while deserializing.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(transparent)]
pub struct ReportDetails(BTreeMap<String, Value>);

impl ReportDetails {
    /// Insert one internally produced named block.
    pub fn insert(&mut self, name: String, value: Value) -> Option<Value> {
        self.0.insert(name, value)
    }
}

impl From<BTreeMap<String, Value>> for ReportDetails {
    fn from(values: BTreeMap<String, Value>) -> Self {
        Self(values)
    }
}

impl Deref for ReportDetails {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a ReportDetails {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::collections::btree_map::Iter<'a, String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for ReportDetails {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let values = BTreeMap::<String, Value>::deserialize(deserializer)?;
        for (name, value) in &values {
            let validation = match name.as_str() {
                "model-request-efficiency" => {
                    serde_json::from_value::<ModelRequestEfficiencyDetails>(value.clone())
                        .map(|_| ())
                }
                "process-hygiene" => {
                    serde_json::from_value::<ProcessHygieneDetails>(value.clone()).map(|_| ())
                }
                "time-to-first-model-request" => {
                    serde_json::from_value::<TimeToFirstModelRequestDetails>(value.clone())
                        .map(|_| ())
                }
                "latency-vs-turn-index" => {
                    serde_json::from_value::<LatencyVsTurnIndexDetails>(value.clone()).map(|_| ())
                }
                "session-residue-sweep" => {
                    serde_json::from_value::<SessionResidueSweepDetails>(value.clone()).map(|_| ())
                }
                "context-limit-recovery" => {
                    serde_json::from_value::<ContextLimitRecoveryDetails>(value.clone()).map(|_| ())
                }
                "resume-latency-vs-length" => {
                    serde_json::from_value::<ResumeLatencyVsLengthDetails>(value.clone())
                        .map(|_| ())
                }
                "journal-torn-tail-sweep" => {
                    serde_json::from_value::<JournalTornTailSweepDetails>(value.clone()).map(|_| ())
                }
                "memory-time-integral" => {
                    serde_json::from_value::<MemoryTimeIntegralDetails>(value.clone()).map(|_| ())
                }
                "retry-budget" => {
                    serde_json::from_value::<RetryBudgetDetails>(value.clone()).map(|_| ())
                }
                "nondeterministic-field-report" => {
                    serde_json::from_value::<NondeterministicFieldDetails>(value.clone())
                        .map(|_| ())
                }
                "cross-run-reproducibility" => {
                    serde_json::from_value::<CrossRunReproducibilityDetails>(value.clone())
                        .map(|_| ())
                }
                "fanout-cliff" => serde_json::from_value::<
                    crate::wave3_concurrency::FanoutCliffDetails,
                >(value.clone())
                .map(|_| ()),
                "fairness-under-fanout" => serde_json::from_value::<
                    crate::wave3_concurrency::FairnessDetails,
                >(value.clone())
                .map(|_| ()),
                "resource-summary" => {
                    serde_json::from_value::<ResourceSummaryDetails>(value.clone()).map(|_| ())
                }
                "automation-score" => {
                    serde_json::from_value::<AutomationScoreDetails>(value.clone()).map(|_| ())
                }
                "injection-surface" => {
                    serde_json::from_value::<InjectionSurfaceDetails>(value.clone()).map(|_| ())
                }
                "budget-enforcement" => {
                    serde_json::from_value::<BudgetEnforcementDetails>(value.clone()).map(|_| ())
                }
                "usage-reporting" => {
                    serde_json::from_value::<UsageReportingDetails>(value.clone()).map(|_| ())
                }
                "session-ops-cli" => {
                    serde_json::from_value::<SessionOpsCliDetails>(value.clone()).map(|_| ())
                }
                "event-stream-completeness" => {
                    serde_json::from_value::<EventStreamCompletenessDetails>(value.clone())
                        .map(|_| ())
                }
                "headless-permission-model" => {
                    serde_json::from_value::<HeadlessPermissionModelDetails>(value.clone())
                        .map(|_| ())
                }
                "secrets-hygiene-on-disk" => {
                    serde_json::from_value::<SecretsHygieneOnDiskDetails>(value.clone()).map(|_| ())
                }
                "tool-result-role-fidelity" => {
                    serde_json::from_value::<ToolResultRoleFidelityDetails>(value.clone())
                        .map(|_| ())
                }
                _ => Ok(()),
            };
            validation.map_err(|error| {
                D::Error::custom(format!("invalid details.{name} block: {error}"))
            })?;
        }
        Ok(Self(values))
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ModelRequestEfficiencyDetails {
    #[serde(default)]
    side_channel_requests_by_role: BTreeMap<String, u64>,
    #[serde(default)]
    unclassified_requests: Vec<UnclassifiedRequestDetail>,
    #[serde(default)]
    repetitions: Vec<ModelRequestEfficiencyRepetitionDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UnclassifiedRequestDetail {
    scenario: String,
    actor: String,
    checkpoint: String,
    semantic_ordinal: u64,
    attempt: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ModelRequestEfficiencyRepetitionDetail {
    repetition: u32,
    completed_turns: u64,
    primary_turns: u64,
    one_primary_per_turn: bool,
    side_channel_requests: u64,
    retry_attempts: u64,
    context_tax_slope_bytes_per_turn: f64,
    measurement_complete: bool,
    reference_envelope_pass: bool,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RetryBudgetDetails {
    attempts: Vec<RetryBudgetAttemptDetail>,
    timer_calibration: Vec<RetryTimerCalibrationDetail>,
    timer_tolerance_ms: f64,
    post_terminal_observation_ms: f64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RetryBudgetAttemptDetail {
    status: u16,
    repetition: u32,
    attempt: u64,
    received_ns: u64,
    previous_backoff_ms: Option<f64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RetryTimerCalibrationDetail {
    status: u16,
    index: u32,
    requested_ms: u64,
    actual_ms: f64,
    absolute_error_ms: f64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ProcessHygieneDetails {
    #[serde(default)]
    residue_identities: Vec<ResidueIdentityDetail>,
    #[serde(default)]
    audits: Vec<ProcessAuditDetail>,
    #[serde(default)]
    growth_diagnostics: Vec<GrowthDiagnosticDetail>,
    #[serde(default)]
    monotonic_growth_ok: Option<bool>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ResidueIdentityDetail {
    pid: u32,
    start_time: u64,
    command: String,
    ownership: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ProcessAuditDetail {
    repetition: u32,
    turn_index: Option<u32>,
    waited_ms: u64,
    processes: u64,
    threads: u64,
    fds: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct GrowthDiagnosticDetail {
    repetition: u32,
    checkpoints: u64,
    failure_threshold: u64,
    increases: BTreeMap<String, u64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct TimeToFirstModelRequestDetails {
    first_request_role: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct LatencyVsTurnIndexDetails {
    measurement_complete: bool,
    #[serde(default)]
    measurement_error: Option<String>,
    #[serde(default)]
    first_decile_p50_ms: Option<f64>,
    #[serde(default)]
    last_decile_p50_ms: Option<f64>,
    #[serde(default)]
    theil_sen_ms_per_turn: Option<f64>,
    #[serde(default)]
    latency_slope_ms_per_100_turns: Option<f64>,
    #[serde(default)]
    latency_last_first_decile_ratio: Option<f64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SessionResidueSweepDetails {
    measurement_complete: bool,
    #[serde(default)]
    measurement_error: Option<String>,
    #[serde(default)]
    store_checkpoints: Vec<crate::wave3_long_horizon::SessionResidueCheckpoint>,
    #[serde(default)]
    repetitions: Vec<crate::wave3_long_horizon::SessionResidueSweep>,
    #[serde(default)]
    final_memory_bound_mib: Option<f64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ContextLimitRecoveryDetails {
    measurement_complete: bool,
    #[serde(default)]
    measurement_error: Option<String>,
    #[serde(default)]
    trials: Vec<crate::wave3_long_horizon::ContextRecoveryTrial>,
    #[serde(default)]
    normalized_compacted_stream_sha256_by_repetition: Vec<(u32, String)>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ResumeLatencyVsLengthDetails {
    measurement_complete: bool,
    #[serde(default)]
    measurement_error: Option<String>,
    #[serde(default)]
    points: Vec<crate::wave3_long_horizon::ResumeLatencyPoint>,
    #[serde(default)]
    length_medians_ms: Vec<(u32, f64)>,
    #[serde(default)]
    long_short_ratio: Option<f64>,
    #[serde(default)]
    identity_cursor_preserved: Option<bool>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct JournalTornTailSweepDetails {
    measurement_complete: bool,
    #[serde(default)]
    measurement_error: Option<String>,
    #[serde(default)]
    cut_positions: Vec<crate::wave3_long_horizon::JournalTornTailTrial>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct MemoryTimeIntegralDetails {
    integration: String,
    #[serde(default)]
    collector: Option<String>,
    #[serde(default)]
    repetitions: Vec<MemoryTimeIntegralRepetitionDetail>,
    #[serde(default)]
    sampler_cadence_ns: Option<u64>,
    #[serde(default)]
    sampler_collection_cpu_ns: Option<u64>,
    #[serde(default)]
    sampler_observation_wall_ns: Option<u64>,
    #[serde(default)]
    sampler_overhead_pct: Option<f64>,
    #[serde(default)]
    sampler_cadence_overruns: Option<u64>,
    #[serde(default)]
    sampler_cadence_gaps: Option<u64>,
    #[serde(default)]
    sampler_warnings: Vec<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct MemoryTimeIntegralRepetitionDetail {
    repetition: u32,
    turns: u32,
    coverage_ratio: f64,
    max_sample_gap_ms: f64,
    memory_time_integral_mib_s_per_turn: f64,
    cpu_per_turn_p95_ms: f64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct NondeterministicFieldDetails {
    #[serde(default)]
    varying_fields: Vec<VaryingFieldDetail>,
    #[serde(default)]
    run_hashes: Vec<String>,
    #[serde(default)]
    collector_complete_by_run: BTreeMap<String, bool>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct VaryingFieldDetail {
    pointer: String,
    occurrences: u64,
    comparison_runs: Vec<u32>,
    before_types: Vec<String>,
    after_types: Vec<String>,
    #[serde(default)]
    dialects: Vec<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CrossRunReproducibilityDetails {
    #[serde(default)]
    stream_sha256_by_run: BTreeMap<String, String>,
    #[serde(default)]
    first_difference: Option<Value>,
    #[serde(default)]
    collector_complete_by_run: BTreeMap<String, bool>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ResourceSummaryDetails {
    measurement_complete: bool,
    #[serde(default)]
    latency_class: Option<String>,
    #[serde(default)]
    cpu_class: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct AutomationScoreDetails {
    profile: String,
    topology: String,
    comparison_scope: String,
    score: Option<u8>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct InjectionSurfaceDetails {
    #[serde(default)]
    verification_cases: Vec<InjectionVerificationCaseDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct InjectionVerificationCaseDetail {
    component: String,
    method: String,
    carrier: String,
    baseline_provider_requests: u64,
    perturbed_provider_requests: u64,
    expected_endpoint_reached: bool,
    unexpected_endpoint_requests: u64,
    credential_accepted: bool,
    baseline_credential_rejected: bool,
    secret_in_argv: bool,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct BudgetEnforcementDetails {
    #[serde(default)]
    cases: Vec<BudgetEnforcementCaseDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct BudgetEnforcementCaseDetail {
    repetition: u32,
    case: String,
    public_operation_start_ns: u64,
    #[serde(default)]
    usage_boundaries: Vec<BudgetUsageBoundaryDetail>,
    tariff_delivered: bool,
    terminal_receipt_ns: Option<u64>,
    #[serde(default)]
    effects: Vec<BudgetEffectDetail>,
    outer_kill: bool,
    #[serde(default)]
    overrun_observations: Vec<BudgetOverrunObservationDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct BudgetUsageBoundaryDetail {
    semantic_request_id: String,
    completed_ns: u64,
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cost_microusd: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct BudgetEffectDetail {
    effect_id: String,
    committed_ns: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct BudgetOverrunObservationDetail {
    kind: String,
    semantic_id: String,
    observed_ns: u64,
    stop_boundary_ns: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UsageReportingDetails {
    #[serde(default)]
    source_pointers: BTreeMap<String, String>,
    usage_event: String,
    usage_scope: String,
    #[serde(default)]
    repetitions: Vec<UsageRepetitionDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UsageRepetitionDetail {
    repetition: u32,
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cost_microusd: u64,
    turns: u64,
    #[serde(default)]
    per_turn: Vec<UsageReadingDetail>,
    #[serde(default)]
    per_response: Vec<UsageReadingDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UsageReadingDetail {
    turn: u32,
    response: Option<u32>,
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cost_microusd: u64,
    turns: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SessionOpsCliDetails {
    #[serde(default)]
    lifecycles: Vec<SessionOpsLifecycleDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SessionOpsLifecycleDetail {
    repetition: u32,
    original_id: String,
    fork_id: String,
    seed_call_id: String,
    seed_result_digest: String,
    committed_cursor: u64,
    #[serde(default)]
    original_history_hashes: Vec<String>,
    #[serde(default)]
    fork_history_hashes: Vec<String>,
    #[serde(default)]
    operation_results: Vec<SessionOperationResultDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SessionOperationResultDetail {
    operation: String,
    exit_code: Option<i32>,
    terminal_type: String,
    #[serde(default)]
    extracted_ids: Vec<String>,
    cursor: Option<u64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct EventStreamCompletenessDetails {
    #[serde(default)]
    missing_components: Vec<String>,
    #[serde(default)]
    component_failures: Vec<EventComponentFailureDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct EventComponentFailureDetail {
    repetition: u32,
    component: String,
    detail: String,
    receipt_start_ns: u64,
    receipt_end_ns: u64,
    receipt_wall_start_ns: u64,
    receipt_wall_end_ns: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct HeadlessPermissionModelDetails {
    mode: String,
    #[serde(default)]
    cases: Vec<PermissionCaseDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct PermissionCaseDetail {
    repetition: u32,
    case: String,
    #[serde(default)]
    argv: Vec<String>,
    exit_code: i32,
    terminal_type: String,
    effect: bool,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SecretsHygieneOnDiskDetails {
    #[serde(default)]
    matches: Vec<SecretMatchDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SecretMatchDetail {
    category: String,
    path: String,
    offset: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ToolResultRoleFidelityDetails {
    #[serde(default)]
    observations: Vec<ToolResultRoleObservationDetail>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct ToolResultRoleObservationDetail {
    repetition: u32,
    dialect: String,
    call_id: String,
    semantic_role: String,
    raw_pointer: String,
}

/// One externally observed semantic-turn interval on the shared monotonic clock.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnObservation {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// One-based turn number inside the repetition.
    pub turn_index: u32,
    /// Stable logical actor ID.
    pub actor: String,
    /// Non-secret stable digest of the harness session ID.
    pub session_id_hash: String,
    /// Stable fixture phase.
    pub phase: String,
    /// Child/controller launch boundary when applicable.
    pub launch_ns: Option<u64>,
    /// Turn submission boundary when applicable.
    pub submit_ns: Option<u64>,
    /// First completed provider request-body boundary when applicable.
    pub first_model_request_ns: Option<u64>,
    /// Structured terminal observation boundary when applicable.
    pub terminal_ns: Option<u64>,
    /// One-shot child exit boundary when applicable.
    pub exit_ns: Option<u64>,
    /// Exact topology-specific external turn interval.
    pub turn_wall_ns: Option<u64>,
}

/// One fake-provider response frame boundary used by streaming rows.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamChunkObservation {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// Stable logical actor ID.
    pub actor: String,
    /// Stable streaming case name.
    pub case: String,
    /// One-based frame ordinal.
    pub ordinal: u32,
    /// AHRB-owned scheduled monotonic boundary.
    pub scheduled_ns: u64,
    /// Fake-provider `Body::poll_frame` yield boundary.
    pub frame_yielded_ns: u64,
    /// Payload bytes in this frame.
    pub bytes: u64,
}

/// One profile-contained filesystem snapshot with stable file identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilesystemSnapshot {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// Stable before/after/checkpoint boundary.
    pub boundary: String,
    /// Journal, log, session, workspace, or other declared category.
    pub category: String,
    /// Lexically profile-relative path.
    pub path_under_profile: String,
    /// Unix device identifier.
    pub device_id: u64,
    /// Unix inode or platform-equivalent file identifier.
    pub inode_or_file_id: u64,
    /// File size at this boundary.
    pub size_bytes: u64,
    /// Lowercase SHA-256 of the complete captured file.
    pub sha256: String,
}

/// One same-confinement network egress observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EgressAttempt {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// External monotonic observation boundary.
    pub monotonic_ns: u64,
    /// Non-secret destination label.
    pub destination: String,
    /// Loopback, public, DNS, or other schema-defined category.
    pub category: String,
    /// Whether confinement allowed the attempt.
    pub allowed: bool,
    /// Stable blocked/connected/error classification.
    #[serde(default)]
    pub outcome: String,
    /// Concrete OS enforcement mechanism.
    pub enforcement: String,
    /// Reviewed identity binding the harness and independent probe to one guard.
    #[serde(default)]
    pub confinement_identity: String,
}

/// One externally sampled process identity and its hygiene counters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneProcess {
    /// Stable PID/start-time identity.
    pub identity: ProcIdentity,
    /// Executable basename captured by the platform sampler.
    pub command: String,
    /// Evidence by which the process belongs to the harness tree.
    pub ownership: ProcOwnership,
    /// Live threads at this checkpoint.
    pub thread_count: Option<u64>,
    /// Live file descriptors at this checkpoint.
    pub open_fds: Option<u64>,
}

/// One externally collected membership/counter sample inside a semantic turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneCadenceSample {
    /// Nanoseconds since the row-44 turn sampler started.
    pub elapsed_ns: u64,
    /// Calling-thread CPU used by discovery plus counter collection.
    pub collection_cpu_ns: u64,
    /// Wall time used by discovery plus counter collection.
    pub collection_wall_ns: u64,
    /// Complete identity/thread/FD observation at this cadence boundary.
    pub processes: Vec<ProcessHygieneProcess>,
}

/// One ordered active-turn process/thread/FD checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneCheckpoint {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// One-based turn number; zero identifies a daemon warm baseline.
    pub turn_index: u32,
    /// Complete externally owned membership at the checkpoint.
    pub processes: Vec<ProcessHygieneProcess>,
    /// Repeated out-of-band samples spanning the active turn. Warm baselines
    /// intentionally leave this empty and use `processes` directly.
    #[serde(default)]
    pub cadence_samples: Vec<ProcessHygieneCadenceSample>,
    /// Submit/release through terminal/exit observation window.
    #[serde(default)]
    pub sampled_wall_ns: u64,
    /// Mandatory process-membership cadence for this platform/profile.
    #[serde(default)]
    pub required_cadence_ns: u64,
}

/// One delayed residue audit after a child exit, daemon close, or shutdown.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneAudit {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// Per-invocation turn index, absent for daemon close/shutdown audits.
    pub turn_index: Option<u32>,
    /// Actual elapsed audit window.
    pub waited_ms: u64,
    /// Complete externally owned membership after the audit window.
    pub processes: Vec<ProcessHygieneProcess>,
}

/// Raw row-44 evidence collected outside the harness turn path.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneEvidence {
    /// Active-turn checkpoints in repetition/turn order.
    pub checkpoints: Vec<ProcessHygieneCheckpoint>,
    /// Exact K=T+1 post-audit growth checkpoints per repetition, including
    /// checkpoint zero before the first measured turn.
    #[serde(default)]
    pub growth_checkpoints: Vec<ProcessHygieneCheckpoint>,
    /// One warm controller baseline per daemon repetition.
    pub warm_baselines: Vec<ProcessHygieneCheckpoint>,
    /// Per-invocation audits after every child exit.
    pub per_turn_audits: Vec<ProcessHygieneAudit>,
    /// Daemon audits after official session close.
    pub post_close_audits: Vec<ProcessHygieneAudit>,
    /// Daemon audits after official controller shutdown.
    pub shutdown_audits: Vec<ProcessHygieneAudit>,
    /// Calling-thread CPU consumed by row-44 discovery and sampling.
    pub sampler_collection_cpu_ns: u64,
    /// Wall time consumed by row-44 discovery and sampling.
    pub sampler_collection_wall_ns: u64,
    /// Row-44 sampler CPU spent while turns were active.
    pub active_sampler_collection_cpu_ns: u64,
    /// Total active-turn wall time covered by row-44 cadence sampling.
    pub sampled_turn_wall_ns: u64,
    /// Process-accounting warnings retained from platform samples.
    pub sampler_warnings: Vec<String>,
}

/// Topology-agnostic resource and performance headline values.
///
/// Every value here is derived after workload execution from AHRB's external
/// whole-tree samples, membership-discovery accounting, and the external turn
/// wall clocks that already enforce turn deadlines. Summary construction never
/// executes synchronously in the harness turn path.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum NullableSummaryValue<T> {
    /// The source row was not selected or did not complete, so omit the field.
    #[default]
    Omitted,
    /// The source row completed and the meaningful result is JSON null.
    Null,
    /// The source row completed and produced a numeric value.
    Value(T),
}

impl<T> NullableSummaryValue<T> {
    fn is_omitted(&self) -> bool {
        matches!(self, Self::Omitted)
    }

    fn as_option(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Omitted | Self::Null => None,
        }
    }
}

impl<T> From<Option<T>> for NullableSummaryValue<T> {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Null, Self::Value)
    }
}

impl<T: PartialEq> PartialEq<Option<T>> for NullableSummaryValue<T> {
    fn eq(&self, other: &Option<T>) -> bool {
        match self {
            Self::Value(value) => other.as_ref() == Some(value),
            Self::Omitted | Self::Null => other.is_none(),
        }
    }
}

impl<T: Serialize> Serialize for NullableSummaryValue<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Value(value) => serializer.serialize_some(value),
            Self::Omitted | Self::Null => serializer.serialize_none(),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for NullableSummaryValue<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Option::<T>::deserialize(deserializer)?.into())
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ResourceSummary {
    /// Architecture topology under which resource values were measured.
    #[serde(default)]
    pub topology: String,
    /// Quick or certification profile under which the values were measured.
    #[serde(default)]
    pub profile: String,
    /// Normative comparison guard for every resource value.
    #[serde(default)]
    pub comparison_scope: String,
    /// Maximum effective owned-tree memory over the sampled run.
    pub peak_rss_mib: f64,
    /// Arithmetic mean effective owned-tree memory over all samples.
    pub mean_rss_mib: f64,
    /// Median effective owned-tree memory over all samples.
    pub median_rss_mib: f64,
    /// Cumulative owned-tree CPU delta over the sampled run.
    pub cpu_total_s: f64,
    /// Cumulative owned-tree CPU divided by executed workflow turns.
    pub cpu_per_turn_ms: f64,
    /// Mean external AHRB turn wall clock.
    pub wall_per_turn_ms: f64,
    /// Nearest-rank median external turn wall clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_per_turn_p50_ms: Option<f64>,
    /// Nearest-rank p95 external turn wall clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_per_turn_p95_ms: Option<f64>,
    /// Maximum external turn wall clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_per_turn_max_ms: Option<f64>,
    /// Median absolute deviation from the nearest-rank median.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_per_turn_mad_ms: Option<f64>,
    /// MAD divided by p50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_per_turn_jitter_ratio: Option<f64>,
    /// Latency class derived from p95: L100 through L1000+.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_class: Option<String>,
    /// Cold launch to first completed model-request body, nearest-rank p50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_model_request_p50_ms: Option<f64>,
    /// Cold launch to first completed model-request body, nearest-rank p95.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_model_request_p95_ms: Option<f64>,
    /// Cold launch to first completed model-request body, maximum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_first_model_request_max_ms: Option<f64>,
    /// Median trapezoidal effective-memory integral per semantic turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_time_integral_mib_s_per_turn: Option<f64>,
    /// Fraction of measured turn wall covered by bracketing sample intervals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_time_integral_coverage_ratio: Option<f64>,
    /// Largest sample-to-sample interval overlapping a measured turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_time_integral_max_sample_gap_ms: Option<f64>,
    /// N=1 whole-tree CPU per turn, nearest-rank p50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_per_turn_p50_ms: Option<f64>,
    /// N=1 whole-tree CPU per turn, nearest-rank p95.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_per_turn_p95_ms: Option<f64>,
    /// CPU class derived from N=1 p95: C10 through C250+.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_class: Option<String>,
    /// Whole-tree disk-write distribution and growth fields (row 47).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_p50: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_p95: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_max: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_journal_growth_bytes_per_turn: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_growth_bytes_per_turn: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_growth_slope_bytes_per_turn2: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_io_counter_complete: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unbounded_disk_growth: Option<bool>,
    /// Model-wait CPU fields (row 48).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_cpu_p50_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_wall_p50_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_cpu_one_core_max_ratio: Option<f64>,
    /// Peak effective-memory increase while streaming the row-60 tool result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_tool_output_peak_rss_delta_mib: Option<f64>,
    /// Long-session latency fields (row 49).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_slope_ms_per_100_turns: Option<f64>,
    #[serde(default, skip_serializing_if = "NullableSummaryValue::is_omitted")]
    pub latency_last_first_decile_ratio: NullableSummaryValue<f64>,
    /// Session residue/store fields (row 50).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_residue_slope_mib_per_session: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_residue_final_mib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_store_byte_slope_per_session: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_store_file_count_slope_per_session: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_store_final_residue_bytes: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_store_final_residue_files: Option<u64>,
    /// Resume latency fields (row 52).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_latency_p50_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_latency_p95_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_latency_slope_ms_per_turn: Option<f64>,
    /// Fanout cliff/scaling fields (row 54).
    #[serde(default, skip_serializing_if = "NullableSummaryValue::is_omitted")]
    pub fanout_cliff_n_rss: NullableSummaryValue<u32>,
    #[serde(default, skip_serializing_if = "NullableSummaryValue::is_omitted")]
    pub fanout_cliff_n_wall: NullableSummaryValue<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fanout_max_local_rss_alpha: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fanout_max_local_wall_alpha: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fanout_global_rss_alpha: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fanout_max_measured_n: Option<u32>,
    /// Fairness fields (row 55).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fairness_latency_cv: Option<f64>,
    #[serde(default, skip_serializing_if = "NullableSummaryValue::is_omitted")]
    pub fairness_latency_max_min_ratio: NullableSummaryValue<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fairness_latency_spread_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fairness_starved_agents: Option<u32>,
    /// Resident daemon baseline; absent for zero-process-between-turns topologies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_rss_mib: Option<f64>,
    /// Parallel marginal memory where a complete sweep supports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_beta_mib_per_agent: Option<f64>,
    /// Parallel scaling exponent where a complete sweep supports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_alpha: Option<f64>,
    /// Membership sampler CPU as a percentage of one core.
    pub sampler_overhead_pct: f64,
}

/// Complete benchmark report and embedded evidence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Report {
    /// Report schema version.
    pub schema: u32,
    /// Authoritative benchmark specification version.
    #[serde(default = "default_spec_version")]
    pub spec_version: u32,
    /// Deterministic run identifier.
    pub run_id: String,
    /// Canonical short root holding isolated harness state for this run.
    #[serde(default)]
    pub profile_path: String,
    /// Reproducibility fingerprint.
    pub fingerprint: Fingerprint,
    /// Matrix results sorted by row.
    pub results: Vec<TestResult>,
    /// Badge when every topology-relative CORE gate passes.
    pub badge: Option<Badge>,
    /// Named non-resource automation diagnostics. Resource observations live
    /// exclusively in `resource_metrics` so topology labels cannot be dropped.
    pub metrics: BTreeMap<String, f64>,
    /// Row-keyed structured details that cannot live in the numeric metric map.
    #[serde(default)]
    pub details: ReportDetails,
    /// Typed daemon-shutdown outcomes and any owned-tree escalation performed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle_notes: Vec<String>,
    /// Raw JSON returned by headless resume/recovery controls, annotated with
    /// the local session and action that produced it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_evidence: Vec<Value>,
    /// Resource metrics with topology labels and within-topology comparison scope.
    #[serde(default)]
    pub resource_metrics: BTreeMap<String, TopologyMetric>,
    /// Cross-topology headline summary derived from external observations.
    #[serde(default)]
    pub resource_summary: ResourceSummary,
    /// Third-pillar harness-economy summary. Absent from ordinary v1/v2 runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub economy_summary: Option<EconomySummary>,
    /// Raw resource samples.
    pub samples: Vec<Sample>,
    /// Raw row-46 continuous whole-tree counter samples.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memory_time_samples: Vec<MemoryTimeIntegralSample>,
    /// Raw process observations.
    pub processes: Vec<ProcessSample>,
    /// Raw recursive process-membership refresh timestamps.
    #[serde(default)]
    pub membership: Vec<MembershipSample>,
    /// Normalized event JSON records.
    pub events: Vec<Value>,
    /// Redacted fake-model request JSON records.
    pub model_requests: Vec<Value>,
    /// External per-turn boundary evidence.
    #[serde(default)]
    pub turns: Vec<TurnObservation>,
    /// Raw streaming-frame evidence.
    #[serde(default)]
    pub stream_chunks: Vec<StreamChunkObservation>,
    /// Raw filesystem identity/size/digest evidence.
    #[serde(default)]
    pub filesystem_snapshots: Vec<FilesystemSnapshot>,
    /// Raw same-confinement network enforcement evidence.
    #[serde(default)]
    pub egress_attempts: Vec<EgressAttempt>,
}

/// Persist the manifest declaration state needed to interpret optional-row
/// transitions without parsing human-readable evidence.
pub fn record_capability_declarations(results: &mut [TestResult], manifest: &Manifest) {
    for result in results {
        result.metadata.capability_declared =
            result.metadata.capability.as_ref().map(|capability| {
                manifest.capabilities.required.contains_key(capability)
                    || manifest.capabilities.optional.contains_key(capability)
            });
    }
}

fn default_spec_version() -> u32 {
    1
}

/// Persist the full report bundle using stable names and ordering.
pub fn write_bundle(report: &Report, directory: &Path, junit: bool) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    write_atomic(
        &directory.join("report.json"),
        &serde_json::to_vec_pretty(report)?,
    )?;
    write_atomic(
        &directory.join("report.md"),
        render_markdown(report).as_bytes(),
    )?;
    write_jsonl(&directory.join("samples.jsonl"), &report.samples)?;
    if !report.memory_time_samples.is_empty() {
        write_jsonl(
            &directory.join("memory-time-samples.jsonl"),
            &report.memory_time_samples,
        )?;
    }
    write_jsonl(&directory.join("processes.jsonl"), &report.processes)?;
    write_jsonl(&directory.join("membership.jsonl"), &report.membership)?;
    write_jsonl(&directory.join("events.jsonl"), &report.events)?;
    write_jsonl(
        &directory.join("model-requests.jsonl"),
        &report.model_requests,
    )?;
    write_jsonl(&directory.join("turns.jsonl"), &report.turns)?;
    write_jsonl(
        &directory.join("stream-chunks.jsonl"),
        &report.stream_chunks,
    )?;
    write_jsonl(
        &directory.join("filesystem-snapshots.jsonl"),
        &report.filesystem_snapshots,
    )?;
    write_jsonl(
        &directory.join("egress-attempts.jsonl"),
        &report.egress_attempts,
    )?;
    if junit {
        write_atomic(
            &directory.join("junit.xml"),
            render_junit(report).as_bytes(),
        )?;
    }
    Ok(())
}

/// Persist a best-effort diagnostic when a run aborts before its bundle is complete.
pub fn write_failure_diagnostic(
    directory: &Path,
    manifest: &Path,
    error: &AhrbError,
) -> Result<PathBuf> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join("run-error.txt");
    let content = format!(
        "AHRB run aborted\nmanifest={}\nreport=report.json\nerror={error}\n",
        manifest.display()
    );
    write_atomic(&path, content.as_bytes())?;
    Ok(path)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = std::path::PathBuf::from(temporary);
    std::fs::write(&temporary, bytes)?;
    let file = std::fs::OpenOptions::new().write(true).open(&temporary)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn write_jsonl<T: Serialize>(path: &Path, records: &[T]) -> Result<()> {
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record)?;
        bytes.push(b'\n');
    }
    write_atomic(path, &bytes)
}

/// Render the human-readable Markdown report.
pub fn render_markdown(report: &Report) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AHRB report `{}`\n", report.run_id);
    if !report.profile_path.is_empty() {
        let _ = writeln!(output, "Profile: `{}`\n", report.profile_path);
    }
    if let Some(badge) = &report.badge {
        let _ = writeln!(output, "**{}**\n", badge_label(badge));
    } else {
        let _ = writeln!(output, "**No badge certified.**\n");
    }
    if let Some(summary) = &report.economy_summary {
        let _ = writeln!(output, "## Harness economy\n");
        let _ = writeln!(
            output,
            "Reference tokenizer: `{}` (`{}`, vocabulary SHA-256 `{}`).\n",
            summary.reference_tokenizer.encoding,
            summary.reference_tokenizer.version,
            summary.reference_tokenizer.vocabulary_sha256,
        );
        let _ = writeln!(
            output,
            "Reference tariff: `${:.2}` per 1M reference request tokens. Completion means **{}**; it is not real-task success.\n",
            summary.reference_tariff_usd_per_million_tokens, summary.completion_label,
        );
        let _ = writeln!(
            output,
            "| Harness | Model turns | Total {} | Tool calls / batching factor | Last context {} | Completion | Reference cost | Tokens / completed task |",
            summary.reference_token_label, summary.reference_token_label,
        );
        let _ = writeln!(output, "|---|---:|---:|---:|---:|---|---:|---:|");
        let tokens_per_task = summary
            .tokens_per_completed_task
            .map_or_else(|| "unavailable".to_owned(), |value| value.to_string());
        let _ = writeln!(
            output,
            "| `{}` | {} | {} | {} / {:.6} | {} | {} | ${:.8} | {} |\n",
            report.fingerprint.harness,
            summary.model_turns,
            summary.total_reference_tokens,
            summary.tool_calls,
            summary.tool_batching_factor,
            summary.last_context_size_tokens,
            crate::economy::completion_name(&summary.completion),
            summary.reference_cost_usd,
            tokens_per_task,
        );
    }
    if report.economy_summary.is_none() {
        let _ = writeln!(output, "| Row | Pillar | Test | Outcome |");
        let _ = writeln!(output, "|---:|---|---|---|");
        let mut results: Vec<&TestResult> = report.results.iter().collect();
        results.sort_by_key(|result| result.row);
        for result in results {
            let _ = writeln!(
                output,
                "| {} | {:?} | `{}` | {} |",
                result.row,
                result.pillar,
                result.id,
                outcome_label(&result.outcome)
            );
        }
        let _ = writeln!(output, "\n## Resource summary\n");
        let _ = writeln!(
            output,
            "`{}`",
            render_resource_summary(&report.resource_summary)
        );
        if !report.resource_metrics.is_empty() {
            let topology = report
                .resource_metrics
                .values()
                .next()
                .map(|metric| metric.topology.as_str())
                .unwrap_or("unknown");
            let _ = writeln!(output, "\n## Resource metrics — `{topology}`\n");
            let _ = writeln!(
                output,
                "> R-class and marginal β are comparable only within the same topology.\n"
            );
            for (name, metric) in &report.resource_metrics {
                let _ = writeln!(
                    output,
                    "- `{name}`: {:.3} (topology: `{}`; profile: `{}`; scope: `{}`)",
                    metric.value, metric.topology, metric.profile, metric.comparison_scope
                );
            }
        }
        let diagnostic_metrics: BTreeMap<_, _> = report
            .metrics
            .iter()
            .filter(|(name, _)| !report.resource_metrics.contains_key(*name))
            .collect();
        if !diagnostic_metrics.is_empty() {
            let _ = writeln!(output, "\n## Automation diagnostics\n");
            for (name, value) in diagnostic_metrics {
                let _ = writeln!(output, "- `{name}`: {value:.3}");
            }
        }
        if !report.details.is_empty() {
            let _ = writeln!(output, "\n## Details\n");
            for (name, value) in &report.details {
                let rendered = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
                let _ = writeln!(output, "- `{name}`: `{rendered}`");
            }
        }
    }
    let _ = writeln!(output, "\n## Fingerprint\n");
    let _ = writeln!(output, "- Harness: `{}`", report.fingerprint.harness);
    let _ = writeln!(output, "- Manifest: `{}`", report.fingerprint.manifest);
    let _ = writeln!(output, "- Workflows: `{}`", report.fingerprint.workflows);
    let _ = writeln!(output, "- Platform: `{}`", report.fingerprint.platform);
    let _ = writeln!(output, "- Profile: `{}`", report.fingerprint.profile);
    output
}

/// Build the concise resource-summary line shared by `ahrb run` and `hbench`.
pub fn render_resource_summary(summary: &ResourceSummary) -> String {
    let mut output = format!(
        "resource_summary peak_rss_mib={:.3} mean_rss_mib={:.3} median_rss_mib={:.3} cpu_total_s={:.3} cpu_per_turn_ms={:.3} wall_per_turn_ms={:.3} wall_per_turn_p50_ms={} wall_per_turn_p95_ms={} wall_per_turn_max_ms={} wall_per_turn_mad_ms={} wall_per_turn_jitter_ratio={} latency_class={} time_to_first_model_request_p50_ms={} time_to_first_model_request_p95_ms={} time_to_first_model_request_max_ms={} memory_time_integral_mib_s_per_turn={} memory_time_integral_coverage_ratio={} memory_time_integral_max_sample_gap_ms={} cpu_per_turn_p50_ms={} cpu_per_turn_p95_ms={} cpu_class={}",
        summary.peak_rss_mib,
        summary.mean_rss_mib,
        summary.median_rss_mib,
        summary.cpu_total_s,
        summary.cpu_per_turn_ms,
        summary.wall_per_turn_ms,
        optional_milliseconds(summary.wall_per_turn_p50_ms),
        optional_milliseconds(summary.wall_per_turn_p95_ms),
        optional_milliseconds(summary.wall_per_turn_max_ms),
        optional_milliseconds(summary.wall_per_turn_mad_ms),
        optional_decimal(summary.wall_per_turn_jitter_ratio),
        summary.latency_class.as_deref().unwrap_or("unavailable"),
        optional_milliseconds(summary.time_to_first_model_request_p50_ms),
        optional_milliseconds(summary.time_to_first_model_request_p95_ms),
        optional_milliseconds(summary.time_to_first_model_request_max_ms),
        optional_decimal(summary.memory_time_integral_mib_s_per_turn),
        optional_decimal(summary.memory_time_integral_coverage_ratio),
        optional_milliseconds(summary.memory_time_integral_max_sample_gap_ms),
        optional_milliseconds(summary.cpu_per_turn_p50_ms),
        optional_milliseconds(summary.cpu_per_turn_p95_ms),
        summary.cpu_class.as_deref().unwrap_or("unavailable"),
    );
    if let Some(value) = summary.idle_rss_mib {
        let _ = write!(output, " idle_rss_mib={value:.3}");
    }
    if let Some(value) = summary.parallel_beta_mib_per_agent {
        let _ = write!(output, " parallel_beta_mib_per_agent={value:.3}");
    }
    if let Some(value) = summary.scaling_alpha {
        let _ = write!(output, " scaling_alpha={value:.3}");
    }
    let _ = write!(
        output,
        " disk_write_bytes_per_turn_p50={} disk_write_bytes_per_turn_p95={} disk_write_bytes_per_turn_max={} session_journal_growth_bytes_per_turn={} log_growth_bytes_per_turn={} disk_write_growth_slope_bytes_per_turn2={} disk_io_counter_complete={} unbounded_disk_growth={}",
        optional_decimal(summary.disk_write_bytes_per_turn_p50),
        optional_decimal(summary.disk_write_bytes_per_turn_p95),
        optional_decimal(summary.disk_write_bytes_per_turn_max),
        optional_decimal(summary.session_journal_growth_bytes_per_turn),
        optional_decimal(summary.log_growth_bytes_per_turn),
        optional_decimal(summary.disk_write_growth_slope_bytes_per_turn2),
        optional_bool(summary.disk_io_counter_complete),
        optional_bool(summary.unbounded_disk_growth),
    );
    let _ = write!(
        output,
        " model_wait_cpu_p50_ms={} model_wait_wall_p50_ms={} model_wait_cpu_one_core_max_ratio={}",
        optional_milliseconds(summary.model_wait_cpu_p50_ms),
        optional_milliseconds(summary.model_wait_wall_p50_ms),
        optional_decimal(summary.model_wait_cpu_one_core_max_ratio),
    );
    let _ = write!(
        output,
        " latency_slope_ms_per_100_turns={} latency_last_first_decile_ratio={}",
        optional_milliseconds(summary.latency_slope_ms_per_100_turns),
        optional_decimal(summary.latency_last_first_decile_ratio.as_option().copied(),),
    );
    let _ = write!(
        output,
        " sampler_overhead_pct={:.3}",
        summary.sampler_overhead_pct
    );
    output
}

fn optional_milliseconds(value: Option<f64>) -> String {
    value.map_or_else(|| "unavailable".to_owned(), |value| format!("{value:.3}"))
}

fn optional_decimal(value: Option<f64>) -> String {
    value.map_or_else(|| "unavailable".to_owned(), |value| format!("{value:.6}"))
}

fn optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "unavailable",
    }
}

/// Derive summary values exclusively from already-collected external evidence.
///
/// `turn_wall_ns` contains durations from AHRB's pre-existing external deadline
/// clocks; this function performs no sampling and does not interact with a
/// harness. On macOS effective memory is physical footprint, while on Linux it
/// is PSS when available and RSS otherwise.
#[allow(clippy::too_many_arguments)]
pub fn summarize_resources(
    samples: &[Sample],
    membership: &[MembershipSample],
    workflow_turns: u64,
    turn_wall_ns: &[u64],
    idle_rss_mib: Option<f64>,
    parallel_beta_mib_per_agent: Option<f64>,
    scaling_alpha: Option<f64>,
) -> ResourceSummary {
    const MIB: f64 = 1_048_576.0;
    let mut memory = samples
        .iter()
        .map(effective_memory_bytes)
        .collect::<Vec<_>>();
    let peak_bytes = memory.iter().copied().max().unwrap_or(0);
    let mean_bytes = if memory.is_empty() {
        0.0
    } else {
        memory.iter().map(|value| *value as f64).sum::<f64>() / memory.len() as f64
    };
    memory.sort_unstable();
    let median_bytes = match memory.len() {
        0 => 0.0,
        length if length % 2 == 1 => memory[length / 2] as f64,
        length => {
            let upper = memory[length / 2] as f64;
            let lower = memory[length / 2 - 1] as f64;
            (lower + upper) / 2.0
        }
    };
    let cpu_ns = samples
        .first()
        .zip(samples.last())
        .map_or(0, |(first, last)| last.cpu_ns.saturating_sub(first.cpu_ns));
    let cpu_per_turn_ms = if workflow_turns == 0 {
        0.0
    } else {
        cpu_ns as f64 / workflow_turns as f64 / 1_000_000.0
    };
    let wall_per_turn_ms = if turn_wall_ns.is_empty() {
        0.0
    } else {
        turn_wall_ns.iter().map(|value| *value as f64).sum::<f64>()
            / turn_wall_ns.len() as f64
            / 1_000_000.0
    };
    let sampling_wall_span = membership
        .iter()
        .map(|sample| sample.elapsed_ns)
        .min()
        .zip(membership.iter().map(|sample| sample.elapsed_ns).max())
        .map_or(0, |(first, last)| last.saturating_sub(first));
    let discovery_cpu_ns = membership.iter().fold(0_u64, |total, sample| {
        total.saturating_add(sample.discovery_cpu_ns)
    });
    let sampler_overhead_pct = if sampling_wall_span == 0 {
        0.0
    } else {
        100.0 * discovery_cpu_ns as f64 / sampling_wall_span as f64
    };
    ResourceSummary {
        topology: String::new(),
        profile: String::new(),
        comparison_scope: String::new(),
        peak_rss_mib: peak_bytes as f64 / MIB,
        mean_rss_mib: mean_bytes / MIB,
        median_rss_mib: median_bytes / MIB,
        cpu_total_s: cpu_ns as f64 / 1_000_000_000.0,
        cpu_per_turn_ms,
        wall_per_turn_ms,
        wall_per_turn_p50_ms: None,
        wall_per_turn_p95_ms: None,
        wall_per_turn_max_ms: None,
        wall_per_turn_mad_ms: None,
        wall_per_turn_jitter_ratio: None,
        latency_class: None,
        time_to_first_model_request_p50_ms: None,
        time_to_first_model_request_p95_ms: None,
        time_to_first_model_request_max_ms: None,
        memory_time_integral_mib_s_per_turn: None,
        memory_time_integral_coverage_ratio: None,
        memory_time_integral_max_sample_gap_ms: None,
        cpu_per_turn_p50_ms: None,
        cpu_per_turn_p95_ms: None,
        cpu_class: None,
        idle_rss_mib,
        parallel_beta_mib_per_agent,
        scaling_alpha,
        sampler_overhead_pct,
        ..ResourceSummary::default()
    }
}

/// Deterministic row-43 distribution and evidence-completeness decision.
#[derive(Clone, Debug, PartialEq)]
pub struct TurnLatencyEvaluation {
    /// Nearest-rank p50 in milliseconds.
    pub wall_per_turn_p50_ms: f64,
    /// Nearest-rank p95 in milliseconds.
    pub wall_per_turn_p95_ms: f64,
    /// Maximum in milliseconds.
    pub wall_per_turn_max_ms: f64,
    /// Median absolute deviation in milliseconds.
    pub wall_per_turn_mad_ms: f64,
    /// MAD divided by p50.
    pub wall_per_turn_jitter_ratio: f64,
    /// Normative latency class.
    pub latency_class: String,
    /// Whether every required external boundary and interval was present.
    pub measurement_complete: bool,
    /// Informational reference-envelope decision.
    pub reference_envelope_pass: bool,
    /// Deterministic measurement diagnostic when incomplete.
    pub measurement_error: Option<String>,
}

/// Deterministic row-49 long-session latency-growth decision.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencyVsTurnIndexEvaluation {
    /// Exact top-level `metrics.latency_vs_turn_index.*` values.
    pub metrics: BTreeMap<String, f64>,
    /// Median of the per-repetition first-decile p50 values.
    pub first_decile_p50_ms: f64,
    /// Median of the per-repetition last-decile p50 values.
    pub last_decile_p50_ms: f64,
    /// Median per-repetition Theil-Sen slope.
    pub theil_sen_ms_per_turn: f64,
    /// Median per-repetition slope scaled to 100 turns.
    pub latency_slope_ms_per_100_turns: f64,
    /// Exact headline last/first decile ratio, absent when the first median is zero.
    pub latency_last_first_decile_ratio: Option<f64>,
    /// Exact structured `details.latency-vs-turn-index` value.
    pub details: Value,
    /// Whether every required external boundary and turn interval was present.
    pub measurement_complete: bool,
    /// CORE oracle decision over the published median-of-repetitions fields.
    pub passed: bool,
    /// Deterministic measurement diagnostic when incomplete.
    pub measurement_error: Option<String>,
    /// Deterministic behavioral diagnostic when complete but outside the oracle.
    pub failure_detail: Option<String>,
}

/// Deterministic row-45 cold-launch distribution and evidence decision.
#[derive(Clone, Debug, PartialEq)]
pub struct TimeToFirstModelRequestEvaluation {
    /// Nearest-rank p50 in milliseconds.
    pub p50_ms: f64,
    /// Nearest-rank p95 in milliseconds.
    pub p95_ms: f64,
    /// Maximum in milliseconds.
    pub max_ms: f64,
    /// Exact structured `details.time-to-first-model-request` value.
    pub details: Value,
    /// Whether every required cold boundary and role was present.
    pub measurement_complete: bool,
    /// Informational reference-envelope decision.
    pub reference_envelope_pass: bool,
    /// Deterministic measurement diagnostic when incomplete.
    pub measurement_error: Option<String>,
}

/// Evaluate cold launch/request observations without interacting with the harness.
pub fn evaluate_time_to_first_model_request(
    observations: &[TurnObservation],
    first_request_roles: &[String],
    expected_repetitions: u32,
    turn_timeout_ms: u64,
) -> TimeToFirstModelRequestEvaluation {
    let incomplete = |detail: String| TimeToFirstModelRequestEvaluation {
        p50_ms: 0.0,
        p95_ms: 0.0,
        max_ms: 0.0,
        details: serde_json::json!({"first_request_role": null}),
        measurement_complete: false,
        reference_envelope_pass: false,
        measurement_error: Some(detail),
    };
    if observations.len() != expected_repetitions as usize
        || first_request_roles.len() != expected_repetitions as usize
    {
        return incomplete(format!(
            "expected {expected_repetitions} cold launch/request pairs and roles, observed {} pairs and {} roles",
            observations.len(),
            first_request_roles.len()
        ));
    }
    let mut latencies = Vec::with_capacity(observations.len());
    for (index, (observation, role)) in observations.iter().zip(first_request_roles).enumerate() {
        let expected_repetition = index as u32 + 1;
        if observation.repetition != expected_repetition || observation.turn_index != 1 {
            return incomplete(format!(
                "cold observation sequence expected repetition {expected_repetition} turn 1, observed repetition {} turn {}",
                observation.repetition, observation.turn_index
            ));
        }
        if role.is_empty() {
            return incomplete(format!(
                "repetition {expected_repetition} lacks a classified first-request role"
            ));
        }
        let Some((launch_ns, request_ns)) = observation
            .launch_ns
            .zip(observation.first_model_request_ns)
        else {
            return incomplete(format!(
                "repetition {expected_repetition} lacks launch/first-request boundaries"
            ));
        };
        if launch_ns == 0 || request_ns == 0 {
            return incomplete(format!(
                "repetition {expected_repetition} has an invalid zero monotonic boundary"
            ));
        }
        let Some(latency_ns) = request_ns.checked_sub(launch_ns) else {
            return incomplete(format!(
                "repetition {expected_repetition} first request precedes launch"
            ));
        };
        latencies.push(latency_ns);
    }
    latencies.sort_unstable();
    let p50_ms = nearest_rank_u64(&latencies, 50) as f64 / 1_000_000.0;
    let p95_ms = nearest_rank_u64(&latencies, 95) as f64 / 1_000_000.0;
    let max_ms = latencies.last().copied().unwrap_or(0) as f64 / 1_000_000.0;
    let first_request_role = first_request_roles
        .first()
        .filter(|first| first_request_roles.iter().all(|role| role == *first))
        .cloned()
        .unwrap_or_else(|| "mixed".to_owned());
    TimeToFirstModelRequestEvaluation {
        p50_ms,
        p95_ms,
        max_ms,
        details: serde_json::json!({"first_request_role": first_request_role}),
        measurement_complete: true,
        reference_envelope_pass: p95_ms <= 2_000.0
            && max_ms <= 10_000.0
            && max_ms < turn_timeout_ms as f64,
        measurement_error: None,
    }
}

/// One dense, out-of-band whole-tree counter sample used by row 46.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MemoryTimeIntegralSample {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// System-wide monotonic timestamp after counter collection.
    pub monotonic_ns: u64,
    /// Effective whole-tree memory: footprint on macOS, PSS/RSS on Linux.
    pub effective_memory_bytes: u64,
    /// Cumulative retired-aware whole-tree CPU.
    pub cpu_ns: u64,
    /// Number of owned process identities observed at this sample.
    pub owned_processes: u64,
    /// Sampler-thread CPU used by this collection.
    pub collection_cpu_ns: u64,
    /// Wall time used by this collection.
    pub collection_wall_ns: u64,
    /// Recoverable retired-process CPU-accounting warnings for this sample.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cpu_accounting_warnings: Vec<String>,
}

/// Complete continuously sampled row-46 evidence.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MemoryTimeIntegralEvidence {
    /// Samples spanning every measured turn, ordered by repetition/time.
    pub samples: Vec<MemoryTimeIntegralSample>,
    /// Exact launch/submit and terminal/exit windows for the measured turns.
    pub turns: Vec<TurnObservation>,
    /// Fresh warm-idle median B by repetition; zero for per-invocation.
    pub warm_idle_baseline_bytes: BTreeMap<u32, u64>,
    /// Platform counter cadence used for the oracle.
    pub sampler_cadence_ns: u64,
    /// Sum of sampler-thread CPU over every sample.
    pub sampler_collection_cpu_ns: u64,
    /// Sum of sampled monotonic spans within repetitions.
    pub sampler_observation_wall_ns: u64,
}

/// Row-46 integral, coverage, CPU distribution, and class decision.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryTimeIntegralEvaluation {
    /// Median per-turn trapezoidal MiB*s.
    pub memory_time_integral_mib_s_per_turn: f64,
    /// Union of covered sample intervals divided by measured turn wall.
    pub memory_time_integral_coverage_ratio: f64,
    /// Largest sample-to-sample interval overlapping a turn.
    pub memory_time_integral_max_sample_gap_ms: f64,
    /// Nearest-rank p50 whole-tree CPU per turn.
    pub cpu_per_turn_p50_ms: f64,
    /// Nearest-rank p95 whole-tree CPU per turn.
    pub cpu_per_turn_p95_ms: f64,
    /// Normative CPU class.
    pub cpu_class: String,
    /// Row-local sampler CPU as a percentage of one core.
    pub sampler_overhead_pct: f64,
    /// Collections whose wall duration exceeded the platform cadence.
    pub sampler_cadence_overruns: usize,
    /// Sample intervals larger than the platform cadence.
    pub sampler_cadence_gaps: usize,
    /// Exact structured `details.memory-time-integral` value.
    pub details: Value,
    /// Whether coverage, bracketing, counters, and sampler health were trustworthy.
    pub measurement_complete: bool,
    /// Informational reference-envelope result.
    pub reference_envelope_pass: bool,
    /// Deterministic diagnostic when evidence is incomplete.
    pub measurement_error: Option<String>,
}

fn interpolate_counter(
    samples: &[&MemoryTimeIntegralSample],
    timestamp_ns: u64,
    memory: bool,
) -> Option<f64> {
    if let Some(sample) = samples
        .iter()
        .find(|sample| sample.monotonic_ns == timestamp_ns)
    {
        return Some(if memory {
            sample.effective_memory_bytes as f64
        } else {
            sample.cpu_ns as f64
        });
    }
    samples.windows(2).find_map(|pair| {
        let left = pair[0];
        let right = pair[1];
        if left.monotonic_ns < timestamp_ns && timestamp_ns < right.monotonic_ns {
            let left_value = if memory {
                left.effective_memory_bytes
            } else {
                left.cpu_ns
            } as f64;
            let right_value = if memory {
                right.effective_memory_bytes
            } else {
                right.cpu_ns
            } as f64;
            let fraction = timestamp_ns.saturating_sub(left.monotonic_ns) as f64
                / right.monotonic_ns.saturating_sub(left.monotonic_ns) as f64;
            Some(left_value + (right_value - left_value) * fraction)
        } else {
            None
        }
    })
}

fn median_sorted_f64(values: &[f64]) -> f64 {
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

fn nearest_rank_f64(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

/// Evaluate row 46 from a continuous out-of-band counter series.
pub fn evaluate_memory_time_integral(
    evidence: &MemoryTimeIntegralEvidence,
    expected_repetitions: u32,
    turns_per_repetition: u32,
    per_invocation: bool,
) -> MemoryTimeIntegralEvaluation {
    let incomplete = |detail: String| MemoryTimeIntegralEvaluation {
        memory_time_integral_mib_s_per_turn: 0.0,
        memory_time_integral_coverage_ratio: 0.0,
        memory_time_integral_max_sample_gap_ms: 0.0,
        cpu_per_turn_p50_ms: 0.0,
        cpu_per_turn_p95_ms: 0.0,
        cpu_class: "unavailable".to_owned(),
        sampler_overhead_pct: 0.0,
        sampler_cadence_overruns: 0,
        sampler_cadence_gaps: 0,
        details: serde_json::json!({"integration": "trapezoidal"}),
        measurement_complete: false,
        reference_envelope_pass: false,
        measurement_error: Some(detail),
    };
    if evidence.sampler_cadence_ns == 0 {
        return incomplete("memory-time-integral sampler cadence is zero".to_owned());
    }
    let expected_turns = expected_repetitions.saturating_mul(turns_per_repetition) as usize;
    if evidence.turns.len() != expected_turns {
        return incomplete(format!(
            "expected {expected_turns} memory-time turn windows, observed {}",
            evidence.turns.len()
        ));
    }
    let derived_collection_cpu_ns = evidence.samples.iter().fold(0_u64, |total, sample| {
        total.saturating_add(sample.collection_cpu_ns)
    });
    if derived_collection_cpu_ns != evidence.sampler_collection_cpu_ns {
        return incomplete(format!(
            "sampler CPU accounting disagrees: declared {}, derived {derived_collection_cpu_ns}",
            evidence.sampler_collection_cpu_ns
        ));
    }
    let mut derived_observation_wall_ns = 0_u64;
    let mut integrals = Vec::with_capacity(expected_turns);
    let mut cpu_per_turn_ms = Vec::with_capacity(expected_turns);
    let mut minimum_coverage_ratio = 1.0_f64;
    let mut maximum_gap_ns = 0_u64;
    let mut cadence_overruns = 0_usize;
    let mut cadence_gaps = 0_usize;
    let mut cadence_untrustworthy = false;
    let mut repetition_details = Vec::new();
    let mut all_repetitions_pass = true;
    for repetition in 1..=expected_repetitions {
        let mut repetition_integrals = Vec::with_capacity(turns_per_repetition as usize);
        let mut repetition_cpu_ms = Vec::with_capacity(turns_per_repetition as usize);
        let mut repetition_covered_ns = 0_u64;
        let mut repetition_turn_wall_ns = 0_u64;
        let mut repetition_maximum_gap_ns = 0_u64;
        let samples = evidence
            .samples
            .iter()
            .filter(|sample| sample.repetition == repetition)
            .collect::<Vec<_>>();
        if samples.len() < 2 {
            return incomplete(format!(
                "repetition {repetition} has fewer than two counter samples"
            ));
        }
        if samples.iter().any(|sample| sample.monotonic_ns == 0)
            || samples
                .windows(2)
                .any(|pair| pair[1].monotonic_ns <= pair[0].monotonic_ns)
        {
            return incomplete(format!(
                "repetition {repetition} sample times are not strictly increasing nonzero clocks"
            ));
        }
        if samples
            .windows(2)
            .any(|pair| pair[1].cpu_ns < pair[0].cpu_ns)
        {
            return incomplete(format!(
                "repetition {repetition} cumulative whole-tree CPU regressed"
            ));
        }
        let first_ns = samples.first().map_or(0, |sample| sample.monotonic_ns);
        let last_ns = samples
            .last()
            .map_or(first_ns, |sample| sample.monotonic_ns);
        derived_observation_wall_ns =
            derived_observation_wall_ns.saturating_add(last_ns.saturating_sub(first_ns));
        let sample_times = samples
            .iter()
            .map(|sample| sample.monotonic_ns)
            .collect::<Vec<_>>();
        let (_, repetition_gaps, trustworthy) =
            cadence_quality(&sample_times, evidence.sampler_cadence_ns);
        cadence_gaps = cadence_gaps.saturating_add(repetition_gaps);
        cadence_untrustworthy |= !trustworthy;
        cadence_overruns = cadence_overruns.saturating_add(
            samples
                .iter()
                .filter(|sample| sample.collection_wall_ns > evidence.sampler_cadence_ns)
                .count(),
        );
        let baseline_bytes = if per_invocation {
            0_u64
        } else {
            let Some(value) = evidence.warm_idle_baseline_bytes.get(&repetition).copied() else {
                return incomplete(format!(
                    "repetition {repetition} lacks a fresh warm-idle baseline"
                ));
            };
            value
        };
        let turns = evidence
            .turns
            .iter()
            .filter(|turn| turn.repetition == repetition)
            .collect::<Vec<_>>();
        if turns.len() != turns_per_repetition as usize
            || turns
                .iter()
                .enumerate()
                .any(|(index, turn)| turn.turn_index != index as u32 + 1)
        {
            return incomplete(format!(
                "repetition {repetition} lacks the ordered 1..={turns_per_repetition} turn sequence"
            ));
        }
        for turn in turns {
            let boundaries = if per_invocation {
                turn.launch_ns.zip(turn.exit_ns)
            } else {
                turn.submit_ns.zip(turn.terminal_ns)
            };
            let Some((start_ns, end_ns)) = boundaries else {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks topology-specific boundaries",
                    turn.turn_index
                ));
            };
            if start_ns == 0 || end_ns <= start_ns {
                return incomplete(format!(
                    "repetition {repetition} turn {} has invalid or reversed boundaries",
                    turn.turn_index
                ));
            }
            let Some(before_index) = samples
                .iter()
                .rposition(|sample| sample.monotonic_ns <= start_ns)
            else {
                return incomplete(format!(
                    "repetition {repetition} turn {} is not bracketed before start",
                    turn.turn_index
                ));
            };
            let Some(after_index) = samples
                .iter()
                .position(|sample| sample.monotonic_ns >= end_ns)
            else {
                return incomplete(format!(
                    "repetition {repetition} turn {} is not bracketed after end",
                    turn.turn_index
                ));
            };
            if before_index >= after_index {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks distinct bracketing samples",
                    turn.turn_index
                ));
            }
            let bracket = &samples[before_index..=after_index];
            if !bracket.iter().any(|sample| sample.owned_processes > 0) {
                return incomplete(format!(
                    "repetition {repetition} turn {} never sampled an owned process",
                    turn.turn_index
                ));
            }
            let Some(start_memory) = interpolate_counter(bracket, start_ns, true) else {
                return incomplete("could not interpolate turn-start memory".to_owned());
            };
            let Some(end_memory) = interpolate_counter(bracket, end_ns, true) else {
                return incomplete("could not interpolate turn-end memory".to_owned());
            };
            let Some(start_cpu) = interpolate_counter(bracket, start_ns, false) else {
                return incomplete("could not interpolate turn-start CPU".to_owned());
            };
            let Some(end_cpu) = interpolate_counter(bracket, end_ns, false) else {
                return incomplete("could not interpolate turn-end CPU".to_owned());
            };
            if end_cpu < start_cpu {
                return incomplete(format!(
                    "repetition {repetition} turn {} interpolated CPU regressed",
                    turn.turn_index
                ));
            }
            let effective = |bytes: f64| (bytes - baseline_bytes as f64).max(0.0);
            let mut points = vec![(start_ns, effective(start_memory))];
            points.extend(bracket.iter().filter_map(|sample| {
                (sample.monotonic_ns > start_ns && sample.monotonic_ns < end_ns).then_some((
                    sample.monotonic_ns,
                    effective(sample.effective_memory_bytes as f64),
                ))
            }));
            points.push((end_ns, effective(end_memory)));
            let integral = points.windows(2).fold(0.0_f64, |total, pair| {
                let elapsed_seconds = pair[1].0.saturating_sub(pair[0].0) as f64 / 1_000_000_000.0;
                let mean_mib = (pair[0].1 + pair[1].1) / 2.0 / 1_048_576.0;
                total + mean_mib * elapsed_seconds
            });
            integrals.push(integral);
            repetition_integrals.push(integral);
            let cpu_ms = (end_cpu - start_cpu) / 1_000_000.0;
            cpu_per_turn_ms.push(cpu_ms);
            repetition_cpu_ms.push(cpu_ms);
            repetition_turn_wall_ns =
                repetition_turn_wall_ns.saturating_add(end_ns.saturating_sub(start_ns));
            for pair in bracket.windows(2) {
                let gap_ns = pair[1].monotonic_ns.saturating_sub(pair[0].monotonic_ns);
                let overlap_start = pair[0].monotonic_ns.max(start_ns);
                let overlap_end = pair[1].monotonic_ns.min(end_ns);
                if overlap_end > overlap_start {
                    repetition_covered_ns = repetition_covered_ns
                        .saturating_add(overlap_end.saturating_sub(overlap_start));
                    repetition_maximum_gap_ns = repetition_maximum_gap_ns.max(gap_ns);
                }
            }
        }
        if repetition_turn_wall_ns == 0 {
            return incomplete(format!(
                "repetition {repetition} measured zero total turn wall"
            ));
        }
        let repetition_coverage = repetition_covered_ns as f64 / repetition_turn_wall_ns as f64;
        if repetition_coverage < 0.99 {
            return incomplete(format!(
                "repetition {repetition} memory-time-integral coverage {repetition_coverage:.6} is below 0.99"
            ));
        }
        if repetition_maximum_gap_ns > evidence.sampler_cadence_ns.saturating_mul(2) {
            return incomplete(format!(
                "repetition {repetition} maximum sample gap {repetition_maximum_gap_ns} ns exceeds twice cadence {} ns",
                evidence.sampler_cadence_ns
            ));
        }
        repetition_integrals.sort_by(f64::total_cmp);
        let repetition_integral = median_sorted_f64(&repetition_integrals);
        let repetition_cpu_p95 = nearest_rank_f64(&repetition_cpu_ms, 95);
        all_repetitions_pass &= repetition_integral <= 1_024.0 && repetition_cpu_p95 <= 250.0;
        minimum_coverage_ratio = minimum_coverage_ratio.min(repetition_coverage);
        maximum_gap_ns = maximum_gap_ns.max(repetition_maximum_gap_ns);
        repetition_details.push(serde_json::json!({
            "repetition": repetition,
            "turns": turns_per_repetition,
            "coverage_ratio": repetition_coverage,
            "max_sample_gap_ms": repetition_maximum_gap_ns as f64 / 1_000_000.0,
            "memory_time_integral_mib_s_per_turn": repetition_integral,
            "cpu_per_turn_p95_ms": repetition_cpu_p95,
        }));
    }
    if derived_observation_wall_ns != evidence.sampler_observation_wall_ns
        || evidence.sampler_observation_wall_ns == 0
    {
        return incomplete(format!(
            "sampler observation wall accounting disagrees: declared {}, derived {derived_observation_wall_ns}",
            evidence.sampler_observation_wall_ns
        ));
    }
    let sampler_cpu_fraction =
        evidence.sampler_collection_cpu_ns as f64 / evidence.sampler_observation_wall_ns as f64;
    integrals.sort_by(f64::total_cmp);
    let integral = median_sorted_f64(&integrals);
    let coverage_ratio = minimum_coverage_ratio;
    let maximum_gap_ms = maximum_gap_ns as f64 / 1_000_000.0;
    if sampler_cpu_fraction > 0.10 || cadence_overruns > 0 || cadence_untrustworthy {
        return incomplete(format!(
            "sampler overload: {:.3}% of one core, {cadence_overruns} cadence overruns, {cadence_gaps} cadence gaps",
            sampler_cpu_fraction * 100.0,
        ));
    }
    let cpu_p50_ms = nearest_rank_f64(&cpu_per_turn_ms, 50);
    let cpu_p95_ms = nearest_rank_f64(&cpu_per_turn_ms, 95);
    let cpu_class = if cpu_p95_ms <= 10.0 {
        "C10"
    } else if cpu_p95_ms <= 50.0 {
        "C50"
    } else if cpu_p95_ms <= 250.0 {
        "C250"
    } else {
        "C250+"
    };
    let sampler_overhead_pct = sampler_cpu_fraction * 100.0;
    MemoryTimeIntegralEvaluation {
        memory_time_integral_mib_s_per_turn: integral,
        memory_time_integral_coverage_ratio: coverage_ratio,
        memory_time_integral_max_sample_gap_ms: maximum_gap_ms,
        cpu_per_turn_p50_ms: cpu_p50_ms,
        cpu_per_turn_p95_ms: cpu_p95_ms,
        cpu_class: cpu_class.to_owned(),
        sampler_overhead_pct,
        sampler_cadence_overruns: cadence_overruns,
        sampler_cadence_gaps: cadence_gaps,
        details: serde_json::json!({
            "integration": "trapezoidal",
            "collector": "continuous-long-horizon",
            "repetitions": repetition_details,
            "sampler_cadence_ns": evidence.sampler_cadence_ns,
            "sampler_collection_cpu_ns": evidence.sampler_collection_cpu_ns,
            "sampler_observation_wall_ns": evidence.sampler_observation_wall_ns,
            "sampler_overhead_pct": sampler_overhead_pct,
            "sampler_cadence_overruns": cadence_overruns,
            "sampler_cadence_gaps": cadence_gaps,
            "sampler_warnings": evidence.samples.iter().flat_map(|sample| sample.cpu_accounting_warnings.iter()).cloned().collect::<BTreeSet<_>>(),
        }),
        measurement_complete: true,
        reference_envelope_pass: all_repetitions_pass,
        measurement_error: None,
    }
}

/// Evaluate external row-43 turn observations without interacting with the harness.
pub fn evaluate_turn_latency(
    observations: &[TurnObservation],
    expected_turns: u32,
    per_invocation: bool,
    turn_timeout_ms: u64,
) -> TurnLatencyEvaluation {
    evaluate_turn_latency_repetitions(
        observations,
        1,
        expected_turns,
        per_invocation,
        turn_timeout_ms,
    )
}

/// Evaluate row 43 per fresh-profile repetition and then aggregate its exact
/// headline statistics without allowing one repetition to hide another.
pub fn evaluate_turn_latency_repetitions(
    observations: &[TurnObservation],
    expected_repetitions: u32,
    turns_per_repetition: u32,
    per_invocation: bool,
    turn_timeout_ms: u64,
) -> TurnLatencyEvaluation {
    let incomplete = |detail: String| TurnLatencyEvaluation {
        wall_per_turn_p50_ms: 0.0,
        wall_per_turn_p95_ms: 0.0,
        wall_per_turn_max_ms: 0.0,
        wall_per_turn_mad_ms: 0.0,
        wall_per_turn_jitter_ratio: 0.0,
        latency_class: "unavailable".to_owned(),
        measurement_complete: false,
        reference_envelope_pass: false,
        measurement_error: Some(detail),
    };
    let expected_turns = expected_repetitions.saturating_mul(turns_per_repetition);
    if observations.len() != expected_turns as usize {
        return incomplete(format!(
            "expected {expected_turns} turn intervals, observed {}",
            observations.len()
        ));
    }
    let timeout_ns = turn_timeout_ms.saturating_mul(1_000_000);
    let mut p50_values = Vec::new();
    let mut p95_values = Vec::new();
    let mut mad_values = Vec::new();
    let mut jitter_values = Vec::new();
    let mut maximum_ns = 0_u64;
    let mut all_repetitions_pass = true;
    for repetition in 1..=expected_repetitions {
        let repetition_observations = observations
            .iter()
            .filter(|observation| observation.repetition == repetition)
            .collect::<Vec<_>>();
        if repetition_observations.len() != turns_per_repetition as usize {
            return incomplete(format!(
                "repetition {repetition} expected {turns_per_repetition} turn intervals, observed {}",
                repetition_observations.len()
            ));
        }
        let mut walls = Vec::with_capacity(repetition_observations.len());
        for (index, observation) in repetition_observations.iter().enumerate() {
            let expected_index = index as u32 + 1;
            if observation.turn_index != expected_index {
                return incomplete(format!(
                    "repetition {repetition} turn index expected {expected_index}, observed {}",
                    observation.turn_index
                ));
            }
            let boundaries = if per_invocation {
                observation.launch_ns.zip(observation.exit_ns)
            } else {
                observation.submit_ns.zip(observation.terminal_ns)
            };
            let Some((start_ns, end_ns)) = boundaries else {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks required {} boundaries",
                    observation.turn_index,
                    if per_invocation {
                        "launch/exit"
                    } else {
                        "submit/terminal"
                    }
                ));
            };
            let Some(expected_wall_ns) = end_ns.checked_sub(start_ns) else {
                return incomplete(format!(
                    "repetition {repetition} turn {} external boundaries are reversed",
                    observation.turn_index
                ));
            };
            let Some(wall_ns) = observation.turn_wall_ns else {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks turn_wall_ns",
                    observation.turn_index
                ));
            };
            if wall_ns == 0 || wall_ns != expected_wall_ns {
                return incomplete(format!(
                    "repetition {repetition} turn {} has an invalid external wall interval",
                    observation.turn_index
                ));
            }
            walls.push(wall_ns);
        }
        walls.sort_unstable();
        let p50_ns = nearest_rank_u64(&walls, 50);
        let p95_ns = nearest_rank_u64(&walls, 95);
        let max_ns = walls.last().copied().unwrap_or(0);
        let mut absolute_deviations = walls
            .iter()
            .map(|value| value.abs_diff(p50_ns))
            .collect::<Vec<_>>();
        absolute_deviations.sort_unstable();
        let mad_ns = nearest_rank_u64(&absolute_deviations, 50);
        let jitter = mad_ns as f64 / p50_ns as f64;
        p50_values.push(p50_ns as f64 / 1_000_000.0);
        p95_values.push(p95_ns as f64 / 1_000_000.0);
        mad_values.push(mad_ns as f64 / 1_000_000.0);
        jitter_values.push(jitter);
        maximum_ns = maximum_ns.max(max_ns);
        all_repetitions_pass &= max_ns < timeout_ns && p95_ns <= 1_000_000_000 && jitter <= 0.25;
    }
    p50_values.sort_by(f64::total_cmp);
    p95_values.sort_by(f64::total_cmp);
    mad_values.sort_by(f64::total_cmp);
    jitter_values.sort_by(f64::total_cmp);
    let p50_ms = median_sorted_f64(&p50_values);
    let p95_ms = median_sorted_f64(&p95_values);
    let max_ms = maximum_ns as f64 / 1_000_000.0;
    let mad_ms = median_sorted_f64(&mad_values);
    let jitter = median_sorted_f64(&jitter_values);
    let latency_class = if p95_ms <= 100.0 {
        "L100"
    } else if p95_ms <= 250.0 {
        "L250"
    } else if p95_ms <= 500.0 {
        "L500"
    } else if p95_ms <= 1_000.0 {
        "L1000"
    } else {
        "L1000+"
    };
    TurnLatencyEvaluation {
        wall_per_turn_p50_ms: p50_ms,
        wall_per_turn_p95_ms: p95_ms,
        wall_per_turn_max_ms: max_ms,
        wall_per_turn_mad_ms: mad_ms,
        wall_per_turn_jitter_ratio: jitter,
        latency_class: latency_class.to_owned(),
        measurement_complete: true,
        reference_envelope_pass: all_repetitions_pass,
        measurement_error: None,
    }
}

const LATENCY_SLOPE_BASELINE_FRACTION: f64 = 0.05;
const LATENCY_SLOPE_ABSOLUTE_FLOOR_MS_PER_100_TURNS: f64 = 25.0;

fn latency_slope_bound_ms_per_100_turns(first_decile_p50_ms: f64) -> f64 {
    (LATENCY_SLOPE_BASELINE_FRACTION * first_decile_p50_ms)
        .max(LATENCY_SLOPE_ABSOLUTE_FLOOR_MS_PER_100_TURNS)
}

/// Evaluate row 49 from the row-29 growing-session turn clocks.
pub fn evaluate_latency_vs_turn_index(
    observations: &[TurnObservation],
    expected_repetitions: u32,
    turns_per_repetition: u32,
    per_invocation: bool,
) -> LatencyVsTurnIndexEvaluation {
    let incomplete = |detail: String| LatencyVsTurnIndexEvaluation {
        metrics: BTreeMap::new(),
        first_decile_p50_ms: 0.0,
        last_decile_p50_ms: 0.0,
        theil_sen_ms_per_turn: 0.0,
        latency_slope_ms_per_100_turns: 0.0,
        latency_last_first_decile_ratio: None,
        details: serde_json::json!({
            "measurement_complete": false,
            "measurement_error": detail,
        }),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(detail),
        failure_detail: None,
    };
    if expected_repetitions == 0 || turns_per_repetition < 10 || turns_per_repetition % 10 != 0 {
        return incomplete(format!(
            "row-49 plan requires repetitions and a positive whole-decile turn count; repetitions={expected_repetitions}, turns={turns_per_repetition}"
        ));
    }
    let expected_turns = expected_repetitions.saturating_mul(turns_per_repetition);
    if observations.len() != expected_turns as usize {
        return incomplete(format!(
            "expected {expected_turns} long-session turn intervals, observed {}",
            observations.len()
        ));
    }

    let decile_len = usize::try_from(turns_per_repetition / 10).unwrap_or(usize::MAX);
    let mut first_deciles = Vec::with_capacity(expected_repetitions as usize);
    let mut last_deciles = Vec::with_capacity(expected_repetitions as usize);
    let mut slopes = Vec::with_capacity(expected_repetitions as usize);
    let mut repetition_details = Vec::with_capacity(expected_repetitions as usize);

    for repetition in 1..=expected_repetitions {
        let mut turns = observations
            .iter()
            .filter(|observation| observation.repetition == repetition)
            .collect::<Vec<_>>();
        turns.sort_by_key(|observation| observation.turn_index);
        if turns.len() != turns_per_repetition as usize {
            return incomplete(format!(
                "repetition {repetition} expected {turns_per_repetition} long-session intervals, observed {}",
                turns.len()
            ));
        }
        let Some(first_turn) = turns.first() else {
            return incomplete(format!("repetition {repetition} has no long-session turns"));
        };
        if first_turn.actor.is_empty() || first_turn.session_id_hash.is_empty() {
            return incomplete(format!(
                "repetition {repetition} lacks the growing-session actor or session identity"
            ));
        }
        let expected_actor = first_turn.actor.as_str();
        let expected_session = first_turn.session_id_hash.as_str();
        let mut wall_ms = Vec::with_capacity(turns.len());
        for (index, observation) in turns.iter().enumerate() {
            let expected_index = index as u32 + 1;
            if observation.turn_index != expected_index {
                return incomplete(format!(
                    "repetition {repetition} turn index expected {expected_index}, observed {}",
                    observation.turn_index
                ));
            }
            if observation.phase != "latency-vs-turn-index"
                || observation.actor != expected_actor
                || observation.session_id_hash != expected_session
            {
                return incomplete(format!(
                    "repetition {repetition} turn {} is not attributable to one growing row-49 session",
                    observation.turn_index
                ));
            }
            if observation.terminal_ns.is_none() {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks a structured-terminal boundary",
                    observation.turn_index
                ));
            }
            let boundaries = if per_invocation {
                observation.launch_ns.zip(observation.exit_ns)
            } else {
                observation.submit_ns.zip(observation.terminal_ns)
            };
            let Some((start_ns, end_ns)) = boundaries else {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks required {} boundaries",
                    observation.turn_index,
                    if per_invocation {
                        "launch/exit"
                    } else {
                        "submit/terminal"
                    }
                ));
            };
            let Some(expected_wall_ns) = end_ns.checked_sub(start_ns) else {
                return incomplete(format!(
                    "repetition {repetition} turn {} external boundaries are reversed",
                    observation.turn_index
                ));
            };
            let Some(turn_wall_ns) = observation.turn_wall_ns else {
                return incomplete(format!(
                    "repetition {repetition} turn {} lacks turn_wall_ns",
                    observation.turn_index
                ));
            };
            if turn_wall_ns != expected_wall_ns {
                return incomplete(format!(
                    "repetition {repetition} turn {} wall interval disagrees with its external boundaries",
                    observation.turn_index
                ));
            }
            wall_ms.push(turn_wall_ns as f64 / 1_000_000.0);
        }

        let first_decile_p50_ms = nearest_rank_f64(&wall_ms[..decile_len], 50);
        let last_decile_p50_ms =
            nearest_rank_f64(&wall_ms[wall_ms.len().saturating_sub(decile_len)..], 50);
        let theil_sen_ms_per_turn = theil_sen_indexed_f64(&wall_ms);
        let slope_ms_per_100_turns = theil_sen_ms_per_turn * 100.0;
        let ratio =
            (first_decile_p50_ms != 0.0).then_some(last_decile_p50_ms / first_decile_p50_ms);
        let slope_bound_ms_per_100_turns =
            latency_slope_bound_ms_per_100_turns(first_decile_p50_ms);
        let repetition_passed = slope_ms_per_100_turns <= slope_bound_ms_per_100_turns
            && ratio.is_some_and(|value| value <= 1.25)
            && last_decile_p50_ms <= 1.25 * first_decile_p50_ms + 50.0;
        first_deciles.push(first_decile_p50_ms);
        last_deciles.push(last_decile_p50_ms);
        slopes.push(theil_sen_ms_per_turn);
        repetition_details.push(serde_json::json!({
            "repetition": repetition,
            "turns": turns_per_repetition,
            "first_decile_p50_ms": first_decile_p50_ms,
            "last_decile_p50_ms": last_decile_p50_ms,
            "theil_sen_ms_per_turn": theil_sen_ms_per_turn,
            "latency_slope_ms_per_100_turns": slope_ms_per_100_turns,
            "latency_last_first_decile_ratio": ratio,
            "slope_bound_ms_per_100_turns": slope_bound_ms_per_100_turns,
            "passed": repetition_passed,
        }));
    }

    first_deciles.sort_by(f64::total_cmp);
    last_deciles.sort_by(f64::total_cmp);
    slopes.sort_by(f64::total_cmp);
    let first_decile_p50_ms = median_sorted_f64(&first_deciles);
    let last_decile_p50_ms = median_sorted_f64(&last_deciles);
    let theil_sen_ms_per_turn = median_sorted_f64(&slopes);
    let latency_slope_ms_per_100_turns = theil_sen_ms_per_turn * 100.0;
    // The schema explicitly defines the headline ratio from the two published
    // headline decile medians, rather than as a separately rounded aggregate.
    let latency_last_first_decile_ratio =
        (first_decile_p50_ms != 0.0).then_some(last_decile_p50_ms / first_decile_p50_ms);
    let headline_bound = latency_slope_bound_ms_per_100_turns(first_decile_p50_ms);
    let headline_passed = latency_slope_ms_per_100_turns <= headline_bound
        && latency_last_first_decile_ratio.is_some_and(|value| value <= 1.25)
        && last_decile_p50_ms <= 1.25 * first_decile_p50_ms + 50.0;
    let failure_detail = (!headline_passed).then(|| {
        format!(
            "headline: slope100={latency_slope_ms_per_100_turns:.6}/{headline_bound:.6}ms ratio={} last={last_decile_p50_ms:.6}/{:.6}ms",
            latency_last_first_decile_ratio
                .map_or_else(|| "null".to_owned(), |value| format!("{value:.6}")),
            1.25 * first_decile_p50_ms + 50.0,
        )
    });
    // SPEC-v2 publishes the median of independently computed repetition
    // fields and applies the growth oracle to those headline fields. Every
    // repetition must still be structurally complete; that is enforced by
    // the fail-closed validation above. Retain each repetition's diagnostic
    // `passed` flag in details without introducing an additional veto.
    let passed = headline_passed;
    let metrics = BTreeMap::from([
        (
            "latency_vs_turn_index.first_decile_p50_ms".to_owned(),
            first_decile_p50_ms,
        ),
        (
            "latency_vs_turn_index.last_decile_p50_ms".to_owned(),
            last_decile_p50_ms,
        ),
        (
            "latency_vs_turn_index.theil_sen_ms_per_turn".to_owned(),
            theil_sen_ms_per_turn,
        ),
    ]);
    LatencyVsTurnIndexEvaluation {
        metrics,
        first_decile_p50_ms,
        last_decile_p50_ms,
        theil_sen_ms_per_turn,
        latency_slope_ms_per_100_turns,
        latency_last_first_decile_ratio,
        details: serde_json::json!({
            "measurement_complete": true,
            "measurement_error": null,
            "first_decile_p50_ms": first_decile_p50_ms,
            "last_decile_p50_ms": last_decile_p50_ms,
            "theil_sen_ms_per_turn": theil_sen_ms_per_turn,
            "latency_slope_ms_per_100_turns": latency_slope_ms_per_100_turns,
            "latency_last_first_decile_ratio": latency_last_first_decile_ratio,
            "repetitions": repetition_details,
            "passed": passed,
            "failure_detail": failure_detail,
        }),
        measurement_complete: true,
        passed,
        measurement_error: None,
        failure_detail,
    }
}

fn theil_sen_indexed_f64(values: &[f64]) -> f64 {
    let pair_count = values.len().saturating_mul(values.len().saturating_sub(1)) / 2;
    let mut slopes = Vec::with_capacity(pair_count);
    for left in 0..values.len() {
        for right in (left + 1)..values.len() {
            slopes.push((values[right] - values[left]) / (right - left) as f64);
        }
    }
    slopes.sort_by(f64::total_cmp);
    median_sorted_f64(&slopes)
}

/// Complete row-44 aggregation and CORE oracle decision.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessHygieneEvaluation {
    /// Exact numeric `metrics` entries required by the v2 schema.
    pub metrics: BTreeMap<String, f64>,
    /// Exact structured `details.process-hygiene` value.
    pub details: Value,
    /// Whether every checkpoint, counter, and delayed audit was present.
    pub measurement_complete: bool,
    /// Whether every topology-specific residue and trend oracle passed.
    pub passed: bool,
    /// Deterministic diagnostic for incomplete evidence.
    pub measurement_error: Option<String>,
    /// Deterministic diagnostic for a measured CORE failure.
    pub failure_detail: Option<String>,
    /// Preserved sampler warnings for report evidence.
    pub sampler_warnings: Vec<String>,
    /// Calling-thread CPU consumed by the out-of-band collector.
    pub sampler_collection_cpu_ns: u64,
    /// Wall time consumed by the out-of-band collector.
    pub sampler_collection_wall_ns: u64,
    /// Active-turn sampler CPU divided by the cadence-covered turn wall time.
    pub sampler_overhead_pct: f64,
}

fn empty_process_hygiene_metrics() -> BTreeMap<String, f64> {
    [
        "process_hygiene.observed_processes_spawned_per_turn_p50",
        "process_hygiene.observed_processes_spawned_per_turn_max",
        "process_hygiene.observed_threads_created_per_turn_p50",
        "process_hygiene.observed_threads_created_per_turn_max",
        "process_hygiene.observed_fds_opened_per_turn_p50",
        "process_hygiene.observed_fds_opened_per_turn_max",
        "process_hygiene.peak_live_processes",
        "process_hygiene.peak_threads",
        "process_hygiene.peak_fds",
        "process_hygiene.residue_processes",
        "process_hygiene.residue_threads_delta",
        "process_hygiene.residue_fds_delta",
        "process_hygiene.unique_process_identities",
    ]
    .into_iter()
    .map(|name| (name.to_owned(), 0.0))
    .collect()
}

fn hygiene_process_map(
    processes: &[ProcessHygieneProcess],
) -> Option<BTreeMap<ProcIdentity, &ProcessHygieneProcess>> {
    let values = processes
        .iter()
        .map(|process| (process.identity, process))
        .collect::<BTreeMap<_, _>>();
    (values.len() == processes.len()).then_some(values)
}

fn hygiene_totals(processes: &[ProcessHygieneProcess]) -> Option<(u64, u64, u64)> {
    let mut threads = 0_u64;
    let mut fds = 0_u64;
    for process in processes {
        threads = threads.checked_add(process.thread_count?)?;
        fds = fds.checked_add(process.open_fds?)?;
    }
    Some((processes.len() as u64, threads, fds))
}

fn hygiene_counter_map(
    processes: &[ProcessHygieneProcess],
) -> Option<BTreeMap<ProcIdentity, (u64, u64)>> {
    let values = processes
        .iter()
        .map(|process| Some((process.identity, (process.thread_count?, process.open_fds?))))
        .collect::<Option<BTreeMap<_, _>>>()?;
    (values.len() == processes.len()).then_some(values)
}

fn ownership_label(ownership: &ProcOwnership) -> &'static str {
    match ownership {
        ProcOwnership::DeclaredRoot => "declared-root",
        ProcOwnership::Descendant => "descendant",
        ProcOwnership::CgroupMember => "cgroup-member",
        ProcOwnership::ProcessGroupMember => "process-group-member",
        ProcOwnership::Reparented => "reparented",
    }
}

/// Evaluate row-44 from already captured external checkpoints and residue audits.
pub fn evaluate_process_hygiene(
    evidence: &ProcessHygieneEvidence,
    expected_repetitions: u32,
    turns_per_repetition: u32,
    per_invocation: bool,
) -> ProcessHygieneEvaluation {
    let incomplete = |detail: String| ProcessHygieneEvaluation {
        metrics: empty_process_hygiene_metrics(),
        details: serde_json::json!({"residue_identities": []}),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(detail),
        failure_detail: None,
        sampler_warnings: evidence.sampler_warnings.clone(),
        sampler_collection_cpu_ns: evidence.sampler_collection_cpu_ns,
        sampler_collection_wall_ns: evidence.sampler_collection_wall_ns,
        sampler_overhead_pct: 0.0,
    };
    let expected_checkpoints = expected_repetitions.saturating_mul(turns_per_repetition) as usize;
    if evidence.checkpoints.len() != expected_checkpoints {
        return incomplete(format!(
            "expected {expected_checkpoints} active checkpoints, observed {}",
            evidence.checkpoints.len()
        ));
    }
    for repetition in 1..=expected_repetitions {
        let checkpoints = evidence
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        if checkpoints.len() != turns_per_repetition as usize
            || checkpoints
                .iter()
                .enumerate()
                .any(|(index, checkpoint)| checkpoint.turn_index != index as u32 + 1)
        {
            return incomplete(format!(
                "repetition {repetition} lacks the exact ordered 1..={turns_per_repetition} checkpoint sequence"
            ));
        }
        let growth = evidence
            .growth_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        if growth.len() != turns_per_repetition as usize + 1
            || growth
                .iter()
                .enumerate()
                .any(|(index, checkpoint)| checkpoint.turn_index != index as u32)
        {
            return incomplete(format!(
                "repetition {repetition} lacks the exact ordered 0..={turns_per_repetition} growth checkpoint sequence"
            ));
        }
    }
    const REQUIRED_CADENCE_NS: u64 = 10_000_000;
    let mut derived_active_cpu_ns = 0_u64;
    let mut derived_sampled_wall_ns = 0_u64;
    for checkpoint in &evidence.checkpoints {
        if checkpoint.required_cadence_ns != REQUIRED_CADENCE_NS {
            return incomplete(format!(
                "repetition {} turn {} row-44 cadence is {} ns, expected {REQUIRED_CADENCE_NS} ns",
                checkpoint.repetition, checkpoint.turn_index, checkpoint.required_cadence_ns
            ));
        }
        if checkpoint.sampled_wall_ns == 0 || checkpoint.cadence_samples.len() < 2 {
            return incomplete(format!(
                "repetition {} turn {} lacks two boundary samples spanning a nonzero active window",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        let times = checkpoint
            .cadence_samples
            .iter()
            .map(|sample| sample.elapsed_ns)
            .collect::<Vec<_>>();
        if times.windows(2).any(|pair| pair[1] <= pair[0]) {
            return incomplete(format!(
                "repetition {} turn {} cadence timestamps are not strictly increasing",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        if times.first().copied().unwrap_or(REQUIRED_CADENCE_NS) > REQUIRED_CADENCE_NS
            || times
                .last()
                .copied()
                .unwrap_or(0)
                .saturating_add(REQUIRED_CADENCE_NS)
                < checkpoint.sampled_wall_ns
        {
            return incomplete(format!(
                "repetition {} turn {} cadence samples do not cover the active boundaries",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        let (maximum_gap_ns, cadence_gaps, trustworthy) =
            crate::sampler::cadence_quality(&times, REQUIRED_CADENCE_NS);
        if !trustworthy {
            return incomplete(format!(
                "repetition {} turn {} has untrustworthy cadence coverage: {cadence_gaps} gap(s), maximum {maximum_gap_ns} ns",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        for sample in &checkpoint.cadence_samples {
            if sample.collection_wall_ns == 0 {
                return incomplete(format!(
                    "repetition {} turn {} sampler collection wall time is absent",
                    checkpoint.repetition, checkpoint.turn_index
                ));
            }
            derived_active_cpu_ns = derived_active_cpu_ns.saturating_add(sample.collection_cpu_ns);
        }
        derived_sampled_wall_ns =
            derived_sampled_wall_ns.saturating_add(checkpoint.sampled_wall_ns);
    }
    if evidence.active_sampler_collection_cpu_ns != derived_active_cpu_ns
        || evidence.sampled_turn_wall_ns != derived_sampled_wall_ns
        || derived_sampled_wall_ns == 0
    {
        return incomplete(format!(
            "row-44 sampler accounting disagrees with cadence samples: cpu {} vs {derived_active_cpu_ns}, wall {} vs {derived_sampled_wall_ns}",
            evidence.active_sampler_collection_cpu_ns, evidence.sampled_turn_wall_ns
        ));
    }
    let sampler_overhead_pct =
        100.0 * derived_active_cpu_ns as f64 / derived_sampled_wall_ns as f64;
    if sampler_overhead_pct > 10.0 {
        return incomplete(format!(
            "sampler overload: {sampler_overhead_pct:.3}% row-44 active-turn sampler CPU"
        ));
    }
    let required_audits = if per_invocation {
        &evidence.per_turn_audits
    } else {
        if evidence.warm_baselines.len() != expected_repetitions as usize
            || evidence.post_close_audits.len() != expected_repetitions as usize
            || evidence.shutdown_audits.len() != expected_repetitions as usize
        {
            return incomplete(
                "daemon hygiene requires one warm baseline, post-close audit, and shutdown audit per repetition"
                    .to_owned(),
            );
        }
        &evidence.shutdown_audits
    };
    if per_invocation && required_audits.len() != expected_checkpoints {
        return incomplete(format!(
            "expected {expected_checkpoints} per-invocation residue audits, observed {}",
            required_audits.len()
        ));
    }
    if per_invocation {
        for repetition in 1..=expected_repetitions {
            let audits = evidence
                .per_turn_audits
                .iter()
                .filter(|audit| audit.repetition == repetition)
                .collect::<Vec<_>>();
            if audits.len() != turns_per_repetition as usize
                || audits
                    .iter()
                    .enumerate()
                    .any(|(index, audit)| audit.turn_index != Some(index as u32 + 1))
            {
                return incomplete(format!(
                    "repetition {repetition} lacks the exact ordered 1..={turns_per_repetition} post-exit audit sequence"
                ));
            }
        }
    } else {
        for repetition in 1..=expected_repetitions {
            if evidence.warm_baselines[repetition as usize - 1].repetition != repetition
                || evidence.post_close_audits[repetition as usize - 1].repetition != repetition
                || evidence.shutdown_audits[repetition as usize - 1].repetition != repetition
            {
                return incomplete(
                    "daemon hygiene audits are not in exact repetition order".to_owned(),
                );
            }
        }
    }
    let delayed_audits = evidence
        .per_turn_audits
        .iter()
        .chain(evidence.post_close_audits.iter())
        .chain(evidence.shutdown_audits.iter());
    if delayed_audits.clone().any(|audit| audit.waited_ms < 2_000) {
        return incomplete("a process-hygiene residue audit was shorter than 2,000 ms".to_owned());
    }
    let all_process_sets = evidence
        .checkpoints
        .iter()
        .flat_map(|checkpoint| {
            checkpoint
                .cadence_samples
                .iter()
                .map(|sample| sample.processes.as_slice())
        })
        .chain(
            evidence
                .warm_baselines
                .iter()
                .map(|checkpoint| checkpoint.processes.as_slice()),
        )
        .chain(
            evidence
                .growth_checkpoints
                .iter()
                .map(|checkpoint| checkpoint.processes.as_slice()),
        )
        .chain(
            evidence
                .per_turn_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .chain(
            evidence
                .post_close_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .chain(
            evidence
                .shutdown_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .collect::<Vec<_>>();
    if all_process_sets.iter().any(|processes| {
        hygiene_process_map(processes).is_none() || hygiene_totals(processes).is_none()
    }) {
        return incomplete(
            "process-hygiene evidence has duplicate identities or incomplete thread/FD counters"
                .to_owned(),
        );
    }

    let mut spawned = Vec::with_capacity(expected_checkpoints);
    let mut threads_created = Vec::with_capacity(expected_checkpoints);
    let mut fds_opened = Vec::with_capacity(expected_checkpoints);
    let mut unique_identities = BTreeSet::new();
    let mut peak_processes = 0_u64;
    let mut peak_threads = 0_u64;
    let mut peak_fds = 0_u64;
    let mut failures = Vec::new();

    for repetition in 1..=expected_repetitions {
        let mut previous = if per_invocation {
            BTreeMap::new()
        } else {
            let Some(baseline) = evidence
                .warm_baselines
                .iter()
                .find(|baseline| baseline.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a warm baseline"));
            };
            let Some(processes) = hygiene_counter_map(&baseline.processes) else {
                return incomplete(format!(
                    "repetition {repetition} warm baseline repeats an identity or lacks counters"
                ));
            };
            for identity in processes.keys() {
                unique_identities.insert(*identity);
            }
            processes
        };
        let checkpoints = evidence
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        for checkpoint in checkpoints {
            let mut turn_threads = 0_u64;
            let mut turn_fds = 0_u64;
            let mut new_processes = 0_u64;
            for sample in &checkpoint.cadence_samples {
                let Some(current) = hygiene_counter_map(&sample.processes) else {
                    return incomplete(format!(
                        "repetition {repetition} turn {} cadence sample repeats an identity or lacks counters",
                        checkpoint.turn_index
                    ));
                };
                for (identity, (current_threads, current_fds)) in &current {
                    let first_observation = unique_identities.insert(*identity);
                    if let Some((prior_threads, prior_fds)) = previous.get(identity) {
                        turn_threads = turn_threads
                            .saturating_add(current_threads.saturating_sub(*prior_threads));
                        turn_fds = turn_fds.saturating_add(current_fds.saturating_sub(*prior_fds));
                    } else if first_observation {
                        new_processes = new_processes.saturating_add(1);
                        turn_threads = turn_threads.saturating_add(*current_threads);
                        turn_fds = turn_fds.saturating_add(*current_fds);
                    }
                }
                let Some(totals) = hygiene_totals(&sample.processes) else {
                    return incomplete("active cadence counters are incomplete".to_owned());
                };
                peak_processes = peak_processes.max(totals.0);
                peak_threads = peak_threads.max(totals.1);
                peak_fds = peak_fds.max(totals.2);
                previous = current;
            }
            spawned.push(new_processes);
            threads_created.push(turn_threads);
            fds_opened.push(turn_fds);
        }
    }

    let mut monotonic_growth_ok = true;
    let mut growth_diagnostics = Vec::new();
    for repetition in 1..=expected_repetitions {
        let growth = evidence
            .growth_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        let mut totals = Vec::with_capacity(growth.len());
        for checkpoint in growth {
            let Some(total) = hygiene_totals(&checkpoint.processes) else {
                return incomplete(format!(
                    "repetition {repetition} growth checkpoint {} has incomplete counters",
                    checkpoint.turn_index
                ));
            };
            totals.push(total);
        }
        let threshold = (turns_per_repetition as usize).div_ceil(2);
        let mut counts = BTreeMap::new();
        for (label, dimension) in [("process", 0_usize), ("thread", 1), ("fd", 2)] {
            let increases = totals
                .windows(2)
                .filter(|pair| match dimension {
                    0 => pair[1].0 > pair[0].0,
                    1 => pair[1].1 > pair[0].1,
                    _ => pair[1].2 > pair[0].2,
                })
                .count();
            counts.insert(label, increases);
            monotonic_growth_ok &= increases < threshold;
        }
        growth_diagnostics.push(serde_json::json!({
            "repetition": repetition,
            "checkpoints": totals.len(),
            "failure_threshold": threshold,
            "increases": counts,
        }));
    }

    let mut residue_processes = 0_u64;
    let mut residue_threads_delta = 0_u64;
    let mut residue_fds_delta = 0_u64;
    let mut residue_identities = BTreeMap::<ProcIdentity, &ProcessHygieneProcess>::new();
    if per_invocation {
        for audit in &evidence.per_turn_audits {
            let Some((processes, threads, fds)) = hygiene_totals(&audit.processes) else {
                return incomplete("per-invocation residue counters are incomplete".to_owned());
            };
            residue_processes = residue_processes.max(processes);
            residue_threads_delta = residue_threads_delta.max(threads);
            residue_fds_delta = residue_fds_delta.max(fds);
            for process in &audit.processes {
                residue_identities
                    .entry(process.identity)
                    .or_insert(process);
            }
        }
        if residue_processes != 0 {
            failures.push(format!(
                "per-invocation residue remained after a 2,000 ms child-exit audit: {residue_processes} process(es)"
            ));
        }
    } else {
        for repetition in 1..=expected_repetitions {
            let Some(baseline) = evidence
                .warm_baselines
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a warm baseline"));
            };
            let Some(post_close) = evidence
                .post_close_audits
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a post-close audit"));
            };
            let Some(shutdown) = evidence
                .shutdown_audits
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a shutdown audit"));
            };
            let Some(baseline_map) = hygiene_process_map(&baseline.processes) else {
                return incomplete("warm baseline repeats an identity".to_owned());
            };
            let new_post_close = post_close
                .processes
                .iter()
                .filter(|process| !baseline_map.contains_key(&process.identity))
                .collect::<Vec<_>>();
            let Some((_, baseline_threads, baseline_fds)) = hygiene_totals(&baseline.processes)
            else {
                return incomplete("warm baseline counters are incomplete".to_owned());
            };
            let Some((_, post_threads, post_fds)) = hygiene_totals(&post_close.processes) else {
                return incomplete("post-close counters are incomplete".to_owned());
            };
            let Some((shutdown_processes, shutdown_threads, shutdown_fds)) =
                hygiene_totals(&shutdown.processes)
            else {
                return incomplete("shutdown counters are incomplete".to_owned());
            };
            let thread_delta = post_threads.saturating_sub(baseline_threads);
            let fd_delta = post_fds.saturating_sub(baseline_fds);
            residue_processes = residue_processes
                .max(new_post_close.len() as u64)
                .max(shutdown_processes);
            residue_threads_delta = residue_threads_delta
                .max(thread_delta)
                .max(shutdown_threads);
            residue_fds_delta = residue_fds_delta.max(fd_delta).max(shutdown_fds);
            for process in new_post_close.into_iter().chain(shutdown.processes.iter()) {
                residue_identities
                    .entry(process.identity)
                    .or_insert(process);
            }
            if post_close
                .processes
                .iter()
                .any(|process| !baseline_map.contains_key(&process.identity))
            {
                failures.push(format!(
                    "repetition {repetition} post-close state contains a new process identity"
                ));
            }
            if post_threads > baseline_threads.saturating_add(2) {
                failures.push(format!(
                    "repetition {repetition} post-close threads {post_threads} exceed warm baseline {baseline_threads} + 2"
                ));
            }
            if post_fds > baseline_fds.saturating_add(4) {
                failures.push(format!(
                    "repetition {repetition} post-close FDs {post_fds} exceed warm baseline {baseline_fds} + 4"
                ));
            }
            if shutdown_processes != 0 {
                failures.push(format!(
                    "repetition {repetition} shutdown left {shutdown_processes} owned process(es) after 2,000 ms"
                ));
            }
        }
    }

    for processes in &all_process_sets {
        for process in *processes {
            unique_identities.insert(process.identity);
        }
    }
    spawned.sort_unstable();
    threads_created.sort_unstable();
    fds_opened.sort_unstable();
    let max_or_zero = |values: &[u64]| values.iter().copied().max().unwrap_or(0) as f64;
    let mut metrics = empty_process_hygiene_metrics();
    metrics.insert(
        "process_hygiene.observed_processes_spawned_per_turn_p50".to_owned(),
        nearest_rank_u64(&spawned, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_processes_spawned_per_turn_max".to_owned(),
        max_or_zero(&spawned),
    );
    metrics.insert(
        "process_hygiene.observed_threads_created_per_turn_p50".to_owned(),
        nearest_rank_u64(&threads_created, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_threads_created_per_turn_max".to_owned(),
        max_or_zero(&threads_created),
    );
    metrics.insert(
        "process_hygiene.observed_fds_opened_per_turn_p50".to_owned(),
        nearest_rank_u64(&fds_opened, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_fds_opened_per_turn_max".to_owned(),
        max_or_zero(&fds_opened),
    );
    metrics.insert(
        "process_hygiene.peak_live_processes".to_owned(),
        peak_processes as f64,
    );
    metrics.insert(
        "process_hygiene.peak_threads".to_owned(),
        peak_threads as f64,
    );
    metrics.insert("process_hygiene.peak_fds".to_owned(), peak_fds as f64);
    metrics.insert(
        "process_hygiene.residue_processes".to_owned(),
        residue_processes as f64,
    );
    metrics.insert(
        "process_hygiene.residue_threads_delta".to_owned(),
        residue_threads_delta as f64,
    );
    metrics.insert(
        "process_hygiene.residue_fds_delta".to_owned(),
        residue_fds_delta as f64,
    );
    metrics.insert(
        "process_hygiene.unique_process_identities".to_owned(),
        unique_identities.len() as f64,
    );
    let residue = residue_identities
        .into_values()
        .map(|process| {
            serde_json::json!({
                "pid": process.identity.pid,
                "start_time": process.identity.start_time,
                "command": process.command,
                "ownership": ownership_label(&process.ownership),
            })
        })
        .collect::<Vec<_>>();
    let audits = evidence
        .per_turn_audits
        .iter()
        .chain(evidence.post_close_audits.iter())
        .chain(evidence.shutdown_audits.iter())
        .map(|audit| {
            let totals = hygiene_totals(&audit.processes).unwrap_or((0, 0, 0));
            serde_json::json!({
                "repetition": audit.repetition,
                "turn_index": audit.turn_index,
                "waited_ms": audit.waited_ms,
                "processes": totals.0,
                "threads": totals.1,
                "fds": totals.2,
            })
        })
        .collect::<Vec<_>>();
    let failure_detail = (!failures.is_empty()).then(|| failures.join("; "));
    ProcessHygieneEvaluation {
        metrics,
        details: serde_json::json!({
            "residue_identities": residue,
            "audits": audits,
            "growth_diagnostics": growth_diagnostics,
            "monotonic_growth_ok": monotonic_growth_ok,
        }),
        measurement_complete: true,
        passed: failure_detail.is_none(),
        measurement_error: None,
        failure_detail,
        sampler_warnings: evidence.sampler_warnings.clone(),
        sampler_collection_cpu_ns: evidence.sampler_collection_cpu_ns,
        sampler_collection_wall_ns: evidence.sampler_collection_wall_ns,
        sampler_overhead_pct,
    }
}

fn nearest_rank_u64(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

fn effective_memory_bytes(sample: &Sample) -> u64 {
    #[cfg(target_os = "macos")]
    {
        sample.footprint_bytes.unwrap_or(sample.rss_bytes)
    }
    #[cfg(target_os = "linux")]
    {
        sample.pss_bytes.unwrap_or(sample.rss_bytes)
    }
}

fn outcome_label(outcome: &TestOutcome) -> &'static str {
    match outcome {
        TestOutcome::Pass => "PASS",
        TestOutcome::Fail(_) => "FAIL",
        TestOutcome::Unsupported(_) => "UNSUPPORTED",
        TestOutcome::Error(_) => "ERROR",
        TestOutcome::Absent(_) => "ABSENT",
    }
}

fn render_junit(report: &Report) -> String {
    let failures = report
        .results
        .iter()
        .filter(|result| {
            !matches!(
                result.outcome,
                TestOutcome::Pass | TestOutcome::Unsupported(_)
            )
        })
        .count();
    let skipped = report
        .results
        .iter()
        .filter(|result| matches!(result.outcome, TestOutcome::Unsupported(_)))
        .count();
    let mut output = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"ahrb\" tests=\"{}\" failures=\"{}\" skipped=\"{}\">\n",
        report.results.len(),
        failures,
        skipped
    );
    let mut results: Vec<&TestResult> = report.results.iter().collect();
    results.sort_by_key(|result| result.row);
    for result in results {
        let _ = writeln!(
            output,
            "  <testcase classname=\"{:?}\" name=\"{}-{}\">",
            result.pillar,
            result.row,
            xml_escape(&result.id)
        );
        match &result.outcome {
            TestOutcome::Pass => {}
            TestOutcome::Unsupported(detail) => {
                let _ = writeln!(output, "    <skipped message=\"{}\" />", xml_escape(detail));
            }
            other => {
                let _ = writeln!(
                    output,
                    "    <failure message=\"{}\" />",
                    xml_escape(outcome_label(other))
                );
            }
        }
        let _ = writeln!(output, "  </testcase>");
    }
    output.push_str("</testsuite>\n");
    output
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod wave4_schema_tests {
    use super::*;

    #[test]
    fn wave4_metric_schema_is_exact_and_has_no_dynamic_keys() {
        assert_eq!(WAVE4_METRIC_KEYS.len(), 53);
        let unique = WAVE4_METRIC_KEYS.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), WAVE4_METRIC_KEYS.len());
        assert_eq!(
            WAVE4_METRIC_KEYS.first().copied(),
            Some("injection_surface.provider_score")
        );
        assert_eq!(
            WAVE4_METRIC_KEYS.last().copied(),
            Some("tool_result_role_fidelity.duplicate_results")
        );
    }

    #[test]
    fn wave4_known_detail_blocks_validate_nested_field_types() {
        let valid = serde_json::json!({
            "secrets-hygiene-on-disk": {
                "matches": [{"category":"log", "path":"logs/run.jsonl", "offset":7}]
            },
            "tool-result-role-fidelity": {
                "observations": [{
                    "repetition":1,
                    "dialect":"openai-chat-completions",
                    "call_id":"call-1",
                    "semantic_role":"tool",
                    "raw_pointer":"/messages/2"
                }]
            }
        });
        serde_json::from_value::<ReportDetails>(valid).expect("valid Wave-4 details");

        let invalid = serde_json::json!({
            "tool-result-role-fidelity": {
                "observations": [{
                    "repetition":"one",
                    "dialect":"openai-chat-completions",
                    "call_id":"call-1",
                    "semantic_role":"tool",
                    "raw_pointer":"/messages/2"
                }]
            }
        });
        let error = serde_json::from_value::<ReportDetails>(invalid)
            .expect_err("known Wave-4 detail field must retain its type");
        assert!(error.to_string().contains("tool-result-role-fidelity"));
    }
}
