//! Typed schema-1 storage evidence. Unknown fields in known row blocks are errors.
use super::*;
use crate::evaluate::TestOutcome;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResponseReceipt {
    pub repetition: u32,
    #[serde(flatten)]
    pub response: crate::fake_model::StorageResponseReceipt,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteSummary {
    pub write_bytes_per_turn_p50: Option<f64>,
    pub write_bytes_per_turn_p95: Option<f64>,
    pub write_bytes_per_turn_max: Option<f64>,
    pub logical_growth_bytes_per_turn: Option<f64>,
    pub net_growth_bytes_per_turn: Option<f64>,
    pub write_amplification_ratio: Option<f64>,
    pub disk_class: Option<String>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurabilitySummary {
    pub fsync_calls_per_turn: Option<f64>,
    pub fdatasync_calls_per_turn: Option<f64>,
    pub fullfsync_calls_per_turn: Option<f64>,
    pub durability_calls_per_turn: Option<f64>,
    pub assumed_fsync_cost_ms: Option<f64>,
    pub estimated_durability_wall_ms_per_turn: Option<f64>,
    pub durability_class: Option<String>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FootprintSummary {
    pub first_turn_allocated_bytes: Option<f64>,
    pub footprint_slope_bytes_per_turn: Option<f64>,
    pub growth_class: Option<GrowthClass>,
    pub footprint_curve: Vec<CurvePoint>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionSummary {
    pub compaction_before_allocated_bytes: Option<f64>,
    pub compaction_after_allocated_bytes: Option<f64>,
    pub compaction_freed_pct: Option<f64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloseSummary {
    pub closed_sessions: Option<u64>,
    pub close_retained_bytes_per_session: Option<f64>,
    pub close_retained_after_sweep_bytes_per_session: Option<f64>,
    pub retention_cap_bytes: Option<u64>,
    pub close_retention_class: Option<BoundClass>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSummary {
    pub delete_residue_allocated_bytes: Option<u64>,
    pub delete_residue_files: Option<u64>,
    pub uninstall_residue_allocated_bytes: Option<u64>,
    pub uninstall_residue_files: Option<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliarySummary {
    pub auxiliaries: Vec<Auxiliary>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionSummary {
    pub request_retention_class: Option<RetentionClass>,
    pub stored_request_bytes: Option<u64>,
    pub unique_request_content_bytes: Option<u64>,
    pub stored_unique_ratio: Option<f64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrashSummary {
    pub crash_residue_allocated_bytes: Option<u64>,
    pub crash_residue_files: Option<u64>,
    pub crash_resume_outcome: Option<CrashOutcome>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeSummary {
    pub resume_read_bytes_p50: Option<f64>,
    pub resume_read_bytes_p95: Option<f64>,
    pub resume_latency_p50_ms: Option<f64>,
    pub resume_latency_p95_ms: Option<f64>,
    pub resume_outcome: Option<ResumeOutcome>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BoundClass {
    Bounded,
    Unbounded,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RetentionClass {
    None,
    Deduplicated,
    Full,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CrashOutcome {
    Preserved,
    Corrupt,
    Failed,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ResumeOutcome {
    Preserved,
    Failed,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSummary {
    pub schema: u32,
    pub task: String,
    pub profile: String,
    pub os: String,
    pub topology: String,
    pub comparison_scope: String,
    pub turn_budget: u32,
    pub repetitions: u32,
    pub completed_turns: u64,
    pub physical_requests: u64,
    pub measurement_label: String,
    pub counter_source: String,
    pub allocation_source: String,
    pub declarations_sha256: String,
    pub write_bytes_per_turn_p50: Option<f64>,
    pub write_bytes_per_turn_p95: Option<f64>,
    pub write_bytes_per_turn_max: Option<f64>,
    pub logical_growth_bytes_per_turn: Option<f64>,
    pub net_growth_bytes_per_turn: Option<f64>,
    pub write_amplification_ratio: Option<f64>,
    pub disk_class: Option<String>,
    pub fsync_calls_per_turn: Option<f64>,
    pub fdatasync_calls_per_turn: Option<f64>,
    pub fullfsync_calls_per_turn: Option<f64>,
    pub durability_calls_per_turn: Option<f64>,
    pub assumed_fsync_cost_ms: Option<f64>,
    pub estimated_durability_wall_ms_per_turn: Option<f64>,
    pub durability_class: Option<String>,
    pub first_turn_allocated_bytes: Option<f64>,
    pub footprint_slope_bytes_per_turn: Option<f64>,
    pub growth_class: Option<GrowthClass>,
    pub compaction_before_allocated_bytes: Option<f64>,
    pub compaction_after_allocated_bytes: Option<f64>,
    pub compaction_freed_pct: Option<f64>,
    pub closed_sessions: Option<u64>,
    pub close_retained_bytes_per_session: Option<f64>,
    pub close_retained_after_sweep_bytes_per_session: Option<f64>,
    pub retention_cap_bytes: Option<u64>,
    pub close_retention_class: Option<BoundClass>,
    pub delete_residue_allocated_bytes: Option<u64>,
    pub delete_residue_files: Option<u64>,
    pub uninstall_residue_allocated_bytes: Option<u64>,
    pub uninstall_residue_files: Option<u64>,
    pub request_retention_class: Option<RetentionClass>,
    pub stored_request_bytes: Option<u64>,
    pub unique_request_content_bytes: Option<u64>,
    pub stored_unique_ratio: Option<f64>,
    pub crash_residue_allocated_bytes: Option<u64>,
    pub crash_residue_files: Option<u64>,
    pub crash_resume_outcome: Option<CrashOutcome>,
    pub resume_read_bytes_p50: Option<f64>,
    pub resume_read_bytes_p95: Option<f64>,
    pub resume_latency_p50_ms: Option<f64>,
    pub resume_latency_p95_ms: Option<f64>,
    pub resume_outcome: Option<ResumeOutcome>,
    pub footprint_curve: Vec<CurvePoint>,
    pub auxiliaries: Vec<Auxiliary>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CurvePoint {
    pub turn: u32,
    pub allocated_bytes: f64,
    pub mad_bytes: f64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Auxiliary {
    pub name: String,
    pub declared: bool,
    pub cap_bytes: Option<u64>,
    pub peak_allocated_bytes: u64,
    pub final_allocated_bytes: u64,
    pub slope_bytes_per_turn: f64,
    pub rotation_observed: bool,
    pub class: Option<BoundClass>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityReceipt {
    pub pid: u32,
    pub start_time: u64,
    pub source: String,
    pub first_bytes: Option<u64>,
    pub last_bytes: Option<u64>,
    pub retirement_method: String,
    pub complete: bool,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteDiagnostics {
    pub physical_write_bytes: Option<u64>,
    pub logical_growth_bytes: Option<u64>,
    pub net_growth_bytes: Option<i64>,
    pub amplification_reason: Option<String>,
    pub counter_complete: bool,
    pub identities: Vec<IdentityReceipt>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRef {
    pub file: String,
    pub sha256: String,
    pub first_record: Option<u64>,
    pub last_record: Option<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSample {
    pub row_id: String,
    pub repetition: u32,
    pub session_ordinal: u32,
    pub turn: u32,
    pub boundary: String,
    pub monotonic_ns: u64,
    pub settle_ms: f64,
    pub sync_start_ns: u64,
    pub sync_end_ns: u64,
    pub allocated_bytes: u64,
    pub apparent_bytes: u64,
    pub regular_files: u64,
    pub families: BTreeMap<String, u64>,
    pub physical_write_bytes: Option<u64>,
    pub physical_read_bytes: Option<u64>,
    pub counter_source: String,
    pub counter_complete: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageFile {
    pub row_id: String,
    pub repetition: u32,
    pub boundary: String,
    #[serde(flatten)]
    pub entry: super::accounting::FileEntry,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Trial<S, D> {
    pub repetition: u32,
    pub outcome: TestOutcome,
    pub measurement_complete: bool,
    pub reason: Option<String>,
    pub summary: S,
    pub diagnostics: D,
    pub evidence_refs: Vec<EvidenceRef>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowDetails<S, D> {
    pub measurement_label: String,
    pub reason: Option<String>,
    pub trials: Vec<Trial<S, D>>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurabilityDiagnostics {
    pub failed_calls: Option<u64>,
    pub primitive_applicability: BTreeMap<Primitive, Applicability>,
    pub instrumentation_ref: Option<u64>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Applicability {
    Measured,
    NotApplicable,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionDiagnostics {
    pub before_boundary: Option<String>,
    pub after_boundary: Option<String>,
    pub compaction_request_sha256: Option<String>,
    pub recovery_outcome: Option<CompactionOutcome>,
    pub accepted_input_tokens: Option<u64>,
    pub accepted_body_bytes: Option<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloseDiagnostics {
    pub store_baseline_allocated_bytes: Option<u64>,
    pub checkpoints: Vec<CloseCheckpoint>,
    pub close_receipts: Vec<CloseReceipt>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloseCheckpoint {
    pub closed_sessions: u64,
    pub elapsed_s: f64,
    pub phase: ClosePhase,
    pub retained_allocated_bytes: u64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CloseReceipt {
    pub session_id_hash: String,
    pub exit_code: i32,
    pub receipt_sha256: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteDiagnostics {
    pub operation_refs: Vec<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryDiagnostics {
    pub checkpoints: Vec<FamilyCheckpoint>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyCheckpoint {
    pub turn: u32,
    pub family: String,
    pub allocated_bytes: u64,
    pub identity_replacements: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionDiagnostics {
    pub coverage: Coverage,
    pub body_blobs: Vec<BodyBlob>,
    pub baseline_exclusions: Vec<BaselineExclusion>,
    pub match_refs: Vec<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BodyBlob {
    pub semantic_ordinal: u64,
    pub attempt: u64,
    pub role: String,
    pub raw_sha256: String,
    pub canonical_sha256: String,
    pub path: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineExclusion {
    pub path: String,
    pub offset_bytes: u64,
    pub length_bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrashDiagnostics {
    pub hold_ns: Option<u64>,
    pub kill_ns: Option<u64>,
    pub exit_ns: Option<u64>,
    pub committed_cursor: Option<u64>,
    pub expected_cursor: Option<u64>,
    pub resumed_cursor: Option<u64>,
    pub lost_events: Option<u64>,
    pub duplicate_events: Option<u64>,
    pub duplicate_effects: Option<u64>,
    pub committed_prefix_sha256: Option<String>,
    pub resumed_session_id_hash: Option<String>,
    pub expected_session_id_hash: Option<String>,
    pub residue_paths: Vec<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeDiagnostics {
    pub per_invocation_topology: bool,
    pub transport_kind: crate::manifest::TransportKind,
    pub resume_control_declared: bool,
    pub resume_path: Option<ResumePath>,
    pub reattach_start_ns: Option<u64>,
    pub read_start_ns: Option<u64>,
    pub control_start_ns: Option<u64>,
    pub control_end_ns: Option<u64>,
    pub continuation_start_ns: Option<u64>,
    pub counter_start_ns: Option<u64>,
    pub resume_start_ns: Option<u64>,
    pub first_request_ns: Option<u64>,
    pub counter_end_ns: Option<u64>,
    pub start_skew_ns: Option<u64>,
    pub end_skew_ns: Option<u64>,
    pub first_read_bytes: Option<u64>,
    pub last_read_bytes: Option<u64>,
    pub cursor: Option<u64>,
    pub expected_cursor: Option<u64>,
    pub total_resume_latency_ms: Option<f64>,
    pub session_id_hash: Option<String>,
    pub expected_session_id_hash: Option<String>,
    pub identities: Vec<IdentityReceipt>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub repetition: Option<u32>,
    pub operation: OperationKind,
    pub declared: bool,
    pub scope: Vec<String>,
    pub baseline_boundary: Option<String>,
    pub after_boundary: Option<String>,
    pub exit_code: Option<i32>,
    pub receipt_sha256: Option<String>,
    pub outcome: TestOutcome,
    pub reason: Option<String>,
    pub residue_paths: Vec<String>,
    pub whole_root_residue_allocated_bytes: Option<u64>,
    pub whole_root_residue_files: Option<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BodyMatch {
    pub repetition: u32,
    pub representation: String,
    pub request_sha256: Option<String>,
    pub block_sha256: Option<String>,
    pub path: String,
    pub device_id: u64,
    pub inode_or_file_id: u64,
    pub offset_bytes: u64,
    pub length_bytes: u64,
    pub excluded_baseline: bool,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Instrumentation {
    pub repetition: u32,
    pub backend: String,
    pub version: String,
    pub executable_sha256: String,
    pub shim_sha256: Option<String>,
    pub environment_keys: Vec<String>,
    pub images: Vec<ImageReceipt>,
    pub drop_count: u64,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImageReceipt {
    pub pid: u32,
    pub start_time: u64,
    pub executable_sha256: String,
    pub load_verified: bool,
    pub self_test_verified: bool,
    pub exit_seen: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FsyncEvent {
    pub repetition: u32,
    pub turn: u32,
    pub pid: u32,
    pub start_time: u64,
    pub sequence: u64,
    pub primitive: Primitive,
    pub enter_ns: u64,
    pub exit_ns: u64,
    pub return_code: i64,
    pub errno: i64,
    pub backend: String,
    pub self_test: bool,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurabilityDetails {
    pub measurement_label: String,
    pub reason: Option<String>,
    pub trials: Vec<Trial<DurabilitySummary, DurabilityDiagnostics>>,
    pub instrumentation: Vec<Instrumentation>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteDetails {
    pub measurement_label: String,
    pub reason: Option<String>,
    pub trials: Vec<Trial<DeleteSummary, DeleteDiagnostics>>,
    pub operations: Vec<Operation>,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionDetails {
    pub measurement_label: String,
    pub reason: Option<String>,
    pub trials: Vec<Trial<RetentionSummary, RetentionDiagnostics>>,
    pub matches: Vec<BodyMatch>,
}
pub fn validate_details(
    name: &str,
    value: &serde_json::Value,
) -> std::result::Result<(), serde_json::Error> {
    match name {
        "write-volume" => {
            serde_json::from_value::<RowDetails<WriteSummary, WriteDiagnostics>>(value.clone())
                .map(|_| ())
        }
        "durability-cost" => serde_json::from_value::<DurabilityDetails>(value.clone()).map(|_| ()),
        "footprint-curve" => {
            serde_json::from_value::<RowDetails<FootprintSummary, CurveEvaluation>>(value.clone())
                .map(|_| ())
        }
        "compaction-vs-disk" => serde_json::from_value::<
            RowDetails<CompactionSummary, CompactionDiagnostics>,
        >(value.clone())
        .map(|_| ()),
        "close-retention" => {
            serde_json::from_value::<RowDetails<CloseSummary, CloseDiagnostics>>(value.clone())
                .map(|_| ())
        }
        "delete-uninstall-residue" => {
            serde_json::from_value::<DeleteDetails>(value.clone()).map(|_| ())
        }
        "bounded-auxiliaries" => serde_json::from_value::<
            RowDetails<AuxiliarySummary, AuxiliaryDiagnostics>,
        >(value.clone())
        .map(|_| ()),
        "request-body-retention" => {
            serde_json::from_value::<RetentionDetails>(value.clone()).map(|_| ())
        }
        "crash-residue" => {
            serde_json::from_value::<RowDetails<CrashSummary, CrashDiagnostics>>(value.clone())
                .map(|_| ())
        }
        "resume-read-cost" => {
            serde_json::from_value::<RowDetails<ResumeSummary, ResumeDiagnostics>>(value.clone())
                .map(|_| ())
        }
        _ => Ok(()),
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBadge {
    pub spec_version: u32,
    pub os: String,
    pub topology: String,
    pub profile: String,
    pub comparison_scope: String,
    pub disk_class: String,
    pub growth_class: GrowthClass,
    pub facets: Vec<String>,
    pub label: String,
}
impl StorageSummary {
    pub fn numeric_metrics(&self) -> Result<BTreeMap<String, f64>> {
        let value = serde_json::to_value(self)?;
        let mut metrics = BTreeMap::new();
        for key in [
            "write_bytes_per_turn_p50",
            "write_bytes_per_turn_p95",
            "write_bytes_per_turn_max",
            "logical_growth_bytes_per_turn",
            "net_growth_bytes_per_turn",
            "write_amplification_ratio",
            "fsync_calls_per_turn",
            "fdatasync_calls_per_turn",
            "fullfsync_calls_per_turn",
            "durability_calls_per_turn",
            "assumed_fsync_cost_ms",
            "estimated_durability_wall_ms_per_turn",
            "first_turn_allocated_bytes",
            "footprint_slope_bytes_per_turn",
            "compaction_before_allocated_bytes",
            "compaction_after_allocated_bytes",
            "compaction_freed_pct",
            "closed_sessions",
            "close_retained_bytes_per_session",
            "close_retained_after_sweep_bytes_per_session",
            "retention_cap_bytes",
            "delete_residue_allocated_bytes",
            "delete_residue_files",
            "uninstall_residue_allocated_bytes",
            "uninstall_residue_files",
            "stored_request_bytes",
            "unique_request_content_bytes",
            "stored_unique_ratio",
            "crash_residue_allocated_bytes",
            "crash_residue_files",
            "resume_read_bytes_p50",
            "resume_read_bytes_p95",
            "resume_latency_p50_ms",
            "resume_latency_p95_ms",
        ] {
            if let Some(n) = value
                .get(key)
                .and_then(serde_json::Value::as_f64)
                .filter(|n| n.is_finite())
            {
                metrics.insert(format!("storage.{key}"), n);
            }
        }
        Ok(metrics)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum Primitive {
    Fsync,
    Fdatasync,
    Fullfsync,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CompactionOutcome {
    Compacted,
    NotCompacted,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ClosePhase {
    Immediate,
    PostSweep,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Coverage {
    Complete,
    RepresentationLimited,
    Partial,
    CaptureError,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    SessionDelete,
    UninstallCleanup,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ResumePath {
    ExecContinuation,
    ExecControlContinuation,
    DaemonExecContinuation,
    DaemonExecControlContinuation,
    DaemonReattach,
}
