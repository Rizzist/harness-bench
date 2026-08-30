//! Typed, row-specific matrix evidence and strict pass evaluation.
//!
//! This module deliberately separates observations from classification. A terminal
//! event alone is never sufficient for a row whose method requires correlation,
//! timing, isolation, recovery, or resource measurements.

use crate::evaluate::{Assertion, TestOutcome, TestResult, classify};
use crate::manifest::{Manifest, TransportKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A typed scalar or ordered collection recorded by a scenario.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "kebab-case")]
pub enum EvidenceValue {
    /// Boolean observation.
    Bool(bool),
    /// Unsigned count, duration, byte size, or cursor.
    U64(u64),
    /// Floating-point ratio, slope, or percentage.
    F64(f64),
    /// Stable text label.
    Text(String),
    /// Deterministically ordered text values.
    TextList(Vec<String>),
    /// Deterministically ordered integer values.
    U64List(Vec<u64>),
}

/// Deterministically keyed observations for one typed row variant.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ObservationSet {
    /// Semantic observation name to typed value.
    pub values: BTreeMap<String, EvidenceValue>,
}

impl ObservationSet {
    /// Construct an empty set. Empty or incomplete evidence never passes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a boolean observation.
    pub fn with_bool(mut self, name: &str, value: bool) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::Bool(value));
        self
    }

    /// Record an unsigned observation.
    pub fn with_u64(mut self, name: &str, value: u64) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::U64(value));
        self
    }

    /// Record a floating-point observation.
    pub fn with_f64(mut self, name: &str, value: f64) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::F64(value));
        self
    }

    /// Record text.
    pub fn with_text(mut self, name: &str, value: impl Into<String>) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::Text(value.into()));
        self
    }

    /// Record an ordered text collection.
    pub fn with_text_list(mut self, name: &str, value: Vec<String>) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::TextList(value));
        self
    }

    /// Record an ordered integer collection.
    pub fn with_u64_list(mut self, name: &str, value: Vec<u64>) -> Self {
        self.values
            .insert(name.to_owned(), EvidenceValue::U64List(value));
        self
    }
}

/// One nominal evidence payload for each authoritative matrix row.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "row", content = "observations", rename_all = "kebab-case")]
pub enum RowEvidence {
    /// Row 1: exact routing.
    Routing(ObservationSet),
    /// Row 2: one tool call.
    SingleToolCall(ObservationSet),
    /// Row 3: sequential tool calls.
    SequentialToolCalls(ObservationSet),
    /// Row 4: parallel tool calls.
    ParallelToolCalls(ObservationSet),
    /// Row 5: fragmented streamed arguments.
    FragmentedToolArgs(ObservationSet),
    /// Row 6: failed tool result.
    FailedToolResult(ObservationSet),
    /// Row 7: malformed or unknown call.
    MalformedToolCall(ObservationSet),
    /// Row 8: call-ID deduplication.
    ToolIdDedup(ObservationSet),
    /// Row 9: structural success.
    TerminalSuccess(ObservationSet),
    /// Row 10: structural failure.
    TerminalFailure(ObservationSet),
    /// Row 11: upstream retry.
    UpstreamRetry(ObservationSet),
    /// Row 12: owned idle deadline.
    IdleDeadline(ObservationSet),
    /// Row 13: workspace and patch effects.
    WorkspaceEffects(ObservationSet),
    /// Row 14: state and network confinement.
    StateNetworkConfinement(ObservationSet),
    /// Row 15: headless workflow.
    HeadlessWorkflow(ObservationSet),
    /// Row 16: transcript determinism.
    TranscriptDeterminism(ObservationSet),
    /// Row 17: actor isolation.
    ActorIsolation(ObservationSet),
    /// Row 18: native delegation.
    NativeDelegation(ObservationSet),
    /// Row 19: deterministic exit codes.
    ExitCodes(ObservationSet),
    /// Row 20: idle RSS.
    IdleRss(ObservationSet),
    /// Row 21: idle CPU.
    IdleCpu(ObservationSet),
    /// Row 22: idle memory drift.
    IdleDrift(ObservationSet),
    /// Row 23: return to idle.
    ReturnToIdle(ObservationSet),
    /// Row 24: cold start.
    ColdStart(ObservationSet),
    /// Row 25: single-agent resources.
    SingleAgentResource(ObservationSet),
    /// Row 26: parallel memory.
    ParallelMemory(ObservationSet),
    /// Row 27: scaling curve.
    ScalingCurve(ObservationSet),
    /// Row 28: post-close reclaim.
    PostCloseReclaim(ObservationSet),
    /// Row 29: long-horizon stability.
    LongHorizon(ObservationSet),
    /// Row 30: session replay.
    SessionReplay(ObservationSet),
    /// Row 31: steer.
    Steer(ObservationSet),
    /// Row 32: pre-tool subturn.
    Subturn(ObservationSet),
    /// Row 33: queued turn.
    QueuedTurn(ObservationSet),
    /// Row 34: noninteractive execution.
    Noninteractive(ObservationSet),
    /// Row 35: crash recovery.
    CrashRecovery(ObservationSet),
    /// Row 36: cancellation and cleanup.
    CancelCleanup(ObservationSet),
    /// Row 37: resume idempotency.
    ResumeIdempotency(ObservationSet),
    /// Row 38: resource bounds.
    ResourceBounds(ObservationSet),
    /// Row 39: hooks.
    Hooks(ObservationSet),
    /// Row 40: durable journal.
    DurableJournal(ObservationSet),
    /// Row 41: profile and network isolation.
    ProfileNetworkIsolation(ObservationSet),
}

impl RowEvidence {
    /// Authoritative row represented by this nominal payload.
    pub fn row(&self) -> u8 {
        match self {
            Self::Routing(_) => 1,
            Self::SingleToolCall(_) => 2,
            Self::SequentialToolCalls(_) => 3,
            Self::ParallelToolCalls(_) => 4,
            Self::FragmentedToolArgs(_) => 5,
            Self::FailedToolResult(_) => 6,
            Self::MalformedToolCall(_) => 7,
            Self::ToolIdDedup(_) => 8,
            Self::TerminalSuccess(_) => 9,
            Self::TerminalFailure(_) => 10,
            Self::UpstreamRetry(_) => 11,
            Self::IdleDeadline(_) => 12,
            Self::WorkspaceEffects(_) => 13,
            Self::StateNetworkConfinement(_) => 14,
            Self::HeadlessWorkflow(_) => 15,
            Self::TranscriptDeterminism(_) => 16,
            Self::ActorIsolation(_) => 17,
            Self::NativeDelegation(_) => 18,
            Self::ExitCodes(_) => 19,
            Self::IdleRss(_) => 20,
            Self::IdleCpu(_) => 21,
            Self::IdleDrift(_) => 22,
            Self::ReturnToIdle(_) => 23,
            Self::ColdStart(_) => 24,
            Self::SingleAgentResource(_) => 25,
            Self::ParallelMemory(_) => 26,
            Self::ScalingCurve(_) => 27,
            Self::PostCloseReclaim(_) => 28,
            Self::LongHorizon(_) => 29,
            Self::SessionReplay(_) => 30,
            Self::Steer(_) => 31,
            Self::Subturn(_) => 32,
            Self::QueuedTurn(_) => 33,
            Self::Noninteractive(_) => 34,
            Self::CrashRecovery(_) => 35,
            Self::CancelCleanup(_) => 36,
            Self::ResumeIdempotency(_) => 37,
            Self::ResourceBounds(_) => 38,
            Self::Hooks(_) => 39,
            Self::DurableJournal(_) => 40,
            Self::ProfileNetworkIsolation(_) => 41,
        }
    }

    fn observations(&self) -> &ObservationSet {
        match self {
            Self::Routing(value)
            | Self::SingleToolCall(value)
            | Self::SequentialToolCalls(value)
            | Self::ParallelToolCalls(value)
            | Self::FragmentedToolArgs(value)
            | Self::FailedToolResult(value)
            | Self::MalformedToolCall(value)
            | Self::ToolIdDedup(value)
            | Self::TerminalSuccess(value)
            | Self::TerminalFailure(value)
            | Self::UpstreamRetry(value)
            | Self::IdleDeadline(value)
            | Self::WorkspaceEffects(value)
            | Self::StateNetworkConfinement(value)
            | Self::HeadlessWorkflow(value)
            | Self::TranscriptDeterminism(value)
            | Self::ActorIsolation(value)
            | Self::NativeDelegation(value)
            | Self::ExitCodes(value)
            | Self::IdleRss(value)
            | Self::IdleCpu(value)
            | Self::IdleDrift(value)
            | Self::ReturnToIdle(value)
            | Self::ColdStart(value)
            | Self::SingleAgentResource(value)
            | Self::ParallelMemory(value)
            | Self::ScalingCurve(value)
            | Self::PostCloseReclaim(value)
            | Self::LongHorizon(value)
            | Self::SessionReplay(value)
            | Self::Steer(value)
            | Self::Subturn(value)
            | Self::QueuedTurn(value)
            | Self::Noninteractive(value)
            | Self::CrashRecovery(value)
            | Self::CancelCleanup(value)
            | Self::ResumeIdempotency(value)
            | Self::ResourceBounds(value)
            | Self::Hooks(value)
            | Self::DurableJournal(value)
            | Self::ProfileNetworkIsolation(value) => value,
        }
    }
}

/// Capability classification before behavioral evidence is evaluated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityStatus {
    /// Manifest declares the capability and its required operation surface exists.
    Supported,
    /// Optional facet is honestly not declared.
    Unsupported(String),
    /// Non-capability evidence declaration is absent. Missing operations are
    /// classified as `Unsupported`, never as benchmark failure/absence.
    Absent(String),
}

impl CapabilityStatus {
    /// Convert to the capability input consumed by [`classify`].
    ///
    /// Supported is `Some(true)`, explicitly unsupported is `Some(false)`, and
    /// only non-capability absence is `None`.
    pub fn as_classify_value(&self) -> Option<bool> {
        match self {
            Self::Supported => Some(true),
            Self::Unsupported(_) => Some(false),
            Self::Absent(_) => None,
        }
    }
}

/// Resolve a row's capability from declarations and concrete operation surfaces.
pub fn capability_for_row(manifest: &Manifest, row: u8) -> CapabilityStatus {
    if !basic_session_surface(manifest) {
        return CapabilityStatus::Unsupported(
            "basic session create/submit/attach surface is absent for this architecture".to_owned(),
        );
    }
    if (20..=29).contains(&row)
        && manifest.transport.kind != TransportKind::Exec
        && manifest.sessions.close_delete.is_empty()
    {
        return CapabilityStatus::Unsupported(
            "resource trial session close/delete surface is absent".to_owned(),
        );
    }
    let requirement = crate::scenarios::all()
        .iter()
        .find(|definition| definition.row == row)
        .map(|definition| definition.requirement());
    let capability_key = match requirement {
        Some(crate::scenarios::RequirementKind::OptionalFacet { capability }) => Some(capability),
        _ => match row {
            15 | 34 => Some("headless"),
            30 => Some("sessions"),
            35 | 37 => Some("resume"),
            40 => Some("durable_journal"),
            _ => None,
        },
    };
    if let Some(key) = capability_key {
        let declared = manifest.capabilities.required.contains_key(key)
            || manifest.capabilities.optional.contains_key(key);
        if !declared {
            return CapabilityStatus::Unsupported(format!(
                "capability {key} is not declared by this architecture"
            ));
        }
        if !operation_surface_present(manifest, row) {
            return CapabilityStatus::Unsupported(format!(
                "capability {key} is declared but its operation surface is absent"
            ));
        }
    }
    if row == 17 && manifest.concurrency.max_agents < 2 {
        return CapabilityStatus::Unsupported("actor concurrency is below N=2".to_owned());
    }
    if row == 26 && manifest.concurrency.max_agents < 8 {
        return CapabilityStatus::Unsupported("parallel resource surface is below N=8".to_owned());
    }
    if row == 36 && manifest.agents.cancel.is_empty() {
        return CapabilityStatus::Unsupported("agent cancellation operation is absent".to_owned());
    }
    CapabilityStatus::Supported
}

fn operation_surface_present(manifest: &Manifest, row: u8) -> bool {
    match row {
        4 => manifest.concurrency.max_agents >= 2,
        15 | 34 => {
            !manifest.transport.command.is_empty() || !manifest.transport.endpoint.is_empty()
        }
        18 => {
            !manifest.agents.spawn.is_empty()
                && !manifest.agents.status.is_empty()
                && !manifest.agents.collect.is_empty()
        }
        30 => {
            if manifest.transport.kind == TransportKind::Exec {
                !manifest.sessions.resume.is_empty()
                    && manifest.events.source == "journal-file"
                    && !manifest.events.path.is_empty()
            } else {
                !manifest.sessions.create.is_empty()
                    && !manifest.sessions.submit.is_empty()
                    && !manifest.sessions.attach.is_empty()
            }
        }
        31 => !manifest.next_input.steer.is_empty(),
        32 => !manifest.next_input.subturn.is_empty(),
        33 => !manifest.next_input.queue.is_empty(),
        35 => {
            !manifest.sessions.resume.is_empty()
                && !manifest.concurrency.release.is_empty()
                && if manifest.transport.kind == TransportKind::Exec {
                    manifest.events.source == "journal-file" && !manifest.events.path.is_empty()
                } else {
                    manifest.daemon.persistent && !manifest.sessions.attach.is_empty()
                }
        }
        37 => {
            !manifest.sessions.resume.is_empty()
                && (manifest.transport.kind == TransportKind::Exec
                    || !manifest.sessions.attach.is_empty())
        }
        39 => !manifest.hooks.acceptance.is_empty() && !manifest.hooks.completion.is_empty(),
        40 => {
            matches!(manifest.events.source.as_str(), "journal" | "journal-file")
                && manifest.events.framing == "jsonl"
                && !manifest.events.path.is_empty()
                && !manifest.events.cursor_pointer.is_empty()
                && (manifest.transport.kind != TransportKind::Exec
                    || !manifest.events.replay_command.is_empty())
        }
        _ => true,
    }
}

/// Whether the architecture exposes the minimum session lifecycle needed to
/// execute ordinary matrix turns. EXEC manifests provide these semantics via
/// the per-invocation driver rather than RPC method names.
pub fn basic_session_surface(manifest: &Manifest) -> bool {
    manifest.transport.kind == TransportKind::Exec
        || (!manifest.sessions.create.is_empty()
            && !manifest.sessions.submit.is_empty()
            && !manifest.sessions.attach.is_empty())
}

/// Evaluate one row from a nominal payload and manifest capability state.
pub fn evaluate_row(manifest: &Manifest, row: u8, evidence: Option<&RowEvidence>) -> TestResult {
    let definition = crate::scenarios::all().iter().find(|item| item.row == row);
    let Some(definition) = definition else {
        return TestResult {
            row,
            id: format!("unknown-row-{row}"),
            pillar: crate::evaluate::Pillar::Functionality,
            outcome: TestOutcome::Error(format!("matrix row {row} is outside 1..=41")),
            evidence: Vec::new(),
        };
    };
    let capability = capability_for_row(manifest, row);
    let capability_value = capability.as_classify_value();
    let capability_detail = match &capability {
        CapabilityStatus::Supported => None,
        CapabilityStatus::Unsupported(reason) | CapabilityStatus::Absent(reason) => {
            Some(reason.clone())
        }
    };
    if !matches!(capability, CapabilityStatus::Supported) {
        let mut result = classify(
            row,
            definition.id,
            definition.pillar,
            capability_value,
            &[],
            None,
        );
        if let Some(detail) = capability_detail {
            result.evidence.push(format!("capability: {detail}"));
        }
        return result;
    }
    let Some(evidence) = evidence else {
        return classify(
            row,
            definition.id,
            definition.pillar,
            Some(true),
            &[],
            Some(format!("row {row} produced no typed evidence")),
        );
    };
    if evidence.row() != row {
        return classify(
            row,
            definition.id,
            definition.pillar,
            Some(true),
            &[],
            Some(format!(
                "row {row} received row {} evidence",
                evidence.row()
            )),
        );
    }
    let assertion = exact_assertion(row, evidence.observations());
    classify(
        row,
        definition.id,
        definition.pillar,
        Some(true),
        &[assertion],
        None,
    )
}

/// Compatibility entry point for topology-relative report status evaluation.
pub fn suite_exit_code(
    results: &[TestResult],
    badge: Option<&crate::evaluate::Badge>,
    manifest: &Manifest,
) -> i32 {
    crate::evaluate::suite_exit_code(results, badge, manifest)
}

fn exact_assertion(row: u8, values: &ObservationSet) -> Assertion {
    let checks: Vec<Check<'_>> = match row {
        1 => vec![
            bt("all_roles_observed"),
            bt("exact_tuple"),
            bf("unexpected_egress"),
            bt("credential_redacted"),
        ],
        2 => vec![
            ue("calls", 1),
            ue("effects", 1),
            bt("args_byte_match"),
            bt("result_correlated"),
            bt("structural_terminal"),
        ],
        3 => vec![
            ue("calls", 2),
            ue("effects", 2),
            bt("a_before_b"),
            bt("b_contains_a_output"),
            bt("correlations_correct"),
        ],
        4 => vec![
            ue("calls", 2),
            umin("max_live_calls", 2),
            bt("reverse_completion"),
            bt("each_once"),
            bt("correlations_correct"),
            bt("frame_order_preserved"),
        ],
        5 => vec![
            umin("fragments", 2),
            bt("arguments_exact"),
            ue("invocations", 1),
            bt("invoked_after_complete"),
            bf("partial_effect"),
        ],
        6 => vec![
            ue("failed_results", 1),
            bt("structured"),
            bt("correlated"),
            bt("reached_next_request"),
            bf("crashed"),
        ],
        7 => vec![
            bt("structured_failure"),
            ue("unintended_effects", 0),
            bt("terminal"),
            bf("crashed"),
            bf("hung"),
        ],
        8 => vec![
            umin("transport_frames", 2),
            ue("semantic_calls", 1),
            ue("effects", 1),
            bt("ids_preserved"),
            bt("order_preserved"),
        ],
        9 => vec![
            ue("success_terminals", 1),
            ue("failure_terminals", 0),
            bt("machine_parseable"),
            bt("exit_matches_contract"),
            bf("later_contradiction"),
        ],
        10 => vec![
            ue("failure_terminals", 1),
            ue("success_terminals", 0),
            bt("machine_parseable"),
            bt("nonzero_exit"),
            bt("exit_matches_contract"),
        ],
        11 => vec![
            bt("tested_429"),
            bt("tested_500"),
            bt("tested_disconnect"),
            bt("bounded_policy"),
            umax("effects", 1),
            bt("structured_terminal"),
            bf("double_commit"),
        ],
        12 => vec![
            bt("own_terminal"),
            bt("structured_failure"),
            bt("before_outer_deadline"),
            bt("within_idle_grace"),
        ],
        13 => vec![
            bt("create_hash_match"),
            bt("patch_hash_match"),
            bt("stdout_truncated"),
            bt("outcome_from_effects"),
        ],
        14 => vec![
            ue("outside_writes", 0),
            ue("external_connections", 0),
            ue("surviving_state", 0),
            bt("only_fake_reachable"),
        ],
        15 => vec![
            ue("reads", 1),
            ue("marker_writes", 1),
            ue("effects", 1),
            bt("headless"),
            ue("exit_code", 0),
        ],
        16 => vec![
            ue("repetitions", 5),
            bt("semantic_hashes_identical"),
            bt("event_counts_identical"),
            ue("turns_per_repetition", 3),
        ],
        17 => vec![
            umin("actors", 2),
            bt("isolated_namespaces"),
            bt("effect_ownership_exact"),
            ue("cross_talk", 0),
        ],
        18 => vec![
            umin("spawns", 1),
            bt("native_surface_used"),
            bt("one_durable_child_per_spawn"),
            bt("one_report_per_child"),
        ],
        19 => vec![
            ue("categories", 6),
            ue("repetitions", 5),
            bt("codes_invariant"),
            bt("structured_non_success"),
            bt("success_zero"),
        ],
        20 => vec![
            umin("samples", 3),
            bt("topology_classified"),
            fmax("relative_spread", 0.05),
            bt("all_owned_present"),
        ],
        21 => vec![
            umin("quiet_window_ms", 10_000),
            fmax("one_core_fraction", 0.01),
            bf("polling_signature"),
        ],
        22 => vec![
            umin("quiet_window_ms", 30_000),
            fmax("slope_mib_per_min", 1.0),
            fmax("net_growth_mib", 8.0),
            ue("worker_growth", 0),
        ],
        23 => vec![
            bt("ordinary_stable_within_10s"),
            bt("n8_stable_within_10s"),
            bt("residual_within_bound"),
        ],
        24 => vec![
            bt("readiness_within_bound"),
            bt("cold_peak_within_profile"),
            bt("idle_plateau_trustworthy"),
        ],
        25 => vec![
            bt("baseline_measured"),
            bt("s1_measured"),
            fmax("cpu_ms_per_turn", 250.0),
            fmax("barrier_cpu_fraction", 0.05),
        ],
        26 => vec![
            bt("sweep_n_1_2_4_8"),
            bt("all_n_present"),
            bt("n8_completed"),
            fmax("cold_peak_gib", 4.0),
            fmax("beta_mib_per_agent", 256.0),
        ],
        27 => vec![
            fmax("alpha", 1.20),
            bt("adjacent_marginals_bounded"),
            bf("unreported_instability"),
        ],
        28 => vec![
            fmin("reclaim_ratio", 0.80),
            bt("residual_within_bound"),
            ue("owned_workers_remaining", 0),
        ],
        29 => vec![
            ue("turns", 1_000),
            fmax("drift_kib_per_turn", 64.0),
            bt("final_residual_within_bound"),
            ue("fd_growth", 0),
            ue("thread_growth", 0),
        ],
        30 => vec![
            bt("same_session"),
            bt("suffix_exact"),
            bt("ordered"),
            ue("duplicates", 0),
            ue("gaps", 0),
            bt("continued_b"),
        ],
        31 => vec![
            ue("input_accepts", 1),
            bt("at_safe_boundary"),
            bt("affected_active_run"),
        ],
        32 => vec![
            ue("input_accepts", 1),
            bt("observed_before_effect"),
            ue("effects_before_input", 0),
        ],
        33 => vec![
            bt("a_terminal_before_b"),
            ue("b_runs", 1),
            bt("distinct_turn"),
        ],
        34 => vec![
            bt("stdin_closed"),
            bf("pty_used"),
            bt("allowed_succeeded"),
            bt("denied_failed_closed"),
            bf("prompt_wait"),
        ],
        35 => vec![
            bt("sigkill_used"),
            bt("named_checkpoint"),
            umax("readiness_ms", 10_000),
            bt("finite_terminal"),
            umax("committed_effects", 1),
        ],
        36 => vec![
            umax("terminal_ms", 5_000),
            ue("owned_pids_after_grace", 0),
            ue("undeclared_artifacts", 0),
            bt("reclaim_met"),
        ],
        37 => vec![
            umin("transport_attempts", 2),
            ue("semantic_turns", 1),
            ue("effects", 1),
            bt("stable_identity_dedup"),
        ],
        38 => vec![
            bt("agent_limit_honored"),
            bt("turn_limit_honored"),
            bt("output_limit_honored"),
            bt("deadline_honored"),
            bt("excess_typed_or_queued"),
        ],
        39 => vec![
            ue("acceptance_hooks", 1),
            ue("completion_hooks", 1),
            ue("replay_refires", 0),
            ue("hook_children_remaining", 0),
            bf("secret_leak"),
        ],
        40 => vec![
            bt("sigkill_used"),
            bt("named_post_commit_checkpoint"),
            bt("suffix_exact"),
            ue("duplicates", 0),
            ue("gaps", 0),
            bt("tail_integrity"),
            bt("journal_replay_agree"),
        ],
        41 => vec![
            bt("unique_profile"),
            ue("external_connections", 0),
            ue("outside_writes", 0),
            bf("credential_leak"),
            bt("all_roles_fake_endpoint"),
        ],
        _ => Vec::new(),
    };
    let mut failures = Vec::new();
    for check in &checks {
        if let Err(detail) = check.evaluate(values) {
            failures.push(detail);
        }
    }
    Assertion {
        name: format!("row-{row}-exact-criteria"),
        passed: !checks.is_empty() && failures.is_empty(),
        detail: if failures.is_empty() {
            format!("all {} row-specific criteria observed", checks.len())
        } else {
            failures.join("; ")
        },
    }
}

enum Check<'a> {
    Bool(&'a str, bool),
    U64Equal(&'a str, u64),
    U64Min(&'a str, u64),
    U64Max(&'a str, u64),
    F64Min(&'a str, f64),
    F64Max(&'a str, f64),
}

impl Check<'_> {
    fn evaluate(&self, values: &ObservationSet) -> std::result::Result<(), String> {
        match self {
            Self::Bool(name, expected) => match values.values.get(*name) {
                Some(EvidenceValue::Bool(actual)) if actual == expected => Ok(()),
                Some(actual) => Err(format!("{name} expected {expected:?}, observed {actual:?}")),
                None => Err(format!("missing {name}")),
            },
            Self::U64Equal(name, expected) => numeric_u64(values, name)
                .and_then(|actual| predicate(actual == *expected, name, actual, *expected, "==")),
            Self::U64Min(name, expected) => numeric_u64(values, name)
                .and_then(|actual| predicate(actual >= *expected, name, actual, *expected, ">=")),
            Self::U64Max(name, expected) => numeric_u64(values, name)
                .and_then(|actual| predicate(actual <= *expected, name, actual, *expected, "<=")),
            Self::F64Min(name, expected) => numeric_f64(values, name).and_then(|actual| {
                float_predicate(actual >= *expected, name, actual, *expected, ">=")
            }),
            Self::F64Max(name, expected) => numeric_f64(values, name).and_then(|actual| {
                float_predicate(actual <= *expected, name, actual, *expected, "<=")
            }),
        }
    }
}

fn numeric_u64(values: &ObservationSet, name: &str) -> std::result::Result<u64, String> {
    match values.values.get(name) {
        Some(EvidenceValue::U64(value)) => Ok(*value),
        Some(value) => Err(format!("{name} is not u64: {value:?}")),
        None => Err(format!("missing {name}")),
    }
}

fn numeric_f64(values: &ObservationSet, name: &str) -> std::result::Result<f64, String> {
    match values.values.get(name) {
        Some(EvidenceValue::F64(value)) if value.is_finite() => Ok(*value),
        Some(value) => Err(format!("{name} is not finite f64: {value:?}")),
        None => Err(format!("missing {name}")),
    }
}

fn predicate(
    actual: bool,
    name: &str,
    value: u64,
    expected: u64,
    operator: &str,
) -> std::result::Result<(), String> {
    if actual {
        Ok(())
    } else {
        Err(format!("{name}: {value} not {operator} {expected}"))
    }
}

fn float_predicate(
    actual: bool,
    name: &str,
    value: f64,
    expected: f64,
    operator: &str,
) -> std::result::Result<(), String> {
    if actual {
        Ok(())
    } else {
        Err(format!("{name}: {value:.6} not {operator} {expected:.6}"))
    }
}

const fn bt(name: &str) -> Check<'_> {
    Check::Bool(name, true)
}
const fn bf(name: &str) -> Check<'_> {
    Check::Bool(name, false)
}
const fn ue(name: &str, value: u64) -> Check<'_> {
    Check::U64Equal(name, value)
}
const fn umin(name: &str, value: u64) -> Check<'_> {
    Check::U64Min(name, value)
}
const fn umax(name: &str, value: u64) -> Check<'_> {
    Check::U64Max(name, value)
}
const fn fmin(name: &str, value: f64) -> Check<'_> {
    Check::F64Min(name, value)
}
const fn fmax(name: &str, value: f64) -> Check<'_> {
    Check::F64Max(name, value)
}
