//! Typed, row-specific matrix evidence and strict pass evaluation.
//!
//! This module deliberately separates observations from classification. A terminal
//! event alone is never sufficient for a row whose method requires correlation,
//! timing, isolation, recovery, or resource measurements.

use crate::evaluate::{Assertion, TestOutcome, TestResult, classify};
use crate::manifest::{InjectionMethod, Manifest, TransportKind};
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
    /// Row 65: provider/base-URL/credential injection ergonomics.
    InjectionSurface(ObservationSet),
    /// Row 66: harness-enforced token, cost, and time budgets.
    BudgetEnforcement(ObservationSet),
    /// Row 67: structured token, cost, and turn usage.
    UsageReporting(ObservationSet),
    /// Row 68: public CLI session lifecycle operations.
    SessionOpsCli(ObservationSet),
    /// Row 69: normalized event-stream completeness.
    EventStreamCompleteness(ObservationSet),
    /// Row 70: headless permission granularity and effects.
    HeadlessPermissionModel(ObservationSet),
    /// Row 71: credential hygiene across captured and on-disk artifacts.
    SecretsHygieneOnDisk(ObservationSet),
    /// Row 72: protocol-native tool-result role fidelity.
    ToolResultRoleFidelity(ObservationSet),
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
            Self::InjectionSurface(_) => 65,
            Self::BudgetEnforcement(_) => 66,
            Self::UsageReporting(_) => 67,
            Self::SessionOpsCli(_) => 68,
            Self::EventStreamCompleteness(_) => 69,
            Self::HeadlessPermissionModel(_) => 70,
            Self::SecretsHygieneOnDisk(_) => 71,
            Self::ToolResultRoleFidelity(_) => 72,
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
            | Self::ProfileNetworkIsolation(value)
            | Self::InjectionSurface(value)
            | Self::BudgetEnforcement(value)
            | Self::UsageReporting(value)
            | Self::SessionOpsCli(value)
            | Self::EventStreamCompleteness(value)
            | Self::HeadlessPermissionModel(value)
            | Self::SecretsHygieneOnDisk(value)
            | Self::ToolResultRoleFidelity(value) => value,
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
    if let Some(status) = wave4_capability_for_row(manifest, row) {
        return status;
    }
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
            35 | 37 | 52 => Some("resume"),
            40 | 53 => Some("durable_journal"),
            51 => Some("context_limit_recovery"),
            _ => None,
        },
    };
    if let Some(key) = capability_key {
        let declared = if row == 51 {
            manifest.capabilities.required.contains_key(key)
        } else {
            manifest.capabilities.required.contains_key(key)
                || manifest.capabilities.optional.contains_key(key)
        };
        if !declared {
            return if matches!(row, 51..=53) {
                CapabilityStatus::Absent(format!(
                    "required capability {key} is not declared by this architecture"
                ))
            } else {
                CapabilityStatus::Unsupported(format!(
                    "capability {key} is not declared by this architecture"
                ))
            };
        }
        if !operation_surface_present(manifest, row) {
            return CapabilityStatus::Unsupported(format!(
                "capability {key} is declared but its operation surface is absent"
            ));
        }
    }
    if row == 17 && manifest.concurrency.max_agents.unwrap_or(0) < 2 {
        return CapabilityStatus::Unsupported("actor concurrency is below N=2".to_owned());
    }
    if row == 26 && manifest.concurrency.max_agents.unwrap_or(0) < 8 {
        return CapabilityStatus::Unsupported("parallel resource surface is below N=8".to_owned());
    }
    if row == 50
        && (manifest.sessions.close_delete.is_empty() || manifest.sessions.store_paths.is_empty())
    {
        return CapabilityStatus::Absent(
            "sessions.close_delete or profile-contained sessions.store_paths is absent".to_owned(),
        );
    }
    if matches!(row, 54 | 55) {
        let Some(max_agents) = manifest.concurrency.max_agents else {
            return CapabilityStatus::Absent(
                "concurrency.max_agents width declaration is absent".to_owned(),
            );
        };
        if max_agents < 8 {
            return CapabilityStatus::Unsupported(
                "fanout architecture explicitly declares concurrency below quick N=8".to_owned(),
            );
        }
    }
    if row == 36 && manifest.agents.cancel.is_empty() {
        return CapabilityStatus::Unsupported("agent cancellation operation is absent".to_owned());
    }
    if row == 57 && manifest.input.prompt_uses_stdin.is_none() {
        return CapabilityStatus::Absent(
            "typed input.prompt_uses_stdin declaration is absent".to_owned(),
        );
    }
    if row == 58
        && (manifest.resources.retry_max_attempts.is_none()
            || manifest.resources.retry_base_delay_ms.is_none()
            || manifest.resources.retry_max_delay_ms.is_none())
    {
        return CapabilityStatus::Absent(
            "documented retry_max_attempts/base_delay/max_delay policy is absent".to_owned(),
        );
    }
    CapabilityStatus::Supported
}

fn wave4_capability_for_row(manifest: &Manifest, row: u8) -> Option<CapabilityStatus> {
    match row {
        65 => {
            let Some(surface) = manifest.capabilities.injection_surface.as_ref() else {
                return Some(CapabilityStatus::Unsupported(
                    "typed injection surface is not declared".to_owned(),
                ));
            };
            if matches!(surface.base_url.method, InjectionMethod::Impossible)
                || matches!(surface.credential.method, InjectionMethod::Impossible)
                || !basic_session_surface(manifest)
            {
                Some(CapabilityStatus::Unsupported(
                    "base route/auth cannot support a fake-provider trial".to_owned(),
                ))
            } else {
                Some(CapabilityStatus::Supported)
            }
        }
        66 => Some(optional_wave4_capability(
            manifest,
            "budget_enforcement",
            manifest.resources.budget_controls.is_some(),
        )),
        67 => Some(optional_wave4_capability(
            manifest,
            "usage_reporting",
            manifest.events.metadata.is_some(),
        )),
        68 => Some(optional_wave4_capability(
            manifest,
            "session_ops_cli",
            !manifest.sessions.create.is_empty()
                && !manifest.sessions.list.is_empty()
                && !manifest.sessions.resume.is_empty()
                && !manifest.sessions.fork.is_empty()
                && !manifest.sessions.delete.is_empty(),
        )),
        69 => Some(
            if manifest.events.source.trim().is_empty()
                || manifest.events.framing.trim().is_empty()
                || manifest.events.rules.is_empty()
            {
                CapabilityStatus::Absent("machine-readable event stream is absent".to_owned())
            } else {
                CapabilityStatus::Supported
            },
        ),
        70 => Some(optional_wave4_capability(
            manifest,
            "headless_permission_model",
            manifest.permissions.is_some(),
        )),
        71 => Some(match manifest.capture.credential_carrier_paths.as_ref() {
            Some(paths) if !paths.is_empty() => CapabilityStatus::Supported,
            _ => CapabilityStatus::Absent(
                "capture.credential_carrier_paths is omitted or empty".to_owned(),
            ),
        }),
        72 => Some(CapabilityStatus::Supported),
        _ => None,
    }
}

fn optional_wave4_capability(
    manifest: &Manifest,
    key: &str,
    operation_surface_present: bool,
) -> CapabilityStatus {
    let declared = manifest.capabilities.required.contains_key(key)
        || manifest.capabilities.optional.contains_key(key);
    if !declared {
        CapabilityStatus::Unsupported(format!(
            "capability {key} is not declared by this architecture"
        ))
    } else if !operation_surface_present || !basic_session_surface(manifest) {
        CapabilityStatus::Absent(format!(
            "declared capability {key} lacks its required operation surface"
        ))
    } else {
        CapabilityStatus::Supported
    }
}

fn operation_surface_present(manifest: &Manifest, row: u8) -> bool {
    match row {
        4 => manifest
            .concurrency
            .max_agents
            .is_some_and(|value| value >= 2),
        15 | 34 => {
            !manifest.transport.command.is_empty() || !manifest.transport.endpoint.is_empty()
        }
        18 => {
            !manifest.agents.spawn.is_empty()
                && !manifest.agents.status.is_empty()
                && !manifest.agents.collect.is_empty()
        }
        56 => {
            !manifest.agents.spawn.is_empty()
                && !manifest.agents.status.is_empty()
                && !manifest.agents.collect.is_empty()
                && !manifest.agents.child_id_pointer.is_empty()
                && !manifest.agents.status_result_pointer.is_empty()
                && !manifest.agents.collect_events_pointer.is_empty()
        }
        30 => {
            if manifest.transport.kind == TransportKind::Exec {
                !manifest.events.replay_command.is_empty()
                    || (!manifest.sessions.resume.is_empty()
                        && manifest.events.source == "journal-file"
                        && !manifest.events.path.is_empty())
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
            if manifest.transport.kind == TransportKind::Exec {
                manifest.daemon.persistent && !manifest.sessions.recover_probe.is_empty()
                    || (!manifest.sessions.resume.is_empty()
                        && !manifest.concurrency.release.is_empty()
                        && manifest.events.source == "journal-file"
                        && !manifest.events.path.is_empty())
            } else {
                !manifest.sessions.resume.is_empty()
                    && !manifest.concurrency.release.is_empty()
                    && manifest.daemon.persistent
                    && !manifest.sessions.attach.is_empty()
            }
        }
        37 | 52 => {
            (!manifest.sessions.resume.is_empty() || !manifest.sessions.resume_control.is_empty())
                && (manifest.transport.kind == TransportKind::Exec
                    || !manifest.sessions.attach.is_empty())
        }
        39 => !manifest.hooks.acceptance.is_empty() && !manifest.hooks.completion.is_empty(),
        40 | 53 => {
            manifest.events.framing == "jsonl"
                && !manifest.events.cursor_pointer.is_empty()
                && if manifest.transport.kind == TransportKind::Exec {
                    !manifest.events.replay_command.is_empty()
                } else {
                    manifest.events.source == "journal" && !manifest.events.path.is_empty()
                }
        }
        51 => manifest.resources.context_window.is_some(),
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
            metadata: crate::evaluate::TestResultMetadata::for_row(
                row,
                &TestOutcome::Error("unknown matrix row".to_owned()),
            ),
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
        if matches!(row, 65 | 66 | 67 | 68 | 70)
            && matches!(result.outcome, TestOutcome::Unsupported(_))
        {
            result.metadata.score = Some(0.0);
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
    let mut result = classify(
        row,
        definition.id,
        definition.pillar,
        Some(true),
        &[assertion],
        None,
    );
    if matches!(row, 65..=70) {
        result.metadata.score = numeric_f64(evidence.observations(), "score")
            .ok()
            .filter(|score| (0.0..=1.0).contains(score));
    }
    result
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
    if matches!(row, 65..=72) {
        return wave4_exact_assertion(row, values);
    }
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

fn wave4_exact_assertion(row: u8, values: &ObservationSet) -> Assertion {
    let evaluated = match row {
        65 => {
            let provider = numeric_f64(values, "provider_score");
            let base_url = numeric_f64(values, "base_url_score");
            let credential = numeric_f64(values, "credential_score");
            let score = numeric_f64(values, "score");
            let verified = numeric_u64(values, "verified_components");
            match (provider, base_url, credential, score, verified) {
                (Ok(provider), Ok(base_url), Ok(credential), Ok(score), Ok(3)) => Ok(provider
                    > 0.0
                    && base_url > 0.0
                    && credential > 0.0
                    && (0.50..=1.0).contains(&score)),
                (provider, base_url, credential, score, verified) => Err(format!(
                    "invalid injection evidence: provider={provider:?}, base_url={base_url:?}, credential={credential:?}, score={score:?}, verified_components={verified:?}"
                )),
            }
        }
        66 => wave4_budget_passes(values),
        67 => wave4_usage_passes(values),
        68 => wave4_session_cli_passes(values),
        69 => wave4_event_stream_passes(values),
        70 => wave4_permission_passes(values),
        71 => wave4_secret_hygiene_passes(values),
        72 => wave4_tool_role_passes(values),
        _ => Err(format!("row {row} has no Wave-4 evaluator")),
    };
    let (passed, detail) = match evaluated {
        Ok(true) => (true, "all row-specific criteria observed".to_owned()),
        Ok(false) => (
            false,
            "complete evidence is outside the row oracle".to_owned(),
        ),
        Err(detail) => (false, detail),
    };
    Assertion {
        name: format!("row-{row}-exact-criteria"),
        passed,
        detail,
    }
}

fn wave4_budget_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    let repetitions = numeric_u64(values, "repetitions")?;
    let score = numeric_f64(values, "score")?;
    Ok(repetitions > 0
        && score == 1.0
        && numeric_u64(values, "case_kinds_passed")? == 3
        && numeric_u64(values, "structured_failures")? == repetitions.saturating_mul(3)
        && numeric_u64(values, "overrun_count")? == 0
        && bool_value(values, "token_boundary_exact")?
        && bool_value(values, "cost_boundary_exact")?
        && bool_value(values, "time_boundary_exact")?
        && !bool_value(values, "outer_kill")?)
}

fn wave4_usage_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    let score = numeric_f64(values, "score")?;
    Ok(score == 1.0
        && numeric_u64(values, "correct_fields")? == 5
        && numeric_u64(values, "crosscheck_errors")? == 0
        && bool_value(values, "all_repetitions_exact")?
        && bool_value(values, "one_carrier_per_turn")?
        && bool_value(values, "tool_turn_two_response_sum")?
        && bool_value(values, "repetitions_identical")?)
}

fn wave4_session_cli_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    let score = numeric_f64(values, "score")?;
    Ok(score == 1.0
        && numeric_u64(values, "create_ok")? == 1
        && numeric_u64(values, "list_ok")? == 1
        && numeric_u64(values, "resume_ok")? == 1
        && numeric_u64(values, "fork_ok")? == 1
        && numeric_u64(values, "delete_ok")? == 1
        && numeric_u64(values, "lifecycles")? > 0
        && bool_value(values, "nonempty_committed_seed")?
        && bool_value(values, "fork_prefix_exact")?
        && bool_value(values, "divergence_isolated")?)
}

fn wave4_event_stream_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    let component = |name| numeric_u64(values, name).map(|value| value == 1);
    let components = [
        component("tool_call_id")?,
        component("correlated_result")?,
        component("timestamps")?,
        component("usage")?,
        component("terminal_typing")?,
        component("schema_version")?,
    ];
    let score = numeric_f64(values, "score")?;
    let expected_score = components.iter().filter(|value| **value).count() as f64 / 6.0;
    Ok((score - expected_score).abs() <= f64::EPSILON
        && crate::evaluate::event_stream_reference_envelope(components))
}

fn wave4_permission_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    Ok(crate::evaluate::headless_permission_model_passes(
        numeric_f64(values, "score")?,
        numeric_u64(values, "repetitions")?,
        numeric_u64(values, "tty_prompts")?,
        numeric_u64(values, "allowed_effects")?,
        numeric_u64(values, "denied_filesystem_effects")?,
        numeric_u64(values, "denied_network_effects")?,
        numeric_u64(values, "scope_violations")?,
    ))
}

fn wave4_secret_hygiene_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    Ok(numeric_u64(values, "files_scanned")? > 0
        && numeric_u64(values, "bytes_scanned")? > 0
        && numeric_u64(values, "declared_carrier_files")? > 0
        && numeric_u64(values, "stdout_matches")? == 0
        && numeric_u64(values, "stderr_matches")? == 0
        && numeric_u64(values, "journal_matches")? == 0
        && numeric_u64(values, "session_matches")? == 0
        && numeric_u64(values, "log_matches")? == 0
        && !bool_value(values, "credential_in_argv")?)
}

fn wave4_tool_role_passes(values: &ObservationSet) -> std::result::Result<bool, String> {
    Ok(crate::evaluate::tool_result_role_fidelity_passes(
        numeric_u64(values, "repetitions")?,
        numeric_u64(values, "checks")?,
        numeric_u64(values, "violations")?,
        numeric_u64(values, "plain_user_text_violations")?,
        numeric_u64(values, "missing_results")?,
        numeric_u64(values, "duplicate_results")?,
    ))
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

fn bool_value(values: &ObservationSet, name: &str) -> std::result::Result<bool, String> {
    match values.values.get(name) {
        Some(EvidenceValue::Bool(value)) => Ok(*value),
        Some(value) => Err(format!("{name} is not bool: {value:?}")),
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

#[cfg(test)]
mod wave4_tests {
    use super::*;

    #[test]
    fn event_stream_oracle_uses_integer_four_of_six_and_hard_trio() {
        let passing = ObservationSet::new()
            .with_u64("tool_call_id", 1)
            .with_u64("correlated_result", 1)
            .with_u64("timestamps", 0)
            .with_u64("usage", 0)
            .with_u64("terminal_typing", 1)
            .with_u64("schema_version", 1)
            .with_f64("score", 4.0 / 6.0);
        assert!(wave4_event_stream_passes(&passing).expect("complete evidence"));

        let missing_hard_component = ObservationSet::new()
            .with_u64("tool_call_id", 1)
            .with_u64("correlated_result", 0)
            .with_u64("timestamps", 1)
            .with_u64("usage", 1)
            .with_u64("terminal_typing", 1)
            .with_u64("schema_version", 1)
            .with_f64("score", 5.0 / 6.0);
        assert!(!wave4_event_stream_passes(&missing_hard_component).expect("complete evidence"));
    }

    #[test]
    fn permission_oracle_counts_every_trial() {
        let exact = ObservationSet::new()
            .with_f64("score", 0.75)
            .with_u64("repetitions", 5)
            .with_u64("tty_prompts", 0)
            .with_u64("allowed_effects", 5)
            .with_u64("denied_filesystem_effects", 5)
            .with_u64("denied_network_effects", 5)
            .with_u64("scope_violations", 0);
        assert!(wave4_permission_passes(&exact).expect("complete evidence"));
        let undercount = exact.clone().with_u64("denied_network_effects", 4);
        assert!(!wave4_permission_passes(&undercount).expect("complete evidence"));
    }

    #[test]
    fn tool_role_oracle_requires_exactly_two_checks_per_repetition() {
        let exact = ObservationSet::new()
            .with_u64("repetitions", 5)
            .with_u64("checks", 10)
            .with_u64("violations", 0)
            .with_u64("plain_user_text_violations", 0)
            .with_u64("missing_results", 0)
            .with_u64("duplicate_results", 0);
        assert!(wave4_tool_role_passes(&exact).expect("complete evidence"));
        let vacuous = exact
            .clone()
            .with_u64("repetitions", 0)
            .with_u64("checks", 0);
        assert!(!wave4_tool_role_passes(&vacuous).expect("complete evidence"));
        let undercount = exact.with_u64("checks", 9);
        assert!(!wave4_tool_role_passes(&undercount).expect("complete evidence"));
    }
}
