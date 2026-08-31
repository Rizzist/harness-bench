//! Deterministic collectors from raw run observations into strict matrix evidence.
//!
//! Collectors do not classify rows and never equate a terminal event with success of
//! the row's method. They derive what can be proven from normalized events, model
//! records, file hashes, exits, and timings. Measurements that require orchestration
//! or OS instrumentation are accepted as typed supplemental observations; omitted
//! measurements remain omitted and therefore fail the strict evaluator.

use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::ModelRequestRecord;
use crate::matrix_evidence::{EvidenceValue, ObservationSet, RowEvidence};
use crate::{AhrbError, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// One expected semantic tool invocation.
#[derive(Clone, Debug)]
pub struct ExpectedToolCall {
    /// Stable call ID.
    pub call_id: String,
    /// Exact tool name.
    pub name: String,
    /// Canonical semantic arguments.
    pub arguments: Value,
    /// Optional value which a dependent call must contain.
    pub dependency_value: Option<String>,
}

/// A file-system effect observed independently of stdout.
#[derive(Clone, Debug)]
pub struct FileObservation {
    /// Run-relative path.
    pub path: String,
    /// Actor that owns the workspace.
    pub actor: String,
    /// Correlated tool-call ID.
    pub call_id: String,
    /// Observed lowercase digest.
    pub sha256: String,
    /// Expected lowercase digest.
    pub expected_sha256: String,
    /// Number of committed writes to this path.
    pub writes: u64,
    /// Whether the path was outside declared roots.
    pub outside_declared_roots: bool,
}

/// Expected routing tuple and credential-redaction probe.
#[derive(Clone, Debug, Default)]
pub struct RoutingExpectation {
    /// Required logical actors or roles.
    pub roles: BTreeSet<String>,
    /// Exact model ID.
    pub model: String,
    /// Fake-model dialect.
    pub dialect: String,
    /// Expected credential fingerprint, when known.
    pub credential_fingerprint: Option<String>,
    /// Raw secret which must not appear in captured records.
    pub secret: Option<String>,
}

/// One observed process exit.
#[derive(Clone, Debug)]
pub struct ExitObservation {
    /// Semantic category.
    pub category: String,
    /// Actual process exit code.
    pub code: i32,
    /// Manifest-defined exit code.
    pub expected_code: i32,
    /// Whether a structural terminal accompanied the exit.
    pub structured_terminal: bool,
}

/// An elapsed interval measured from named evidence boundaries.
#[derive(Clone, Debug)]
pub struct TimingObservation {
    /// Stable measurement name.
    pub name: String,
    /// Elapsed milliseconds.
    pub elapsed_ms: u64,
    /// Applicable upper bound.
    pub limit_ms: u64,
}

/// Raw and independently measured inputs for one row collector.
#[derive(Clone, Debug, Default)]
pub struct CollectionInput<'a> {
    /// Normalized events. Collection sorts and stable-ID deduplicates these.
    pub events: &'a [NormalizedEvent],
    /// Fake-model request records.
    pub model_requests: &'a [ModelRequestRecord],
    /// Expected calls for the scripted workload.
    pub expected_tools: &'a [ExpectedToolCall],
    /// Independently hashed file effects.
    pub files: &'a [FileObservation],
    /// Actual OS exit observations.
    pub exits: &'a [ExitObservation],
    /// Named monotonic timings.
    pub timings: &'a [TimingObservation],
    /// Exact expected route, when applicable.
    pub routing: Option<&'a RoutingExpectation>,
    /// Low-level measurements not representable by event/file/request records.
    /// Event-derived keys overwrite conflicting supplemental keys.
    pub supplemental: Option<&'a ObservationSet>,
}

/// Collect strict evidence for correctness/functionality rows 1 through 19.
pub fn collect_correctness_functionality(
    row: u8,
    input: &CollectionInput<'_>,
) -> Result<RowEvidence> {
    if !(1..=19).contains(&row) {
        return Err(AhrbError::Validation(format!(
            "correctness/functionality collector does not accept row {row}"
        )));
    }
    let events = canonical_events(input.events);
    let mut values = input.supplemental.cloned().unwrap_or_default();
    derive_common(&mut values, &events);
    match row {
        1 => collect_routing(&mut values, input),
        2 => collect_single_tool(&mut values, input, &events),
        3 => collect_sequential_tools(&mut values, input, &events),
        4 => collect_parallel_tools(&mut values, input, &events),
        5 => collect_fragmented(&mut values, &events),
        6 => collect_failed_tool(&mut values, &events),
        7 => collect_malformed(&mut values, &events),
        8 => collect_dedup(&mut values, input, &events),
        9 | 10 => collect_terminal_exit(&mut values, input, &events, row),
        11 => collect_retry(&mut values, input, &events),
        12 => collect_idle_deadline(&mut values, input, &events),
        13 => collect_workspace_effects(&mut values, input),
        14 => collect_confinement(&mut values, input),
        15 => collect_headless(&mut values, input, &events),
        16 => {}
        17 => collect_actor_isolation(&mut values, input, &events),
        18 => collect_delegation(&mut values, &events),
        19 => collect_exit_matrix(&mut values, input),
        _ => {
            return Err(AhrbError::Validation(format!(
                "correctness/functionality collector has no row {row}"
            )));
        }
    }
    row_evidence(row, values)
}

/// Collect strict evidence for automation rows 30 through 41.
pub fn collect_automation(row: u8, input: &CollectionInput<'_>) -> Result<RowEvidence> {
    if !(30..=41).contains(&row) {
        return Err(AhrbError::Validation(format!(
            "automation collector does not accept row {row}"
        )));
    }
    let events = canonical_events(input.events);
    let mut values = input.supplemental.cloned().unwrap_or_default();
    derive_common(&mut values, &events);
    match row {
        30 => collect_session_replay(&mut values, &events),
        31 => collect_input_phase(&mut values, &events, "steer"),
        32 => collect_input_phase(&mut values, &events, "subturn"),
        33 => collect_queued(&mut values, &events),
        34 => {}
        35 => collect_crash(&mut values, input, &events),
        36 => collect_cancel(&mut values, input, &events),
        37 => collect_resume_idempotency(&mut values, &events),
        38 => {}
        39 => collect_hooks(&mut values, &events),
        40 => collect_journal(&mut values, &events),
        41 => collect_profile_isolation(&mut values, input),
        _ => {
            return Err(AhrbError::Validation(format!(
                "automation collector has no row {row}"
            )));
        }
    }
    row_evidence(row, values)
}

fn canonical_events(events: &[NormalizedEvent]) -> Vec<&NormalizedEvent> {
    let mut ordered: Vec<_> = events.iter().collect();
    ordered.sort_by(|left, right| {
        (left.session_id.as_str(), left.cursor, left.id.as_str()).cmp(&(
            right.session_id.as_str(),
            right.cursor,
            right.id.as_str(),
        ))
    });
    let mut seen = BTreeSet::new();
    ordered.retain(|event| seen.insert(event.id.as_str()));
    ordered
}

fn derive_common(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    set_u64(
        values,
        "success_terminals",
        count(events, EventVocab::TerminalSuccess),
    );
    set_u64(
        values,
        "failure_terminals",
        count(events, EventVocab::TerminalFailure),
    );
    set_u64(values, "calls", count(events, EventVocab::ToolCall));
    set_u64(
        values,
        "semantic_calls",
        count(events, EventVocab::ToolCall),
    );
    set_u64(
        values,
        "tool_results",
        count(events, EventVocab::ToolResult),
    );
    set_bool(values, "structural_terminal", terminal_count(events) == 1);
}

fn collect_routing(values: &mut ObservationSet, input: &CollectionInput<'_>) {
    let Some(expected) = input.routing else {
        return;
    };
    let mut records: Vec<_> = input.model_requests.iter().collect();
    records.sort_by(|left, right| {
        (
            left.request.actor.as_str(),
            left.request.checkpoint.as_str(),
            left.canonical_hash.as_str(),
        )
            .cmp(&(
                right.request.actor.as_str(),
                right.request.checkpoint.as_str(),
                right.canonical_hash.as_str(),
            ))
    });
    let observed: BTreeSet<_> = records
        .iter()
        .map(|record| record.request.actor.clone())
        .collect();
    let exact_tuple = !records.is_empty()
        && records.iter().all(|record| {
            record.request.model == expected.model
                && record.request.dialect == expected.dialect
                && expected
                    .credential_fingerprint
                    .as_ref()
                    .is_none_or(|value| record.request.credential_fingerprint == *value)
        });
    let redacted = records
        .iter()
        .all(|record| record.request.credential_fingerprint != "absent")
        && expected.secret.as_ref().is_none_or(|secret| {
            !records
                .iter()
                .any(|record| model_record_contains(record, secret))
        });
    set_bool(values, "all_roles_observed", observed == expected.roles);
    set_bool(values, "exact_tuple", exact_tuple);
    set_bool(values, "credential_redacted", redacted);
}

fn collect_single_tool(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let expected = input.expected_tools.first();
    let calls = of_type(events, EventVocab::ToolCall);
    let results = of_type(events, EventVocab::ToolResult);
    let args_match = expected.is_some_and(|expected| {
        calls.len() == 1
            && event_call_id(calls[0]) == Some(expected.call_id.as_str())
            && calls[0].payload.get("name").and_then(Value::as_str) == Some(expected.name.as_str())
            && calls[0].payload.get("arguments") == Some(&expected.arguments)
    });
    let correlated = expected.is_some_and(|expected| {
        results.len() == 1 && event_call_id(results[0]) == Some(expected.call_id.as_str())
    });
    let effects = expected.map_or(0, |expected| {
        input
            .files
            .iter()
            .filter(|file| {
                file.call_id == expected.call_id
                    && file.writes == 1
                    && file.sha256 == file.expected_sha256
            })
            .count() as u64
    });
    set_bool(values, "args_byte_match", args_match);
    set_bool(values, "result_correlated", correlated);
    set_u64(values, "effects", effects);
}

fn collect_sequential_tools(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let calls = of_type(events, EventVocab::ToolCall);
    let results = of_type(events, EventVocab::ToolResult);
    let expected_ids: Vec<_> = input
        .expected_tools
        .iter()
        .map(|call| call.call_id.as_str())
        .collect();
    let observed_ids: Vec<_> = calls
        .iter()
        .filter_map(|event| event_call_id(event))
        .collect();
    let a_before_b = expected_ids.len() == 2
        && observed_ids == expected_ids
        && calls[0].cursor < results.first().map_or(u64::MAX, |event| event.cursor)
        && results
            .first()
            .is_some_and(|result| result.cursor < calls.get(1).map_or(0, |event| event.cursor));
    let correlations = expected_ids.len() == 2
        && results
            .iter()
            .filter_map(|event| event_call_id(event))
            .eq(expected_ids.iter().copied());
    let dependency = input.expected_tools.get(1).is_some_and(|second| {
        second.dependency_value.as_ref().is_some_and(|needle| {
            calls
                .get(1)
                .and_then(|event| event.payload.get("arguments"))
                .is_some_and(|arguments| json_contains(arguments, needle))
        })
    });
    let effects = results.len() as u64;
    set_bool(values, "a_before_b", a_before_b);
    set_bool(values, "b_contains_a_output", dependency);
    set_bool(values, "correlations_correct", correlations);
    set_u64(values, "effects", effects);
}

fn collect_parallel_tools(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let mut live = BTreeSet::new();
    let mut max_live = 0_u64;
    let mut result_ids = Vec::new();
    for event in events {
        match event.event {
            EventVocab::ToolCall => {
                if let Some(id) = event_call_id(event) {
                    live.insert(id.to_owned());
                    max_live = max_live.max(live.len() as u64);
                }
            }
            EventVocab::ToolResult => {
                if let Some(id) = event_call_id(event) {
                    live.remove(id);
                    result_ids.push(id.to_owned());
                }
            }
            _ => {}
        }
    }
    let expected_ids: Vec<_> = input
        .expected_tools
        .iter()
        .map(|call| call.call_id.clone())
        .collect();
    let reversed: Vec<_> = expected_ids.iter().rev().cloned().collect();
    set_u64(values, "max_live_calls", max_live);
    set_bool(values, "reverse_completion", result_ids == reversed);
    set_bool(
        values,
        "each_once",
        unique_call_result_pairs(events, &expected_ids),
    );
    set_bool(
        values,
        "correlations_correct",
        unique_call_result_pairs(events, &expected_ids),
    );
    let calls = of_type(events, EventVocab::ToolCall);
    let observed_call_ids: Vec<_> = calls
        .iter()
        .filter_map(|event| event_call_id(event).map(str::to_owned))
        .collect();
    set_bool(
        values,
        "frame_order_preserved",
        observed_call_ids == expected_ids,
    );
}

fn collect_fragmented(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    set_u64(values, "invocations", count(events, EventVocab::ToolCall));
}

fn collect_failed_tool(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    let results = of_type(events, EventVocab::ToolResult);
    let failed: Vec<_> = results
        .iter()
        .filter(|event| event.payload.pointer("/result/ok").and_then(Value::as_bool) == Some(false))
        .copied()
        .collect();
    let correlated = failed
        .first()
        .and_then(|event| event_call_id(event))
        .is_some_and(|id| {
            of_type(events, EventVocab::ToolCall)
                .iter()
                .any(|event| event_call_id(event) == Some(id))
        });
    let reached_next = failed.first().is_some_and(|failed| {
        events
            .iter()
            .any(|event| event.event == EventVocab::ModelRequest && event.cursor > failed.cursor)
    });
    set_u64(values, "failed_results", failed.len() as u64);
    set_bool(values, "structured", failed.len() == 1);
    set_bool(values, "correlated", correlated);
    set_bool(values, "reached_next_request", reached_next);
}

fn collect_malformed(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    let failures = of_type(events, EventVocab::TerminalFailure);
    set_bool(values, "structured_failure", failures.len() == 1);
    set_bool(values, "terminal", terminal_count(events) == 1);
    set_u64(
        values,
        "unintended_effects",
        count(events, EventVocab::ToolResult),
    );
}

fn collect_dedup(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    set_u64(
        values,
        "effects",
        input.files.iter().map(|file| file.writes).sum(),
    );
    let ids: Vec<_> = input
        .expected_tools
        .iter()
        .map(|call| call.call_id.clone())
        .collect();
    set_bool(
        values,
        "ids_preserved",
        unique_call_result_pairs(events, &ids),
    );
    let calls = of_type(events, EventVocab::ToolCall);
    let observed: Vec<_> = calls
        .iter()
        .filter_map(|event| event_call_id(event).map(str::to_owned))
        .collect();
    set_bool(values, "order_preserved", observed == ids);
}

fn collect_terminal_exit(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
    row: u8,
) {
    let exit = input.exits.first();
    set_bool(
        values,
        "machine_parseable",
        terminal_count(events) == 1 && exit.is_some_and(|item| item.structured_terminal),
    );
    set_bool(
        values,
        "exit_matches_contract",
        exit.is_some_and(|item| item.code == item.expected_code),
    );
    if row == 9 {
        set_bool(values, "later_contradiction", terminal_count(events) > 1);
    } else {
        set_bool(
            values,
            "nonzero_exit",
            exit.is_some_and(|item| item.code != 0),
        );
    }
}

fn collect_retry(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let attempts: u64 = input
        .model_requests
        .iter()
        .map(|record| record.attempts)
        .sum();
    set_u64(values, "attempts", attempts);
    set_u64(
        values,
        "effects",
        input.files.iter().map(|file| file.writes).sum(),
    );
    set_bool(values, "structured_terminal", terminal_count(events) == 1);
}

fn collect_idle_deadline(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let failure = of_type(events, EventVocab::TerminalFailure)
        .into_iter()
        .find(|event| {
            event.payload.get("category").and_then(Value::as_str) == Some("idle-timeout")
        });
    let idle = timing(input.timings, "idle-deadline");
    let outer = timing(input.timings, "outer-deadline");
    set_bool(values, "own_terminal", failure.is_some());
    set_bool(
        values,
        "structured_failure",
        failure.is_some() && terminal_count(events) == 1,
    );
    if let (Some(idle), Some(outer)) = (idle, outer) {
        set_bool(
            values,
            "before_outer_deadline",
            idle.elapsed_ms < outer.limit_ms,
        );
        set_bool(
            values,
            "within_idle_grace",
            idle.elapsed_ms <= idle.limit_ms && idle.elapsed_ms < outer.limit_ms,
        );
    }
}

fn collect_workspace_effects(values: &mut ObservationSet, input: &CollectionInput<'_>) {
    let matching: Vec<_> = input
        .files
        .iter()
        .filter(|file| file.sha256 == file.expected_sha256 && file.writes == 1)
        .collect();
    set_bool(values, "create_hash_match", !matching.is_empty());
    set_bool(values, "patch_hash_match", matching.len() >= 2);
}

fn collect_confinement(values: &mut ObservationSet, input: &CollectionInput<'_>) {
    set_u64(
        values,
        "outside_writes",
        input
            .files
            .iter()
            .filter(|file| file.outside_declared_roots)
            .count() as u64,
    );
}

fn collect_headless(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let calls = of_type(events, EventVocab::ToolCall);
    set_u64(
        values,
        "reads",
        calls
            .iter()
            .filter(|event| {
                event.payload.get("name").and_then(Value::as_str) == Some("read_fixture")
            })
            .count() as u64,
    );
    set_u64(values, "marker_writes", input.files.len() as u64);
    set_u64(
        values,
        "effects",
        input.files.iter().map(|file| file.writes).sum(),
    );
    if let Some(exit) = input.exits.first() {
        set_u64(
            values,
            "exit_code",
            u64::try_from(exit.code).map_or(0, |code| code),
        );
    }
}

fn collect_actor_isolation(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    let actors: BTreeSet<_> = events.iter().map(|event| event.actor.as_str()).collect();
    let owned = input.files.iter().all(|file| {
        !file.actor.is_empty()
            && events.iter().any(|event| {
                event.actor == file.actor && event_call_id(event) == Some(&file.call_id)
            })
    });
    set_u64(values, "actors", actors.len() as u64);
    set_bool(
        values,
        "effect_ownership_exact",
        owned && !input.files.is_empty(),
    );
}

fn collect_delegation(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    set_u64(values, "spawns", count(events, EventVocab::AgentSpawned));
}

fn collect_exit_matrix(values: &mut ObservationSet, input: &CollectionInput<'_>) {
    let categories: BTreeSet<_> = input
        .exits
        .iter()
        .map(|exit| exit.category.as_str())
        .collect();
    set_u64(values, "categories", categories.len() as u64);
    set_bool(
        values,
        "codes_invariant",
        input
            .exits
            .iter()
            .all(|exit| exit.code == exit.expected_code),
    );
    set_bool(
        values,
        "structured_non_success",
        input
            .exits
            .iter()
            .filter(|exit| exit.code != 0)
            .all(|exit| exit.structured_terminal),
    );
    set_bool(
        values,
        "success_zero",
        input
            .exits
            .iter()
            .filter(|exit| exit.category == "success")
            .all(|exit| exit.code == 0),
    );
}

fn collect_session_replay(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    let sessions: BTreeSet<_> = events
        .iter()
        .map(|event| event.session_id.as_str())
        .collect();
    set_bool(
        values,
        "same_session",
        sessions.len() == 1 && !events.is_empty(),
    );
    set_bool(values, "ordered", cursors_ordered(events));
    set_u64(values, "duplicates", duplicate_ids(events));
    set_u64(values, "gaps", cursor_gaps(events));
    set_bool(
        values,
        "continued_b",
        count(events, EventVocab::TurnAccepted) >= 2,
    );
}

fn collect_input_phase(values: &mut ObservationSet, events: &[&NormalizedEvent], phase: &str) {
    let accepted: Vec<_> = of_type(events, EventVocab::InputAccepted)
        .into_iter()
        .filter(|event| event.payload.get("phase").and_then(Value::as_str) == Some(phase))
        .collect();
    set_u64(values, "input_accepts", accepted.len() as u64);
    if phase == "subturn" {
        if let Some(input) = accepted.first() {
            let effects_before = events
                .iter()
                .filter(|event| {
                    event.event == EventVocab::ToolResult && event.cursor < input.cursor
                })
                .count() as u64;
            set_u64(values, "effects_before_input", effects_before);
            set_bool(
                values,
                "observed_before_effect",
                events.iter().any(|event| {
                    event.event == EventVocab::ToolResult && event.cursor > input.cursor
                }),
            );
        }
    }
}

fn collect_queued(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    let accepted = of_type(events, EventVocab::TurnAccepted);
    let terminals: Vec<_> = events
        .iter()
        .filter(|event| is_terminal(&event.event))
        .collect();
    set_u64(values, "b_runs", accepted.len().saturating_sub(1) as u64);
    set_bool(values, "distinct_turn", accepted.len() == 2);
    set_bool(
        values,
        "a_terminal_before_b",
        terminals
            .first()
            .zip(accepted.get(1))
            .is_some_and(|(a, b)| a.cursor < b.cursor),
    );
}

fn collect_crash(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    if let Some(readiness) = timing(input.timings, "crash-readiness") {
        set_u64(values, "readiness_ms", readiness.elapsed_ms);
    }
    set_bool(values, "finite_terminal", terminal_count(events) == 1);
    set_u64(
        values,
        "committed_effects",
        input.files.iter().map(|file| file.writes).sum(),
    );
}

fn collect_cancel(
    values: &mut ObservationSet,
    input: &CollectionInput<'_>,
    events: &[&NormalizedEvent],
) {
    if let Some(cancel) = timing(input.timings, "cancel-terminal") {
        set_u64(values, "terminal_ms", cancel.elapsed_ms);
    }
    set_bool(
        values,
        "cancel_terminal_observed",
        count(events, EventVocab::TerminalCancelled) >= 1,
    );
}

fn collect_resume_idempotency(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    set_u64(
        values,
        "semantic_turns",
        count(events, EventVocab::TurnAccepted),
    );
    set_u64(values, "effects", count(events, EventVocab::ToolResult));
    set_bool(values, "stable_identity_dedup", duplicate_ids(events) == 0);
}

fn collect_hooks(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    let hooks = of_type(events, EventVocab::HookCompleted);
    set_u64(
        values,
        "acceptance_hooks",
        hooks
            .iter()
            .filter(|event| event.payload.get("kind").and_then(Value::as_str) == Some("acceptance"))
            .count() as u64,
    );
    set_u64(
        values,
        "completion_hooks",
        hooks
            .iter()
            .filter(|event| event.payload.get("kind").and_then(Value::as_str) == Some("completion"))
            .count() as u64,
    );
}

fn collect_journal(values: &mut ObservationSet, events: &[&NormalizedEvent]) {
    set_u64(values, "duplicates", duplicate_ids(events));
    set_u64(values, "gaps", cursor_gaps(events));
    set_bool(values, "ordered", cursors_ordered(events));
}

fn collect_profile_isolation(values: &mut ObservationSet, input: &CollectionInput<'_>) {
    set_u64(
        values,
        "outside_writes",
        input
            .files
            .iter()
            .filter(|file| file.outside_declared_roots)
            .count() as u64,
    );
}

fn row_evidence(row: u8, values: ObservationSet) -> Result<RowEvidence> {
    let evidence = match row {
        1 => RowEvidence::Routing(values),
        2 => RowEvidence::SingleToolCall(values),
        3 => RowEvidence::SequentialToolCalls(values),
        4 => RowEvidence::ParallelToolCalls(values),
        5 => RowEvidence::FragmentedToolArgs(values),
        6 => RowEvidence::FailedToolResult(values),
        7 => RowEvidence::MalformedToolCall(values),
        8 => RowEvidence::ToolIdDedup(values),
        9 => RowEvidence::TerminalSuccess(values),
        10 => RowEvidence::TerminalFailure(values),
        11 => RowEvidence::UpstreamRetry(values),
        12 => RowEvidence::IdleDeadline(values),
        13 => RowEvidence::WorkspaceEffects(values),
        14 => RowEvidence::StateNetworkConfinement(values),
        15 => RowEvidence::HeadlessWorkflow(values),
        16 => RowEvidence::TranscriptDeterminism(values),
        17 => RowEvidence::ActorIsolation(values),
        18 => RowEvidence::NativeDelegation(values),
        19 => RowEvidence::ExitCodes(values),
        30 => RowEvidence::SessionReplay(values),
        31 => RowEvidence::Steer(values),
        32 => RowEvidence::Subturn(values),
        33 => RowEvidence::QueuedTurn(values),
        34 => RowEvidence::Noninteractive(values),
        35 => RowEvidence::CrashRecovery(values),
        36 => RowEvidence::CancelCleanup(values),
        37 => RowEvidence::ResumeIdempotency(values),
        38 => RowEvidence::ResourceBounds(values),
        39 => RowEvidence::Hooks(values),
        40 => RowEvidence::DurableJournal(values),
        41 => RowEvidence::ProfileNetworkIsolation(values),
        _ => {
            return Err(AhrbError::Validation(format!(
                "no event collector for row {row}"
            )));
        }
    };
    Ok(evidence)
}

fn count(events: &[&NormalizedEvent], event: EventVocab) -> u64 {
    events.iter().filter(|item| item.event == event).count() as u64
}

fn of_type<'a>(events: &[&'a NormalizedEvent], event: EventVocab) -> Vec<&'a NormalizedEvent> {
    events
        .iter()
        .filter(|item| item.event == event)
        .copied()
        .collect()
}

fn terminal_count(events: &[&NormalizedEvent]) -> usize {
    events
        .iter()
        .filter(|event| is_terminal(&event.event))
        .count()
}

fn is_terminal(event: &EventVocab) -> bool {
    matches!(
        event,
        EventVocab::TerminalSuccess
            | EventVocab::TerminalFailure
            | EventVocab::TerminalCancelled
            | EventVocab::TerminalTimeout
    )
}

fn event_call_id(event: &NormalizedEvent) -> Option<&str> {
    event.payload.get("call_id").and_then(Value::as_str)
}

fn unique_call_result_pairs(events: &[&NormalizedEvent], expected: &[String]) -> bool {
    expected.iter().all(|id| {
        events
            .iter()
            .filter(|event| event.event == EventVocab::ToolCall && event_call_id(event) == Some(id))
            .count()
            == 1
            && events
                .iter()
                .filter(|event| {
                    event.event == EventVocab::ToolResult && event_call_id(event) == Some(id)
                })
                .count()
                == 1
    })
}

fn json_contains(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(items) => items.iter().any(|item| json_contains(item, needle)),
        Value::Object(items) => items.values().any(|item| json_contains(item, needle)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn model_record_contains(record: &ModelRequestRecord, needle: &str) -> bool {
    [
        record.request.dialect.as_str(),
        record.request.model.as_str(),
        record.request.scenario.as_str(),
        record.request.actor.as_str(),
        record.request.checkpoint.as_str(),
        record.request.credential_fingerprint.as_str(),
        record.canonical_hash.as_str(),
    ]
    .iter()
    .any(|value| value.contains(needle))
        || json_contains(&record.request.canonical, needle)
}

fn timing<'a>(timings: &'a [TimingObservation], name: &str) -> Option<&'a TimingObservation> {
    timings.iter().find(|timing| timing.name == name)
}

fn cursors_ordered(events: &[&NormalizedEvent]) -> bool {
    let mut by_session: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for event in events {
        by_session
            .entry(event.session_id.as_str())
            .or_default()
            .push(event.cursor);
    }
    by_session
        .values()
        .all(|cursors| cursors.windows(2).all(|pair| pair[0] < pair[1]))
}

fn cursor_gaps(events: &[&NormalizedEvent]) -> u64 {
    let mut by_session: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for event in events {
        by_session
            .entry(event.session_id.as_str())
            .or_default()
            .push(event.cursor);
    }
    by_session
        .values_mut()
        .map(|cursors| {
            cursors.sort_unstable();
            cursors
                .windows(2)
                .map(|pair| pair[1].saturating_sub(pair[0]).saturating_sub(1))
                .sum::<u64>()
        })
        .sum()
}

fn duplicate_ids(events: &[&NormalizedEvent]) -> u64 {
    let mut seen = BTreeSet::new();
    events
        .iter()
        .filter(|event| !seen.insert(event.id.as_str()))
        .count() as u64
}

fn set_bool(values: &mut ObservationSet, name: &str, value: bool) {
    values
        .values
        .insert(name.to_owned(), EvidenceValue::Bool(value));
}

fn set_u64(values: &mut ObservationSet, name: &str, value: u64) {
    values
        .values
        .insert(name.to_owned(), EvidenceValue::U64(value));
}
