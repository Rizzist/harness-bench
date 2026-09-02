//! Wave-3 long-horizon, session lifecycle, context recovery, resume, and journal oracles.
//!
//! Every input is observed at an AHRB-owned process, filesystem, driver, or fake-provider
//! boundary.  The evaluators never instrument the harness turn path and never manufacture
//! favorable zeroes for missing evidence.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const TORN_RECORD_BYTES: u64 = 1_048_576;
const TORN_CUT_OFFSETS: [u64; 5] = [0, 262_144, 524_288, 786_432, 1_048_575];

/// One identity-safe filesystem entry observed under a declared session-store root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionStoreEntry {
    pub root_index: u32,
    pub relative_path: String,
    pub file_type: String,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub sha256: Option<String>,
}

/// One post-close row-50 checkpoint, expressed relative to its warm baseline.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionResidueCheckpoint {
    pub repetition: u32,
    pub sessions_created: u32,
    pub memory_residue_mib: f64,
    pub fd_delta: f64,
    pub thread_delta: f64,
    pub process_delta: f64,
    pub store_residue_bytes: u64,
    pub store_residue_files: u64,
    pub close_delete_validated_through: u32,
    pub traversal_valid: bool,
    pub store_entries: Vec<SessionStoreEntry>,
}

/// Per-sweep counters and reclaim evidence for row 50.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionResidueSweep {
    pub repetition: u32,
    pub created_sessions: u32,
    pub closed_sessions: u32,
    pub unretired_sessions: u32,
    pub maximum_active_delta_mib: f64,
    pub final_memory_residue_mib: f64,
    pub checkpoints: Vec<SessionResidueCheckpoint>,
    #[serde(default)]
    pub process_audits: Vec<SessionResidueProcessAudit>,
}

/// Per-session external process-lifetime evidence for an exec-topology row-50 sweep.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionResidueProcessAudit {
    pub repetition: u32,
    pub session_ordinal: u32,
    pub invocation_sample_count: u32,
    pub invocation_peak_memory_bytes: u64,
    pub invocation_peak_open_fds: u64,
    pub invocation_peak_threads: u64,
    pub invocation_peak_processes: u64,
    pub invocation_residue_memory_bytes: u64,
    pub invocation_residue_open_fds: u64,
    pub invocation_residue_threads: u64,
    pub invocation_residue_processes: u64,
    pub invocation_reclaim_validated: bool,
    pub close_delete_sample_count: u32,
    pub close_delete_peak_memory_bytes: u64,
    pub close_delete_peak_open_fds: u64,
    pub close_delete_peak_threads: u64,
    pub close_delete_peak_processes: u64,
    pub close_delete_residue_memory_bytes: u64,
    pub close_delete_residue_open_fds: u64,
    pub close_delete_residue_threads: u64,
    pub close_delete_residue_processes: u64,
    pub close_delete_reclaim_validated: bool,
    pub close_delete_result_validated: bool,
}

/// Exact row-50 result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SessionResidueEvaluation {
    pub resource_values: BTreeMap<String, f64>,
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
    pub session_store_final_residue_files: u64,
}

/// Evaluate row 50 after every official close-delete has completed.
pub fn evaluate_session_residue_sweep(
    sweeps: &[SessionResidueSweep],
    expected_repetitions: u32,
    expected_sessions: u32,
    per_invocation_topology: bool,
) -> SessionResidueEvaluation {
    let incomplete = |message: String| SessionResidueEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..SessionResidueEvaluation::default()
    };
    if expected_sessions == 0 || expected_sessions % 10 != 0 {
        return incomplete("row-50 session count must be a positive multiple of ten".to_owned());
    }
    if u32::try_from(sweeps.len()).ok() != Some(expected_repetitions) {
        return incomplete(format!(
            "row-50 has {} sweeps; expected {expected_repetitions}",
            sweeps.len()
        ));
    }
    let expected_checkpoints = (0..=expected_sessions).step_by(10).collect::<Vec<_>>();
    let mut seen_repetitions = BTreeSet::new();
    let mut memory_slopes = Vec::new();
    let mut fd_slopes = Vec::new();
    let mut thread_slopes = Vec::new();
    let mut process_slopes = Vec::new();
    let mut store_byte_slopes = Vec::new();
    let mut store_file_slopes = Vec::new();
    let mut maximum_final_memory = 0.0_f64;
    let mut maximum_final_store_bytes = 0_u64;
    let mut maximum_final_store_files = 0_u64;
    let mut maximum_active_delta = 0.0_f64;
    let mut created_total = 0_u64;
    let mut closed_total = 0_u64;
    let mut unretired_total = 0_u64;
    let mut effective_unretired_by_repetition = Vec::new();
    let mut all_passed = true;

    for sweep in sweeps {
        if sweep.repetition == 0
            || sweep.repetition > expected_repetitions
            || !seen_repetitions.insert(sweep.repetition)
        {
            return incomplete("row-50 has an invalid or duplicate repetition".to_owned());
        }
        if sweep.created_sessions != expected_sessions
            || sweep.closed_sessions > sweep.created_sessions
            || sweep.maximum_active_delta_mib.is_sign_negative()
            || !sweep.maximum_active_delta_mib.is_finite()
            || sweep.final_memory_residue_mib.is_sign_negative()
            || !sweep.final_memory_residue_mib.is_finite()
        {
            return incomplete(format!(
                "row-50 repetition {} has invalid lifecycle counters or memory evidence",
                sweep.repetition
            ));
        }
        let audit_unretired_sessions = if per_invocation_topology {
            let mut audits = sweep.process_audits.iter().collect::<Vec<_>>();
            audits.sort_by_key(|audit| audit.session_ordinal);
            if audits.len() != expected_sessions as usize
                || audits.iter().enumerate().any(|(index, audit)| {
                    audit.repetition != sweep.repetition
                        || audit.session_ordinal as usize != index.saturating_add(1)
                        || audit.invocation_sample_count == 0
                        || audit.close_delete_sample_count == 0
                        || !audit.close_delete_result_validated
                })
            {
                return incomplete(format!(
                    "row-50 repetition {} lacks complete invocation/close-delete process audits",
                    sweep.repetition
                ));
            }
            u32::try_from(
                audits
                    .iter()
                    .filter(|audit| {
                        !audit.invocation_reclaim_validated
                            || !audit.close_delete_reclaim_validated
                            || audit.invocation_residue_memory_bytes != 0
                            || audit.invocation_residue_open_fds != 0
                            || audit.invocation_residue_threads != 0
                            || audit.invocation_residue_processes != 0
                            || audit.close_delete_residue_memory_bytes != 0
                            || audit.close_delete_residue_open_fds != 0
                            || audit.close_delete_residue_threads != 0
                            || audit.close_delete_residue_processes != 0
                    })
                    .count(),
            )
            .unwrap_or(u32::MAX)
        } else if !sweep.process_audits.is_empty() {
            return incomplete(format!(
                "row-50 daemon repetition {} unexpectedly contains exec process audits",
                sweep.repetition
            ));
        } else {
            0
        };
        let mut checkpoints = sweep.checkpoints.iter().collect::<Vec<_>>();
        checkpoints.sort_by_key(|checkpoint| checkpoint.sessions_created);
        if checkpoints
            .iter()
            .map(|checkpoint| checkpoint.sessions_created)
            .collect::<Vec<_>>()
            != expected_checkpoints
        {
            return incomplete(format!(
                "row-50 repetition {} lacks the exact 0/every-10/N checkpoints",
                sweep.repetition
            ));
        }
        for checkpoint in &checkpoints {
            if checkpoint.repetition != sweep.repetition
                || checkpoint.close_delete_validated_through != checkpoint.sessions_created
                || !checkpoint.traversal_valid
                || !checkpoint.memory_residue_mib.is_finite()
                || checkpoint.memory_residue_mib.is_sign_negative()
                || !checkpoint.fd_delta.is_finite()
                || !checkpoint.thread_delta.is_finite()
                || !checkpoint.process_delta.is_finite()
            {
                return incomplete(format!(
                    "row-50 repetition {} has unvalidated close-delete or invalid checkpoint evidence",
                    sweep.repetition
                ));
            }
            let mut identities = BTreeSet::new();
            for entry in &checkpoint.store_entries {
                if entry.relative_path.is_empty()
                    || !identities.insert((entry.device, entry.inode))
                    || (entry.file_type == "regular" && entry.sha256.is_none())
                {
                    return incomplete(format!(
                        "row-50 repetition {} has duplicate or incomplete store identity evidence",
                        sweep.repetition
                    ));
                }
            }
        }
        let x = checkpoints
            .iter()
            .map(|checkpoint| checkpoint.sessions_created as f64)
            .collect::<Vec<_>>();
        memory_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.memory_residue_mib)
                .collect::<Vec<_>>(),
        ));
        fd_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.fd_delta)
                .collect::<Vec<_>>(),
        ));
        thread_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.thread_delta)
                .collect::<Vec<_>>(),
        ));
        process_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.process_delta)
                .collect::<Vec<_>>(),
        ));
        store_byte_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.store_residue_bytes as f64)
                .collect::<Vec<_>>(),
        ));
        store_file_slopes.push(theil_sen_xy(
            &x,
            &checkpoints
                .iter()
                .map(|checkpoint| checkpoint.store_residue_files as f64)
                .collect::<Vec<_>>(),
        ));
        let final_checkpoint = match checkpoints.last() {
            Some(value) => *value,
            None => return incomplete("row-50 checkpoint set is empty".to_owned()),
        };
        maximum_final_memory = maximum_final_memory.max(sweep.final_memory_residue_mib);
        maximum_final_store_bytes =
            maximum_final_store_bytes.max(final_checkpoint.store_residue_bytes);
        maximum_final_store_files =
            maximum_final_store_files.max(final_checkpoint.store_residue_files);
        maximum_active_delta = maximum_active_delta.max(sweep.maximum_active_delta_mib);
        created_total = created_total.saturating_add(u64::from(sweep.created_sessions));
        closed_total = closed_total.saturating_add(u64::from(sweep.closed_sessions));
        let effective_unretired = sweep.unretired_sessions.max(audit_unretired_sessions);
        effective_unretired_by_repetition.push(effective_unretired);
        unretired_total = unretired_total.saturating_add(u64::from(effective_unretired));
    }

    let memory_slope = median(&memory_slopes);
    let fd_slope = median(&fd_slopes);
    let thread_slope = median(&thread_slopes);
    let process_slope = median(&process_slopes);
    let store_byte_slope = median(&store_byte_slopes);
    let store_file_slope = median(&store_file_slopes);
    let final_bound_mib = 64.0_f64.max(0.20 * maximum_active_delta);
    for index in 0..sweeps.len() {
        all_passed &= sweeps[index].closed_sessions == expected_sessions
            && effective_unretired_by_repetition[index] == 0
            && memory_slopes[index] <= 0.03125
            && fd_slopes[index] <= 0.0
            && thread_slopes[index] <= 0.0
            && process_slopes[index] <= 0.0
            && store_byte_slopes[index] <= 0.0
            && store_file_slopes[index] <= 0.0
            && sweeps[index].final_memory_residue_mib <= final_bound_mib
            && sweeps[index]
                .checkpoints
                .iter()
                .find(|checkpoint| checkpoint.sessions_created == expected_sessions)
                .is_some_and(|checkpoint| {
                    checkpoint.store_residue_bytes == 0 && checkpoint.store_residue_files == 0
                });
    }
    let resource_values = BTreeMap::from([
        (
            "session_residue_slope_mib_per_session".to_owned(),
            memory_slope,
        ),
        ("session_residue_final_mib".to_owned(), maximum_final_memory),
        (
            "session_store_byte_slope_per_session".to_owned(),
            store_byte_slope,
        ),
        (
            "session_store_file_count_slope_per_session".to_owned(),
            store_file_slope,
        ),
        (
            "session_store_final_residue_bytes".to_owned(),
            maximum_final_store_bytes as f64,
        ),
    ]);
    let metrics = BTreeMap::from([
        (
            "session_residue_sweep.fd_slope_per_session".to_owned(),
            fd_slope,
        ),
        (
            "session_residue_sweep.thread_slope_per_session".to_owned(),
            thread_slope,
        ),
        (
            "session_residue_sweep.process_slope_per_session".to_owned(),
            process_slope,
        ),
        (
            "session_residue_sweep.unretired_sessions".to_owned(),
            unretired_total as f64,
        ),
        (
            "session_residue_sweep.closed_sessions".to_owned(),
            closed_total as f64,
        ),
        (
            "session_residue_sweep.created_sessions".to_owned(),
            created_total as f64,
        ),
    ]);
    SessionResidueEvaluation {
        resource_values,
        metrics,
        details: json!({
            "measurement_complete": true,
            "store_checkpoints": sweeps.iter().flat_map(|sweep| sweep.checkpoints.iter()).collect::<Vec<_>>(),
            "repetitions": sweeps,
            "final_memory_bound_mib": final_bound_mib,
            "session_store_final_residue_files": maximum_final_store_files,
        }),
        measurement_complete: true,
        passed: all_passed,
        measurement_error: None,
        session_store_final_residue_files: maximum_final_store_files,
    }
}

/// One independently executed context-error/recovery trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextToolPairRecord {
    pub call_id: String,
    pub function_name: String,
    pub canonical_arguments: Value,
    pub result_content: Value,
}

/// One dialect-native context item carrying the row-51 goal marker.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextGoalItemRecord {
    pub role: String,
    pub content: String,
}

/// One independently executed context-error/recovery trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextRecoveryTrial {
    pub repetition: u32,
    pub window_tokens: u64,
    pub public_turn_start_ns: u64,
    pub error_final_byte_ns: u64,
    pub accepted_request_received_ns: u64,
    pub terminal_received_ns: u64,
    pub pre_error_input_tokens: u64,
    pub pre_error_body_bytes: u64,
    pub accepted_input_tokens: u64,
    pub accepted_body_bytes: u64,
    pub context_errors: u32,
    pub extra_requests: u32,
    pub terminal_success: bool,
    #[serde(default)]
    pub structural_terminal_count: u32,
    pub tool_pairs_before: u32,
    pub tool_pairs_after: u32,
    #[serde(default)]
    pub committed_tool_pairs: Vec<ContextToolPairRecord>,
    #[serde(default)]
    pub faulting_tool_pairs: Vec<ContextToolPairRecord>,
    #[serde(default)]
    pub accepted_tool_pairs: Vec<ContextToolPairRecord>,
    #[serde(default)]
    pub faulting_goal_items: Vec<ContextGoalItemRecord>,
    #[serde(default)]
    pub accepted_goal_items: Vec<ContextGoalItemRecord>,
    #[serde(default)]
    pub committed_orphan_tool_calls: u32,
    #[serde(default)]
    pub committed_orphan_tool_results: u32,
    #[serde(default)]
    pub faulting_orphan_tool_calls: u32,
    #[serde(default)]
    pub faulting_orphan_tool_results: u32,
    #[serde(default)]
    pub accepted_orphan_tool_calls: u32,
    #[serde(default)]
    pub accepted_orphan_tool_results: u32,
    /// Aggregate across committed, faulting, and accepted streams.
    pub orphan_tool_calls: u32,
    /// Aggregate across committed, faulting, and accepted streams.
    pub orphan_tool_results: u32,
    pub duplicate_effects: u32,
    pub same_session_identity: bool,
    pub instructions_and_tools_unchanged: bool,
    pub required_markers_retained: bool,
    pub omitted_markers_summarized_exactly: bool,
    pub compacted_request_hash: String,
    pub retained_markers: Vec<String>,
    pub omitted_markers: Vec<String>,
    pub summary_markers: Vec<String>,
}

/// Exact row-51 result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContextRecoveryEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate deterministic recovery from one provider-owned context-length error.
pub fn evaluate_context_limit_recovery(
    trials: &[ContextRecoveryTrial],
    expected_repetitions: u32,
    expected_window_tokens: u64,
    expected_tool_pairs: u32,
    turn_timeout_ms: u64,
) -> ContextRecoveryEvaluation {
    let incomplete = |message: String| ContextRecoveryEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..ContextRecoveryEvaluation::default()
    };
    if u32::try_from(trials.len()).ok() != Some(expected_repetitions) {
        return incomplete("row-51 trial set is incomplete".to_owned());
    }
    let mut repetitions = BTreeSet::new();
    let mut compacted_hashes = BTreeSet::new();
    let mut recovery_ms = Vec::new();
    let mut context_errors = 0_u64;
    let mut extra_requests = 0_u64;
    let mut successes = 0_u64;
    let mut pairs_before = 0_u64;
    let mut pairs_after = 0_u64;
    let mut orphan_calls = 0_u64;
    let mut orphan_results = 0_u64;
    let mut duplicate_effects = 0_u64;
    let mut passed = true;
    for trial in trials {
        if trial.repetition == 0
            || trial.repetition > expected_repetitions
            || !repetitions.insert(trial.repetition)
            || trial.window_tokens != expected_window_tokens
            || trial.public_turn_start_ns == 0
            || trial.error_final_byte_ns < trial.public_turn_start_ns
            || trial.accepted_request_received_ns < trial.error_final_byte_ns
            || trial.terminal_received_ns < trial.accepted_request_received_ns
            || trial.compacted_request_hash.len() != 64
            || !trial
                .compacted_request_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return incomplete("row-51 has invalid repetition or external boundaries".to_owned());
        }
        let expected_pre_body = expected_window_tokens
            .checked_mul(8)
            .and_then(|value| value.checked_add(1_024));
        if trial.pre_error_input_tokens != expected_window_tokens.saturating_add(256)
            || Some(trial.pre_error_body_bytes) != expected_pre_body
        {
            return incomplete(
                "row-51 provider did not observe the exact W+256 token and 8W+1024 byte fixture"
                    .to_owned(),
            );
        }
        let recovery = trial
            .accepted_request_received_ns
            .saturating_sub(trial.error_final_byte_ns) as f64
            / 1_000_000.0;
        let public_turn_ms = trial
            .terminal_received_ns
            .saturating_sub(trial.public_turn_start_ns) as f64
            / 1_000_000.0;
        recovery_ms.push(recovery);
        compacted_hashes.insert(trial.compacted_request_hash.clone());
        context_errors = context_errors.saturating_add(u64::from(trial.context_errors));
        extra_requests = extra_requests.saturating_add(u64::from(trial.extra_requests));
        successes = successes.saturating_add(u64::from(trial.terminal_success));
        pairs_before = pairs_before.saturating_add(u64::from(trial.tool_pairs_before));
        pairs_after = pairs_after.saturating_add(u64::from(trial.tool_pairs_after));
        orphan_calls = orphan_calls.saturating_add(u64::from(trial.orphan_tool_calls));
        orphan_results = orphan_results.saturating_add(u64::from(trial.orphan_tool_results));
        duplicate_effects = duplicate_effects.saturating_add(u64::from(trial.duplicate_effects));
        let stream_orphan_calls = trial
            .committed_orphan_tool_calls
            .saturating_add(trial.faulting_orphan_tool_calls)
            .saturating_add(trial.accepted_orphan_tool_calls);
        let stream_orphan_results = trial
            .committed_orphan_tool_results
            .saturating_add(trial.faulting_orphan_tool_results)
            .saturating_add(trial.accepted_orphan_tool_results);
        passed &= trial.context_errors == 1
            && trial.extra_requests <= 2
            && recovery <= turn_timeout_ms as f64
            && public_turn_ms <= turn_timeout_ms as f64
            && trial.terminal_success
            && trial.structural_terminal_count == 1
            && trial.tool_pairs_before == expected_tool_pairs
            && trial.tool_pairs_after == expected_tool_pairs
            && trial.committed_tool_pairs.len() == expected_tool_pairs as usize
            && trial.committed_tool_pairs == trial.faulting_tool_pairs
            && trial.committed_tool_pairs == trial.accepted_tool_pairs
            && trial.faulting_goal_items.len() == 1
            && trial.faulting_goal_items == trial.accepted_goal_items
            && trial
                .faulting_goal_items
                .first()
                .is_some_and(|goal| goal.role == "user")
            && trial.orphan_tool_calls == stream_orphan_calls
            && trial.orphan_tool_results == stream_orphan_results
            && trial.committed_orphan_tool_calls == 0
            && trial.committed_orphan_tool_results == 0
            && trial.faulting_orphan_tool_calls == 0
            && trial.faulting_orphan_tool_results == 0
            && trial.accepted_orphan_tool_calls == 0
            && trial.accepted_orphan_tool_results == 0
            && trial.orphan_tool_calls == 0
            && trial.orphan_tool_results == 0
            && trial.duplicate_effects == 0
            && trial.same_session_identity
            && trial.instructions_and_tools_unchanged
            && trial.required_markers_retained
            && trial.omitted_markers_summarized_exactly
            && !trial.omitted_markers.is_empty()
            && trial.accepted_input_tokens <= expected_window_tokens
            && trial.accepted_body_bytes <= expected_window_tokens.saturating_mul(8);
    }
    passed &= compacted_hashes.len() == 1;
    let recovery_headline = median(&recovery_ms);
    ContextRecoveryEvaluation {
        metrics: BTreeMap::from([
            (
                "context_limit_recovery.context_errors".to_owned(),
                context_errors as f64,
            ),
            (
                "context_limit_recovery.extra_requests".to_owned(),
                extra_requests as f64,
            ),
            (
                "context_limit_recovery.recovery_ms".to_owned(),
                recovery_headline,
            ),
            (
                "context_limit_recovery.terminal_success".to_owned(),
                successes as f64,
            ),
            (
                "context_limit_recovery.tool_pairs_before".to_owned(),
                pairs_before as f64,
            ),
            (
                "context_limit_recovery.tool_pairs_after".to_owned(),
                pairs_after as f64,
            ),
            (
                "context_limit_recovery.orphan_tool_calls".to_owned(),
                orphan_calls as f64,
            ),
            (
                "context_limit_recovery.orphan_tool_results".to_owned(),
                orphan_results as f64,
            ),
            (
                "context_limit_recovery.duplicate_effects".to_owned(),
                duplicate_effects as f64,
            ),
        ]),
        details: json!({
            "measurement_complete": true,
            "trials": trials,
            "pre_error_input_tokens": trials.iter().map(|trial| trial.pre_error_input_tokens).collect::<Vec<_>>(),
            "pre_error_body_bytes": trials.iter().map(|trial| trial.pre_error_body_bytes).collect::<Vec<_>>(),
            "compaction_request_hashes": trials.iter().map(|trial| trial.compacted_request_hash.clone()).collect::<Vec<_>>(),
            "accepted_input_tokens": trials.iter().map(|trial| trial.accepted_input_tokens).collect::<Vec<_>>(),
            "accepted_body_bytes": trials.iter().map(|trial| trial.accepted_body_bytes).collect::<Vec<_>>(),
            "retained_markers": trials.iter().map(|trial| trial.retained_markers.clone()).collect::<Vec<_>>(),
            "omitted_markers": trials.iter().map(|trial| trial.omitted_markers.clone()).collect::<Vec<_>>(),
            "summary_markers": trials.iter().map(|trial| trial.summary_markers.clone()).collect::<Vec<_>>(),
            "normalized_compacted_stream_sha256_by_repetition": trials.iter().map(|trial| (&trial.repetition, &trial.compacted_request_hash)).collect::<Vec<_>>(),
        }),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// Count fake-provider input tokens using the normative revision-2.3 algorithm.
pub fn fake_context_input_tokens(dialect: &str, canonical: &Value) -> u64 {
    let pointers: &[&str] = match dialect {
        "openai-responses" => &["/instructions", "/input", "/tools"],
        "anthropic-messages" => &["/system", "/messages", "/tools"],
        _ => &["/messages", "/tools"],
    };
    pointers.iter().fold(0_u64, |total, pointer| {
        total.saturating_add(
            canonical
                .pointer(pointer)
                .map_or(0, count_json_string_value_tokens),
        )
    })
}

fn count_json_string_value_tokens(value: &Value) -> u64 {
    match value {
        Value::String(text) => count_fake_string_tokens(text),
        Value::Array(items) => items.iter().fold(0_u64, |total, item| {
            total.saturating_add(count_json_string_value_tokens(item))
        }),
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
            keys.into_iter().fold(0_u64, |total, key| {
                total.saturating_add(object.get(key).map_or(0, count_json_string_value_tokens))
            })
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

fn count_fake_string_tokens(text: &str) -> u64 {
    let mut tokens = 0_u64;
    let mut in_ascii_run = false;
    for character in text.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            if !in_ascii_run {
                tokens = tokens.saturating_add(1);
                in_ascii_run = true;
            }
        } else {
            in_ascii_run = false;
            if !character.is_ascii_whitespace() {
                tokens = tokens.saturating_add(1);
            }
        }
    }
    tokens
}

/// One external resume interval at a prebuilt session length.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ResumeLatencyPoint {
    pub length: u32,
    pub repetition: u32,
    pub resume_start_ns: u64,
    pub first_request_ns: u64,
    pub latency_ms: f64,
    pub session_id_hash: String,
    #[serde(default, skip_serializing)]
    pub expected_session_id_hash: String,
    pub cursor: u64,
    #[serde(default, skip_serializing)]
    pub expected_cursor: u64,
}

/// Exact row-52 result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResumeLatencyEvaluation {
    pub resource_values: BTreeMap<String, f64>,
    pub metrics: BTreeMap<String, f64>,
    pub long_short_ratio: Option<f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate official resume latency after aggregating each length first.
pub fn evaluate_resume_latency_vs_length(
    points: &[ResumeLatencyPoint],
    expected_lengths: &[u32],
    expected_repetitions: u32,
) -> ResumeLatencyEvaluation {
    let incomplete = |message: String| ResumeLatencyEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..ResumeLatencyEvaluation::default()
    };
    if expected_lengths.len() < 3
        || expected_lengths.windows(2).any(|pair| pair[0] >= pair[1])
        || points.len()
            != expected_lengths
                .len()
                .saturating_mul(expected_repetitions as usize)
    {
        return incomplete("row-52 length/repetition matrix is incomplete".to_owned());
    }
    let length_set = expected_lengths.iter().copied().collect::<BTreeSet<_>>();
    let mut keys = BTreeSet::new();
    let mut latencies_by_length = BTreeMap::<u32, Vec<f64>>::new();
    let mut identity_cursor_ok = true;
    for point in points {
        if !length_set.contains(&point.length)
            || point.repetition == 0
            || point.repetition > expected_repetitions
            || !keys.insert((point.length, point.repetition))
            || point.resume_start_ns == 0
            || point.first_request_ns < point.resume_start_ns
            || !point.latency_ms.is_finite()
            || point.latency_ms.is_sign_negative()
            || point.session_id_hash.is_empty()
            || point.expected_session_id_hash.is_empty()
        {
            return incomplete("row-52 has invalid or duplicate external intervals".to_owned());
        }
        identity_cursor_ok &= point.session_id_hash == point.expected_session_id_hash
            && point.cursor == point.expected_cursor;
        let observed_latency_ms =
            point.first_request_ns.saturating_sub(point.resume_start_ns) as f64 / 1_000_000.0;
        if (point.latency_ms - observed_latency_ms).abs() > f64::EPSILON {
            return incomplete(
                "row-52 serialized latency disagrees with its boundaries".to_owned(),
            );
        }
        latencies_by_length
            .entry(point.length)
            .or_default()
            .push(observed_latency_ms);
    }
    for values in latencies_by_length.values_mut() {
        values.sort_by(f64::total_cmp);
    }
    if latencies_by_length
        .values()
        .any(|values| u32::try_from(values.len()).ok() != Some(expected_repetitions))
    {
        return incomplete("row-52 has an incomplete length group".to_owned());
    }
    let medians = expected_lengths
        .iter()
        .map(|length| {
            latencies_by_length
                .get(length)
                .map_or(0.0, |values| median(values))
        })
        .collect::<Vec<_>>();
    let slope = theil_sen_xy(
        &expected_lengths
            .iter()
            .map(|length| *length as f64)
            .collect::<Vec<_>>(),
        &medians,
    );
    let short = medians[0];
    let mid = medians[expected_lengths.len() / 2];
    let long = medians[medians.len() - 1];
    let long_short_ratio = (short > 0.0).then_some(long / short);
    let longest_values = match expected_lengths
        .last()
        .and_then(|length| latencies_by_length.get(length))
    {
        Some(values) => values,
        None => return incomplete("row-52 longest-length group is absent".to_owned()),
    };
    let long_p50 = nearest_rank(longest_values, 50);
    let long_p95 = nearest_rank(longest_values, 95);
    let passed = identity_cursor_ok
        && long_p95 <= 5_000.0
        && slope <= 5.0
        && long_short_ratio.is_some_and(|ratio| ratio <= 2.5);
    let resource_values = BTreeMap::from([
        ("resume_latency_p50_ms".to_owned(), long_p50),
        ("resume_latency_p95_ms".to_owned(), long_p95),
        ("resume_latency_slope_ms_per_turn".to_owned(), slope),
    ]);
    let mut metrics = BTreeMap::from([
        ("resume_latency_vs_length.short_p50_ms".to_owned(), short),
        ("resume_latency_vs_length.mid_p50_ms".to_owned(), mid),
        ("resume_latency_vs_length.long_p50_ms".to_owned(), long),
    ]);
    if let Some(ratio) = long_short_ratio {
        metrics.insert(
            "resume_latency_vs_length.long_short_ratio".to_owned(),
            ratio,
        );
    }
    ResumeLatencyEvaluation {
        resource_values,
        metrics,
        long_short_ratio,
        details: json!({
            "measurement_complete": true,
            "points": points,
            "length_medians_ms": expected_lengths.iter().copied().zip(medians).collect::<Vec<_>>(),
            "long_short_ratio": long_short_ratio,
            "identity_cursor_preserved": identity_cursor_ok,
        }),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// One independently copied, killed, truncated, and recovered journal trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JournalTornTailTrial {
    pub trial: u32,
    pub pre_size: u64,
    pub observed_size: u64,
    pub cut_offset: u64,
    pub truncated_size: u64,
    pub growth_observed_ns: u64,
    pub kill_ns: u64,
    pub record_digest_matches: bool,
    pub recovery_ms: f64,
    pub clean_recovery: bool,
    pub committed_prefix_suffix_exact: bool,
    pub final_record_present_or_absent: bool,
    pub committed_stream_sha256: String,
    pub recovered_stream_sha256: String,
    pub committed_event_count: u32,
    pub recovered_event_count: u32,
    pub post_committed_suffix_events: u32,
    pub corrupt_recoveries: u32,
    pub lost_committed_events: u32,
    pub duplicate_events: u32,
    pub duplicate_effects: u32,
}

/// Exact row-53 result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JournalTornTailEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate the exact pre-size-relative row-53 cut sweep.
pub fn evaluate_journal_torn_tail_sweep(
    trials: &[JournalTornTailTrial],
    expected_trials: u32,
) -> JournalTornTailEvaluation {
    let incomplete = |message: String| JournalTornTailEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..JournalTornTailEvaluation::default()
    };
    if u32::try_from(trials.len()).ok() != Some(expected_trials) {
        return incomplete("row-53 trial set is incomplete".to_owned());
    }
    let mut keys = BTreeSet::new();
    let mut kill_cycles = BTreeSet::new();
    let mut recovery = Vec::new();
    let mut growth_observed_trials = 0_u64;
    let mut clean_recoveries = 0_u64;
    let mut corrupt_recoveries = 0_u64;
    let mut lost_events = 0_u64;
    let mut duplicate_events = 0_u64;
    let mut duplicate_effects = 0_u64;
    let mut passed = true;
    for trial in trials {
        if trial.trial == 0
            || trial.trial > expected_trials
            || !keys.insert(trial.trial)
            || !kill_cycles.insert((trial.growth_observed_ns, trial.kill_ns))
            || !trial.recovery_ms.is_finite()
            || trial.recovery_ms.is_sign_negative()
            || trial.committed_stream_sha256.len() != 64
            || trial.recovered_stream_sha256.len() != 64
            || !trial
                .committed_stream_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !trial
                .recovered_stream_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return incomplete("row-53 has invalid or duplicate trial evidence".to_owned());
        }
        let offset = TORN_CUT_OFFSETS[((trial.trial - 1) as usize) % TORN_CUT_OFFSETS.len()];
        let expected_observed = trial.pre_size.checked_add(TORN_RECORD_BYTES);
        let expected_truncated = trial.pre_size.checked_add(offset);
        if trial.cut_offset != offset
            || Some(trial.observed_size) != expected_observed
            || Some(trial.truncated_size) != expected_truncated
            || trial.growth_observed_ns == 0
            || trial.kill_ns < trial.growth_observed_ns
        {
            return incomplete(
                "row-53 did not observe full growth or truncate to pre_size+cut_offset".to_owned(),
            );
        }
        growth_observed_trials = growth_observed_trials.saturating_add(1);
        clean_recoveries = clean_recoveries.saturating_add(u64::from(trial.clean_recovery));
        corrupt_recoveries = corrupt_recoveries.saturating_add(u64::from(trial.corrupt_recoveries));
        lost_events = lost_events.saturating_add(u64::from(trial.lost_committed_events));
        duplicate_events = duplicate_events.saturating_add(u64::from(trial.duplicate_events));
        duplicate_effects = duplicate_effects.saturating_add(u64::from(trial.duplicate_effects));
        recovery.push(trial.recovery_ms);
        passed &= trial.record_digest_matches
            && trial.recovery_ms <= 10_000.0
            && trial.clean_recovery
            && trial.committed_prefix_suffix_exact
            && trial.final_record_present_or_absent
            && trial.committed_stream_sha256 == trial.recovered_stream_sha256
            && trial.committed_event_count == trial.recovered_event_count
            && trial.post_committed_suffix_events == 0
            && trial.corrupt_recoveries == 0
            && trial.lost_committed_events == 0
            && trial.duplicate_events == 0
            && trial.duplicate_effects == 0;
    }
    JournalTornTailEvaluation {
        metrics: BTreeMap::from([
            (
                "journal_torn_tail_sweep.trials".to_owned(),
                expected_trials as f64,
            ),
            (
                "journal_torn_tail_sweep.kill_after_growth_observed_trials".to_owned(),
                growth_observed_trials as f64,
            ),
            (
                "journal_torn_tail_sweep.clean_recoveries".to_owned(),
                clean_recoveries as f64,
            ),
            (
                "journal_torn_tail_sweep.corrupt_recoveries".to_owned(),
                corrupt_recoveries as f64,
            ),
            (
                "journal_torn_tail_sweep.lost_committed_events".to_owned(),
                lost_events as f64,
            ),
            (
                "journal_torn_tail_sweep.duplicate_events".to_owned(),
                duplicate_events as f64,
            ),
            (
                "journal_torn_tail_sweep.duplicate_effects".to_owned(),
                duplicate_effects as f64,
            ),
            (
                "journal_torn_tail_sweep.recovery_p95_ms".to_owned(),
                nearest_rank(&recovery, 95),
            ),
        ]),
        details: json!({
            "measurement_complete": true,
            "cut_positions": trials,
        }),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

fn theil_sen_xy(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }
    let mut slopes = Vec::new();
    for left in 0..x.len() - 1 {
        for right in left + 1..x.len() {
            let denominator = x[right] - x[left];
            if denominator != 0.0 {
                slopes.push((y[right] - y[left]) / denominator);
            }
        }
    }
    median(&slopes)
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn nearest_rank(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resume_points(
        latencies_ms: &[f64],
        lengths: &[u32],
        repetitions: u32,
    ) -> Vec<ResumeLatencyPoint> {
        lengths
            .iter()
            .enumerate()
            .flat_map(|(index, length)| {
                (1..=repetitions).map(move |repetition| {
                    let start = 1_000_000_000_u64
                        .saturating_add(u64::from(*length) * 1_000_000)
                        .saturating_add(u64::from(repetition));
                    ResumeLatencyPoint {
                        length: *length,
                        repetition,
                        resume_start_ns: start,
                        first_request_ns: start
                            .saturating_add((latencies_ms[index] * 1_000_000.0) as u64),
                        latency_ms: latencies_ms[index],
                        session_id_hash: format!("session-{length}-{repetition}"),
                        expected_session_id_hash: format!("session-{length}-{repetition}"),
                        cursor: u64::from(*length),
                        expected_cursor: u64::from(*length),
                    }
                })
            })
            .collect()
    }

    #[test]
    fn fake_context_tokenizer_is_exact_and_ignores_keys() {
        let request = json!({
            "messages": [
                {"role":"user","content":"aa bb,café"},
                {"key_that_is_not_counted":"x_y"}
            ],
            "tools": null,
            "model": "also-not-counted"
        });
        // user + aa + bb + comma + caf + é + x_y
        assert_eq!(
            fake_context_input_tokens("open-ai-chat-completions", &request),
            7
        );
    }

    #[test]
    fn resume_latency_passes_and_zero_short_denominator_fails_with_null_ratio() {
        let lengths = [1, 10, 50];
        let passing = evaluate_resume_latency_vs_length(
            &resume_points(&[10.0, 12.0, 14.0], &lengths, 3),
            &lengths,
            3,
        );
        assert!(passing.measurement_complete);
        assert!(passing.passed);
        assert!(passing.long_short_ratio.is_some());

        let zero = evaluate_resume_latency_vs_length(
            &resume_points(&[0.0, 1.0, 2.0], &lengths, 3),
            &lengths,
            3,
        );
        assert!(zero.measurement_complete);
        assert!(!zero.passed);
        assert_eq!(zero.long_short_ratio, None);
        assert!(
            !zero
                .metrics
                .contains_key("resume_latency_vs_length.long_short_ratio")
        );
        assert!(zero.details["long_short_ratio"].is_null());
    }

    #[test]
    fn resume_latency_detects_linear_blowup() {
        let lengths = [1, 10, 50];
        let evaluation = evaluate_resume_latency_vs_length(
            &resume_points(&[10.0, 100.0, 600.0], &lengths, 3),
            &lengths,
            3,
        );
        assert!(evaluation.measurement_complete);
        assert!(!evaluation.passed);
        assert!(evaluation.resource_values["resume_latency_slope_ms_per_turn"] > 5.0);
    }

    fn torn_trial(trial: u32) -> JournalTornTailTrial {
        let pre_size = 128_u64;
        let cut_offset = TORN_CUT_OFFSETS[((trial - 1) as usize) % TORN_CUT_OFFSETS.len()];
        JournalTornTailTrial {
            trial,
            pre_size,
            observed_size: pre_size + TORN_RECORD_BYTES,
            cut_offset,
            truncated_size: pre_size + cut_offset,
            growth_observed_ns: 100 + u64::from(trial).saturating_mul(2),
            kill_ns: 101 + u64::from(trial).saturating_mul(2),
            record_digest_matches: true,
            recovery_ms: 10.0,
            clean_recovery: true,
            committed_prefix_suffix_exact: true,
            final_record_present_or_absent: true,
            committed_stream_sha256: "a".repeat(64),
            recovered_stream_sha256: "a".repeat(64),
            committed_event_count: 1,
            recovered_event_count: 1,
            post_committed_suffix_events: 0,
            corrupt_recoveries: 0,
            lost_committed_events: 0,
            duplicate_events: 0,
            duplicate_effects: 0,
        }
    }

    #[test]
    fn torn_tail_requires_exact_pre_size_relative_cut() {
        let trials = (1..=5).map(torn_trial).collect::<Vec<_>>();
        let passing = evaluate_journal_torn_tail_sweep(&trials, 5);
        assert!(passing.measurement_complete);
        assert!(passing.passed);

        let mut wrong = trials;
        wrong[2].truncated_size = wrong[2].truncated_size.saturating_sub(1);
        let incomplete = evaluate_journal_torn_tail_sweep(&wrong, 5);
        assert!(!incomplete.measurement_complete);

        let mut reused_kill = (1..=5).map(torn_trial).collect::<Vec<_>>();
        reused_kill[1].growth_observed_ns = reused_kill[0].growth_observed_ns;
        reused_kill[1].kill_ns = reused_kill[0].kill_ns;
        assert!(!evaluate_journal_torn_tail_sweep(&reused_kill, 5).measurement_complete);
    }

    #[test]
    fn context_recovery_preserves_all_pairing_and_is_deterministic() {
        let trials = (1..=3)
            .map(|repetition| {
                let pairs = (1..=4)
                    .map(|ordinal| ContextToolPairRecord {
                        call_id: format!("call-{ordinal}"),
                        function_name: "write_fixture".to_owned(),
                        canonical_arguments: json!({"ordinal":ordinal}),
                        result_content: Value::String(format!("result-{ordinal}")),
                    })
                    .collect::<Vec<_>>();
                ContextRecoveryTrial {
                    repetition,
                    window_tokens: 4_096,
                    public_turn_start_ns: 1,
                    error_final_byte_ns: 2_000_001,
                    accepted_request_received_ns: 3_000_001,
                    terminal_received_ns: 4_000_001,
                    pre_error_input_tokens: 4_352,
                    pre_error_body_bytes: 33_792,
                    accepted_input_tokens: 3_000,
                    accepted_body_bytes: 20_000,
                    context_errors: 1,
                    extra_requests: 1,
                    terminal_success: true,
                    structural_terminal_count: 1,
                    tool_pairs_before: 4,
                    tool_pairs_after: 4,
                    committed_tool_pairs: pairs.clone(),
                    faulting_tool_pairs: pairs.clone(),
                    accepted_tool_pairs: pairs,
                    faulting_goal_items: vec![ContextGoalItemRecord {
                        role: "user".to_owned(),
                        content: "AHRB-GOAL-MARKER recover".to_owned(),
                    }],
                    accepted_goal_items: vec![ContextGoalItemRecord {
                        role: "user".to_owned(),
                        content: "AHRB-GOAL-MARKER recover".to_owned(),
                    }],
                    committed_orphan_tool_calls: 0,
                    committed_orphan_tool_results: 0,
                    faulting_orphan_tool_calls: 0,
                    faulting_orphan_tool_results: 0,
                    accepted_orphan_tool_calls: 0,
                    accepted_orphan_tool_results: 0,
                    orphan_tool_calls: 0,
                    orphan_tool_results: 0,
                    duplicate_effects: 0,
                    same_session_identity: true,
                    instructions_and_tools_unchanged: true,
                    required_markers_retained: true,
                    omitted_markers_summarized_exactly: true,
                    compacted_request_hash: "a".repeat(64),
                    retained_markers: vec!["goal".to_owned()],
                    omitted_markers: vec!["history-1".to_owned()],
                    summary_markers: vec!["history-1".to_owned()],
                }
            })
            .collect::<Vec<_>>();
        let evaluation = evaluate_context_limit_recovery(&trials, 3, 4_096, 4, 10_000);
        assert!(evaluation.measurement_complete);
        assert!(evaluation.passed);

        let mut reordered = trials.clone();
        reordered[0].accepted_tool_pairs.swap(0, 1);
        let rejected = evaluate_context_limit_recovery(&reordered, 3, 4_096, 4, 10_000);
        assert!(rejected.measurement_complete);
        assert!(!rejected.passed);

        let mut faulting_orphan = trials.clone();
        faulting_orphan[0].faulting_orphan_tool_calls = 1;
        faulting_orphan[0].orphan_tool_calls = 1;
        let rejected = evaluate_context_limit_recovery(&faulting_orphan, 3, 4_096, 4, 10_000);
        assert!(rejected.measurement_complete);
        assert!(!rejected.passed);

        let mut mutated_goal = trials.clone();
        mutated_goal[0].accepted_goal_items[0]
            .content
            .push_str(" mutated");
        assert!(!evaluate_context_limit_recovery(&mutated_goal, 3, 4_096, 4, 10_000).passed);

        let mut duplicated_goal = trials.clone();
        let duplicate = duplicated_goal[0].accepted_goal_items[0].clone();
        duplicated_goal[0].accepted_goal_items.push(duplicate);
        assert!(!evaluate_context_limit_recovery(&duplicated_goal, 3, 4_096, 4, 10_000).passed);

        let mut wrong_role_goal = trials.clone();
        wrong_role_goal[0].faulting_goal_items[0].role = "system".to_owned();
        wrong_role_goal[0].accepted_goal_items[0].role = "system".to_owned();
        assert!(!evaluate_context_limit_recovery(&wrong_role_goal, 3, 4_096, 4, 10_000).passed);

        let mut contradictory_terminal = trials;
        contradictory_terminal[0].structural_terminal_count = 2;
        assert!(contradictory_terminal[0].terminal_success);
        assert!(
            !evaluate_context_limit_recovery(&contradictory_terminal, 3, 4_096, 4, 10_000).passed
        );
    }

    #[test]
    fn session_residue_flags_a_slow_store_leak() {
        let sweeps = (1..=3)
            .map(|repetition| SessionResidueSweep {
                repetition,
                created_sessions: 20,
                closed_sessions: 20,
                unretired_sessions: 0,
                maximum_active_delta_mib: 8.0,
                final_memory_residue_mib: 0.0,
                process_audits: Vec::new(),
                checkpoints: [0_u32, 10, 20]
                    .into_iter()
                    .map(|sessions_created| SessionResidueCheckpoint {
                        repetition,
                        sessions_created,
                        memory_residue_mib: 0.0,
                        fd_delta: 0.0,
                        thread_delta: 0.0,
                        process_delta: 0.0,
                        store_residue_bytes: u64::from(sessions_created) * 40_000,
                        store_residue_files: u64::from(sessions_created),
                        close_delete_validated_through: sessions_created,
                        traversal_valid: true,
                        store_entries: Vec::new(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let evaluation = evaluate_session_residue_sweep(&sweeps, 3, 20, false);
        assert!(evaluation.measurement_complete);
        assert!(!evaluation.passed);
        assert!(evaluation.resource_values["session_store_byte_slope_per_session"] > 0.0);
        assert!(
            !evaluation
                .resource_values
                .contains_key("session_store_final_residue_files")
        );

        let missing_process_evidence = evaluate_session_residue_sweep(&sweeps, 3, 20, true);
        assert!(!missing_process_evidence.measurement_complete);
        assert!(
            missing_process_evidence
                .measurement_error
                .as_deref()
                .is_some_and(|message| message.contains("process audits"))
        );

        let mut measured_residue = sweeps;
        for sweep in &mut measured_residue {
            for checkpoint in &mut sweep.checkpoints {
                checkpoint.store_residue_bytes = 0;
                checkpoint.store_residue_files = 0;
            }
            sweep.process_audits = (1..=20)
                .map(|session_ordinal| SessionResidueProcessAudit {
                    repetition: sweep.repetition,
                    session_ordinal,
                    invocation_sample_count: 1,
                    invocation_peak_memory_bytes: 1,
                    invocation_peak_open_fds: 1,
                    invocation_peak_threads: 1,
                    invocation_peak_processes: 1,
                    invocation_residue_memory_bytes: 0,
                    invocation_residue_open_fds: 0,
                    invocation_residue_threads: 0,
                    invocation_residue_processes: 0,
                    invocation_reclaim_validated: true,
                    close_delete_sample_count: 1,
                    close_delete_peak_memory_bytes: 1,
                    close_delete_peak_open_fds: 1,
                    close_delete_peak_threads: 1,
                    close_delete_peak_processes: 1,
                    close_delete_residue_memory_bytes: 0,
                    close_delete_residue_open_fds: 0,
                    close_delete_residue_threads: 0,
                    close_delete_residue_processes: 0,
                    close_delete_reclaim_validated: true,
                    close_delete_result_validated: true,
                })
                .collect();
        }
        measured_residue[0].process_audits[0].invocation_residue_processes = 1;
        let measured_failure = evaluate_session_residue_sweep(&measured_residue, 3, 20, true);
        assert!(measured_failure.measurement_complete);
        assert!(!measured_failure.passed);
        assert_eq!(
            measured_failure.metrics["session_residue_sweep.unretired_sessions"],
            1.0
        );
    }
}
