//! Wave-2 supervision and offline-confinement oracles.
//!
//! Inputs are external process, driver, and OS-enforcement observations.  The
//! evaluators deliberately distinguish untrustworthy infrastructure evidence
//! from complete observations of harness behavior.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// A child failure-propagation subcase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChildFailureCase {
    Crash,
    Hang,
}

/// One complete parent/child observation for row 56.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChildFailureTrial {
    pub repetition: u32,
    pub case: ChildFailureCase,
    pub parent_operation_start_ns: u64,
    pub child_failure_received_ns: Option<u64>,
    pub child_response_headers_ns: Option<u64>,
    pub parent_terminal_received_ns: u64,
    pub parent_failure_terminals: u32,
    pub child_failure_terminals: u32,
    pub child_cancelled: bool,
    pub success_contradiction: bool,
    pub child_residue_count: u32,
    pub outer_kill_used: bool,
}

/// Row-56 result and exact metric/detail maps.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChildFailureEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate crash and hang propagation relative to their distinct origins.
pub fn evaluate_child_failure_propagation(
    trials: &[ChildFailureTrial],
    expected_repetitions: u32,
    turn_timeout_ms: u64,
) -> ChildFailureEvaluation {
    let incomplete = |message: String| ChildFailureEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..ChildFailureEvaluation::default()
    };
    let expected = u64::from(expected_repetitions).saturating_mul(2);
    if u64::try_from(trials.len()).ok() != Some(expected) {
        return incomplete("child-failure trial set is incomplete".to_owned());
    }
    let mut keys = BTreeSet::new();
    for trial in trials {
        if trial.repetition == 0
            || trial.repetition > expected_repetitions
            || !keys.insert((trial.repetition, trial.case as u8))
            || trial.parent_terminal_received_ns < trial.parent_operation_start_ns
        {
            return incomplete(
                "child-failure boundaries or repetition keys are invalid".to_owned(),
            );
        }
        match trial.case {
            ChildFailureCase::Crash => {
                let Some(child_failure_ns) = trial.child_failure_received_ns else {
                    return incomplete("crash trial omitted child failure receipt".to_owned());
                };
                if child_failure_ns > trial.parent_terminal_received_ns {
                    return incomplete(
                        "crash child/parent receipt ordering is contradictory".to_owned(),
                    );
                }
            }
            ChildFailureCase::Hang => {
                let Some(headers_ns) = trial.child_response_headers_ns else {
                    return incomplete("hang trial omitted response-header boundary".to_owned());
                };
                if headers_ns < trial.parent_operation_start_ns
                    || headers_ns >= trial.parent_terminal_received_ns
                {
                    return incomplete("hang response-header boundary is contradictory".to_owned());
                }
            }
        }
    }
    let elapsed_ms = |start: u64, end: u64| end.saturating_sub(start) as f64 / 1_000_000.0;
    let mut crash_max = 0.0_f64;
    let mut hang_max = 0.0_f64;
    let mut crash_terminals = 0_u64;
    let mut hang_terminals = 0_u64;
    let mut child_terminals = 0_u64;
    let mut residues = 0_u64;
    let mut outer_kills = 0_u64;
    let mut hang_deadlines = 0_u64;
    let mut passed = true;
    for trial in trials {
        child_terminals = child_terminals.saturating_add(u64::from(trial.child_failure_terminals));
        residues = residues.saturating_add(u64::from(trial.child_residue_count));
        outer_kills = outer_kills.saturating_add(u64::from(trial.outer_kill_used));
        match trial.case {
            ChildFailureCase::Crash => {
                let child_failure_ns = match trial.child_failure_received_ns {
                    Some(value) => value,
                    None => continue,
                };
                let latency = elapsed_ms(child_failure_ns, trial.parent_terminal_received_ns);
                crash_max = crash_max.max(latency);
                crash_terminals =
                    crash_terminals.saturating_add(u64::from(trial.parent_failure_terminals));
                passed &= trial.child_failure_terminals == 1
                    && trial.parent_failure_terminals == 1
                    && latency <= (turn_timeout_ms.min(5_000)) as f64
                    && !trial.success_contradiction
                    && trial.child_residue_count == 0
                    && !trial.outer_kill_used;
            }
            ChildFailureCase::Hang => {
                let latency = elapsed_ms(
                    trial.parent_operation_start_ns,
                    trial.parent_terminal_received_ns,
                );
                hang_max = hang_max.max(latency);
                hang_terminals =
                    hang_terminals.saturating_add(u64::from(trial.parent_failure_terminals));
                let deadline_fired = latency >= turn_timeout_ms as f64
                    && latency <= turn_timeout_ms.saturating_add(1_000) as f64;
                hang_deadlines = hang_deadlines.saturating_add(u64::from(deadline_fired));
                passed &= deadline_fired
                    && latency < turn_timeout_ms.saturating_add(2_000) as f64
                    && trial.parent_failure_terminals == 1
                    && trial.child_cancelled
                    && trial.child_residue_count == 0
                    && !trial.success_contradiction
                    && !trial.outer_kill_used;
            }
        }
    }
    ChildFailureEvaluation {
        metrics: BTreeMap::from([
            (
                "child_failure_propagation.crash_parent_terminal_ms".to_owned(),
                crash_max,
            ),
            (
                "child_failure_propagation.hang_parent_terminal_ms".to_owned(),
                hang_max,
            ),
            (
                "child_failure_propagation.crash_parent_failure_terminals".to_owned(),
                crash_terminals as f64,
            ),
            (
                "child_failure_propagation.hang_parent_failure_terminals".to_owned(),
                hang_terminals as f64,
            ),
            (
                "child_failure_propagation.hang_deadline_fired".to_owned(),
                hang_deadlines as f64,
            ),
            (
                "child_failure_propagation.child_terminal_count".to_owned(),
                child_terminals as f64,
            ),
            (
                "child_failure_propagation.child_residue_count".to_owned(),
                residues as f64,
            ),
            (
                "child_failure_propagation.outer_kill_used".to_owned(),
                outer_kills as f64,
            ),
        ]),
        details: json!({"measurement_complete":true,"trials":trials}),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// One signal/EOF case for row 57.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SignalCaseTrial {
    pub repetition: u32,
    #[serde(rename = "signal")]
    pub case: String,
    pub applicable: bool,
    pub not_applicable_reason: Option<String>,
    pub delivery_succeeded: Option<bool>,
    pub ownership_resolved: Option<bool>,
    pub origin_ns: Option<u64>,
    pub terminal_ns: Option<u64>,
    pub terminal_type: Option<String>,
    pub terminal_count: Option<u32>,
    pub exit_code: Option<i32>,
    pub exit_was_signal: Option<bool>,
    pub residue_processes: Option<u32>,
}

/// Row-57 result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SignalMatrixEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate signal delivery only after ownership and boundary proof is complete.
pub fn evaluate_signal_matrix(
    trials: &[SignalCaseTrial],
    expected_repetitions: u32,
    grace_ms: u64,
    outer_deadline_ms: u64,
) -> SignalMatrixEvaluation {
    let incomplete = |message: String| SignalMatrixEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..SignalMatrixEvaluation::default()
    };
    if grace_ms.saturating_add(250) >= outer_deadline_ms {
        return incomplete(
            "signal grace plus tolerance is not below the outer deadline".to_owned(),
        );
    }
    let required = ["sigterm", "sigint2", "sighup"];
    for case in required {
        let count = trials.iter().filter(|trial| trial.case == case).count();
        if u32::try_from(count).ok() != Some(expected_repetitions) {
            return incomplete(format!("signal case {case} has incomplete repetitions"));
        }
    }
    let eof_count = trials
        .iter()
        .filter(|trial| trial.case == "stdin-eof")
        .count();
    if u32::try_from(eof_count).ok() != Some(expected_repetitions) {
        return incomplete("stdin-eof case has incomplete repetitions".to_owned());
    }
    for trial in trials.iter().filter(|trial| trial.applicable) {
        if trial.ownership_resolved != Some(true) || trial.delivery_succeeded != Some(true) {
            return incomplete(format!(
                "{} delivery/ownership evidence is incomplete",
                trial.case
            ));
        }
        let (Some(origin_ns), Some(terminal_ns)) = (trial.origin_ns, trial.terminal_ns) else {
            return incomplete(format!("{} origin/terminal boundary is absent", trial.case));
        };
        if terminal_ns < origin_ns {
            return incomplete(format!("{} terminal precedes delivery origin", trial.case));
        }
        if trial.terminal_type.is_none() || trial.terminal_count.is_none() {
            return incomplete(format!(
                "{} structured terminal boundary is absent",
                trial.case
            ));
        }
        if trial.exit_code.is_none() && trial.exit_was_signal.is_none() {
            return incomplete(format!("{} process exit boundary is absent", trial.case));
        }
        if trial.residue_processes.is_none() {
            return incomplete(format!("{} residue observation is absent", trial.case));
        }
    }
    for trial in trials.iter().filter(|trial| !trial.applicable) {
        if trial.case != "stdin-eof"
            || trial.not_applicable_reason.is_none()
            || trial.origin_ns.is_some()
            || trial.terminal_ns.is_some()
            || trial.delivery_succeeded.is_some()
            || trial.ownership_resolved.is_some()
            || trial.terminal_count.is_some()
            || trial.exit_was_signal.is_some()
            || trial.residue_processes.is_some()
        {
            return incomplete(
                "only stdin-eof may be not_applicable with a typed reason".to_owned(),
            );
        }
    }
    let mut metrics = BTreeMap::new();
    let mut applicable_cases = 0_u64;
    let mut passed_cases = 0_u64;
    let mut applicable_kinds = 0_u64;
    let mut passed = true;
    for case in ["sigterm", "sigint2", "sighup", "stdin-eof"] {
        let applicable = trials
            .iter()
            .filter(|trial| trial.case == case && trial.applicable)
            .collect::<Vec<_>>();
        if applicable.is_empty() {
            continue;
        }
        if u32::try_from(applicable.len()).ok() != Some(expected_repetitions) {
            return incomplete(format!("signal case {case} has inconsistent applicability"));
        }
        applicable_kinds = applicable_kinds.saturating_add(1);
        applicable_cases = applicable_cases
            .saturating_add(u64::try_from(applicable.len()).map_or(u64::MAX, |value| value));
        let latency = applicable
            .iter()
            .filter_map(|trial| trial.origin_ns.zip(trial.terminal_ns))
            .map(|(origin, terminal)| terminal.saturating_sub(origin) as f64 / 1_000_000.0)
            .fold(0.0_f64, f64::max);
        let residue = applicable
            .iter()
            .filter_map(|trial| trial.residue_processes)
            .max()
            .map_or(0.0, f64::from);
        let passing_trials = applicable.iter().filter(|trial| {
            matches!(
                trial.terminal_type.as_deref(),
                Some("failure" | "cancelled")
            ) && trial.terminal_count == Some(1)
                && trial.exit_was_signal == Some(false)
                && trial.exit_code.is_some()
                && trial.residue_processes == Some(0)
                && trial
                    .origin_ns
                    .zip(trial.terminal_ns)
                    .is_some_and(|(origin, terminal)| {
                        terminal.saturating_sub(origin) as f64 / 1_000_000.0
                            <= grace_ms.saturating_add(250) as f64
                    })
        });
        let passed_trial_count =
            u64::try_from(passing_trials.count()).map_or(u64::MAX, |value| value);
        let case_passed =
            passed_trial_count == u64::try_from(applicable.len()).map_or(u64::MAX, |value| value);
        passed &= case_passed;
        passed_cases = passed_cases.saturating_add(passed_trial_count);
        metrics.insert(
            format!("signal_matrix.{case}_terminal_ms").replace('-', "_"),
            latency,
        );
        metrics.insert(
            format!("signal_matrix.{case}_residue_processes").replace('-', "_"),
            residue,
        );
    }
    passed &= applicable_kinds >= 3;
    metrics.insert(
        "signal_matrix.applicable_cases".to_owned(),
        applicable_cases as f64,
    );
    metrics.insert("signal_matrix.passed_cases".to_owned(), passed_cases as f64);
    SignalMatrixEvaluation {
        metrics,
        details: json!({"measurement_complete":true,"cases":trials}),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// One externally observed connection attempt for row 62.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OfflineAttempt {
    pub repetition: u32,
    pub destination: String,
    pub category: String,
    pub outcome: String,
    pub allowed: bool,
    pub confinement_identity: String,
}

/// One complete offline run and same-confinement probe.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OfflineTrial {
    pub repetition: u32,
    pub provider_requests: u64,
    pub terminal_success: bool,
    pub control_probe_blocked: bool,
    pub harness_confinement_identity: String,
    pub probe_confinement_identity: String,
    pub egress_enforcement: String,
    pub attempts: Vec<OfflineAttempt>,
}

/// Row-62 PASS/ERROR-only evaluation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OfflineModeEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate reviewed same-confinement evidence. An observed escape invalidates
/// the guard and is therefore an infrastructure error rather than harness FAIL.
pub fn evaluate_offline_mode(
    trials: &[OfflineTrial],
    expected_repetitions: u32,
) -> OfflineModeEvaluation {
    let incomplete = |message: String| OfflineModeEvaluation {
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_error: Some(message),
        ..OfflineModeEvaluation::default()
    };
    if u32::try_from(trials.len()).ok() != Some(expected_repetitions) {
        return incomplete("egress enforcement unavailable: incomplete trials".to_owned());
    }
    let categories = [
        "update-check",
        "model-catalog",
        "telemetry",
        "other",
        "control-probe",
    ];
    for trial in trials {
        if trial.harness_confinement_identity.is_empty()
            || trial.harness_confinement_identity != trial.probe_confinement_identity
            || trial.egress_enforcement.is_empty()
            || !trial.control_probe_blocked
            || trial.provider_requests == 0
        {
            return incomplete(
                "egress enforcement unavailable: same-confinement proof failed".to_owned(),
            );
        }
        if !trial.terminal_success {
            return incomplete(
                "egress enforcement unavailable: offline workflow did not complete successfully"
                    .to_owned(),
            );
        }
        if trial.attempts.iter().any(|attempt| {
            !categories.contains(&attempt.category.as_str())
                || attempt.confinement_identity != trial.harness_confinement_identity
                || attempt.allowed
        }) {
            return incomplete(
                "egress enforcement unavailable: guard observation escaped or is unclassified"
                    .to_owned(),
            );
        }
    }
    let provider_requests = trials
        .iter()
        .map(|trial| trial.provider_requests)
        .sum::<u64>();
    let blocked = trials
        .iter()
        .flat_map(|trial| &trial.attempts)
        .filter(|attempt| !attempt.allowed && attempt.category != "control-probe")
        .count();
    let offline_success = true;
    let attempts = trials
        .iter()
        .flat_map(|trial| trial.attempts.iter().cloned())
        .collect::<Vec<_>>();
    let mut blocked_by_category = BTreeMap::<String, u64>::new();
    for category in categories {
        let count = attempts
            .iter()
            .filter(|attempt| !attempt.allowed && attempt.category == category)
            .count();
        blocked_by_category.insert(category.to_owned(), count as u64);
    }
    OfflineModeEvaluation {
        metrics: BTreeMap::from([
            (
                "offline_mode.provider_requests".to_owned(),
                provider_requests as f64,
            ),
            (
                "offline_mode.blocked_egress_attempts".to_owned(),
                blocked as f64,
            ),
            (
                "offline_mode.successful_non_provider_connections".to_owned(),
                attempts.iter().filter(|attempt| attempt.allowed).count() as f64,
            ),
            (
                "offline_mode.offline_run_success".to_owned(),
                if offline_success { 1.0 } else { 0.0 },
            ),
            ("offline_mode.control_probe_blocked".to_owned(), 1.0),
        ]),
        details: json!({
            "measurement_complete":true,
            "egress_enforcement":trials.first().map(|trial|trial.egress_enforcement.as_str()),
            "confinement_identity":trials.first().map(|trial|trial.harness_confinement_identity.as_str()),
            "attempts_total":attempts.len(),
            "blocked_by_category":blocked_by_category,
            "attempts":attempts,
        }),
        measurement_complete: true,
        passed: offline_success,
        measurement_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hang_deadline_is_measured_from_parent_operation_start() {
        let trials = vec![
            ChildFailureTrial {
                repetition: 1,
                case: ChildFailureCase::Crash,
                parent_operation_start_ns: 1,
                child_failure_received_ns: Some(2_000_000),
                child_response_headers_ns: None,
                parent_terminal_received_ns: 3_000_000,
                parent_failure_terminals: 1,
                child_failure_terminals: 1,
                child_cancelled: false,
                success_contradiction: false,
                child_residue_count: 0,
                outer_kill_used: false,
            },
            ChildFailureTrial {
                repetition: 1,
                case: ChildFailureCase::Hang,
                parent_operation_start_ns: 1_000_000,
                child_failure_received_ns: None,
                child_response_headers_ns: Some(9_000_000_000),
                parent_terminal_received_ns: 10_001_000_000,
                parent_failure_terminals: 1,
                child_failure_terminals: 0,
                child_cancelled: true,
                success_contradiction: false,
                child_residue_count: 0,
                outer_kill_used: false,
            },
        ];
        let evaluated = evaluate_child_failure_propagation(&trials, 1, 10_000);
        assert!(evaluated.measurement_complete);
        assert!(evaluated.passed);
    }

    #[test]
    fn signal_matrix_counts_trials_and_omits_not_applicable_eof_metrics() {
        let signal_trial = |case: &str| SignalCaseTrial {
            repetition: 1,
            case: case.to_owned(),
            applicable: true,
            not_applicable_reason: None,
            delivery_succeeded: Some(true),
            ownership_resolved: Some(true),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue_processes: Some(0),
        };
        let trials = vec![
            signal_trial("sigterm"),
            signal_trial("sigint2"),
            signal_trial("sighup"),
            SignalCaseTrial {
                repetition: 1,
                case: "stdin-eof".to_owned(),
                applicable: false,
                not_applicable_reason: Some(
                    "typed prompt input is false and process stdin is /dev/null".to_owned(),
                ),
                delivery_succeeded: None,
                ownership_resolved: None,
                origin_ns: None,
                terminal_ns: None,
                terminal_type: None,
                terminal_count: None,
                exit_code: None,
                exit_was_signal: None,
                residue_processes: None,
            },
        ];
        let evaluated = evaluate_signal_matrix(&trials, 1, 2_000, 10_000);
        assert!(evaluated.measurement_complete);
        assert!(evaluated.passed);
        assert_eq!(evaluated.metrics["signal_matrix.applicable_cases"], 3.0);
        assert_eq!(evaluated.metrics["signal_matrix.passed_cases"], 3.0);
        assert!(
            !evaluated
                .metrics
                .contains_key("signal_matrix.stdin_eof_terminal_ms")
        );
        assert!(evaluated.details["cases"][3]["residue_processes"].is_null());
        assert!(evaluated.details["cases"][3]["terminal_count"].is_null());
        assert!(evaluated.details["cases"][3]["exit_was_signal"].is_null());
        assert_eq!(evaluated.details["cases"][0]["signal"], "sigterm");
    }

    #[test]
    fn signal_matrix_missing_exit_boundary_is_infrastructure_error() {
        let evaluated = evaluate_signal_matrix(
            &[
                SignalCaseTrial {
                    repetition: 1,
                    case: "sigterm".to_owned(),
                    applicable: true,
                    not_applicable_reason: None,
                    delivery_succeeded: Some(true),
                    ownership_resolved: Some(true),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    exit_code: None,
                    exit_was_signal: None,
                    residue_processes: Some(0),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "sigint2".to_owned(),
                    applicable: true,
                    not_applicable_reason: None,
                    delivery_succeeded: Some(true),
                    ownership_resolved: Some(true),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    exit_code: Some(0),
                    exit_was_signal: Some(false),
                    residue_processes: Some(0),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "sighup".to_owned(),
                    applicable: true,
                    not_applicable_reason: None,
                    delivery_succeeded: Some(true),
                    ownership_resolved: Some(true),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    exit_code: Some(0),
                    exit_was_signal: Some(false),
                    residue_processes: Some(0),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "stdin-eof".to_owned(),
                    applicable: false,
                    not_applicable_reason: Some("typed non-control stdin".to_owned()),
                    delivery_succeeded: None,
                    ownership_resolved: None,
                    origin_ns: None,
                    terminal_ns: None,
                    terminal_type: None,
                    terminal_count: None,
                    exit_code: None,
                    exit_was_signal: None,
                    residue_processes: None,
                },
            ],
            1,
            2_000,
            10_000,
        );
        assert!(!evaluated.measurement_complete);
        assert!(
            evaluated
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("exit boundary"))
        );
    }

    #[test]
    fn offline_escape_is_error_not_fail() {
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 1,
                terminal_success: true,
                control_probe_blocked: true,
                harness_confinement_identity: "guard-1".to_owned(),
                probe_confinement_identity: "guard-1".to_owned(),
                egress_enforcement: "reviewed-test-guard".to_owned(),
                attempts: vec![OfflineAttempt {
                    repetition: 1,
                    destination: "forbidden".to_owned(),
                    category: "control-probe".to_owned(),
                    outcome: "connected".to_owned(),
                    allowed: true,
                    confinement_identity: "guard-1".to_owned(),
                }],
            }],
            1,
        );
        assert!(!evaluated.measurement_complete);
        assert!(!evaluated.passed);
    }

    #[test]
    fn offline_owned_boundary_with_refused_real_probe_passes() {
        let identity = "owned-reference-mock-v1:sha256:test".to_owned();
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 2,
                terminal_success: true,
                control_probe_blocked: true,
                harness_confinement_identity: identity.clone(),
                probe_confinement_identity: identity.clone(),
                egress_enforcement: "reviewed reference-mock owned loopback connector".to_owned(),
                attempts: vec![OfflineAttempt {
                    repetition: 1,
                    destination: "203.0.113.1:9".to_owned(),
                    category: "control-probe".to_owned(),
                    outcome: "blocked-permission-denied".to_owned(),
                    allowed: false,
                    confinement_identity: identity,
                }],
            }],
            1,
        );
        assert!(evaluated.measurement_complete);
        assert!(evaluated.passed);
        assert_eq!(evaluated.metrics["offline_mode.provider_requests"], 2.0);
        assert_eq!(evaluated.metrics["offline_mode.control_probe_blocked"], 1.0);
    }

    #[test]
    fn signal_matrix_complete_behavioral_violation_is_fail() {
        let trial = |case: &str| SignalCaseTrial {
            repetition: 1,
            case: case.to_owned(),
            applicable: true,
            not_applicable_reason: None,
            delivery_succeeded: Some(true),
            ownership_resolved: Some(true),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue_processes: Some(0),
        };
        let mut trials = vec![
            trial("sigterm"),
            trial("sigint2"),
            trial("sighup"),
            trial("stdin-eof"),
        ];
        trials[1].terminal_count = Some(2);
        let evaluated = evaluate_signal_matrix(&trials, 1, 2_000, 10_000);
        assert!(evaluated.measurement_complete);
        assert!(!evaluated.passed);
        assert!(evaluated.measurement_error.is_none());
    }

    #[test]
    fn offline_workflow_failure_is_error_not_fail() {
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 1,
                terminal_success: false,
                control_probe_blocked: true,
                harness_confinement_identity: "guard-1".to_owned(),
                probe_confinement_identity: "guard-1".to_owned(),
                egress_enforcement: "reviewed-test-guard".to_owned(),
                attempts: vec![OfflineAttempt {
                    repetition: 1,
                    destination: "forbidden".to_owned(),
                    category: "control-probe".to_owned(),
                    outcome: "blocked".to_owned(),
                    allowed: false,
                    confinement_identity: "guard-1".to_owned(),
                }],
            }],
            1,
        );
        assert!(!evaluated.measurement_complete);
        assert!(!evaluated.passed);
        assert!(
            evaluated
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("workflow did not complete"))
        );
    }
}
