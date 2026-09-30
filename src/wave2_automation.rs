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
    pub cleanup_escalated: Option<bool>,
    pub origin_ns: Option<u64>,
    pub terminal_ns: Option<u64>,
    pub terminal_type: Option<String>,
    pub terminal_count: Option<u32>,
    /// Terminals normalized from harness output rather than synthesized exits.
    pub source_terminal_count: Option<u32>,
    pub exit_code: Option<i32>,
    pub exit_was_signal: Option<bool>,
    /// Unexpected owned processes at the fixed two-second observation. A
    /// declared daemon with the same stable identity is excluded here.
    pub residue: Option<SignalResidueObservation>,
    /// Observation of the manifest-declared daemon idle-linger contract.
    pub declared_daemon_linger: Option<SignalDaemonLingerObservation>,
    /// Owned processes still live at the end of the applicable measurement.
    pub residue_processes: Option<u32>,
    #[serde(default)]
    pub residue_identities: Vec<crate::process::ProcessInfo>,
}

/// Unexpected owned process residue at row 57's intermediate observation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SignalResidueObservation {
    pub observed_after_ms: u64,
    pub processes: u32,
    #[serde(default)]
    pub identities: Vec<crate::process::ProcessInfo>,
}

/// Evidence for a declared persistent daemon idle-linger policy.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SignalDaemonLingerObservation {
    pub idle_linger_ms: u64,
    pub identity: crate::process::ProcIdentity,
    /// External observation immediately after the signalled client exited.
    pub idle_origin_ns: u64,
    /// `identity-present-at-2s` or `identity-absent-at-2s`.
    pub observation_2s: String,
    /// `exited-by-declared-deadline` or `survived-declared-deadline`.
    pub deadline_outcome: String,
    pub deadline_ns: u64,
    /// Number of owned-process samples from the idle origin through the later
    /// of the two-second snapshot and declared deadline plus tolerance.
    pub sample_count: u64,
    /// Fixed interval used between scheduled owned-process samples.
    pub sample_interval_ms: u64,
    /// `stable-identity-or-absent` or `residue-or-identity-change`.
    pub identity_continuity: String,
    /// Whether the recorded stable identity was present in the sample scheduled
    /// at the declared boundary, `idle_origin + idle_linger_ms + 250 ms`.
    pub identity_present_at_deadline: bool,
    /// Honest observation limit for processes that start and exit between samples.
    pub resolution_limit: String,
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
    idle_linger_ms: Option<u64>,
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
        if trial.cleanup_escalated.is_none() {
            return incomplete(format!(
                "{} cleanup-escalation evidence is incomplete",
                trial.case
            ));
        }
        let (Some(origin_ns), Some(terminal_ns)) = (trial.origin_ns, trial.terminal_ns) else {
            return incomplete(format!("{} origin/terminal boundary is absent", trial.case));
        };
        if terminal_ns < origin_ns {
            return incomplete(format!("{} terminal precedes delivery origin", trial.case));
        }
        if trial.terminal_type.is_none()
            || trial.terminal_count.is_none()
            || trial.source_terminal_count.is_none()
        {
            return incomplete(format!(
                "{} structured terminal boundary is absent",
                trial.case
            ));
        }
        if trial.exit_code.is_none() && trial.exit_was_signal.is_none() {
            return incomplete(format!("{} process exit boundary is absent", trial.case));
        }
        let Some(residue_processes) = trial.residue_processes else {
            return incomplete(format!("{} residue observation is absent", trial.case));
        };
        if usize::try_from(residue_processes).ok() != Some(trial.residue_identities.len()) {
            return incomplete(format!(
                "{} residue count does not match its identity evidence",
                trial.case
            ));
        }
        let Some(residue) = trial.residue.as_ref() else {
            return incomplete(format!(
                "{} two-second residue observation is absent",
                trial.case
            ));
        };
        if residue.observed_after_ms != 2_000
            || usize::try_from(residue.processes).ok() != Some(residue.identities.len())
        {
            return incomplete(format!(
                "{} two-second residue evidence is inconsistent",
                trial.case
            ));
        }
        match (idle_linger_ms, trial.declared_daemon_linger.as_ref()) {
            (None, None) => {}
            (Some(expected), Some(linger))
                if linger.idle_linger_ms == expected
                    && linger.deadline_ns
                        == linger.idle_origin_ns.saturating_add(
                            expected.saturating_add(250).saturating_mul(1_000_000),
                        )
                    && matches!(
                        linger.observation_2s.as_str(),
                        "identity-present-at-2s" | "identity-absent-at-2s"
                    )
                    && matches!(
                        linger.deadline_outcome.as_str(),
                        "exited-by-declared-deadline" | "survived-declared-deadline"
                    )
                    && linger.sample_count > 0
                    && (1..=250).contains(&linger.sample_interval_ms)
                    && matches!(
                        linger.identity_continuity.as_str(),
                        "stable-identity-or-absent" | "residue-or-identity-change"
                    )
                    && linger.resolution_limit
                        == "processes shorter than one sample interval can be missed"
                    && linger.identity_present_at_deadline
                        == (linger.deadline_outcome == "survived-declared-deadline") => {}
            (Some(_), Some(_)) => {
                return incomplete(format!(
                    "{} declared daemon linger evidence is inconsistent",
                    trial.case
                ));
            }
            (Some(_), None) => {
                return incomplete(format!(
                    "{} declared daemon linger evidence is absent",
                    trial.case
                ));
            }
            (None, Some(_)) => {
                return incomplete(format!(
                    "{} has undeclared daemon linger evidence",
                    trial.case
                ));
            }
        }
        if let Some(linger) = trial.declared_daemon_linger.as_ref() {
            if residue
                .identities
                .iter()
                .any(|process| process.identity == linger.identity)
            {
                return incomplete(format!(
                    "{} declared daemon identity evidence is inconsistent",
                    trial.case
                ));
            }
        }
    }
    for trial in trials.iter().filter(|trial| !trial.applicable) {
        if trial.case != "stdin-eof"
            || trial.not_applicable_reason.is_none()
            || trial.origin_ns.is_some()
            || trial.terminal_ns.is_some()
            || trial.delivery_succeeded.is_some()
            || trial.ownership_resolved.is_some()
            || trial.cleanup_escalated.is_some()
            || trial.terminal_count.is_some()
            || trial.source_terminal_count.is_some()
            || trial.exit_was_signal.is_some()
            || trial.residue.is_some()
            || trial.declared_daemon_linger.is_some()
            || trial.residue_processes.is_some()
            || !trial.residue_identities.is_empty()
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
                && trial.source_terminal_count == Some(1)
                && trial.cleanup_escalated == Some(false)
                && trial.exit_was_signal == Some(false)
                && trial.exit_code.is_some()
                && trial
                    .residue
                    .as_ref()
                    .is_some_and(|residue| residue.processes == 0)
                && trial.declared_daemon_linger.as_ref().is_none_or(|linger| {
                    linger.deadline_outcome == "exited-by-declared-deadline"
                        && linger.identity_continuity == "stable-identity-or-absent"
                })
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_failure: Option<String>,
    pub control_probe_blocked: bool,
    pub harness_confinement_identity: String,
    pub probe_confinement_identity: String,
    pub egress_enforcement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_profile_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_rendered_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard_launcher_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_rule: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_destination: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_bind_address: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owned_ipv4_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_port_owned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_probe_allowed: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub udp_probe_destinations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp_probes_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_ipv4_probe_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_loopback_probe_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_inheritance_proven: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_write_blocked: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_hash_verified: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_ipv4_monitor: Option<crate::offline_guard::LocalIpv4MonitorEvidence>,
    /// Guard-setup TCP delivery probes, one per non-loopback owned address.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_delivery_probes: Vec<crate::offline_guard::LocalDeliveryProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_delivery_proven: Option<bool>,
    pub attempts: Vec<OfflineAttempt>,
}

/// Row-62 enforcement identity of the reviewed reference-mock owned connector.
pub(crate) const OFFLINE_OWNED_CONNECTOR_ENFORCEMENT: &str =
    "reviewed reference-mock owned loopback connector";
/// Row-62 enforcement identity of the reviewed macOS Seatbelt TCP-only guard.
pub(crate) const OFFLINE_SEATBELT_ENFORCEMENT: &str = "macos-seatbelt-owned-local-tcp-port-v3";

/// Row-62 PASS/ERROR-only evaluation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OfflineModeEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Row 62 requires every local IPv4 address the Seatbelt `localhost` rule
/// permits to be owned by AHRB: each non-loopback owned address must have
/// exactly one setup delivery probe at the provider port that AHRB's sentinel
/// recorded (or that Seatbelt denied).
fn seatbelt_local_delivery_error(trial: &OfflineTrial) -> Option<String> {
    use crate::offline_guard::{LOCAL_DELIVERY_FAILURE_PREFIX, local_delivery_failure};
    if let Some(reason) = local_delivery_failure(&trial.local_delivery_probes) {
        return Some(reason);
    }
    let port = trial
        .provider_destination
        .as_deref()
        .and_then(|destination| destination.rsplit_once(':'))
        .map(|(_, port)| port)
        .unwrap_or_default();
    let expected = trial
        .owned_ipv4_addresses
        .iter()
        .filter(|address| {
            !address
                .parse::<std::net::Ipv4Addr>()
                .is_ok_and(|address| address.is_loopback())
        })
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let probed = trial
        .local_delivery_probes
        .iter()
        .map(|probe| probe.address.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if trial.local_delivery_proven != Some(true)
        || port.is_empty()
        || probed != expected
        || trial.local_delivery_probes.len() != expected.len()
        || trial
            .local_delivery_probes
            .iter()
            .any(|probe| probe.destination != format!("{}:{port}", probe.address))
    {
        return Some(format!(
            "{LOCAL_DELIVERY_FAILURE_PREFIX}: setup delivery probes do not cover every non-loopback owned address (owned [{}], probed [{}])",
            expected.into_iter().collect::<Vec<_>>().join(", "),
            probed.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    None
}

/// Evaluate reviewed same-confinement evidence. An observed escape invalidates
/// the guard and is therefore an infrastructure error rather than harness FAIL.
pub fn evaluate_offline_mode(
    trials: &[OfflineTrial],
    expected_repetitions: u32,
) -> OfflineModeEvaluation {
    let incomplete = |message: String| OfflineModeEvaluation {
        details: json!({
            "measurement_complete": false,
            "measurement_error": message,
            "trials": trials,
        }),
        measurement_error: Some(message),
        ..OfflineModeEvaluation::default()
    };
    // A guard whose setup could not prove local delivery refuses to launch
    // the harness and ends the row early, so this reason takes precedence
    // over the trial count.
    if let Some(reason) = trials
        .iter()
        .filter(|trial| trial.egress_enforcement == OFFLINE_SEATBELT_ENFORCEMENT)
        .find_map(seatbelt_local_delivery_error)
    {
        return incomplete(format!("egress enforcement unavailable: {reason}"));
    }
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
        {
            return incomplete(
                "egress enforcement unavailable: same-confinement proof failed".to_owned(),
            );
        }
        // Only reviewed guard identities are accepted; an unknown or retired
        // identity would otherwise bypass the guard-specific evidence checks.
        if trial.egress_enforcement != OFFLINE_OWNED_CONNECTOR_ENFORCEMENT
            && trial.egress_enforcement != OFFLINE_SEATBELT_ENFORCEMENT
        {
            return incomplete(format!(
                "egress enforcement unavailable: unreviewed enforcement identity {:?}",
                trial.egress_enforcement
            ));
        }
        if trial.egress_enforcement == OFFLINE_SEATBELT_ENFORCEMENT {
            let profile_hash = trial.guard_profile_sha256.as_deref().unwrap_or_default();
            let launcher_hash = trial.guard_launcher_sha256.as_deref().unwrap_or_default();
            let provider_destination = trial.provider_destination.as_deref().unwrap_or_default();
            let provider_port = provider_destination
                .rsplit_once(':')
                .map(|(_, port)| port)
                .unwrap_or_default();
            let expected_provider_rule = format!(
                "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:{provider_port}\")))"
            );
            let expected_provider_bind = provider_destination.to_owned();
            let monitor = trial.local_ipv4_monitor.as_ref();
            if trial.udp_probes_blocked != Some(true) {
                return incomplete(format!(
                    "egress enforcement unavailable: guarded UDP probes to {} were not all denied",
                    trial.udp_probe_destinations.join(", ")
                ));
            }
            if let Some(monitor) = monitor {
                if monitor.change_detected
                    || !monitor.first_change_old_addresses.is_empty()
                    || !monitor.first_change_new_addresses.is_empty()
                {
                    return incomplete(format!(
                        "egress enforcement unavailable: local IPv4 address set changed during the trial (old [{}], new [{}]); sentinel coverage no longer matches",
                        monitor.first_change_old_addresses.join(", "),
                        monitor.first_change_new_addresses.join(", ")
                    ));
                }
                if let Some(error) = &monitor.error {
                    return incomplete(format!(
                        "egress enforcement unavailable: local IPv4 address monitor failed: {error}"
                    ));
                }
            }
            // The retained profile must be the exact hashed bytes and contain
            // only the TCP provider rule plus the profile-root Unix-socket
            // allowance as network allowances.
            let rendered_profile = trial.guard_rendered_profile.as_deref().unwrap_or_default();
            let rendered_hash = {
                use sha2::Digest as _;
                format!("{:x}", sha2::Sha256::digest(rendered_profile.as_bytes()))
            };
            let network_allowances = rendered_profile
                .lines()
                .filter(|line| line.starts_with("(allow network"))
                .collect::<Vec<_>>();
            if rendered_profile.is_empty()
                || rendered_hash != profile_hash
                || !rendered_profile
                    .lines()
                    .any(|line| line == "(deny network-outbound)")
                || network_allowances.len() != 2
                || !network_allowances.contains(&expected_provider_rule.as_str())
                || !network_allowances.iter().any(|line| {
                    line.starts_with("(allow network-outbound (subpath ")
                        && !line.contains("(remote ")
                })
                || rendered_profile.contains("(remote ip ")
                || rendered_profile.contains("(remote udp ")
            {
                return incomplete(
                    "egress enforcement unavailable: retained Seatbelt profile does not match its hash or the TCP-only provider rule"
                        .to_owned(),
                );
            }
            if profile_hash.is_empty()
                || launcher_hash.is_empty()
                || provider_destination.is_empty()
                || trial.provider_bind_address.as_deref() != Some(expected_provider_bind.as_str())
                || trial.owned_ipv4_addresses.is_empty()
                || trial.provider_port_owned != Some(true)
                || trial.provider_rule.as_deref() != Some(expected_provider_rule.as_str())
                || !provider_destination.starts_with("127.0.0.1:")
                || !trial
                    .harness_confinement_identity
                    .contains(&format!("profile-sha256:{profile_hash}"))
                || !trial
                    .harness_confinement_identity
                    .contains(&format!("launcher-sha256:{launcher_hash}"))
                || !trial
                    .harness_confinement_identity
                    .contains(&format!("bind:{expected_provider_bind}"))
                || !trial
                    .owned_ipv4_addresses
                    .iter()
                    .any(|address| address == "127.0.0.1")
                || trial.provider_probe_allowed != Some(true)
                || trial.udp_probe_destinations.is_empty()
                || trial.udp_probes_blocked != Some(true)
                || trial.udp_probe_destinations.len() != trial.owned_ipv4_addresses.len()
                || !trial.owned_ipv4_addresses.iter().all(|address| {
                    trial
                        .udp_probe_destinations
                        .iter()
                        .any(|destination| destination == &format!("{address}:{provider_port}"))
                })
                || trial.alternate_ipv4_probe_blocked != Some(true)
                || trial.alternate_loopback_probe_blocked != Some(true)
                || trial.child_inheritance_proven != Some(true)
                || trial.profile_write_blocked != Some(true)
                || trial.launch_hash_verified != Some(true)
                || monitor.is_none_or(|monitor| {
                    monitor.sample_interval_ms == 0
                        || monitor.sample_interval_ms > 1_000
                        || monitor.samples_completed < 2
                        || monitor.final_addresses != trial.owned_ipv4_addresses
                        || monitor.change_detected
                        || !monitor.first_change_old_addresses.is_empty()
                        || !monitor.first_change_new_addresses.is_empty()
                        || monitor.error.is_some()
                })
            {
                return incomplete(
                    "egress enforcement unavailable: Seatbelt identity/probe evidence incomplete"
                        .to_owned(),
                );
            }
        }
        if !trial.terminal_success {
            let failure = trial
                .terminal_failure
                .as_deref()
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default();
            return incomplete(format!(
                "egress enforcement unavailable: guarded harness workflow did not complete successfully{failure}"
            ));
        }
        if trial.provider_requests == 0 {
            return incomplete(
                "egress enforcement unavailable: guarded harness made no provider request"
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
            "guard_profile_sha256":trials.first().and_then(|trial|trial.guard_profile_sha256.as_deref()),
            "guard_rendered_profile":trials.first().and_then(|trial|trial.guard_rendered_profile.as_deref()),
            "guard_launcher_sha256":trials.first().and_then(|trial|trial.guard_launcher_sha256.as_deref()),
            "provider_rule":trials.first().and_then(|trial|trial.provider_rule.as_deref()),
            "provider_destination":trials.first().and_then(|trial|trial.provider_destination.as_deref()),
            "provider_bind_address":trials.first().and_then(|trial|trial.provider_bind_address.as_deref()),
            "owned_ipv4_addresses":trials.first().map(|trial|trial.owned_ipv4_addresses.as_slice()),
            "provider_port_owned":trials.first().and_then(|trial|trial.provider_port_owned),
            "provider_probe_allowed":trials.first().and_then(|trial|trial.provider_probe_allowed),
            "udp_probe_destinations":trials.first().map(|trial|trial.udp_probe_destinations.as_slice()),
            "udp_probes_blocked":trials.first().and_then(|trial|trial.udp_probes_blocked),
            "alternate_ipv4_probe_blocked":trials.first().and_then(|trial|trial.alternate_ipv4_probe_blocked),
            "alternate_loopback_probe_blocked":trials.first().and_then(|trial|trial.alternate_loopback_probe_blocked),
            "child_inheritance_proven":trials.first().and_then(|trial|trial.child_inheritance_proven),
            "profile_write_blocked":trials.first().and_then(|trial|trial.profile_write_blocked),
            "launch_hash_verified":trials.first().and_then(|trial|trial.launch_hash_verified),
            "local_ipv4_monitor":trials.first().and_then(|trial|trial.local_ipv4_monitor.as_ref()),
            "local_delivery_probes":trials.first().map(|trial|trial.local_delivery_probes.as_slice()),
            "local_delivery_proven":trials.first().and_then(|trial|trial.local_delivery_proven),
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

    /// A setup delivery probe to `address:43123` recorded by AHRB's sentinel.
    fn delivered_probe(address: &str) -> crate::offline_guard::LocalDeliveryProbe {
        crate::offline_guard::LocalDeliveryProbe {
            address: address.to_owned(),
            interfaces: vec!["en0".to_owned()],
            destination: format!("{address}:43123"),
            outcome: crate::offline_guard::LOCAL_DELIVERY_OWNED.to_owned(),
            probe_local_address: Some(format!("{address}:50000")),
            sentinel_recorded: true,
            detail: "AHRB sentinel recorded the probe".to_owned(),
        }
    }

    /// A rendered TCP-only Seatbelt profile for port 43123 and its SHA-256.
    fn seatbelt_test_profile() -> (String, String) {
        use sha2::Digest as _;
        let rendered = concat!(
            "(version 1)\n",
            "(allow default)\n",
            "(deny file-write* (subpath \"/tmp/ahrb-offline-guard-1-1\"))\n",
            "(deny network-outbound)\n",
            "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))\n",
            "(allow network-outbound (subpath \"/tmp/profile\"))\n"
        )
        .to_owned();
        let hash = format!("{:x}", sha2::Sha256::digest(rendered.as_bytes()));
        (rendered, hash)
    }

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
            cleanup_escalated: Some(false),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            source_terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue: Some(SignalResidueObservation {
                observed_after_ms: 2_000,
                processes: 0,
                identities: Vec::new(),
            }),
            declared_daemon_linger: None,
            residue_processes: Some(0),
            residue_identities: Vec::new(),
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
                cleanup_escalated: None,
                origin_ns: None,
                terminal_ns: None,
                terminal_type: None,
                terminal_count: None,
                source_terminal_count: None,
                exit_code: None,
                exit_was_signal: None,
                residue: None,
                declared_daemon_linger: None,
                residue_processes: None,
                residue_identities: Vec::new(),
            },
        ];
        let evaluated = evaluate_signal_matrix(&trials, 1, 2_000, None, 10_000);
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
    fn signal_matrix_retains_profile_owned_residue_identity() {
        // Every case is otherwise a clean pass (source-credited terminal, no
        // escalation), so the only failure is the recorded SIGINTx2 residue.
        let trial = |case: &str, residue: bool| SignalCaseTrial {
            repetition: 1,
            case: case.to_owned(),
            applicable: true,
            not_applicable_reason: None,
            delivery_succeeded: Some(true),
            ownership_resolved: Some(true),
            cleanup_escalated: Some(false),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            source_terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue: Some(SignalResidueObservation {
                observed_after_ms: 2_000,
                processes: u32::from(residue),
                identities: residue
                    .then_some(crate::process::ProcessInfo {
                        identity: crate::process::ProcIdentity {
                            pid: 41,
                            start_time: 9001,
                        },
                        ppid: 1,
                        command: "worker".to_owned(),
                        ownership: crate::process::ProcOwnership::ProfilePath,
                    })
                    .into_iter()
                    .collect(),
            }),
            declared_daemon_linger: None,
            residue_processes: Some(u32::from(residue)),
            residue_identities: residue
                .then_some(crate::process::ProcessInfo {
                    identity: crate::process::ProcIdentity {
                        pid: 41,
                        start_time: 9001,
                    },
                    ppid: 1,
                    command: "haiderd".to_owned(),
                    ownership: crate::process::ProcOwnership::ProfilePath,
                })
                .into_iter()
                .collect(),
        };
        let evaluated = evaluate_signal_matrix(
            &[
                trial("sigterm", false),
                trial("sigint2", true),
                trial("sighup", false),
                trial("stdin-eof", false),
            ],
            1,
            2_000,
            None,
            10_000,
        );
        assert!(evaluated.measurement_complete);
        assert!(!evaluated.passed);
        assert_eq!(
            evaluated.metrics["signal_matrix.sigint2_residue_processes"],
            1.0
        );
        assert_eq!(
            evaluated.details["cases"][1]["residue_identities"][0]["identity"]["pid"],
            41
        );
        assert_eq!(
            evaluated.details["cases"][1]["residue_identities"][0]["ownership"],
            "profile-path"
        );
        assert_eq!(evaluated.metrics["signal_matrix.passed_cases"], 3.0);
    }

    #[test]
    fn signal_matrix_credits_only_a_declared_daemon_that_exits_by_its_deadline() {
        let daemon_identity = crate::process::ProcIdentity {
            pid: 57,
            start_time: 6_000,
        };
        let daemon_process = crate::process::ProcessInfo {
            identity: daemon_identity,
            ppid: 1,
            command: "declared-daemon".to_owned(),
            ownership: crate::process::ProcOwnership::ProfilePath,
        };
        let trial = |case: &str| SignalCaseTrial {
            repetition: 1,
            case: case.to_owned(),
            applicable: true,
            not_applicable_reason: None,
            delivery_succeeded: Some(true),
            ownership_resolved: Some(true),
            cleanup_escalated: Some(false),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            source_terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue: Some(SignalResidueObservation {
                observed_after_ms: 2_000,
                processes: 0,
                identities: Vec::new(),
            }),
            declared_daemon_linger: Some(SignalDaemonLingerObservation {
                idle_linger_ms: 4_000,
                identity: daemon_identity,
                idle_origin_ns: 1_000_000,
                observation_2s: "identity-present-at-2s".to_owned(),
                deadline_outcome: "exited-by-declared-deadline".to_owned(),
                deadline_ns: 4_251_000_000,
                sample_count: 10,
                sample_interval_ms: 250,
                identity_continuity: "stable-identity-or-absent".to_owned(),
                identity_present_at_deadline: false,
                resolution_limit: "processes shorter than one sample interval can be missed"
                    .to_owned(),
            }),
            residue_processes: Some(0),
            residue_identities: Vec::new(),
        };
        let trials = [
            trial("sigterm"),
            trial("sigint2"),
            trial("sighup"),
            trial("stdin-eof"),
        ];
        let exited = evaluate_signal_matrix(&trials, 1, 2_000, Some(4_000), 10_000);
        assert!(exited.measurement_complete);
        assert!(exited.passed, "{}", exited.details);
        assert_eq!(
            exited.details["cases"][0]["declared_daemon_linger"]["observation_2s"],
            "identity-present-at-2s"
        );

        let mut late_exit = trials.to_vec();
        for trial in &mut late_exit {
            let linger = trial.declared_daemon_linger.as_mut().unwrap();
            linger.observation_2s = "identity-absent-at-2s".to_owned();
            linger.deadline_outcome = "survived-declared-deadline".to_owned();
            linger.identity_present_at_deadline = true;
        }
        let late = evaluate_signal_matrix(&late_exit, 1, 2_000, Some(4_000), 10_000);
        assert!(late.measurement_complete);
        assert!(!late.passed);
        assert_eq!(
            late.details["cases"][0]["declared_daemon_linger"]["observation_2s"],
            "identity-absent-at-2s"
        );
        assert_eq!(
            late.details["cases"][0]["declared_daemon_linger"]["deadline_outcome"],
            "survived-declared-deadline"
        );

        let mut survived = trials.to_vec();
        survived[0]
            .declared_daemon_linger
            .as_mut()
            .unwrap()
            .deadline_outcome = "survived-declared-deadline".to_owned();
        survived[0]
            .declared_daemon_linger
            .as_mut()
            .unwrap()
            .identity_present_at_deadline = true;
        survived[0].residue_processes = Some(1);
        survived[0].residue_identities = vec![daemon_process.clone()];
        let leaked = evaluate_signal_matrix(&survived, 1, 2_000, Some(4_000), 10_000);
        assert!(leaked.measurement_complete);
        assert!(!leaked.passed);
        assert_eq!(
            leaked.metrics["signal_matrix.sigterm_residue_processes"],
            1.0
        );

        let mut replaced = trials.to_vec();
        replaced[0]
            .declared_daemon_linger
            .as_mut()
            .unwrap()
            .identity_continuity = "residue-or-identity-change".to_owned();
        replaced[0].residue_processes = Some(1);
        replaced[0].residue_identities = vec![daemon_process];
        let changed = evaluate_signal_matrix(&replaced, 1, 2_000, Some(4_000), 10_000);
        assert!(changed.measurement_complete);
        assert!(!changed.passed);
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
                    cleanup_escalated: Some(false),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    source_terminal_count: Some(1),
                    exit_code: None,
                    exit_was_signal: None,
                    residue: Some(SignalResidueObservation {
                        observed_after_ms: 2_000,
                        processes: 0,
                        identities: Vec::new(),
                    }),
                    declared_daemon_linger: None,
                    residue_processes: Some(0),
                    residue_identities: Vec::new(),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "sigint2".to_owned(),
                    applicable: true,
                    not_applicable_reason: None,
                    delivery_succeeded: Some(true),
                    ownership_resolved: Some(true),
                    cleanup_escalated: Some(false),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    source_terminal_count: Some(1),
                    exit_code: Some(0),
                    exit_was_signal: Some(false),
                    residue: Some(SignalResidueObservation {
                        observed_after_ms: 2_000,
                        processes: 0,
                        identities: Vec::new(),
                    }),
                    declared_daemon_linger: None,
                    residue_processes: Some(0),
                    residue_identities: Vec::new(),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "sighup".to_owned(),
                    applicable: true,
                    not_applicable_reason: None,
                    delivery_succeeded: Some(true),
                    ownership_resolved: Some(true),
                    cleanup_escalated: Some(false),
                    origin_ns: Some(1),
                    terminal_ns: Some(2),
                    terminal_type: Some("cancelled".to_owned()),
                    terminal_count: Some(1),
                    source_terminal_count: Some(1),
                    exit_code: Some(0),
                    exit_was_signal: Some(false),
                    residue: Some(SignalResidueObservation {
                        observed_after_ms: 2_000,
                        processes: 0,
                        identities: Vec::new(),
                    }),
                    declared_daemon_linger: None,
                    residue_processes: Some(0),
                    residue_identities: Vec::new(),
                },
                SignalCaseTrial {
                    repetition: 1,
                    case: "stdin-eof".to_owned(),
                    applicable: false,
                    not_applicable_reason: Some("typed non-control stdin".to_owned()),
                    delivery_succeeded: None,
                    ownership_resolved: None,
                    cleanup_escalated: None,
                    origin_ns: None,
                    terminal_ns: None,
                    terminal_type: None,
                    terminal_count: None,
                    source_terminal_count: None,
                    exit_code: None,
                    exit_was_signal: None,
                    residue: None,
                    declared_daemon_linger: None,
                    residue_processes: None,
                    residue_identities: Vec::new(),
                },
            ],
            1,
            2_000,
            None,
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
                terminal_failure: None,
                control_probe_blocked: true,
                harness_confinement_identity: "guard-1".to_owned(),
                probe_confinement_identity: "guard-1".to_owned(),
                egress_enforcement: OFFLINE_OWNED_CONNECTOR_ENFORCEMENT.to_owned(),
                guard_profile_sha256: None,
                guard_rendered_profile: None,
                guard_launcher_sha256: None,
                provider_rule: None,
                provider_destination: None,
                provider_bind_address: None,
                owned_ipv4_addresses: Vec::new(),
                provider_port_owned: None,
                provider_probe_allowed: None,
                udp_probe_destinations: Vec::new(),
                udp_probes_blocked: None,
                alternate_ipv4_probe_blocked: None,
                alternate_loopback_probe_blocked: None,
                child_inheritance_proven: None,
                profile_write_blocked: None,
                launch_hash_verified: None,
                local_ipv4_monitor: None,
                local_delivery_probes: Vec::new(),
                local_delivery_proven: None,
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
                terminal_failure: None,
                control_probe_blocked: true,
                harness_confinement_identity: identity.clone(),
                probe_confinement_identity: identity.clone(),
                egress_enforcement: OFFLINE_OWNED_CONNECTOR_ENFORCEMENT.to_owned(),
                guard_profile_sha256: None,
                guard_rendered_profile: None,
                guard_launcher_sha256: None,
                provider_rule: None,
                provider_destination: None,
                provider_bind_address: None,
                owned_ipv4_addresses: Vec::new(),
                provider_port_owned: None,
                provider_probe_allowed: None,
                udp_probe_destinations: Vec::new(),
                udp_probes_blocked: None,
                alternate_ipv4_probe_blocked: None,
                alternate_loopback_probe_blocked: None,
                child_inheritance_proven: None,
                profile_write_blocked: None,
                launch_hash_verified: None,
                local_ipv4_monitor: None,
                local_delivery_probes: Vec::new(),
                local_delivery_proven: None,
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
    fn offline_seatbelt_owned_port_passes_without_unexpected_arrivals() {
        let (rendered, profile_hash) = seatbelt_test_profile();
        let launcher_hash = "b".repeat(64);
        let identity = format!(
            "macos-seatbelt-v3:profile-sha256:{profile_hash}:launcher-sha256:{launcher_hash}:provider:127.0.0.1:43123:bind:127.0.0.1:43123"
        );
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 2,
                terminal_success: true,
                terminal_failure: None,
                control_probe_blocked: true,
                harness_confinement_identity: identity.clone(),
                probe_confinement_identity: identity.clone(),
                egress_enforcement: OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
                guard_profile_sha256: Some(profile_hash),
                guard_rendered_profile: Some(rendered.clone()),
                guard_launcher_sha256: Some(launcher_hash),
                provider_rule: Some(
                    "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))".to_owned(),
                ),
                provider_destination: Some("127.0.0.1:43123".to_owned()),
                provider_bind_address: Some("127.0.0.1:43123".to_owned()),
                owned_ipv4_addresses: vec![
                    "127.0.0.1".to_owned(),
                    "192.168.10.80".to_owned(),
                ],
                provider_port_owned: Some(true),
                provider_probe_allowed: Some(true),
                udp_probe_destinations: vec![
                    "127.0.0.1:43123".to_owned(),
                    "192.168.10.80:43123".to_owned(),
                ],
                udp_probes_blocked: Some(true),
                alternate_ipv4_probe_blocked: Some(true),
                alternate_loopback_probe_blocked: Some(true),
                child_inheritance_proven: Some(true),
                profile_write_blocked: Some(true),
                launch_hash_verified: Some(true),
                local_ipv4_monitor: Some(crate::offline_guard::LocalIpv4MonitorEvidence {
                    sample_interval_ms: 500,
                    samples_completed: 3,
                    final_addresses: vec![
                        "127.0.0.1".to_owned(),
                        "192.168.10.80".to_owned(),
                    ],
                    change_detected: false,
                    first_change_old_addresses: Vec::new(),
                    first_change_new_addresses: Vec::new(),
                    error: None,
                }),
                local_delivery_probes: vec![delivered_probe("192.168.10.80")],
                local_delivery_proven: Some(true),
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
    }

    #[test]
    fn offline_seatbelt_rejects_udp_escape_and_address_set_change() {
        let (rendered, profile_hash) = seatbelt_test_profile();
        let launcher_hash = "b".repeat(64);
        let identity = format!(
            "macos-seatbelt-v3:profile-sha256:{profile_hash}:launcher-sha256:{launcher_hash}:provider:127.0.0.1:43123:bind:127.0.0.1:43123"
        );
        let mut trial = OfflineTrial {
            repetition: 1,
            provider_requests: 2,
            terminal_success: true,
            terminal_failure: None,
            control_probe_blocked: true,
            harness_confinement_identity: identity.clone(),
            probe_confinement_identity: identity.clone(),
            egress_enforcement: OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
            guard_profile_sha256: Some(profile_hash.clone()),
            guard_rendered_profile: Some(rendered.clone()),
            guard_launcher_sha256: Some(launcher_hash),
            provider_rule: Some(
                "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))".to_owned(),
            ),
            provider_destination: Some("127.0.0.1:43123".to_owned()),
            provider_bind_address: Some("127.0.0.1:43123".to_owned()),
            owned_ipv4_addresses: vec!["127.0.0.1".to_owned(), "192.0.2.10".to_owned()],
            provider_port_owned: Some(true),
            provider_probe_allowed: Some(true),
            udp_probe_destinations: vec![
                "127.0.0.1:43123".to_owned(),
                "192.0.2.10:43123".to_owned(),
            ],
            udp_probes_blocked: Some(false),
            alternate_ipv4_probe_blocked: Some(true),
            alternate_loopback_probe_blocked: Some(true),
            child_inheritance_proven: Some(true),
            profile_write_blocked: Some(true),
            launch_hash_verified: Some(true),
            local_ipv4_monitor: Some(crate::offline_guard::LocalIpv4MonitorEvidence {
                sample_interval_ms: 500,
                samples_completed: 4,
                final_addresses: vec!["127.0.0.1".to_owned(), "192.0.2.10".to_owned()],
                change_detected: false,
                first_change_old_addresses: Vec::new(),
                first_change_new_addresses: Vec::new(),
                error: None,
            }),
            local_delivery_probes: vec![delivered_probe("192.0.2.10")],
            local_delivery_proven: Some(true),
            attempts: vec![OfflineAttempt {
                repetition: 1,
                destination: "203.0.113.1:9".to_owned(),
                category: "control-probe".to_owned(),
                outcome: "blocked-permission-denied".to_owned(),
                allowed: false,
                confinement_identity: identity,
            }],
        };
        let udp_escape = evaluate_offline_mode(&[trial.clone()], 1);
        assert!(!udp_escape.measurement_complete);
        assert!(!udp_escape.passed);
        assert!(
            udp_escape
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("UDP probes"))
        );

        trial.udp_probes_blocked = Some(true);
        let monitor = trial.local_ipv4_monitor.as_mut().expect("monitor evidence");
        monitor.change_detected = true;
        monitor.first_change_old_addresses = vec!["127.0.0.1".to_owned(), "192.0.2.10".to_owned()];
        monitor.first_change_new_addresses = vec![
            "127.0.0.1".to_owned(),
            "192.0.2.10".to_owned(),
            "198.18.0.9".to_owned(),
        ];
        let address_change = evaluate_offline_mode(&[trial.clone()], 1);
        assert!(!address_change.measurement_complete);
        assert!(!address_change.passed);
        assert_eq!(
            address_change.measurement_error.as_deref(),
            Some(
                "egress enforcement unavailable: local IPv4 address set changed during the trial (old [127.0.0.1, 192.0.2.10], new [127.0.0.1, 192.0.2.10, 198.18.0.9]); sentinel coverage no longer matches"
            )
        );
        assert_eq!(
            address_change.details["trials"][0]["local_ipv4_monitor"]["first_change_new_addresses"]
                [2],
            "198.18.0.9"
        );

        // A removed address is also a change, even if a later sample reverts.
        let monitor = trial.local_ipv4_monitor.as_mut().expect("monitor evidence");
        monitor.first_change_new_addresses = vec!["127.0.0.1".to_owned()];
        let removal = evaluate_offline_mode(&[trial.clone()], 1);
        assert!(!removal.passed);
        assert!(
            removal
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("new [127.0.0.1])"))
        );

        let monitor = trial.local_ipv4_monitor.as_mut().expect("monitor evidence");
        monitor.change_detected = false;
        monitor.first_change_old_addresses.clear();
        monitor.first_change_new_addresses.clear();
        monitor.error = Some("final local IPv4 address enumeration failed: test".to_owned());
        let monitor_error = evaluate_offline_mode(&[trial.clone()], 1);
        assert!(!monitor_error.passed);
        assert!(
            monitor_error
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("address monitor failed"))
        );

        // With complete UDP and address evidence the trial passes; the
        // retained profile must then match its hash and stay TCP-only.
        trial
            .local_ipv4_monitor
            .as_mut()
            .expect("monitor evidence")
            .error = None;
        assert!(evaluate_offline_mode(&[trial.clone()], 1).passed);
        let profile_rejected = |trial: &OfflineTrial| {
            let evaluated = evaluate_offline_mode(std::slice::from_ref(trial), 1);
            !evaluated.passed
                && evaluated
                    .measurement_error
                    .as_deref()
                    .is_some_and(|error| error.contains("retained Seatbelt profile"))
        };
        let mut tampered = trial.clone();
        tampered.guard_rendered_profile =
            Some(rendered.replace("(allow default)", "(allow default) "));
        assert!(
            profile_rejected(&tampered),
            "hash mismatch must be rejected"
        );
        let mut missing = trial.clone();
        missing.guard_rendered_profile = None;
        assert!(
            profile_rejected(&missing),
            "missing profile must be rejected"
        );
        for broadened in [
            rendered.replace("(remote tcp ", "(remote ip "),
            rendered.replace("(remote tcp ", "(remote udp "),
            format!("{rendered}(allow network-outbound (remote udp \"localhost:43123\"))\n"),
        ] {
            use sha2::Digest as _;
            let mut widened = trial.clone();
            widened.guard_profile_sha256 =
                Some(format!("{:x}", sha2::Sha256::digest(broadened.as_bytes())));
            widened.harness_confinement_identity = widened.harness_confinement_identity.replace(
                &profile_hash,
                widened.guard_profile_sha256.as_deref().unwrap(),
            );
            widened.probe_confinement_identity = widened.harness_confinement_identity.clone();
            for attempt in &mut widened.attempts {
                attempt.confinement_identity = widened.harness_confinement_identity.clone();
            }
            widened.guard_rendered_profile = Some(broadened);
            assert!(
                profile_rejected(&widened),
                "widened profile must be rejected"
            );
        }
    }

    /// A complete Seatbelt trial for owned addresses 127.0.0.1 and
    /// 192.0.2.10 whose setup delivery probe reached AHRB's sentinel.
    fn passing_seatbelt_trial() -> OfflineTrial {
        let (rendered, profile_hash) = seatbelt_test_profile();
        let launcher_hash = "b".repeat(64);
        let identity = format!(
            "macos-seatbelt-v3:profile-sha256:{profile_hash}:launcher-sha256:{launcher_hash}:provider:127.0.0.1:43123:bind:127.0.0.1:43123"
        );
        OfflineTrial {
            repetition: 1,
            provider_requests: 2,
            terminal_success: true,
            terminal_failure: None,
            control_probe_blocked: true,
            harness_confinement_identity: identity.clone(),
            probe_confinement_identity: identity.clone(),
            egress_enforcement: OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
            guard_profile_sha256: Some(profile_hash),
            guard_rendered_profile: Some(rendered),
            guard_launcher_sha256: Some(launcher_hash),
            provider_rule: Some(
                "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))".to_owned(),
            ),
            provider_destination: Some("127.0.0.1:43123".to_owned()),
            provider_bind_address: Some("127.0.0.1:43123".to_owned()),
            owned_ipv4_addresses: vec!["127.0.0.1".to_owned(), "192.0.2.10".to_owned()],
            provider_port_owned: Some(true),
            provider_probe_allowed: Some(true),
            udp_probe_destinations: vec![
                "127.0.0.1:43123".to_owned(),
                "192.0.2.10:43123".to_owned(),
            ],
            udp_probes_blocked: Some(true),
            alternate_ipv4_probe_blocked: Some(true),
            alternate_loopback_probe_blocked: Some(true),
            child_inheritance_proven: Some(true),
            profile_write_blocked: Some(true),
            launch_hash_verified: Some(true),
            local_ipv4_monitor: Some(crate::offline_guard::LocalIpv4MonitorEvidence {
                sample_interval_ms: 500,
                samples_completed: 3,
                final_addresses: vec!["127.0.0.1".to_owned(), "192.0.2.10".to_owned()],
                ..Default::default()
            }),
            local_delivery_probes: vec![delivered_probe("192.0.2.10")],
            local_delivery_proven: Some(true),
            attempts: vec![OfflineAttempt {
                repetition: 1,
                destination: "203.0.113.1:9".to_owned(),
                category: "control-probe".to_owned(),
                outcome: "blocked-permission-denied".to_owned(),
                allowed: false,
                confinement_identity: identity,
            }],
        }
    }

    #[test]
    fn offline_seatbelt_requires_sentinel_delivery_for_every_non_loopback_address() {
        let trial = passing_seatbelt_trial();
        let passed = evaluate_offline_mode(std::slice::from_ref(&trial), 1);
        assert!(passed.passed, "{:?}", passed.measurement_error);
        assert_eq!(
            passed.details["local_delivery_probes"][0]["outcome"],
            "delivered-to-ahrb-sentinel"
        );

        // A tunnel/proxy interface answered the setup connect: the guard
        // refused the launch after one trial, and this reason (naming the
        // address and interface) outranks the cert trial count.
        let mut tunnel = trial.clone();
        tunnel.owned_ipv4_addresses.push("198.18.0.1".to_owned());
        tunnel
            .local_delivery_probes
            .push(crate::offline_guard::LocalDeliveryProbe {
                address: "198.18.0.1".to_owned(),
                interfaces: vec!["utun4".to_owned()],
                destination: "198.18.0.1:43123".to_owned(),
                outcome: crate::offline_guard::LOCAL_DELIVERY_ANSWERED_BY_OTHER.to_owned(),
                probe_local_address: Some("198.18.0.1:50001".to_owned()),
                sentinel_recorded: false,
                detail: "no sentinel arrival".to_owned(),
            });
        tunnel.local_delivery_proven = Some(false);
        tunnel.provider_requests = 0;
        tunnel.terminal_success = false;
        let refused = evaluate_offline_mode(std::slice::from_ref(&tunnel), 3);
        assert!(!refused.passed);
        assert!(!refused.measurement_complete);
        assert_eq!(
            refused.measurement_error.as_deref(),
            Some(
                "egress enforcement unavailable: guard boundary includes a local address AHRB does not own: 198.18.0.1 (utun4) answered-by-other: no sentinel arrival"
            )
        );
        assert_eq!(
            refused.details["trials"][0]["local_delivery_probes"][1]["interfaces"][0],
            "utun4"
        );

        // Every other outcome except a Seatbelt denial is also ERROR.
        for (outcome, recorded) in [
            (crate::offline_guard::LOCAL_DELIVERY_REFUSED, false),
            (crate::offline_guard::LOCAL_DELIVERY_TIMEOUT, false),
            (crate::offline_guard::LOCAL_DELIVERY_AMBIGUOUS, false),
            (crate::offline_guard::LOCAL_DELIVERY_OWNED, false),
            (crate::offline_guard::LOCAL_DELIVERY_BLOCKED, true),
        ] {
            let mut failed = trial.clone();
            failed.local_delivery_probes[0].outcome = outcome.to_owned();
            failed.local_delivery_probes[0].sentinel_recorded = recorded;
            let evaluated = evaluate_offline_mode(std::slice::from_ref(&failed), 1);
            assert!(!evaluated.passed, "{outcome}");
            assert!(
                evaluated.measurement_error.as_deref().is_some_and(|error| error
                    .contains("guard boundary includes a local address AHRB does not own: 192.0.2.10 (en0)")),
                "{outcome}: {:?}",
                evaluated.measurement_error
            );
        }
        let mut blocked = trial.clone();
        blocked.local_delivery_probes[0].outcome =
            crate::offline_guard::LOCAL_DELIVERY_BLOCKED.to_owned();
        blocked.local_delivery_probes[0].sentinel_recorded = false;
        assert!(evaluate_offline_mode(&[blocked], 1).passed);

        // Missing, extra, misaddressed, or unproven coverage is ERROR.
        let mut missing = trial.clone();
        missing.local_delivery_probes.clear();
        let mut extra = trial.clone();
        extra
            .local_delivery_probes
            .push(delivered_probe("192.0.2.99"));
        let mut misaddressed = trial.clone();
        misaddressed.local_delivery_probes[0].destination = "192.0.2.10:9".to_owned();
        let mut unproven = trial;
        unproven.local_delivery_proven = None;
        for (name, case) in [
            ("missing", missing),
            ("extra", extra),
            ("misaddressed", misaddressed),
            ("unproven", unproven),
        ] {
            let evaluated = evaluate_offline_mode(&[case], 1);
            assert!(!evaluated.passed, "{name}");
            assert!(
                evaluated
                    .measurement_error
                    .as_deref()
                    .is_some_and(|error| error.contains(
                        "setup delivery probes do not cover every non-loopback owned address"
                    )),
                "{name}: {:?}",
                evaluated.measurement_error
            );
        }
    }

    #[test]
    fn offline_probe_under_different_guard_is_rejected() {
        let (rendered, _) = seatbelt_test_profile();
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 1,
                terminal_success: true,
                terminal_failure: None,
                control_probe_blocked: true,
                harness_confinement_identity: "guard-profile-a".to_owned(),
                probe_confinement_identity: "guard-profile-b".to_owned(),
                egress_enforcement: OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
                guard_profile_sha256: Some("a".repeat(64)),
                guard_rendered_profile: Some(rendered.clone()),
                guard_launcher_sha256: Some("b".repeat(64)),
                provider_rule: Some(
                    "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))".to_owned(),
                ),
                provider_destination: Some("127.0.0.1:43123".to_owned()),
                provider_bind_address: Some("127.0.0.1:43123".to_owned()),
                owned_ipv4_addresses: vec!["127.0.0.1".to_owned()],
                provider_port_owned: Some(true),
                provider_probe_allowed: Some(true),
                udp_probe_destinations: Vec::new(),
                udp_probes_blocked: None,
                alternate_ipv4_probe_blocked: Some(true),
                alternate_loopback_probe_blocked: Some(true),
                child_inheritance_proven: Some(true),
                profile_write_blocked: Some(true),
                launch_hash_verified: Some(true),
                local_ipv4_monitor: None,
                local_delivery_probes: Vec::new(),
                local_delivery_proven: Some(true),
                attempts: Vec::new(),
            }],
            1,
        );
        assert!(!evaluated.measurement_complete);
        assert!(!evaluated.passed);
    }

    #[test]
    fn offline_seatbelt_identity_must_bind_profile_and_launcher_hashes() {
        let (rendered, profile_hash) = seatbelt_test_profile();
        let launcher_hash = "b".repeat(64);
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 1,
                terminal_success: true,
                terminal_failure: None,
                control_probe_blocked: true,
                harness_confinement_identity: "macos-seatbelt-v1:wrong".to_owned(),
                probe_confinement_identity: "macos-seatbelt-v1:wrong".to_owned(),
                egress_enforcement: OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
                guard_profile_sha256: Some(profile_hash),
                guard_rendered_profile: Some(rendered.clone()),
                guard_launcher_sha256: Some(launcher_hash),
                provider_rule: Some(
                    "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:43123\")))".to_owned(),
                ),
                provider_destination: Some("127.0.0.1:43123".to_owned()),
                provider_bind_address: Some("127.0.0.1:43123".to_owned()),
                owned_ipv4_addresses: vec!["127.0.0.1".to_owned()],
                provider_port_owned: Some(true),
                provider_probe_allowed: Some(true),
                udp_probe_destinations: vec!["127.0.0.1:43123".to_owned()],
                udp_probes_blocked: Some(true),
                alternate_ipv4_probe_blocked: Some(true),
                alternate_loopback_probe_blocked: Some(true),
                child_inheritance_proven: Some(true),
                profile_write_blocked: Some(true),
                launch_hash_verified: Some(true),
                local_ipv4_monitor: Some(crate::offline_guard::LocalIpv4MonitorEvidence {
                    sample_interval_ms: 500,
                    samples_completed: 2,
                    final_addresses: vec!["127.0.0.1".to_owned()],
                    ..Default::default()
                }),
                local_delivery_probes: Vec::new(),
                local_delivery_proven: Some(true),
                attempts: Vec::new(),
            }],
            1,
        );
        assert!(!evaluated.measurement_complete);
        assert!(
            evaluated
                .measurement_error
                .as_deref()
                .is_some_and(|error| error.contains("identity/probe evidence"))
        );
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
            cleanup_escalated: Some(false),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            source_terminal_count: Some(1),
            exit_code: Some(0),
            exit_was_signal: Some(false),
            residue: Some(SignalResidueObservation {
                observed_after_ms: 2_000,
                processes: 0,
                identities: Vec::new(),
            }),
            declared_daemon_linger: None,
            residue_processes: Some(0),
            residue_identities: Vec::new(),
        };
        let mut trials = vec![
            trial("sigterm"),
            trial("sigint2"),
            trial("sighup"),
            trial("stdin-eof"),
        ];
        trials[1].terminal_count = Some(2);
        let evaluated = evaluate_signal_matrix(&trials, 1, 2_000, None, 10_000);
        assert!(evaluated.measurement_complete);
        assert!(!evaluated.passed);
        assert!(evaluated.measurement_error.is_none());
    }

    #[test]
    fn signal_matrix_synthesized_terminal_is_complete_fail() {
        let trial = |case: &str| SignalCaseTrial {
            repetition: 1,
            case: case.to_owned(),
            applicable: true,
            not_applicable_reason: None,
            delivery_succeeded: Some(true),
            ownership_resolved: Some(true),
            cleanup_escalated: Some(false),
            origin_ns: Some(1_000_000),
            terminal_ns: Some(2_000_000),
            terminal_type: Some("cancelled".to_owned()),
            terminal_count: Some(1),
            source_terminal_count: Some(0),
            exit_code: Some(130),
            exit_was_signal: Some(false),
            residue: Some(SignalResidueObservation {
                observed_after_ms: 2_000,
                processes: 0,
                identities: Vec::new(),
            }),
            declared_daemon_linger: None,
            residue_processes: Some(0),
            residue_identities: Vec::new(),
        };
        let evaluated = evaluate_signal_matrix(
            &[
                trial("sigterm"),
                trial("sigint2"),
                trial("sighup"),
                trial("stdin-eof"),
            ],
            1,
            2_000,
            None,
            10_000,
        );
        assert!(evaluated.measurement_complete);
        assert!(!evaluated.passed);
        assert!(evaluated.measurement_error.is_none());
        assert_eq!(evaluated.metrics["signal_matrix.passed_cases"], 0.0);
    }

    #[test]
    fn offline_unreviewed_or_retired_enforcement_identity_is_rejected() {
        let identity = "guard-1".to_owned();
        for enforcement in ["reviewed-test-guard", "macos-seatbelt-owned-local-port-v2"] {
            let evaluated = evaluate_offline_mode(
                &[OfflineTrial {
                    repetition: 1,
                    provider_requests: 1,
                    terminal_success: true,
                    terminal_failure: None,
                    control_probe_blocked: true,
                    harness_confinement_identity: identity.clone(),
                    probe_confinement_identity: identity.clone(),
                    egress_enforcement: enforcement.to_owned(),
                    guard_profile_sha256: None,
                    guard_rendered_profile: None,
                    guard_launcher_sha256: None,
                    provider_rule: None,
                    provider_destination: None,
                    provider_bind_address: None,
                    owned_ipv4_addresses: Vec::new(),
                    provider_port_owned: None,
                    provider_probe_allowed: None,
                    udp_probe_destinations: Vec::new(),
                    udp_probes_blocked: None,
                    alternate_ipv4_probe_blocked: None,
                    alternate_loopback_probe_blocked: None,
                    child_inheritance_proven: None,
                    profile_write_blocked: None,
                    launch_hash_verified: None,
                    local_ipv4_monitor: None,
                    local_delivery_probes: Vec::new(),
                    local_delivery_proven: None,
                    attempts: vec![OfflineAttempt {
                        repetition: 1,
                        destination: "203.0.113.1:9".to_owned(),
                        category: "control-probe".to_owned(),
                        outcome: "blocked-permission-denied".to_owned(),
                        allowed: false,
                        confinement_identity: identity.clone(),
                    }],
                }],
                1,
            );
            assert!(!evaluated.passed, "{enforcement}");
            assert!(
                evaluated
                    .measurement_error
                    .as_deref()
                    .is_some_and(|error| error.contains("unreviewed enforcement identity")),
                "{enforcement}"
            );
        }
    }

    #[test]
    fn offline_workflow_failure_is_error_not_fail() {
        let evaluated = evaluate_offline_mode(
            &[OfflineTrial {
                repetition: 1,
                provider_requests: 1,
                terminal_success: false,
                terminal_failure: Some(
                    "terminal-failure {\"exit_code\":71,\"category\":\"unmapped-exit\"}".to_owned(),
                ),
                control_probe_blocked: true,
                harness_confinement_identity: "guard-1".to_owned(),
                probe_confinement_identity: "guard-1".to_owned(),
                egress_enforcement: OFFLINE_OWNED_CONNECTOR_ENFORCEMENT.to_owned(),
                guard_profile_sha256: None,
                guard_rendered_profile: None,
                guard_launcher_sha256: None,
                provider_rule: None,
                provider_destination: None,
                provider_bind_address: None,
                owned_ipv4_addresses: Vec::new(),
                provider_port_owned: None,
                provider_probe_allowed: None,
                udp_probe_destinations: Vec::new(),
                udp_probes_blocked: None,
                alternate_ipv4_probe_blocked: None,
                alternate_loopback_probe_blocked: None,
                child_inheritance_proven: None,
                profile_write_blocked: None,
                launch_hash_verified: None,
                local_ipv4_monitor: None,
                local_delivery_probes: Vec::new(),
                local_delivery_proven: None,
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
        assert!(evaluated.measurement_error.as_deref().is_some_and(|error| {
            error.contains("workflow did not complete")
                && error.contains("exit_code")
                && error.contains("71")
        }));
    }
}
