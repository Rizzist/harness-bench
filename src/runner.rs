//! End-to-end benchmark orchestration.

use crate::cli::{Profile, RunOptions};
use crate::determinism::{
    CrossRunReproducibilityEvaluation, DeterminismRun, NondeterministicFieldEvaluation,
    NormalizationContext, evaluate_cross_run_reproducibility, evaluate_nondeterministic_fields,
    normalize_canonical_request,
};
use crate::driver::{
    ClientExit, Cursor, Driver, DriverOperations, GenericDriver, HttpTransport,
    ManagedDaemonConfig, ManagedDaemonTransport, PerInvocationConfig, PerInvocationDriver,
    SocketJsonRpcTransport, StdinRpcTransport, Transport,
};
use crate::evaluate::{
    Assertion, TestOutcome, TestResult, TestResultMetadata, automation_score, badge_label, certify,
    classify, suite_exit_code,
};
use crate::events::{EventVocab, NormalizedEvent, rule_matches};
use crate::fake_model::{
    FakeModelEngine, FakeModelMailboxServer, FakeModelPreconnectedServer, FakeModelServer,
    FakeModelUnixServer, ProviderMailboxRequest, ProviderMailboxResponse, is_transient_bind_error,
    monotonic_timestamp_ns,
};
use crate::manifest::{ArgvPosition, InjectionComponent, InjectionMethod, Manifest, TransportKind};
use crate::mock_harness::OwnedEgressLedgerRecord;
use crate::process::{
    DiskIdentityStatus, ProcessSample, ProcessTree, Sample, Sampler, TreeDiskTracker,
};
use crate::report::{
    EgressAttempt, FilesystemSnapshot, Fingerprint, LatencyVsTurnIndexEvaluation, MembershipSample,
    MemoryTimeIntegralEvaluation, MemoryTimeIntegralEvidence, MemoryTimeIntegralSample,
    ProcessHygieneAudit, ProcessHygieneCadenceSample, ProcessHygieneCheckpoint,
    ProcessHygieneEvaluation, ProcessHygieneEvidence, ProcessHygieneProcess, Report, ReportDetails,
    ResourceSummary, StreamChunkObservation, TimeToFirstModelRequestEvaluation, TopologyMetric,
    TurnLatencyEvaluation, TurnObservation, evaluate_latency_vs_turn_index,
    evaluate_memory_time_integral, evaluate_process_hygiene, evaluate_time_to_first_model_request,
    evaluate_turn_latency_repetitions, render_resource_summary, summarize_resources,
};
use crate::resource_certification::{
    CleanupObservation, ColdStartObservation, IdleObservation, IdlePhaseRepetition,
    IdleProcessModel, LongHorizonObservation, LongHorizonPoint, LongHorizonToolResult,
    MembershipRefreshEvidence, PerInvocationObservation, RepetitionIdentity,
    ResourceCadenceEvidence, ResourceCertification, ResourceCounterKind, ResourceEnvelope,
    ResourceEvidence, ResourcePhases, ResourceProfile, ResourceTimingPlan, ReturnToIdleObservation,
    SingleAgentObservation, SweepObservation, WarmupObservation, detect_busy_polling,
    evaluate_per_invocation_resources, evaluate_resources,
};
use crate::sampler::{MemoryMetric, SampleSeries};
use crate::wave2::{
    DiskIoEvaluation, DiskTurnEvidence, LargeOutputEvaluation, LargeOutputTrial, LogEvidenceKind,
    ModelWaitCpuEvaluation, ModelWaitCpuSample, ModelWaitCpuTrialEvidence,
    SlowStreamStallEvaluation, SlowStreamStallTrialEvidence, WorkspaceFaultEvaluation,
    WorkspaceFaultTrial, evaluate_disk_io_per_turn, evaluate_large_tool_output,
    evaluate_model_wait_cpu, evaluate_slow_stream_vs_stall, evaluate_workspace_fault,
    interpolated_model_wait_cpu_ns,
};
use crate::wave2_automation::{
    ChildFailureCase, ChildFailureEvaluation, ChildFailureTrial, OfflineAttempt,
    OfflineModeEvaluation, OfflineTrial, SignalCaseTrial, SignalMatrixEvaluation,
    evaluate_child_failure_propagation, evaluate_offline_mode, evaluate_signal_matrix,
};
use crate::wave3_concurrency::{
    FairnessActorEvidence, FairnessEvaluation, FanoutCliffEvaluation, FanoutPlan,
    FanoutTrialEvidence, apply_fairness_summary, apply_fanout_cliff_summary,
    evaluate_fairness_under_fanout, evaluate_fanout_cliff, monotonic_clock_resolution_ns,
};
use crate::wave3_long_horizon::{
    ContextGoalItemRecord, ContextRecoveryEvaluation, ContextRecoveryTrial, ContextToolPairRecord,
    JournalTornTailEvaluation, JournalTornTailTrial, ResumeLatencyEvaluation, ResumeLatencyPoint,
    evaluate_context_limit_recovery, evaluate_journal_torn_tail_sweep,
    evaluate_resume_latency_vs_length,
};
use crate::wave3_long_horizon::{
    SessionResidueCheckpoint, SessionResidueEvaluation, SessionResidueProcessAudit,
    SessionResidueSweep, SessionStoreEntry, evaluate_session_residue_sweep,
};
use crate::workflow::{Actor, Barrier, Fault, ScriptedResponse, WORKFLOW_SCHEMA_VERSION, Workflow};
use crate::{AhrbError, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type HarnessDriver = Box<dyn Driver>;

enum ModelServer {
    Tcp(FakeModelServer),
    Unix {
        server: FakeModelUnixServer,
        directory: PathBuf,
    },
    Mailbox(FakeModelMailboxServer),
    Preconnected(FakeModelPreconnectedServer),
    Embedded,
}

impl ModelServer {
    async fn shutdown(self) -> Result<()> {
        match self {
            Self::Tcp(server) => server.shutdown().await,
            Self::Unix { server, directory } => {
                server.shutdown().await?;
                std::fs::remove_dir(directory)?;
                Ok(())
            }
            Self::Mailbox(server) => server.shutdown().await,
            Self::Preconnected(server) => server.shutdown().await,
            Self::Embedded => Ok(()),
        }
    }
}

struct RunState {
    events: BTreeMap<u8, Vec<NormalizedEvent>>,
    sessions: BTreeMap<u8, Vec<crate::driver::SessionId>>,
    samples: Vec<Sample>,
    session_replay_valid: Option<bool>,
    session_replay_detail: Option<String>,
    crash_recovery_ms: Option<f64>,
    crash_recovery_tree_cleared: Option<bool>,
    crash_recovery_valid: Option<bool>,
    crash_recovery_detail: Option<String>,
    journal_recovered_events: Option<usize>,
    journal_recovery_valid: Option<bool>,
    journal_recovery_detail: Option<String>,
    journal_torn_tail_injected: Option<bool>,
    journal_native_replay_valid: Option<bool>,
    lifecycle_notes: Vec<String>,
    control_evidence: Vec<Value>,
    cancel_cleanup_valid: Option<bool>,
    cancel_cleanup_detail: Option<String>,
    resume_idempotency_valid: Option<bool>,
    resume_idempotency_detail: Option<String>,
    parallel_agents: usize,
    resource_evidence: Option<ResourceEvidence>,
    per_invocation_resources: Vec<PerInvocationObservation>,
    per_invocation_membership: Vec<MembershipSample>,
    per_invocation_turn_wall_ns: Vec<u64>,
    long_horizon_turns: Vec<TurnObservation>,
    row_errors: BTreeMap<u8, String>,
}

#[derive(Clone, Default)]
struct RunProgress {
    inner: Arc<Mutex<RunProgressState>>,
}

#[derive(Default)]
struct RunProgressState {
    launched: BTreeSet<u8>,
    completed: BTreeSet<u8>,
    events: BTreeMap<u8, Vec<NormalizedEvent>>,
    results: BTreeMap<u8, TestResult>,
    row_errors: BTreeMap<u8, String>,
}

impl RunProgress {
    fn update(&self, update: impl FnOnce(&mut RunProgressState)) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| AhrbError::Protocol("run progress ledger lock was poisoned".to_owned()))?;
        update(&mut state);
        Ok(())
    }

    fn snapshot(&self) -> Result<RunProgressState> {
        let state = self
            .inner
            .lock()
            .map_err(|_| AhrbError::Protocol("run progress ledger lock was poisoned".to_owned()))?;
        Ok(RunProgressState {
            launched: state.launched.clone(),
            completed: state.completed.clone(),
            events: state.events.clone(),
            results: state.results.clone(),
            row_errors: state.row_errors.clone(),
        })
    }
}

struct PerInvocationResourceCollection {
    observations: Vec<PerInvocationObservation>,
    samples: Vec<Sample>,
    membership: Vec<MembershipSample>,
    turn_wall_ns: Vec<u64>,
    long_horizon_turns: Vec<TurnObservation>,
}

struct ModelRequestEfficiencyTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    process_hygiene: Option<ProcessHygieneEvidence>,
}

struct TurnLatencyTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    turns: Vec<TurnObservation>,
}

struct TimeToFirstModelRequestTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    turns: Vec<TurnObservation>,
    first_request_roles: Vec<String>,
}

struct MemoryTimeIntegralTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: MemoryTimeIntegralEvidence,
}

struct DiskIoTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    turns: Vec<DiskTurnEvidence>,
    counter_snapshots: Vec<Value>,
    filesystem_snapshots: Vec<FilesystemSnapshot>,
    log_evidence: LogEvidenceKind,
}

struct ModelWaitCpuTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: Vec<ModelWaitCpuTrialEvidence>,
    stream_chunks: Vec<StreamChunkObservation>,
}

struct SessionResidueTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    sweeps: Vec<SessionResidueSweep>,
    samples: Vec<Sample>,
}

struct ContextRecoveryTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    trials: Vec<ContextRecoveryTrial>,
}

struct ResumeLatencyTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    points: Vec<ResumeLatencyPoint>,
}

struct SlowStreamStallTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: Vec<SlowStreamStallTrialEvidence>,
    stream_chunks: Vec<StreamChunkObservation>,
}

struct LargeOutputTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: Vec<LargeOutputTrial>,
}

struct WorkspaceFaultTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: Vec<WorkspaceFaultTrial>,
    filesystem_snapshots: Vec<FilesystemSnapshot>,
}

struct ChildFailureTrials {
    events: Vec<NormalizedEvent>,
    evidence: Vec<ChildFailureTrial>,
    status_observations: Vec<Value>,
}

struct SignalMatrixTrials {
    events: Vec<NormalizedEvent>,
    evidence: Vec<SignalCaseTrial>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
}

struct OfflineModeTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    evidence: Vec<OfflineTrial>,
    egress_attempts: Vec<EgressAttempt>,
}

struct DeterminismTrials {
    events: Vec<NormalizedEvent>,
    runs: Vec<DeterminismRun>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
}

struct FanoutTrials {
    events: Vec<NormalizedEvent>,
    trials: Vec<FanoutTrialEvidence>,
    actors: Vec<FairnessActorEvidence>,
    samples: Vec<Sample>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct RetryTimerCalibration {
    status: u16,
    index: u32,
    requested_ms: u64,
    actual_ms: f64,
    absolute_error_ms: f64,
}

#[derive(Clone, Debug)]
struct RetryTrialEvidence {
    status: u16,
    repetition: u32,
    actor: String,
    terminal_received_ns: u64,
    events: Vec<NormalizedEvent>,
    post_terminal_requested_ms: u64,
    post_terminal_actual_ms: f64,
    requests_at_terminal: u64,
    requests_after_observation: u64,
    effects_after_terminal: u64,
}

struct RetryBudgetTrials {
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
    calibrations: Vec<RetryTimerCalibration>,
    trials: Vec<RetryTrialEvidence>,
}

#[derive(Clone, Debug, Default)]
struct InjectionSurfaceEvaluation {
    metrics: BTreeMap<String, f64>,
    details: Value,
    measurement_complete: bool,
    measurement_error: Option<String>,
    baseline_runnable: bool,
    passed: bool,
    score: f64,
}

#[derive(Clone, Debug)]
struct InjectionTrialEvidence {
    component: &'static str,
    method: InjectionMethod,
    carrier: String,
    baseline_provider_requests: u64,
    perturbed_provider_requests: u64,
    expected_endpoint_reached: bool,
    unexpected_endpoint_requests: u64,
    credential_accepted: bool,
    baseline_credential_rejected: bool,
    secret_in_argv: bool,
    component_verified: bool,
}

#[derive(Clone, Debug, Default)]
struct InjectionSurfaceTrials {
    evaluation: InjectionSurfaceEvaluation,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
}

fn injection_method_score(method: InjectionMethod) -> f64 {
    match method {
        InjectionMethod::Environment => 1.0,
        InjectionMethod::Cli => 0.75,
        InjectionMethod::GeneratedConfig => 0.50,
        InjectionMethod::Impossible => 0.0,
    }
}

fn injection_method_label(method: InjectionMethod) -> &'static str {
    match method {
        InjectionMethod::Environment => "environment",
        InjectionMethod::Cli => "cli",
        InjectionMethod::GeneratedConfig => "generated-config",
        InjectionMethod::Impossible => "impossible",
    }
}

fn evaluate_injection_surface(
    provider_method: InjectionMethod,
    base_url_method: InjectionMethod,
    credential_method: InjectionMethod,
    baseline_runnable: bool,
    trials: Vec<InjectionTrialEvidence>,
) -> InjectionSurfaceEvaluation {
    let component_score = |name: &str, method: InjectionMethod| {
        trials
            .iter()
            .find(|trial| trial.component == name)
            .filter(|trial| trial.component_verified)
            .map_or(0.0, |_| injection_method_score(method))
    };
    let provider_score = component_score("provider", provider_method);
    let base_url_score = component_score("base_url", base_url_method);
    let credential_score = component_score("credential", credential_method);
    let verified_components = trials
        .iter()
        .filter(|trial| trial.component_verified)
        .count() as u64;
    let score = (provider_score + base_url_score + credential_score) / 3.0;
    let details = json!({
        "verification_cases": trials
            .iter()
            .map(|trial| json!({
                "component": trial.component,
                "method": injection_method_label(trial.method),
                "carrier": trial.carrier,
                "baseline_provider_requests": trial.baseline_provider_requests,
                "perturbed_provider_requests": trial.perturbed_provider_requests,
                "expected_endpoint_reached": trial.expected_endpoint_reached,
                "unexpected_endpoint_requests": trial.unexpected_endpoint_requests,
                "credential_accepted": trial.credential_accepted,
                "baseline_credential_rejected": trial.baseline_credential_rejected,
                "secret_in_argv": trial.secret_in_argv,
            }))
            .collect::<Vec<_>>(),
    });
    let metrics = BTreeMap::from([
        (
            "injection_surface.provider_score".to_owned(),
            provider_score,
        ),
        (
            "injection_surface.base_url_score".to_owned(),
            base_url_score,
        ),
        (
            "injection_surface.credential_score".to_owned(),
            credential_score,
        ),
        ("injection_surface.score".to_owned(), score),
        (
            "injection_surface.verified_components".to_owned(),
            verified_components as f64,
        ),
    ]);
    InjectionSurfaceEvaluation {
        metrics,
        details,
        measurement_complete: true,
        measurement_error: None,
        baseline_runnable,
        passed: baseline_runnable && verified_components == 3 && score >= 0.50,
        score,
    }
}

#[cfg(test)]
mod injection_surface_tests {
    use super::*;

    fn evidence(
        component: &'static str,
        method: InjectionMethod,
        verified: bool,
    ) -> InjectionTrialEvidence {
        InjectionTrialEvidence {
            component,
            method,
            carrier: injection_method_label(method).to_owned(),
            baseline_provider_requests: 1,
            perturbed_provider_requests: u64::from(verified),
            expected_endpoint_reached: verified,
            unexpected_endpoint_requests: 0,
            credential_accepted: verified,
            baseline_credential_rejected: component == "credential" && verified,
            secret_in_argv: false,
            component_verified: verified,
        }
    }

    #[test]
    fn injection_surface_scores_only_behaviorally_verified_carriers() {
        let evaluation = evaluate_injection_surface(
            InjectionMethod::Environment,
            InjectionMethod::Cli,
            InjectionMethod::GeneratedConfig,
            true,
            vec![
                evidence("provider", InjectionMethod::Environment, true),
                evidence("base_url", InjectionMethod::Cli, true),
                evidence("credential", InjectionMethod::GeneratedConfig, true),
            ],
        );
        assert_eq!(evaluation.metrics["injection_surface.provider_score"], 1.0);
        assert_eq!(evaluation.metrics["injection_surface.base_url_score"], 0.75);
        assert_eq!(
            evaluation.metrics["injection_surface.credential_score"],
            0.50
        );
        assert_eq!(
            evaluation.metrics["injection_surface.verified_components"],
            3.0
        );
        assert_eq!(evaluation.score, 0.75);
        assert!(evaluation.passed);
    }

    #[test]
    fn injection_surface_unverified_component_scores_zero() {
        let evaluation = evaluate_injection_surface(
            InjectionMethod::Environment,
            InjectionMethod::Environment,
            InjectionMethod::Environment,
            true,
            vec![
                evidence("provider", InjectionMethod::Environment, true),
                evidence("base_url", InjectionMethod::Environment, false),
                evidence("credential", InjectionMethod::Environment, true),
            ],
        );
        assert_eq!(evaluation.metrics["injection_surface.base_url_score"], 0.0);
        assert_eq!(
            evaluation.metrics["injection_surface.verified_components"],
            2.0
        );
        assert!(!evaluation.passed);
    }

    #[test]
    fn injection_surface_unrunnable_baseline_cannot_pass() {
        let evaluation = evaluate_injection_surface(
            InjectionMethod::Environment,
            InjectionMethod::Environment,
            InjectionMethod::Environment,
            false,
            vec![
                evidence("provider", InjectionMethod::Environment, true),
                evidence("base_url", InjectionMethod::Environment, true),
                evidence("credential", InjectionMethod::Environment, true),
            ],
        );
        assert!(!evaluation.baseline_runnable);
        assert!(!evaluation.passed);
    }
}

/// Minimum scheduling error that row 58 treats as calibratable on a loaded
/// general-purpose host. The retry ladder still has its independent 50%-150%
/// timing envelope, so this floor only prevents ordinary scheduler jitter from
/// invalidating the measurement itself.
const RETRY_TIMER_ABSOLUTE_TOLERANCE_FLOOR_MS: f64 = 5.0;

fn retry_timer_calibratable_tolerance_ms(requested_ms: u64) -> f64 {
    (requested_ms as f64 * 0.02).max(RETRY_TIMER_ABSOLUTE_TOLERANCE_FLOOR_MS)
}

#[derive(Clone, Debug, Default)]
struct RetryBudgetEvaluation {
    metrics: BTreeMap<String, f64>,
    details: Value,
    measurement_complete: bool,
    measurement_error: Option<String>,
    reference_envelope_pass: bool,
}

fn evaluate_retry_budget(
    collected: Option<&RetryBudgetTrials>,
    manifest: &Manifest,
    profile: Profile,
) -> RetryBudgetEvaluation {
    let Some(collected) = collected else {
        return RetryBudgetEvaluation {
            details: json!({
                "attempts": [],
                "timer_calibration": [],
                "timer_tolerance_ms": 0.0,
                "post_terminal_observation_ms": 0.0,
            }),
            measurement_error: Some("retry-budget trials were not collected".to_owned()),
            ..RetryBudgetEvaluation::default()
        };
    };
    let (Some(max_attempts), Some(base_delay_ms), Some(max_delay_ms)) = (
        manifest.resources.retry_max_attempts,
        manifest.resources.retry_base_delay_ms,
        manifest.resources.retry_max_delay_ms,
    ) else {
        return RetryBudgetEvaluation {
            details: json!({
                "attempts": [],
                "timer_calibration": [],
                "timer_tolerance_ms": 0.0,
                "post_terminal_observation_ms": 0.0,
            }),
            measurement_error: Some("documented retry policy is absent".to_owned()),
            ..RetryBudgetEvaluation::default()
        };
    };
    let expected_statuses = match profile {
        Profile::Quick => vec![429_u16],
        Profile::Cert => vec![429_u16, 500_u16],
    };
    let requested_calibration_ms = base_delay_ms.min(100);
    let mut infrastructure_errors = Vec::new();
    let mut timer_tolerance_ms = 0.0_f64;
    for status in &expected_statuses {
        let calibration = collected
            .calibrations
            .iter()
            .filter(|sample| sample.status == *status)
            .collect::<Vec<_>>();
        if calibration.len() != 20 {
            infrastructure_errors.push(format!(
                "status {status} has {} timer calibration samples; expected 20",
                calibration.len()
            ));
            continue;
        }
        let calibration_indices = calibration
            .iter()
            .map(|sample| sample.index)
            .collect::<BTreeSet<_>>();
        if calibration_indices != (1..=20_u32).collect::<BTreeSet<_>>()
            || calibration.iter().any(|sample| {
                sample.requested_ms != requested_calibration_ms
                    || !sample.actual_ms.is_finite()
                    || !sample.absolute_error_ms.is_finite()
            })
        {
            infrastructure_errors.push(format!(
                "status {status} has invalid timer calibration boundaries"
            ));
            continue;
        }
        let mut errors = calibration
            .iter()
            .map(|sample| sample.absolute_error_ms)
            .collect::<Vec<_>>();
        errors.sort_by(f64::total_cmp);
        let (Some(median_lower), Some(median_upper), Some(p95_error)) =
            (errors.get(9), errors.get(10), errors.get(18))
        else {
            infrastructure_errors.push(format!(
                "status {status} has incomplete sorted timer calibration"
            ));
            continue;
        };
        let median_error = (*median_lower + *median_upper) / 2.0;
        let calibration_limit = retry_timer_calibratable_tolerance_ms(requested_calibration_ms);
        timer_tolerance_ms = timer_tolerance_ms.max((*p95_error).max(calibration_limit));
        if median_error > calibration_limit {
            infrastructure_errors.push(format!(
                "status {status} timer calibration median error {median_error:.3}ms exceeds {calibration_limit:.3}ms"
            ));
        }
    }

    let delay_sum_ms = (0..max_attempts.saturating_sub(1))
        .map(|exponent| {
            (u128::from(base_delay_ms) * (1_u128 << exponent)).min(u128::from(max_delay_ms))
        })
        .sum::<u128>();
    let declared_worst_case_ms = 1.5 * delay_sum_ms as f64 + 1_000.0;
    let required_post_terminal_ms = max_delay_ms.saturating_add(250).max(1_000);
    let mut attempts_details = Vec::new();
    let mut trial_details = Vec::new();
    let mut requests_total = 0_u64;
    let mut elapsed_ms = 0.0_f64;
    let mut failure_terminals = 0_u64;
    let mut committed_effects = 0_u64;
    let mut backoff_jittered = false;
    let mut status_jittered = BTreeMap::new();
    let mut all_trials_pass = true;
    let mut minimum_post_observation_ms = f64::INFINITY;

    for status in &expected_statuses {
        let status_trials = collected
            .trials
            .iter()
            .filter(|trial| trial.status == *status)
            .collect::<Vec<_>>();
        let repetitions = status_trials
            .iter()
            .map(|trial| trial.repetition)
            .collect::<BTreeSet<_>>();
        if status_trials.len() != 3 || repetitions != BTreeSet::from([1_u32, 2, 3]) {
            infrastructure_errors.push(format!(
                "status {status} lacks the exact three repetition set"
            ));
        }
        let mut jitter_observed_for_status = false;
        for trial in status_trials {
            let mut records = collected
                .requests
                .iter()
                .filter(|record| record.request.actor == trial.actor)
                .collect::<Vec<_>>();
            records.sort_by_key(|record| record.attempt);
            let attempt_ordinals = records
                .iter()
                .map(|record| record.attempt)
                .collect::<Vec<_>>();
            let expected_ordinals = (1..=records.len() as u64).collect::<Vec<_>>();
            if attempt_ordinals != expected_ordinals
                || records.iter().any(|record| record.received_ns == 0)
            {
                infrastructure_errors.push(format!(
                    "status {status} repetition {} has incomplete attempt boundaries",
                    trial.repetition
                ));
            }
            let requests = records.len() as u64;
            requests_total = requests_total.max(requests);
            let first_request_received_ns = records.first().map_or(0, |first| first.received_ns);
            let trial_elapsed_ms = trial
                .terminal_received_ns
                .saturating_sub(first_request_received_ns)
                as f64
                / 1_000_000.0;
            elapsed_ms = elapsed_ms.max(trial_elapsed_ms);
            if records.is_empty() || trial.terminal_received_ns == 0 {
                infrastructure_errors.push(format!(
                    "status {status} repetition {} lacks elapsed boundaries",
                    trial.repetition
                ));
            }

            let terminal_events = trial
                .events
                .iter()
                .filter(|event| is_terminal(&event.event))
                .collect::<Vec<_>>();
            let provider_failures = terminal_events
                .iter()
                .filter(|event| {
                    event.event == EventVocab::TerminalFailure
                        && event.payload.get("category").and_then(Value::as_str) == Some("provider")
                        && event.payload.get("http_status").and_then(Value::as_u64)
                            == Some(u64::from(*status))
                })
                .count() as u64;
            failure_terminals = failure_terminals.saturating_add(provider_failures);
            let effects = trial
                .events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count() as u64;
            committed_effects = committed_effects.saturating_add(effects);

            let mut intervals_in_bounds = true;
            for (index, record) in records.iter().enumerate() {
                let previous_backoff_ms = if index == 0 {
                    None
                } else {
                    Some(
                        record
                            .received_ns
                            .saturating_sub(records[index - 1].received_ns)
                            as f64
                            / 1_000_000.0,
                    )
                };
                if let Some(measured_ms) = previous_backoff_ms {
                    let exponent = u32::try_from(index.saturating_sub(1)).unwrap_or(u32::MAX);
                    let nominal_ms = base_delay_ms
                        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
                        .min(max_delay_ms) as f64;
                    let lower = (0.5 * nominal_ms - timer_tolerance_ms).max(0.0);
                    let upper = 1.5 * nominal_ms + timer_tolerance_ms;
                    intervals_in_bounds &= measured_ms >= lower && measured_ms <= upper;
                    let jitter_threshold = (0.05 * nominal_ms).max(timer_tolerance_ms);
                    if (measured_ms - nominal_ms).abs() >= jitter_threshold {
                        jitter_observed_for_status = true;
                        backoff_jittered = true;
                    }
                }
                attempts_details.push(json!({
                    "status": status,
                    "repetition": trial.repetition,
                    "attempt": record.attempt,
                    "received_ns": record.received_ns,
                    "previous_backoff_ms": previous_backoff_ms,
                }));
            }
            let response_statuses_match = records
                .iter()
                .all(|record| record.response_status == Some(*status));
            let observation_complete = trial.post_terminal_requested_ms
                == required_post_terminal_ms
                && trial.post_terminal_actual_ms >= required_post_terminal_ms as f64;
            if !observation_complete {
                infrastructure_errors.push(format!(
                    "status {status} repetition {} has incomplete post-terminal observation",
                    trial.repetition
                ));
            }
            let post_terminal_quiet = trial.requests_at_terminal
                == trial.requests_after_observation
                && trial.effects_after_terminal == 0;
            minimum_post_observation_ms =
                minimum_post_observation_ms.min(trial.post_terminal_actual_ms);
            let trial_pass = (2..=u64::from(max_attempts)).contains(&requests)
                && trial_elapsed_ms <= declared_worst_case_ms
                && declared_worst_case_ms <= 10_000.0
                && intervals_in_bounds
                && terminal_events.len() == 1
                && provider_failures == 1
                && effects <= 1
                && response_statuses_match
                && observation_complete
                && post_terminal_quiet;
            all_trials_pass &= trial_pass;
            trial_details.push(json!({
                "status": status,
                "repetition": trial.repetition,
                "requests": requests,
                "first_request_received_ns": first_request_received_ns,
                "terminal_received_ns": trial.terminal_received_ns,
                "elapsed_ms": trial_elapsed_ms,
                "failure_terminals": provider_failures,
                "committed_effects": effects,
                "post_terminal_requested_ms": trial.post_terminal_requested_ms,
                "post_terminal_actual_ms": trial.post_terminal_actual_ms,
                "requests_at_terminal": trial.requests_at_terminal,
                "requests_after_observation": trial.requests_after_observation,
                "effects_after_terminal": trial.effects_after_terminal,
                "observation_complete": observation_complete,
                "post_terminal_quiet": post_terminal_quiet,
                "intervals_in_bounds": intervals_in_bounds,
                "passed": trial_pass,
            }));
        }
        status_jittered.insert(*status, jitter_observed_for_status);
        all_trials_pass &= jitter_observed_for_status;
    }

    attempts_details.sort_by(|left, right| {
        (
            left.get("status").and_then(Value::as_u64),
            left.get("repetition").and_then(Value::as_u64),
            left.get("attempt").and_then(Value::as_u64),
        )
            .cmp(&(
                right.get("status").and_then(Value::as_u64),
                right.get("repetition").and_then(Value::as_u64),
                right.get("attempt").and_then(Value::as_u64),
            ))
    });
    trial_details.sort_by(|left, right| {
        (
            left.get("status").and_then(Value::as_u64),
            left.get("repetition").and_then(Value::as_u64),
        )
            .cmp(&(
                right.get("status").and_then(Value::as_u64),
                right.get("repetition").and_then(Value::as_u64),
            ))
    });
    let mut calibrations = collected.calibrations.clone();
    calibrations.sort_by_key(|sample| (sample.status, sample.index));
    let post_terminal_observation_ms = if minimum_post_observation_ms.is_finite() {
        minimum_post_observation_ms
    } else {
        0.0
    };
    let metrics = BTreeMap::from([
        (
            "retry_budget.requests_total".to_owned(),
            requests_total as f64,
        ),
        (
            "retry_budget.declared_max_requests".to_owned(),
            f64::from(max_attempts),
        ),
        (
            "retry_budget.declared_worst_case_ms".to_owned(),
            declared_worst_case_ms,
        ),
        ("retry_budget.elapsed_ms".to_owned(), elapsed_ms),
        (
            "retry_budget.backoff_jittered".to_owned(),
            if backoff_jittered { 1.0 } else { 0.0 },
        ),
        (
            "retry_budget.failure_terminals".to_owned(),
            failure_terminals as f64,
        ),
        (
            "retry_budget.committed_effects".to_owned(),
            committed_effects as f64,
        ),
    ]);
    let details = json!({
        "attempts": attempts_details,
        "timer_calibration": calibrations,
        "timer_tolerance_ms": timer_tolerance_ms,
        "post_terminal_observation_ms": post_terminal_observation_ms,
        "status_jittered": status_jittered,
        "trials": trial_details,
    });
    let measurement_complete = infrastructure_errors.is_empty();
    RetryBudgetEvaluation {
        metrics,
        details,
        measurement_complete,
        measurement_error: (!measurement_complete).then(|| infrastructure_errors.join("; ")),
        reference_envelope_pass: measurement_complete && all_trials_pass,
    }
}

#[cfg(test)]
mod retry_timer_tolerance_tests {
    use super::*;

    #[test]
    fn retry_timer_calibration_uses_five_millisecond_floor_for_small_bases() {
        assert_eq!(retry_timer_calibratable_tolerance_ms(50), 5.0);
        assert_eq!(retry_timer_calibratable_tolerance_ms(100), 5.0);
    }

    #[test]
    fn retry_timer_calibration_retains_percentage_for_large_delays() {
        assert_eq!(retry_timer_calibratable_tolerance_ms(1_000), 20.0);
    }
}

struct DerivedRowEvaluations<'a> {
    injection_surface: &'a InjectionSurfaceEvaluation,
    budget_enforcement: &'a crate::wave4::Wave4Evaluation,
    usage_reporting: &'a crate::wave4::Wave4Evaluation,
    session_ops_cli: &'a crate::wave4::Wave4Evaluation,
    event_stream_completeness: &'a crate::wave4::Wave4Evaluation,
    headless_permissions: &'a crate::wave4::Wave4Evaluation,
    secrets_hygiene: &'a SecretsHygieneEvaluation,
    model_request_efficiency: &'a crate::fake_model::ModelRequestEfficiencyEvaluation,
    turn_latency: &'a TurnLatencyEvaluation,
    latency_vs_turn_index: &'a LatencyVsTurnIndexEvaluation,
    session_residue: &'a SessionResidueEvaluation,
    context_limit_recovery: &'a ContextRecoveryEvaluation,
    resume_latency: &'a ResumeLatencyEvaluation,
    journal_torn_tail: &'a JournalTornTailEvaluation,
    fanout_cliff: &'a FanoutCliffEvaluation,
    fairness: &'a FairnessEvaluation,
    process_hygiene: &'a ProcessHygieneEvaluation,
    time_to_first_model_request: &'a TimeToFirstModelRequestEvaluation,
    memory_time_integral: &'a MemoryTimeIntegralEvaluation,
    disk_io_per_turn: &'a DiskIoEvaluation,
    model_wait_cpu: &'a ModelWaitCpuEvaluation,
    child_failure_propagation: &'a ChildFailureEvaluation,
    signal_matrix: &'a SignalMatrixEvaluation,
    retry_budget: &'a RetryBudgetEvaluation,
    slow_stream_vs_stall: &'a SlowStreamStallEvaluation,
    large_tool_output: &'a LargeOutputEvaluation,
    workspace_fault: &'a WorkspaceFaultEvaluation,
    offline_mode: &'a OfflineModeEvaluation,
    nondeterministic_fields: &'a NondeterministicFieldEvaluation,
    cross_run_reproducibility: &'a CrossRunReproducibilityEvaluation,
}

#[derive(Clone, Debug, Default)]
struct ToolResultRoleFidelityEvaluation {
    metrics: BTreeMap<String, f64>,
    details: Value,
    measurement_complete: bool,
    measurement_error: Option<String>,
    passed: bool,
}

#[derive(Clone, Debug, Default)]
struct SecretsHygieneEvaluation {
    metrics: BTreeMap<String, f64>,
    details: Value,
    measurement_complete: bool,
    measurement_error: Option<String>,
    passed: bool,
}

#[derive(Debug)]
struct DirectCommandObservation {
    argv: Vec<String>,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    operation_start_ns: u64,
    terminal_receipt_ns: Option<u64>,
    terminal: Value,
    outer_kill: bool,
}

async fn run_wave4_command(
    argv: &[String],
    environment: &BTreeMap<String, String>,
    deadline: Duration,
    capture_limit: usize,
) -> Result<DirectCommandObservation> {
    let argv = resolve_local_program(argv)?;
    let (program, arguments) = argv
        .split_first()
        .ok_or_else(|| AhrbError::Validation("Wave-4 direct command argv is empty".to_owned()))?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let operation_start_ns = monotonic_timestamp_ns();
    let child = command.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| AhrbError::Protocol("Wave-4 child has no process ID".to_owned()))?;
    crate::process::register_child(&child)?;
    let output = match tokio::time::timeout(deadline, child.wait_with_output()).await {
        Ok(output) => output?,
        Err(_) => {
            crate::process::retire_process(pid)?;
            return Ok(DirectCommandObservation {
                argv,
                exit_code: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                operation_start_ns,
                terminal_receipt_ns: None,
                terminal: Value::Null,
                outer_kill: true,
            });
        }
    };
    crate::process::retire_process(pid)?;
    if output.stdout.len() > capture_limit || output.stderr.len() > capture_limit {
        return Err(AhrbError::Protocol(format!(
            "Wave-4 direct command exceeded {capture_limit}-byte capture limit"
        )));
    }
    let terminal = output
        .stdout
        .split(|byte| *byte == b'\n')
        .rev()
        .find(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
        .map(serde_json::from_slice)
        .transpose()?
        .unwrap_or(Value::Null);
    let terminal_receipt_ns = monotonic_timestamp_ns();
    Ok(DirectCommandObservation {
        argv,
        exit_code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
        operation_start_ns,
        terminal_receipt_ns: Some(terminal_receipt_ns),
        terminal,
        outer_kill: false,
    })
}

fn inject_tariff_environment(manifest: &Manifest, environment: &mut BTreeMap<String, String>) {
    if let Some(tariff) = manifest
        .resources
        .budget_controls
        .as_ref()
        .and_then(|controls| controls.tariff.as_ref())
        && tariff.surface == crate::manifest::TariffSurface::Environment
    {
        environment.insert(
            tariff.input_environment.clone(),
            tariff.input_microusd_per_token.to_string(),
        );
        environment.insert(
            tariff.output_environment.clone(),
            tariff.output_microusd_per_token.to_string(),
        );
    }
}

async fn collect_budget_enforcement(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    environment: &BTreeMap<String, String>,
    engine: &FakeModelEngine,
    profile: Profile,
) -> Result<crate::wave4::Wave4Evaluation> {
    let controls = manifest
        .resources
        .budget_controls
        .as_ref()
        .ok_or_else(|| AhrbError::Validation("row-66 budget controls are absent".to_owned()))?;
    let (repetitions, token_limit, cost_limit, time_limit_ms) = match profile {
        Profile::Quick => (3_u32, 128_u64, 1_000_u64, 2_000_u64),
        Profile::Cert => (7_u32, 1_024_u64, 10_000_u64, 5_000_u64),
    };
    let mut environment = environment.clone();
    inject_tariff_environment(manifest, &mut environment);
    let mut cases = Vec::new();
    let mut kind_passes = BTreeMap::from([("tokens", true), ("cost", true), ("time", true)]);
    let mut structured_failures = 0_u64;
    let mut overrun_count = 0_u64;
    let mut token_observed = 0_u64;
    let mut cost_observed = 0_u64;
    let mut time_observed = 0_f64;
    for repetition in 1..=repetitions {
        for (kind, template) in [
            ("tokens", &controls.max_tokens),
            ("cost", &controls.max_cost),
            ("time", &controls.max_time),
        ] {
            let mut command_variables = variables.clone();
            command_variables.insert("budget_tokens".to_owned(), token_limit.to_string());
            command_variables.insert(
                "budget_cost_usd".to_owned(),
                format!("{}.{:06}", cost_limit / 1_000_000, cost_limit % 1_000_000),
            );
            command_variables.insert("budget_time_ms".to_owned(), time_limit_ms.to_string());
            let argv = render_argv(template, &command_variables)?;
            let observation = run_wave4_command(
                &argv,
                &environment,
                Duration::from_millis(time_limit_ms.saturating_add(5_000)),
                manifest.capture.max_bytes,
            )
            .await?;
            tokio::time::sleep(Duration::from_millis(1_000)).await;
            let mut provider_records = engine
                .request_records()
                .await
                .into_iter()
                .filter(|record| {
                    record.accepted
                        && record.request.scenario == "ahrb-matrix-v1"
                        && record.request.actor == format!("{kind}-r{repetition}")
                })
                .collect::<Vec<_>>();
            provider_records.sort_by_key(|record| record.semantic_ordinal);
            let fixture_cost = cost_limit.saturating_sub(25);
            let fixture_cost_input = fixture_cost.saturating_sub(3) / 2;
            let usage_boundaries = provider_records
                .iter()
                .map(|record| {
                    let (input_tokens, output_tokens) = match (kind, record.request.checkpoint.as_str()) {
                        ("tokens", "fixture") => (token_limit.saturating_sub(32), 16),
                        ("tokens", "crossing") => (8, 16),
                        ("cost", "fixture") => (fixture_cost_input, 1),
                        ("cost", "crossing") => (10, 10),
                        ("time", "crossing") => (1, 1),
                        _ => (0, 0),
                    };
                    let total_tokens = input_tokens.saturating_add(output_tokens);
                    let cost_microusd = input_tokens
                        .saturating_mul(2)
                        .saturating_add(output_tokens.saturating_mul(3));
                    let semantic_request_id = record
                        .request
                        .canonical
                        .pointer("/metadata/ahrb/request_id")
                        .and_then(Value::as_str)
                        .unwrap_or(&record.canonical_hash)
                        .to_owned();
                    json!({
                        "semantic_request_id": semantic_request_id,
                        "completed_ns": record.response_last_frame_yield_ns.or(record.response_headers_ns).unwrap_or(0),
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "total_tokens": total_tokens,
                        "cost_microusd": cost_microusd,
                    })
                })
                .collect::<Vec<_>>();
            let terminal = &observation.terminal;
            let terminal_typed =
                terminal.get("terminal_type").and_then(Value::as_str) == Some("budget-exceeded");
            let declared_output_overruns = terminal
                .get("overrun_count")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            let crossing_id = provider_records
                .iter()
                .find(|record| record.request.checkpoint == "crossing")
                .map(|record| record.canonical_hash.clone());
            let stop_boundary_ns = if kind == "time" {
                observation
                    .operation_start_ns
                    .saturating_add(time_limit_ms.saturating_mul(1_000_000))
            } else {
                provider_records
                    .iter()
                    .find(|record| record.request.checkpoint == "crossing")
                    .and_then(|record| {
                        record
                            .response_last_frame_yield_ns
                            .or(record.response_headers_ns)
                    })
                    .unwrap_or(0)
            };
            let mut seen_overruns = BTreeSet::new();
            let overrun_observations = provider_records
                .iter()
                .filter_map(|record| {
                    let completed_ns = record
                        .response_last_frame_yield_ns
                        .or(record.response_headers_ns)?;
                    let semantic_id = record.canonical_hash.clone();
                    (completed_ns > stop_boundary_ns
                        && crossing_id.as_deref() != Some(semantic_id.as_str())
                        && seen_overruns.insert(semantic_id.clone()))
                    .then(|| {
                        json!({
                            "kind": "provider-request",
                            "semantic_id": semantic_id,
                            "observed_ns": completed_ns,
                            "stop_boundary_ns": stop_boundary_ns,
                        })
                    })
                })
                .collect::<Vec<_>>();
            let case_overruns = u64::try_from(overrun_observations.len()).unwrap_or(u64::MAX);
            overrun_count = overrun_count.saturating_add(case_overruns);
            structured_failures = structured_failures.saturating_add(u64::from(terminal_typed));
            let case_pass = match kind {
                "tokens" => {
                    let observed = terminal.get("observed").and_then(Value::as_u64);
                    token_observed = token_observed.max(observed.unwrap_or(0));
                    observed == token_limit.checked_add(8)
                        && terminal.get("limit").and_then(Value::as_u64) == Some(token_limit)
                }
                "cost" => {
                    let observed = terminal.get("observed_microusd").and_then(Value::as_u64);
                    cost_observed = cost_observed.max(observed.unwrap_or(0));
                    observed == cost_limit.checked_add(25)
                        && terminal.get("limit_microusd").and_then(Value::as_u64)
                            == Some(cost_limit)
                }
                "time" => {
                    let observed = terminal.get("observed_ms").and_then(Value::as_u64);
                    let receipt_elapsed_ms =
                        observation
                            .terminal_receipt_ns
                            .map_or(f64::INFINITY, |receipt| {
                                receipt.saturating_sub(observation.operation_start_ns) as f64
                                    / 1_000_000.0
                            });
                    time_observed = time_observed.max(receipt_elapsed_ms);
                    let tolerance = 250_u64.max(time_limit_ms / 10);
                    observed.is_some_and(|value| {
                        value >= time_limit_ms && value <= time_limit_ms.saturating_add(tolerance)
                    }) && receipt_elapsed_ms >= time_limit_ms as f64
                        && receipt_elapsed_ms
                            <= time_limit_ms.saturating_add(tolerance).saturating_add(250) as f64
                }
                _ => false,
            } && terminal_typed
                && observation.exit_code == Some(0)
                && !observation.outer_kill
                && declared_output_overruns == 0
                && case_overruns == 0
                && stop_boundary_ns > 0
                && provider_records.len() == if kind == "time" { 1 } else { 2 };
            if !case_pass && let Some(passed) = kind_passes.get_mut(kind) {
                *passed = false;
            }
            cases.push(json!({
                "repetition": repetition,
                "case": kind,
                "argv": observation.argv,
                "public_operation_start_ns": observation.operation_start_ns,
                "usage_boundaries": usage_boundaries,
                "tariff_delivered": true,
                "terminal_receipt_ns": observation.terminal_receipt_ns,
                "terminal": terminal,
                "effects": [],
                "outer_kill": observation.outer_kill,
                "overrun_observations": overrun_observations,
                "stdout_bytes": observation.stdout.len(),
                "stderr_bytes": observation.stderr.len(),
                "passed": case_pass,
            }));
        }
    }
    let passed_kinds = kind_passes.values().filter(|passed| **passed).count() as u64;
    let score = passed_kinds as f64 / 3.0;
    let expected_structured = u64::from(repetitions).saturating_mul(3);
    let passed = score == 1.0 && overrun_count == 0 && structured_failures == expected_structured;
    Ok(crate::wave4::Wave4Evaluation {
        metrics: BTreeMap::from([
            (
                "budget_enforcement.token_limit".to_owned(),
                token_limit as f64,
            ),
            (
                "budget_enforcement.token_observed".to_owned(),
                token_observed as f64,
            ),
            (
                "budget_enforcement.cost_limit_microusd".to_owned(),
                cost_limit as f64,
            ),
            (
                "budget_enforcement.cost_observed_microusd".to_owned(),
                cost_observed as f64,
            ),
            (
                "budget_enforcement.time_limit_ms".to_owned(),
                time_limit_ms as f64,
            ),
            (
                "budget_enforcement.time_observed_ms".to_owned(),
                time_observed,
            ),
            (
                "budget_enforcement.overrun_count".to_owned(),
                overrun_count as f64,
            ),
            (
                "budget_enforcement.structured_failures".to_owned(),
                structured_failures as f64,
            ),
            ("budget_enforcement.score".to_owned(), score),
        ]),
        details: json!({"cases": cases, "kind_passes": kind_passes}),
        measurement_complete: true,
        measurement_error: None,
        passed,
    })
}

async fn collect_headless_permissions(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    environment: &BTreeMap<String, String>,
    profile_root: &Path,
    profile: Profile,
) -> Result<crate::wave4::Wave4Evaluation> {
    let permissions = manifest.permissions.as_ref().ok_or_else(|| {
        AhrbError::Validation("row-70 permissions declaration is absent".to_owned())
    })?;
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 5_u32,
    };
    let mut cases = Vec::new();
    let mut tty_prompts = 0_u64;
    let mut allowed_effects = 0_u64;
    let mut denied_filesystem_effects = 0_u64;
    let mut denied_network_effects = 0_u64;
    let mut scope_violations = 0_u64;
    for repetition in 1..=repetitions {
        let workspace = profile_root.join(format!("wave4-permissions-r{repetition}"));
        let outside_path = profile_root.join(format!("wave4-outside-r{repetition}.txt"));
        let listener = match tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await
        {
            Ok(listener) => Some(listener),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => return Err(error.into()),
        };
        let blocked_port = listener
            .as_ref()
            .map(tokio::net::TcpListener::local_addr)
            .transpose()?
            .map_or(9, |address| address.port());
        for (kind, template) in [
            ("allow", &permissions.allow),
            ("deny-filesystem", &permissions.deny_filesystem),
            ("deny-network", &permissions.deny_network),
        ] {
            let mut command_variables = variables.clone();
            command_variables.insert(
                "workspace".to_owned(),
                workspace.to_string_lossy().into_owned(),
            );
            command_variables.insert(
                "outside_path".to_owned(),
                outside_path.to_string_lossy().into_owned(),
            );
            command_variables.insert("blocked_host".to_owned(), "127.0.0.1".to_owned());
            command_variables.insert("blocked_port".to_owned(), blocked_port.to_string());
            let argv = render_argv(template, &command_variables)?;
            let observation = run_wave4_command(
                &argv,
                environment,
                Duration::from_secs(5),
                manifest.capture.max_bytes,
            )
            .await?;
            let combined = [observation.stdout.as_slice(), observation.stderr.as_slice()].concat();
            let prompt = String::from_utf8_lossy(&combined).to_ascii_lowercase();
            let prompted = observation.outer_kill
                || ["press enter", "allow?", "approve?", "password:"]
                    .iter()
                    .any(|needle| prompt.contains(needle));
            tty_prompts = tty_prompts.saturating_add(u64::from(prompted));
            let terminal_type = observation
                .terminal
                .get("terminal_type")
                .and_then(Value::as_str)
                .unwrap_or("missing");
            let effect = match kind {
                "allow" => {
                    workspace.join("ahrb-permission-allowed.txt").is_file()
                        && terminal_type == "success"
                }
                "deny-filesystem" => !outside_path.exists() && terminal_type == "permission-denied",
                "deny-network" => {
                    let accepted = if let Some(listener) = &listener {
                        tokio::time::timeout(Duration::from_millis(50), listener.accept())
                            .await
                            .is_ok()
                    } else {
                        false
                    };
                    !accepted
                        && terminal_type == "permission-denied"
                        && observation
                            .terminal
                            .get("os_connect_attempted")
                            .and_then(Value::as_bool)
                            == Some(false)
                }
                _ => false,
            };
            match kind {
                "allow" => allowed_effects = allowed_effects.saturating_add(u64::from(effect)),
                "deny-filesystem" => {
                    denied_filesystem_effects =
                        denied_filesystem_effects.saturating_add(u64::from(effect));
                }
                "deny-network" => {
                    denied_network_effects =
                        denied_network_effects.saturating_add(u64::from(effect));
                }
                _ => {}
            }
            let scope_violation = observation
                .terminal
                .get("scope_violation")
                .and_then(Value::as_bool)
                == Some(true);
            scope_violations = scope_violations.saturating_add(u64::from(scope_violation));
            cases.push(json!({
                "repetition": repetition,
                "case": kind,
                "argv": observation.argv,
                "exit_code": observation.exit_code,
                "terminal_type": terminal_type,
                "effect": effect,
            }));
        }
    }
    let score = match permissions.mode {
        crate::manifest::PermissionMode::AllowListAndSandbox => 1.0,
        crate::manifest::PermissionMode::Sandbox => 0.75,
        crate::manifest::PermissionMode::WorkspaceYolo => 0.50,
        crate::manifest::PermissionMode::AllowList => 0.50,
        crate::manifest::PermissionMode::None => 0.25,
    };
    let passed = crate::evaluate::headless_permission_model_passes(
        score,
        u64::from(repetitions),
        tty_prompts,
        allowed_effects,
        denied_filesystem_effects,
        denied_network_effects,
        scope_violations,
    );
    Ok(crate::wave4::Wave4Evaluation {
        metrics: BTreeMap::from([
            ("headless_permission_model.score".to_owned(), score),
            (
                "headless_permission_model.tty_prompts".to_owned(),
                tty_prompts as f64,
            ),
            (
                "headless_permission_model.allowed_effects".to_owned(),
                allowed_effects as f64,
            ),
            (
                "headless_permission_model.denied_filesystem_effects".to_owned(),
                denied_filesystem_effects as f64,
            ),
            (
                "headless_permission_model.denied_network_effects".to_owned(),
                denied_network_effects as f64,
            ),
            (
                "headless_permission_model.scope_violations".to_owned(),
                scope_violations as f64,
            ),
        ]),
        details: json!({"mode": permissions.mode, "cases": cases, "repetitions": repetitions}),
        measurement_complete: true,
        measurement_error: None,
        passed,
    })
}

fn stdout_events(observation: &DirectCommandObservation) -> Result<Vec<NormalizedEvent>> {
    observation
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
        .map(|line| serde_json::from_slice(line).map_err(AhrbError::from))
        .collect()
}

fn string_array(value: &Value, pointer: &str) -> Option<Vec<String>> {
    value
        .pointer(pointer)?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect()
}

async fn collect_session_cli(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    environment: &BTreeMap<String, String>,
    profile: Profile,
) -> Result<crate::wave4::Wave4Evaluation> {
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 5_u32,
    };
    if manifest.sessions.continue_turn.is_empty() {
        return Err(AhrbError::Validation(
            "session-ops-cli requires a public seed/continue command".to_owned(),
        ));
    }
    let mut lifecycles = Vec::new();
    let mut operation_all = BTreeMap::from([
        ("create", true),
        ("list", true),
        ("resume", true),
        ("fork", true),
        ("delete", true),
    ]);
    for repetition in 1..=repetitions {
        let seed_actor = format!("r68cli{repetition}-seed");
        let original_actor = format!("r68cli{repetition}-original");
        let fork_actor = format!("r68cli{repetition}-fork");
        let mut operation_results = Vec::new();
        let mut dynamic = variables.clone();
        dynamic.insert("marker".to_owned(), seed_actor.clone());
        let create_argv = render_argv(&manifest.sessions.create, &dynamic)?;
        let create = run_wave4_command(
            &create_argv,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let original_id = create
            .terminal
            .pointer(&manifest.sessions.id_pointer)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let create_ok = create.exit_code == Some(0) && !original_id.is_empty();
        operation_results.push(json!({
            "operation":"create","exit_code":create.exit_code,
            "terminal_type":create.terminal.get("terminal_type"),
            "extracted_ids":[original_id.clone()],"cursor":0
        }));

        dynamic.insert("session_id".to_owned(), original_id.clone());
        dynamic.insert("marker".to_owned(), seed_actor.clone());
        dynamic.insert("turn_key".to_owned(), format!("row68-seed-{repetition}"));
        dynamic.insert(
            "prompt".to_owned(),
            format!(
                "AHRB session CLI committed seed {}",
                route_marker("ahrb-matrix-v1", &seed_actor, "start")
            ),
        );
        dynamic.insert("workspace".to_owned(), ".".to_owned());
        let seed_argv = render_argv(&manifest.sessions.continue_turn, &dynamic)?;
        let seed = run_wave4_command(
            &seed_argv,
            environment,
            Duration::from_millis(manifest.resources.turn_timeout_ms.saturating_add(2_000)),
            manifest.capture.max_bytes,
        )
        .await?;
        let seed_events = stdout_events(&seed)?;
        let seed_call = seed_events
            .iter()
            .find(|event| event.event == EventVocab::ToolCall);
        let seed_result = seed_events
            .iter()
            .find(|event| event.event == EventVocab::ToolResult);
        let seed_call_id = seed_call
            .and_then(|event| event.payload.get("call_id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let seed_result_digest = seed_result
            .and_then(|event| event.payload.get("result"))
            .map(serde_json::to_vec)
            .transpose()?
            .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
            .unwrap_or_default();
        let committed_cursor = seed_events.last().map_or(0, |event| event.cursor);
        let nonempty_seed = !seed_call_id.is_empty()
            && !seed_result_digest.is_empty()
            && committed_cursor > 0
            && seed_events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess);
        operation_results.push(json!({
            "operation":"submit-seed","exit_code":seed.exit_code,
            "terminal_type":"terminal-success","extracted_ids":[seed_call_id.clone()],
            "cursor":committed_cursor
        }));

        let list_argv = render_argv(&manifest.sessions.list, &dynamic)?;
        let listed = run_wave4_command(
            &list_argv,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let listed_ids = listed
            .terminal
            .pointer(&manifest.sessions.list_array_pointer)
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.pointer(&manifest.sessions.list_item_id_pointer)
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let list_ok = listed_ids.iter().filter(|id| *id == &original_id).count() == 1;
        operation_results.push(json!({
            "operation":"list","exit_code":listed.exit_code,"terminal_type":"success",
            "extracted_ids":listed_ids,"cursor":committed_cursor
        }));

        let resume_argv = render_argv(&manifest.sessions.resume, &dynamic)?;
        let resumed = run_wave4_command(
            &resume_argv,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let seed_history = string_array(&resumed.terminal, "/history_hashes").unwrap_or_default();
        let resume_ok = resumed.exit_code == Some(0)
            && resumed.terminal.get("session_id").and_then(Value::as_str)
                == Some(original_id.as_str())
            && resumed
                .terminal
                .get("committed_cursor")
                .and_then(Value::as_u64)
                == Some(committed_cursor)
            && !seed_history.is_empty();
        operation_results.push(json!({
            "operation":"resume","exit_code":resumed.exit_code,"terminal_type":"success",
            "extracted_ids":[original_id.clone()],"cursor":committed_cursor
        }));

        let fork_argv = render_argv(&manifest.sessions.fork, &dynamic)?;
        let forked = run_wave4_command(
            &fork_argv,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let fork_id = forked
            .terminal
            .pointer(&manifest.sessions.fork_id_pointer)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let fork_seed_history =
            string_array(&forked.terminal, "/history_hashes").unwrap_or_default();
        let fork_created = forked.exit_code == Some(0)
            && !fork_id.is_empty()
            && fork_id != original_id
            && fork_seed_history == seed_history;
        operation_results.push(json!({
            "operation":"fork","exit_code":forked.exit_code,"terminal_type":"success",
            "extracted_ids":[fork_id.clone()],"cursor":committed_cursor
        }));

        for (session_id, actor, key) in [
            (&original_id, &original_actor, "original-divergence"),
            (&fork_id, &fork_actor, "fork-divergence"),
        ] {
            dynamic.insert("session_id".to_owned(), session_id.clone());
            dynamic.insert("turn_key".to_owned(), format!("row68-{key}-{repetition}"));
            dynamic.insert(
                "prompt".to_owned(),
                format!(
                    "AHRB session CLI {key} {}",
                    route_marker("ahrb-matrix-v1", actor, "start")
                ),
            );
            let argv = render_argv(&manifest.sessions.continue_turn, &dynamic)?;
            let divergence = run_wave4_command(
                &argv,
                environment,
                Duration::from_millis(manifest.resources.turn_timeout_ms.saturating_add(2_000)),
                manifest.capture.max_bytes,
            )
            .await?;
            operation_results.push(json!({
                "operation":key,"exit_code":divergence.exit_code,
                "terminal_type":"terminal-success","extracted_ids":[session_id],"cursor":null
            }));
        }

        dynamic.insert("session_id".to_owned(), original_id.clone());
        let original_after = run_wave4_command(
            &render_argv(&manifest.sessions.resume, &dynamic)?,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        dynamic.insert("session_id".to_owned(), fork_id.clone());
        let fork_after = run_wave4_command(
            &render_argv(&manifest.sessions.resume, &dynamic)?,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let original_history =
            string_array(&original_after.terminal, "/history_hashes").unwrap_or_default();
        let fork_history =
            string_array(&fork_after.terminal, "/history_hashes").unwrap_or_default();
        let fork_prefix_exact = original_history.starts_with(&seed_history)
            && fork_history.starts_with(&seed_history)
            && original_history.len() > seed_history.len()
            && fork_history.len() > seed_history.len();
        let divergence_isolated = original_history != fork_history;

        let mut delete_ok = true;
        for session_id in [&original_id, &fork_id] {
            dynamic.insert("session_id".to_owned(), session_id.clone());
            let deleted = run_wave4_command(
                &render_argv(&manifest.sessions.delete, &dynamic)?,
                environment,
                Duration::from_secs(5),
                manifest.capture.max_bytes,
            )
            .await?;
            delete_ok &= deleted.exit_code == Some(0)
                && deleted.terminal.get("deleted").and_then(Value::as_bool) == Some(true);
            operation_results.push(json!({
                "operation":"delete","exit_code":deleted.exit_code,"terminal_type":"success",
                "extracted_ids":[session_id],"cursor":null
            }));
        }
        dynamic.insert("session_id".to_owned(), original_id.clone());
        let repeated = run_wave4_command(
            &render_argv(&manifest.sessions.delete, &dynamic)?,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        delete_ok &= repeated.exit_code == Some(0)
            && repeated
                .terminal
                .get("already_absent")
                .and_then(Value::as_bool)
                == Some(true);
        let list_after = run_wave4_command(
            &render_argv(&manifest.sessions.list, &dynamic)?,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        let after_text = serde_json::to_string(&list_after.terminal)?;
        delete_ok &= !after_text.contains(&original_id) && !after_text.contains(&fork_id);
        let resume_deleted = run_wave4_command(
            &render_argv(&manifest.sessions.resume, &dynamic)?,
            environment,
            Duration::from_secs(5),
            manifest.capture.max_bytes,
        )
        .await?;
        delete_ok &= resume_deleted.exit_code != Some(0)
            && resume_deleted
                .terminal
                .get("not_found")
                .and_then(Value::as_bool)
                == Some(true);

        let resume_all = resume_ok && nonempty_seed;
        let fork_all = fork_created && fork_prefix_exact && divergence_isolated;
        for (name, passed) in [
            ("create", create_ok),
            ("list", list_ok),
            ("resume", resume_all),
            ("fork", fork_all),
            ("delete", delete_ok),
        ] {
            if !passed && let Some(all) = operation_all.get_mut(name) {
                *all = false;
            }
        }
        lifecycles.push(json!({
            "repetition":repetition,"original_id":original_id,"fork_id":fork_id,
            "seed_call_id":seed_call_id,"seed_result_digest":seed_result_digest,
            "committed_cursor":committed_cursor,
            "original_history_hashes":original_history,"fork_history_hashes":fork_history,
            "operation_results":operation_results,
            "nonempty_committed_seed":nonempty_seed,
            "fork_prefix_exact":fork_prefix_exact,"divergence_isolated":divergence_isolated
        }));
    }
    let booleans = ["create", "list", "resume", "fork", "delete"]
        .map(|name| operation_all.get(name).copied().unwrap_or(false));
    let score = booleans.iter().filter(|passed| **passed).count() as f64 / 5.0;
    let metrics = BTreeMap::from([
        (
            "session_ops_cli.create_ok".to_owned(),
            f64::from(booleans[0]),
        ),
        ("session_ops_cli.list_ok".to_owned(), f64::from(booleans[1])),
        (
            "session_ops_cli.resume_ok".to_owned(),
            f64::from(booleans[2]),
        ),
        ("session_ops_cli.fork_ok".to_owned(), f64::from(booleans[3])),
        (
            "session_ops_cli.delete_ok".to_owned(),
            f64::from(booleans[4]),
        ),
        ("session_ops_cli.score".to_owned(), score),
    ]);
    Ok(crate::wave4::Wave4Evaluation {
        metrics,
        details: json!({"lifecycles":lifecycles}),
        measurement_complete: true,
        measurement_error: None,
        passed: score == 1.0,
    })
}

fn base64_standard(bytes: &[u8]) -> Vec<u8> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = Vec::with_capacity(bytes.len().div_ceil(3).saturating_mul(4));
    for chunk in bytes.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = u32::from(*chunk.get(1).unwrap_or(&0));
        let third = u32::from(*chunk.get(2).unwrap_or(&0));
        let bits = (first << 16) | (second << 8) | third;
        output.push(ALPHABET[((bits >> 18) & 0x3f) as usize]);
        output.push(ALPHABET[((bits >> 12) & 0x3f) as usize]);
        output.push(if chunk.len() > 1 {
            ALPHABET[((bits >> 6) & 0x3f) as usize]
        } else {
            b'='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(bits & 0x3f) as usize]
        } else {
            b'='
        });
    }
    output
}

fn credential_encodings(secret: &[u8]) -> Vec<(&'static str, Vec<u8>)> {
    let hex = secret
        .iter()
        .flat_map(|byte| format!("{byte:02x}").into_bytes())
        .collect::<Vec<_>>();
    let percent = secret
        .iter()
        .flat_map(|byte| format!("%{byte:02X}").into_bytes())
        .collect::<Vec<_>>();
    let mut encodings = vec![
        ("exact", secret.to_vec()),
        ("base64", base64_standard(secret)),
        ("hex", hex),
        ("percent", percent),
    ];
    encodings.sort_by_key(|(name, _)| *name);
    encodings.dedup_by(|left, right| left.1 == right.1);
    encodings
}

fn collect_profile_regular_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = std::fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_profile_regular_files(&path, output)?;
        } else if metadata.is_file() {
            output.push(path);
        }
    }
    Ok(())
}

fn byte_match_offsets(haystack: &[u8], needle: &[u8]) -> Vec<u64> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return Vec::new();
    }
    haystack
        .windows(needle.len())
        .enumerate()
        .filter(|(_, window)| *window == needle)
        .map(|(offset, _)| u64::try_from(offset).unwrap_or(u64::MAX))
        .collect()
}

fn path_is_within(path: &Path, roots: &[PathBuf]) -> bool {
    roots
        .iter()
        .any(|root| path == root || path.starts_with(root))
}

struct SecretsHygieneInputs<'a> {
    profile_root: &'a Path,
    credential: &'a str,
    carrier_paths: &'a [PathBuf],
    session_roots: &'a [PathBuf],
    journal_roots: &'a [PathBuf],
    expected_successes: u64,
    expected_failures: u64,
    events: &'a [NormalizedEvent],
    credential_in_argv: bool,
}

fn evaluate_secrets_hygiene(inputs: SecretsHygieneInputs<'_>) -> SecretsHygieneEvaluation {
    let SecretsHygieneInputs {
        profile_root,
        credential,
        carrier_paths,
        session_roots,
        journal_roots,
        expected_successes,
        expected_failures,
        events,
        credential_in_argv,
    } = inputs;
    if carrier_paths.is_empty() {
        return SecretsHygieneEvaluation {
            details: json!({"matches": []}),
            measurement_error: Some(
                "capture.credential_carrier_paths is empty; row 71 declaration is ABSENT"
                    .to_owned(),
            ),
            ..SecretsHygieneEvaluation::default()
        };
    }
    let success_count = events
        .iter()
        .filter(|event| event.event == EventVocab::TerminalSuccess)
        .count() as u64;
    let failure_count = events
        .iter()
        .filter(|event| event.event == EventVocab::TerminalFailure)
        .count() as u64;
    if success_count != expected_successes || failure_count != expected_failures {
        return SecretsHygieneEvaluation {
            details: json!({
                "matches": [],
                "success_terminals": success_count,
                "failure_terminals": failure_count,
            }),
            measurement_error: Some(format!(
                "row 71 observed {success_count}/{failure_count} success/failure terminals; expected {expected_successes}/{expected_failures}"
            )),
            ..SecretsHygieneEvaluation::default()
        };
    }
    let mut files = Vec::new();
    if let Err(error) = collect_profile_regular_files(profile_root, &mut files) {
        return SecretsHygieneEvaluation {
            details: json!({"matches": []}),
            measurement_error: Some(format!("profile scan failed: {error}")),
            ..SecretsHygieneEvaluation::default()
        };
    }
    files.sort();
    files.dedup();
    let carrier_set = carrier_paths.iter().cloned().collect::<BTreeSet<_>>();
    let declared_carrier_files = files
        .iter()
        .filter(|path| carrier_set.contains(*path))
        .count() as u64;
    if declared_carrier_files != carrier_paths.len() as u64 {
        return SecretsHygieneEvaluation {
            details: json!({"matches": []}),
            measurement_error: Some(format!(
                "only {declared_carrier_files} of {} declared credential carrier files exist",
                carrier_paths.len()
            )),
            ..SecretsHygieneEvaluation::default()
        };
    }
    let encodings = credential_encodings(credential.as_bytes());
    let mut files_scanned = 0_u64;
    let mut bytes_scanned = 0_u64;
    let mut category_matches = BTreeMap::from([
        ("stdout", 0_u64),
        ("stderr", 0_u64),
        ("journal", 0_u64),
        ("session", 0_u64),
        ("log", 0_u64),
    ]);
    let mut matches = Vec::new();
    for path in files {
        if carrier_set.contains(&path) {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                return SecretsHygieneEvaluation {
                    details: json!({"matches": matches}),
                    measurement_error: Some(format!(
                        "could not read scanned artifact {}: {error}",
                        path.display()
                    )),
                    ..SecretsHygieneEvaluation::default()
                };
            }
        };
        files_scanned = files_scanned.saturating_add(1);
        bytes_scanned = bytes_scanned.saturating_add(bytes.len() as u64);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let category = if file_name.contains("stdout") {
            "stdout"
        } else if file_name.contains("stderr") {
            "stderr"
        } else if path_is_within(&path, journal_roots) || file_name.contains("journal") {
            "journal"
        } else if path_is_within(&path, session_roots) {
            "session"
        } else {
            "log"
        };
        for (encoding, needle) in &encodings {
            for offset in byte_match_offsets(&bytes, needle) {
                if let Some(count) = category_matches.get_mut(category) {
                    *count = count.saturating_add(1);
                }
                matches.push(json!({
                    "category": category,
                    "path": path.strip_prefix(profile_root).unwrap_or(&path),
                    "offset": offset,
                    "encoding": encoding,
                }));
            }
        }
    }
    matches.sort_by(|left, right| {
        (
            left.get("category").and_then(Value::as_str),
            left.get("path").and_then(Value::as_str),
            left.get("offset").and_then(Value::as_u64),
            left.get("encoding").and_then(Value::as_str),
        )
            .cmp(&(
                right.get("category").and_then(Value::as_str),
                right.get("path").and_then(Value::as_str),
                right.get("offset").and_then(Value::as_u64),
                right.get("encoding").and_then(Value::as_str),
            ))
    });
    let stdout_matches = category_matches["stdout"];
    let stderr_matches = category_matches["stderr"];
    let journal_matches = category_matches["journal"];
    let session_matches = category_matches["session"];
    let log_matches = category_matches["log"];
    let passed = files_scanned > 0
        && bytes_scanned > 0
        && declared_carrier_files > 0
        && !credential_in_argv
        && stdout_matches == 0
        && stderr_matches == 0
        && journal_matches == 0
        && session_matches == 0
        && log_matches == 0;
    SecretsHygieneEvaluation {
        metrics: BTreeMap::from([
            (
                "secrets_hygiene_on_disk.files_scanned".to_owned(),
                files_scanned as f64,
            ),
            (
                "secrets_hygiene_on_disk.bytes_scanned".to_owned(),
                bytes_scanned as f64,
            ),
            (
                "secrets_hygiene_on_disk.stdout_matches".to_owned(),
                stdout_matches as f64,
            ),
            (
                "secrets_hygiene_on_disk.stderr_matches".to_owned(),
                stderr_matches as f64,
            ),
            (
                "secrets_hygiene_on_disk.journal_matches".to_owned(),
                journal_matches as f64,
            ),
            (
                "secrets_hygiene_on_disk.session_matches".to_owned(),
                session_matches as f64,
            ),
            (
                "secrets_hygiene_on_disk.log_matches".to_owned(),
                log_matches as f64,
            ),
            (
                "secrets_hygiene_on_disk.declared_carrier_files".to_owned(),
                declared_carrier_files as f64,
            ),
        ]),
        details: json!({
            "matches": matches,
            "success_terminals": success_count,
            "failure_terminals": failure_count,
            "encodings_scanned": encodings.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            "credential_in_argv": credential_in_argv,
        }),
        measurement_complete: true,
        measurement_error: None,
        passed,
    }
}

#[derive(Clone, Debug)]
struct ToolResultRoleObservation {
    semantic_role: &'static str,
    raw_pointer: String,
    typed_matches: u64,
    plain_user_matches: u64,
}

fn value_contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| value_contains_text(value, needle)),
        Value::Object(values) => values
            .values()
            .any(|value| value_contains_text(value, needle)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn inspect_tool_result_role(
    canonical: &Value,
    dialect: &str,
    call_id: &str,
) -> ToolResultRoleObservation {
    if dialect.contains("anthropic") {
        let mut typed_matches = 0_u64;
        let mut plain_user_matches = 0_u64;
        if let Some(messages) = canonical.get("messages").and_then(Value::as_array) {
            for message in messages {
                if message.get("role").and_then(Value::as_str) != Some("user") {
                    continue;
                }
                match message.get("content") {
                    Some(Value::Array(blocks)) => {
                        for block in blocks {
                            let typed = block.get("type").and_then(Value::as_str)
                                == Some("tool_result")
                                && block.get("tool_use_id").and_then(Value::as_str)
                                    == Some(call_id);
                            if typed {
                                typed_matches = typed_matches.saturating_add(1);
                            } else if value_contains_text(block, call_id) {
                                plain_user_matches = plain_user_matches.saturating_add(1);
                            }
                        }
                    }
                    Some(content) if value_contains_text(content, call_id) => {
                        plain_user_matches = plain_user_matches.saturating_add(1);
                    }
                    _ => {}
                }
            }
        }
        return ToolResultRoleObservation {
            semantic_role: "tool_result",
            raw_pointer: "/messages/*/content/*".to_owned(),
            typed_matches,
            plain_user_matches,
        };
    }
    if dialect.contains("responses") {
        let mut typed_matches = 0_u64;
        let mut plain_user_matches = 0_u64;
        if let Some(items) = canonical.get("input").and_then(Value::as_array) {
            for item in items {
                let typed = item.get("type").and_then(Value::as_str)
                    == Some("function_call_output")
                    && item.get("call_id").and_then(Value::as_str) == Some(call_id);
                if typed {
                    typed_matches = typed_matches.saturating_add(1);
                } else if item.get("role").and_then(Value::as_str) == Some("user")
                    && value_contains_text(item, call_id)
                {
                    plain_user_matches = plain_user_matches.saturating_add(1);
                }
            }
        }
        return ToolResultRoleObservation {
            semantic_role: "function_call_output",
            raw_pointer: "/input/*".to_owned(),
            typed_matches,
            plain_user_matches,
        };
    }
    let mut typed_matches = 0_u64;
    let mut plain_user_matches = 0_u64;
    if let Some(messages) = canonical.get("messages").and_then(Value::as_array) {
        for message in messages {
            let typed = message.get("role").and_then(Value::as_str) == Some("tool")
                && message.get("tool_call_id").and_then(Value::as_str) == Some(call_id);
            if typed {
                typed_matches = typed_matches.saturating_add(1);
            } else if message.get("role").and_then(Value::as_str) == Some("user")
                && value_contains_text(message.get("content").unwrap_or(&Value::Null), call_id)
            {
                plain_user_matches = plain_user_matches.saturating_add(1);
            }
        }
    }
    ToolResultRoleObservation {
        semantic_role: "tool",
        raw_pointer: "/messages/*".to_owned(),
        typed_matches,
        plain_user_matches,
    }
}

fn evaluate_tool_result_role_fidelity(
    records: &[crate::fake_model::ModelRequestRecord],
    expected_repetitions: u32,
) -> ToolResultRoleFidelityEvaluation {
    let mut observations = Vec::new();
    let mut infrastructure_errors = Vec::new();
    let mut checks = 0_u64;
    let mut violations = 0_u64;
    let mut plain_user_text_violations = 0_u64;
    let mut missing_results = 0_u64;
    let mut duplicate_results = 0_u64;
    for repetition in 1..=expected_repetitions {
        let actor = if expected_repetitions == 1 {
            "r72".to_owned()
        } else {
            format!("r72a{repetition}")
        };
        for (checkpoint, outcome, call_id) in [
            ("after-success", "success", format!("row72-success-{actor}")),
            ("terminal", "failure", format!("row72-failure-{actor}")),
        ] {
            let matching = records
                .iter()
                .filter(|record| {
                    record.accepted
                        && record.role == "primary"
                        && record.request.actor == actor
                        && record.request.checkpoint == checkpoint
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                infrastructure_errors.push(format!(
                    "repetition {repetition} {outcome} result has {} next accepted primary requests; expected one",
                    matching.len()
                ));
                continue;
            }
            let record = matching[0];
            let observation = inspect_tool_result_role(
                &record.request.canonical,
                &record.request.dialect,
                &call_id,
            );
            checks = checks.saturating_add(1);
            if observation.typed_matches == 0 {
                missing_results = missing_results.saturating_add(1);
            }
            if observation.typed_matches > 1 {
                duplicate_results =
                    duplicate_results.saturating_add(observation.typed_matches.saturating_sub(1));
            }
            plain_user_text_violations =
                plain_user_text_violations.saturating_add(observation.plain_user_matches);
            let observation_violations = u64::from(observation.typed_matches != 1)
                .saturating_add(observation.plain_user_matches);
            violations = violations.saturating_add(observation_violations);
            observations.push(json!({
                "repetition": repetition,
                "outcome": outcome,
                "dialect": record.request.dialect,
                "call_id": call_id,
                "semantic_role": observation.semantic_role,
                "raw_pointer": observation.raw_pointer,
                "typed_matches": observation.typed_matches,
                "plain_user_matches": observation.plain_user_matches,
            }));
        }
    }
    let expected_checks = u64::from(expected_repetitions).saturating_mul(2);
    let measurement_complete = infrastructure_errors.is_empty() && checks == expected_checks;
    let passed = measurement_complete
        && violations == 0
        && plain_user_text_violations == 0
        && missing_results == 0
        && duplicate_results == 0;
    ToolResultRoleFidelityEvaluation {
        metrics: BTreeMap::from([
            ("tool_result_role_fidelity.checks".to_owned(), checks as f64),
            (
                "tool_result_role_fidelity.violations".to_owned(),
                violations as f64,
            ),
            (
                "tool_result_role_fidelity.plain_user_text_violations".to_owned(),
                plain_user_text_violations as f64,
            ),
            (
                "tool_result_role_fidelity.missing_results".to_owned(),
                missing_results as f64,
            ),
            (
                "tool_result_role_fidelity.duplicate_results".to_owned(),
                duplicate_results as f64,
            ),
        ]),
        details: json!({
            "expected_repetitions": expected_repetitions,
            "expected_checks": expected_checks,
            "observations": observations,
        }),
        measurement_complete,
        measurement_error: (!infrastructure_errors.is_empty())
            .then(|| infrastructure_errors.join("; ")),
        passed,
    }
}

fn apply_memory_time_integral_summary(
    summary: &mut crate::report::ResourceSummary,
    evaluation: &MemoryTimeIntegralEvaluation,
) {
    summary.memory_time_integral_mib_s_per_turn =
        Some(evaluation.memory_time_integral_mib_s_per_turn);
    summary.memory_time_integral_coverage_ratio =
        Some(evaluation.memory_time_integral_coverage_ratio);
    summary.memory_time_integral_max_sample_gap_ms =
        Some(evaluation.memory_time_integral_max_sample_gap_ms);
    summary.cpu_per_turn_p50_ms = Some(evaluation.cpu_per_turn_p50_ms);
    summary.cpu_per_turn_p95_ms = Some(evaluation.cpu_per_turn_p95_ms);
    summary.cpu_class = Some(evaluation.cpu_class.clone());
    summary.sampler_overhead_pct = summary
        .sampler_overhead_pct
        .max(evaluation.sampler_overhead_pct);
}

fn apply_disk_io_summary(summary: &mut ResourceSummary, evaluation: &DiskIoEvaluation) {
    summary.disk_write_bytes_per_turn_p50 = evaluation
        .resource_values
        .get("disk_write_bytes_per_turn_p50")
        .copied();
    summary.disk_write_bytes_per_turn_p95 = evaluation
        .resource_values
        .get("disk_write_bytes_per_turn_p95")
        .copied();
    summary.disk_write_bytes_per_turn_max = evaluation
        .resource_values
        .get("disk_write_bytes_per_turn_max")
        .copied();
    summary.session_journal_growth_bytes_per_turn = evaluation
        .resource_values
        .get("session_journal_growth_bytes_per_turn")
        .copied();
    summary.log_growth_bytes_per_turn = evaluation.log_growth_bytes_per_turn;
    summary.disk_write_growth_slope_bytes_per_turn2 = evaluation
        .resource_values
        .get("disk_write_growth_slope_bytes_per_turn2")
        .copied();
    summary.disk_io_counter_complete = Some(evaluation.disk_io_counter_complete);
    summary.unbounded_disk_growth = Some(evaluation.unbounded_disk_growth);
}

fn disk_io_resource_metrics(evaluation: &DiskIoEvaluation) -> BTreeMap<String, f64> {
    let mut metrics = evaluation.resource_values.clone();
    if let Some(value) = evaluation.log_growth_bytes_per_turn {
        metrics.insert("log_growth_bytes_per_turn".to_owned(), value);
    }
    metrics
}

fn memory_time_integral_resource_metrics(
    evaluation: &MemoryTimeIntegralEvaluation,
) -> BTreeMap<String, f64> {
    BTreeMap::from([
        (
            "memory_time_integral_mib_s_per_turn".to_owned(),
            evaluation.memory_time_integral_mib_s_per_turn,
        ),
        (
            "memory_time_integral_coverage_ratio".to_owned(),
            evaluation.memory_time_integral_coverage_ratio,
        ),
        (
            "memory_time_integral_max_sample_gap_ms".to_owned(),
            evaluation.memory_time_integral_max_sample_gap_ms,
        ),
        (
            "cpu_per_turn_p50_ms".to_owned(),
            evaluation.cpu_per_turn_p50_ms,
        ),
        (
            "cpu_per_turn_p95_ms".to_owned(),
            evaluation.cpu_per_turn_p95_ms,
        ),
    ])
}

fn apply_time_to_first_model_request_summary(
    summary: &mut crate::report::ResourceSummary,
    evaluation: &TimeToFirstModelRequestEvaluation,
) {
    summary.time_to_first_model_request_p50_ms = Some(evaluation.p50_ms);
    summary.time_to_first_model_request_p95_ms = Some(evaluation.p95_ms);
    summary.time_to_first_model_request_max_ms = Some(evaluation.max_ms);
}

fn time_to_first_model_request_resource_metrics(
    evaluation: &TimeToFirstModelRequestEvaluation,
) -> BTreeMap<String, f64> {
    BTreeMap::from([
        (
            "time_to_first_model_request_p50_ms".to_owned(),
            evaluation.p50_ms,
        ),
        (
            "time_to_first_model_request_p95_ms".to_owned(),
            evaluation.p95_ms,
        ),
        (
            "time_to_first_model_request_max_ms".to_owned(),
            evaluation.max_ms,
        ),
    ])
}

fn apply_turn_latency_summary(
    summary: &mut crate::report::ResourceSummary,
    evaluation: &TurnLatencyEvaluation,
) {
    summary.wall_per_turn_p50_ms = Some(evaluation.wall_per_turn_p50_ms);
    summary.wall_per_turn_p95_ms = Some(evaluation.wall_per_turn_p95_ms);
    summary.wall_per_turn_max_ms = Some(evaluation.wall_per_turn_max_ms);
    summary.wall_per_turn_mad_ms = Some(evaluation.wall_per_turn_mad_ms);
    summary.wall_per_turn_jitter_ratio = Some(evaluation.wall_per_turn_jitter_ratio);
    summary.latency_class = Some(evaluation.latency_class.clone());
}

fn turn_latency_resource_metrics(evaluation: &TurnLatencyEvaluation) -> BTreeMap<String, f64> {
    BTreeMap::from([
        (
            "wall_per_turn_p50_ms".to_owned(),
            evaluation.wall_per_turn_p50_ms,
        ),
        (
            "wall_per_turn_p95_ms".to_owned(),
            evaluation.wall_per_turn_p95_ms,
        ),
        (
            "wall_per_turn_max_ms".to_owned(),
            evaluation.wall_per_turn_max_ms,
        ),
        (
            "wall_per_turn_mad_ms".to_owned(),
            evaluation.wall_per_turn_mad_ms,
        ),
        (
            "wall_per_turn_jitter_ratio".to_owned(),
            evaluation.wall_per_turn_jitter_ratio,
        ),
    ])
}

fn apply_latency_vs_turn_index_summary(
    summary: &mut ResourceSummary,
    evaluation: &LatencyVsTurnIndexEvaluation,
) {
    summary.latency_slope_ms_per_100_turns = Some(evaluation.latency_slope_ms_per_100_turns);
    summary.latency_last_first_decile_ratio = evaluation.latency_last_first_decile_ratio.into();
}

fn latency_vs_turn_index_resource_metrics(
    evaluation: &LatencyVsTurnIndexEvaluation,
) -> BTreeMap<String, f64> {
    BTreeMap::from([(
        "latency_slope_ms_per_100_turns".to_owned(),
        evaluation.latency_slope_ms_per_100_turns,
    )])
}

fn apply_session_residue_summary(
    summary: &mut ResourceSummary,
    evaluation: &SessionResidueEvaluation,
) {
    summary.session_residue_slope_mib_per_session = evaluation
        .resource_values
        .get("session_residue_slope_mib_per_session")
        .copied();
    summary.session_residue_final_mib = evaluation
        .resource_values
        .get("session_residue_final_mib")
        .copied();
    summary.session_store_byte_slope_per_session = evaluation
        .resource_values
        .get("session_store_byte_slope_per_session")
        .copied();
    summary.session_store_file_count_slope_per_session = evaluation
        .resource_values
        .get("session_store_file_count_slope_per_session")
        .copied();
    summary.session_store_final_residue_bytes = evaluation
        .resource_values
        .get("session_store_final_residue_bytes")
        .copied();
    summary.session_store_final_residue_files = evaluation
        .measurement_complete
        .then_some(evaluation.session_store_final_residue_files);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunInterruption {
    Deadline,
    Abort,
}

fn per_invocation_topology(manifest: &Manifest) -> bool {
    !manifest.daemon.persistent
        && matches!(
            crate::manifest::topology_family(&manifest.concurrency.topology),
            Some(crate::manifest::TopologyFamily::PerInvocation)
        )
}

/// Execute selected workflows and write their complete evidence bundle.
pub async fn run(mut options: RunOptions) -> Result<i32> {
    let manifest = crate::manifest::load(&options.manifest)?;
    let persistence = crate::results::prepare(&options, &manifest)?;
    options.output = persistence.output.clone();
    let selected = selected_definitions(&options)?;
    let deadline_secs = crate::cli::deadline_secs(&options)?;
    let started = Instant::now();
    let progress = RunProgress::default();
    let outcome = tokio::time::timeout(
        Duration::from_secs(deadline_secs),
        run_inner(
            options.clone(),
            manifest.clone(),
            progress.clone(),
            persistence.clone(),
        ),
    )
    .await;
    let outcome = match outcome {
        Ok(Ok(code)) => Ok(code),
        Ok(Err(error)) => {
            let error = match ensure_owned_cleanup() {
                Ok(()) => error,
                Err(cleanup_error) => AhrbError::Protocol(format!(
                    "{error}; abort cleanup also failed: {cleanup_error}"
                )),
            };
            crate::report::write_failure_diagnostic(&options.output, &options.manifest, &error)?;
            write_abort_report(
                &options,
                &manifest,
                &selected,
                &progress,
                &persistence,
                &error.to_string(),
            )?;
            eprintln!(
                "ahrb: run aborted for manifest {}: {error}; wrote {}",
                options.manifest.display(),
                options.output.join("report.json").display()
            );
            return Ok(2);
        }
        Err(_) => {
            ensure_owned_cleanup()?;
            let detail = format!("deadline after {deadline_secs}s");
            let error = AhrbError::Timeout(detail.clone());
            crate::report::write_failure_diagnostic(&options.output, &options.manifest, &error)?;
            write_deadline_report(
                &options,
                &manifest,
                &selected,
                &progress,
                &persistence,
                &detail,
            )?;
            eprintln!(
                "ahrb: run deadline reached after {:.3}s; stopped launching rows and wrote {}",
                started.elapsed().as_secs_f64(),
                options.output.join("report.json").display()
            );
            Ok(2)
        }
    };
    let cleanup = ensure_owned_cleanup();
    match (outcome, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(code), Ok(())) => Ok(code),
    }
}

fn ensure_owned_cleanup() -> Result<()> {
    let survivors = crate::process::cleanup_owned_processes(Duration::from_millis(500))?;
    if survivors.is_empty() {
        Ok(())
    } else {
        Err(AhrbError::Protocol(format!(
            "owned-process cleanup left {} process(es) alive: {survivors:?}",
            survivors.len()
        )))
    }
}

fn selected_definitions(
    options: &RunOptions,
) -> Result<Vec<&'static crate::scenarios::TestDefinition>> {
    let selected: Vec<_> = crate::scenarios::all()
        .iter()
        .filter(|definition| options.tests.is_empty() || options.tests.contains(&definition.row))
        .collect();
    if selected.is_empty() {
        return Err(AhrbError::Validation("no tests selected".to_owned()));
    }
    Ok(selected)
}

fn row_timeout<T>(
    row: u8,
    result: Result<T>,
    row_errors: &mut BTreeMap<u8, String>,
    progress: &RunProgress,
) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(AhrbError::Timeout(detail)) => {
            let detail = format!("turn timeout: {detail}");
            row_errors.insert(row, detail.clone());
            progress.update(|state| {
                state.row_errors.insert(row, detail);
            })?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn write_deadline_report(
    options: &RunOptions,
    manifest: &Manifest,
    selected: &[&crate::scenarios::TestDefinition],
    progress: &RunProgress,
    persistence: &crate::results::RunPersistence,
    detail: &str,
) -> Result<()> {
    write_interrupted_report(
        options,
        manifest,
        selected,
        progress,
        persistence,
        detail,
        RunInterruption::Deadline,
    )
}

fn write_abort_report(
    options: &RunOptions,
    manifest: &Manifest,
    selected: &[&crate::scenarios::TestDefinition],
    progress: &RunProgress,
    persistence: &crate::results::RunPersistence,
    detail: &str,
) -> Result<()> {
    write_interrupted_report(
        options,
        manifest,
        selected,
        progress,
        persistence,
        detail,
        RunInterruption::Abort,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_interrupted_report(
    options: &RunOptions,
    manifest: &Manifest,
    selected: &[&crate::scenarios::TestDefinition],
    progress: &RunProgress,
    persistence: &crate::results::RunPersistence,
    detail: &str,
    interruption: RunInterruption,
) -> Result<()> {
    let manifest_hash = crate::manifest::hash(manifest)?;
    let selected_rows: Vec<u8> = selected.iter().map(|definition| definition.row).collect();
    let progress = progress.snapshot()?;
    let mut results: Vec<TestResult> = selected
        .iter()
        .map(|definition| TestResult {
            row: definition.row,
            id: definition.id.to_owned(),
            pillar: definition.pillar,
            outcome: match interruption {
                RunInterruption::Deadline if progress.completed.contains(&definition.row) => {
                    TestOutcome::Error("deadline interrupted final evaluation".to_owned())
                }
                RunInterruption::Deadline => TestOutcome::Error("deadline".to_owned()),
                RunInterruption::Abort => TestOutcome::Error("run aborted".to_owned()),
            },
            evidence: if interruption == RunInterruption::Abort {
                vec![
                    "run aborted before a trustworthy final report".to_owned(),
                    detail.to_owned(),
                ]
            } else if progress.completed.contains(&definition.row) {
                vec![
                    "row terminalized before the run deadline".to_owned(),
                    detail.to_owned(),
                ]
            } else if progress.launched.contains(&definition.row) {
                vec![
                    "row was active when the run deadline elapsed".to_owned(),
                    detail.to_owned(),
                ]
            } else {
                vec![
                    "row was not launched before the run deadline".to_owned(),
                    detail.to_owned(),
                ]
            },
            metadata: TestResultMetadata::for_row(
                definition.row,
                &TestOutcome::Error("run interrupted".to_owned()),
            ),
        })
        .map(|fallback| {
            if interruption == RunInterruption::Abort {
                return fallback;
            }
            progress
                .row_errors
                .get(&fallback.row)
                .map(|error| TestResult {
                    row: fallback.row,
                    id: fallback.id.clone(),
                    pillar: fallback.pillar,
                    outcome: TestOutcome::Error(error.clone()),
                    evidence: vec![error.clone()],
                    metadata: TestResultMetadata::for_row(
                        fallback.row,
                        &TestOutcome::Error(error.clone()),
                    ),
                })
                .or_else(|| progress.results.get(&fallback.row).cloned())
                .unwrap_or(fallback)
        })
        .collect();
    crate::report::record_capability_declarations(&mut results, manifest);
    let mut raw_events = Vec::new();
    for events in progress.events.values() {
        for event in events {
            raw_events.push(serde_json::to_value(event)?);
        }
    }
    let automation = automation_score(&results);
    let details = ReportDetails::from(BTreeMap::from([
        (
            "automation-score".to_owned(),
            json!({
                "profile": format!("{:?}", options.profile).to_lowercase(),
                "topology": manifest.concurrency.topology.clone(),
                "comparison_scope": "within-topology-only",
                "score": automation.score,
            }),
        ),
        (
            "resource-summary".to_owned(),
            json!({"measurement_complete": false}),
        ),
    ]));
    let report = Report {
        schema: 3,
        spec_version: 2,
        run_id: deterministic_run_id(&manifest_hash, &selected_rows),
        profile_path: persistence.profile_path.to_string_lossy().into_owned(),
        fingerprint: Fingerprint {
            harness: manifest.identity.id.clone(),
            harness_version: persistence.harness_version.clone(),
            manifest: manifest_hash,
            workflows: workflow_hash(),
            fake_model: env!("CARGO_PKG_VERSION").to_owned(),
            normalizer: env!("CARGO_PKG_VERSION").to_owned(),
            ahrb_revision: crate::results::ahrb_revision(),
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            host_memory_bytes: host_memory_bytes(),
            profile: format!("{:?}", options.profile).to_lowercase(),
        },
        results,
        details,
        resource_summary: ResourceSummary {
            topology: manifest.concurrency.topology.clone(),
            profile: format!("{:?}", options.profile).to_lowercase(),
            comparison_scope: "within-topology-only".to_owned(),
            ..ResourceSummary::default()
        },
        events: raw_events,
        ..Report::default()
    };
    crate::results::persist_report(persistence, &report, options.junit, true)
}

async fn run_inner(
    options: RunOptions,
    manifest: Manifest,
    progress: RunProgress,
    persistence: crate::results::RunPersistence,
) -> Result<i32> {
    let selected = selected_definitions(&options)?;
    let mut row_errors = BTreeMap::new();
    progress.update(|state| {
        for definition in &selected {
            let capability = crate::matrix_evidence::capability_for_row(&manifest, definition.row);
            if let crate::matrix_evidence::CapabilityStatus::Unsupported(reason)
            | crate::matrix_evidence::CapabilityStatus::Absent(reason) = &capability
            {
                let declaration = match &capability {
                    crate::matrix_evidence::CapabilityStatus::Unsupported(_) => Some(false),
                    crate::matrix_evidence::CapabilityStatus::Absent(_) => None,
                    crate::matrix_evidence::CapabilityStatus::Supported => Some(true),
                };
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    declaration,
                    &[],
                    None,
                );
                result.evidence.push(format!("capability: {reason}"));
                state.completed.insert(definition.row);
                state.results.insert(definition.row, result);
            }
        }
    })?;

    let manifest_hash = crate::manifest::hash(&manifest)?;
    let selected_rows: Vec<u8> = selected.iter().map(|definition| definition.row).collect();
    let run_id = deterministic_run_id(&manifest_hash, &selected_rows);
    let profile_root = persistence.profile_path.clone();
    prepare_profile(&manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare run profile: {error}")))?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);

    // Provider evidence must remain outside the harness process. When TCP loopback is
    // unavailable, `start_model` falls back to its Unix-domain HTTP transport.
    let embedded_model = false;
    let (workflow, actors_by_row) = build_workflow(
        &selected_rows,
        &profile_root,
        !embedded_model,
        options.profile,
        &manifest,
    )?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        embedded_model,
        &manifest.fake_model.base_url_env,
    )
    .await
    .map_err(|error| AhrbError::Protocol(format!("start fake model: {error}")))?;
    let server = Some(server);
    let credential = format!("ahrb-{}-{}", &manifest_hash[..16], std::process::id());
    let mut environment = isolated_environment(&manifest, &variables)?;
    environment.extend(model_environment.clone());
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MAX_OUTPUT_BYTES".to_owned(),
        manifest.resources.max_output_bytes.to_string(),
    );
    environment.insert(
        "AHRB_MOCK_TURN_TIMEOUT_MS".to_owned(),
        manifest.resources.turn_timeout_ms.to_string(),
    );
    inject_tariff_environment(&manifest, &mut environment);
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential.clone());
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(&manifest, &variables, &profile_root)?;
    if !manifest.hooks.acceptance.is_empty() {
        let hook = render_argv(&manifest.hooks.acceptance, &variables)?;
        environment.insert(
            "AHRB_MOCK_ACCEPTANCE_HOOK".to_owned(),
            serde_json::to_string(&hook)?,
        );
    }
    if !manifest.hooks.completion.is_empty() {
        let hook = render_argv(&manifest.hooks.completion, &variables)?;
        environment.insert(
            "AHRB_MOCK_COMPLETION_HOOK".to_owned(),
            serde_json::to_string(&hook)?,
        );
    }
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let mut driver = make_driver(
        &manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
    )?;
    driver
        .start()
        .await
        .map_err(|error| AhrbError::Protocol(format!("start harness driver: {error}")))?;
    driver
        .await_readiness()
        .await
        .map_err(|error| AhrbError::Protocol(format!("await harness readiness: {error}")))?;
    if crate::matrix_evidence::basic_session_surface(&manifest) {
        let warmup = driver
            .create_session("ahrb-warmup")
            .await
            .map_err(|error| AhrbError::Protocol(format!("warm-up readiness RPC: {error}")))?;
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&warmup).await?;
        }
    }

    let root_pid = if manifest.daemon.readiness.pid_pointer.is_empty() {
        await_owned_pid(&manifest, &variables)
            .await
            .map_err(|error| AhrbError::Protocol(format!("locate owned process: {error}")))?
    } else {
        Some(driver.daemon_pid().ok_or_else(|| {
            AhrbError::Protocol(
                "daemon readiness declared pid_pointer but the driver retained no PID".to_owned(),
            )
        })?)
    };
    let mut platform_sampler = platform_sampler();
    let resource_selected = selected_rows.iter().any(|row| {
        ((20..=29).contains(row) || *row == 49)
            && matches!(
                crate::matrix_evidence::capability_for_row(&manifest, *row),
                crate::matrix_evidence::CapabilityStatus::Supported
            )
    });
    let main_roots = if manifest.daemon.persistent {
        verified_process_roots(
            &manifest,
            platform_sampler.as_mut(),
            driver.owned_pids(),
            root_pid,
        )?
    } else {
        Vec::new()
    };
    let per_invocation_collection = if resource_selected && per_invocation_topology(&manifest) {
        match collect_per_invocation_resource_observations(
            &manifest,
            options.profile,
            &profile_root,
            &workflow,
            &model_environment,
            &credential,
            selected_rows.contains(&49),
        )
        .await
        {
            Ok(collection) => {
                progress.update(|state| {
                    for row in selected_rows
                        .iter()
                        .copied()
                        .filter(|row| (20..=29).contains(row) || *row == 49)
                    {
                        state.launched.insert(row);
                        state.completed.insert(row);
                    }
                })?;
                Some(collection)
            }
            Err(AhrbError::Timeout(detail)) => {
                for row in selected_rows
                    .iter()
                    .copied()
                    .filter(|row| (20..=29).contains(row) || *row == 49)
                {
                    let result: Result<()> = Err(AhrbError::Timeout(detail.clone()));
                    let _ = row_timeout(row, result, &mut row_errors, &progress)?;
                }
                None
            }
            Err(error) => {
                return Err(AhrbError::Protocol(format!(
                    "collect per-invocation resources: {error}"
                )));
            }
        }
    } else {
        None
    };
    let resource_evidence = if resource_selected
        && per_invocation_collection.is_none()
        && !per_invocation_topology(&manifest)
    {
        match collect_resource_evidence(
            &manifest,
            options.profile,
            &profile_root,
            &workflow,
            &model_environment,
            &credential,
        )
        .await
        {
            Ok(evidence) => {
                progress.update(|state| {
                    for row in selected_rows
                        .iter()
                        .copied()
                        .filter(|row| (20..=29).contains(row) || *row == 49)
                    {
                        state.launched.insert(row);
                        state.completed.insert(row);
                    }
                })?;
                Some(evidence)
            }
            Err(AhrbError::Timeout(detail)) => {
                for row in selected_rows
                    .iter()
                    .copied()
                    .filter(|row| (20..=29).contains(row) || *row == 49)
                {
                    let result: Result<()> = Err(AhrbError::Timeout(detail.clone()));
                    let _ = row_timeout(row, result, &mut row_errors, &progress)?;
                }
                None
            }
            Err(error) => {
                return Err(AhrbError::Protocol(format!("collect resources: {error}")));
            }
        }
    } else {
        None
    };
    let mut samples = if let Some(collection) = &per_invocation_collection {
        collection.samples.clone()
    } else if let Some(evidence) = &resource_evidence {
        evidence.series.samples.clone()
    } else {
        baseline_samples(platform_sampler.as_mut(), &main_roots, options.profile)
            .await
            .map_err(|error| AhrbError::Protocol(format!("sample warm idle: {error}")))?
    };
    let mut sessions: BTreeMap<u8, Vec<crate::driver::SessionId>> = BTreeMap::new();
    let mut cancel_cleanup_valid = None;
    let mut cancel_cleanup_detail = None;

    for (row, actor_names) in &actors_by_row {
        if (20..=29).contains(row) || matches!(*row, 42..=55 | 56..=58 | 60 | 63..=65) {
            continue;
        }
        if !matches!(
            crate::matrix_evidence::capability_for_row(&manifest, *row),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
            continue;
        }
        progress.update(|state| {
            state.launched.insert(*row);
        })?;
        let launched = async {
            let mut launched = Vec::new();
            for (index, actor_name) in actor_names.iter().enumerate() {
                let actor = workflow.actors.get(actor_name).ok_or_else(|| {
                    AhrbError::Protocol(format!("workflow actor {actor_name:?} disappeared"))
                })?;
                let session = driver
                    .create_session(&format!("{}:{actor_name}", workflow.scenario))
                    .await?;
                driver
                    .submit(
                        &session,
                        &actor.prompt,
                        &format!("row-{row}-turn-{}", index + 1),
                    )
                    .await?;
                launched.push(session);
            }
            Ok(launched)
        }
        .await;
        if let Some(launched) = row_timeout(*row, launched, &mut row_errors, &progress)? {
            sessions.insert(*row, launched);
        }
        if *row == 36
            && let Some(session) = sessions.get(&36).and_then(|items| items.first())
        {
            let row_result: Result<()> = async {
                wait_for_session_event(
                    &mut driver,
                    session,
                    EventVocab::ModelRequest,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?;
                let roots = driver.session_pids(session);
                driver.cancel(session).await?;
                if manifest.transport.kind == TransportKind::Exec {
                    // A persistent daemon is expected to outlive the cancelled
                    // turn's thin client (verified: haider 0.0.967 lingers for its
                    // idle TTL, re-parented to init). Cancel cleanup asks only that
                    // the client subtree of THIS turn is gone, so exclude the
                    // declared daemon root from the "settled" test.
                    let excluded: Vec<u32> = driver.daemon_pid().into_iter().collect();
                    let process_cleared = !roots.is_empty()
                        && await_owned_tree_settled(
                            platform_sampler.as_mut(),
                            &roots,
                            &excluded,
                            Duration::from_millis(manifest.daemon.grace_ms.max(100)),
                        )
                        .await?;
                    let workspace_cleared =
                        per_invocation_workspaces_clean(&profile_root, session)?;
                    cancel_cleanup_valid = Some(process_cleared && workspace_cleared);
                    cancel_cleanup_detail = Some(format!(
                        "stopped the run and terminated {} thin-client process root(s): cleared={process_cleared}; driver and harness workspaces clean={workspace_cleared}",
                        roots.len()
                    ));
                } else {
                    cancel_cleanup_valid = Some(true);
                    cancel_cleanup_detail = Some(
                        "shared controller acknowledged session cancellation; terminal evidence verifies cleanup"
                            .to_owned(),
                    );
                }
                Ok(())
            }
            .await;
            let _ = row_timeout(36, row_result, &mut row_errors, &progress)?;
        }
    }

    let mut precollected_events = BTreeMap::new();
    let mut session_replay_valid = None;
    let mut session_replay_detail = None;
    let mut resume_idempotency_valid = None;
    let mut resume_idempotency_detail = None;
    let mut control_evidence = Vec::new();
    if let Some(session) = sessions.get(&16).and_then(|items| items.first()) {
        let row_result: Result<Vec<NormalizedEvent>> = async {
            let mut after = None;
            let mut transcript = Vec::new();
            let first = collect_session_terminal(
                &mut driver,
                session,
                after,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
            .await?;
            after = first.iter().map(|event| Cursor(event.cursor)).max();
            transcript.extend(first);
            for (turn, actor) in [(2_u8, "r16t2"), (3_u8, "r16t3")] {
                let prompt = format!(
                    "AHRB matrix row 16 turn {turn} {}",
                    route_marker(&workflow.scenario, actor, "start")
                );
                driver
                    .submit(session, &prompt, &format!("row-16-turn-{turn}"))
                    .await?;
                let suffix = collect_session_terminal(
                    &mut driver,
                    session,
                    after,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?;
                after = suffix
                    .iter()
                    .map(|event| Cursor(event.cursor))
                    .max()
                    .or(after);
                transcript.extend(suffix);
            }
            Ok(transcript)
        }
        .await;
        if let Some(transcript) = row_timeout(16, row_result, &mut row_errors, &progress)? {
            progress.update(|state| {
                state.completed.insert(16);
                state.events.insert(16, transcript.clone());
            })?;
            precollected_events.insert(16_u8, transcript);
        }
    }

    if let Some(session) = sessions.get(&30).and_then(|items| items.first()) {
        let row_result: Result<Vec<NormalizedEvent>> = async {
            let original = collect_session_terminal(
                &mut driver,
                session,
                None,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
            .await?;
            let after = original
                .first()
                .map(|event| Cursor(event.cursor))
                .ok_or_else(|| AhrbError::Protocol("row-30 replay source was empty".to_owned()))?;
            let native_replay = manifest.transport.kind == TransportKind::Exec
                && !manifest.events.replay_command.is_empty()
                && manifest.sessions.continue_turn.is_empty()
                && manifest.sessions.resume.is_empty();
            let suffix = if native_replay {
                driver.replay_persisted(session, Some(after)).await?
            } else {
                driver.resume(session).await?;
                driver.attach(session, Some(after)).await?
            };
            let suffix_validation = validate_recovered_suffix(&original, Some(after), &suffix);
            if native_replay {
                match suffix_validation {
                    Ok(()) => {
                        session_replay_valid = Some(true);
                        session_replay_detail = Some(format!(
                            "native replay returned {} exact events strictly after cursor {}",
                            suffix.len(),
                            after.0
                        ));
                    }
                    Err(detail) => {
                        session_replay_valid = Some(false);
                        session_replay_detail = Some(detail);
                    }
                }
                return Ok(original);
            }
            let last_a = original
                .last()
                .map(|event| Cursor(event.cursor))
                .ok_or_else(|| AhrbError::Protocol("row-30 replay source was empty".to_owned()))?;
            let turn_b_prompt = format!(
                "AHRB matrix row 30 continued turn B {}",
                route_marker(&workflow.scenario, "r30b", "start")
            );
            driver
                .submit(session, &turn_b_prompt, "row-30-turn-2")
                .await?;
            let turn_b = collect_session_terminal(
                &mut driver,
                session,
                Some(last_a),
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
            .await?;
            let mut transcript = original;
            transcript.extend(turn_b);
            let accepted = transcript
                .iter()
                .filter(|event| event.event == EventVocab::TurnAccepted)
                .count();
            let terminals = transcript
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count();
            let continued_b = accepted == 2 && terminals == 2;
            match suffix_validation {
                Ok(()) if continued_b => {
                    session_replay_valid = Some(true);
                    session_replay_detail = Some(format!(
                        "reopened the persisted session journal, replayed {} exact events strictly after cursor {}, then continued turn B in the same session",
                        suffix.len(),
                        after.0
                    ));
                }
                Ok(()) => {
                    session_replay_valid = Some(false);
                    session_replay_detail = Some(format!(
                        "replay suffix was exact, but turn B did not complete distinctly: accepted={accepted}, terminals={terminals}"
                    ));
                }
                Err(detail) => {
                    session_replay_valid = Some(false);
                    session_replay_detail = Some(detail);
                }
            }
            Ok(transcript)
        }
        .await;
        if let Some(transcript) = row_timeout(30, row_result, &mut row_errors, &progress)? {
            progress.update(|state| {
                state.completed.insert(30);
                state.events.insert(30, transcript.clone());
            })?;
            precollected_events.insert(30_u8, transcript);
        }
    }

    if let Some(session) = sessions.get(&37).and_then(|items| items.first()) {
        let row_result: Result<Vec<NormalizedEvent>> = async {
            let original = collect_session_terminal(
                &mut driver,
                session,
                None,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
            .await?;
            let native_resume = manifest.transport.kind == TransportKind::Exec
                && !manifest.sessions.resume_control.is_empty();
            let replayed = if native_resume {
                let requests_before = engine
                    .request_records()
                    .await
                    .iter()
                    .filter(|record| record.request.actor.starts_with("r37"))
                    .count();
                let before_controls = driver.control_evidence(session).len();
                driver.resume(session).await?;
                driver.resume(session).await?;
                let control = driver.control_evidence(session);
                let new_control = control.get(before_controls..).unwrap_or_default();
                if new_control.len() != 2
                    || !new_control.iter().all(control_response_succeeded)
                {
                    return Err(AhrbError::Protocol(format!(
                        "two resume attempts produced {} successful JSON control responses; responses: {}",
                        new_control
                            .iter()
                            .filter(|value| control_response_succeeded(value))
                            .count(),
                        serde_json::to_string(new_control).unwrap_or_default()
                    )));
                }
                control_evidence.extend(new_control.iter().map(|response| {
                    json!({"session_id":session.0,"action":"resume","response":response})
                }));
                let durable = driver.replay_persisted(session, None).await?;
                let requests_after = engine
                    .request_records()
                    .await
                    .iter()
                    .filter(|record| record.request.actor.starts_with("r37"))
                    .count();
                if requests_after != requests_before {
                    return Err(AhrbError::Protocol(format!(
                        "resume idempotency generated {} additional model request(s)",
                        requests_after.saturating_sub(requests_before)
                    )));
                }
                durable
            } else {
                driver.resume(session).await?;
                let actor = workflow.actors.get("r37").ok_or_else(|| {
                    AhrbError::Protocol("row-37 workflow actor is absent".to_owned())
                })?;
                driver
                    .submit(session, &actor.prompt, "row-37-turn-1")
                    .await?;
                collect_session_terminal(
                    &mut driver,
                    session,
                    None,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?
            };
            let accepted = original
                .iter()
                .filter(|event| event.event == EventVocab::TurnAccepted)
                .count();
            let effects = original
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count();
            let terminals = original
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count();
            let unchanged = validate_recovered_suffix(&original, None, &replayed).is_ok();
            let valid = unchanged && accepted == 1 && effects == 1 && terminals == 1;
            resume_idempotency_valid = Some(valid);
            let mechanism = if native_resume {
                "two successful JSON resume responses produced no new model requests and native replay"
            } else {
                "disk reopen plus duplicate submit"
            };
            resume_idempotency_detail = Some(format!(
                "{mechanism} preserved the exact durable session journal: unchanged={unchanged}, accepted={accepted}, committed_effects={effects}, terminals={terminals}"
            ));
            Ok(original)
        }
        .await;
        if let Some(replayed) = row_timeout(37, row_result, &mut row_errors, &progress)? {
            progress.update(|state| {
                state.completed.insert(37);
                state.events.insert(37, replayed.clone());
            })?;
            precollected_events.insert(37_u8, replayed);
        }
    }

    if let Some(parent) = sessions.get(&18).and_then(|items| items.first()) {
        let result = driver
            .spawn_agent(parent, "ahrb-matrix-v1:r18-child", None)
            .await;
        let _ = row_timeout(18, result, &mut row_errors, &progress)?;
    }
    if let Some(session) = sessions.get(&31).and_then(|items| items.first()) {
        let result = driver.steer(session, "row-31 safe-boundary steer").await;
        let _ = row_timeout(31, result, &mut row_errors, &progress)?;
    }
    if let Some(session) = sessions.get(&32).and_then(|items| items.first()) {
        let result = driver
            .subturn(session, "row-32 pre-tool intervention")
            .await;
        let _ = row_timeout(32, result, &mut row_errors, &progress)?;
    }
    if let Some(session) = sessions.get(&33).and_then(|items| items.first()) {
        let actor = workflow
            .actors
            .get("r33")
            .ok_or_else(|| AhrbError::Protocol("row-33 workflow actor is absent".to_owned()))?;
        let result = driver
            .queue(session, &actor.prompt, "row-33-queued-turn")
            .await;
        let _ = row_timeout(33, result, &mut row_errors, &progress)?;
    }
    let recovery_requested = [35_u8, 40].iter().any(|row| {
        selected_rows.contains(row)
            && matches!(
                crate::matrix_evidence::capability_for_row(&manifest, *row),
                crate::matrix_evidence::CapabilityStatus::Supported
            )
    });
    let native_recovery = manifest.transport.kind == TransportKind::Exec
        && !manifest.sessions.recover_probe.is_empty();
    let native_journal_replay = manifest.transport.kind == TransportKind::Exec
        && !manifest.events.replay_command.is_empty()
        && manifest.events.source == "stdout";
    let crash_pre_events = if recovery_requested {
        if let Some(session) = sessions.get(&35).and_then(|items| items.first()) {
            let result = if native_recovery {
                collect_session_terminal(
                    &mut driver,
                    session,
                    None,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await
            } else {
                collect_session_checkpoint(
                    &mut driver,
                    session,
                    "row-35-post-commit",
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await
            };
            row_timeout(35, result, &mut row_errors, &progress)?
        } else {
            None
        }
    } else {
        None
    };
    let journal_pre_events = if recovery_requested {
        if let Some(session) = sessions.get(&40).and_then(|items| items.first()) {
            let result = if native_journal_replay {
                collect_session_terminal(
                    &mut driver,
                    session,
                    None,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await
            } else {
                collect_session_checkpoint(
                    &mut driver,
                    session,
                    "row-40-post-commit",
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await
            };
            row_timeout(40, result, &mut row_errors, &progress)?
        } else {
            None
        }
    } else {
        None
    };
    let needs_recovery =
        (crash_pre_events.is_some() || journal_pre_events.is_some()) && recovery_requested;

    let parallel_agents = per_invocation_collection
        .as_ref()
        .and_then(|collection| {
            collection
                .observations
                .iter()
                .map(|point| point.agents)
                .max()
        })
        .or_else(|| {
            resource_evidence
                .as_ref()
                .and_then(|evidence| evidence.sweep.iter().map(|point| point.agents).max())
        })
        .map_or(0, |agents| agents as usize);

    let sessions_to_collect: BTreeMap<_, _> = sessions
        .iter()
        .filter(|(row, _)| {
            !(precollected_events.contains_key(row) || needs_recovery && matches!(**row, 35 | 40))
        })
        .map(|(row, sessions)| (*row, sessions.clone()))
        .collect();
    let (mut events, terminal_errors) = collect_terminals(
        &mut driver,
        &sessions_to_collect,
        Duration::from_millis(manifest.resources.turn_timeout_ms),
        &progress,
    )
    .await?;
    row_errors.extend(terminal_errors);
    events.extend(precollected_events);
    if let Some(pre_crash) = &crash_pre_events {
        events.insert(35, pre_crash.clone());
    }
    if let Some(pre_crash) = &journal_pre_events {
        events.insert(40, pre_crash.clone());
    }
    progress.update(|state| {
        for (row, row_events) in &events {
            state.events.insert(*row, row_events.clone());
            if !row_errors.contains_key(row) && !matches!(*row, 35 | 40) {
                state.completed.insert(*row);
            }
        }
    })?;

    // A complete resource collection is one coherent sampler timeline. Do not
    // append a one-off sample from the main driver's independent CPU tracker:
    // doing so would make samples.jsonl's last-first CPU and peak disagree with
    // the reproducible summary. Non-resource runs retain the diagnostic sample.
    if !main_roots.is_empty() && resource_evidence.is_none() {
        let tree = platform_sampler.discover(&main_roots)?;
        samples.push(platform_sampler.sample(&tree, "post-turn")?);
    }

    let mut crash_recovery_ms = None;
    let mut crash_recovery_tree_cleared = None;
    let mut crash_recovery_valid = None;
    let mut crash_recovery_detail = None;
    let mut journal_recovered_events = None;
    let mut journal_recovery_valid = None;
    let mut journal_recovery_detail = None;
    let mut journal_torn_tail_injected = None;
    let mut journal_native_replay_valid = None;
    let mut lifecycle_notes = resource_evidence
        .as_ref()
        .map(|evidence| evidence.lifecycle_notes.clone())
        .unwrap_or_default();
    if needs_recovery {
        let recovery_started = Instant::now();
        let recovery_roots = if main_roots.is_empty() {
            driver.owned_pids()
        } else {
            main_roots.clone()
        };
        let owned_tree = platform_sampler.discover(&recovery_roots)?;
        let had_owned_process = !owned_tree.members.is_empty();
        signal_owned_tree(&owned_tree)?;
        let recovery_grace = Duration::from_millis(manifest.daemon.grace_ms.max(100));
        if let Ok(reap_result) =
            tokio::time::timeout(recovery_grace, driver.reap_after_external_kill()).await
        {
            reap_result.map_err(|error| {
                AhrbError::Protocol(format!(
                    "reap externally killed recovery launchers: {error}"
                ))
            })?;
        }
        drop(driver);
        let tree_cleared = had_owned_process
            && await_owned_tree_empty(platform_sampler.as_mut(), &recovery_roots, recovery_grace)
                .await?;
        crash_recovery_tree_cleared = Some(tree_cleared);
        if tree_cleared {
            if let Some(session) = sessions.get(&40).and_then(|items| items.first()) {
                if native_journal_replay {
                    // Native replay proves restart durability, not torn-tail
                    // recovery: AHRB never mutates harness-owned storage.
                    journal_torn_tail_injected = Some(false);
                } else {
                    match inject_torn_journal_tail(&manifest, &variables, session) {
                        Ok(()) => journal_torn_tail_injected = Some(true),
                        Err(error) => {
                            journal_torn_tail_injected = Some(false);
                            journal_recovery_valid = Some(false);
                            journal_recovery_detail =
                                Some(format!("could not induce a durable torn tail: {error}"));
                        }
                    }
                }
            }
            let mut recovered = make_driver(
                &manifest,
                &command,
                &environment,
                &variables,
                &profile_root,
                false,
            )?;
            recovered.start().await?;
            recovered.await_readiness().await?;
            crash_recovery_ms = Some(recovery_started.elapsed().as_secs_f64() * 1_000.0);
            if let Some(session) = sessions.get(&35).and_then(|items| items.first()) {
                if native_recovery {
                    let recovered_result: Result<Vec<NormalizedEvent>> = async {
                        let requests_before = engine
                            .request_records()
                            .await
                            .iter()
                            .filter(|record| record.request.actor.starts_with("r35"))
                            .count();
                        let before_controls = recovered.control_evidence(session).len();
                        recovered.recover_probe(session).await?;
                        let control = recovered.control_evidence(session);
                        let response = control.get(before_controls).ok_or_else(|| {
                            AhrbError::Protocol(
                                "native recovery probe produced no JSON evidence".to_owned(),
                            )
                        })?;
                        // The row-35 pre-crash turn ran to TERMINAL before the
                        // owned tree was signalled, so a typed "nothing to
                        // reconcile" answer (verified on haider 0.0.967:
                        // error.code="no_recovery", "run_state is idle") is the
                        // correct probe response; the durable replay and the
                        // suffix validation below remain the arbiter.
                        let no_recovery_window = typed_no_recovery_response(response);
                        if !no_recovery_window && !control_response_succeeded(response) {
                            return Err(AhrbError::Protocol(format!(
                                "native recovery probe reported failure: {response}"
                            )));
                        }
                        control_evidence.push(json!({
                            "session_id": session.0,
                            "action": "recover-probe",
                            "no_recovery_window": no_recovery_window,
                            "response": response
                        }));
                        let durable = recovered.replay_persisted(session, None).await?;
                        let original = crash_pre_events.as_deref().unwrap_or_default();
                        validate_recovered_suffix(original, None, &durable)
                            .map_err(AhrbError::Protocol)?;
                        let requests_after = engine
                            .request_records()
                            .await
                            .iter()
                            .filter(|record| record.request.actor.starts_with("r35"))
                            .count();
                        if requests_after != requests_before {
                            return Err(AhrbError::Protocol(format!(
                                "recovery generated {} additional model request(s)",
                                requests_after.saturating_sub(requests_before)
                            )));
                        }
                        Ok(durable)
                    }
                    .await;
                    if let Some(recovered_events) =
                        row_timeout(35, recovered_result, &mut row_errors, &progress)?
                    {
                        let durable_events = recovered_events.len();
                        let original = crash_pre_events.as_deref().unwrap_or_default();
                        let accepted = original
                            .iter()
                            .filter(|event| event.event == EventVocab::TurnAccepted)
                            .count();
                        let effects = original
                            .iter()
                            .filter(|event| event.event == EventVocab::ToolResult)
                            .count();
                        let terminals = original
                            .iter()
                            .filter(|event| is_terminal(&event.event))
                            .count();
                        let valid = accepted == 1 && effects == 1 && terminals == 1;
                        crash_recovery_valid = Some(valid);
                        crash_recovery_detail = Some(format!(
                            "daemon respawn, successful recovery-probe JSON, and {durable_events} fresh durable replay events preserved accepted={accepted}, committed_effects={effects}, terminals={terminals} with no new model request"
                        ));
                        progress.update(|state| {
                            state.completed.insert(35);
                            state.events.insert(35, original.to_vec());
                        })?;
                        events.insert(35, original.to_vec());
                    }
                } else {
                    let release_token = crash_pre_events.as_ref().and_then(|pre_crash| {
                        pre_crash.iter().find_map(|event| {
                            (event.event == EventVocab::BarrierReached
                                && event.payload.get("name").and_then(Value::as_str)
                                    == Some("row-35-post-commit"))
                            .then(|| event.payload.get("release_token").and_then(Value::as_str))
                            .flatten()
                        })
                    });
                    if let Some(release_token) = release_token {
                        let recovered_result: Result<Vec<NormalizedEvent>> = async {
                            recovered.resume(session).await?;
                            let actor = workflow.actors.get("r35").ok_or_else(|| {
                                AhrbError::Protocol("row-35 workflow actor is absent".to_owned())
                            })?;
                            recovered
                                .submit(session, &actor.prompt, "row-35-turn-1")
                                .await?;
                            recovered.release_checkpoint(session, release_token).await?;
                            collect_session_terminal(
                                &mut recovered,
                                session,
                                None,
                                Duration::from_millis(manifest.resources.turn_timeout_ms),
                            )
                            .await
                        }
                        .await;
                        if let Some(recovered_events) =
                            row_timeout(35, recovered_result, &mut row_errors, &progress)?
                        {
                            let accepted = recovered_events
                                .iter()
                                .filter(|event| event.event == EventVocab::TurnAccepted)
                                .count();
                            let effects = recovered_events
                                .iter()
                                .filter(|event| event.event == EventVocab::ToolResult)
                                .count();
                            let terminals = recovered_events
                                .iter()
                                .filter(|event| is_terminal(&event.event))
                                .count();
                            let valid = accepted == 1 && effects == 1 && terminals == 1;
                            crash_recovery_valid = Some(valid);
                            crash_recovery_detail = Some(format!(
                                "post-commit restart+attach+resume+duplicate-submit observed accepted={accepted}, committed_effects={effects}, terminals={terminals}"
                            ));
                            progress.update(|state| {
                                state.completed.insert(35);
                                state.events.insert(35, recovered_events.clone());
                            })?;
                            events.insert(35, recovered_events);
                        }
                    } else {
                        crash_recovery_valid = Some(false);
                        crash_recovery_detail = Some(
                            "named post-commit checkpoint omitted its durable release token"
                                .to_owned(),
                        );
                    }
                }
            }
            if let Some(session) = sessions.get(&40).and_then(|items| items.first()) {
                let original = events.get(&40).cloned().unwrap_or_default();
                let after = original
                    .first()
                    .map(|event| crate::driver::Cursor(event.cursor));
                match recovered.replay_persisted(session, after).await {
                    Ok(suffix) => {
                        journal_recovered_events = Some(suffix.len());
                        match validate_recovered_suffix(&original, after, &suffix) {
                            Ok(()) => {
                                if journal_torn_tail_injected == Some(true) || native_journal_replay
                                {
                                    journal_recovery_valid = Some(true);
                                    journal_native_replay_valid = Some(native_journal_replay);
                                    progress.update(|state| {
                                        state.completed.insert(40);
                                    })?;
                                    journal_recovery_detail = Some(if native_journal_replay {
                                        format!(
                                            "native journal replay survived daemon restart with {} exact, contiguous, duplicate-free events",
                                            suffix.len()
                                        )
                                    } else {
                                        format!(
                                            "replayed {} exact, contiguous, duplicate-free events and cleanly ignored the induced torn tail",
                                            suffix.len()
                                        )
                                    });
                                }
                            }
                            Err(detail) => {
                                journal_recovery_valid = Some(false);
                                journal_recovery_detail = Some(detail);
                            }
                        }
                    }
                    Err(AhrbError::Timeout(detail)) => {
                        let result: Result<()> = Err(AhrbError::Timeout(detail));
                        let _ = row_timeout(40, result, &mut row_errors, &progress)?;
                    }
                    Err(error) => {
                        journal_recovery_valid = Some(false);
                        journal_recovery_detail = Some(format!(
                            "journal replay could not be decoded after restart: {error}"
                        ));
                    }
                }
            }
            recovered.shutdown().await?;
            lifecycle_notes.extend(recovered.lifecycle_notes());
        } else {
            crash_recovery_valid = Some(false);
            crash_recovery_detail =
                Some("owned process tree was not fully gone; refused crash-resume".to_owned());
            journal_recovery_valid = Some(false);
            journal_recovery_detail = Some(
                "owned process tree was not fully gone; refused to start a second journal owner"
                    .to_owned(),
            );
        }
    } else {
        driver.shutdown().await?;
        lifecycle_notes.extend(driver.lifecycle_notes());
    }

    // Derived-row workloads own independent drivers and provider ledgers. Run
    // them only after the v1 driver has completed its terminal or recovery
    // lifecycle so their duration cannot change any v1 observation.
    let row42_trials = if selected_rows.contains(&42) || selected_rows.contains(&44) {
        match collect_model_request_efficiency_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
            selected_rows.contains(&44),
        )
        .await
        {
            Ok(trials) => {
                let evidence_row = if selected_rows.contains(&42) { 42 } else { 44 };
                events.insert(evidence_row, trials.events.clone());
                Some(trials)
            }
            Err(AhrbError::Timeout(detail)) => {
                for row in [42_u8, 44_u8] {
                    if selected_rows.contains(&row) {
                        let result: Result<()> = Err(AhrbError::Timeout(detail.clone()));
                        let _ = row_timeout(row, result, &mut row_errors, &progress)?;
                    }
                }
                None
            }
            Err(error) => {
                return Err(AhrbError::Protocol(format!(
                    "collect shared model-request-efficiency/process-hygiene sequence: {error}"
                )));
            }
        }
    } else {
        None
    };
    let row43_trials = if selected_rows.contains(&43) {
        match collect_turn_latency_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                events.insert(43_u8, trials.events.clone());
                Some(trials)
            }
            Err(AhrbError::Timeout(detail)) => {
                let result: Result<()> = Err(AhrbError::Timeout(detail));
                let _ = row_timeout(43, result, &mut row_errors, &progress)?;
                None
            }
            Err(error) => {
                return Err(AhrbError::Protocol(format!(
                    "collect turn-latency-distribution: {error}"
                )));
            }
        }
    } else {
        None
    };
    let row50_trials = if selected_rows.contains(&50)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 50),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_session_residue_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(50_u8, trials.events.clone());
                samples.extend(trials.samples.iter().cloned());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("session-residue-sweep evidence collection: {error}");
                row_errors.insert(50, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(50, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row45_trials = if selected_rows.contains(&45) {
        match collect_time_to_first_model_request_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(45_u8, trials.events.clone());
                Some(trials)
            }
            Err(AhrbError::Timeout(detail)) => {
                let result: Result<()> = Err(AhrbError::Timeout(detail));
                let _ = row_timeout(45, result, &mut row_errors, &progress)?;
                None
            }
            Err(error) => {
                let detail = format!("time-to-first-model-request evidence collection: {error}");
                row_errors.insert(45, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(45, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row46_trials = if selected_rows.contains(&46) {
        match collect_memory_time_integral_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(46_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("memory-time-integral evidence collection: {error}");
                row_errors.insert(46, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(46, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row47_trials = if selected_rows.contains(&47) {
        match collect_disk_io_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                events.insert(47_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("disk-io-per-turn evidence collection: {error}");
                row_errors.insert(47, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(47, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row48_trials = if selected_rows.contains(&48) {
        match collect_model_wait_cpu_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(48_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("model-wait-cpu evidence collection: {error}");
                row_errors.insert(48, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(48, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row51_trials = if selected_rows.contains(&51)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 51),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_context_recovery_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(51_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("context-limit-recovery evidence collection: {error}");
                row_errors.insert(51, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(51, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row52_trials = if selected_rows.contains(&52)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 52),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_resume_latency_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(52_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("resume-latency-vs-length evidence collection: {error}");
                row_errors.insert(52, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(52, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row53_trials = if selected_rows.contains(&53)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 53),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_journal_torn_tail_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => Some(trials),
            Err(error) => {
                let detail = format!("journal-torn-tail-sweep evidence collection: {error}");
                row_errors.insert(53, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(53, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row54_trials = if (selected_rows.contains(&54) || selected_rows.contains(&55))
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 54),
            crate::matrix_evidence::CapabilityStatus::Supported
        )
        && (options.profile == Profile::Quick
            || manifest
                .concurrency
                .max_agents
                .is_some_and(|value| value >= 32))
    {
        match collect_fanout_trials(&manifest, options.profile, &profile_root, &manifest_hash).await
        {
            Ok(trials) => {
                if selected_rows.contains(&54) {
                    events.insert(54_u8, trials.events.clone());
                }
                if selected_rows.contains(&55) {
                    events.insert(55_u8, trials.events.clone());
                }
                samples.extend(trials.samples.iter().cloned());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("fanout/fairness evidence collection: {error}");
                for row in [54_u8, 55_u8] {
                    if selected_rows.contains(&row) {
                        row_errors.insert(row, detail.clone());
                    }
                }
                progress.update(|state| {
                    for row in [54_u8, 55_u8] {
                        if selected_rows.contains(&row) {
                            state.row_errors.insert(row, detail.clone());
                        }
                    }
                })?;
                None
            }
        }
    } else {
        None
    };
    let row56_trials = if selected_rows.contains(&56)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 56),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_child_failure_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(56_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("child-failure-propagation evidence collection: {error}");
                row_errors.insert(56, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(56, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row57_trials = if selected_rows.contains(&57)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 57),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_signal_matrix_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(57_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("signal-matrix evidence collection: {error}");
                row_errors.insert(57, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(57, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row58_trials = if selected_rows.contains(&58)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 58),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_retry_budget_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                events.insert(58_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("retry-budget evidence collection: {error}");
                row_errors.insert(58, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(58, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row59_trials = if selected_rows.contains(&59) {
        match collect_slow_stream_stall_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(59_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("slow-stream-vs-stall evidence collection: {error}");
                row_errors.insert(59, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(59, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row60_trials = if selected_rows.contains(&60) {
        match collect_large_output_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                events.insert(60_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("large-tool-output evidence collection: {error}");
                row_errors.insert(60, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(60, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row61_trials = if selected_rows.contains(&61) {
        match collect_workspace_fault_trials(
            &manifest,
            options.profile,
            &profile_root,
            &manifest_hash,
        )
        .await
        {
            Ok(trials) => {
                events.insert(61_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("workspace-fault evidence collection: {error}");
                row_errors.insert(61, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(61, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row62_trials = if selected_rows.contains(&62) {
        match collect_offline_mode_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                events.insert(62_u8, trials.events.clone());
                Some(trials)
            }
            Err(error) => {
                let detail = format!("egress enforcement unavailable: {error}");
                row_errors.insert(62, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(62, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row65_trials = if selected_rows.contains(&65)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 65),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_injection_surface_trials(&manifest, &profile_root, &manifest_hash).await {
            Ok(trials) => Some(trials),
            Err(error) => {
                let detail = format!("injection-surface evidence collection: {error}");
                row_errors.insert(65, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(65, detail);
                })?;
                None
            }
        }
    } else {
        None
    };
    let row66_evaluation = if selected_rows.contains(&66)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 66),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_budget_enforcement(
            &manifest,
            &variables,
            &environment,
            engine.as_ref(),
            options.profile,
        )
        .await
        {
            Ok(evaluation) => evaluation,
            Err(error) => {
                let detail = format!("budget-enforcement evidence collection: {error}");
                row_errors.insert(66, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(66, detail);
                })?;
                crate::wave4::Wave4Evaluation::default()
            }
        }
    } else {
        crate::wave4::Wave4Evaluation::default()
    };
    let row70_evaluation = if selected_rows.contains(&70)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 70),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_headless_permissions(
            &manifest,
            &variables,
            &environment,
            &profile_root,
            options.profile,
        )
        .await
        {
            Ok(evaluation) => evaluation,
            Err(error) => {
                let detail = format!("headless-permission evidence collection: {error}");
                row_errors.insert(70, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(70, detail);
                })?;
                crate::wave4::Wave4Evaluation::default()
            }
        }
    } else {
        crate::wave4::Wave4Evaluation::default()
    };
    let row68_evaluation = if selected_rows.contains(&68)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 68),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        match collect_session_cli(&manifest, &variables, &environment, options.profile).await {
            Ok(evaluation) => evaluation,
            Err(error) => {
                let detail = format!("session-ops-cli evidence collection: {error}");
                row_errors.insert(68, detail.clone());
                progress.update(|state| {
                    state.row_errors.insert(68, detail);
                })?;
                crate::wave4::Wave4Evaluation {
                    details: json!({"lifecycles": []}),
                    ..crate::wave4::Wave4Evaluation::default()
                }
            }
        }
    } else {
        crate::wave4::Wave4Evaluation {
            details: json!({"lifecycles": []}),
            ..crate::wave4::Wave4Evaluation::default()
        }
    };
    let determinism_trials = if selected_rows.contains(&63) || selected_rows.contains(&64) {
        match collect_determinism_trials(&manifest, options.profile, &profile_root, &manifest_hash)
            .await
        {
            Ok(trials) => {
                if selected_rows.contains(&63) {
                    events.insert(63_u8, trials.events.clone());
                }
                if selected_rows.contains(&64) {
                    events.insert(64_u8, trials.events.clone());
                }
                Some(trials)
            }
            Err(error) => {
                let detail = format!("cross-execution determinism evidence collection: {error}");
                for row in [63_u8, 64_u8] {
                    if selected_rows.contains(&row) {
                        row_errors.insert(row, detail.clone());
                    }
                }
                progress.update(|state| {
                    for row in [63_u8, 64_u8] {
                        if selected_rows.contains(&row) {
                            state.row_errors.insert(row, detail.clone());
                        }
                    }
                })?;
                None
            }
        }
    } else {
        None
    };

    let state = RunState {
        events,
        sessions,
        samples,
        session_replay_valid,
        session_replay_detail,
        crash_recovery_ms,
        crash_recovery_tree_cleared,
        crash_recovery_valid,
        crash_recovery_detail,
        journal_recovered_events,
        journal_recovery_valid,
        journal_recovery_detail,
        journal_torn_tail_injected,
        journal_native_replay_valid,
        lifecycle_notes,
        control_evidence,
        cancel_cleanup_valid,
        cancel_cleanup_detail,
        resume_idempotency_valid,
        resume_idempotency_detail,
        parallel_agents,
        resource_evidence,
        per_invocation_membership: per_invocation_collection
            .as_ref()
            .map(|collection| collection.membership.clone())
            .unwrap_or_default(),
        per_invocation_turn_wall_ns: per_invocation_collection
            .as_ref()
            .map(|collection| collection.turn_wall_ns.clone())
            .unwrap_or_default(),
        long_horizon_turns: per_invocation_collection
            .as_ref()
            .map(|collection| collection.long_horizon_turns.clone())
            .unwrap_or_default(),
        per_invocation_resources: per_invocation_collection
            .map(|collection| collection.observations)
            .unwrap_or_default(),
        row_errors,
    };
    let mut request_records = engine.request_records().await;
    if let Some(trials) = &row42_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row43_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row50_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row45_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row46_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row47_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row48_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row51_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row52_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row57_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row58_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row59_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row60_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row61_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row62_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row65_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &determinism_trials {
        request_records.extend(trials.requests.clone());
    }
    if row42_trials.is_some()
        || row43_trials.is_some()
        || row50_trials.is_some()
        || row45_trials.is_some()
        || row46_trials.is_some()
        || row47_trials.is_some()
        || row48_trials.is_some()
        || row51_trials.is_some()
        || row52_trials.is_some()
        || row57_trials.is_some()
        || row58_trials.is_some()
        || row59_trials.is_some()
        || row60_trials.is_some()
        || row61_trials.is_some()
        || row62_trials.is_some()
        || row65_trials.is_some()
        || determinism_trials.is_some()
    {
        request_records.sort_by(|left, right| {
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
    }
    if let Some(active_server) = server {
        active_server.shutdown().await?;
    }
    // Final cleanup is part of the run outcome, not a post-report afterthought:
    // a protocol-level residue failure must be persisted as an aborted run.
    ensure_owned_cleanup()?;

    let resource_evidence = state
        .resource_evidence
        .clone()
        .unwrap_or_else(|| incomplete_resource_evidence(&state, &manifest));
    let mut resource_certification =
        if per_invocation_topology(&manifest) && !state.per_invocation_resources.is_empty() {
            evaluate_per_invocation_resources(
                ResourceProfile::from(options.profile),
                &state.per_invocation_resources,
                &ResourceEnvelope::default(),
            )
        } else {
            evaluate_resources(
                ResourceProfile::from(options.profile),
                &resource_evidence,
                &ResourceEnvelope::default(),
            )
        };
    let membership = if state.per_invocation_resources.is_empty() {
        resource_evidence
            .phases
            .cadence
            .as_ref()
            .map(|cadence| membership_report_samples(&cadence.membership_refreshes_by_phase))
            .unwrap_or_default()
    } else {
        state.per_invocation_membership.clone()
    };
    let summary_samples = if state.per_invocation_resources.is_empty() {
        &resource_evidence.series.samples
    } else {
        &state.samples
    };
    let workflow_turns = if state.per_invocation_resources.is_empty() {
        resource_evidence_turns(&resource_evidence)
    } else {
        state
            .per_invocation_resources
            .iter()
            .fold(0_u64, |total, observation| {
                total.saturating_add(u64::from(observation.completed_processes))
            })
    };
    let turn_wall_ns = if state.per_invocation_resources.is_empty() {
        resource_evidence.turn_wall_ns.as_slice()
    } else {
        state.per_invocation_turn_wall_ns.as_slice()
    };
    let idle_rss_mib = manifest.daemon.persistent.then(|| {
        resource_certification
            .metrics
            .get("idle_median_bytes")
            .copied()
            .unwrap_or(0.0)
            / (1024.0 * 1024.0)
    });
    let mut resource_summary = summarize_resources(
        summary_samples,
        &membership,
        workflow_turns,
        turn_wall_ns,
        idle_rss_mib,
        resource_certification
            .metrics
            .get("parallel_beta_mib_per_agent")
            .copied(),
        resource_certification
            .metrics
            .get("parallel_scaling_exponent")
            .copied(),
    );
    resource_summary.topology = manifest.concurrency.topology.clone();
    resource_summary.profile = format!("{:?}", options.profile).to_lowercase();
    resource_summary.comparison_scope = "within-topology-only".to_owned();
    enforce_sampler_overhead(
        &mut resource_certification.rows,
        resource_summary.sampler_overhead_pct,
    );
    let row42_records = request_records
        .iter()
        .filter(|record| record.request.actor.starts_with("r42"))
        .cloned()
        .collect::<Vec<_>>();
    let mut row42_completed_turns = BTreeMap::from([(1_u32, 0_u64), (2_u32, 0_u64)]);
    for event in state
        .events
        .get(&42)
        .into_iter()
        .flatten()
        .filter(|event| is_terminal(&event.event))
    {
        if let Some(repetition) = event
            .actor
            .strip_prefix("ahrb-row42-r")
            .and_then(|value| value.split(':').next())
            .and_then(|value| value.parse::<u32>().ok())
            && let Some(count) = row42_completed_turns.get_mut(&repetition)
        {
            *count = count.saturating_add(1);
        }
    }
    let row42_expected_turns_per_repetition = match options.profile {
        Profile::Quick => 20,
        Profile::Cert => 100,
    };
    let row42_evaluation = crate::fake_model::evaluate_model_request_efficiency_repetitions(
        &row42_records,
        &row42_completed_turns,
        row42_expected_turns_per_repetition,
    );
    let row43_turns_per_repetition = match options.profile {
        Profile::Quick => 100,
        Profile::Cert => 1_000,
    };
    let row43_expected_repetitions =
        ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions;
    let row43_observations = row43_trials
        .as_ref()
        .map_or(&[][..], |trials| trials.turns.as_slice());
    let row43_evaluation = evaluate_turn_latency_repetitions(
        row43_observations,
        row43_expected_repetitions,
        row43_turns_per_repetition,
        per_invocation_topology(&manifest),
        manifest.resources.turn_timeout_ms,
    );
    if selected_rows.contains(&43) && row43_evaluation.measurement_complete {
        apply_turn_latency_summary(&mut resource_summary, &row43_evaluation);
    }
    let row49_observations = if per_invocation_topology(&manifest) {
        state.long_horizon_turns.clone()
    } else {
        resource_evidence
            .long_horizon
            .as_ref()
            .into_iter()
            .flatten()
            .flat_map(|observation| observation.turns.iter().cloned())
            .collect::<Vec<_>>()
    };
    let row49_evaluation = evaluate_latency_vs_turn_index(
        &row49_observations,
        ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions,
        match options.profile {
            Profile::Quick => 100,
            Profile::Cert => 1_000,
        },
        per_invocation_topology(&manifest),
    );
    if selected_rows.contains(&49) && row49_evaluation.measurement_complete {
        apply_latency_vs_turn_index_summary(&mut resource_summary, &row49_evaluation);
    }
    let row45_expected_repetitions =
        ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions;
    let row45_observations = row45_trials
        .as_ref()
        .map_or(&[][..], |trials| trials.turns.as_slice());
    let row45_roles = row45_trials
        .as_ref()
        .map_or(&[][..], |trials| trials.first_request_roles.as_slice());
    let row45_evaluation = evaluate_time_to_first_model_request(
        row45_observations,
        row45_roles,
        row45_expected_repetitions,
        manifest.resources.turn_timeout_ms,
    );
    if selected_rows.contains(&45) && row45_evaluation.measurement_complete {
        apply_time_to_first_model_request_summary(&mut resource_summary, &row45_evaluation);
    }
    let row46_turns_per_repetition = match options.profile {
        Profile::Quick => 20_u32,
        Profile::Cert => 100_u32,
    };
    let row46_evidence = row46_trials
        .as_ref()
        .map_or_else(MemoryTimeIntegralEvidence::default, |trials| {
            trials.evidence.clone()
        });
    let row46_evaluation = evaluate_memory_time_integral(
        &row46_evidence,
        ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions,
        row46_turns_per_repetition,
        per_invocation_topology(&manifest),
    );
    if selected_rows.contains(&46) {
        if row46_evidence.sampler_observation_wall_ns > 0 {
            resource_summary.sampler_overhead_pct = resource_summary.sampler_overhead_pct.max(
                100.0 * row46_evidence.sampler_collection_cpu_ns as f64
                    / row46_evidence.sampler_observation_wall_ns as f64,
            );
        }
        if row46_evaluation.measurement_complete {
            apply_memory_time_integral_summary(&mut resource_summary, &row46_evaluation);
        }
    }
    let row47_expected_repetitions = match options.profile {
        Profile::Quick => 3_u32,
        Profile::Cert => 7_u32,
    };
    let row47_expected_turns = match options.profile {
        Profile::Quick => 20_u32,
        Profile::Cert => 100_u32,
    };
    let row47_log_evidence = row47_trials.as_ref().map_or(
        match manifest.resources.log_paths.as_ref() {
            None => LogEvidenceKind::Omitted,
            Some(paths) if paths.is_empty() => LogEvidenceKind::VerifiedNoLog,
            Some(_) => LogEvidenceKind::DeclaredPaths,
        },
        |trials| trials.log_evidence,
    );
    let row47_evaluation = evaluate_disk_io_per_turn(
        row47_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.turns.as_slice()),
        row47_expected_repetitions,
        row47_expected_turns,
        row47_log_evidence,
    );
    if selected_rows.contains(&47) {
        apply_disk_io_summary(&mut resource_summary, &row47_evaluation);
    }
    let streaming_repetitions =
        ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions;
    let streaming_count = match options.profile {
        Profile::Quick => 5_u32,
        Profile::Cert => 20_u32,
    };
    let row48_evaluation = evaluate_model_wait_cpu(
        row48_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        streaming_repetitions,
        streaming_count,
        manifest.resources.idle_timeout_ms,
    );
    if selected_rows.contains(&48) && row48_evaluation.measurement_complete {
        resource_summary.model_wait_cpu_p50_ms = Some(row48_evaluation.cpu_p50_ms);
        resource_summary.model_wait_wall_p50_ms = Some(row48_evaluation.wall_p50_ms);
        resource_summary.model_wait_cpu_one_core_max_ratio =
            Some(row48_evaluation.one_core_max_ratio);
    }
    let fanout_plan = FanoutPlan::for_profile(options.profile);
    let row54_evaluation = evaluate_fanout_cliff(
        row54_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.trials.as_slice()),
        &fanout_plan,
    );
    let row55_clock_resolution = selected_rows
        .contains(&55)
        .then(monotonic_clock_resolution_ns)
        .transpose()
        .unwrap_or(None);
    let row55_evaluation = evaluate_fairness_under_fanout(
        row54_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.actors.as_slice()),
        &fanout_plan,
        manifest.resources.turn_timeout_ms,
        row55_clock_resolution,
    );
    if selected_rows.contains(&54) && row54_evaluation.measurement_complete {
        apply_fanout_cliff_summary(&mut resource_summary, &row54_evaluation);
    }
    if selected_rows.contains(&54) || selected_rows.contains(&55) {
        if let Some(trials) = &row54_trials {
            let sampler_overhead_pct = trials
                .trials
                .iter()
                .filter(|trial| trial.sampler_observation_wall_ns > 0)
                .map(|trial| {
                    100.0 * trial.sampler_collection_cpu_ns as f64
                        / trial.sampler_observation_wall_ns as f64
                })
                .fold(0.0_f64, f64::max);
            resource_summary.sampler_overhead_pct = resource_summary
                .sampler_overhead_pct
                .max(sampler_overhead_pct);
        }
    }
    if selected_rows.contains(&55) && row55_evaluation.measurement_complete {
        apply_fairness_summary(&mut resource_summary, &row55_evaluation);
    }
    let (row50_repetitions, row50_sessions) = match options.profile {
        Profile::Quick => (3_u32, 20_u32),
        Profile::Cert => (7_u32, 200_u32),
    };
    let row50_evaluation = evaluate_session_residue_sweep(
        row50_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.sweeps.as_slice()),
        row50_repetitions,
        row50_sessions,
        per_invocation_topology(&manifest),
    );
    if selected_rows.contains(&50) && row50_evaluation.measurement_complete {
        apply_session_residue_summary(&mut resource_summary, &row50_evaluation);
    }
    let (row51_window, row51_pairs, row51_repetitions) = match options.profile {
        Profile::Quick => (4_096_u64, 4_u32, 3_u32),
        Profile::Cert => (16_384_u64, 32_u32, 7_u32),
    };
    let row51_evaluation = evaluate_context_limit_recovery(
        row51_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.trials.as_slice()),
        row51_repetitions,
        row51_window,
        row51_pairs,
        manifest.resources.turn_timeout_ms,
    );
    let (row52_lengths, row52_repetitions): (&[u32], u32) = match options.profile {
        Profile::Quick => (&[1, 10, 50], 3),
        Profile::Cert => (&[1, 50, 100, 250, 500], 7),
    };
    let row52_evaluation = evaluate_resume_latency_vs_length(
        row52_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.points.as_slice()),
        row52_lengths,
        row52_repetitions,
    );
    if selected_rows.contains(&52) && row52_evaluation.measurement_complete {
        resource_summary.resume_latency_p50_ms = row52_evaluation
            .resource_values
            .get("resume_latency_p50_ms")
            .copied();
        resource_summary.resume_latency_p95_ms = row52_evaluation
            .resource_values
            .get("resume_latency_p95_ms")
            .copied();
        resource_summary.resume_latency_slope_ms_per_turn = row52_evaluation
            .resource_values
            .get("resume_latency_slope_ms_per_turn")
            .copied();
    }
    let row53_expected_trials = match options.profile {
        Profile::Quick => 5_u32,
        Profile::Cert => 25_u32,
    };
    let row53_evaluation = evaluate_journal_torn_tail_sweep(
        row53_trials.as_deref().unwrap_or(&[]),
        row53_expected_trials,
    );
    let child_failure_repetitions = match options.profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let mut row56_evaluation = evaluate_child_failure_propagation(
        row56_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        child_failure_repetitions,
        manifest.resources.turn_timeout_ms,
    );
    if let Some(object) = row56_evaluation.details.as_object_mut() {
        object.insert(
            "status_observations".to_owned(),
            Value::Array(
                row56_trials
                    .as_ref()
                    .map_or_else(Vec::new, |trials| trials.status_observations.clone()),
            ),
        );
    }
    let signal_matrix_repetitions = match options.profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let row57_evaluation = evaluate_signal_matrix(
        row57_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        signal_matrix_repetitions,
        manifest.daemon.grace_ms,
        manifest.resources.turn_timeout_ms.saturating_add(3_000),
    );
    let row58_evaluation = evaluate_retry_budget(row58_trials.as_ref(), &manifest, options.profile);
    let row59_evaluation = evaluate_slow_stream_vs_stall(
        row59_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        streaming_repetitions,
        streaming_count,
        manifest.resources.idle_timeout_ms,
    );
    let fault_repetitions = match options.profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let row60_evaluation = evaluate_large_tool_output(
        row60_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        fault_repetitions,
    );
    if selected_rows.contains(&60) && row60_evaluation.measurement_complete {
        resource_summary.large_tool_output_peak_rss_delta_mib = row60_evaluation.peak_rss_delta_mib;
    }
    let row61_evaluation = evaluate_workspace_fault(
        row61_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        fault_repetitions,
        manifest.resources.turn_timeout_ms,
    );
    let row62_evaluation = evaluate_offline_mode(
        row62_trials
            .as_ref()
            .map_or(&[][..], |trials| trials.evidence.as_slice()),
        fault_repetitions,
    );
    let determinism_expected_runs = match options.profile {
        Profile::Quick => 2_u32,
        Profile::Cert => 7_u32,
    };
    let determinism_runs = determinism_trials
        .as_ref()
        .map_or(&[][..], |trials| trials.runs.as_slice());
    let row63_evaluation =
        evaluate_nondeterministic_fields(determinism_runs, determinism_expected_runs);
    let row64_evaluation =
        evaluate_cross_run_reproducibility(determinism_runs, determinism_expected_runs);
    let row65_evaluation = row65_trials
        .as_ref()
        .map(|trials| trials.evaluation.clone())
        .unwrap_or_default();
    let row67_evaluation = crate::wave4::evaluate_usage_reporting(
        state.events.get(&67).map_or(&[][..], Vec::as_slice),
        manifest.events.metadata.as_ref(),
        match options.profile {
            Profile::Quick => 3,
            Profile::Cert => 7,
        },
        match options.profile {
            Profile::Quick => 3,
            Profile::Cert => 20,
        },
    );
    let row69_evaluation = crate::wave4::evaluate_event_stream_completeness(
        state.events.get(&69).map_or(&[][..], Vec::as_slice),
        manifest.events.metadata.as_ref(),
        match options.profile {
            Profile::Quick => 3,
            Profile::Cert => 7,
        },
    );
    let row71_evaluation = if selected_rows.contains(&71)
        && matches!(
            crate::matrix_evidence::capability_for_row(&manifest, 71),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
        let render_paths = |templates: &[String], extra: Option<(&str, &str)>| {
            let mut rendered_variables = variables.clone();
            if let Some((name, value)) = extra {
                rendered_variables.insert(name.to_owned(), value.to_owned());
            }
            templates
                .iter()
                .map(|template| {
                    crate::manifest::render_template(template, &rendered_variables)
                        .map(PathBuf::from)
                })
                .collect::<Result<Vec<_>>>()
        };
        let mut carrier_paths = render_paths(
            manifest
                .capture
                .credential_carrier_paths
                .as_deref()
                .unwrap_or(&[]),
            None,
        )?;
        if resource_selected {
            let timing = ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile));
            let prefix = if per_invocation_topology(&manifest) {
                "pr"
            } else {
                "rr"
            };
            for repetition in 0..timing.repetitions {
                let resource_profile = profile_root.join(format!("{prefix}{repetition}"));
                let rendered_profile = resource_profile.to_string_lossy();
                carrier_paths.extend(
                    render_paths(
                        manifest
                            .capture
                            .credential_carrier_paths
                            .as_deref()
                            .unwrap_or(&[]),
                        Some(("profile", rendered_profile.as_ref())),
                    )?
                    .into_iter()
                    .filter(|path| path.is_file()),
                );
            }
        }
        carrier_paths.sort();
        carrier_paths.dedup();
        let session_roots = render_paths(&manifest.sessions.store_paths, None)?;
        let mut journal_roots = render_paths(
            manifest.resources.journal_paths.as_deref().unwrap_or(&[]),
            None,
        )?;
        if !manifest.events.path.trim().is_empty() {
            for session in state.sessions.get(&71).into_iter().flatten() {
                let rendered = render_paths(
                    std::slice::from_ref(&manifest.events.path),
                    Some(("session_id", &session.0)),
                )?;
                journal_roots.extend(rendered);
            }
        }
        let (expected_successes, expected_failures) = match options.profile {
            Profile::Quick => (1, 1),
            Profile::Cert => (3, 3),
        };
        evaluate_secrets_hygiene(SecretsHygieneInputs {
            profile_root: &profile_root,
            credential: &credential,
            carrier_paths: &carrier_paths,
            session_roots: &session_roots,
            journal_roots: &journal_roots,
            expected_successes,
            expected_failures,
            events: state.events.get(&71).map_or(&[][..], Vec::as_slice),
            credential_in_argv: manifest.capture.allow_credential_argv,
        })
    } else {
        SecretsHygieneEvaluation::default()
    };
    let row72_evaluation = evaluate_tool_result_role_fidelity(
        &request_records,
        match options.profile {
            Profile::Quick => 1,
            Profile::Cert => 5,
        },
    );
    let row44_evidence = row42_trials
        .as_ref()
        .and_then(|trials| trials.process_hygiene.as_ref())
        .cloned()
        .unwrap_or_default();
    let row44_turns = match options.profile {
        Profile::Quick => 20,
        Profile::Cert => 100,
    };
    let row44_evaluation = evaluate_process_hygiene(
        &row44_evidence,
        2,
        row44_turns,
        per_invocation_topology(&manifest),
    );
    let mut results = evaluate_rows(
        &selected,
        &state,
        &request_records,
        &manifest,
        &resource_certification,
        &profile_root,
        &DerivedRowEvaluations {
            injection_surface: &row65_evaluation,
            budget_enforcement: &row66_evaluation,
            usage_reporting: &row67_evaluation,
            session_ops_cli: &row68_evaluation,
            event_stream_completeness: &row69_evaluation,
            headless_permissions: &row70_evaluation,
            secrets_hygiene: &row71_evaluation,
            model_request_efficiency: &row42_evaluation,
            turn_latency: &row43_evaluation,
            latency_vs_turn_index: &row49_evaluation,
            session_residue: &row50_evaluation,
            context_limit_recovery: &row51_evaluation,
            resume_latency: &row52_evaluation,
            journal_torn_tail: &row53_evaluation,
            fanout_cliff: &row54_evaluation,
            fairness: &row55_evaluation,
            process_hygiene: &row44_evaluation,
            time_to_first_model_request: &row45_evaluation,
            memory_time_integral: &row46_evaluation,
            disk_io_per_turn: &row47_evaluation,
            model_wait_cpu: &row48_evaluation,
            child_failure_propagation: &row56_evaluation,
            signal_matrix: &row57_evaluation,
            retry_budget: &row58_evaluation,
            slow_stream_vs_stall: &row59_evaluation,
            large_tool_output: &row60_evaluation,
            workspace_fault: &row61_evaluation,
            offline_mode: &row62_evaluation,
            nondeterministic_fields: &row63_evaluation,
            cross_run_reproducibility: &row64_evaluation,
        },
        options.profile,
    );
    crate::report::record_capability_declarations(&mut results, &manifest);
    if !state.lifecycle_notes.is_empty() {
        let note = format!("daemon lifecycle: {}", state.lifecycle_notes.join("; "));
        for result in results
            .iter_mut()
            .filter(|result| matches!(result.row, 28 | 36))
        {
            result.evidence.push(note.clone());
        }
    }
    results.sort_by_key(|result| result.row);
    progress.update(|state| {
        for result in &results {
            state.completed.insert(result.row);
            state.results.insert(result.row, result.clone());
        }
    })?;
    let mut resource_metric_values = resource_certification.metrics.clone();
    if let Some(beta) = resource_metric_values
        .get("parallel_beta_bytes_per_agent")
        .copied()
    {
        resource_metric_values.insert(
            "parallel_beta_mib_per_agent".to_owned(),
            beta / (1024.0 * 1024.0),
        );
    }
    resource_metric_values.insert(
        "resource_completed_repetitions".to_owned(),
        if state.per_invocation_resources.is_empty() {
            resource_evidence.completed_repetitions as f64
        } else {
            ResourceTimingPlan::for_profile(ResourceProfile::from(options.profile)).repetitions
                as f64
        },
    );
    if selected_rows.contains(&43) && row43_evaluation.measurement_complete {
        resource_metric_values.extend(turn_latency_resource_metrics(&row43_evaluation));
    }
    if selected_rows.contains(&49) && row49_evaluation.measurement_complete {
        resource_metric_values.extend(latency_vs_turn_index_resource_metrics(&row49_evaluation));
    }
    if selected_rows.contains(&50) && row50_evaluation.measurement_complete {
        resource_metric_values.extend(row50_evaluation.resource_values.clone());
    }
    if selected_rows.contains(&52) && row52_evaluation.measurement_complete {
        resource_metric_values.extend(row52_evaluation.resource_values.clone());
    }
    if selected_rows.contains(&54) && row54_evaluation.measurement_complete {
        resource_metric_values.extend(row54_evaluation.resource_values.clone());
    }
    if selected_rows.contains(&55) && row55_evaluation.measurement_complete {
        resource_metric_values.extend(row55_evaluation.resource_values.clone());
    }
    if selected_rows.contains(&45) && row45_evaluation.measurement_complete {
        resource_metric_values.extend(time_to_first_model_request_resource_metrics(
            &row45_evaluation,
        ));
    }
    if selected_rows.contains(&46) && row46_evaluation.measurement_complete {
        resource_metric_values.extend(memory_time_integral_resource_metrics(&row46_evaluation));
    }
    if selected_rows.contains(&48) && row48_evaluation.measurement_complete {
        resource_metric_values.extend(BTreeMap::from([
            (
                "model_wait_cpu_p50_ms".to_owned(),
                row48_evaluation.cpu_p50_ms,
            ),
            (
                "model_wait_wall_p50_ms".to_owned(),
                row48_evaluation.wall_p50_ms,
            ),
            (
                "model_wait_cpu_one_core_max_ratio".to_owned(),
                row48_evaluation.one_core_max_ratio,
            ),
        ]));
    }
    if selected_rows.contains(&47) && row47_evaluation.measurement_complete {
        resource_metric_values.extend(disk_io_resource_metrics(&row47_evaluation));
    }
    if selected_rows.contains(&60)
        && row60_evaluation.measurement_complete
        && let Some(value) = row60_evaluation.peak_rss_delta_mib
    {
        resource_metric_values.insert("large_tool_output_peak_rss_delta_mib".to_owned(), value);
    }
    let marginal_bytes = resource_metric_values
        .get("parallel_beta_bytes_per_agent")
        .copied()
        .unwrap_or(f64::INFINITY);
    let resource_metrics = resource_metric_values
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                TopologyMetric {
                    value: *value,
                    profile: format!("{:?}", options.profile).to_lowercase(),
                    topology: manifest.concurrency.topology.clone(),
                    comparison_scope: "within-topology-only".to_owned(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut metrics = BTreeMap::new();
    if let Some(value) = state.crash_recovery_ms {
        metrics.insert("crash_recovery_ms".to_owned(), value);
    }
    if let Some(value) = state.journal_recovered_events {
        metrics.insert("journal_recovered_events".to_owned(), value as f64);
    }
    if let Some(value) = state.crash_recovery_tree_cleared {
        metrics.insert(
            "crash_recovery_tree_cleared".to_owned(),
            if value { 1.0 } else { 0.0 },
        );
    }
    if let Some(value) = state.crash_recovery_valid {
        metrics.insert(
            "crash_recovery_valid".to_owned(),
            if value { 1.0 } else { 0.0 },
        );
    }
    if let Some(value) = state.journal_recovery_valid {
        metrics.insert(
            "journal_recovery_valid".to_owned(),
            if value { 1.0 } else { 0.0 },
        );
    }
    if let Some(value) = state.journal_torn_tail_injected {
        metrics.insert(
            "journal_torn_tail_injected".to_owned(),
            if value { 1.0 } else { 0.0 },
        );
    }
    if selected_rows.contains(&42) && row42_evaluation.measurement_complete {
        metrics.extend(row42_evaluation.metrics.clone());
    }
    if selected_rows.contains(&44) && row44_evaluation.measurement_complete {
        metrics.extend(row44_evaluation.metrics.clone());
    }
    if selected_rows.contains(&48) && row48_evaluation.measurement_complete {
        metrics.extend(row48_evaluation.metrics.clone());
    }
    if selected_rows.contains(&49) && row49_evaluation.measurement_complete {
        metrics.extend(row49_evaluation.metrics.clone());
    }
    if selected_rows.contains(&50) && row50_evaluation.measurement_complete {
        metrics.extend(row50_evaluation.metrics.clone());
    }
    if selected_rows.contains(&51) && row51_evaluation.measurement_complete {
        metrics.extend(row51_evaluation.metrics.clone());
    }
    if selected_rows.contains(&52) && row52_evaluation.measurement_complete {
        metrics.extend(row52_evaluation.metrics.clone());
    }
    if selected_rows.contains(&53) && row53_evaluation.measurement_complete {
        metrics.extend(row53_evaluation.metrics.clone());
    }
    if selected_rows.contains(&56) && row56_evaluation.measurement_complete {
        metrics.extend(row56_evaluation.metrics.clone());
    }
    if selected_rows.contains(&57) && row57_evaluation.measurement_complete {
        metrics.extend(row57_evaluation.metrics.clone());
    }
    if selected_rows.contains(&58) && row58_evaluation.measurement_complete {
        metrics.extend(row58_evaluation.metrics.clone());
    }
    if selected_rows.contains(&59) && row59_evaluation.measurement_complete {
        metrics.extend(row59_evaluation.metrics.clone());
    }
    if selected_rows.contains(&60) && row60_evaluation.measurement_complete {
        metrics.extend(row60_evaluation.metrics.clone());
    }
    if selected_rows.contains(&61) && row61_evaluation.measurement_complete {
        metrics.extend(row61_evaluation.metrics.clone());
    }
    if selected_rows.contains(&62) && row62_evaluation.measurement_complete {
        metrics.extend(row62_evaluation.metrics.clone());
    }
    if selected_rows.contains(&63) && row63_evaluation.measurement_complete {
        metrics.extend(BTreeMap::from([
            (
                "nondeterministic_field_report.score".to_owned(),
                row63_evaluation.score,
            ),
            (
                "nondeterministic_field_report.comparable_leaf_occurrences".to_owned(),
                row63_evaluation.comparable_leaf_occurrences as f64,
            ),
            (
                "nondeterministic_field_report.varying_leaf_occurrences".to_owned(),
                row63_evaluation.varying_leaf_occurrences as f64,
            ),
            (
                "nondeterministic_field_report.varying_pointer_count".to_owned(),
                row63_evaluation.varying_pointer_count as f64,
            ),
            (
                "nondeterministic_field_report.varying_critical_field_count".to_owned(),
                row63_evaluation.varying_critical_field_count as f64,
            ),
        ]));
    }
    if selected_rows.contains(&64) && row64_evaluation.measurement_complete {
        metrics.extend(BTreeMap::from([
            (
                "cross_run_reproducibility.identical".to_owned(),
                if row64_evaluation.identical { 1.0 } else { 0.0 },
            ),
            (
                "cross_run_reproducibility.request_stream_count".to_owned(),
                row64_evaluation.request_stream_count as f64,
            ),
            (
                "cross_run_reproducibility.attempt_count".to_owned(),
                row64_evaluation.attempt_count as f64,
            ),
        ]));
    }
    if selected_rows.contains(&65) && row65_evaluation.measurement_complete {
        metrics.extend(row65_evaluation.metrics.clone());
    }
    if selected_rows.contains(&66) && row66_evaluation.measurement_complete {
        metrics.extend(row66_evaluation.metrics.clone());
    }
    if selected_rows.contains(&67) && row67_evaluation.measurement_complete {
        metrics.extend(row67_evaluation.metrics.clone());
    }
    if selected_rows.contains(&68) && row68_evaluation.measurement_complete {
        metrics.extend(row68_evaluation.metrics.clone());
    }
    if selected_rows.contains(&69) && row69_evaluation.measurement_complete {
        metrics.extend(row69_evaluation.metrics.clone());
    }
    if selected_rows.contains(&70) && row70_evaluation.measurement_complete {
        metrics.extend(row70_evaluation.metrics.clone());
    }
    if selected_rows.contains(&71) && row71_evaluation.measurement_complete {
        metrics.extend(row71_evaluation.metrics.clone());
    }
    if selected_rows.contains(&72) && row72_evaluation.measurement_complete {
        metrics.extend(row72_evaluation.metrics.clone());
    }
    let mut details = ReportDetails::default();
    if selected_rows.contains(&47) {
        let mut disk_details = row47_evaluation.details.clone();
        if let Some(object) = disk_details.as_object_mut() {
            object.insert(
                "turns".to_owned(),
                serde_json::to_value(
                    row47_trials
                        .as_ref()
                        .map_or(&[][..], |trials| trials.turns.as_slice()),
                )?,
            );
            object.insert(
                "counter_snapshots".to_owned(),
                Value::Array(
                    row47_trials
                        .as_ref()
                        .map_or_else(Vec::new, |trials| trials.counter_snapshots.clone()),
                ),
            );
        }
        details.insert("disk-io-per-turn".to_owned(), disk_details);
    }
    if selected_rows.contains(&42) {
        details.insert(
            "model-request-efficiency".to_owned(),
            row42_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&44) {
        details.insert(
            "process-hygiene".to_owned(),
            row44_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&45) {
        details.insert(
            "time-to-first-model-request".to_owned(),
            row45_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&46) {
        details.insert(
            "memory-time-integral".to_owned(),
            row46_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&48) {
        details.insert(
            "model-wait-cpu".to_owned(),
            row48_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&49) {
        details.insert(
            "latency-vs-turn-index".to_owned(),
            row49_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&50) {
        details.insert(
            "session-residue-sweep".to_owned(),
            row50_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&51) {
        details.insert(
            "context-limit-recovery".to_owned(),
            row51_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&52) {
        details.insert(
            "resume-latency-vs-length".to_owned(),
            row52_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&53) {
        details.insert(
            "journal-torn-tail-sweep".to_owned(),
            row53_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&54) {
        details.insert("fanout-cliff".to_owned(), row54_evaluation.details.clone());
    }
    if selected_rows.contains(&55) {
        details.insert(
            "fairness-under-fanout".to_owned(),
            row55_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&56) {
        details.insert(
            "child-failure-propagation".to_owned(),
            row56_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&57) {
        details.insert("signal-matrix".to_owned(), row57_evaluation.details.clone());
    }
    if selected_rows.contains(&58) {
        details.insert("retry-budget".to_owned(), row58_evaluation.details.clone());
    }
    if selected_rows.contains(&59) {
        details.insert(
            "slow-stream-vs-stall".to_owned(),
            row59_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&60) {
        details.insert(
            "large-tool-output".to_owned(),
            row60_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&61) {
        details.insert(
            "workspace-fault".to_owned(),
            row61_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&62) {
        let detail = state.row_errors.get(&62).map_or_else(
            || row62_evaluation.details.clone(),
            |error| json!({"measurement_complete":false,"measurement_error":error}),
        );
        details.insert("offline-mode".to_owned(), detail);
    }
    if selected_rows.contains(&63) {
        details.insert(
            "nondeterministic-field-report".to_owned(),
            row63_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&64) {
        details.insert(
            "cross-run-reproducibility".to_owned(),
            row64_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&65) {
        details.insert(
            "injection-surface".to_owned(),
            row65_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&66) {
        details.insert(
            "budget-enforcement".to_owned(),
            row66_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&67) {
        details.insert(
            "usage-reporting".to_owned(),
            row67_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&68) {
        details.insert(
            "session-ops-cli".to_owned(),
            row68_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&69) {
        details.insert(
            "event-stream-completeness".to_owned(),
            row69_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&70) {
        details.insert(
            "headless-permission-model".to_owned(),
            row70_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&71) {
        details.insert(
            "secrets-hygiene-on-disk".to_owned(),
            row71_evaluation.details.clone(),
        );
    }
    if selected_rows.contains(&72) {
        details.insert(
            "tool-result-role-fidelity".to_owned(),
            row72_evaluation.details.clone(),
        );
    }
    let automation = automation_score(&results);
    let automation_components_missing = automation.provisional;
    details.insert(
        "resource-summary".to_owned(),
        json!({
            "measurement_complete": (!selected_rows.contains(&43) || row43_evaluation.measurement_complete)
                && (!selected_rows.contains(&45) || row45_evaluation.measurement_complete)
                && (!selected_rows.contains(&46) || row46_evaluation.measurement_complete)
                && (!selected_rows.contains(&47) || row47_evaluation.measurement_complete)
                && (!selected_rows.contains(&48) || row48_evaluation.measurement_complete)
                && (!selected_rows.contains(&49) || row49_evaluation.measurement_complete)
                && (!selected_rows.contains(&50) || row50_evaluation.measurement_complete)
                && (!selected_rows.contains(&60) || row60_evaluation.measurement_complete),
            "latency_class": if selected_rows.contains(&43) && row43_evaluation.measurement_complete {
                resource_summary.latency_class.clone()
            } else {
                None
            },
            "cpu_class": if selected_rows.contains(&46) {
                resource_summary.cpu_class.clone()
            } else {
                None
            },
        }),
    );
    details.insert(
        "automation-score".to_owned(),
        json!({
            "profile": format!("{:?}", options.profile).to_lowercase(),
            "topology": manifest.concurrency.topology,
            "comparison_scope": "within-topology-only",
            "score": automation.score,
        }),
    );
    if let Some(value) = state.journal_native_replay_valid {
        metrics.insert(
            "journal_native_replay_valid".to_owned(),
            if value { 1.0 } else { 0.0 },
        );
    }
    let certified_parallel_width = row54_evaluation
        .fanout_max_measured_n
        .and_then(|width| usize::try_from(width).ok())
        .unwrap_or(state.parallel_agents);
    let badge = certify(
        &results,
        &manifest,
        std::env::consts::OS,
        &format!("{:?}", options.profile).to_lowercase(),
        certified_parallel_width,
        marginal_bytes,
        resource_summary
            .latency_class
            .as_deref()
            .unwrap_or("unavailable"),
        resource_summary
            .cpu_class
            .as_deref()
            .unwrap_or("unavailable"),
    );
    let mut raw_events = Vec::new();
    for row_events in state.events.values() {
        for event in row_events {
            raw_events.push(serde_json::to_value(event)?);
        }
    }
    let processes = process_observations(&state.samples);
    let model_requests = request_records
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let report = Report {
        schema: 3,
        spec_version: 2,
        run_id,
        profile_path: profile_root.to_string_lossy().into_owned(),
        fingerprint: Fingerprint {
            harness: manifest.identity.id.clone(),
            harness_version: persistence.harness_version.clone(),
            manifest: manifest_hash,
            workflows: workflow_hash(),
            fake_model: env!("CARGO_PKG_VERSION").to_owned(),
            normalizer: env!("CARGO_PKG_VERSION").to_owned(),
            ahrb_revision: crate::results::ahrb_revision(),
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            host_memory_bytes: host_memory_bytes(),
            profile: format!("{:?}", options.profile).to_lowercase(),
        },
        results,
        badge,
        metrics,
        details,
        lifecycle_notes: state.lifecycle_notes,
        control_evidence: state.control_evidence,
        resource_metrics,
        resource_summary,
        samples: state.samples,
        memory_time_samples: row46_trials
            .as_ref()
            .map_or_else(Vec::new, |trials| trials.evidence.samples.clone()),
        processes,
        membership,
        events: raw_events,
        model_requests,
        turns: {
            let mut turns = row43_trials
                .as_ref()
                .map_or_else(Vec::new, |trials| trials.turns.clone());
            if selected_rows.contains(&49) {
                turns.extend(row49_observations.clone());
            }
            if let Some(trials) = &row45_trials {
                turns.extend(trials.turns.clone());
            }
            if let Some(trials) = &row46_trials {
                turns.extend(trials.evidence.turns.clone());
            }
            turns.sort_by(|left, right| {
                (&left.phase, left.repetition, left.turn_index, &left.actor).cmp(&(
                    &right.phase,
                    right.repetition,
                    right.turn_index,
                    &right.actor,
                ))
            });
            turns
        },
        stream_chunks: {
            let mut chunks = row48_trials
                .as_ref()
                .map_or_else(Vec::new, |trials| trials.stream_chunks.clone());
            if let Some(trials) = &row59_trials {
                chunks.extend(trials.stream_chunks.clone());
            }
            chunks.sort_by(|left, right| {
                (left.repetition, &left.case, &left.actor, left.ordinal).cmp(&(
                    right.repetition,
                    &right.case,
                    &right.actor,
                    right.ordinal,
                ))
            });
            chunks
        },
        filesystem_snapshots: {
            let mut snapshots = row47_trials
                .as_ref()
                .map_or_else(Vec::new, |trials| trials.filesystem_snapshots.clone());
            if let Some(trials) = &row61_trials {
                snapshots.extend(trials.filesystem_snapshots.clone());
            }
            snapshots.sort_by(|left, right| {
                (left.repetition, &left.boundary, &left.path_under_profile).cmp(&(
                    right.repetition,
                    &right.boundary,
                    &right.path_under_profile,
                ))
            });
            snapshots
        },
        egress_attempts: row62_trials
            .as_ref()
            .map_or_else(Vec::new, |trials| trials.egress_attempts.clone()),
    };
    crate::results::persist_report(&persistence, &report, options.junit, false)?;
    println!("{}", render_resource_summary(&report.resource_summary));
    match &report.badge {
        Some(badge) => println!("badge {}", badge_label(badge)),
        None => println!("badge none"),
    }
    if automation_components_missing {
        println!("badge_note A unavailable until rows 65-72 are measured");
    }
    Ok(suite_exit_code(
        &report.results,
        report.badge.as_ref(),
        &manifest,
    ))
}

fn prepare_profile(manifest: &Manifest, profile_root: &Path) -> Result<()> {
    let parent = profile_root.parent().ok_or_else(|| {
        AhrbError::Validation(format!(
            "fresh profile {} has no parent directory",
            profile_root.display()
        ))
    })?;
    std::fs::create_dir_all(parent)?;
    std::fs::create_dir(profile_root).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            AhrbError::Validation(format!(
                "refusing to reuse non-cold profile {}; choose a fresh output directory",
                profile_root.display()
            ))
        } else {
            error.into()
        }
    })?;
    set_owner_private(profile_root)?;
    let variables = BTreeMap::from([(
        "profile".to_owned(),
        profile_root.to_string_lossy().into_owned(),
    )]);
    let mut roots = Vec::new();
    for value in manifest.isolation.roots.values() {
        let rendered = crate::manifest::render_template(value, &variables)?;
        std::fs::create_dir_all(&rendered).map_err(|error| {
            AhrbError::Protocol(format!(
                "create isolation root {}: {error}",
                Path::new(&rendered).display()
            ))
        })?;
        set_owner_private(Path::new(&rendered))?;
        roots.push(PathBuf::from(rendered));
    }
    for root in &roots {
        for suffix in &manifest.isolation.socket_path_suffixes {
            let candidate = root.join(suffix);
            let length = unix_path_bytes(&candidate);
            if length >= 100 {
                return Err(AhrbError::Validation(format!(
                    "Unix socket path {} is {length} bytes; isolation paths must stay under 100 bytes",
                    candidate.display()
                )));
            }
        }
    }
    Ok(())
}

fn set_owner_private(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn unix_path_bytes(path: &Path) -> usize {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        path.as_os_str().as_bytes().len()
    }
    #[cfg(not(unix))]
    {
        path.to_string_lossy().len()
    }
}

fn write_generated_files(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<()> {
    for specification in manifest
        .isolation
        .generated_files
        .iter()
        .chain(manifest.fake_model.provider_templates.iter())
    {
        let path = PathBuf::from(crate::manifest::render_template(
            &specification.path,
            variables,
        )?);
        if !path.starts_with(profile_root) {
            return Err(AhrbError::Validation(format!(
                "generated file {} escapes fresh profile {}",
                path.display(),
                profile_root.display()
            )));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                AhrbError::Protocol(format!(
                    "create generated-file parent {} for {}: {error}",
                    parent.display(),
                    path.display()
                ))
            })?;
        }
        let content = crate::manifest::render_template(&specification.content, variables)?;
        std::fs::write(&path, content.as_bytes()).map_err(|error| {
            AhrbError::Protocol(format!("write generated file {}: {error}", path.display()))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = u32::from_str_radix(specification.mode.trim_start_matches('0'), 8).map_err(
                |_| {
                    AhrbError::Validation(format!(
                        "invalid generated-file mode {:?}",
                        specification.mode
                    ))
                },
            )?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).map_err(
                |error| {
                    AhrbError::Protocol(format!(
                        "set permissions on generated file {}: {error}",
                        path.display()
                    ))
                },
            )?;
        }
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|error| {
                AhrbError::Protocol(format!(
                    "open generated file {} for sync: {error}",
                    path.display()
                ))
            })?;
        file.sync_all().map_err(|error| {
            AhrbError::Protocol(format!("sync generated file {}: {error}", path.display()))
        })?;
    }
    Ok(())
}

fn isolated_environment(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let mut environment = BTreeMap::new();
    for (name, template) in &manifest.isolation.roots {
        environment.insert(
            name.clone(),
            crate::manifest::render_template(template, variables)?,
        );
    }
    for (name, template) in &manifest.isolation.environment {
        environment.insert(
            name.clone(),
            crate::manifest::render_template(template, variables)?,
        );
    }
    Ok(environment)
}

async fn start_model(
    engine: Arc<FakeModelEngine>,
    workflow: &Workflow,
    profile_root: &Path,
    embedded: bool,
    base_url_env: &str,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    if embedded {
        let path = profile_root.join("embedded-workflow.json");
        std::fs::write(&path, serde_json::to_vec(workflow)?)?;
        let file = std::fs::OpenOptions::new().write(true).open(&path)?;
        file.sync_all()?;
        return Ok((
            ModelServer::Embedded,
            BTreeMap::from([(
                "AHRB_MOCK_EMBEDDED_WORKFLOW".to_owned(),
                path.to_string_lossy().into_owned(),
            )]),
        ));
    }
    let address = SocketAddr::from(([127, 0, 0, 1], 0));
    match FakeModelServer::bind(address, Arc::clone(&engine)).await {
        Ok(server) => {
            let environment = BTreeMap::from([(base_url_env.to_owned(), server.base_url())]);
            Ok((ModelServer::Tcp(server), environment))
        }
        Err(error) => {
            let tcp_error = error.to_string();
            start_mailbox_model(engine, base_url_env)
                .await
                .map_err(|mailbox_error| {
                AhrbError::Protocol(format!(
                    "TCP fake-model bind failed after bounded retries ({tcp_error}); provider mailbox fallback failed: {mailbox_error}"
                ))
            })
        }
    }
}

async fn start_streaming_model(
    engine: Arc<FakeModelEngine>,
    base_url_env: &str,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    let address = SocketAddr::from(([127, 0, 0, 1], 0));
    match FakeModelServer::bind(address, Arc::clone(&engine)).await {
        Ok(server) => {
            let base_url = server.base_url();
            Ok((
                ModelServer::Tcp(server),
                BTreeMap::from([(base_url_env.to_owned(), base_url)]),
            ))
        }
        Err(AhrbError::Io(error))
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || is_transient_bind_error(&error) =>
        {
            match start_unix_model(Arc::clone(&engine)).await {
                Ok(started) => Ok(started),
                Err(unix_error) => {
                    let server = FakeModelPreconnectedServer::pair(engine).map_err(|pair_error| {
                        AhrbError::Protocol(format!(
                            "streaming fake-model HTTP is unavailable: TCP={error}; Unix={unix_error}; preconnected={pair_error}"
                        ))
                    })?;
                    let peer_fd = server.peer_fd();
                    Ok((
                        ModelServer::Preconnected(server),
                        BTreeMap::from([
                            (
                                base_url_env.to_owned(),
                                "http://ahrb-preconnected.invalid".to_owned(),
                            ),
                            ("AHRB_MOCK_PROVIDER_FD".to_owned(), peer_fd.to_string()),
                        ]),
                    ))
                }
            }
        }
        Err(error) => Err(error),
    }
}

async fn start_mailbox_model(
    engine: Arc<FakeModelEngine>,
    base_url_env: &str,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    #[cfg(target_os = "macos")]
    let root = PathBuf::from("/private/tmp");
    #[cfg(not(target_os = "macos"))]
    let root = PathBuf::from("/tmp");
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = root.join(format!("ahrb-fmb-{}-{sequence}", std::process::id()));
    let server = FakeModelMailboxServer::bind(directory.clone(), engine).await?;
    let environment = BTreeMap::from([
        (
            "AHRB_MOCK_PROVIDER_MAILBOX".to_owned(),
            directory.to_string_lossy().into_owned(),
        ),
        (
            base_url_env.to_owned(),
            format!("ahrb+mailbox://{}", directory.display()),
        ),
    ]);
    Ok((ModelServer::Mailbox(server), environment))
}

async fn start_unix_model(
    engine: Arc<FakeModelEngine>,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    #[cfg(target_os = "macos")]
    let root = PathBuf::from("/private/tmp");
    #[cfg(not(target_os = "macos"))]
    let root = PathBuf::from("/tmp");
    let mut directory = None;
    for _ in 0..8 {
        let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = root.join(format!("ahrb-fm-{}-{sequence}", std::process::id()));
        match std::fs::create_dir(&candidate) {
            Ok(()) => {
                directory = Some(candidate);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(AhrbError::Protocol(format!(
                    "create Unix fake-model directory {}: {error}",
                    candidate.display()
                )));
            }
        }
    }
    let directory = directory.ok_or_else(|| {
        AhrbError::Protocol(
            "allocate a fresh Unix fake-model directory after 8 attempts".to_owned(),
        )
    })?;
    let path = directory.join("model.sock");
    let server = FakeModelUnixServer::bind(&path, engine)
        .await
        .map_err(|error| {
            AhrbError::Protocol(format!(
                "bind Unix fake-model socket {}: {error}",
                path.display()
            ))
        })?;
    let environment = BTreeMap::from([(
        "AHRB_MOCK_UNIX_SOCKET".to_owned(),
        path.to_string_lossy().into_owned(),
    )]);
    Ok((ModelServer::Unix { server, directory }, environment))
}

fn make_driver(
    manifest: &Manifest,
    command: &[String],
    environment: &BTreeMap<String, String>,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
    gate_exec_launch: bool,
) -> Result<HarnessDriver> {
    make_driver_with_timeout(
        manifest,
        command,
        environment,
        variables,
        profile_root,
        gate_exec_launch,
        Duration::from_millis(manifest.transport.timeout_ms),
    )
}

#[allow(clippy::too_many_arguments)]
fn make_driver_with_timeout(
    manifest: &Manifest,
    command: &[String],
    environment: &BTreeMap<String, String>,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
    gate_exec_launch: bool,
    timeout: Duration,
) -> Result<HarnessDriver> {
    if manifest.transport.kind == TransportKind::Exec {
        let first_command = resolve_local_program(&manifest.transport.command)?;
        let continuation = if manifest.sessions.continue_turn.is_empty() {
            &manifest.sessions.resume
        } else {
            &manifest.sessions.continue_turn
        };
        let resume_command = resolve_local_program(continuation)?;
        let daemon = manifest
            .daemon
            .persistent
            .then(|| rendered_managed_daemon_config(manifest, environment, variables, profile_root))
            .transpose()?;
        return Ok(Box::new(PerInvocationDriver::new(PerInvocationConfig {
            daemon,
            command: first_command,
            resume_command,
            resume_control_command: resolve_local_program(&manifest.sessions.resume_control)?,
            recover_probe_command: resolve_local_program(&manifest.sessions.recover_probe)?,
            close_delete_command: resolve_local_program(&manifest.sessions.close_delete)?,
            release_command: resolve_local_program(&manifest.concurrency.release)?,
            cancel_command: resolve_local_program(&manifest.agents.cancel)?,
            replay_command: resolve_local_program(&manifest.events.replay_command)?,
            wait_ready_command: resolve_local_program(&manifest.sessions.wait_ready)?,
            environment: environment.clone(),
            base_variables: variables.clone(),
            profile_root: profile_root.to_path_buf(),
            events: manifest.events.clone(),
            exit: manifest.exit.clone(),
            session_id_pointer: manifest.sessions.id_pointer.clone(),
            run_id_pointer: manifest.sessions.run_id_pointer.clone(),
            timeout,
            max_output_bytes: manifest
                .capture
                .max_bytes
                .max(manifest.resources.max_output_bytes),
            gate_launch: gate_exec_launch,
        })));
    }
    let endpoint = crate::manifest::render_template(&manifest.transport.endpoint, variables)?;
    let mut transport: Box<dyn Transport> = match manifest.transport.kind {
        TransportKind::Exec => {
            return Err(AhrbError::Protocol(
                "exec transport did not select the per-invocation driver".to_owned(),
            ));
        }
        TransportKind::StdinRpc => {
            let mut transport = StdinRpcTransport::new(command.to_vec(), timeout)
                .with_environment(environment.clone());
            if manifest.daemon.persistent && !manifest.daemon.readiness.kind.is_empty() {
                let mut readiness = manifest.daemon.readiness.clone();
                readiness.target = crate::manifest::render_template(&readiness.target, variables)?;
                readiness.command =
                    resolve_local_program(&render_argv(&readiness.command, variables)?)?;
                transport = transport.with_readiness(readiness);
            }
            Box::new(transport)
        }
        TransportKind::SocketJsonrpc => Box::new(SocketJsonRpcTransport::new(
            PathBuf::from(endpoint),
            timeout,
        )),
        TransportKind::Http => Box::new(HttpTransport::new(endpoint, timeout)),
    };
    if manifest.daemon.persistent
        && matches!(
            manifest.transport.kind,
            TransportKind::SocketJsonrpc | TransportKind::Http
        )
    {
        transport = Box::new(ManagedDaemonTransport::new(
            transport,
            rendered_managed_daemon_config(manifest, environment, variables, profile_root)?,
        ));
    }
    let optional = |values: &[String]| values.first().cloned().unwrap_or_default();
    let operations = DriverOperations {
        create_session: optional(&manifest.sessions.create),
        submit: optional(&manifest.sessions.submit),
        attach: optional(&manifest.sessions.attach),
        resume: optional(&manifest.sessions.resume),
        steer: optional(&manifest.next_input.steer),
        subturn: optional(&manifest.next_input.subturn),
        queue: optional(&manifest.next_input.queue),
        release_checkpoint: optional(&manifest.concurrency.release),
        spawn_agent: optional(&manifest.agents.spawn),
        agent_child_id_pointer: manifest.agents.child_id_pointer.clone(),
        agent_status: optional(&manifest.agents.status),
        agent_status_result_pointer: manifest.agents.status_result_pointer.clone(),
        agent_collect: optional(&manifest.agents.collect),
        agent_collect_events_pointer: manifest.agents.collect_events_pointer.clone(),
        cancel: optional(&manifest.agents.cancel),
        close: optional(&manifest.sessions.close_delete),
        shutdown: optional(&manifest.daemon.shutdown),
        wait_ready: optional(&manifest.sessions.wait_ready),
        shutdown_result: manifest.daemon.shutdown_result.clone(),
    };
    Ok(Box::new(
        GenericDriver::new(transport).with_operations(operations),
    ))
}

fn rendered_managed_daemon_config(
    manifest: &Manifest,
    environment: &BTreeMap<String, String>,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<ManagedDaemonConfig> {
    let mut readiness = manifest.daemon.readiness.clone();
    readiness.target = crate::manifest::render_template(&readiness.target, variables)?;
    readiness.command = resolve_local_program(&render_argv(&readiness.command, variables)?)?;
    Ok(ManagedDaemonConfig {
        command: resolve_local_program(&render_argv(&manifest.daemon.start, variables)?)?,
        launcher_exits: manifest.daemon.launcher_exits,
        initialize_command: resolve_local_program(&render_argv(
            &manifest.daemon.initialize,
            variables,
        )?)?,
        initialize_marker: profile_root.join("daemon-initialized"),
        environment: environment.clone(),
        readiness,
        shutdown_command: resolve_local_program(&render_argv(
            &manifest.daemon.shutdown,
            variables,
        )?)?,
        shutdown_result: manifest.daemon.shutdown_result.clone(),
        grace: Duration::from_millis(manifest.daemon.grace_ms.max(1)),
        log_directory: profile_root.join("daemon-logs"),
    })
}

fn resolve_local_program(template: &[String]) -> Result<Vec<String>> {
    let mut command = template.to_vec();
    if let Some(program) = command.first_mut() {
        if Path::new(program).is_file() {
            *program = std::fs::canonicalize(&*program)?
                .to_string_lossy()
                .into_owned();
        } else if !Path::new(program).is_file()
            && Path::new(program)
                .file_name()
                .and_then(|name| name.to_str())
                == Some("ahrb-mock-harness")
        {
            let executable = std::env::current_exe()?;
            if let Some(parent) = executable.parent() {
                let sibling = parent.join("ahrb-mock-harness");
                if sibling.is_file() {
                    *program = sibling.to_string_lossy().into_owned();
                }
            }
        }
    }
    Ok(command)
}

fn render_argv(argv: &[String], variables: &BTreeMap<String, String>) -> Result<Vec<String>> {
    let mut rendered: Vec<String> = argv
        .iter()
        .map(|argument| crate::manifest::render_template(argument, variables))
        .collect::<Result<Vec<_>>>()?;
    if let Some(program) = rendered.first_mut() {
        if !Path::new(program).is_file()
            && Path::new(program)
                .file_name()
                .and_then(|name| name.to_str())
                == Some("ahrb-mock-harness")
        {
            let executable = std::env::current_exe()?;
            if let Some(parent) = executable.parent() {
                let sibling = parent.join("ahrb-mock-harness");
                if sibling.is_file() {
                    *program = sibling.to_string_lossy().into_owned();
                }
            }
        }
    }
    Ok(rendered)
}

fn build_workflow(
    rows: &[u8],
    profile_root: &Path,
    state_barrier: bool,
    profile: Profile,
    manifest: &Manifest,
) -> Result<(Workflow, BTreeMap<u8, Vec<String>>)> {
    let scenario = "ahrb-matrix-v1";
    let mut actors = BTreeMap::new();
    let mut barriers = BTreeMap::new();
    let mut responses = Vec::new();
    let mut actors_by_row = BTreeMap::new();
    for row in rows {
        let count = match *row {
            26 => 8,
            67 => match profile {
                Profile::Quick => 3 * 3,
                Profile::Cert => 7 * 20,
            },
            69 => match profile {
                Profile::Quick => 3 * 2,
                Profile::Cert => 7 * 2,
            },
            60 | 61 | 71 | 72 => match profile {
                Profile::Quick if *row == 71 => 2,
                Profile::Quick => 1,
                Profile::Cert if *row == 71 => 6,
                Profile::Cert if *row == 72 => 5,
                Profile::Cert => 3,
            },
            _ => 1,
        };
        let mut row_actors = Vec::new();
        for index in 0..count {
            let actor = if *row == 67 {
                let turns = match profile {
                    Profile::Quick => 3,
                    Profile::Cert => 20,
                };
                format!("r67p{}t{}", index / turns + 1, index % turns + 1)
            } else if *row == 69 {
                format!(
                    "r69p{}{}",
                    index / 2 + 1,
                    if index % 2 == 0 { "s" } else { "f" }
                )
            } else if count == 1 {
                format!("r{row:02}")
            } else {
                format!("r{row:02}a{}", index + 1)
            };
            let prompt = format!(
                "AHRB matrix row {row} {}",
                route_marker(scenario, &actor, "start")
            );
            actors.insert(
                actor.clone(),
                Actor {
                    id: actor.clone(),
                    parent: None,
                    prompt,
                    workspace: profile_root
                        .join("workspaces")
                        .join(&actor)
                        .to_string_lossy()
                        .into_owned(),
                },
            );
            let mut row_responses = scripted_row(*row, scenario, &actor, manifest)?;
            if *row == 42 {
                let turns = match profile {
                    Profile::Quick => 20,
                    Profile::Cert => 100,
                };
                for turn in 2..=turns {
                    let turn_actor = format!("{actor}-turn-{turn}");
                    actors.insert(
                        turn_actor.clone(),
                        Actor {
                            id: turn_actor.clone(),
                            parent: None,
                            prompt: format!(
                                "AHRB model request efficiency turn {turn} {}",
                                route_marker(scenario, &turn_actor, "start")
                            ),
                            workspace: profile_root
                                .join("workspaces")
                                .join(&turn_actor)
                                .to_string_lossy()
                                .into_owned(),
                        },
                    );
                    row_responses.push(ScriptedResponse {
                        scenario: scenario.to_owned(),
                        actor: turn_actor,
                        checkpoint: "start".to_owned(),
                        request_hash: String::new(),
                        response: success_value(),
                        fault: None,
                        barrier: None,
                    });
                }
            }
            if *row == 26 && state_barrier {
                if let Some(first) = row_responses.first_mut() {
                    first.barrier = Some("row26-steady".to_owned());
                }
            }
            responses.extend(row_responses);
            row_actors.push(actor);
        }
        actors_by_row.insert(*row, row_actors);
    }
    if state_barrier {
        if let Some(row_actors) = actors_by_row.get(&26) {
            barriers.insert(
                "row26-steady".to_owned(),
                Barrier {
                    name: "row26-steady".to_owned(),
                    actors: row_actors.clone(),
                    checkpoint: "start".to_owned(),
                },
            );
        }
    }
    if rows.contains(&16) {
        for actor in ["r16t2", "r16t3"] {
            actors.insert(
                actor.to_owned(),
                Actor {
                    id: actor.to_owned(),
                    parent: Some("r16".to_owned()),
                    prompt: format!("AHRB persisted transcript actor {actor}"),
                    workspace: profile_root
                        .join("workspaces")
                        .join("r16")
                        .to_string_lossy()
                        .into_owned(),
                },
            );
            responses.push(ScriptedResponse {
                scenario: scenario.to_owned(),
                actor: actor.to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            });
        }
    }
    if rows.contains(&30) {
        actors.insert(
            "r30b".to_owned(),
            Actor {
                id: "r30b".to_owned(),
                parent: Some("r30".to_owned()),
                prompt: "AHRB session replay continuation B".to_owned(),
                workspace: profile_root
                    .join("workspaces")
                    .join("r30")
                    .to_string_lossy()
                    .into_owned(),
            },
        );
        responses.push(ScriptedResponse {
            scenario: scenario.to_owned(),
            actor: "r30b".to_owned(),
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        });
    }
    if rows.iter().any(|row| (20..=29).contains(row) || *row == 49) {
        add_resource_workflow(
            scenario,
            profile_root,
            ResourceTimingPlan::for_profile(ResourceProfile::from(profile)),
            manifest,
            &mut actors,
            &mut responses,
        )?;
    }
    if rows.contains(&66) {
        let (repetitions, token_limit, cost_limit, time_limit_ms) = match profile {
            Profile::Quick => (3_u32, 128_u64, 1_000_u64, 2_000_u64),
            Profile::Cert => (7_u32, 1_024_u64, 10_000_u64, 5_000_u64),
        };
        let cost_fixture = cost_limit.saturating_sub(25);
        let cost_fixture_input = cost_fixture.saturating_sub(3) / 2;
        for repetition in 1..=repetitions {
            for case in ["tokens", "cost", "time"] {
                let actor = format!("{case}-r{repetition}");
                actors.insert(
                    actor.clone(),
                    Actor {
                        id: actor.clone(),
                        parent: None,
                        prompt: format!("AHRB Wave-4 budget provider actor {actor}"),
                        workspace: profile_root
                            .join("workspaces")
                            .join(format!("budget-{actor}"))
                            .to_string_lossy()
                            .into_owned(),
                    },
                );
                let usage_value = |input_tokens: u64, output_tokens: u64| {
                    json!({
                        "text": "SUCCESS",
                        "_ahrb_usage": {
                            "input_tokens": input_tokens,
                            "output_tokens": output_tokens
                        }
                    })
                };
                match case {
                    "tokens" => {
                        responses.push(ScriptedResponse {
                            scenario: "ahrb-matrix-v1".to_owned(),
                            actor: actor.clone(),
                            checkpoint: "fixture".to_owned(),
                            request_hash: String::new(),
                            response: usage_value(token_limit.saturating_sub(32), 16),
                            fault: None,
                            barrier: None,
                        });
                        responses.push(ScriptedResponse {
                            scenario: "ahrb-matrix-v1".to_owned(),
                            actor,
                            checkpoint: "crossing".to_owned(),
                            request_hash: String::new(),
                            response: usage_value(8, 16),
                            fault: None,
                            barrier: None,
                        });
                    }
                    "cost" => {
                        responses.push(ScriptedResponse {
                            scenario: "ahrb-matrix-v1".to_owned(),
                            actor: actor.clone(),
                            checkpoint: "fixture".to_owned(),
                            request_hash: String::new(),
                            response: usage_value(cost_fixture_input, 1),
                            fault: None,
                            barrier: None,
                        });
                        responses.push(ScriptedResponse {
                            scenario: "ahrb-matrix-v1".to_owned(),
                            actor,
                            checkpoint: "crossing".to_owned(),
                            request_hash: String::new(),
                            response: usage_value(10, 10),
                            fault: None,
                            barrier: None,
                        });
                    }
                    "time" => responses.push(ScriptedResponse {
                        scenario: "ahrb-matrix-v1".to_owned(),
                        actor,
                        checkpoint: "crossing".to_owned(),
                        request_hash: String::new(),
                        response: usage_value(1, 1),
                        fault: Some(Fault::Delay {
                            delay_ms: time_limit_ms,
                        }),
                        barrier: None,
                    }),
                    _ => {}
                }
            }
        }
    }
    if rows.contains(&68) {
        let repetitions = match profile {
            Profile::Quick => 1_u32,
            Profile::Cert => 5_u32,
        };
        for repetition in 1..=repetitions {
            for phase in ["seed", "original", "fork"] {
                let actor = format!("r68cli{repetition}-{phase}");
                actors.insert(
                    actor.clone(),
                    Actor {
                        id: actor.clone(),
                        parent: None,
                        prompt: format!("AHRB Wave-4 session CLI {phase}"),
                        workspace: profile_root
                            .join("workspaces")
                            .join(&actor)
                            .to_string_lossy()
                            .into_owned(),
                    },
                );
                if phase == "seed" {
                    let terminal = route_marker("ahrb-matrix-v1", &actor, "terminal");
                    let call = mapped_tool_call(
                        manifest,
                        "write",
                        format!("row68-seed-{repetition}"),
                        json!({
                            "path": format!("row68-seed-{repetition}.txt"),
                            "content": format!("committed-seed{terminal}")
                        }),
                    )?;
                    responses.push(ScriptedResponse {
                        scenario: "ahrb-matrix-v1".to_owned(),
                        actor: actor.clone(),
                        checkpoint: "start".to_owned(),
                        request_hash: String::new(),
                        response: json!({
                            "tool_calls": [call],
                            "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
                        }),
                        fault: None,
                        barrier: None,
                    });
                    responses.push(ScriptedResponse {
                        scenario: "ahrb-matrix-v1".to_owned(),
                        actor,
                        checkpoint: "terminal".to_owned(),
                        request_hash: String::new(),
                        response: json!({
                            "text": "SUCCESS",
                            "_ahrb_usage": {"input_tokens": 140, "output_tokens": 30}
                        }),
                        fault: None,
                        barrier: None,
                    });
                } else {
                    responses.push(ScriptedResponse {
                        scenario: "ahrb-matrix-v1".to_owned(),
                        actor,
                        checkpoint: "start".to_owned(),
                        request_hash: String::new(),
                        response: json!({
                            "text": format!("SUCCESS-{phase}-{repetition}"),
                            "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
                        }),
                        fault: None,
                        barrier: None,
                    });
                }
            }
        }
    }
    Ok((
        Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario: scenario.to_owned(),
            actors,
            barriers,
            responses,
        },
        actors_by_row,
    ))
}

fn injection_surface_workflow(profile_root: &Path, label: &str) -> Workflow {
    let scenario = format!("ahrb-row65-injection-{label}");
    let actor = format!("r65-{label}");
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB injection-surface {label} {}",
                    route_marker(&scenario, &actor, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        }],
    }
}

fn injection_carrier_label(component: &InjectionComponent) -> String {
    match component.method {
        InjectionMethod::Environment => format!("env:{}", component.environment),
        InjectionMethod::Cli => format!(
            "cli:{}:{}",
            match component.argv_position {
                ArgvPosition::Prefix => "prefix",
                ArgvPosition::Suffix => "suffix",
            },
            match serde_json::to_string(&component.argv) {
                Ok(value) => value,
                Err(_) => "[]".to_owned(),
            }
        ),
        InjectionMethod::GeneratedConfig => format!(
            "generated-config:{}#{}",
            component.generated_path, component.json_pointer
        ),
        InjectionMethod::Impossible => "impossible".to_owned(),
    }
}

fn insert_cli_injection(
    command: &mut Vec<String>,
    argv: Vec<String>,
    position: ArgvPosition,
) -> Result<()> {
    if command.is_empty() {
        return Err(AhrbError::Validation(
            "row-65 CLI injection has no public command".to_owned(),
        ));
    }
    match position {
        ArgvPosition::Prefix => {
            command.splice(1..1, argv);
        }
        ArgvPosition::Suffix => command.extend(argv),
    }
    Ok(())
}

fn set_generated_injection_value(
    component: &InjectionComponent,
    value: &str,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<()> {
    let path = PathBuf::from(crate::manifest::render_template(
        &component.generated_path,
        variables,
    )?);
    if !path.starts_with(profile_root) {
        return Err(AhrbError::Validation(format!(
            "row-65 generated carrier {} escapes {}",
            path.display(),
            profile_root.display()
        )));
    }
    let text = std::fs::read_to_string(&path).map_err(|error| {
        AhrbError::Protocol(format!(
            "read row-65 generated carrier {}: {error}",
            path.display()
        ))
    })?;
    let (mut document, json_encoding) = match serde_json::from_str::<Value>(&text) {
        Ok(document) => (document, true),
        Err(json_error) => {
            let document = toml::from_str::<toml::Value>(&text).map_err(|toml_error| {
                AhrbError::Validation(format!(
                    "row-65 generated carrier {} is neither JSON ({json_error}) nor TOML ({toml_error})",
                    path.display()
                ))
            })?;
            (serde_json::to_value(document)?, false)
        }
    };
    let destination = document
        .pointer_mut(&component.json_pointer)
        .ok_or_else(|| {
            AhrbError::Validation(format!(
                "row-65 generated carrier {} lacks pointer {}",
                path.display(),
                component.json_pointer
            ))
        })?;
    *destination = Value::String(value.to_owned());
    let encoded = if json_encoding {
        serde_json::to_vec_pretty(&document)?
    } else {
        toml::to_string_pretty(&document)
            .map_err(|error| {
                AhrbError::Protocol(format!(
                    "serialize row-65 TOML carrier {}: {error}",
                    path.display()
                ))
            })?
            .into_bytes()
    };
    std::fs::write(&path, encoded).map_err(|error| {
        AhrbError::Protocol(format!(
            "write row-65 generated carrier {}: {error}",
            path.display()
        ))
    })?;
    let file = std::fs::OpenOptions::new().write(true).open(&path)?;
    file.sync_all()?;
    Ok(())
}

fn apply_injection_component(
    manifest: &mut Manifest,
    component: &InjectionComponent,
    value: &str,
    variables: &BTreeMap<String, String>,
    environment: &mut BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<()> {
    match component.method {
        InjectionMethod::Environment => {
            environment.insert(component.environment.clone(), value.to_owned());
        }
        InjectionMethod::Cli => {
            let rendered = render_argv(&component.argv, variables)?;
            let command = if matches!(
                manifest.transport.kind,
                TransportKind::SocketJsonrpc | TransportKind::Http
            ) && manifest.daemon.persistent
            {
                &mut manifest.daemon.start
            } else {
                &mut manifest.transport.command
            };
            insert_cli_injection(command, rendered, component.argv_position)?;
        }
        InjectionMethod::GeneratedConfig => {
            set_generated_injection_value(component, value, variables, profile_root)?;
        }
        InjectionMethod::Impossible => {}
    }
    Ok(())
}

fn command_contains_credential(manifest: &Manifest, credential: &str) -> bool {
    manifest
        .transport
        .command
        .iter()
        .chain(manifest.daemon.start.iter())
        .chain(manifest.sessions.create.iter())
        .chain(manifest.sessions.submit.iter())
        .chain(manifest.sessions.resume.iter())
        .any(|argument| argument.contains("{{credential}}") || argument.contains(credential))
}

fn injection_credential_fingerprints(credential: &str) -> BTreeSet<String> {
    [
        credential.to_owned(),
        format!("Bearer {credential}"),
        format!("bearer {credential}"),
        format!("Basic {credential}"),
    ]
    .into_iter()
    .map(|value| format!("{:x}", Sha256::digest(value.as_bytes())))
    .collect()
}

#[derive(Clone, Debug, Default)]
struct InjectionRunObservation {
    expected_records: Vec<crate::fake_model::ModelRequestRecord>,
    unexpected_records: Vec<crate::fake_model::ModelRequestRecord>,
    expected_physical_requests: u64,
    unexpected_physical_requests: u64,
    credential_rejections: u64,
    active_baseline_credential_rejected: bool,
    terminal_success: bool,
    secret_in_argv: bool,
}

#[derive(Debug)]
enum InjectionTrapServer {
    Tcp(FakeModelServer),
    Mailbox(FakeModelMailboxServer),
}

#[derive(Debug)]
struct BoundInjectionTrap {
    server: InjectionTrapServer,
    base_url: String,
}

impl BoundInjectionTrap {
    async fn bind(
        engine: Arc<FakeModelEngine>,
        credential: Option<&str>,
        label: &str,
    ) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let tcp = if let Some(credential) = credential {
            FakeModelServer::bind_requiring_credential(
                address,
                Arc::clone(&engine),
                credential.to_owned(),
            )
            .await
        } else {
            FakeModelServer::bind(address, Arc::clone(&engine)).await
        };
        match tcp {
            Ok(server) => {
                let base_url = server.base_url();
                Ok(Self {
                    server: InjectionTrapServer::Tcp(server),
                    base_url,
                })
            }
            Err(_tcp_error) => {
                #[cfg(target_os = "macos")]
                let root = PathBuf::from("/private/tmp");
                #[cfg(not(target_os = "macos"))]
                let root = PathBuf::from("/tmp");
                let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
                let directory = root.join(format!(
                    "ahrb-r65-{}-{sequence}-{label}",
                    std::process::id()
                ));
                let server = if let Some(credential) = credential {
                    FakeModelMailboxServer::bind_requiring_credential(
                        directory.clone(),
                        engine,
                        credential.to_owned(),
                    )
                    .await?
                } else {
                    FakeModelMailboxServer::bind(directory.clone(), engine).await?
                };
                Ok(Self {
                    server: InjectionTrapServer::Mailbox(server),
                    base_url: format!("ahrb+mailbox://{}", directory.display()),
                })
            }
        }
    }

    fn physical_request_count(&self) -> u64 {
        match &self.server {
            InjectionTrapServer::Tcp(server) => server.physical_request_count(),
            InjectionTrapServer::Mailbox(server) => server.physical_request_count(),
        }
    }

    fn credential_rejection_count(&self) -> u64 {
        match &self.server {
            InjectionTrapServer::Tcp(server) => server.credential_rejection_count(),
            InjectionTrapServer::Mailbox(server) => server.credential_rejection_count(),
        }
    }

    async fn probe_rejected_credential(&self, credential: &str) -> Result<bool> {
        match &self.server {
            InjectionTrapServer::Tcp(server) => {
                probe_rejected_credential(server.local_addr(), credential).await
            }
            InjectionTrapServer::Mailbox(server) => {
                probe_rejected_mailbox_credential(server.directory(), credential).await
            }
        }
    }

    async fn shutdown(self) -> Result<()> {
        match self.server {
            InjectionTrapServer::Tcp(server) => server.shutdown().await,
            InjectionTrapServer::Mailbox(server) => server.shutdown().await,
        }
    }
}

async fn probe_rejected_credential(address: SocketAddr, credential: &str) -> Result<bool> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    let body = b"{}";
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .map_err(|_| AhrbError::Timeout("row-65 credential rejection control".to_owned()))??;
    let Some(first_line) = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
    else {
        return Ok(false);
    };
    Ok(first_line
        .split_ascii_whitespace()
        .nth(1)
        .and_then(|status| status.parse::<u16>().ok())
        == Some(401))
}

async fn probe_rejected_mailbox_credential(directory: &Path, credential: &str) -> Result<bool> {
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let id = format!("row65-probe-{}-{sequence}", std::process::id());
    let envelope = ProviderMailboxRequest {
        id: id.clone(),
        method: "POST".to_owned(),
        path: "/v1/chat/completions".to_owned(),
        headers: BTreeMap::from([("Authorization".to_owned(), format!("Bearer {credential}"))]),
        body: b"{}".to_vec(),
    };
    let temporary = directory.join(format!("{id}.request.tmp"));
    let request = directory.join(format!("{id}.request.json"));
    tokio::fs::write(&temporary, serde_json::to_vec(&envelope)?).await?;
    tokio::fs::rename(temporary, request).await?;
    let response_path = directory.join(format!("{id}.response.json"));
    let bytes = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match tokio::fs::read(&response_path).await {
                Ok(bytes) => break Ok::<Vec<u8>, AhrbError>(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(error) => break Err(error.into()),
            }
        }
    })
    .await
    .map_err(|_| AhrbError::Timeout("row-65 mailbox credential rejection control".to_owned()))??;
    tokio::fs::remove_file(response_path).await?;
    let response: ProviderMailboxResponse = serde_json::from_slice(&bytes)?;
    Ok(response.id == id && response.status == 401 && response.error.is_none())
}

#[allow(clippy::too_many_arguments)]
async fn collect_injection_run(
    manifest: &Manifest,
    run_profile_root: &Path,
    label: &str,
    provider: &str,
    credential: &str,
    baseline_credential: &str,
    separate_baseline_listener: bool,
) -> Result<InjectionRunObservation> {
    let profile_root = run_profile_root.join(format!("derived-row65-{label}"));
    prepare_profile(manifest, &profile_root)?;
    let workflow = injection_surface_workflow(&profile_root, label);
    workflow.validate()?;
    let expected_engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let expected_server = BoundInjectionTrap::bind(
        Arc::clone(&expected_engine),
        (label == "credential").then_some(credential),
        &format!("{label}-expected"),
    )
    .await
    .map_err(|error| AhrbError::Protocol(format!("row-65 {label} expected trap: {error}")))?;
    let expected_base_url = expected_server.base_url.clone();
    let unexpected = if separate_baseline_listener {
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let server =
            BoundInjectionTrap::bind(Arc::clone(&engine), None, &format!("{label}-baseline"))
                .await
                .map_err(|error| {
                    AhrbError::Protocol(format!("row-65 {label} baseline trap: {error}"))
                })?;
        Some((engine, server))
    } else {
        None
    };
    let baseline_base_url = unexpected
        .as_ref()
        .map_or(expected_base_url.as_str(), |(_, server)| {
            server.base_url.as_str()
        });
    let variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
        ("base_url".to_owned(), baseline_base_url.to_owned()),
        ("credential".to_owned(), baseline_credential.to_owned()),
        ("model".to_owned(), manifest.fake_model.model.clone()),
        ("provider".to_owned(), manifest.fake_model.model.clone()),
    ]);
    let mut trial_manifest = manifest.clone();
    let mut environment = isolated_environment(&trial_manifest, &variables)?;
    environment.insert(
        manifest.fake_model.base_url_env.clone(),
        baseline_base_url.to_owned(),
    );
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        baseline_credential.to_owned(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    write_generated_files(&trial_manifest, &variables, &profile_root)?;
    let surface = trial_manifest
        .capabilities
        .injection_surface
        .clone()
        .ok_or_else(|| AhrbError::Validation("row-65 injection surface disappeared".to_owned()))?;
    let mut provider_variables = variables.clone();
    provider_variables.insert("provider".to_owned(), provider.to_owned());
    provider_variables.insert("model".to_owned(), provider.to_owned());
    apply_injection_component(
        &mut trial_manifest,
        &surface.provider,
        provider,
        &provider_variables,
        &mut environment,
        &profile_root,
    )?;
    let mut base_url_variables = variables.clone();
    base_url_variables.insert("base_url".to_owned(), expected_base_url);
    let base_url_value = base_url_variables
        .get("base_url")
        .cloned()
        .unwrap_or_default();
    apply_injection_component(
        &mut trial_manifest,
        &surface.base_url,
        &base_url_value,
        &base_url_variables,
        &mut environment,
        &profile_root,
    )?;
    let mut credential_variables = variables.clone();
    credential_variables.insert("credential".to_owned(), credential.to_owned());
    apply_injection_component(
        &mut trial_manifest,
        &surface.credential,
        credential,
        &credential_variables,
        &mut environment,
        &profile_root,
    )?;
    let secret_in_argv = command_contains_credential(&trial_manifest, credential)
        || command_contains_credential(&trial_manifest, baseline_credential);
    let active_baseline_credential_rejected = if label == "credential" {
        expected_server
            .probe_rejected_credential(baseline_credential)
            .await?
    } else {
        false
    };
    let command = if trial_manifest.transport.kind == TransportKind::Exec {
        trial_manifest.transport.command.clone()
    } else {
        render_argv(&trial_manifest.transport.command, &variables)?
    };
    let mut driver = make_driver_with_timeout(
        &trial_manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_turn_timeout(&trial_manifest),
    )?;
    let actor_name = format!("r65-{label}");
    let actor = workflow
        .actors
        .get(&actor_name)
        .ok_or_else(|| AhrbError::Protocol(format!("row-65 actor {actor_name:?} disappeared")))?;
    let mut terminal_success = false;
    let behavioral_run: Result<()> = async {
        driver.start().await?;
        driver.await_readiness().await?;
        let session = driver
            .create_session(&format!("{}:{actor_name}", workflow.scenario))
            .await?;
        driver
            .submit(&session, &actor.prompt, &format!("row-65-{label}"))
            .await?;
        let events = collect_session_terminal(
            &mut driver,
            &session,
            None,
            outer_turn_timeout(&trial_manifest),
        )
        .await?;
        terminal_success = events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count()
            == 1;
        if trial_manifest.transport.kind == TransportKind::Exec
            || !trial_manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        Ok(())
    }
    .await;
    let shutdown = driver.shutdown().await;
    if behavioral_run.is_ok() {
        shutdown?;
    }
    let expected_records = expected_engine.request_records().await;
    let expected_physical_requests = expected_server.physical_request_count();
    let credential_rejections = expected_server.credential_rejection_count();
    let unexpected_records = if let Some((engine, server)) = unexpected {
        let records = engine.request_records().await;
        let physical_requests = server.physical_request_count();
        server.shutdown().await?;
        (records, physical_requests)
    } else {
        (Vec::new(), 0)
    };
    expected_server.shutdown().await?;
    Ok(InjectionRunObservation {
        expected_records,
        unexpected_records: unexpected_records.0,
        expected_physical_requests,
        unexpected_physical_requests: unexpected_records.1,
        credential_rejections,
        active_baseline_credential_rejected,
        terminal_success,
        secret_in_argv,
    })
}

async fn collect_injection_surface_trials(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<InjectionSurfaceTrials> {
    let surface = manifest
        .capabilities
        .injection_surface
        .as_ref()
        .ok_or_else(|| AhrbError::Validation("row-65 requires injection_surface".to_owned()))?;
    let baseline_credential = format!("ahrb-row65-a-{}", &manifest_hash[..16]);
    let trap_credential = format!("ahrb-row65-b-{}", &manifest_hash[..16]);
    let baseline = collect_injection_run(
        manifest,
        run_profile_root,
        "baseline",
        &manifest.fake_model.model,
        &baseline_credential,
        &baseline_credential,
        false,
    )
    .await?;
    let baseline_records = baseline
        .expected_records
        .iter()
        .filter(|record| record.accepted && record.request.actor == "r65-baseline")
        .collect::<Vec<_>>();
    let baseline_fingerprints = injection_credential_fingerprints(&baseline_credential);
    let baseline_runnable = baseline.terminal_success
        && baseline.expected_physical_requests == 1
        && baseline_records.len() == 1
        && baseline_records
            .iter()
            .all(|record| record.request.model == manifest.fake_model.model)
        && baseline_records
            .iter()
            .all(|record| baseline_fingerprints.contains(&record.request.credential_fingerprint))
        && !baseline.secret_in_argv;
    let baseline_provider_requests = baseline.expected_physical_requests;
    let mut requests = baseline.expected_records;
    let mut evidence = Vec::new();
    for (component_name, component) in [
        ("provider", &surface.provider),
        ("base_url", &surface.base_url),
        ("credential", &surface.credential),
    ] {
        if component.method == InjectionMethod::Impossible {
            evidence.push(InjectionTrialEvidence {
                component: component_name,
                method: component.method,
                carrier: injection_carrier_label(component),
                baseline_provider_requests,
                perturbed_provider_requests: 0,
                expected_endpoint_reached: false,
                unexpected_endpoint_requests: 0,
                credential_accepted: false,
                baseline_credential_rejected: false,
                secret_in_argv: false,
                component_verified: false,
            });
            continue;
        }
        let provider = if component_name == "provider" {
            "ahrb-trap-model"
        } else {
            manifest.fake_model.model.as_str()
        };
        let credential = if component_name == "credential" {
            trap_credential.as_str()
        } else {
            baseline_credential.as_str()
        };
        let observation = collect_injection_run(
            manifest,
            run_profile_root,
            component_name,
            provider,
            credential,
            &baseline_credential,
            component_name == "base_url",
        )
        .await?;
        let actor = format!("r65-{component_name}");
        let expected_records = observation
            .expected_records
            .iter()
            .filter(|record| record.accepted && record.request.actor == actor)
            .collect::<Vec<_>>();
        let expected_physical_requests = if component_name == "credential" { 2 } else { 1 };
        let expected_endpoint_reached = expected_records.len() == 1
            && observation.expected_physical_requests == expected_physical_requests;
        let credential_fingerprints = injection_credential_fingerprints(credential);
        let credential_accepted = !expected_records.is_empty()
            && expected_records.iter().all(|record| {
                credential_fingerprints.contains(&record.request.credential_fingerprint)
            });
        let baseline_credential_rejected = component_name == "credential"
            && observation.active_baseline_credential_rejected
            && observation.credential_rejections == 1;
        let component_verified = baseline_runnable
            && observation.terminal_success
            && expected_endpoint_reached
            && observation.unexpected_physical_requests == 0
            && credential_accepted
            && !observation.secret_in_argv
            && match component_name {
                "provider" => expected_records.iter().all(|record| {
                    record.request.model == "ahrb-trap-model"
                        && record.request.model != manifest.fake_model.model
                }),
                "base_url" => true,
                "credential" => baseline_credential_rejected,
                _ => false,
            };
        evidence.push(InjectionTrialEvidence {
            component: component_name,
            method: component.method,
            carrier: injection_carrier_label(component),
            baseline_provider_requests,
            perturbed_provider_requests: observation.expected_physical_requests,
            expected_endpoint_reached,
            unexpected_endpoint_requests: observation.unexpected_physical_requests,
            credential_accepted,
            baseline_credential_rejected,
            secret_in_argv: observation.secret_in_argv,
            component_verified,
        });
        requests.extend(observation.expected_records);
        requests.extend(observation.unexpected_records);
    }
    requests.sort_by(|left, right| {
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
    Ok(InjectionSurfaceTrials {
        evaluation: evaluate_injection_surface(
            surface.provider.method,
            surface.base_url.method,
            surface.credential.method,
            baseline_runnable,
            evidence,
        ),
        requests,
    })
}

fn retry_budget_workflow(profile_root: &Path, profile: Profile) -> Workflow {
    let scenario = "ahrb-row58-retry-budget".to_owned();
    let statuses = match profile {
        Profile::Quick => vec![429_u16],
        Profile::Cert => vec![429_u16, 500_u16],
    };
    let mut actors = BTreeMap::new();
    let mut responses = Vec::new();
    for status in statuses {
        for repetition in 1..=3_u32 {
            let actor = format!("r58-s{status}-r{repetition}");
            actors.insert(
                actor.clone(),
                Actor {
                    id: actor.clone(),
                    parent: None,
                    prompt: format!(
                        "AHRB sustained status {status} retry budget repetition {repetition} {}",
                        route_marker(&scenario, &actor, "start")
                    ),
                    workspace: profile_root
                        .join("workspaces")
                        .join(&actor)
                        .to_string_lossy()
                        .into_owned(),
                },
            );
            responses.push(ScriptedResponse {
                scenario: scenario.clone(),
                actor,
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: Some(Fault::SustainedHttpStatus {
                    status,
                    body: format!("{{\"error\":\"sustained-{status}\"}}"),
                }),
                barrier: None,
            });
        }
    }
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors,
        barriers: BTreeMap::new(),
        responses,
    }
}

async fn calibrate_retry_timer(status: u16, requested_ms: u64) -> Vec<RetryTimerCalibration> {
    let mut samples = Vec::with_capacity(20);
    for index in 1..=20_u32 {
        let started_ns = monotonic_timestamp_ns();
        tokio::time::sleep(Duration::from_millis(requested_ms)).await;
        let finished_ns = monotonic_timestamp_ns();
        let actual_ms = finished_ns.saturating_sub(started_ns) as f64 / 1_000_000.0;
        samples.push(RetryTimerCalibration {
            status,
            index,
            requested_ms,
            actual_ms,
            absolute_error_ms: (actual_ms - requested_ms as f64).abs(),
        });
    }
    samples
}

fn row56_case_label(case: ChildFailureCase) -> &'static str {
    match case {
        ChildFailureCase::Crash => "crash",
        ChildFailureCase::Hang => "hang",
    }
}

#[allow(clippy::too_many_arguments)]
fn row56_record_events(
    collected: &mut BTreeMap<String, NormalizedEvent>,
    events: Vec<NormalizedEvent>,
    role: &str,
    case: ChildFailureCase,
    parent_operation_start_ns: u64,
    receipt_start_ns: u64,
    receipt_end_ns: u64,
) {
    for mut event in events {
        let receipt = json!({
            "role": role,
            "case": row56_case_label(case),
            "parent_operation_start_ns": parent_operation_start_ns,
            "receipt_start_ns": receipt_start_ns,
            "receipt_end_ns": receipt_end_ns,
        });
        if let Some(payload) = event.payload.as_object_mut() {
            payload.insert("_ahrb_row56_receipt".to_owned(), receipt);
        } else {
            let payload = std::mem::replace(&mut event.payload, Value::Null);
            event.payload = json!({
                "source_payload": payload,
                "_ahrb_row56_receipt": receipt,
            });
        }
        collected.entry(event.id.clone()).or_insert(event);
    }
}

async fn row56_child_residue_count(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    baseline: &BTreeSet<crate::process::ProcIdentity>,
    grace: Duration,
) -> Result<u32> {
    let started = Instant::now();
    loop {
        let tree = sampler.discover(roots)?;
        if !baseline
            .iter()
            .all(|identity| tree.members.contains_key(identity))
        {
            return Err(AhrbError::Protocol(
                "row-56 parent controller identity disappeared during child residue audit"
                    .to_owned(),
            ));
        }
        let residue = tree
            .members
            .keys()
            .filter(|identity| !baseline.contains(identity))
            .count();
        if residue == 0 || started.elapsed() >= grace {
            return Ok(u32::try_from(residue).map_or(u32::MAX, |value| value));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn row56_status_observation(
    status: &Value,
    repetition: u32,
    case: ChildFailureCase,
    receipt_start_ns: u64,
    receipt_end_ns: u64,
) -> Result<(String, Value)> {
    let events = status.as_array().ok_or_else(|| {
        AhrbError::Protocol(
            "row-56 declared agent.status result locator did not select an event array".to_owned(),
        )
    })?;
    let bytes = serde_json::to_vec(status)?;
    let fingerprint = format!("{:x}", Sha256::digest(bytes));
    let terminal_count = events.iter().filter(|event| {
        event
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|kind| {
                matches!(
                    kind,
                    "terminal-success"
                        | "terminal-failure"
                        | "terminal-cancelled"
                        | "terminal-timeout"
                )
            })
    });
    Ok((
        fingerprint.clone(),
        json!({
            "repetition": repetition,
            "case": row56_case_label(case),
            "receipt_start_ns": receipt_start_ns,
            "receipt_end_ns": receipt_end_ns,
            "event_count": events.len(),
            "terminal_count": terminal_count.count(),
            "result_sha256": fingerprint,
        }),
    ))
}

async fn collect_child_failure_case(
    driver: &mut HarnessDriver,
    sampler: &mut dyn Sampler,
    roots: &[u32],
    repetition: u32,
    case: ChildFailureCase,
    turn_timeout_ms: u64,
    close_sessions: bool,
) -> Result<(ChildFailureTrial, Vec<NormalizedEvent>, Vec<Value>)> {
    let label = row56_case_label(case);
    let parent = driver
        .create_session(&format!("ahrb-row56-{label}-parent-r{repetition}"))
        .await?;
    let baseline_tree = sampler.discover(roots)?;
    if baseline_tree.members.is_empty() {
        return Err(AhrbError::Protocol(
            "row-56 could not resolve the parent controller owned tree".to_owned(),
        ));
    }
    let baseline = baseline_tree
        .members
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    let parent_operation_start_ns = monotonic_timestamp_ns();
    let outer_deadline_ms = turn_timeout_ms.saturating_add(2_000);
    let outer_deadline_ns = parent_operation_start_ns
        .checked_add(outer_deadline_ms.saturating_mul(1_000_000))
        .ok_or_else(|| AhrbError::Validation("row-56 outer deadline overflow".to_owned()))?;
    let spawn_remaining =
        Duration::from_nanos(outer_deadline_ns.saturating_sub(monotonic_timestamp_ns()));
    let child = tokio::time::timeout(
        spawn_remaining,
        driver.spawn_agent(&parent, &format!("ahrb-row56-{label}-r{repetition}"), None),
    )
    .await
    .map_err(|_| {
        AhrbError::Timeout(format!(
            "row-56 {label} agent.spawn exceeded the parent-start outer deadline"
        ))
    })??;
    let mut child_events = BTreeMap::new();
    let mut parent_events = BTreeMap::new();
    let mut status_observations = Vec::new();
    let mut last_status_fingerprint = None;
    let mut child_failure_received_ns = None;
    let mut child_response_headers_ns = None;
    let mut parent_terminal_received_ns = None;
    let mut parent_after = None;
    let mut outer_kill_used = false;

    loop {
        let now_ns = monotonic_timestamp_ns();
        if now_ns >= outer_deadline_ns {
            outer_kill_used = true;
            break;
        }
        let remaining = Duration::from_nanos(outer_deadline_ns.saturating_sub(now_ns));
        let poll = tokio::time::timeout(remaining, async {
            let status_start_ns = monotonic_timestamp_ns();
            let status = driver.agent_status(&child).await?;
            let status_end_ns = monotonic_timestamp_ns();
            let child_start_ns = monotonic_timestamp_ns();
            let child_suffix = driver.agent_collect(&child).await?;
            let child_end_ns = monotonic_timestamp_ns();
            let parent_start_ns = monotonic_timestamp_ns();
            let parent_suffix = driver.attach(&parent, parent_after).await?;
            let parent_end_ns = monotonic_timestamp_ns();
            Ok::<_, AhrbError>((
                status,
                status_start_ns,
                status_end_ns,
                child_suffix,
                child_start_ns,
                child_end_ns,
                parent_suffix,
                parent_start_ns,
                parent_end_ns,
            ))
        })
        .await;
        let (
            status,
            status_start_ns,
            status_end_ns,
            child_suffix,
            child_start_ns,
            child_end_ns,
            parent_suffix,
            parent_start_ns,
            parent_end_ns,
        ) = match poll {
            Ok(result) => result?,
            Err(_) => {
                outer_kill_used = true;
                break;
            }
        };
        let (fingerprint, observation) =
            row56_status_observation(&status, repetition, case, status_start_ns, status_end_ns)?;
        if last_status_fingerprint.as_deref() != Some(fingerprint.as_str()) {
            status_observations.push(observation);
            last_status_fingerprint = Some(fingerprint);
        }
        if child_failure_received_ns.is_none()
            && child_suffix.iter().any(|event| {
                case == ChildFailureCase::Crash
                    && event.event == EventVocab::TerminalFailure
                    && event.payload.get("checkpoint").and_then(Value::as_str)
                        == Some("row56-crash")
            })
        {
            child_failure_received_ns = Some(child_end_ns);
        }
        if child_response_headers_ns.is_none()
            && child_suffix.iter().any(|event| {
                case == ChildFailureCase::Hang
                    && event.event == EventVocab::ModelResponse
                    && event.payload.get("checkpoint").and_then(Value::as_str) == Some("row56-hang")
                    && event
                        .payload
                        .get("response_headers")
                        .and_then(Value::as_bool)
                        == Some(true)
            })
        {
            child_response_headers_ns = Some(child_end_ns);
        }
        row56_record_events(
            &mut child_events,
            child_suffix,
            "child",
            case,
            parent_operation_start_ns,
            child_start_ns,
            child_end_ns,
        );
        if parent_terminal_received_ns.is_none()
            && parent_suffix.iter().any(|event| is_terminal(&event.event))
        {
            parent_terminal_received_ns = Some(parent_end_ns);
        }
        parent_after = parent_suffix
            .iter()
            .map(|event| Cursor(event.cursor))
            .max()
            .or(parent_after);
        row56_record_events(
            &mut parent_events,
            parent_suffix,
            "parent",
            case,
            parent_operation_start_ns,
            parent_start_ns,
            parent_end_ns,
        );
        let child_terminal = child_events.values().any(|event| is_terminal(&event.event));
        if parent_terminal_received_ns.is_some() && child_terminal {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    if outer_kill_used {
        let cancel_grace = Duration::from_millis(500);
        let _ = tokio::time::timeout(cancel_grace, driver.cancel(&child)).await;
        let _ = tokio::time::timeout(cancel_grace, driver.cancel(&parent)).await;
        parent_terminal_received_ns = Some(monotonic_timestamp_ns());
    }
    let accepted_spawns = parent_events
        .values()
        .filter(|event| {
            event.event == EventVocab::TurnAccepted
                && event.payload.get("operation").and_then(Value::as_str) == Some("agent.spawn")
                && event
                    .payload
                    .get("child_session_id")
                    .and_then(Value::as_str)
                    == Some(child.0.as_str())
        })
        .count();
    if accepted_spawns != 1 {
        return Err(AhrbError::Protocol(format!(
            "row-56 {label} observed {accepted_spawns} accepted parent spawn boundaries"
        )));
    }
    let parent_failure_terminals = parent_events
        .values()
        .filter(|event| match case {
            ChildFailureCase::Crash => event.event == EventVocab::TerminalFailure,
            ChildFailureCase::Hang => matches!(
                event.event,
                EventVocab::TerminalFailure | EventVocab::TerminalCancelled
            ),
        })
        .count();
    let child_failure_terminals = child_events
        .values()
        .filter(|event| is_terminal(&event.event))
        .count();
    let child_cancelled = child_events
        .values()
        .any(|event| event.event == EventVocab::TerminalCancelled);
    let success_contradiction = parent_events
        .values()
        .chain(child_events.values())
        .any(|event| event.event == EventVocab::TerminalSuccess);
    let child_residue_count =
        row56_child_residue_count(sampler, roots, &baseline, Duration::from_millis(2_000)).await?;
    let parent_terminal_received_ns = parent_terminal_received_ns.ok_or_else(|| {
        AhrbError::Protocol(format!(
            "row-56 {label} omitted the parent terminal receipt boundary"
        ))
    })?;
    let mut events = parent_events
        .into_values()
        .chain(child_events.into_values())
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        (&left.session_id, left.cursor, &left.id).cmp(&(&right.session_id, right.cursor, &right.id))
    });
    if close_sessions {
        driver.close(&child).await?;
        driver.close(&parent).await?;
    }
    Ok((
        ChildFailureTrial {
            repetition,
            case,
            parent_operation_start_ns,
            child_failure_received_ns,
            child_response_headers_ns,
            parent_terminal_received_ns,
            parent_failure_terminals: u32::try_from(parent_failure_terminals)
                .map_or(u32::MAX, |value| value),
            child_failure_terminals: u32::try_from(child_failure_terminals)
                .map_or(u32::MAX, |value| value),
            child_cancelled,
            success_contradiction,
            child_residue_count,
            outer_kill_used,
        },
        events,
        status_observations,
    ))
}

fn child_failure_workflow(profile_root: &Path) -> Workflow {
    let scenario = "ahrb-row56-child-failure".to_owned();
    let actor_name = "row56-provider-probe".to_owned();
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor_name.clone(),
            Actor {
                id: actor_name.clone(),
                parent: None,
                prompt: format!(
                    "AHRB row 56 provider probe {}",
                    route_marker(&scenario, &actor_name, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor: actor_name,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        }],
    }
}

async fn collect_child_failure_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<ChildFailureTrials> {
    let profile_root = run_profile_root.join("derived-row56");
    prepare_profile(manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare row-56 profile: {error}")))?;
    let workflow = child_failure_workflow(&profile_root);
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!("ahrb-{}-row56-{}", &manifest_hash[..16], std::process::id());
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    environment.insert(
        "AHRB_MOCK_TURN_TIMEOUT_MS".to_owned(),
        manifest.resources.turn_timeout_ms.to_string(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential.clone());
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        Duration::from_millis(manifest.resources.turn_timeout_ms.saturating_add(2_000)),
    )?;
    driver.start().await?;
    driver.await_readiness().await?;
    let mut sampler = platform_sampler();
    let roots = verified_process_roots(
        manifest,
        sampler.as_mut(),
        driver.owned_pids(),
        driver.daemon_pid(),
    )?;
    if roots.is_empty() {
        return Err(AhrbError::Protocol(
            "row-56 declared native delegation but exposed no owned controller root".to_owned(),
        ));
    }
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let mut collected = ChildFailureTrials {
        events: Vec::new(),
        evidence: Vec::new(),
        status_observations: Vec::new(),
    };
    for repetition in 1..=repetitions {
        for case in [ChildFailureCase::Crash, ChildFailureCase::Hang] {
            let (trial, events, statuses) = collect_child_failure_case(
                &mut driver,
                sampler.as_mut(),
                &roots,
                repetition,
                case,
                manifest.resources.turn_timeout_ms,
                manifest.transport.kind == TransportKind::Exec
                    || !manifest.sessions.close_delete.is_empty(),
            )
            .await?;
            collected.evidence.push(trial);
            collected.events.extend(events);
            collected.status_observations.extend(statuses);
        }
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    Ok(collected)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalMatrixCase {
    Sigterm,
    Sigint2,
    Sighup,
    StdinEof,
}

impl SignalMatrixCase {
    fn label(self) -> &'static str {
        match self {
            Self::Sigterm => "sigterm",
            Self::Sigint2 => "sigint2",
            Self::Sighup => "sighup",
            Self::StdinEof => "stdin-eof",
        }
    }

    #[cfg(unix)]
    fn unix_signal(self) -> Option<i32> {
        match self {
            Self::Sigterm => Some(libc::SIGTERM),
            Self::Sigint2 => Some(libc::SIGINT),
            Self::Sighup => Some(libc::SIGHUP),
            Self::StdinEof => None,
        }
    }
}

fn signal_matrix_workflow(
    profile_root: &Path,
    case: SignalMatrixCase,
    repetition: u32,
) -> Workflow {
    let scenario = "ahrb-row57-signal-matrix".to_owned();
    let actor_name = format!("r57-{}-r{repetition}", case.label());
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor_name.clone(),
            Actor {
                id: actor_name.clone(),
                parent: None,
                prompt: format!(
                    "AHRB row 57 held turn {}",
                    route_marker(&scenario, &actor_name, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor: actor_name,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: Some(Fault::Stall),
            barrier: None,
        }],
    }
}

fn signal_target_identity(
    tree: &ProcessTree,
    roots: &[u32],
) -> Result<crate::process::ProcIdentity> {
    roots
        .iter()
        .find_map(|pid| {
            tree.members
                .keys()
                .copied()
                .find(|identity| identity.pid == *pid)
        })
        .ok_or_else(|| {
            AhrbError::Protocol(
                "row-57 could not resolve a stable owned signal target identity".to_owned(),
            )
        })
}

fn read_signal_matrix_journal(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
    session: &crate::driver::SessionId,
) -> Result<Vec<NormalizedEvent>> {
    if manifest.events.path.trim().is_empty() {
        return Err(AhrbError::Protocol(
            "row-57 durable event journal path is absent".to_owned(),
        ));
    }
    let mut rendered_variables = variables.clone();
    rendered_variables.insert("session_id".to_owned(), session.0.clone());
    let rendered = crate::manifest::render_template(&manifest.events.path, &rendered_variables)?;
    let canonical_profile = profile_root.canonicalize()?;
    let canonical_journal = Path::new(&rendered).canonicalize()?;
    if !canonical_journal.starts_with(&canonical_profile) || !canonical_journal.is_file() {
        return Err(AhrbError::Protocol(format!(
            "row-57 journal {} is not a regular file inside {}",
            canonical_journal.display(),
            canonical_profile.display()
        )));
    }
    let bytes = std::fs::read(&canonical_journal)?;
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err(AhrbError::Protocol(format!(
            "row-57 durable journal {} has no complete committed tail",
            canonical_journal.display()
        )));
    }
    let mut events = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let event: NormalizedEvent = serde_json::from_slice(line).map_err(|error| {
            AhrbError::Protocol(format!(
                "row-57 durable journal {} is not normalized event JSONL: {error}",
                canonical_journal.display()
            ))
        })?;
        if event.session_id == session.0 {
            events.push(event);
        }
    }
    events.sort_by(|left, right| (left.cursor, &left.id).cmp(&(right.cursor, &right.id)));
    Ok(events)
}

async fn wait_for_signal_fixture_hold(
    driver: &mut HarnessDriver,
    engine: &Arc<FakeModelEngine>,
    sampler: &mut dyn Sampler,
    roots: &[u32],
    session: &crate::driver::SessionId,
    actor: &str,
    timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let tree = sampler.discover(roots)?;
        if tree.members.is_empty() {
            return Err(AhrbError::Protocol(
                "row-57 owned harness tree exited before signal delivery".to_owned(),
            ));
        }
        let events = driver.attach(session, None).await?;
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Err(AhrbError::Protocol(
                "row-57 held turn terminalized before signal delivery".to_owned(),
            ));
        }
        let provider_received = engine
            .request_records()
            .await
            .iter()
            .any(|record| record.request.actor == actor);
        if provider_received {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(AhrbError::Timeout(
                "row-57 held turn did not reach the provider".to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_signal_process_exit(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    per_invocation: bool,
    deadline: Instant,
) -> Result<ClientExit> {
    loop {
        let state = if per_invocation {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    AhrbError::Timeout(
                        "row-57 harness did not exit before the applicable outer deadline"
                            .to_owned(),
                    )
                })?;
            let _ = tokio::time::timeout(remaining, driver.attach(session, None))
                .await
                .map_err(|_| {
                    AhrbError::Timeout(
                        "row-57 exit attach exceeded the applicable outer deadline".to_owned(),
                    )
                })??;
            driver.client_exit(session)
        } else {
            driver.harness_exit()?
        };
        if matches!(state, ClientExit::Exited(_)) {
            return Ok(state);
        }
        if matches!(state, ClientExit::NotApplicable) {
            return Err(AhrbError::Protocol(
                "row-57 driver omitted the actual process exit boundary".to_owned(),
            ));
        }
        if Instant::now() >= deadline {
            return Err(AhrbError::Timeout(
                "row-57 harness did not exit before the applicable outer deadline".to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_signal_terminal_receipt(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
    session: &crate::driver::SessionId,
    deadline: Instant,
) -> Result<(Vec<NormalizedEvent>, u64)> {
    loop {
        let events = read_signal_matrix_journal(manifest, variables, profile_root, session)?;
        let receipt_ns = monotonic_timestamp_ns();
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Ok((events, receipt_ns));
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                AhrbError::Timeout(
                    "row-57 durable terminal was not received before the applicable outer deadline"
                        .to_owned(),
                )
            })?;
        tokio::time::sleep(Duration::from_millis(2).min(remaining)).await;
    }
}

async fn signal_matrix_residue_count(sampler: &mut dyn Sampler, roots: &[u32]) -> Result<u32> {
    let started = Instant::now();
    loop {
        let count = sampler.discover(roots)?.members.len();
        if count == 0 || started.elapsed() >= Duration::from_millis(2_000) {
            return Ok(u32::try_from(count).map_or(u32::MAX, |value| value));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn annotate_signal_matrix_events(
    events: &mut [NormalizedEvent],
    case: SignalMatrixCase,
    origin_ns: u64,
    terminal_receipt_ns: u64,
) {
    for event in events {
        let receipt = json!({
            "signal": case.label(),
            "origin_ns": origin_ns,
            "terminal_receipt_ns": terminal_receipt_ns,
        });
        if let Some(payload) = event.payload.as_object_mut() {
            payload.insert("_ahrb_row57_receipt".to_owned(), receipt);
        } else {
            let payload = std::mem::replace(&mut event.payload, Value::Null);
            event.payload = json!({
                "source_payload": payload,
                "_ahrb_row57_receipt": receipt,
            });
        }
    }
}

async fn collect_signal_matrix_case(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
    repetition: u32,
    case: SignalMatrixCase,
) -> Result<(
    SignalCaseTrial,
    Vec<NormalizedEvent>,
    Vec<crate::fake_model::ModelRequestRecord>,
)> {
    let profile_root = run_profile_root
        .join("derived-row57")
        .join(format!("{}-r{repetition}", case.label()));
    prepare_profile(manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare row-57 profile: {error}")))?;
    let workflow = signal_matrix_workflow(&profile_root, case, repetition);
    let actor = workflow
        .actors
        .keys()
        .next()
        .cloned()
        .ok_or_else(|| AhrbError::Protocol("row-57 workflow has no actor".to_owned()))?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row57-{}-r{repetition}",
        &manifest_hash[..16],
        case.label()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .map_or_else(String::new, Clone::clone),
    );
    variables.insert("credential".to_owned(), credential.clone());
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let outer_deadline = outer_turn_timeout(manifest);
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_deadline,
    )?;
    let collected = async {
        driver.start().await?;
        driver.await_readiness().await?;
        let session = driver.create_session(&actor).await?;
        let prompt = workflow
            .actors
            .get(&actor)
            .map(|actor| actor.prompt.as_str())
            .ok_or_else(|| AhrbError::Protocol("row-57 actor disappeared".to_owned()))?;
        driver
            .submit(
                &session,
                prompt,
                &format!("row57-{}-r{repetition}", case.label()),
            )
            .await?;
        if manifest.transport.kind == TransportKind::Exec {
            driver.release_invocations().await?;
        }
        let mut sampler = platform_sampler();
        let roots = verified_process_roots(
            manifest,
            sampler.as_mut(),
            driver.owned_pids(),
            driver.daemon_pid(),
        )?;
        wait_for_signal_fixture_hold(
            &mut driver,
            &engine,
            sampler.as_mut(),
            &roots,
            &session,
            &actor,
            outer_deadline,
        )
        .await?;
        let owned_tree = sampler.discover(&roots)?;
        let target = signal_target_identity(&owned_tree, &roots)?;
        #[cfg(unix)]
        let (origin_ns, deadline, early_terminal_receipt) = if let Some(signal) = case.unix_signal() {
            crate::process::deliver_registered_tree_signal(target, signal)?;
            let origin = monotonic_timestamp_ns();
            let deadline = Instant::now() + outer_deadline;
            let early_terminal_receipt = if case == SignalMatrixCase::Sigint2 {
                let second_delivery_at = Instant::now() + Duration::from_millis(250);
                loop {
                    let events =
                        read_signal_matrix_journal(manifest, &variables, &profile_root, &session)?;
                    let receipt_ns = monotonic_timestamp_ns();
                    if events.iter().any(|event| is_terminal(&event.event)) {
                        break Some((events, receipt_ns));
                    }
                    let now = Instant::now();
                    if now >= second_delivery_at {
                        if crate::process::registered_tree_is_live(target)? {
                            crate::process::deliver_registered_tree_signal(target, signal)?;
                        }
                        break None;
                    }
                    let remaining = deadline.checked_duration_since(now).ok_or_else(|| {
                        AhrbError::Timeout(
                            "row-57 durable terminal was not received before the applicable outer deadline"
                                .to_owned(),
                        )
                    })?;
                    tokio::time::sleep(
                        Duration::from_millis(2)
                            .min(second_delivery_at.saturating_duration_since(now))
                            .min(remaining),
                    )
                    .await;
                }
            } else {
                None
            };
            (origin, deadline, early_terminal_receipt)
        } else {
            driver.close_stdin().await?;
            (
                monotonic_timestamp_ns(),
                Instant::now() + outer_deadline,
                None,
            )
        };
        #[cfg(not(unix))]
        let (origin_ns, deadline, early_terminal_receipt) = if case == SignalMatrixCase::StdinEof {
            driver.close_stdin().await?;
            (
                monotonic_timestamp_ns(),
                Instant::now() + outer_deadline,
                None,
            )
        } else {
            return Err(AhrbError::Unsupported(
                "row-57 Unix signal delivery is unavailable".to_owned(),
            ));
        };
        let (first_terminal_events, terminal_ns) = match early_terminal_receipt {
            Some(receipt) => receipt,
            None => {
                wait_for_signal_terminal_receipt(
                    manifest,
                    &variables,
                    &profile_root,
                    &session,
                    deadline,
                )
                .await?
            }
        };
        let exit = wait_for_signal_process_exit(
            &mut driver,
            &session,
            manifest.transport.kind == TransportKind::Exec,
            deadline,
        )
        .await?;
        let mut events = read_signal_matrix_journal(manifest, &variables, &profile_root, &session)?;
        if first_terminal_events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count()
            != 1
        {
            return Err(AhrbError::Protocol(format!(
                "row-57 {} first durable terminal receipt was not singular",
                case.label()
            )));
        }
        let terminals = events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .collect::<Vec<_>>();
        if terminals.is_empty() {
            return Err(AhrbError::Protocol(format!(
                "row-57 {} durable journal omitted the structured terminal boundary",
                case.label()
            )));
        }
        let terminal_type = terminals
            .last()
            .map(|event| match event.event {
                EventVocab::TerminalFailure => "failure",
                EventVocab::TerminalCancelled => "cancelled",
                EventVocab::TerminalSuccess => "success",
                EventVocab::TerminalTimeout => "timeout",
                _ => "other",
            })
            .map(str::to_owned);
        let terminal_count = u32::try_from(terminals.len()).map_or(u32::MAX, |value| value);
        drop(terminals);
        let residue_processes = signal_matrix_residue_count(sampler.as_mut(), &roots).await?;
        annotate_signal_matrix_events(&mut events, case, origin_ns, terminal_ns);
        let requests = engine.request_records().await;
        let (exit_code, exit_was_signal) = match exit {
            ClientExit::Exited(code) => (code, code.is_none()),
            ClientExit::Running | ClientExit::NotApplicable => {
                return Err(AhrbError::Protocol(
                    "row-57 process exit boundary disappeared".to_owned(),
                ));
            }
        };
        Ok((
            SignalCaseTrial {
                repetition,
                case: case.label().to_owned(),
                applicable: true,
                not_applicable_reason: None,
                delivery_succeeded: Some(true),
                ownership_resolved: Some(true),
                origin_ns: Some(origin_ns),
                terminal_ns: Some(terminal_ns),
                terminal_type,
                terminal_count: Some(terminal_count),
                exit_code,
                exit_was_signal: Some(exit_was_signal),
                residue_processes: Some(residue_processes),
            },
            events,
            requests,
        ))
    }
    .await;
    if collected.is_err() {
        let _ = driver.shutdown().await;
    }
    drop(driver);
    let shutdown = server.shutdown().await;
    match (collected, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

async fn collect_signal_matrix_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<SignalMatrixTrials> {
    let applicable_outer_ms = u64::try_from(outer_turn_timeout(manifest).as_millis())
        .map_err(|_| AhrbError::Validation("row-57 outer deadline overflow".to_owned()))?;
    if manifest.daemon.grace_ms.saturating_add(250) >= applicable_outer_ms {
        return Err(AhrbError::Validation(
            "row-57 daemon grace plus 250ms tolerance must be below its outer deadline".to_owned(),
        ));
    }
    let prompt_uses_stdin = manifest.input.prompt_uses_stdin.ok_or_else(|| {
        AhrbError::Validation("row-57 typed input.prompt_uses_stdin is absent".to_owned())
    })?;
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let mut collected = SignalMatrixTrials {
        events: Vec::new(),
        evidence: Vec::new(),
        requests: Vec::new(),
    };
    for repetition in 1..=repetitions {
        for case in [
            SignalMatrixCase::Sigterm,
            SignalMatrixCase::Sigint2,
            SignalMatrixCase::Sighup,
            SignalMatrixCase::StdinEof,
        ] {
            let eof_applicable =
                manifest.transport.kind == TransportKind::StdinRpc || prompt_uses_stdin;
            if case == SignalMatrixCase::StdinEof && !eof_applicable {
                collected.evidence.push(SignalCaseTrial {
                    repetition,
                    case: case.label().to_owned(),
                    applicable: false,
                    not_applicable_reason: Some(
                        "typed prompt input is false and the non-stdin transport launches with /dev/null stdin"
                            .to_owned(),
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
                });
                continue;
            }
            let (trial, events, requests) = collect_signal_matrix_case(
                manifest,
                run_profile_root,
                manifest_hash,
                repetition,
                case,
            )
            .await?;
            collected.evidence.push(trial);
            collected.events.extend(events);
            collected.requests.extend(requests);
        }
    }
    Ok(collected)
}

async fn collect_retry_budget_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<RetryBudgetTrials> {
    let _max_attempts = manifest.resources.retry_max_attempts.ok_or_else(|| {
        AhrbError::Validation("retry-budget requires retry_max_attempts".to_owned())
    })?;
    let base_delay_ms = manifest.resources.retry_base_delay_ms.ok_or_else(|| {
        AhrbError::Validation("retry-budget requires retry_base_delay_ms".to_owned())
    })?;
    let max_delay_ms = manifest.resources.retry_max_delay_ms.ok_or_else(|| {
        AhrbError::Validation("retry-budget requires retry_max_delay_ms".to_owned())
    })?;
    let profile_root = run_profile_root.join("derived-row58");
    prepare_profile(manifest, &profile_root)?;
    let workflow = retry_budget_workflow(&profile_root, profile);
    workflow.validate()?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!("ahrb-retry-{}", &manifest_hash[..16]);
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential);
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_turn_timeout(manifest),
    )?;
    driver.start().await?;
    driver.await_readiness().await?;

    let statuses = match profile {
        Profile::Quick => vec![429_u16],
        Profile::Cert => vec![429_u16, 500_u16],
    };
    let calibration_requested_ms = base_delay_ms.min(100);
    let post_terminal_requested_ms = max_delay_ms.saturating_add(250).max(1_000);
    let mut calibrations = Vec::new();
    let mut trials = Vec::new();
    let mut all_events = Vec::new();
    for status in statuses {
        calibrations.extend(calibrate_retry_timer(status, calibration_requested_ms).await);
        for repetition in 1..=3_u32 {
            let actor_name = format!("r58-s{status}-r{repetition}");
            let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
                AhrbError::Protocol(format!("retry-budget actor {actor_name:?} disappeared"))
            })?;
            let session = driver
                .create_session(&format!("{}:{actor_name}", workflow.scenario))
                .await?;
            driver
                .submit(
                    &session,
                    &actor.prompt,
                    &format!("row-58-s{status}-r{repetition}"),
                )
                .await?;
            let mut events =
                collect_session_terminal(&mut driver, &session, None, outer_turn_timeout(manifest))
                    .await?;
            let terminal_received_ns = monotonic_timestamp_ns();
            let requests_at_terminal = engine
                .request_records()
                .await
                .iter()
                .filter(|record| record.request.actor == actor_name)
                .count() as u64;
            let after = events.iter().map(|event| Cursor(event.cursor)).max();
            let observation_started_ns = monotonic_timestamp_ns();
            tokio::time::sleep(Duration::from_millis(post_terminal_requested_ms)).await;
            let observation_finished_ns = monotonic_timestamp_ns();
            let later = driver.attach(&session, after).await?;
            let effects_after_terminal = later
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count() as u64;
            events.extend(later);
            let requests_after_observation = engine
                .request_records()
                .await
                .iter()
                .filter(|record| record.request.actor == actor_name)
                .count() as u64;
            let post_terminal_actual_ms =
                observation_finished_ns.saturating_sub(observation_started_ns) as f64 / 1_000_000.0;
            all_events.extend(events.clone());
            trials.push(RetryTrialEvidence {
                status,
                repetition,
                actor: actor_name,
                terminal_received_ns,
                events,
                post_terminal_requested_ms,
                post_terminal_actual_ms,
                requests_at_terminal,
                requests_after_observation,
                effects_after_terminal,
            });
            if manifest.transport.kind == TransportKind::Exec
                || !manifest.sessions.close_delete.is_empty()
            {
                driver.close(&session).await?;
            }
        }
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    let requests = engine.request_records().await;
    Ok(RetryBudgetTrials {
        events: all_events,
        requests,
        calibrations,
        trials,
    })
}

fn model_efficiency_workflow(profile_root: &Path, repetition: u32, turns: u32) -> Workflow {
    let scenario = format!("ahrb-row42-r{repetition}");
    let actor = format!("r42-r{repetition}");
    let actors = BTreeMap::from([(
        actor.clone(),
        Actor {
            id: actor.clone(),
            parent: None,
            prompt: format!(
                "AHRB model request efficiency direct terminal turn 1 {}",
                route_marker(&scenario, &actor, "turn-001")
            ),
            workspace: profile_root
                .join("workspace")
                .to_string_lossy()
                .into_owned(),
        },
    )]);
    let mut responses = Vec::new();
    for turn in 1..=turns {
        responses.push(ScriptedResponse {
            scenario: scenario.clone(),
            actor: actor.clone(),
            checkpoint: format!("turn-{turn:03}"),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        });
    }
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors,
        barriers: BTreeMap::new(),
        responses,
    }
}

fn sample_process_hygiene(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    phase: &str,
    elapsed_ns: u64,
) -> Result<(ProcessHygieneCadenceSample, Vec<String>)> {
    const COUNTER_RACE_ATTEMPTS: u32 = 3;
    let wall_started = Instant::now();
    let cpu_started = sampler_thread_cpu_ns()?;
    let mut accounting_warnings = Vec::new();
    let sample = {
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let tree = sampler.discover(roots)?;
            let mut candidate = sampler.sample(&tree, phase)?;
            accounting_warnings.append(&mut candidate.cpu_accounting_warnings);
            let counters_complete = candidate
                .process_samples
                .iter()
                .all(|process| process.thread_count.is_some() && process.open_fds.is_some());
            if counters_complete {
                break candidate;
            }
            if attempt >= COUNTER_RACE_ATTEMPTS {
                return Err(AhrbError::Protocol(format!(
                    "row-44 {phase} could not collect complete thread/FD counters after {COUNTER_RACE_ATTEMPTS} identity-safe attempts"
                )));
            }
            // A short-lived process can disappear after discovery but before its
            // platform counters are read. Re-discovery distinguishes that race
            // from persistent counter unavailability without inventing zeros or
            // silently dropping a still-live owned identity.
            std::thread::yield_now();
        }
    };
    let collection_cpu_ns = sampler_thread_cpu_ns()?.saturating_sub(cpu_started);
    let collection_wall_ns = duration_ns(wall_started.elapsed());
    let mut warnings = Vec::new();
    for warning in accounting_warnings {
        warnings.push(serde_json::to_string(&warning)?);
    }
    let mut processes = sample
        .process_samples
        .into_iter()
        .map(|process| ProcessHygieneProcess {
            identity: process.process.identity,
            command: process.process.command,
            ownership: process.process.ownership,
            thread_count: process.thread_count,
            open_fds: process.open_fds,
        })
        .collect::<Vec<_>>();
    processes.sort_by_key(|process| process.identity);
    Ok((
        ProcessHygieneCadenceSample {
            elapsed_ns,
            collection_cpu_ns,
            collection_wall_ns,
            processes,
        },
        warnings,
    ))
}

fn collect_process_hygiene_snapshot(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    phase: &str,
    evidence: &mut ProcessHygieneEvidence,
) -> Result<Vec<ProcessHygieneProcess>> {
    let (sample, warnings) = sample_process_hygiene(sampler, roots, phase, 0)?;
    evidence.sampler_collection_cpu_ns = evidence
        .sampler_collection_cpu_ns
        .saturating_add(sample.collection_cpu_ns);
    evidence.sampler_collection_wall_ns = evidence
        .sampler_collection_wall_ns
        .saturating_add(sample.collection_wall_ns);
    evidence.sampler_warnings.extend(warnings);
    Ok(sample.processes)
}

struct ProcessHygieneTurnCollection {
    sampler: Box<dyn Sampler>,
    samples: Vec<ProcessHygieneCadenceSample>,
    warnings: Vec<String>,
    sampled_wall_ns: u64,
}

struct ProcessHygieneTurnSampler {
    stop: Arc<(Mutex<bool>, Condvar)>,
    started: Instant,
    join: std::thread::JoinHandle<Result<ProcessHygieneTurnCollection>>,
}

impl ProcessHygieneTurnSampler {
    fn finish(self) -> Result<ProcessHygieneTurnCollection> {
        let sampled_wall_ns = duration_ns(self.started.elapsed()).max(1);
        {
            let (lock, wake) = &*self.stop;
            let mut stopping = lock.lock().map_err(|_| {
                AhrbError::Protocol("row-44 sampler stop lock was poisoned".to_owned())
            })?;
            *stopping = true;
            wake.notify_one();
        }
        let mut collection = self
            .join
            .join()
            .map_err(|_| AhrbError::Protocol("row-44 sampler thread panicked".to_owned()))??;
        collection.sampled_wall_ns = sampled_wall_ns;
        Ok(collection)
    }
}

fn start_process_hygiene_turn_sampler(
    mut sampler: Box<dyn Sampler>,
    roots: Vec<u32>,
    phase: String,
    cadence: Duration,
) -> Result<ProcessHygieneTurnSampler> {
    let sample_interval = cadence
        .checked_div(2)
        .filter(|interval| !interval.is_zero())
        .unwrap_or(cadence);
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_stop = Arc::clone(&stop);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let join = std::thread::Builder::new()
        .name("ahrb-row44-sampler".to_owned())
        .spawn(move || {
            prioritize_counter_thread();
            let mut samples = Vec::new();
            let mut warnings = Vec::new();
            // Establish the active-window origin only after the sampler thread is
            // running and its pre-operation boundary snapshot is complete. Thread
            // startup is not part of the measured turn, and can exceed one cadence
            // on a loaded host even though sampling during the turn is healthy.
            let first = sample_process_hygiene(sampler.as_mut(), &roots, &phase, 0);
            let (sample, first_warnings) = match first {
                Ok(first) => first,
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return Err(error);
                }
            };
            samples.push(sample);
            warnings.extend(first_warnings);
            let active_started = Instant::now();
            if ready_tx.send(Ok(active_started)).is_err() {
                return Err(AhrbError::Protocol(
                    "row-44 sampler readiness receiver disappeared".to_owned(),
                ));
            }
            let mut deadline = active_started + sample_interval;
            loop {
                let (lock, wake) = &*thread_stop;
                let stopping = lock.lock().map_err(|_| {
                    AhrbError::Protocol("row-44 sampler stop lock was poisoned".to_owned())
                })?;
                let now = Instant::now();
                let stopping = if *stopping || now >= deadline {
                    *stopping
                } else {
                    let (guard, _) = wake
                        .wait_timeout(stopping, deadline.duration_since(now))
                        .map_err(|_| {
                            AhrbError::Protocol("row-44 sampler stop lock was poisoned".to_owned())
                        })?;
                    *guard
                };
                let previous_elapsed = samples.last().map_or(0, |sample| sample.elapsed_ns);
                let mut elapsed_ns = duration_ns(active_started.elapsed());
                while elapsed_ns <= previous_elapsed {
                    std::thread::yield_now();
                    elapsed_ns = duration_ns(active_started.elapsed());
                }
                let (sample, sample_warnings) =
                    sample_process_hygiene(sampler.as_mut(), &roots, &phase, elapsed_ns)?;
                samples.push(sample);
                warnings.extend(sample_warnings);
                if stopping {
                    break;
                }
                let due = Instant::now();
                while deadline <= due {
                    deadline += sample_interval;
                }
            }
            Ok(ProcessHygieneTurnCollection {
                sampler,
                samples,
                warnings,
                sampled_wall_ns: 0,
            })
        })?;
    match ready_rx.recv() {
        Ok(Ok(started)) => Ok(ProcessHygieneTurnSampler {
            stop,
            started,
            join,
        }),
        Ok(Err(detail)) => {
            let _ = join.join();
            Err(AhrbError::Protocol(format!(
                "row-44 initial cadence sample failed: {detail}"
            )))
        }
        Err(_) => {
            let _ = join.join();
            Err(AhrbError::Protocol(
                "row-44 sampler exited before publishing its initial sample".to_owned(),
            ))
        }
    }
}

fn record_process_hygiene_turn(
    evidence: &mut ProcessHygieneEvidence,
    repetition: u32,
    turn_index: u32,
    required_cadence_ns: u64,
    collection: &ProcessHygieneTurnCollection,
) {
    let mut processes = BTreeMap::new();
    let mut active_cpu_ns = 0_u64;
    let mut active_wall_ns = 0_u64;
    for sample in &collection.samples {
        active_cpu_ns = active_cpu_ns.saturating_add(sample.collection_cpu_ns);
        active_wall_ns = active_wall_ns.saturating_add(sample.collection_wall_ns);
        for process in &sample.processes {
            processes.insert(process.identity, process.clone());
        }
    }
    evidence.active_sampler_collection_cpu_ns = evidence
        .active_sampler_collection_cpu_ns
        .saturating_add(active_cpu_ns);
    evidence.sampled_turn_wall_ns = evidence
        .sampled_turn_wall_ns
        .saturating_add(collection.sampled_wall_ns);
    evidence.sampler_collection_cpu_ns = evidence
        .sampler_collection_cpu_ns
        .saturating_add(active_cpu_ns);
    evidence.sampler_collection_wall_ns = evidence
        .sampler_collection_wall_ns
        .saturating_add(active_wall_ns);
    evidence
        .sampler_warnings
        .extend(collection.warnings.iter().cloned());
    evidence.checkpoints.push(ProcessHygieneCheckpoint {
        repetition,
        turn_index,
        processes: processes.into_values().collect(),
        cadence_samples: collection.samples.clone(),
        sampled_wall_ns: collection.sampled_wall_ns,
        required_cadence_ns,
    });
}

async fn collect_process_hygiene_audit(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    phase: &str,
    evidence: &mut ProcessHygieneEvidence,
) -> Result<(u64, Vec<ProcessHygieneProcess>)> {
    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let processes = collect_process_hygiene_snapshot(sampler, roots, phase, evidence)?;
    let waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((waited_ms, processes))
}

async fn collect_model_request_efficiency_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
    collect_process_hygiene: bool,
) -> Result<ModelRequestEfficiencyTrials> {
    let turns = match profile {
        Profile::Quick => 20_u32,
        Profile::Cert => 100_u32,
    };
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    let mut process_hygiene = collect_process_hygiene.then(ProcessHygieneEvidence::default);
    for repetition in 1..=2_u32 {
        let per_invocation = per_invocation_topology(manifest);
        let profile_root = run_profile_root.join(format!("derived-row42-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-42 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let mut workflow = model_efficiency_workflow(&profile_root, repetition, turns);
        let hygiene_warmup_actor = format!("r44-warmup-r{repetition}");
        if collect_process_hygiene && !per_invocation {
            workflow.actors.insert(
                hygiene_warmup_actor.clone(),
                Actor {
                    id: hygiene_warmup_actor.clone(),
                    parent: None,
                    prompt: format!(
                        "AHRB process hygiene daemon warm-up {}",
                        route_marker(&workflow.scenario, &hygiene_warmup_actor, "warmup")
                    ),
                    workspace: profile_root
                        .join("workspace")
                        .to_string_lossy()
                        .into_owned(),
                },
            );
            workflow.responses.push(ScriptedResponse {
                scenario: workflow.scenario.clone(),
                actor: hygiene_warmup_actor.clone(),
                checkpoint: "warmup".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            });
        }
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row42-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .map_or_else(String::new, Clone::clone),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            collect_process_hygiene && per_invocation,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        let mut hygiene_sampler = collect_process_hygiene.then(platform_sampler);
        let daemon_roots = if collect_process_hygiene && !per_invocation {
            let roots = driver.owned_pids();
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-44 repetition {repetition} daemon exposed no owned root PID"
                )));
            }
            roots
        } else {
            Vec::new()
        };
        let session = driver
            .create_session(&format!("{}:row42", workflow.scenario))
            .await?;
        let mut after = None;
        if let Some(evidence) = process_hygiene.as_mut()
            && per_invocation
        {
            evidence.growth_checkpoints.push(ProcessHygieneCheckpoint {
                repetition,
                turn_index: 0,
                processes: Vec::new(),
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            });
        }
        if collect_process_hygiene && !per_invocation {
            let warmup = workflow.actors.get(&hygiene_warmup_actor).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-44 repetition {repetition} daemon warm-up actor disappeared"
                ))
            })?;
            driver
                .submit(
                    &session,
                    &warmup.prompt,
                    &format!("row-44-r{repetition}-warmup"),
                )
                .await?;
            let suffix = collect_session_terminal(
                &mut driver,
                &session,
                after,
                outer_turn_timeout(manifest),
            )
            .await?;
            after = suffix
                .iter()
                .map(|event| Cursor(event.cursor))
                .max()
                .or(after);
            let (Some(sampler), Some(evidence)) =
                (hygiene_sampler.as_deref_mut(), process_hygiene.as_mut())
            else {
                return Err(AhrbError::Protocol(
                    "row-44 daemon warm-up lost its sampler".to_owned(),
                ));
            };
            let processes = collect_process_hygiene_snapshot(
                sampler,
                &daemon_roots,
                &format!("row44-r{repetition}-warm-baseline"),
                evidence,
            )?;
            let baseline = ProcessHygieneCheckpoint {
                repetition,
                turn_index: 0,
                processes,
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            };
            evidence.warm_baselines.push(baseline.clone());
            evidence.growth_checkpoints.push(baseline);
        }
        let hygiene_cadence = Duration::from_millis(
            ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).membership_cadence_ms,
        );
        let hygiene_cadence_ns = duration_ns(hygiene_cadence);
        for turn in 1..=turns {
            let actor_name = format!("r42-r{repetition}");
            let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-42 repetition {repetition} actor {actor_name:?} disappeared"
                ))
            })?;
            let prompt = if turn == 1 {
                actor.prompt.clone()
            } else {
                format!(
                    "AHRB model request efficiency direct terminal turn {turn} {}",
                    route_marker(&workflow.scenario, &actor_name, &format!("turn-{turn:03}"))
                )
            };
            let mut turn_sampler = if collect_process_hygiene && !per_invocation {
                let sampler = hygiene_sampler.take().ok_or_else(|| {
                    AhrbError::Protocol("row-44 daemon cadence sampler disappeared".to_owned())
                })?;
                Some(start_process_hygiene_turn_sampler(
                    sampler,
                    daemon_roots.clone(),
                    format!("row44-r{repetition}-turn-{turn:03}"),
                    hygiene_cadence,
                )?)
            } else {
                None
            };
            if let Err(error) = driver
                .submit(
                    &session,
                    &prompt,
                    &format!("row-42-r{repetition}-t{turn:03}"),
                )
                .await
            {
                if let Some(sampler) = turn_sampler.take() {
                    let _ = sampler.finish();
                }
                return Err(error);
            }
            let turn_roots = if per_invocation {
                driver.session_pids(&session)
            } else {
                daemon_roots.clone()
            };
            if collect_process_hygiene && turn_roots.is_empty() {
                if let Some(sampler) = turn_sampler.take() {
                    let _ = sampler.finish();
                }
                return Err(AhrbError::Protocol(format!(
                    "row-44 repetition {repetition} turn {turn} exposed no owned root PID"
                )));
            }
            if collect_process_hygiene && per_invocation {
                let sampler = hygiene_sampler.take().ok_or_else(|| {
                    AhrbError::Protocol(
                        "row-44 per-invocation cadence sampler disappeared".to_owned(),
                    )
                })?;
                turn_sampler = Some(start_process_hygiene_turn_sampler(
                    sampler,
                    turn_roots.clone(),
                    format!("row44-r{repetition}-turn-{turn:03}"),
                    hygiene_cadence,
                )?);
                if let Err(error) = driver.release_invocations().await {
                    if let Some(sampler) = turn_sampler.take() {
                        let _ = sampler.finish();
                    }
                    return Err(error);
                }
            }
            let terminal_result = collect_session_terminal(
                &mut driver,
                &session,
                after,
                outer_turn_timeout(manifest),
            )
            .await;
            let turn_collection = turn_sampler
                .take()
                .map(ProcessHygieneTurnSampler::finish)
                .transpose()?;
            let suffix = terminal_result?;
            after = suffix
                .iter()
                .map(|event| Cursor(event.cursor))
                .max()
                .or(after);
            all_events.extend(suffix);
            if let (Some(collection), Some(evidence)) = (turn_collection, process_hygiene.as_mut())
            {
                record_process_hygiene_turn(
                    evidence,
                    repetition,
                    turn,
                    hygiene_cadence_ns,
                    &collection,
                );
                hygiene_sampler = Some(collection.sampler);
            }
            if let (Some(sampler), Some(evidence)) =
                (hygiene_sampler.as_deref_mut(), process_hygiene.as_mut())
            {
                if per_invocation {
                    let (waited_ms, processes) = collect_process_hygiene_audit(
                        sampler,
                        &turn_roots,
                        &format!("row44-r{repetition}-turn-{turn:03}-post-exit"),
                        evidence,
                    )
                    .await?;
                    evidence.per_turn_audits.push(ProcessHygieneAudit {
                        repetition,
                        turn_index: Some(turn),
                        waited_ms,
                        processes: processes.clone(),
                    });
                    evidence.growth_checkpoints.push(ProcessHygieneCheckpoint {
                        repetition,
                        turn_index: turn,
                        processes,
                        cadence_samples: Vec::new(),
                        sampled_wall_ns: 0,
                        required_cadence_ns: 0,
                    });
                } else {
                    let processes = collect_process_hygiene_snapshot(
                        sampler,
                        &daemon_roots,
                        &format!("row44-r{repetition}-turn-{turn:03}-post-terminal"),
                        evidence,
                    )?;
                    evidence.growth_checkpoints.push(ProcessHygieneCheckpoint {
                        repetition,
                        turn_index: turn,
                        processes,
                        cadence_samples: Vec::new(),
                        sampled_wall_ns: 0,
                        required_cadence_ns: 0,
                    });
                }
            }
        }
        if collect_process_hygiene
            || manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        if let (Some(sampler), Some(evidence)) =
            (hygiene_sampler.as_deref_mut(), process_hygiene.as_mut())
            && !per_invocation
        {
            let (waited_ms, processes) = collect_process_hygiene_audit(
                sampler,
                &daemon_roots,
                &format!("row44-r{repetition}-post-close"),
                evidence,
            )
            .await?;
            evidence.post_close_audits.push(ProcessHygieneAudit {
                repetition,
                turn_index: None,
                waited_ms,
                processes,
            });
        }
        driver.shutdown().await?;
        if let (Some(sampler), Some(evidence)) =
            (hygiene_sampler.as_deref_mut(), process_hygiene.as_mut())
            && !per_invocation
        {
            let (waited_ms, processes) = collect_process_hygiene_audit(
                sampler,
                &daemon_roots,
                &format!("row44-r{repetition}-shutdown"),
                evidence,
            )
            .await?;
            evidence.shutdown_audits.push(ProcessHygieneAudit {
                repetition,
                turn_index: None,
                waited_ms,
                processes,
            });
        }
        server.shutdown().await?;
        all_requests.extend(engine.request_records().await);
    }
    all_requests.sort_by(|left, right| {
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
    Ok(ModelRequestEfficiencyTrials {
        events: all_events,
        requests: all_requests,
        process_hygiene,
    })
}

fn fanout_workflow(
    manifest: &Manifest,
    profile_root: &Path,
    repetition: u32,
    width: u32,
) -> Result<Workflow> {
    let scenario = format!("ahrb-row54-r{repetition}-n{width}");
    let barrier_name = "fanout-held".to_owned();
    let mut actors = BTreeMap::new();
    let mut responses = Vec::new();
    let mut barrier_actors = Vec::new();
    for index in 1..=width {
        let actor = format!("r54-r{repetition}-n{width}-a{index:02}");
        let held = route_marker(&scenario, &actor, "held");
        actors.insert(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB fanout fixture AHRB-FANOUT-WIDTH={width} {}",
                    route_marker(&scenario, &actor, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .join(&actor)
                    .to_string_lossy()
                    .into_owned(),
            },
        );
        let call = mapped_tool_call(
            manifest,
            "write",
            format!("row54-r{repetition}-n{width}-a{index}"),
            json!({
                "path": "fanout-fixture.txt",
                "content": format!("fanout actor {index} {held}"),
            }),
        )?;
        responses.push(ScriptedResponse {
            scenario: scenario.clone(),
            actor: actor.clone(),
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: json!({"tool_calls":[call]}),
            fault: None,
            barrier: None,
        });
        responses.push(ScriptedResponse {
            scenario: scenario.clone(),
            actor: actor.clone(),
            checkpoint: "held".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: Some(Fault::Delay { delay_ms: 100 }),
            barrier: Some(barrier_name.clone()),
        });
        barrier_actors.push(actor);
    }
    Ok(Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors,
        barriers: BTreeMap::from([(
            barrier_name.clone(),
            Barrier {
                name: barrier_name,
                actors: barrier_actors,
                checkpoint: "held".to_owned(),
            },
        )]),
        responses,
    })
}

fn observe_wave3_owned_memory(
    manifest: &Manifest,
    driver: &dyn Driver,
    phase: &str,
) -> Result<u64> {
    let owned = driver.owned_pids();
    if owned.is_empty() && driver.daemon_pid().is_none() {
        return Ok(0);
    }
    let mut sampler = platform_sampler();
    let roots = verified_process_roots(manifest, sampler.as_mut(), owned, driver.daemon_pid())?;
    if roots.is_empty() {
        return Ok(0);
    }
    let tree = sampler.discover(&roots)?;
    let sample = sampler.sample(&tree, phase)?;
    Ok(effective_sample_bytes(&sample))
}

struct Wave3PeakObserver {
    stop: Arc<AtomicBool>,
    roots: Arc<Mutex<Vec<u32>>>,
    samples: Arc<Mutex<Vec<Sample>>>,
    cadence: Duration,
    observation_started: Instant,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
}

struct Wave3PeakCollection {
    peak_bytes: u64,
    samples: Vec<Sample>,
    cadence_ns: u64,
    collection_cpu_ns: u64,
    collection_wall_ns: u64,
    observation_wall_ns: u64,
    max_gap_ns: u64,
}

impl Drop for Wave3PeakObserver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn start_wave3_peak_observer(cadence: Duration) -> Result<Wave3PeakObserver> {
    if cadence.is_zero() {
        return Err(AhrbError::Protocol(
            "row-54 peak sampler cadence is zero".to_owned(),
        ));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let roots = Arc::new(Mutex::new(Vec::<u32>::new()));
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let observation_started = Instant::now();
    let thread_stop = Arc::clone(&stop);
    let thread_roots = Arc::clone(&roots);
    let thread_samples = Arc::clone(&samples);
    let thread = std::thread::Builder::new()
        .name("ahrb-row54-peak-sampler".to_owned())
        .spawn(move || {
            prioritize_counter_thread();
            let mut sampler = platform_sampler();
            let mut deadline = Instant::now();
            loop {
                let now = Instant::now();
                if now < deadline {
                    std::thread::sleep(deadline.duration_since(now));
                }
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                let current_roots = thread_roots
                    .lock()
                    .map_err(|_| {
                        AhrbError::Protocol("row-54 sampler roots lock poisoned".to_owned())
                    })?
                    .clone();
                if !current_roots.is_empty() {
                    let collection_wall_started = Instant::now();
                    let collection_cpu_started = sampler_thread_cpu_ns()?;
                    let tree = sampler.discover(&current_roots)?;
                    let mut sample = sampler.sample(&tree, "row54-workload")?;
                    sample.elapsed_ns = duration_ns(observation_started.elapsed());
                    sample.collection_ns =
                        sampler_thread_cpu_ns()?.saturating_sub(collection_cpu_started);
                    sample.collection_wall_ns = duration_ns(collection_wall_started.elapsed());
                    thread_samples
                        .lock()
                        .map_err(|_| {
                            AhrbError::Protocol("row-54 samples lock poisoned".to_owned())
                        })?
                        .push(sample);
                }
                let due = Instant::now();
                while deadline <= due {
                    deadline += cadence;
                }
            }
            Ok(())
        })?;
    Ok(Wave3PeakObserver {
        stop,
        roots,
        samples,
        cadence,
        observation_started,
        thread: Some(thread),
    })
}

fn update_wave3_peak_roots(
    observer: &Wave3PeakObserver,
    manifest: &Manifest,
    driver: &dyn Driver,
) -> Result<Vec<u32>> {
    let mut sampler = platform_sampler();
    let roots = verified_process_roots(
        manifest,
        sampler.as_mut(),
        driver.owned_pids(),
        driver.daemon_pid(),
    )?;
    *observer
        .roots
        .lock()
        .map_err(|_| AhrbError::Protocol("row-54 sampler roots lock poisoned".to_owned()))? =
        roots.clone();
    Ok(roots)
}

async fn wait_for_wave3_peak_sample(
    observer: &Wave3PeakObserver,
    expected_roots: &[u32],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let observed = observer
            .samples
            .lock()
            .map_err(|_| AhrbError::Protocol("row-54 samples lock poisoned".to_owned()))?
            .iter()
            .any(|sample| {
                expected_roots.iter().all(|root| {
                    sample
                        .processes
                        .iter()
                        .any(|process| process.identity.pid == *root)
                })
            });
        if observed {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "row-54 peak sampler did not observe all {} gated launch roots",
                expected_roots.len()
            )));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn finish_wave3_peak_observer(mut observer: Wave3PeakObserver) -> Result<Wave3PeakCollection> {
    observer.stop.store(true, Ordering::Release);
    if let Some(thread) = observer.thread.take() {
        thread.join().map_err(|_| {
            AhrbError::Protocol("row-54 peak observer thread panicked".to_owned())
        })??;
    }
    let mut samples = observer
        .samples
        .lock()
        .map_err(|_| AhrbError::Protocol("row-54 samples lock poisoned".to_owned()))?
        .clone();
    samples.sort_by_key(|sample| sample.elapsed_ns);
    if samples.len() < 2 {
        return Err(AhrbError::Protocol(
            "row-54 peak sampler collected fewer than two samples".to_owned(),
        ));
    }
    let cadence_ns = duration_ns(observer.cadence);
    let collection_cpu_ns = samples.iter().fold(0_u64, |total, sample| {
        total.saturating_add(sample.collection_ns)
    });
    let collection_wall_ns = samples.iter().fold(0_u64, |total, sample| {
        total.saturating_add(sample.collection_wall_ns)
    });
    let observation_wall_ns = duration_ns(observer.observation_started.elapsed());
    let max_gap_ns = samples
        .windows(2)
        .map(|pair| pair[1].elapsed_ns.saturating_sub(pair[0].elapsed_ns))
        .max()
        .unwrap_or(0);
    let sampler_overloaded = observation_wall_ns == 0
        || collection_cpu_ns.saturating_mul(10) > observation_wall_ns
        || max_gap_ns > cadence_ns.saturating_mul(2)
        || samples
            .iter()
            .any(|sample| sample.collection_wall_ns > cadence_ns);
    if sampler_overloaded {
        return Err(AhrbError::Protocol(format!(
            "sampler overload: row-54 cpu={collection_cpu_ns}ns observation={observation_wall_ns}ns max_gap={max_gap_ns}ns cadence={cadence_ns}ns"
        )));
    }
    let peak_bytes = samples
        .iter()
        .map(effective_sample_bytes)
        .max()
        .unwrap_or(0);
    Ok(Wave3PeakCollection {
        peak_bytes,
        samples,
        cadence_ns,
        collection_cpu_ns,
        collection_wall_ns,
        observation_wall_ns,
        max_gap_ns,
    })
}

fn raw_wave3_terminal(raw: &Value, mapping: &crate::manifest::EventMapping) -> bool {
    let event_type = (!mapping.type_pointer.is_empty())
        .then(|| raw.pointer(&mapping.type_pointer))
        .flatten()
        .and_then(Value::as_str)
        .or_else(|| raw.get("type").and_then(Value::as_str))
        .or_else(|| raw.get("event").and_then(Value::as_str));
    let Some(event_type) = event_type else {
        return false;
    };
    mapping.rules.iter().any(|rule| {
        rule.matches == event_type
            && rule_matches(raw, rule)
            && matches!(
                rule.event.as_str(),
                "terminal-success" | "terminal-failure" | "terminal-cancelled" | "terminal-timeout"
            )
    })
}

fn watch_wave3_terminal_journal(
    path: PathBuf,
    mapping: crate::manifest::EventMapping,
    release_ns: Arc<AtomicU64>,
    turn_timeout: Duration,
) -> Result<Option<u64>> {
    while release_ns.load(Ordering::Acquire) == 0 {
        std::thread::sleep(Duration::from_micros(100));
    }
    let deadline = Instant::now() + turn_timeout;
    loop {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let ends_with_newline = bytes.last() == Some(&b'\n');
        let lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
        let complete_count = if ends_with_newline {
            lines.len()
        } else {
            lines.len().saturating_sub(1)
        };
        for line in lines.into_iter().take(complete_count) {
            if line.is_empty() {
                continue;
            }
            let raw: Value = serde_json::from_slice(line).map_err(|error| {
                AhrbError::Protocol(format!(
                    "row-55 terminal watcher found corrupt complete record in {}: {error}",
                    path.display()
                ))
            })?;
            if raw_wave3_terminal(&raw, &mapping) {
                let terminal_ns = monotonic_timestamp_ns();
                if terminal_ns == 0 {
                    return Err(AhrbError::Protocol(
                        "row-55 could not read a positive monotonic terminal timestamp".to_owned(),
                    ));
                }
                return Ok(Some(terminal_ns));
            }
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

async fn collect_one_fanout_trial(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
    repetition: u32,
    width: u32,
) -> Result<FanoutTrials> {
    let profile_root = run_profile_root.join(format!("derived-row54-r{repetition}-n{width}"));
    prepare_profile(manifest, &profile_root)?;
    let workflow = fanout_workflow(manifest, &profile_root, repetition, width)?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row54-r{repetition}-n{width}-{}",
        &manifest_hash[..16],
        std::process::id()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential);
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let trial_deadline = Duration::from_millis(FanoutPlan::trial_deadline_ms(
        manifest.resources.turn_timeout_ms,
    ));
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        per_invocation_topology(manifest),
        trial_deadline,
    )?;
    driver.start().await?;
    let timing = ResourceTimingPlan::for_profile(ResourceProfile::from(profile));
    #[cfg(target_os = "macos")]
    let sampler_cadence = Duration::from_millis(timing.macos_rusage_cadence_ms);
    #[cfg(target_os = "linux")]
    let sampler_cadence = Duration::from_millis(timing.linux_smaps_cadence_ms);
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let sampler_cadence = Duration::from_millis(timing.macos_rusage_cadence_ms);
    let peak_observer = start_wave3_peak_observer(sampler_cadence)?;
    // A per-invocation harness has no owned process until the first submit,
    // but the observer itself must already be armed so that launch is inside
    // the measured interval. Persistent topologies must expose their root at
    // start/readiness as usual.
    if !per_invocation_topology(manifest) {
        let _ = update_wave3_peak_roots(&peak_observer, manifest, driver.as_ref())?;
    }
    let baseline_bytes = observe_wave3_owned_memory(manifest, driver.as_ref(), "row54-baseline")?;
    let mut sessions = Vec::new();
    let mut launch_roots = Vec::new();
    for (index, (actor_name, actor)) in workflow.actors.iter().enumerate() {
        let session = driver.create_session(actor_name).await?;
        driver
            .submit(
                &session,
                &actor.prompt,
                &format!("row-54-r{repetition}-n{width}-turn-{}", index + 1),
            )
            .await?;
        launch_roots = update_wave3_peak_roots(&peak_observer, manifest, driver.as_ref())?;
        sessions.push((actor_name.clone(), session));
    }
    if per_invocation_topology(manifest) {
        if launch_roots.len() != usize::try_from(width).unwrap_or(usize::MAX) {
            return Err(AhrbError::Protocol(format!(
                "row-54 N={width} exposed {} gated invocation roots",
                launch_roots.len()
            )));
        }
        wait_for_wave3_peak_sample(&peak_observer, &launch_roots, trial_deadline).await?;
        driver.release_invocations().await?;
    }
    tokio::time::timeout(
        trial_deadline,
        engine.barriers().wait_until_ready("fanout-held"),
    )
    .await
    .map_err(|_| AhrbError::Timeout(format!("row-54 N={width} did not reach its barrier")))??;
    let mut active_samples = Vec::new();
    for sample_index in 0..3_u32 {
        active_samples.push(observe_wave3_owned_memory(
            manifest,
            driver.as_ref(),
            &format!("row54-active-{sample_index}"),
        )?);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    active_samples.sort_unstable();
    let steady_bytes = active_samples[active_samples.len() / 2];
    let active_snapshot_peak_bytes = active_samples.iter().copied().max().unwrap_or(steady_bytes);
    let active_memory_delta_bytes = steady_bytes.saturating_sub(baseline_bytes);

    let release_signal = Arc::new(AtomicU64::new(0));
    let mut terminal_watchers = Vec::with_capacity(sessions.len());
    for (actor, session) in &sessions {
        let mut journal_variables = variables.clone();
        journal_variables.insert("session_id".to_owned(), session.0.clone());
        let journal_path = if manifest.events.path.is_empty() {
            driver.live_event_path(session).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-54/55 has no live event carrier for actor {actor}"
                ))
            })?
        } else {
            PathBuf::from(crate::manifest::render_template(
                &manifest.events.path,
                &journal_variables,
            )?)
        };
        let mapping = manifest.events.clone();
        let watcher_release = Arc::clone(&release_signal);
        let turn_timeout = Duration::from_millis(manifest.resources.turn_timeout_ms);
        let watcher = std::thread::spawn(move || {
            watch_wave3_terminal_journal(journal_path, mapping, watcher_release, turn_timeout)
        });
        terminal_watchers.push((actor.clone(), watcher));
    }
    let barrier_release_ns = monotonic_timestamp_ns();
    if barrier_release_ns == 0 {
        return Err(AhrbError::Protocol(
            "row-54/55 could not read a positive monotonic release timestamp".to_owned(),
        ));
    }
    release_signal.store(barrier_release_ns, Ordering::Release);
    engine.barriers().release("fanout-held").await?;

    let watcher_results = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(terminal_watchers.len());
        for (actor, watcher) in terminal_watchers {
            let receipt = watcher.join().map_err(|_| {
                AhrbError::Protocol(format!(
                    "row-55 terminal watcher for actor {actor} panicked"
                ))
            })??;
            results.push((actor, receipt));
        }
        Ok::<_, AhrbError>(results)
    })
    .await
    .map_err(|error| AhrbError::Protocol(format!("row-55 watcher join task failed: {error}")))??;
    let terminal_ns = watcher_results
        .into_iter()
        .filter_map(|(actor, terminal)| terminal.map(|terminal| (actor, terminal)))
        .collect::<BTreeMap<_, _>>();
    let mut events = Vec::new();
    for (_, session) in &sessions {
        events.extend(driver.attach(session, None).await?);
    }
    let peak_collection = finish_wave3_peak_observer(peak_observer)?;
    let peak_bytes = active_snapshot_peak_bytes.max(peak_collection.peak_bytes);
    let deadline_ns = manifest.resources.turn_timeout_ms.saturating_mul(1_000_000);
    let mut wall_latencies_ns = sessions
        .iter()
        .map(|(actor, _)| {
            terminal_ns
                .get(actor)
                .copied()
                .map_or(deadline_ns, |terminal| {
                    terminal.saturating_sub(barrier_release_ns)
                })
        })
        .collect::<Vec<_>>();
    wall_latencies_ns.sort_unstable();
    let p95_index = wall_latencies_ns
        .len()
        .saturating_mul(95)
        .saturating_add(99)
        / 100;
    let wall_p95_ms =
        wall_latencies_ns[p95_index.max(1).min(wall_latencies_ns.len()) - 1] as f64 / 1_000_000.0;
    let actors = sessions
        .iter()
        .filter(|_| width >= 2)
        .map(|(actor, _)| FairnessActorEvidence {
            repetition,
            n: width,
            actor: actor.clone(),
            barrier_release_ns: Some(barrier_release_ns),
            terminal_ns: terminal_ns.get(actor).copied(),
        })
        .collect::<Vec<_>>();
    let terminalized = terminal_ns.len() == sessions.len();
    for (_, session) in &sessions {
        if terminalized {
            driver.close(session).await?;
        } else {
            let _ = driver.cancel(session).await;
        }
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    Ok(FanoutTrials {
        events,
        trials: vec![FanoutTrialEvidence {
            repetition,
            n: width,
            steady_bytes,
            peak_bytes,
            active_memory_delta_bytes,
            wall_p95_ms,
            sampler_cadence_ns: peak_collection.cadence_ns,
            sampler_sample_count: u32::try_from(peak_collection.samples.len()).unwrap_or(u32::MAX),
            sampler_collection_cpu_ns: peak_collection.collection_cpu_ns,
            sampler_collection_wall_ns: peak_collection.collection_wall_ns,
            sampler_observation_wall_ns: peak_collection.observation_wall_ns,
            sampler_max_gap_ns: peak_collection.max_gap_ns,
            terminalized,
        }],
        actors,
        samples: peak_collection.samples,
    })
}

async fn collect_fanout_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<FanoutTrials> {
    let plan = FanoutPlan::for_profile(profile);
    let collect = async {
        let mut combined = FanoutTrials {
            events: Vec::new(),
            trials: Vec::new(),
            actors: Vec::new(),
            samples: Vec::new(),
        };
        for repetition in 1..=plan.repetitions {
            let mut widths = plan.widths.clone();
            let offset = (usize::try_from(repetition).unwrap_or(usize::MAX) + 0xA53) % widths.len();
            widths.rotate_left(offset);
            for width in widths {
                let trial_deadline = Duration::from_millis(FanoutPlan::trial_deadline_ms(
                    manifest.resources.turn_timeout_ms,
                ));
                let trial = tokio::time::timeout(
                    trial_deadline,
                    collect_one_fanout_trial(
                        manifest,
                        profile,
                        run_profile_root,
                        manifest_hash,
                        repetition,
                        width,
                    ),
                )
                .await
                .map_err(|_| {
                    AhrbError::Timeout(format!(
                        "row-54 repetition {repetition} N={width} exceeded its absolute trial deadline"
                    ))
                })??;
                combined.events.extend(trial.events);
                combined.trials.extend(trial.trials);
                combined.actors.extend(trial.actors);
                combined.samples.extend(trial.samples);
            }
        }
        Ok(combined)
    };
    tokio::time::timeout(
        Duration::from_millis(plan.row_deadline_ms(manifest.resources.turn_timeout_ms)),
        collect,
    )
    .await
    .map_err(|_| AhrbError::Timeout("row-54/55 scheduling deadline expired".to_owned()))?
}

fn journal_torn_tail_workflow(profile_root: &Path, group: u32) -> Workflow {
    let scenario = format!("ahrb-row53-g{group}");
    let actor = format!("r53-g{group}");
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB-ROW53-LARGE-JOURNAL {}",
                    route_marker(&scenario, &actor, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        }],
    }
}

fn copy_row53_tree(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(AhrbError::Protocol(format!(
            "row-53 refused symlink while copying {}",
            source.display()
        )));
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(destination)?;
        let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            copy_row53_tree(&entry.path(), &destination.join(entry.file_name()))?;
        }
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(source, destination)?;
    } else {
        return Err(AhrbError::Protocol(format!(
            "row-53 refused non-regular source {}",
            source.display()
        )));
    }
    Ok(())
}

fn row53_record_digest() -> String {
    const PREFIX: &[u8] = br#"{"type":"ahrb-large","payload":""#;
    const SUFFIX: &[u8] = b"\"}\n";
    const TOTAL: usize = 1_048_576;
    let payload = TOTAL.saturating_sub(PREFIX.len().saturating_add(SUFFIX.len()));
    let mut digest = Sha256::new();
    digest.update(PREFIX);
    let block = [b'A'; 8192];
    let mut remaining = payload;
    while remaining > 0 {
        let count = remaining.min(block.len());
        digest.update(&block[..count]);
        remaining = remaining.saturating_sub(count);
    }
    digest.update(SUFFIX);
    format!("{:x}", digest.finalize())
}

fn row53_tail_digest(path: &Path, pre_size: u64) -> Result<String> {
    use std::io::{Seek as _, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(pre_size))?;
    let mut digest = Sha256::new();
    let mut remaining = 1_048_576_usize;
    let mut buffer = [0_u8; 8192];
    while remaining > 0 {
        let count = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..count])?;
        digest.update(&buffer[..count]);
        remaining = remaining.saturating_sub(count);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn row53_committed_prefix(path: &Path, pre_size: u64) -> Result<Vec<NormalizedEvent>> {
    let bytes = std::fs::read(path)?;
    let prefix_len = usize::try_from(pre_size)
        .map_err(|_| AhrbError::Protocol("row-53 pre-size does not fit usize".to_owned()))?;
    let prefix = bytes.get(..prefix_len).ok_or_else(|| {
        AhrbError::Protocol("row-53 pre-size exceeds the observed journal".to_owned())
    })?;
    prefix
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).map_err(AhrbError::from))
        .collect()
}

fn signal_row53_kill(tree: &ProcessTree) -> Result<u64> {
    let identities = tree.members.keys().copied().collect::<Vec<_>>();
    if identities.is_empty() {
        return Err(AhrbError::Protocol(
            "row-53 source has no owned process to kill".to_owned(),
        ));
    }
    for identity in &identities {
        let pid = i32::try_from(identity.pid)
            .map_err(|_| AhrbError::Protocol("row-53 PID exceeds pid_t".to_owned()))?;
        // SAFETY: identities were freshly discovered from verified owned roots.
        if unsafe { libc::kill(pid, libc::SIGSTOP) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    let kill_ns = monotonic_timestamp_ns();
    for identity in &identities {
        let pid = i32::try_from(identity.pid)
            .map_err(|_| AhrbError::Protocol("row-53 PID exceeds pid_t".to_owned()))?;
        // SAFETY: the same stopped, owned identities are killed before reuse is possible.
        if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    Ok(kill_ns)
}

async fn collect_journal_torn_tail_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<Vec<JournalTornTailTrial>> {
    const RECORD_BYTES: u64 = 1_048_576;
    const CUTS: [u64; 5] = [0, 262_144, 524_288, 786_432, 1_048_575];
    let expected_trials = match profile {
        Profile::Quick => 5_u32,
        Profile::Cert => 25_u32,
    };
    let expected_digest = row53_record_digest();
    let mut trials = Vec::with_capacity(expected_trials as usize);
    for trial_number in 1..=expected_trials {
        let group = trial_number;
        let stage = std::cell::Cell::new("prepare source profile");
        let group_result: Result<()> = async {
            let source_root = run_profile_root.join(format!("derived-row53-source-{group}"));
            prepare_profile(manifest, &source_root)?;
            stage.set("start source fake provider");
            let workflow = journal_torn_tail_workflow(&source_root, group);
            let engine = Arc::new(FakeModelEngine::with_request_roles(
                &workflow,
                &manifest.model_roles,
                &manifest.request_role_rules,
            )?);
            let (server, model_environment) = start_model(
                engine,
                &workflow,
                &source_root,
                false,
                &manifest.fake_model.base_url_env,
            )
            .await?;
            stage.set("render source environment");
            let mut source_variables = BTreeMap::from([
                (
                    "profile".to_owned(),
                    source_root.to_string_lossy().into_owned(),
                ),
                ("endpoint".to_owned(), String::new()),
            ]);
            let credential = format!(
                "ahrb-{}-row53-g{group}-{}",
                &manifest_hash[..16],
                std::process::id()
            );
            let mut source_environment = isolated_environment(manifest, &source_variables)?;
            source_environment.extend(model_environment.clone());
            source_environment.insert(
                manifest.fake_model.credential_env.clone(),
                credential.clone(),
            );
            source_environment.insert(
                "AHRB_MOCK_MODEL".to_owned(),
                manifest.fake_model.model.clone(),
            );
            source_variables.insert(
                "base_url".to_owned(),
                source_environment
                    .get(&manifest.fake_model.base_url_env)
                    .cloned()
                    .unwrap_or_default(),
            );
            source_variables.insert("credential".to_owned(), credential.clone());
            source_variables.insert("model".to_owned(), manifest.fake_model.model.clone());
            write_generated_files(manifest, &source_variables, &source_root)?;
            stage.set("construct source driver");
            let source_command = if manifest.transport.kind == TransportKind::Exec {
                manifest.transport.command.clone()
            } else {
                render_argv(&manifest.transport.command, &source_variables)?
            };
            let mut source_driver = make_driver_with_timeout(
                manifest,
                &source_command,
                &source_environment,
                &source_variables,
                &source_root,
                false,
                Duration::from_secs(10),
            )?;
            stage.set("start source driver");
            source_driver.start().await?;
            let actor_name = format!("r53-g{group}");
            let actor = workflow
                .actors
                .get(&actor_name)
                .ok_or_else(|| AhrbError::Protocol("row-53 source actor disappeared".to_owned()))?;
            stage.set("create source session");
            let session = source_driver.create_session(&actor_name).await?;
            let mut journal_variables = source_variables.clone();
            journal_variables.insert("session_id".to_owned(), session.0.clone());
            let source_journal = PathBuf::from(crate::manifest::render_template(
                &manifest.events.path,
                &journal_variables,
            )?);
            stage.set("read source journal baseline");
            let initial_size = match std::fs::metadata(&source_journal) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error.into()),
            };
            stage.set("submit source torn-tail turn");
            source_driver
                .submit(&session, &actor.prompt, &format!("row-53-source-{group}"))
                .await?;
            stage.set("observe source journal growth");
            let watch_started = Instant::now();
            let mut growth_observed_ns = 0_u64;
            let observed_size = loop {
                let size = match std::fs::metadata(&source_journal) {
                    Ok(metadata) => metadata.len(),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(error) => return Err(error.into()),
                };
                if size > initial_size && growth_observed_ns == 0 {
                    growth_observed_ns = monotonic_timestamp_ns();
                }
                if size >= initial_size.saturating_add(RECORD_BYTES) {
                    break size;
                }
                if watch_started.elapsed() >= Duration::from_secs(10) {
                    return Err(AhrbError::Timeout(
                        "row-53 did not observe the complete one-MiB journal growth".to_owned(),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            };
            let pre_size = observed_size.checked_sub(RECORD_BYTES).ok_or_else(|| {
                AhrbError::Protocol(
                    "row-53 observed journal is shorter than its fixture".to_owned(),
                )
            })?;
            if observed_size != pre_size.saturating_add(RECORD_BYTES) {
                return Err(AhrbError::Protocol(
                    "row-53 journal grew beyond the exact fixture boundary".to_owned(),
                ));
            }
            let record_digest_matches =
                row53_tail_digest(&source_journal, pre_size)? == expected_digest;
            let committed = row53_committed_prefix(&source_journal, pre_size)?;
            let committed_hash = stable_json_stream_hash(&committed)?;
            stage.set("discover and kill source process tree");
            let mut sampler = platform_sampler();
            let roots = verified_process_roots(
                manifest,
                sampler.as_mut(),
                source_driver.owned_pids(),
                source_driver.daemon_pid(),
            )?;
            let tree = sampler.discover(&roots)?;
            let kill_ns = signal_row53_kill(&tree)?;
            source_driver.reap_after_external_kill().await?;
            if !await_owned_tree_empty(sampler.as_mut(), &roots, Duration::from_secs(10)).await? {
                return Err(AhrbError::Protocol(
                    "row-53 killed source tree did not disappear".to_owned(),
                ));
            }
            drop(source_driver);
            stage.set("validate source store placement");
            let source_session_dir = source_journal.parent().ok_or_else(|| {
                AhrbError::Protocol("row-53 source journal has no session directory".to_owned())
            })?;
            let source_store_roots = crate::manifest::render_session_store_paths(
                manifest,
                &source_variables,
                &source_root,
            )?;
            if !source_store_roots
                .iter()
                .any(|root| source_session_dir.starts_with(root))
            {
                return Err(AhrbError::Protocol(format!(
                    "row-53 source journal {} is outside declared session stores",
                    source_journal.display()
                )));
            }
            let journal_relative = source_journal.strip_prefix(&source_root).map_err(|_| {
                AhrbError::Protocol(format!(
                    "row-53 source journal {} is outside profile {}",
                    source_journal.display(),
                    source_root.display()
                ))
            })?;
            let cut_offset = CUTS[usize::try_from(trial_number.saturating_sub(1))
                .unwrap_or(usize::MAX)
                % CUTS.len()];
            stage.set("prepare recovery profile");
            let trial_root = run_profile_root.join(format!("derived-row53-trial-{trial_number}"));
            prepare_profile(manifest, &trial_root)?;
            let target_journal = trial_root.join(journal_relative);
            let target_session_dir = target_journal.parent().ok_or_else(|| {
                AhrbError::Protocol("row-53 target journal has no session directory".to_owned())
            })?;
            stage.set("copy and truncate recovery journal");
            copy_row53_tree(source_session_dir, target_session_dir)?;
            let truncated_size = pre_size.saturating_add(cut_offset);
            let target_file = std::fs::OpenOptions::new()
                .write(true)
                .open(&target_journal)?;
            target_file.set_len(truncated_size)?;
            target_file.sync_all()?;

            stage.set("render recovery environment");
            let mut variables = BTreeMap::from([
                (
                    "profile".to_owned(),
                    trial_root.to_string_lossy().into_owned(),
                ),
                ("endpoint".to_owned(), String::new()),
            ]);
            let mut environment = isolated_environment(manifest, &variables)?;
            environment.extend(model_environment.clone());
            environment.insert(
                manifest.fake_model.credential_env.clone(),
                credential.clone(),
            );
            environment.insert(
                "AHRB_MOCK_MODEL".to_owned(),
                manifest.fake_model.model.clone(),
            );
            variables.insert(
                "base_url".to_owned(),
                environment
                    .get(&manifest.fake_model.base_url_env)
                    .cloned()
                    .unwrap_or_default(),
            );
            variables.insert("credential".to_owned(), credential.clone());
            variables.insert("model".to_owned(), manifest.fake_model.model.clone());
            write_generated_files(manifest, &variables, &trial_root)?;
            let command = if manifest.transport.kind == TransportKind::Exec {
                manifest.transport.command.clone()
            } else {
                render_argv(&manifest.transport.command, &variables)?
            };
            let recovery_started = Instant::now();
            stage.set("construct and start recovery driver");
            let mut recovered_driver = make_driver_with_timeout(
                manifest,
                &command,
                &environment,
                &variables,
                &trial_root,
                false,
                Duration::from_secs(10),
            )?;
            recovered_driver.start().await?;
            stage.set("replay recovered committed prefix");
            let recovered = if manifest.transport.kind == TransportKind::Exec {
                // Per-invocation drivers keep client-side reconstruction
                // metadata separately from the adapter-declared harness
                // journal. Recreate that local handle through the public
                // session path before invoking the official replay command.
                let recovered_session = recovered_driver.create_session(&actor_name).await?;
                if recovered_session != session {
                    return Err(AhrbError::Protocol(format!(
                        "row-53 recovery changed session identity from {} to {}",
                        session.0, recovered_session.0
                    )));
                }
                recovered_driver
                    .replay_persisted(&recovered_session, None)
                    .await?
            } else {
                recovered_driver.attach(&session, None).await?
            };
            let last_committed_cursor = committed.iter().map(|event| Cursor(event.cursor)).max();
            let post_committed_suffix = if manifest.transport.kind == TransportKind::Exec {
                recovered_driver
                    .replay_persisted(&session, last_committed_cursor)
                    .await?
            } else {
                recovered_driver
                    .attach(&session, last_committed_cursor)
                    .await?
            };
            let recovery_ms = recovery_started.elapsed().as_secs_f64() * 1_000.0;
            let recovered_hash = stable_json_stream_hash(&recovered)?;
            let unique_events = recovered
                .iter()
                .map(|event| event.id.as_str())
                .collect::<BTreeSet<_>>()
                .len();
            let duplicate_events = recovered.len().saturating_sub(unique_events) as u32;
            let committed_prefix_suffix_exact =
                recovered.len() == committed.len() && recovered_hash == committed_hash;
            let lost_committed_events = committed
                .len()
                .saturating_sub(recovered.len().min(committed.len()))
                as u32;
            stage.set("close recovered session");
            recovered_driver.close(&session).await?;
            recovered_driver.shutdown().await?;
            trials.push(JournalTornTailTrial {
                trial: trial_number,
                pre_size,
                observed_size,
                cut_offset,
                truncated_size,
                growth_observed_ns,
                kill_ns,
                record_digest_matches,
                recovery_ms,
                clean_recovery: committed_prefix_suffix_exact,
                committed_prefix_suffix_exact,
                final_record_present_or_absent: recovered.len() == committed.len(),
                committed_stream_sha256: committed_hash.clone(),
                recovered_stream_sha256: recovered_hash,
                committed_event_count: u32::try_from(committed.len()).unwrap_or(u32::MAX),
                recovered_event_count: u32::try_from(recovered.len()).unwrap_or(u32::MAX),
                post_committed_suffix_events: u32::try_from(post_committed_suffix.len())
                    .unwrap_or(u32::MAX),
                corrupt_recoveries: 0,
                lost_committed_events,
                duplicate_events,
                duplicate_effects: 0,
            });
            stage.set("shutdown source fake provider");
            server.shutdown().await?;
            Ok(())
        }
        .await;
        group_result.map_err(|error| {
            AhrbError::Protocol(format!(
                "row-53 trial {trial_number} during {}: {error}",
                stage.get()
            ))
        })?;
    }
    Ok(trials)
}

fn stable_json_stream_hash(events: &[NormalizedEvent]) -> Result<String> {
    let mut digest = Sha256::new();
    for event in events {
        let mut payload = event.payload.clone();
        if let Some(object) = payload.as_object_mut() {
            // These are AHRB observer envelopes added while normalizing a
            // carrier. They are neither durable journal content nor part of
            // the semantic stream whose prefix row 53 compares.
            object.remove("_ahrb_receipt");
            object.remove("_ahrb_source_raw");
        }
        // Driver-local replay IDs/cursors are transport bookkeeping. Hash the
        // exact ordered normalized semantic stream so daemon and official
        // per-invocation replay can prove the same committed prefix without a
        // topology-specific identity exception.
        let bytes = serde_json::to_vec(&json!({
            "session_id": event.session_id,
            "actor": event.actor,
            "event": event.event,
            "payload": payload,
        }))?;
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn turn_latency_workflow(profile_root: &Path, repetition: u32, turns: u32) -> Workflow {
    let scenario = format!("ahrb-row43-r{repetition}");
    let actor = format!("r43-latency-r{repetition}");
    let actors = BTreeMap::from([(
        actor.clone(),
        Actor {
            id: actor.clone(),
            parent: None,
            prompt: format!(
                "AHRB turn latency unmeasured direct terminal warm-up {}",
                route_marker(&scenario, &actor, "warmup")
            ),
            workspace: profile_root
                .join("workspace")
                .to_string_lossy()
                .into_owned(),
        },
    )]);
    let mut responses = vec![ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: "warmup".to_owned(),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: None,
    }];
    responses.extend((1..=turns).map(|turn| ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: format!("turn-{turn:04}"),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: None,
    }));
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors,
        barriers: BTreeMap::new(),
        responses,
    }
}

async fn await_completed_turn_boundary(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<Cursor>,
    previous_count: usize,
    timeout: Duration,
) -> Result<crate::driver::CompletedTurnBoundary> {
    let started = Instant::now();
    loop {
        if let Some(boundary) = driver
            .completed_turn_boundaries()
            .get(previous_count)
            .copied()
        {
            return Ok(boundary);
        }
        if started.elapsed() >= timeout {
            return Err(AhrbError::Timeout(
                "per-invocation row-43 child did not reach its exit boundary".to_owned(),
            ));
        }
        let _events = driver.attach(session, after).await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Row50ProcessCounters {
    memory_bytes: u64,
    open_fds: u64,
    threads: u64,
    processes: u64,
}

#[derive(Clone, Debug, Default)]
struct Row50StoreSnapshot {
    entries: Vec<SessionStoreEntry>,
}

fn session_residue_workflow(profile_root: &Path, repetition: u32, sessions: u32) -> Workflow {
    let scenario = format!("ahrb-row50-session-residue-r{repetition}");
    let mut actors = BTreeMap::new();
    let mut responses = Vec::new();
    let actors_to_add = (1..=10_u32)
        .map(|ordinal| {
            (
                format!("r50-warmup-r{repetition}-s{ordinal:02}"),
                format!("warm-up session {ordinal}"),
            )
        })
        .chain((1..=sessions).map(|ordinal| {
            (
                format!("r50-r{repetition}-s{ordinal:03}"),
                format!("measured session {ordinal}"),
            )
        }));
    for (actor, label) in actors_to_add {
        actors.insert(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB row 50 session residue {label} {}",
                    route_marker(&scenario, &actor, "start"),
                ),
                workspace: profile_root
                    .join("workspaces")
                    .join(&actor)
                    .to_string_lossy()
                    .into_owned(),
            },
        );
        responses.push(ScriptedResponse {
            scenario: scenario.clone(),
            actor,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        });
    }
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors,
        barriers: BTreeMap::new(),
        responses,
    }
}

fn row50_process_counters(sample: &Sample) -> Result<Row50ProcessCounters> {
    Ok(Row50ProcessCounters {
        memory_bytes: effective_sample_bytes(sample),
        open_fds: sample.open_fds.ok_or_else(|| {
            AhrbError::Protocol("row-50 sampler omitted open-FD counters".to_owned())
        })?,
        threads: sample.thread_count.ok_or_else(|| {
            AhrbError::Protocol("row-50 sampler omitted thread counters".to_owned())
        })?,
        processes: u64::try_from(sample.processes.len()).map_err(|_| {
            AhrbError::Protocol("row-50 process count exceeds report range".to_owned())
        })?,
    })
}

fn row50_median(mut values: Vec<u64>) -> Result<u64> {
    if values.is_empty() {
        return Err(AhrbError::Protocol(
            "row-50 warm baseline has no process samples".to_owned(),
        ));
    }
    values.sort_unstable();
    let middle = values.len() / 2;
    Ok(if values.len() % 2 == 1 {
        values[middle]
    } else {
        values[middle - 1].saturating_add(values[middle]) / 2
    })
}

fn row50_baseline_counters(samples: &[Sample]) -> Result<Row50ProcessCounters> {
    let counters = samples
        .iter()
        .map(row50_process_counters)
        .collect::<Result<Vec<_>>>()?;
    Ok(Row50ProcessCounters {
        memory_bytes: row50_median(
            counters
                .iter()
                .map(|counter| counter.memory_bytes)
                .collect(),
        )?,
        open_fds: row50_median(counters.iter().map(|counter| counter.open_fds).collect())?,
        threads: row50_median(counters.iter().map(|counter| counter.threads).collect())?,
        processes: row50_median(counters.iter().map(|counter| counter.processes).collect())?,
    })
}

fn row50_sample_owned_tree(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    phase: &str,
) -> Result<Sample> {
    if roots.is_empty() {
        return Err(AhrbError::Protocol(
            "row-50 daemon exposes no owned process root".to_owned(),
        ));
    }
    let tree = sampler.discover(roots)?;
    if tree.members.is_empty() {
        return Err(AhrbError::Protocol(
            "row-50 daemon owned tree disappeared during a sweep".to_owned(),
        ));
    }
    sampler.sample(&tree, phase)
}

fn row50_peak_counters(samples: &[Sample], label: &str) -> Result<Row50ProcessCounters> {
    if samples.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "row-50 captured no external process samples for {label}"
        )));
    }
    let counters = samples
        .iter()
        .map(row50_process_counters)
        .collect::<Result<Vec<_>>>()?;
    Ok(Row50ProcessCounters {
        memory_bytes: counters
            .iter()
            .map(|counter| counter.memory_bytes)
            .max()
            .unwrap_or(0),
        open_fds: counters
            .iter()
            .map(|counter| counter.open_fds)
            .max()
            .unwrap_or(0),
        threads: counters
            .iter()
            .map(|counter| counter.threads)
            .max()
            .unwrap_or(0),
        processes: counters
            .iter()
            .map(|counter| counter.processes)
            .max()
            .unwrap_or(0),
    })
}

async fn collect_row50_invocation_terminal(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    sampler: &mut dyn Sampler,
    roots: &[u32],
    deadline: Duration,
    phase: &str,
) -> Result<(
    Vec<NormalizedEvent>,
    Vec<Sample>,
    Row50ProcessCounters,
    bool,
)> {
    if roots.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "row-50 session {} exposed no invocation process root",
            session.0
        )));
    }
    let started = Instant::now();
    let mut samples = Vec::new();
    let events = loop {
        let tree = sampler.discover(roots)?;
        crate::process::track_process_tree(&tree)?;
        if !tree.members.is_empty() {
            samples.push(sampler.sample(&tree, phase)?);
        }
        let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
            AhrbError::Timeout(format!("session {} did not terminalize", session.0))
        })?;
        let observed = tokio::time::timeout(remaining, driver.attach(session, None))
            .await
            .map_err(|_| {
                AhrbError::Timeout(format!(
                    "session {} attach exceeded its row-50 terminal deadline",
                    session.0
                ))
            })??;
        if observed.iter().any(|event| is_terminal(&event.event)) {
            break observed;
        }
        let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
            AhrbError::Timeout(format!("session {} did not terminalize", session.0))
        })?;
        tokio::time::sleep(Duration::from_millis(10).min(remaining)).await;
    };

    let reclaim_started = Instant::now();
    let reclaim_timeout = Duration::from_secs(2);
    loop {
        let tree = sampler.discover(roots)?;
        crate::process::track_process_tree(&tree)?;
        if tree.members.is_empty() {
            return Ok((
                events,
                samples,
                Row50ProcessCounters {
                    memory_bytes: 0,
                    open_fds: 0,
                    threads: 0,
                    processes: 0,
                },
                true,
            ));
        }
        let residue_sample = sampler.sample(&tree, phase)?;
        let residue_counters = row50_process_counters(&residue_sample)?;
        samples.push(residue_sample);
        if reclaim_started.elapsed() >= reclaim_timeout {
            return Ok((events, samples, residue_counters, false));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
fn row50_metadata_identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
fn row50_metadata_identity(_metadata: &std::fs::Metadata) -> (u64, u64) {
    (0, 0)
}

#[cfg(unix)]
fn row50_file_digest(path: &Path, expected: &std::fs::Metadata) -> Result<String> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            AhrbError::Protocol(format!(
                "open row-50 store file {} without following links: {error}",
                path.display()
            ))
        })?;
    let opened = file.metadata()?;
    if row50_metadata_identity(&opened) != row50_metadata_identity(expected)
        || opened.len() != expected.len()
        || !opened.file_type().is_file()
    {
        return Err(AhrbError::Protocol(format!(
            "row-50 store file {} changed identity while being opened",
            path.display()
        )));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16_384];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    if row50_metadata_identity(&after) != row50_metadata_identity(expected)
        || after.len() != expected.len()
    {
        return Err(AhrbError::Protocol(format!(
            "row-50 store file {} changed while being hashed",
            path.display()
        )));
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(not(unix))]
fn row50_file_digest(_path: &Path, _expected: &std::fs::Metadata) -> Result<String> {
    Err(AhrbError::Unsupported(
        "row-50 identity-safe store traversal is unavailable on this platform".to_owned(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn row50_walk_store(
    profile_canonical: &Path,
    profile_device: u64,
    root: &Path,
    root_index: u32,
    directory: &Path,
    identities: &mut BTreeSet<(u64, u64)>,
    entries: &mut Vec<SessionStoreEntry>,
) -> Result<()> {
    let before = std::fs::symlink_metadata(directory)?;
    if before.file_type().is_symlink() || !before.file_type().is_dir() {
        return Err(AhrbError::Protocol(format!(
            "row-50 store directory {} is not a real directory",
            directory.display()
        )));
    }
    let (directory_device, directory_inode) = row50_metadata_identity(&before);
    if directory_device != profile_device {
        return Err(AhrbError::Protocol(format!(
            "row-50 store directory {} crosses device {} from profile device {}",
            directory.display(),
            directory_device,
            profile_device
        )));
    }
    let resolved = std::fs::canonicalize(directory)?;
    if !resolved.starts_with(profile_canonical) {
        return Err(AhrbError::Protocol(format!(
            "row-50 store directory {} resolves outside profile {}",
            directory.display(),
            profile_canonical.display()
        )));
    }
    let mut children = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    children.sort_by_key(|entry| entry.file_name());
    for child in children {
        let path = child.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(AhrbError::Protocol(format!(
                "row-50 refuses symlink entry {}",
                path.display()
            )));
        }
        let (device, inode) = row50_metadata_identity(&metadata);
        if device != profile_device {
            return Err(AhrbError::Protocol(format!(
                "row-50 store entry {} crosses device {} from profile device {}",
                path.display(),
                device,
                profile_device
            )));
        }
        if !identities.insert((device, inode)) {
            return Err(AhrbError::Protocol(format!(
                "row-50 store identity ({device},{inode}) is reachable more than once"
            )));
        }
        let relative = path.strip_prefix(root).map_err(|_| {
            AhrbError::Protocol(format!(
                "row-50 store entry {} is outside declared root {}",
                path.display(),
                root.display()
            ))
        })?;
        let relative_path = relative.to_str().ok_or_else(|| {
            AhrbError::Protocol(format!(
                "row-50 store path {} is not valid UTF-8",
                relative.display()
            ))
        })?;
        let (kind, digest) = if file_type.is_file() {
            ("regular", Some(row50_file_digest(&path, &metadata)?))
        } else if file_type.is_dir() {
            ("directory", None)
        } else {
            ("other", None)
        };
        entries.push(SessionStoreEntry {
            root_index,
            relative_path: relative_path.to_owned(),
            file_type: kind.to_owned(),
            device,
            inode,
            size: metadata.len(),
            sha256: digest,
        });
        if file_type.is_dir() {
            row50_walk_store(
                profile_canonical,
                profile_device,
                root,
                root_index,
                &path,
                identities,
                entries,
            )?;
        }
    }
    let after = std::fs::symlink_metadata(directory)?;
    if after.file_type().is_symlink()
        || row50_metadata_identity(&after) != (directory_device, directory_inode)
    {
        return Err(AhrbError::Protocol(format!(
            "row-50 store directory {} changed identity during traversal",
            directory.display()
        )));
    }
    Ok(())
}

fn row50_store_snapshot(profile_root: &Path, roots: &[PathBuf]) -> Result<Row50StoreSnapshot> {
    let profile_metadata = std::fs::symlink_metadata(profile_root)?;
    if profile_metadata.file_type().is_symlink() || !profile_metadata.file_type().is_dir() {
        return Err(AhrbError::Protocol(format!(
            "row-50 profile {} is not a real directory",
            profile_root.display()
        )));
    }
    let profile_canonical = std::fs::canonicalize(profile_root)?;
    let (profile_device, _) = row50_metadata_identity(&profile_metadata);
    let mut identities = BTreeSet::new();
    let mut entries = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        let relative = root.strip_prefix(profile_root).map_err(|_| {
            AhrbError::Protocol(format!(
                "row-50 declared store root {} escapes profile {}",
                root.display(),
                profile_root.display()
            ))
        })?;
        let mut cursor = profile_root.to_path_buf();
        let mut missing = false;
        for component in relative.components() {
            cursor.push(component.as_os_str());
            match std::fs::symlink_metadata(&cursor) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() {
                        return Err(AhrbError::Protocol(format!(
                            "row-50 declared store component {} is a symlink",
                            cursor.display()
                        )));
                    }
                    let (device, _) = row50_metadata_identity(&metadata);
                    if device != profile_device {
                        return Err(AhrbError::Protocol(format!(
                            "row-50 declared store component {} crosses device",
                            cursor.display()
                        )));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing = true;
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        if missing {
            continue;
        }
        let root_index = u32::try_from(index).map_err(|_| {
            AhrbError::Validation("row-50 has too many declared store roots".to_owned())
        })?;
        let root_metadata = std::fs::symlink_metadata(root)?;
        let root_identity = row50_metadata_identity(&root_metadata);
        if !identities.insert(root_identity) {
            return Err(AhrbError::Protocol(format!(
                "row-50 declared store root identity ({},{}) is reachable more than once",
                root_identity.0, root_identity.1
            )));
        }
        row50_walk_store(
            &profile_canonical,
            profile_device,
            root,
            root_index,
            root,
            &mut identities,
            &mut entries,
        )?;
    }
    entries.sort_by(|left, right| {
        (left.root_index, &left.relative_path).cmp(&(right.root_index, &right.relative_path))
    });
    Ok(Row50StoreSnapshot { entries })
}

fn row50_store_residue(baseline: &Row50StoreSnapshot, current: &Row50StoreSnapshot) -> (u64, u64) {
    let baseline_entries = baseline
        .entries
        .iter()
        .map(|entry| ((entry.root_index, entry.relative_path.as_str()), entry))
        .collect::<BTreeMap<_, _>>();
    current
        .entries
        .iter()
        .filter(|entry| entry.file_type == "regular")
        .filter(|entry| {
            baseline_entries
                .get(&(entry.root_index, entry.relative_path.as_str()))
                .is_none_or(|baseline| *baseline != *entry)
        })
        .fold((0_u64, 0_u64), |(bytes, files), entry| {
            (bytes.saturating_add(entry.size), files.saturating_add(1))
        })
}

fn row50_checkpoint(
    repetition: u32,
    sessions_created: u32,
    baseline: Row50ProcessCounters,
    current: Row50ProcessCounters,
    store_baseline: &Row50StoreSnapshot,
    store_current: Row50StoreSnapshot,
) -> SessionResidueCheckpoint {
    let (store_residue_bytes, store_residue_files) =
        row50_store_residue(store_baseline, &store_current);
    SessionResidueCheckpoint {
        repetition,
        sessions_created,
        memory_residue_mib: current.memory_bytes.saturating_sub(baseline.memory_bytes) as f64
            / 1_048_576.0,
        fd_delta: current.open_fds as f64 - baseline.open_fds as f64,
        thread_delta: current.threads as f64 - baseline.threads as f64,
        process_delta: current.processes as f64 - baseline.processes as f64,
        store_residue_bytes,
        store_residue_files,
        close_delete_validated_through: sessions_created,
        traversal_valid: true,
        store_entries: store_current.entries,
    }
}

#[cfg(test)]
mod row50_store_tests {
    use super::*;

    fn fresh_profile(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ahrb-row50-{label}-{}-{}",
            std::process::id(),
            SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn row50_store_snapshot_records_digest_and_rejects_hardlink_aliases() {
        let profile = fresh_profile("hardlink");
        let store = profile.join("state/sessions");
        std::fs::create_dir_all(&store).expect("create store");
        let first = store.join("one");
        std::fs::write(&first, b"row-50").expect("write store fixture");
        let snapshot = row50_store_snapshot(&profile, std::slice::from_ref(&store))
            .expect("snapshot one regular file");
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].file_type, "regular");
        assert!(snapshot.entries[0].sha256.is_some());

        std::fs::hard_link(&first, store.join("two")).expect("create hardlink alias");
        let error = row50_store_snapshot(&profile, std::slice::from_ref(&store))
            .expect_err("duplicate identity must be rejected");
        assert!(error.to_string().contains("reachable more than once"));
        std::fs::remove_dir_all(profile).expect("remove row-50 hardlink fixture");
    }

    #[cfg(unix)]
    #[test]
    fn row50_store_snapshot_never_follows_symlinks() {
        use std::os::unix::fs::symlink;
        let profile = fresh_profile("symlink");
        let store = profile.join("state/sessions");
        std::fs::create_dir_all(&store).expect("create store");
        symlink(
            profile.parent().expect("profile parent"),
            store.join("escape"),
        )
        .expect("create symlink fixture");
        let error = row50_store_snapshot(&profile, std::slice::from_ref(&store))
            .expect_err("symlink must be rejected");
        assert!(error.to_string().contains("refuses symlink"));
        std::fs::remove_dir_all(profile).expect("remove row-50 symlink fixture");
    }
}

async fn collect_session_residue_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<SessionResidueTrials> {
    let (expected_repetitions, expected_sessions) = match profile {
        Profile::Quick => (3_u32, 20_u32),
        Profile::Cert => (7_u32, 200_u32),
    };
    let per_invocation = per_invocation_topology(manifest);
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    let mut all_samples = Vec::new();
    let mut sweeps = Vec::new();
    for repetition in 1..=expected_repetitions {
        let profile_root = run_profile_root.join(format!("derived-row50-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-50 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let workflow = session_residue_workflow(&profile_root, repetition, expected_sessions);
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row50-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let store_roots =
            crate::manifest::render_session_store_paths(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        driver.await_readiness().await?;

        for warmup_ordinal in 1..=10_u32 {
            let warmup_actor = format!("r50-warmup-r{repetition}-s{warmup_ordinal:02}");
            let warmup = workflow.actors.get(&warmup_actor).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-50 repetition {repetition} warm-up actor {warmup_actor:?} disappeared"
                ))
            })?;
            let warmup_session = driver
                .create_session(&format!("{}:{warmup_actor}", workflow.scenario))
                .await?;
            driver
                .submit(
                    &warmup_session,
                    &warmup.prompt,
                    &format!("row-50-warmup-{warmup_ordinal:02}"),
                )
                .await?;
            all_events.extend(
                collect_session_terminal(
                    &mut driver,
                    &warmup_session,
                    None,
                    outer_turn_timeout(manifest),
                )
                .await?,
            );
            driver.close_delete(&warmup_session).await?;
        }

        let mut sampler = platform_sampler();
        let daemon_roots = if per_invocation {
            Vec::new()
        } else {
            let roots = verified_process_roots(
                manifest,
                sampler.as_mut(),
                driver.owned_pids(),
                driver.daemon_pid(),
            )?;
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-50 repetition {repetition} daemon exposed no owned PID"
                )));
            }
            roots
        };
        let baseline_process = if per_invocation {
            Row50ProcessCounters {
                memory_bytes: 0,
                open_fds: 0,
                threads: 0,
                processes: 0,
            }
        } else {
            let baseline = baseline_samples(sampler.as_mut(), &daemon_roots, profile).await?;
            all_samples.extend(baseline.iter().cloned());
            row50_baseline_counters(&baseline)?
        };
        let baseline_store = row50_store_snapshot(&profile_root, &store_roots)?;
        let mut checkpoints = vec![row50_checkpoint(
            repetition,
            0,
            baseline_process,
            baseline_process,
            &baseline_store,
            baseline_store.clone(),
        )];
        let mut maximum_active_delta_bytes = 0_u64;
        let mut closed_sessions = 0_u32;
        let mut unretired_sessions = 0_u32;
        let mut process_audits = Vec::new();

        for ordinal in 1..=expected_sessions {
            let actor_name = format!("r50-r{repetition}-s{ordinal:03}");
            let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-50 repetition {repetition} actor {actor_name:?} disappeared"
                ))
            })?;
            let session = driver
                .create_session(&format!("{}:{actor_name}", workflow.scenario))
                .await?;
            driver
                .submit(
                    &session,
                    &actor.prompt,
                    &format!("row-50-r{repetition}-s{ordinal:03}"),
                )
                .await?;
            let invocation_roots = if per_invocation {
                driver.session_pids(&session)
            } else {
                daemon_roots.clone()
            };
            if !per_invocation && !invocation_roots.is_empty() {
                let tree = sampler.discover(&invocation_roots)?;
                if !tree.members.is_empty() {
                    let active = sampler
                        .sample(&tree, &format!("row50-r{repetition}-s{ordinal:03}-active"))?;
                    maximum_active_delta_bytes = maximum_active_delta_bytes.max(
                        effective_sample_bytes(&active)
                            .saturating_sub(baseline_process.memory_bytes),
                    );
                    all_samples.push(active);
                }
            }
            let (suffix, invocation_samples, invocation_residue, invocation_reclaimed) =
                if per_invocation {
                    collect_row50_invocation_terminal(
                        &mut driver,
                        &session,
                        sampler.as_mut(),
                        &invocation_roots,
                        outer_turn_timeout(manifest),
                        &format!("row50-r{repetition}-s{ordinal:03}-invocation"),
                    )
                    .await?
                } else {
                    (
                        collect_session_terminal(
                            &mut driver,
                            &session,
                            None,
                            outer_turn_timeout(manifest),
                        )
                        .await?,
                        Vec::new(),
                        Row50ProcessCounters {
                            memory_bytes: 0,
                            open_fds: 0,
                            threads: 0,
                            processes: 0,
                        },
                        true,
                    )
                };
            let terminal_count = suffix
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count();
            if terminal_count != 1 {
                return Err(AhrbError::Protocol(format!(
                    "row-50 repetition {repetition} session {ordinal} emitted {terminal_count} terminals"
                )));
            }
            all_events.extend(suffix);
            driver.close_delete(&session).await.map_err(|error| {
                AhrbError::Protocol(format!(
                    "row-50 repetition {repetition} session {ordinal} public close-delete failed: {error}"
                ))
            })?;
            closed_sessions = closed_sessions.saturating_add(1);
            if per_invocation {
                let exited_cleanly =
                    matches!(driver.client_exit(&session), ClientExit::Exited(Some(0)));
                let invocation_peak = row50_peak_counters(
                    &invocation_samples,
                    &format!("invocation r{repetition}/s{ordinal}"),
                )?;
                maximum_active_delta_bytes =
                    maximum_active_delta_bytes.max(invocation_peak.memory_bytes);
                all_samples.extend(invocation_samples.iter().cloned());
                let close_observation = driver
                    .close_delete_process_observation(&session)
                    .ok_or_else(|| {
                        AhrbError::Protocol(format!(
                            "row-50 repetition {repetition} session {ordinal} has no public close-delete process receipt"
                        ))
                    })?;
                let close_peak = row50_peak_counters(
                    &close_observation.samples,
                    &format!("close-delete r{repetition}/s{ordinal}"),
                )?;
                all_samples.extend(close_observation.samples.iter().cloned());
                let close_residue = close_observation
                    .residue_sample
                    .as_ref()
                    .map(row50_process_counters)
                    .transpose()?
                    .unwrap_or(Row50ProcessCounters {
                        memory_bytes: 0,
                        open_fds: 0,
                        threads: 0,
                        processes: 0,
                    });
                if !exited_cleanly
                    || !invocation_reclaimed
                    || invocation_residue.processes != 0
                    || close_residue.processes != 0
                    || !close_observation.result_validated
                {
                    unretired_sessions = unretired_sessions.saturating_add(1);
                }
                process_audits.push(SessionResidueProcessAudit {
                    repetition,
                    session_ordinal: ordinal,
                    invocation_sample_count: u32::try_from(invocation_samples.len()).map_err(
                        |_| {
                            AhrbError::Protocol(
                                "row-50 invocation sample count exceeds report range".to_owned(),
                            )
                        },
                    )?,
                    invocation_peak_memory_bytes: invocation_peak.memory_bytes,
                    invocation_peak_open_fds: invocation_peak.open_fds,
                    invocation_peak_threads: invocation_peak.threads,
                    invocation_peak_processes: invocation_peak.processes,
                    invocation_residue_memory_bytes: invocation_residue.memory_bytes,
                    invocation_residue_open_fds: invocation_residue.open_fds,
                    invocation_residue_threads: invocation_residue.threads,
                    invocation_residue_processes: invocation_residue.processes,
                    invocation_reclaim_validated: invocation_reclaimed,
                    close_delete_sample_count: u32::try_from(close_observation.samples.len())
                        .map_err(|_| {
                            AhrbError::Protocol(
                                "row-50 close-delete sample count exceeds report range".to_owned(),
                            )
                        })?,
                    close_delete_peak_memory_bytes: close_peak.memory_bytes,
                    close_delete_peak_open_fds: close_peak.open_fds,
                    close_delete_peak_threads: close_peak.threads,
                    close_delete_peak_processes: close_peak.processes,
                    close_delete_residue_memory_bytes: close_residue.memory_bytes,
                    close_delete_residue_open_fds: close_residue.open_fds,
                    close_delete_residue_threads: close_residue.threads,
                    close_delete_residue_processes: close_residue.processes,
                    close_delete_reclaim_validated: close_observation.residue_sample.is_none(),
                    close_delete_result_validated: close_observation.result_validated,
                });
            }
            if ordinal % 10 == 0 {
                if ordinal == expected_sessions {
                    let reclaim_started = Instant::now();
                    while reclaim_started.elapsed() < Duration::from_secs(10) {
                        if !per_invocation {
                            let sample = row50_sample_owned_tree(
                                sampler.as_mut(),
                                &daemon_roots,
                                &format!("row50-r{repetition}-final-reclaim"),
                            )?;
                            all_samples.push(sample);
                        }
                        let remaining =
                            Duration::from_secs(10).saturating_sub(reclaim_started.elapsed());
                        if remaining.is_zero() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100).min(remaining)).await;
                    }
                }
                let current_process = if per_invocation {
                    let audited = process_audits.iter().filter(|audit| {
                        audit.session_ordinal <= ordinal
                            && audit.invocation_reclaim_validated
                            && audit.close_delete_reclaim_validated
                    });
                    Row50ProcessCounters {
                        memory_bytes: 0,
                        open_fds: 0,
                        threads: 0,
                        processes: audited.fold(0_u64, |residue, audit| {
                            residue
                                .saturating_add(audit.invocation_residue_processes)
                                .saturating_add(audit.close_delete_residue_processes)
                        }),
                    }
                } else {
                    let sample = row50_sample_owned_tree(
                        sampler.as_mut(),
                        &daemon_roots,
                        &format!("row50-r{repetition}-checkpoint-{ordinal}"),
                    )?;
                    let counters = row50_process_counters(&sample)?;
                    all_samples.push(sample);
                    counters
                };
                let current_store = row50_store_snapshot(&profile_root, &store_roots)?;
                checkpoints.push(row50_checkpoint(
                    repetition,
                    ordinal,
                    baseline_process,
                    current_process,
                    &baseline_store,
                    current_store,
                ));
            }
        }
        let final_memory_residue_mib = checkpoints
            .last()
            .map_or(0.0, |checkpoint| checkpoint.memory_residue_mib);
        sweeps.push(SessionResidueSweep {
            repetition,
            created_sessions: expected_sessions,
            closed_sessions,
            unretired_sessions,
            maximum_active_delta_mib: maximum_active_delta_bytes as f64 / 1_048_576.0,
            final_memory_residue_mib,
            checkpoints,
            process_audits,
        });
        driver.shutdown().await?;
        server.shutdown().await?;
        all_requests.extend(engine.request_records().await);
    }
    Ok(SessionResidueTrials {
        events: all_events,
        requests: all_requests,
        sweeps,
        samples: all_samples,
    })
}

async fn collect_turn_latency_repetition(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
    repetition: u32,
    turns: u32,
) -> Result<TurnLatencyTrials> {
    let profile_root = run_profile_root.join(format!("derived-row43-r{repetition}"));
    prepare_profile(manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare row-43 fresh profile: {error}")))?;
    let workflow = turn_latency_workflow(&profile_root, repetition, turns);
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row43-r{repetition}-{}",
        &manifest_hash[..16],
        std::process::id()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential);
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_turn_timeout(manifest),
    )?;
    driver.start().await?;
    let session = driver
        .create_session(&format!("{}:row43", workflow.scenario))
        .await?;
    let session_id_hash = stable_evidence_hash(&session.0);
    let actor = format!("r43-latency-r{repetition}");
    let outer_timeout = outer_turn_timeout(manifest);
    let mut events = Vec::new();
    let mut observations = Vec::with_capacity(turns as usize);
    let warmup_prompt = workflow
        .actors
        .get(&actor)
        .map(|actor| actor.prompt.as_str())
        .ok_or_else(|| AhrbError::Protocol("row-43 actor disappeared".to_owned()))?;
    let warmup_boundary_count = driver.completed_turn_boundaries().len();
    driver
        .submit(&session, warmup_prompt, "row-43-warmup")
        .await?;
    let warmup_events =
        collect_session_terminal(&mut driver, &session, None, outer_timeout).await?;
    let mut after = warmup_events.iter().map(|event| Cursor(event.cursor)).max();
    let warmup_terminals = warmup_events
        .iter()
        .filter(|event| is_terminal(&event.event))
        .count();
    let warmup_tool_events = warmup_events
        .iter()
        .filter(|event| matches!(event.event, EventVocab::ToolCall | EventVocab::ToolResult))
        .count();
    if warmup_terminals != 1 || warmup_tool_events != 0 {
        return Err(AhrbError::Protocol(format!(
            "row-43 warm-up was not one direct-terminal semantic turn: terminals={warmup_terminals}, tool_events={warmup_tool_events}"
        )));
    }
    if per_invocation_topology(manifest) {
        let _warmup_boundary = await_completed_turn_boundary(
            &mut driver,
            &session,
            after,
            warmup_boundary_count,
            outer_timeout,
        )
        .await?;
    }
    for turn in 1..=turns {
        let prompt = format!(
            "AHRB turn latency direct terminal turn {turn} {}",
            route_marker(&workflow.scenario, &actor, &format!("turn-{turn:04}"))
        );
        let previous_boundary_count = driver.completed_turn_boundaries().len();
        let submit_ns = monotonic_timestamp_ns();
        driver
            .submit(&session, &prompt, &format!("row-43-turn-{turn:04}"))
            .await?;
        let suffix = collect_session_terminal_with_poll(
            &mut driver,
            &session,
            after,
            outer_timeout,
            Duration::from_millis(1),
        )
        .await?;
        let terminal_ns = monotonic_timestamp_ns();
        after = suffix
            .iter()
            .map(|event| Cursor(event.cursor))
            .max()
            .or(after);
        events.extend(suffix);
        let (launch_ns, exit_ns, turn_wall_ns) = if per_invocation_topology(manifest) {
            let boundary = await_completed_turn_boundary(
                &mut driver,
                &session,
                after,
                previous_boundary_count,
                outer_timeout,
            )
            .await?;
            let wall_ns = boundary
                .exit_ns
                .checked_sub(boundary.launch_ns)
                .ok_or_else(|| {
                    AhrbError::Protocol("row-43 launch/exit boundaries are reversed".to_owned())
                })?;
            (Some(boundary.launch_ns), Some(boundary.exit_ns), wall_ns)
        } else {
            let wall_ns = terminal_ns.checked_sub(submit_ns).ok_or_else(|| {
                AhrbError::Protocol("row-43 submit/terminal boundaries are reversed".to_owned())
            })?;
            (None, None, wall_ns)
        };
        observations.push(TurnObservation {
            repetition,
            turn_index: turn,
            actor: actor.clone(),
            session_id_hash: session_id_hash.clone(),
            phase: "turn-latency".to_owned(),
            launch_ns,
            submit_ns: Some(submit_ns),
            first_model_request_ns: None,
            terminal_ns: Some(terminal_ns),
            exit_ns,
            turn_wall_ns: Some(turn_wall_ns),
        });
    }
    if manifest.transport.kind == TransportKind::Exec {
        driver.close(&session).await?;
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    let all_requests = engine.request_records().await;
    let scripted_primary_checkpoints = all_requests
        .iter()
        .filter(|request| request.accepted && request.role == "primary")
        .map(|request| request.request.checkpoint.as_str())
        .collect::<BTreeSet<_>>();
    let expected_primary_checkpoints = std::iter::once("warmup".to_owned())
        .chain((1..=turns).map(|turn| format!("turn-{turn:04}")))
        .collect::<BTreeSet<_>>();
    if scripted_primary_checkpoints
        != expected_primary_checkpoints
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
    {
        return Err(AhrbError::Protocol(format!(
            "row-43 provider scripts did not cover exactly one warm-up plus {turns} measured checkpoints"
        )));
    }
    let requests = all_requests
        .into_iter()
        .filter(|request| request.request.checkpoint != "warmup")
        .collect::<Vec<_>>();
    let first_requests = requests
        .iter()
        .fold(BTreeMap::new(), |mut values, request| {
            values
                .entry(request.request.checkpoint.as_str())
                .and_modify(|timestamp: &mut u64| {
                    *timestamp = (*timestamp).min(request.received_ns)
                })
                .or_insert(request.received_ns);
            values
        });
    for observation in &mut observations {
        observation.first_model_request_ns = first_requests
            .get(format!("turn-{:04}", observation.turn_index).as_str())
            .copied();
    }
    Ok(TurnLatencyTrials {
        events,
        requests,
        turns: observations,
    })
}

async fn collect_turn_latency_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<TurnLatencyTrials> {
    let turns = match profile {
        Profile::Quick => 100_u32,
        Profile::Cert => 1_000_u32,
    };
    let repetitions = ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).repetitions;
    let mut combined = TurnLatencyTrials {
        events: Vec::new(),
        requests: Vec::new(),
        turns: Vec::new(),
    };
    for repetition in 1..=repetitions {
        let trial = collect_turn_latency_repetition(
            manifest,
            run_profile_root,
            manifest_hash,
            repetition,
            turns,
        )
        .await?;
        combined.events.extend(trial.events);
        combined.requests.extend(trial.requests);
        combined.turns.extend(trial.turns);
    }
    combined.requests.sort_by(|left, right| {
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
    combined
        .turns
        .sort_by_key(|turn| (turn.repetition, turn.turn_index));
    Ok(combined)
}

fn context_recovery_workflow(
    manifest: &Manifest,
    profile_root: &Path,
    ordinary_turns: u32,
    tool_pairs: u32,
    window_tokens: u64,
) -> Result<Workflow> {
    let scenario = "ahrb-row51-context-limit".to_owned();
    let actor = "r51-context".to_owned();
    let mut responses = Vec::new();
    for turn in 1..=ordinary_turns {
        let checkpoint = format!("history-{turn:04}");
        if turn <= tool_pairs {
            let terminal_checkpoint = format!("history-{turn:04}-terminal");
            let terminal_marker = route_marker(&scenario, &actor, &terminal_checkpoint);
            let call = mapped_tool_call(
                manifest,
                "write",
                format!("row51-call-{turn:04}"),
                json!({
                    "path": format!("row51-effect-{turn:04}.txt"),
                    "content": format!("AHRB-COMMITTED-EFFECT-{turn:04} {terminal_marker}"),
                }),
            )?;
            responses.push(ScriptedResponse {
                scenario: scenario.clone(),
                actor: actor.clone(),
                checkpoint,
                request_hash: String::new(),
                response: json!({"tool_calls":[call]}),
                fault: None,
                barrier: None,
            });
            responses.push(ScriptedResponse {
                scenario: scenario.clone(),
                actor: actor.clone(),
                checkpoint: terminal_checkpoint,
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            });
        } else {
            responses.push(ScriptedResponse {
                scenario: scenario.clone(),
                actor: actor.clone(),
                checkpoint,
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            });
        }
    }
    responses.push(ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: "recover".to_owned(),
        request_hash: String::new(),
        response: success_value(),
        fault: Some(Fault::ContextLength { window_tokens }),
        barrier: None,
    });
    Ok(Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor,
                parent: None,
                prompt: "AHRB row 51 context recovery".to_owned(),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses,
    })
}

fn apply_context_window_surface(
    manifest: &Manifest,
    window_tokens: u64,
    variables: &mut BTreeMap<String, String>,
    environment: &mut BTreeMap<String, String>,
) -> Result<Vec<String>> {
    let context = manifest.resources.context_window.as_ref().ok_or_else(|| {
        AhrbError::Protocol("row-51 selected without resources.context_window".to_owned())
    })?;
    variables.insert(
        "context_window_tokens".to_owned(),
        window_tokens.to_string(),
    );
    if context.surface == "environment" {
        environment.insert(context.environment.clone(), window_tokens.to_string());
    }
    if context.surface == "cli" {
        render_argv(&context.argv, variables)
    } else {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
enum NativeContextPairItem {
    Call {
        call_id: String,
        function_name: String,
        arguments: Value,
    },
    Result {
        call_id: String,
        content: Value,
    },
}

#[derive(Debug, Default)]
struct ContextPairExtraction {
    pairs: Vec<ContextToolPairRecord>,
    orphan_calls: u32,
    orphan_results: u32,
}

#[derive(Debug)]
struct PendingContextPair {
    call_id: String,
    function_name: String,
    canonical_arguments: Value,
    result_content: Option<Value>,
}

fn canonical_context_arguments(value: &Value) -> Result<Value> {
    let decoded = match value {
        Value::String(text) => serde_json::from_str::<Value>(text).map_err(|error| {
            AhrbError::Protocol(format!(
                "row-51 tool arguments are not canonical JSON: {error}"
            ))
        })?,
        other => other.clone(),
    };
    Ok(crate::fake_model::canonicalize_json(&decoded))
}

fn collect_responses_pair_items(
    value: &Value,
    items: &mut Vec<NativeContextPairItem>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_responses_pair_items(value, items)?;
            }
        }
        Value::Object(object) => match object.get("type").and_then(Value::as_str) {
            Some("function_call") => {
                let call_id = object
                    .get("call_id")
                    .or_else(|| object.get("id"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "row-51 Responses function_call omitted call_id".to_owned(),
                        )
                    })?;
                let function_name =
                    object.get("name").and_then(Value::as_str).ok_or_else(|| {
                        AhrbError::Protocol(
                            "row-51 Responses function_call omitted name".to_owned(),
                        )
                    })?;
                let arguments = object.get("arguments").ok_or_else(|| {
                    AhrbError::Protocol(
                        "row-51 Responses function_call omitted arguments".to_owned(),
                    )
                })?;
                items.push(NativeContextPairItem::Call {
                    call_id: call_id.to_owned(),
                    function_name: function_name.to_owned(),
                    arguments: canonical_context_arguments(arguments)?,
                });
            }
            Some("function_call_output") => {
                let call_id = object
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "row-51 Responses function_call_output omitted call_id".to_owned(),
                        )
                    })?;
                items.push(NativeContextPairItem::Result {
                    call_id: call_id.to_owned(),
                    content: crate::fake_model::canonicalize_json(
                        object.get("output").unwrap_or(&Value::Null),
                    ),
                });
            }
            _ => {
                for value in object.values() {
                    collect_responses_pair_items(value, items)?;
                }
            }
        },
        _ => {}
    }
    Ok(())
}

fn native_context_pair_items(
    dialect: &str,
    canonical: &Value,
) -> Result<Vec<NativeContextPairItem>> {
    let mut items = Vec::new();
    match dialect {
        "openai-chat-completions" => {
            let messages = canonical
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AhrbError::Protocol("row-51 Chat request omitted messages".to_owned())
                })?;
            for message in messages {
                if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
                    for call in calls {
                        let call_id = call.get("id").and_then(Value::as_str).ok_or_else(|| {
                            AhrbError::Protocol("row-51 Chat tool call omitted id".to_owned())
                        })?;
                        let function =
                            call.get("function")
                                .and_then(Value::as_object)
                                .ok_or_else(|| {
                                    AhrbError::Protocol(
                                        "row-51 Chat tool call omitted function".to_owned(),
                                    )
                                })?;
                        let function_name = function
                            .get("name")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                AhrbError::Protocol(
                                    "row-51 Chat tool call omitted function.name".to_owned(),
                                )
                            })?;
                        let arguments = function.get("arguments").ok_or_else(|| {
                            AhrbError::Protocol(
                                "row-51 Chat tool call omitted function.arguments".to_owned(),
                            )
                        })?;
                        items.push(NativeContextPairItem::Call {
                            call_id: call_id.to_owned(),
                            function_name: function_name.to_owned(),
                            arguments: canonical_context_arguments(arguments)?,
                        });
                    }
                }
                if message.get("role").and_then(Value::as_str) == Some("tool") {
                    let call_id = message
                        .get("tool_call_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            AhrbError::Protocol(
                                "row-51 Chat tool result omitted tool_call_id".to_owned(),
                            )
                        })?;
                    items.push(NativeContextPairItem::Result {
                        call_id: call_id.to_owned(),
                        content: crate::fake_model::canonicalize_json(
                            message.get("content").unwrap_or(&Value::Null),
                        ),
                    });
                }
            }
        }
        "openai-responses" => {
            collect_responses_pair_items(
                canonical.get("input").unwrap_or(&Value::Null),
                &mut items,
            )?;
        }
        "anthropic-messages" => {
            let messages = canonical
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AhrbError::Protocol("row-51 Anthropic request omitted messages".to_owned())
                })?;
            for message in messages {
                let blocks = message
                    .get("content")
                    .and_then(Value::as_array)
                    .map_or(&[][..], Vec::as_slice);
                for block in blocks {
                    match block.get("type").and_then(Value::as_str) {
                        Some("tool_use") => {
                            items.push(NativeContextPairItem::Call {
                                call_id: block
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| {
                                        AhrbError::Protocol(
                                            "row-51 Anthropic tool_use omitted id".to_owned(),
                                        )
                                    })?
                                    .to_owned(),
                                function_name: block
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| {
                                        AhrbError::Protocol(
                                            "row-51 Anthropic tool_use omitted name".to_owned(),
                                        )
                                    })?
                                    .to_owned(),
                                arguments: canonical_context_arguments(
                                    block.get("input").unwrap_or(&Value::Null),
                                )?,
                            });
                        }
                        Some("tool_result") => {
                            items.push(NativeContextPairItem::Result {
                                call_id: block
                                    .get("tool_use_id")
                                    .and_then(Value::as_str)
                                    .ok_or_else(|| {
                                        AhrbError::Protocol(
                                            "row-51 Anthropic tool_result omitted tool_use_id"
                                                .to_owned(),
                                        )
                                    })?
                                    .to_owned(),
                                content: crate::fake_model::canonicalize_json(
                                    block.get("content").unwrap_or(&Value::Null),
                                ),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        other => {
            return Err(AhrbError::Protocol(format!(
                "row-51 cannot extract dialect-native tool pairs for {other:?}"
            )));
        }
    }
    Ok(items)
}

fn pair_context_items(items: Vec<NativeContextPairItem>) -> ContextPairExtraction {
    let mut pending = Vec::<PendingContextPair>::new();
    let mut orphan_results = 0_u32;
    for item in items {
        match item {
            NativeContextPairItem::Call {
                call_id,
                function_name,
                arguments,
            } => pending.push(PendingContextPair {
                call_id,
                function_name,
                canonical_arguments: arguments,
                result_content: None,
            }),
            NativeContextPairItem::Result { call_id, content } => {
                if let Some(pair) = pending
                    .iter_mut()
                    .find(|pair| pair.call_id == call_id && pair.result_content.is_none())
                {
                    pair.result_content = Some(content);
                } else {
                    orphan_results = orphan_results.saturating_add(1);
                }
            }
        }
    }
    let orphan_calls = pending
        .iter()
        .filter(|pair| pair.result_content.is_none())
        .count() as u32;
    let pairs = pending
        .into_iter()
        .filter_map(|pair| {
            Some(ContextToolPairRecord {
                call_id: pair.call_id,
                function_name: pair.function_name,
                canonical_arguments: pair.canonical_arguments,
                result_content: pair.result_content?,
            })
        })
        .collect();
    ContextPairExtraction {
        pairs,
        orphan_calls,
        orphan_results,
    }
}

fn context_tool_pair_evidence(dialect: &str, canonical: &Value) -> Result<ContextPairExtraction> {
    Ok(pair_context_items(native_context_pair_items(
        dialect, canonical,
    )?))
}

fn committed_context_tool_pairs(events: &[NormalizedEvent]) -> Result<ContextPairExtraction> {
    let mut items = Vec::new();
    for event in events {
        match event.event {
            EventVocab::ToolCall => {
                items.push(NativeContextPairItem::Call {
                    call_id: event
                        .payload
                        .get("call_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            AhrbError::Protocol(
                                "row-51 committed ToolCall omitted call_id".to_owned(),
                            )
                        })?
                        .to_owned(),
                    function_name: event
                        .payload
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            AhrbError::Protocol("row-51 committed ToolCall omitted name".to_owned())
                        })?
                        .to_owned(),
                    arguments: canonical_context_arguments(
                        event.payload.get("arguments").unwrap_or(&Value::Null),
                    )?,
                });
            }
            EventVocab::ToolResult => {
                let result = event.payload.get("result").ok_or_else(|| {
                    AhrbError::Protocol("row-51 committed ToolResult omitted result".to_owned())
                })?;
                items.push(NativeContextPairItem::Result {
                    call_id: event
                        .payload
                        .get("call_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            AhrbError::Protocol(
                                "row-51 committed ToolResult omitted call_id".to_owned(),
                            )
                        })?
                        .to_owned(),
                    content: Value::String(serde_json::to_string(result)?),
                });
            }
            _ => {}
        }
    }
    Ok(pair_context_items(items))
}

fn collect_context_strings(value: &Value, strings: &mut Vec<String>) {
    match value {
        Value::String(text) => strings.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                collect_context_strings(item, strings);
            }
        }
        Value::Object(object) => {
            for value in object.values() {
                collect_context_strings(value, strings);
            }
        }
        _ => {}
    }
}

#[derive(Debug)]
struct DialectContextTextItem {
    role: String,
    text: String,
    summary_eligible: bool,
    ordinary_history_eligible: bool,
}

fn context_content_fragments(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items.iter().flat_map(context_content_fragments).collect(),
        Value::Object(object)
            if matches!(
                object.get("type").and_then(Value::as_str),
                Some("text" | "input_text" | "output_text")
            ) =>
        {
            object
                .get("text")
                .and_then(Value::as_str)
                .map(|text| vec![text.to_owned()])
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn combined_context_content(value: &Value) -> Option<String> {
    let fragments = context_content_fragments(value);
    (!fragments.is_empty()).then(|| fragments.join(" "))
}

fn push_message_context_item(items: &mut Vec<DialectContextTextItem>, message: &Value, role: &str) {
    let Some(text) = combined_context_content(message.get("content").unwrap_or(&Value::Null))
    else {
        return;
    };
    items.push(DialectContextTextItem {
        role: role.to_owned(),
        text,
        summary_eligible: matches!(role, "system" | "developer"),
        ordinary_history_eligible: matches!(role, "user" | "assistant"),
    });
}

fn dialect_context_text_items(
    dialect: &str,
    canonical: &Value,
) -> Result<Vec<DialectContextTextItem>> {
    let mut items = Vec::new();
    match dialect {
        "openai-chat-completions" => {
            let messages = canonical
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AhrbError::Protocol("row-51 Chat request omitted messages".to_owned())
                })?;
            for message in messages {
                let role = message.get("role").and_then(Value::as_str).unwrap_or("");
                push_message_context_item(&mut items, message, role);
            }
        }
        "openai-responses" => {
            if let Some(instructions) = canonical.get("instructions") {
                let instruction_items = match instructions {
                    Value::Array(values) => values.as_slice(),
                    other => std::slice::from_ref(other),
                };
                for instruction in instruction_items {
                    if let Some(text) = combined_context_content(instruction) {
                        items.push(DialectContextTextItem {
                            role: "instructions".to_owned(),
                            text,
                            summary_eligible: true,
                            ordinary_history_eligible: false,
                        });
                    }
                }
            }
            match canonical.get("input") {
                Some(Value::Array(input)) => {
                    for item in input {
                        let role = item.get("role").and_then(Value::as_str).unwrap_or("");
                        push_message_context_item(&mut items, item, role);
                    }
                }
                Some(Value::String(text)) => items.push(DialectContextTextItem {
                    role: "user".to_owned(),
                    text: text.clone(),
                    summary_eligible: false,
                    ordinary_history_eligible: true,
                }),
                _ => {}
            }
        }
        "anthropic-messages" => {
            if let Some(system) = canonical.get("system") {
                let system_items = match system {
                    Value::Array(values) => values.as_slice(),
                    other => std::slice::from_ref(other),
                };
                for item in system_items {
                    if let Some(text) = combined_context_content(item) {
                        items.push(DialectContextTextItem {
                            role: "system".to_owned(),
                            text,
                            summary_eligible: true,
                            ordinary_history_eligible: false,
                        });
                    }
                }
            }
            let messages = canonical
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    AhrbError::Protocol("row-51 Anthropic request omitted messages".to_owned())
                })?;
            for message in messages {
                let role = message.get("role").and_then(Value::as_str).unwrap_or("");
                push_message_context_item(&mut items, message, role);
            }
        }
        other => {
            return Err(AhrbError::Protocol(format!(
                "row-51 cannot inspect dialect-native context for {other:?}"
            )));
        }
    }
    Ok(items)
}

fn marker_tokens(value: &Value, prefix: &str) -> Vec<String> {
    let mut strings = Vec::new();
    collect_context_strings(value, &mut strings);
    strings
        .iter()
        .flat_map(|text| text.split_whitespace())
        .filter(|token| token.starts_with(prefix))
        .map(str::to_owned)
        .collect()
}

struct ContextMarkerEvidence {
    ordinary: Vec<String>,
    effects: Vec<String>,
    goals: Vec<ContextGoalItemRecord>,
    summaries: Vec<String>,
    summary_items: usize,
}

#[derive(Debug)]
struct ContextHistoryValidation {
    retained_latest_four_exactly: bool,
    omitted_summarized_exactly: bool,
    omitted: Vec<String>,
}

fn expected_context_history_markers(ordinary_turns: u32) -> Vec<String> {
    (1..=ordinary_turns)
        .map(|turn| {
            let digest = Sha256::digest(format!("ahrb-history-{turn}").as_bytes());
            format!("AHRB-HISTORY-{turn:04}-{digest:x}")
        })
        .collect()
}

fn validate_context_history(
    before: &ContextMarkerEvidence,
    after: &ContextMarkerEvidence,
    expected: &[String],
) -> ContextHistoryValidation {
    let retain_from = expected.len().saturating_sub(4);
    let omitted = expected[..retain_from].to_vec();
    let faulting_history_exact = before.summary_items == 0 && before.ordinary == expected;
    ContextHistoryValidation {
        retained_latest_four_exactly: faulting_history_exact
            && after.ordinary == expected[retain_from..],
        omitted_summarized_exactly: faulting_history_exact
            && after.summary_items == 1
            && after.summaries == omitted,
        omitted,
    }
}

fn context_marker_evidence(
    dialect: &str,
    canonical: &Value,
    pairs: &[ContextToolPairRecord],
) -> Result<ContextMarkerEvidence> {
    let items = dialect_context_text_items(dialect, canonical)?;
    let mut ordinary = Vec::new();
    let mut summaries = Vec::new();
    let mut goals = Vec::new();
    let mut summary_items = 0_usize;
    for item in &items {
        if item.text.starts_with("AHRB-GOAL-MARKER ") {
            goals.push(ContextGoalItemRecord {
                role: item.role.clone(),
                content: item.text.clone(),
            });
        }
        if item.summary_eligible
            && let Some(summary) = item.text.strip_prefix("AHRB-COMPACTION-SUMMARY ")
        {
            summary_items = summary_items.saturating_add(1);
            summaries.extend(
                summary
                    .split_whitespace()
                    .filter(|token| token.starts_with("AHRB-HISTORY-"))
                    .map(str::to_owned),
            );
        } else if item.ordinary_history_eligible {
            ordinary.extend(
                item.text
                    .split_whitespace()
                    .filter(|token| token.starts_with("AHRB-HISTORY-"))
                    .map(str::to_owned),
            );
        }
    }
    let effects = pairs
        .iter()
        .flat_map(|pair| {
            marker_tokens(&pair.canonical_arguments, "AHRB-COMMITTED-EFFECT-")
                .into_iter()
                .chain(marker_tokens(
                    &pair.result_content,
                    "AHRB-COMMITTED-EFFECT-",
                ))
        })
        .collect();
    Ok(ContextMarkerEvidence {
        ordinary,
        effects,
        goals,
        summaries,
        summary_items,
    })
}

fn normalized_instruction_tool_projection(dialect: &str, canonical: &Value) -> Result<Value> {
    let is_compaction_summary_content = |content: &Value| {
        context_content_fragments(content)
            .iter()
            .any(|text| text.starts_with("AHRB-COMPACTION-SUMMARY "))
    };
    let without_compaction_summary = |value: &Value| match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .filter(|item| !is_compaction_summary_content(item))
                .cloned()
                .collect(),
        ),
        other if is_compaction_summary_content(other) => Value::Null,
        other => other.clone(),
    };
    let projection = match dialect {
        "openai-chat-completions" => {
            let instructions = canonical
                .get("messages")
                .and_then(Value::as_array)
                .map(|messages| {
                    messages
                        .iter()
                        .filter(|message| {
                            matches!(
                                message.get("role").and_then(Value::as_str),
                                Some("system" | "developer")
                            ) && !is_compaction_summary_content(
                                message.get("content").unwrap_or(&Value::Null),
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            json!({"instructions":instructions,"tools":canonical.get("tools").cloned().unwrap_or_else(|| json!([]))})
        }
        "openai-responses" => {
            let instruction_items = canonical
                .get("input")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            matches!(
                                item.get("role").and_then(Value::as_str),
                                Some("system" | "developer")
                            ) && !is_compaction_summary_content(
                                item.get("content").unwrap_or(&Value::Null),
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            json!({"instructions":without_compaction_summary(canonical.get("instructions").unwrap_or(&Value::Null)),"instruction_items":instruction_items,"tools":canonical.get("tools").cloned().unwrap_or_else(|| json!([]))})
        }
        "anthropic-messages" => json!({
            "system":without_compaction_summary(canonical.get("system").unwrap_or(&Value::Null)),
            "tools":canonical.get("tools").cloned().unwrap_or_else(|| json!([])),
        }),
        other => {
            return Err(AhrbError::Protocol(format!(
                "row-51 cannot project dialect-native instructions for {other:?}"
            )));
        }
    };
    Ok(crate::fake_model::canonicalize_json(&projection))
}

fn normalized_candidate_hash(
    dialect: &str,
    canonical: &Value,
    normalization: &NormalizationContext,
) -> Result<String> {
    let normalized = normalize_canonical_request(dialect, canonical, normalization);
    let bytes = serde_json::to_vec(&normalized)?;
    let mut hasher = Sha256::new();
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod row51_context_evidence_tests {
    use super::*;

    fn chat_faulting_history(markers: &[String]) -> Value {
        json!({
            "messages": markers
                .iter()
                .map(|marker| json!({"role":"user","content":marker}))
                .collect::<Vec<_>>()
        })
    }

    fn chat_compacted_history(
        markers: &[String],
        summary_role: &str,
        summary_markers: &[String],
    ) -> Value {
        let retain_from = markers.len().saturating_sub(4);
        let mut messages = vec![json!({
            "role":summary_role,
            "content":format!("AHRB-COMPACTION-SUMMARY {}", summary_markers.join(" ")),
        })];
        messages.extend(
            markers[retain_from..]
                .iter()
                .map(|marker| json!({"role":"user","content":marker})),
        );
        messages.push(json!({"role":"user","content":"AHRB-GOAL-MARKER recover"}));
        json!({"messages":messages})
    }

    fn chat_history_validation(
        faulting: &Value,
        compacted: &Value,
        expected: &[String],
    ) -> Result<ContextHistoryValidation> {
        let before = context_marker_evidence("openai-chat-completions", faulting, &[])?;
        let after = context_marker_evidence("openai-chat-completions", compacted, &[])?;
        Ok(validate_context_history(&before, &after, expected))
    }

    #[test]
    fn chat_pair_extraction_preserves_duplicate_ids_content_and_call_order() -> Result<()> {
        let canonical = json!({
            "messages": [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "duplicate-id",
                            "type": "function",
                            "function": {
                                "name": "first_tool",
                                "arguments": "{\"z\":2,\"a\":1}"
                            }
                        },
                        {
                            "id": "duplicate-id",
                            "type": "function",
                            "function": {
                                "name": "second_tool",
                                "arguments": "{\"ordinal\":2}"
                            }
                        }
                    ]
                },
                {"role":"tool","tool_call_id":"duplicate-id","content":"first result"},
                {"role":"tool","tool_call_id":"duplicate-id","content":"second result"}
            ]
        });
        let evidence = context_tool_pair_evidence("openai-chat-completions", &canonical)?;
        assert_eq!(evidence.orphan_calls, 0);
        assert_eq!(evidence.orphan_results, 0);
        assert_eq!(
            evidence.pairs,
            vec![
                ContextToolPairRecord {
                    call_id: "duplicate-id".to_owned(),
                    function_name: "first_tool".to_owned(),
                    canonical_arguments: json!({"a":1,"z":2}),
                    result_content: Value::String("first result".to_owned()),
                },
                ContextToolPairRecord {
                    call_id: "duplicate-id".to_owned(),
                    function_name: "second_tool".to_owned(),
                    canonical_arguments: json!({"ordinal":2}),
                    result_content: Value::String("second result".to_owned()),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn responses_pair_extraction_uses_native_items() -> Result<()> {
        let canonical = json!({
            "input": [
                {"type":"function_call","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"x\"}"},
                {"type":"function_call_output","call_id":"call-1","output":{"ok":true}}
            ]
        });
        let evidence = context_tool_pair_evidence("openai-responses", &canonical)?;
        assert_eq!(evidence.pairs.len(), 1);
        assert_eq!(evidence.pairs[0].function_name, "lookup");
        assert_eq!(evidence.pairs[0].canonical_arguments, json!({"q":"x"}));
        assert_eq!(evidence.pairs[0].result_content, json!({"ok":true}));
        Ok(())
    }

    #[test]
    fn synthetic_compaction_summary_does_not_change_instruction_projection() -> Result<()> {
        let faulting = json!({
            "messages": [{"role":"system","content":"retain this instruction"}],
            "tools": [{"type":"function","function":{"name":"write_fixture"}}]
        });
        let compacted = json!({
            "messages": [
                {"role":"system","content":"retain this instruction"},
                {"role":"system","content":"AHRB-COMPACTION-SUMMARY AHRB-HISTORY-0001"}
            ],
            "tools": [{"type":"function","function":{"name":"write_fixture"}}]
        });
        assert_eq!(
            normalized_instruction_tool_projection("openai-chat-completions", &faulting)?,
            normalized_instruction_tool_projection("openai-chat-completions", &compacted)?,
        );
        Ok(())
    }

    #[test]
    fn context_summary_requires_a_dialect_native_system_or_developer_role() -> Result<()> {
        let expected = expected_context_history_markers(16);
        let faulting = chat_faulting_history(&expected);
        let omitted = &expected[..12];
        for wrong_role in ["user", "assistant", "tool"] {
            let compacted = chat_compacted_history(&expected, wrong_role, omitted);
            let validation = chat_history_validation(&faulting, &compacted, &expected)?;
            assert!(!validation.omitted_summarized_exactly);
        }
        let valid = chat_compacted_history(&expected, "developer", omitted);
        let validation = chat_history_validation(&faulting, &valid, &expected)?;
        assert!(validation.retained_latest_four_exactly);
        assert!(validation.omitted_summarized_exactly);
        Ok(())
    }

    #[test]
    fn context_summary_rejects_a_duplicate_marker() -> Result<()> {
        let expected = expected_context_history_markers(16);
        let faulting = chat_faulting_history(&expected);
        let mut duplicated = expected[..12].to_vec();
        duplicated.insert(1, duplicated[0].clone());
        let compacted = chat_compacted_history(&expected, "system", &duplicated);
        assert!(
            !chat_history_validation(&faulting, &compacted, &expected)?.omitted_summarized_exactly
        );
        Ok(())
    }

    #[test]
    fn context_summary_rejects_reordered_markers() -> Result<()> {
        let expected = expected_context_history_markers(16);
        let faulting = chat_faulting_history(&expected);
        let mut reordered = expected[..12].to_vec();
        reordered.swap(0, 1);
        let compacted = chat_compacted_history(&expected, "system", &reordered);
        assert!(
            !chat_history_validation(&faulting, &compacted, &expected)?.omitted_summarized_exactly
        );
        Ok(())
    }

    #[test]
    fn context_history_rejects_a_bad_faulting_digest() -> Result<()> {
        let expected = expected_context_history_markers(16);
        let mut bad = expected.clone();
        bad[7] = "AHRB-HISTORY-0008-not-the-expected-digest".to_owned();
        let faulting = chat_faulting_history(&bad);
        let compacted = chat_compacted_history(&expected, "system", &expected[..12]);
        let validation = chat_history_validation(&faulting, &compacted, &expected)?;
        assert!(!validation.retained_latest_four_exactly);
        assert!(!validation.omitted_summarized_exactly);
        Ok(())
    }
}

async fn collect_context_recovery_repetition(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
    repetition: u32,
    ordinary_turns: u32,
    tool_pairs: u32,
    window_tokens: u64,
) -> Result<ContextRecoveryTrials> {
    let profile_root = run_profile_root.join(format!("derived-row51-r{repetition}"));
    prepare_profile(manifest, &profile_root).map_err(|error| {
        AhrbError::Protocol(format!("prepare row-51 repetition {repetition}: {error}"))
    })?;
    let workflow = context_recovery_workflow(
        manifest,
        &profile_root,
        ordinary_turns,
        tool_pairs,
        window_tokens,
    )?;
    let engine = Arc::new(FakeModelEngine::with_request_roles_and_context_window(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
        Some(window_tokens),
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let normalization_model_environment = model_environment.clone();
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row51-r{repetition}-{}",
        &manifest_hash[..16],
        std::process::id()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential.clone());
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    let context_argv =
        apply_context_window_surface(manifest, window_tokens, &mut variables, &mut environment)?;
    write_generated_files(manifest, &variables, &profile_root)?;
    let mut command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    command.extend(context_argv);
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_turn_timeout(manifest),
    )?;
    driver.start().await?;
    driver.await_readiness().await?;
    let session = driver.create_session("ahrb-row51-context-limit").await?;
    let expected_session_hash = stable_evidence_hash(&session.0);
    let scenario = "ahrb-row51-context-limit";
    let actor = "r51-context";
    let timeout = outer_turn_timeout(manifest);
    let mut events = Vec::new();
    let mut after = None;
    for turn in 1..=ordinary_turns {
        let checkpoint = format!("history-{turn:04}");
        let digest = Sha256::digest(format!("ahrb-history-{turn}").as_bytes());
        let prompt = format!(
            "AHRB-HISTORY-{turn:04}-{digest:x} {}",
            route_marker(scenario, actor, &checkpoint)
        );
        driver
            .submit(&session, &prompt, &format!("row-51-history-{turn:04}"))
            .await?;
        let suffix = collect_session_terminal(&mut driver, &session, after, timeout).await?;
        after = suffix
            .iter()
            .map(|event| Cursor(event.cursor))
            .max()
            .or(after);
        events.extend(suffix);
    }
    let durable_tool_calls = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolCall)
        .count() as u32;
    let durable_tool_results = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolResult)
        .count() as u32;
    if durable_tool_calls != tool_pairs || durable_tool_results != tool_pairs {
        return Err(AhrbError::Protocol(format!(
            "row-51 growing session committed {durable_tool_calls}/{durable_tool_results} tool calls/results; expected {tool_pairs}/{tool_pairs}"
        )));
    }
    let committed_pairs = committed_context_tool_pairs(&events)?;
    let public_turn_start_ns = monotonic_timestamp_ns();
    let recovery_prompt = format!(
        "ahrb-row51 deterministic context recovery {}",
        route_marker(scenario, actor, "recover")
    );
    driver
        .submit(&session, &recovery_prompt, "row-51-recover")
        .await?;
    let recovery_events = collect_session_terminal(&mut driver, &session, after, timeout).await?;
    let terminal_received_ns = monotonic_timestamp_ns();
    let structural_terminal_count = recovery_events
        .iter()
        .filter(|event| is_terminal(&event.event))
        .count() as u32;
    let terminal_success = structural_terminal_count == 1
        && recovery_events
            .iter()
            .any(|event| event.event == EventVocab::TerminalSuccess);
    let same_session_identity = recovery_events
        .iter()
        .all(|event| stable_evidence_hash(&event.session_id) == expected_session_hash);
    events.extend(recovery_events);
    if manifest.transport.kind == TransportKind::Exec || !manifest.sessions.close_delete.is_empty()
    {
        driver.close(&session).await?;
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    let requests = engine.request_records().await;
    let context_error_records = requests
        .iter()
        .filter(|record| {
            record.received_ns >= public_turn_start_ns
                && record.received_ns <= terminal_received_ns
                && record.response_status == Some(400)
        })
        .collect::<Vec<_>>();
    let faulting = context_error_records
        .iter()
        .min_by_key(|record| record.received_ns)
        .copied()
        .ok_or_else(|| {
            let observed = requests
                .iter()
                .filter(|record| {
                    record.received_ns >= public_turn_start_ns
                        && record.received_ns <= terminal_received_ns
                })
                .map(|record| {
                    format!(
                        "{}:{}:{:?}:{:?}:{}",
                        record.request.checkpoint,
                        record.role,
                        record.response_status,
                        record.input_tokens,
                        record.body_bytes,
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            AhrbError::Protocol(format!(
                "row-51 provider observed no context error; public-window requests [{observed}]"
            ))
        })?;
    let error_final_byte_ns = faulting.response_last_frame_yield_ns.ok_or_else(|| {
        AhrbError::Protocol("row-51 context error lacks its final-byte boundary".to_owned())
    })?;
    let accepted = requests
        .iter()
        .filter(|record| {
            record.received_ns > error_final_byte_ns
                && record.received_ns <= terminal_received_ns
                && record.role == "primary"
                && record
                    .input_tokens
                    .is_some_and(|tokens| tokens <= window_tokens)
                && record.body_bytes <= window_tokens.saturating_mul(8)
                && record
                    .response_status
                    .is_some_and(|status| (200..300).contains(&status))
        })
        .min_by_key(|record| record.received_ns)
        .ok_or_else(|| {
            AhrbError::Protocol("row-51 provider observed no accepted compacted request".to_owned())
        })?;
    if accepted.request.dialect != faulting.request.dialect {
        return Err(AhrbError::Protocol(
            "row-51 compacted candidate changed provider dialect".to_owned(),
        ));
    }
    let faulting_pairs =
        context_tool_pair_evidence(&faulting.request.dialect, &faulting.request.canonical)?;
    let accepted_pairs =
        context_tool_pair_evidence(&accepted.request.dialect, &accepted.request.canonical)?;
    let before_markers = context_marker_evidence(
        &faulting.request.dialect,
        &faulting.request.canonical,
        &faulting_pairs.pairs,
    )?;
    let after_markers = context_marker_evidence(
        &accepted.request.dialect,
        &accepted.request.canonical,
        &accepted_pairs.pairs,
    )?;
    let expected_history = expected_context_history_markers(ordinary_turns);
    let history_validation =
        validate_context_history(&before_markers, &after_markers, &expected_history);
    let omitted_markers = history_validation.omitted;
    let required_markers_retained = history_validation.retained_latest_four_exactly
        && before_markers.effects == after_markers.effects;
    let omitted_markers_summarized_exactly = history_validation.omitted_summarized_exactly;
    let extra_requests = requests
        .iter()
        .filter(|record| {
            record.received_ns > error_final_byte_ns && record.received_ns <= terminal_received_ns
        })
        .count() as u32;
    let mut socket_paths = Vec::new();
    let mut temporary_paths = Vec::new();
    for (key, value) in &normalization_model_environment {
        if key.contains("SOCKET") {
            socket_paths.push(value.clone());
        } else if value.starts_with(profile_root.to_string_lossy().as_ref()) {
            temporary_paths.push(value.clone());
        }
    }
    let mut run_markers = vec![route_marker(scenario, actor, "recover")];
    for turn in 1..=ordinary_turns {
        run_markers.push(route_marker(scenario, actor, &format!("history-{turn:04}")));
        if turn <= tool_pairs {
            run_markers.push(route_marker(
                scenario,
                actor,
                &format!("history-{turn:04}-terminal"),
            ));
        }
    }
    let normalization = NormalizationContext {
        credential,
        profile_paths: vec![profile_root.to_string_lossy().into_owned()],
        workspace_paths: workflow
            .actors
            .values()
            .map(|actor| actor.workspace.clone())
            .collect(),
        temporary_paths,
        socket_paths,
        run_markers,
        execution_id: format!("ahrb-row51-execution-{repetition}"),
    };
    let faulting_normalized = normalize_canonical_request(
        &faulting.request.dialect,
        &faulting.request.canonical,
        &normalization,
    );
    let accepted_normalized = normalize_canonical_request(
        &accepted.request.dialect,
        &accepted.request.canonical,
        &normalization,
    );
    let trial = ContextRecoveryTrial {
        repetition,
        window_tokens,
        public_turn_start_ns,
        error_final_byte_ns,
        accepted_request_received_ns: accepted.received_ns,
        terminal_received_ns,
        pre_error_input_tokens: faulting.input_tokens.unwrap_or(0),
        pre_error_body_bytes: faulting.body_bytes,
        accepted_input_tokens: accepted.input_tokens.unwrap_or(u64::MAX),
        accepted_body_bytes: accepted.body_bytes,
        context_errors: context_error_records.len() as u32,
        extra_requests,
        terminal_success,
        structural_terminal_count,
        tool_pairs_before: faulting_pairs.pairs.len() as u32,
        tool_pairs_after: accepted_pairs.pairs.len() as u32,
        committed_tool_pairs: committed_pairs.pairs,
        faulting_tool_pairs: faulting_pairs.pairs,
        accepted_tool_pairs: accepted_pairs.pairs,
        faulting_goal_items: before_markers.goals,
        accepted_goal_items: after_markers.goals,
        committed_orphan_tool_calls: committed_pairs.orphan_calls,
        committed_orphan_tool_results: committed_pairs.orphan_results,
        faulting_orphan_tool_calls: faulting_pairs.orphan_calls,
        faulting_orphan_tool_results: faulting_pairs.orphan_results,
        accepted_orphan_tool_calls: accepted_pairs.orphan_calls,
        accepted_orphan_tool_results: accepted_pairs.orphan_results,
        orphan_tool_calls: committed_pairs
            .orphan_calls
            .saturating_add(faulting_pairs.orphan_calls)
            .saturating_add(accepted_pairs.orphan_calls),
        orphan_tool_results: committed_pairs
            .orphan_results
            .saturating_add(faulting_pairs.orphan_results)
            .saturating_add(accepted_pairs.orphan_results),
        duplicate_effects: after_markers
            .effects
            .len()
            .saturating_sub(after_markers.effects.iter().collect::<BTreeSet<_>>().len())
            as u32,
        same_session_identity,
        instructions_and_tools_unchanged: normalized_instruction_tool_projection(
            &faulting.request.dialect,
            &faulting_normalized,
        )? == normalized_instruction_tool_projection(
            &accepted.request.dialect,
            &accepted_normalized,
        )?,
        required_markers_retained,
        omitted_markers_summarized_exactly,
        compacted_request_hash: normalized_candidate_hash(
            &accepted.request.dialect,
            &accepted.request.canonical,
            &normalization,
        )?,
        retained_markers: after_markers.ordinary,
        omitted_markers,
        summary_markers: after_markers.summaries,
    };
    Ok(ContextRecoveryTrials {
        events,
        requests,
        trials: vec![trial],
    })
}

async fn collect_context_recovery_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<ContextRecoveryTrials> {
    let (ordinary_turns, tool_pairs, window_tokens, repetitions) = match profile {
        Profile::Quick => (16_u32, 4_u32, 4_096_u64, 3_u32),
        Profile::Cert => (128_u32, 32_u32, 16_384_u64, 7_u32),
    };
    let mut combined = ContextRecoveryTrials {
        events: Vec::new(),
        requests: Vec::new(),
        trials: Vec::new(),
    };
    for repetition in 1..=repetitions {
        let trial = collect_context_recovery_repetition(
            manifest,
            run_profile_root,
            manifest_hash,
            repetition,
            ordinary_turns,
            tool_pairs,
            window_tokens,
        )
        .await?;
        combined.events.extend(trial.events);
        combined.requests.extend(trial.requests);
        combined.trials.extend(trial.trials);
    }
    Ok(combined)
}

fn resume_latency_workflow(profile_root: &Path, length: u32, repetition: u32) -> Workflow {
    let scenario = format!("ahrb-row52-l{length}-r{repetition}");
    let actor = format!("r52-l{length}-r{repetition}");
    let mut responses = (1..=length)
        .map(|turn| ScriptedResponse {
            scenario: scenario.clone(),
            actor: actor.clone(),
            checkpoint: format!("history-{turn:04}"),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        })
        .collect::<Vec<_>>();
    responses.push(ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: "resume".to_owned(),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: None,
    });
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor,
                parent: None,
                prompt: "AHRB row 52 resume latency".to_owned(),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses,
    }
}

async fn collect_resume_latency_point(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
    length: u32,
    repetition: u32,
) -> Result<ResumeLatencyTrials> {
    let profile_root = run_profile_root.join(format!("derived-row52-l{length}-r{repetition}"));
    prepare_profile(manifest, &profile_root).map_err(|error| {
        AhrbError::Protocol(format!("prepare row-52 L={length} r={repetition}: {error}"))
    })?;
    let workflow = resume_latency_workflow(&profile_root, length, repetition);
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) = start_model(
        Arc::clone(&engine),
        &workflow,
        &profile_root,
        false,
        &manifest.fake_model.base_url_env,
    )
    .await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row52-l{length}-r{repetition}-{}",
        &manifest_hash[..16],
        std::process::id()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential);
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
        outer_turn_timeout(manifest),
    )?;
    driver.start().await?;
    driver.await_readiness().await?;
    let session = driver.create_session(&workflow.scenario).await?;
    let expected_session_id_hash = stable_evidence_hash(&session.0);
    let actor = format!("r52-l{length}-r{repetition}");
    let timeout = outer_turn_timeout(manifest);
    let mut events = Vec::new();
    let mut after = None;
    for turn in 1..=length {
        let checkpoint = format!("history-{turn:04}");
        let prompt = format!(
            "AHRB resume history turn {turn} {}",
            route_marker(&workflow.scenario, &actor, &checkpoint)
        );
        driver
            .submit(&session, &prompt, &format!("row-52-history-{turn:04}"))
            .await?;
        let suffix = collect_session_terminal(&mut driver, &session, after, timeout).await?;
        after = suffix
            .iter()
            .map(|event| Cursor(event.cursor))
            .max()
            .or(after);
        events.extend(suffix);
    }
    let detached_cursor = after.ok_or_else(|| {
        AhrbError::Protocol(format!("row-52 L={length} produced no durable cursor"))
    })?;
    let previous_boundary_count = driver.completed_turn_boundaries().len();
    let reattach_submit_start_ns = monotonic_timestamp_ns();
    driver.resume(&session).await?;
    let prompt = format!(
        "AHRB resume continuation {}",
        route_marker(&workflow.scenario, &actor, "resume")
    );
    driver.submit(&session, &prompt, "row-52-resume").await?;
    let resumed =
        collect_session_terminal(&mut driver, &session, Some(detached_cursor), timeout).await?;
    let resume_start_ns = if per_invocation_topology(manifest) {
        await_completed_turn_boundary(
            &mut driver,
            &session,
            Some(detached_cursor),
            previous_boundary_count,
            timeout,
        )
        .await?
        .launch_ns
    } else {
        reattach_submit_start_ns
    };
    let first_cursor = resumed
        .iter()
        .map(|event| event.cursor)
        .min()
        .ok_or_else(|| {
            AhrbError::Protocol("row-52 resumed turn emitted no suffix events".to_owned())
        })?;
    let session_id_hash = resumed
        .first()
        .map(|event| stable_evidence_hash(&event.session_id))
        .unwrap_or_default();
    if resumed.iter().any(|event| event.session_id != session.0) {
        return Err(AhrbError::Protocol(
            "row-52 resumed suffix changed session identity".to_owned(),
        ));
    }
    events.extend(resumed);
    if manifest.transport.kind == TransportKind::Exec || !manifest.sessions.close_delete.is_empty()
    {
        driver.close(&session).await?;
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    let requests = engine.request_records().await;
    let first_request_ns = requests
        .iter()
        .filter(|record| record.request.checkpoint == "resume")
        .map(|record| record.received_ns)
        .min()
        .ok_or_else(|| AhrbError::Protocol("row-52 provider saw no resumed request".to_owned()))?;
    Ok(ResumeLatencyTrials {
        events,
        requests,
        points: vec![ResumeLatencyPoint {
            length,
            repetition,
            resume_start_ns,
            first_request_ns,
            latency_ms: first_request_ns.saturating_sub(resume_start_ns) as f64 / 1_000_000.0,
            session_id_hash,
            expected_session_id_hash,
            cursor: first_cursor,
            expected_cursor: detached_cursor.0.saturating_add(1),
        }],
    })
}

async fn collect_resume_latency_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<ResumeLatencyTrials> {
    let (lengths, repetitions): (&[u32], u32) = match profile {
        Profile::Quick => (&[1, 10, 50], 3),
        Profile::Cert => (&[1, 50, 100, 250, 500], 7),
    };
    let mut combined = ResumeLatencyTrials {
        events: Vec::new(),
        requests: Vec::new(),
        points: Vec::new(),
    };
    for length in lengths {
        for repetition in 1..=repetitions {
            let trial = collect_resume_latency_point(
                manifest,
                run_profile_root,
                manifest_hash,
                *length,
                repetition,
            )
            .await?;
            combined.events.extend(trial.events);
            combined.requests.extend(trial.requests);
            combined.points.extend(trial.points);
        }
    }
    Ok(combined)
}

fn time_to_first_model_request_workflow(profile_root: &Path, repetition: u32) -> Workflow {
    let scenario = format!("ahrb-row45-r{repetition}");
    let actor = format!("r45-r{repetition}");
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB cold time to first model request {}",
                    route_marker(&scenario, &actor, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        }],
    }
}

async fn collect_time_to_first_model_request_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<TimeToFirstModelRequestTrials> {
    let repetitions = ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).repetitions;
    let per_invocation = per_invocation_topology(manifest);
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    let mut turns = Vec::with_capacity(repetitions as usize);
    let mut first_request_roles = Vec::with_capacity(repetitions as usize);
    for repetition in 1..=repetitions {
        let profile_root = run_profile_root.join(format!("derived-row45-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-45 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let workflow = time_to_first_model_request_workflow(&profile_root, repetition);
        let actor_name = format!("r45-r{repetition}");
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("row-45 repetition {repetition} actor disappeared"))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row45-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
            outer_turn_timeout(manifest),
        )?;
        let daemon_launch_ns = (!per_invocation).then(monotonic_timestamp_ns);
        driver.start().await?;
        let session = driver
            .create_session(&format!("{}:row45", workflow.scenario))
            .await?;
        let session_id_hash = stable_evidence_hash(&session.0);
        let previous_boundary_count = driver.completed_turn_boundaries().len();
        let submit_ns = monotonic_timestamp_ns();
        driver
            .submit(&session, &actor.prompt, &format!("row-45-r{repetition}"))
            .await?;
        let suffix = collect_session_terminal_with_poll(
            &mut driver,
            &session,
            None,
            outer_turn_timeout(manifest),
            Duration::from_millis(1),
        )
        .await?;
        let terminal_ns = monotonic_timestamp_ns();
        let terminal_count = suffix
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count();
        if terminal_count != 1 {
            return Err(AhrbError::Protocol(format!(
                "row-45 repetition {repetition} observed {terminal_count} terminal events"
            )));
        }
        all_events.extend(suffix);
        let boundary = if per_invocation {
            Some(
                await_completed_turn_boundary(
                    &mut driver,
                    &session,
                    None,
                    previous_boundary_count,
                    outer_turn_timeout(manifest),
                )
                .await?,
            )
        } else {
            None
        };
        let requests = engine.request_records().await;
        let first_request = requests
            .iter()
            .min_by(|left, right| {
                (
                    left.received_ns,
                    &left.request.actor,
                    &left.request.checkpoint,
                    left.semantic_ordinal,
                    left.attempt,
                )
                    .cmp(&(
                        right.received_ns,
                        &right.request.actor,
                        &right.request.checkpoint,
                        right.semantic_ordinal,
                        right.attempt,
                    ))
            })
            .ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-45 repetition {repetition} produced no model request"
                ))
            })?;
        let first_role = if first_request.role == "primary" {
            "primary".to_owned()
        } else {
            first_request
                .side_channel_kind
                .clone()
                .unwrap_or_else(|| first_request.role.clone())
        };
        let launch_ns = boundary
            .as_ref()
            .map(|value| value.launch_ns)
            .or(daemon_launch_ns);
        let exit_ns = boundary.as_ref().map(|value| value.exit_ns);
        let turn_wall_ns = boundary.as_ref().map_or_else(
            || terminal_ns.checked_sub(submit_ns),
            |value| value.exit_ns.checked_sub(value.launch_ns),
        );
        turns.push(TurnObservation {
            repetition,
            turn_index: 1,
            actor: actor_name,
            session_id_hash,
            phase: "time-to-first-model-request".to_owned(),
            launch_ns,
            submit_ns: Some(submit_ns),
            first_model_request_ns: Some(first_request.received_ns),
            terminal_ns: Some(terminal_ns),
            exit_ns,
            turn_wall_ns,
        });
        first_request_roles.push(first_role);
        all_requests.extend(requests);
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        driver.shutdown().await?;
        server.shutdown().await?;
    }
    all_requests.sort_by(|left, right| {
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
    Ok(TimeToFirstModelRequestTrials {
        events: all_events,
        requests: all_requests,
        turns,
        first_request_roles,
    })
}

struct MemoryTimeSamplerCollection {
    samples: Vec<MemoryTimeIntegralSample>,
}

struct MemoryTimeSamplerThread {
    roots: Arc<Mutex<Vec<u32>>>,
    samples: Arc<Mutex<Vec<MemoryTimeIntegralSample>>>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    join: Option<std::thread::JoinHandle<Result<Vec<String>>>>,
}

impl MemoryTimeSamplerThread {
    fn set_roots(&self, roots: &[u32]) -> Result<()> {
        let mut roots = roots.to_vec();
        roots.sort_unstable();
        roots.dedup();
        let mut current = self.roots.lock().map_err(|_| {
            AhrbError::Protocol("row-46 sampler roots lock was poisoned".to_owned())
        })?;
        *current = roots;
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<MemoryTimeIntegralSample>> {
        self.samples
            .lock()
            .map(|samples| samples.clone())
            .map_err(|_| AhrbError::Protocol("row-46 sampler samples lock was poisoned".to_owned()))
    }

    async fn wait_for_sample_after(
        &self,
        boundary_ns: u64,
        require_owned_process: bool,
        timeout: Duration,
    ) -> Result<()> {
        let started = Instant::now();
        loop {
            let observed = self.snapshot()?.iter().any(|sample| {
                sample.monotonic_ns >= boundary_ns
                    && (!require_owned_process || sample.owned_processes > 0)
            });
            if observed {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err(AhrbError::Timeout(format!(
                    "row-46 sampler did not publish a {}sample after boundary {boundary_ns}",
                    if require_owned_process {
                        "live-process "
                    } else {
                        ""
                    }
                )));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn request_stop(&self) {
        let (lock, wake) = &*self.stop;
        if let Ok(mut stopping) = lock.lock() {
            *stopping = true;
            wake.notify_one();
        }
    }

    fn finish(mut self) -> Result<MemoryTimeSamplerCollection> {
        self.request_stop();
        let _warnings = self
            .join
            .take()
            .ok_or_else(|| AhrbError::Protocol("row-46 sampler join disappeared".to_owned()))?
            .join()
            .map_err(|_| AhrbError::Protocol("row-46 sampler thread panicked".to_owned()))??;
        let samples = self.snapshot()?;
        Ok(MemoryTimeSamplerCollection { samples })
    }
}

impl Drop for MemoryTimeSamplerThread {
    fn drop(&mut self) {
        self.request_stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn collect_memory_time_sample(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    repetition: u32,
) -> Result<(MemoryTimeIntegralSample, Vec<String>)> {
    let wall_started = Instant::now();
    let cpu_started = sampler_thread_cpu_ns()?;
    let tree = sampler.discover(roots)?;
    let sample = sampler.sample(&tree, "memory-time-integral")?;
    let monotonic_ns = monotonic_timestamp_ns();
    let collection_cpu_ns = sampler_thread_cpu_ns()?.saturating_sub(cpu_started);
    let collection_wall_ns = duration_ns(wall_started.elapsed());
    if monotonic_ns == 0 {
        return Err(AhrbError::Protocol(
            "row-46 sampler could not read CLOCK_MONOTONIC".to_owned(),
        ));
    }
    let warnings = sample
        .cpu_accounting_warnings
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((
        MemoryTimeIntegralSample {
            repetition,
            monotonic_ns,
            effective_memory_bytes: effective_sample_bytes(&sample),
            cpu_ns: sample.cpu_ns,
            owned_processes: sample.processes.len() as u64,
            collection_cpu_ns,
            collection_wall_ns,
            cpu_accounting_warnings: warnings.clone(),
        },
        warnings,
    ))
}

fn start_memory_time_sampler(
    repetition: u32,
    initial_roots: Vec<u32>,
    cadence: Duration,
) -> Result<MemoryTimeSamplerThread> {
    let interval = cadence
        .checked_div(4)
        .filter(|interval| !interval.is_zero())
        .unwrap_or(cadence);
    let roots = Arc::new(Mutex::new(initial_roots));
    let samples = Arc::new(Mutex::new(Vec::<MemoryTimeIntegralSample>::new()));
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_roots = Arc::clone(&roots);
    let thread_samples = Arc::clone(&samples);
    let thread_stop = Arc::clone(&stop);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let join = std::thread::Builder::new()
        .name("ahrb-row46-sampler".to_owned())
        .spawn(move || {
            prioritize_counter_thread();
            let mut sampler = platform_sampler();
            let mut warnings = Vec::new();
            let mut deadline = Instant::now();
            let mut first = true;
            loop {
                let roots = thread_roots
                    .lock()
                    .map_err(|_| {
                        AhrbError::Protocol("row-46 sampler roots lock was poisoned".to_owned())
                    })?
                    .clone();
                let collected = collect_memory_time_sample(sampler.as_mut(), &roots, repetition);
                if first {
                    let ready = collected.as_ref().map(|_| ()).map_err(ToString::to_string);
                    let _ = ready_tx.send(ready);
                    first = false;
                }
                let (mut sample, sample_warnings) = collected?;
                {
                    let mut values = thread_samples.lock().map_err(|_| {
                        AhrbError::Protocol("row-46 sampler samples lock was poisoned".to_owned())
                    })?;
                    if let Some(previous) = values.last()
                        && sample.monotonic_ns <= previous.monotonic_ns
                    {
                        sample.monotonic_ns = previous.monotonic_ns.saturating_add(1);
                    }
                    values.push(sample);
                }
                warnings.extend(sample_warnings);
                deadline += interval;
                let (lock, wake) = &*thread_stop;
                let stopping = lock.lock().map_err(|_| {
                    AhrbError::Protocol("row-46 sampler stop lock was poisoned".to_owned())
                })?;
                if *stopping {
                    break;
                }
                let now = Instant::now();
                let stopping = if now >= deadline {
                    stopping
                } else {
                    wake.wait_timeout(stopping, deadline.duration_since(now))
                        .map_err(|_| {
                            AhrbError::Protocol("row-46 sampler stop lock was poisoned".to_owned())
                        })?
                        .0
                };
                if *stopping {
                    break;
                }
                let due = Instant::now();
                while deadline <= due {
                    deadline += interval;
                }
            }
            Ok(warnings)
        })?;
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(MemoryTimeSamplerThread {
            roots,
            samples,
            stop,
            join: Some(join),
        }),
        Ok(Err(detail)) => {
            let _ = join.join();
            Err(AhrbError::Protocol(format!(
                "row-46 initial sampler collection failed: {detail}"
            )))
        }
        Err(_) => {
            let _ = join.join();
            Err(AhrbError::Protocol(
                "row-46 sampler exited before its initial sample".to_owned(),
            ))
        }
    }
}

fn memory_time_integral_workflow(profile_root: &Path, repetition: u32, turns: u32) -> Workflow {
    let scenario = format!("ahrb-row46-r{repetition}");
    let actor = format!("r46-r{repetition}");
    let mut responses = vec![ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: "warmup".to_owned(),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: None,
    }];
    responses.extend((1..=turns).map(|turn| ScriptedResponse {
        scenario: scenario.clone(),
        actor: actor.clone(),
        checkpoint: format!("turn-{turn:03}"),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: None,
    }));
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB memory-time-integral unmeasured warm-up {}",
                    route_marker(&scenario, &actor, "warmup")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses,
    }
}

fn determinism_workflow(profile_root: &Path, manifest: &Manifest) -> Result<Workflow> {
    let scenario = "ahrb-row63-64";
    let direct_actor = "d63-direct";
    let tool_actor = "d63-tool";
    let terminal = route_marker(scenario, tool_actor, "terminal");
    let call = mapped_tool_call(
        manifest,
        "write",
        "determinism-call".to_owned(),
        json!({
            "path": "determinism.txt",
            "content": format!("deterministic{terminal}"),
        }),
    )?;
    Ok(Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.to_owned(),
        actors: BTreeMap::from([
            (
                direct_actor.to_owned(),
                Actor {
                    id: direct_actor.to_owned(),
                    parent: None,
                    prompt: format!(
                        "AHRB deterministic direct terminal {}",
                        route_marker(scenario, direct_actor, "start")
                    ),
                    workspace: profile_root
                        .join("workspaces")
                        .join(direct_actor)
                        .to_string_lossy()
                        .into_owned(),
                },
            ),
            (
                tool_actor.to_owned(),
                Actor {
                    id: tool_actor.to_owned(),
                    parent: None,
                    prompt: format!(
                        "AHRB deterministic tool workflow {}",
                        route_marker(scenario, tool_actor, "start")
                    ),
                    workspace: profile_root
                        .join("workspaces")
                        .join(tool_actor)
                        .to_string_lossy()
                        .into_owned(),
                },
            ),
        ]),
        barriers: BTreeMap::new(),
        responses: vec![
            ScriptedResponse {
                scenario: scenario.to_owned(),
                actor: direct_actor.to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            },
            ScriptedResponse {
                scenario: scenario.to_owned(),
                actor: tool_actor.to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({"tool_calls": [call]}),
                fault: None,
                barrier: None,
            },
            ScriptedResponse {
                scenario: scenario.to_owned(),
                actor: tool_actor.to_owned(),
                checkpoint: "terminal".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            },
        ],
    })
}

async fn collect_determinism_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<DeterminismTrials> {
    let expected_runs = match profile {
        Profile::Quick => 2_u32,
        Profile::Cert => 7_u32,
    };
    let mut runs = Vec::with_capacity(expected_runs as usize);
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    for execution in 1..=expected_runs {
        let profile_root = run_profile_root.join(format!("derived-row63-run-{execution:03}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-63 execution {execution} fresh profile: {error}"
            ))
        })?;
        let workflow = determinism_workflow(&profile_root, manifest)?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row63-run-{execution}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment.clone());
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential.clone());
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        let mut execution_events = Vec::new();
        for actor_name in ["d63-direct", "d63-tool"] {
            let actor = workflow.actors.get(actor_name).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-63 execution {execution} actor {actor_name:?} disappeared"
                ))
            })?;
            let session = driver
                .create_session(&format!("{}:{actor_name}", workflow.scenario))
                .await?;
            driver
                .submit(
                    &session,
                    &actor.prompt,
                    &format!("row-63-run-{execution}-{actor_name}"),
                )
                .await?;
            let events =
                collect_session_terminal(&mut driver, &session, None, outer_turn_timeout(manifest))
                    .await?;
            if events
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count()
                != 1
            {
                return Err(AhrbError::Protocol(format!(
                    "row-63 execution {execution} actor {actor_name} did not terminalize exactly once"
                )));
            }
            execution_events.extend(events);
            if manifest.transport.kind == TransportKind::Exec
                || !manifest.sessions.close_delete.is_empty()
            {
                driver.close(&session).await?;
            }
        }
        driver.shutdown().await?;
        server.shutdown().await?;
        let records = engine.request_records().await;
        let mut socket_paths = Vec::new();
        let mut temporary_paths = Vec::new();
        for (key, value) in &model_environment {
            if key.contains("SOCKET") {
                socket_paths.push(value.clone());
            } else if value.starts_with(profile_root.to_string_lossy().as_ref()) {
                temporary_paths.push(value.clone());
            }
        }
        let workspace_paths = workflow
            .actors
            .values()
            .map(|actor| actor.workspace.clone())
            .collect::<Vec<_>>();
        runs.push(DeterminismRun {
            run: execution,
            records: records.clone(),
            request_collector_complete: true,
            normalization: NormalizationContext {
                credential,
                profile_paths: vec![profile_root.to_string_lossy().into_owned()],
                workspace_paths,
                temporary_paths,
                socket_paths,
                run_markers: vec![
                    route_marker(&workflow.scenario, "d63-direct", "start"),
                    route_marker(&workflow.scenario, "d63-tool", "start"),
                    route_marker(&workflow.scenario, "d63-tool", "terminal"),
                ],
                execution_id: format!("ahrb-row63-execution-{execution}"),
            },
        });
        all_events.extend(execution_events);
        all_requests.extend(records);
    }
    all_requests.sort_by(|left, right| {
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
    Ok(DeterminismTrials {
        events: all_events,
        runs,
        requests: all_requests,
    })
}

#[derive(Clone, Debug)]
struct Row47FileState {
    device_id: u64,
    inode_or_file_id: u64,
    size_bytes: u64,
    sha256: String,
}

fn disk_io_workflow(profile_root: &Path, repetition: u32, turns: u32) -> Workflow {
    let scenario = format!("ahrb-row47-r{repetition}");
    let actor_name = format!("r47-r{repetition}");
    let mut responses = Vec::with_capacity(turns as usize);
    for turn in 1..=turns {
        responses.push(ScriptedResponse {
            scenario: scenario.clone(),
            actor: actor_name.clone(),
            checkpoint: format!("turn-{turn:03}"),
            request_hash: String::new(),
            response: success_value(),
            fault: None,
            barrier: None,
        });
    }
    let actor = Actor {
        id: actor_name.clone(),
        parent: None,
        prompt: format!(
            "AHRB disk IO tiny journaled turn 1 {}",
            route_marker(&scenario, &actor_name, "turn-001")
        ),
        workspace: profile_root
            .join("workspace")
            .to_string_lossy()
            .into_owned(),
    };
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario,
        actors: BTreeMap::from([(actor_name, actor)]),
        barriers: BTreeMap::new(),
        responses,
    }
}

fn row47_render_paths(
    templates: impl IntoIterator<Item = (String, &'static str)>,
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<Vec<(PathBuf, &'static str)>> {
    let mut rendered = BTreeMap::new();
    for (template, category) in templates {
        if template.trim().is_empty() {
            continue;
        }
        let path = PathBuf::from(crate::manifest::render_template(&template, variables)?);
        if !path.starts_with(profile_root) {
            return Err(AhrbError::Validation(format!(
                "row-47 declared path {} escapes fresh profile {}",
                path.display(),
                profile_root.display()
            )));
        }
        rendered.entry(path).or_insert(category);
    }
    Ok(rendered.into_iter().collect())
}

fn row47_hash_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn row47_snapshot_file(
    path: &Path,
    profile_root: &Path,
    repetition: u32,
    boundary: &str,
    category: &str,
    raw: &mut Vec<FilesystemSnapshot>,
) -> Result<Option<Row47FileState>> {
    let before = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !before.file_type().is_file() {
        return Err(AhrbError::Protocol(format!(
            "row-47 snapshot path {} is not a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt as _;
    #[cfg(unix)]
    let before_identity = (before.dev(), before.ino());
    #[cfg(not(unix))]
    let before_identity = (0_u64, 0_u64);
    let digest = row47_hash_file(path)?;
    let after = std::fs::symlink_metadata(path)?;
    #[cfg(unix)]
    let after_identity = (after.dev(), after.ino());
    #[cfg(not(unix))]
    let after_identity = (0_u64, 0_u64);
    if !after.file_type().is_file()
        || before_identity != after_identity
        || before.len() != after.len()
    {
        return Err(AhrbError::Protocol(format!(
            "row-47 snapshot path {} changed identity or size during capture",
            path.display()
        )));
    }
    let relative = path.strip_prefix(profile_root).map_err(|_| {
        AhrbError::Validation(format!(
            "row-47 snapshot path {} is outside {}",
            path.display(),
            profile_root.display()
        ))
    })?;
    let state = Row47FileState {
        device_id: before_identity.0,
        inode_or_file_id: before_identity.1,
        size_bytes: before.len(),
        sha256: digest,
    };
    raw.push(FilesystemSnapshot {
        repetition,
        boundary: boundary.to_owned(),
        category: category.to_owned(),
        path_under_profile: relative.to_string_lossy().into_owned(),
        device_id: state.device_id,
        inode_or_file_id: state.inode_or_file_id,
        size_bytes: state.size_bytes,
        sha256: state.sha256.clone(),
    });
    Ok(Some(state))
}

fn row47_snapshot_paths(
    paths: &[(PathBuf, &'static str)],
    profile_root: &Path,
    repetition: u32,
    boundary: &str,
    raw: &mut Vec<FilesystemSnapshot>,
) -> Result<BTreeMap<PathBuf, Option<Row47FileState>>> {
    paths
        .iter()
        .map(|(path, category)| {
            row47_snapshot_file(path, profile_root, repetition, boundary, category, raw)
                .map(|snapshot| (path.clone(), snapshot))
        })
        .collect()
}

fn row47_file_growth(before: Option<&Row47FileState>, after: Option<&Row47FileState>) -> u64 {
    match (before, after) {
        (Some(before), Some(after))
            if before.device_id == after.device_id
                && before.inode_or_file_id == after.inode_or_file_id =>
        {
            after.size_bytes.saturating_sub(before.size_bytes)
        }
        _ => 0,
    }
}

fn row47_recursive_regular_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut entries =
            std::fs::read_dir(&directory)?.collect::<std::result::Result<Vec<_>, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::path);
        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() {
                files.push(path);
            } else if file_type.is_symlink() {
                return Err(AhrbError::Protocol(format!(
                    "row-47 isolated-root audit found symlink {}",
                    path.display()
                )));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn row47_looks_like_log(path: &Path) -> bool {
    if path.extension().and_then(|value| value.to_str()) == Some("log") {
        return true;
    }
    path.components().any(|component| {
        let value = component.as_os_str().to_string_lossy().to_ascii_lowercase();
        value == "log" || value == "logs" || value.ends_with(".log") || value.contains(".log.")
    })
}

fn row47_verify_no_log(
    profile_root: &Path,
    repetition: u32,
    journal_paths: &BTreeSet<PathBuf>,
    raw: &mut Vec<FilesystemSnapshot>,
) -> Result<()> {
    let boundary = format!("row47-r{repetition}-verified-no-log-audit");
    for path in row47_recursive_regular_files(profile_root)? {
        let _snapshot = row47_snapshot_file(
            &path,
            profile_root,
            repetition,
            &boundary,
            "isolated-root-audit",
            raw,
        )?;
        if !journal_paths.contains(&path) && row47_looks_like_log(&path) {
            return Err(AhrbError::Protocol(format!(
                "resources.log_paths=[] contradicted by isolated-root log artifact {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn row47_terminal_record_count(path: &Path, manifest: &Manifest) -> Result<u64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let records = match manifest.events.framing.as_str() {
        "jsonl" | "json-seq" => {
            let mut records = Vec::new();
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if line.last() != Some(&b'\n') {
                    // The writer can be between bytes while this out-of-band
                    // observer polls. A complete final record is required
                    // before it can establish the structured boundary.
                    break;
                }
                let line = &line[..line.len().saturating_sub(1)];
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                records.push(serde_json::from_slice::<Value>(line).map_err(|error| {
                    AhrbError::Protocol(format!(
                        "row-47 event journal {} contains an invalid complete record: {error}",
                        path.display()
                    ))
                })?);
            }
            records
        }
        "json" => {
            if bytes.iter().all(u8::is_ascii_whitespace) {
                Vec::new()
            } else {
                let value: Value = serde_json::from_slice(&bytes)?;
                match value {
                    Value::Array(records) => records,
                    record => vec![record],
                }
            }
        }
        other => {
            return Err(AhrbError::Validation(format!(
                "row-47 pre-reap terminal observation requires jsonl, json-seq, or json framing, not {other:?}"
            )));
        }
    };
    let terminal_rules = manifest.events.rules.iter().filter(|rule| {
        matches!(
            rule.event.as_str(),
            "terminal-success" | "terminal-failure" | "terminal-cancelled" | "terminal-timeout"
        )
    });
    let terminal_rules = terminal_rules.collect::<Vec<_>>();
    let mut count = 0_u64;
    for record in records {
        let event_type = (!manifest.events.type_pointer.is_empty())
            .then(|| record.pointer(&manifest.events.type_pointer))
            .flatten()
            .and_then(Value::as_str)
            .or_else(|| record.get("type").and_then(Value::as_str))
            .or_else(|| record.get("event").and_then(Value::as_str));
        if terminal_rules
            .iter()
            .any(|rule| event_type == Some(rule.matches.as_str()) && rule_matches(&record, rule))
        {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

async fn row47_wait_for_pre_reap_terminal(
    path: &Path,
    manifest: &Manifest,
    previous_count: u64,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        if row47_terminal_record_count(path, manifest)? > previous_count {
            return Ok(());
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "row-47 event journal {} did not expose a new structured terminal before the per-invocation deadline",
                path.display()
            )));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn collect_disk_io_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<DiskIoTrials> {
    let log_evidence = match manifest.resources.log_paths.as_ref() {
        None => LogEvidenceKind::Omitted,
        Some(paths) if paths.is_empty() => LogEvidenceKind::VerifiedNoLog,
        Some(_) => LogEvidenceKind::DeclaredPaths,
    };
    if log_evidence == LogEvidenceKind::Omitted {
        return Ok(DiskIoTrials {
            events: Vec::new(),
            requests: Vec::new(),
            turns: Vec::new(),
            counter_snapshots: Vec::new(),
            filesystem_snapshots: Vec::new(),
            log_evidence,
        });
    }
    let repetitions = match profile {
        Profile::Quick => 3_u32,
        Profile::Cert => 7_u32,
    };
    let turns_per_repetition = match profile {
        Profile::Quick => 20_u32,
        Profile::Cert => 100_u32,
    };
    let per_invocation = per_invocation_topology(manifest);
    let mut trials = DiskIoTrials {
        events: Vec::new(),
        requests: Vec::new(),
        turns: Vec::new(),
        counter_snapshots: Vec::new(),
        filesystem_snapshots: Vec::new(),
        log_evidence,
    };
    for repetition in 1..=repetitions {
        let profile_root = run_profile_root.join(format!("derived-row47-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-47 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let workflow = disk_io_workflow(&profile_root, repetition, turns_per_repetition);
        let actor_name = format!("r47-r{repetition}");
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("row-47 repetition {repetition} actor disappeared"))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row47-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        driver.await_readiness().await?;
        let mut sampler = platform_sampler();
        let daemon_roots = if per_invocation {
            Vec::new()
        } else {
            verified_process_roots(
                manifest,
                sampler.as_mut(),
                driver.owned_pids(),
                driver.daemon_pid(),
            )?
        };
        let session = driver
            .create_session(&format!("{}:row47", workflow.scenario))
            .await?;
        let mut path_variables = variables.clone();
        path_variables.insert("session_id".to_owned(), session.0.clone());
        let pre_reap_event_path = if per_invocation {
            if manifest.events.source != "journal-file" || manifest.events.path.trim().is_empty() {
                return Err(AhrbError::Protocol(
                    "row-47 per-invocation disk accounting requires a file-backed structured terminal boundary before reap"
                        .to_owned(),
                ));
            }
            let mut paths = row47_render_paths(
                [(manifest.events.path.clone(), "journal")],
                &path_variables,
                &profile_root,
            )?;
            paths.pop().map(|(path, _)| path).ok_or_else(|| {
                AhrbError::Protocol(
                    "row-47 per-invocation event journal path rendered empty".to_owned(),
                )
            })?
        } else {
            PathBuf::new()
        };
        let mut journal_templates = Vec::new();
        if !manifest.events.path.trim().is_empty() {
            journal_templates.push((manifest.events.path.clone(), "journal"));
        }
        journal_templates.extend(
            manifest
                .resources
                .journal_paths
                .iter()
                .flatten()
                .cloned()
                .map(|path| (path, "journal")),
        );
        let journal_paths = row47_render_paths(journal_templates, &path_variables, &profile_root)?;
        let log_templates = manifest
            .resources
            .log_paths
            .iter()
            .flatten()
            .cloned()
            .map(|path| (path, "log"));
        let log_paths = row47_render_paths(log_templates, &path_variables, &profile_root)?;
        let all_paths = journal_paths
            .iter()
            .chain(log_paths.iter())
            .cloned()
            .collect::<Vec<_>>();
        let journal_path_set = journal_paths
            .iter()
            .map(|(path, _)| path.clone())
            .collect::<BTreeSet<_>>();
        let mut tracker = TreeDiskTracker::default();
        if !per_invocation {
            let tree = sampler.discover(&daemon_roots)?;
            let observation = sampler.disk_counters(&tree)?;
            let _initial = tracker.observe(&observation)?;
        }
        let mut after = None;
        for turn in 1..=turns_per_repetition {
            let before_boundary = format!("row47-r{repetition}-t{turn:03}-before");
            let after_boundary = format!("row47-r{repetition}-t{turn:03}-after");
            let before_files = row47_snapshot_paths(
                &all_paths,
                &profile_root,
                repetition,
                &before_boundary,
                &mut trials.filesystem_snapshots,
            )?;
            let before_disk = tracker.snapshot();
            let prompt = if turn == 1 {
                actor.prompt.clone()
            } else {
                format!(
                    "AHRB disk IO tiny journaled turn {turn} {}",
                    route_marker(&workflow.scenario, &actor_name, &format!("turn-{turn:03}"))
                )
            };
            let previous_boundary_count = driver.completed_turn_boundaries().len();
            let previous_terminal_count = if per_invocation {
                row47_terminal_record_count(&pre_reap_event_path, manifest)?
            } else {
                0
            };
            driver
                .submit(
                    &session,
                    &prompt,
                    &format!("row-47-r{repetition}-turn-{turn:03}"),
                )
                .await?;
            let invocation_roots = if per_invocation {
                let roots = driver.session_pids(&session);
                if roots.is_empty() {
                    return Err(AhrbError::Protocol(format!(
                        "row-47 repetition {repetition} turn {turn} exposed no process root"
                    )));
                }
                let tree = sampler.discover(&roots)?;
                let observation = sampler.disk_counters(&tree)?;
                let _live = tracker.observe(&observation)?;
                roots
            } else {
                Vec::new()
            };
            if per_invocation {
                row47_wait_for_pre_reap_terminal(
                    &pre_reap_event_path,
                    manifest,
                    previous_terminal_count,
                    outer_turn_timeout(manifest),
                )
                .await?;
                let terminal_tree = sampler.discover(&invocation_roots)?;
                if terminal_tree.members.is_empty() {
                    return Err(AhrbError::Protocol(format!(
                        "row-47 repetition {repetition} turn {turn} process exited before the terminal-before-reap disk sample"
                    )));
                }
                let terminal_observation = sampler.disk_counters(&terminal_tree)?;
                let _terminal_live = tracker.observe(&terminal_observation)?;
                for identity in terminal_tree.members.keys().copied() {
                    tracker.note_structured_terminal(identity)?;
                    if let Some(write_bytes) = terminal_observation
                        .write_bytes_by_identity
                        .get(&identity)
                        .copied()
                    {
                        tracker.record_final_sample_before_reap(identity, write_bytes)?;
                        tracker.retire_after_final_sample(identity)?;
                    }
                }
            }
            let suffix = collect_session_terminal_with_poll(
                &mut driver,
                &session,
                after,
                outer_turn_timeout(manifest),
                Duration::from_millis(2),
            )
            .await?;
            if suffix
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count()
                != 1
            {
                return Err(AhrbError::Protocol(format!(
                    "row-47 repetition {repetition} turn {turn} did not terminalize exactly once"
                )));
            }
            after = suffix
                .iter()
                .map(|event| Cursor(event.cursor))
                .max()
                .or(after);
            trials.events.extend(suffix);
            if !per_invocation {
                let tree = sampler.discover(&daemon_roots)?;
                let observation = sampler.disk_counters(&tree)?;
                let _after = tracker.observe(&observation)?;
            }
            let after_disk = tracker.snapshot();
            let after_files = row47_snapshot_paths(
                &all_paths,
                &profile_root,
                repetition,
                &after_boundary,
                &mut trials.filesystem_snapshots,
            )?;
            let journal_growth_bytes = journal_paths.iter().fold(0_u64, |total, (path, _)| {
                total.saturating_add(row47_file_growth(
                    before_files.get(path).and_then(Option::as_ref),
                    after_files.get(path).and_then(Option::as_ref),
                ))
            });
            let log_growth_bytes = (log_evidence == LogEvidenceKind::DeclaredPaths).then(|| {
                log_paths.iter().fold(0_u64, |total, (path, _)| {
                    total.saturating_add(row47_file_growth(
                        before_files.get(path).and_then(Option::as_ref),
                        after_files.get(path).and_then(Option::as_ref),
                    ))
                })
            });
            let disk_write_delta = before_disk
                .cumulative_write_bytes
                .zip(after_disk.cumulative_write_bytes)
                .and_then(|(before, after)| after.checked_sub(before));
            let disk_write_bytes = disk_write_delta.map_or(0, |value| value);
            let retired_accounting_complete = after_disk.identities.iter().all(|identity| {
                !matches!(
                    identity.status,
                    DiskIdentityStatus::CounterUnavailable
                        | DiskIdentityStatus::MissingWithoutRetirementEvidence
                        | DiskIdentityStatus::TerminalAwaitingFinalSample
                )
            });
            trials.turns.push(DiskTurnEvidence {
                repetition,
                turn,
                disk_write_bytes,
                journal_growth_bytes,
                log_growth_bytes,
                counter_complete: before_disk.counter_complete && after_disk.counter_complete,
                file_identity_complete: true,
                retired_accounting_complete,
            });
            trials.counter_snapshots.push(json!({
                "repetition": repetition,
                "turn": turn,
                "before": before_disk,
                "after": after_disk,
            }));
            if per_invocation {
                let _boundary = await_completed_turn_boundary(
                    &mut driver,
                    &session,
                    after,
                    previous_boundary_count,
                    outer_turn_timeout(manifest),
                )
                .await?;
            }
        }
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        driver.shutdown().await?;
        server.shutdown().await?;
        if log_evidence == LogEvidenceKind::VerifiedNoLog {
            row47_verify_no_log(
                &profile_root,
                repetition,
                &journal_path_set,
                &mut trials.filesystem_snapshots,
            )?;
        }
        trials.requests.extend(engine.request_records().await);
    }
    trials.requests.sort_by(|left, right| {
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
    Ok(trials)
}

async fn collect_memory_time_integral_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<MemoryTimeIntegralTrials> {
    let plan = ResourceTimingPlan::for_profile(ResourceProfile::from(profile));
    let turns_per_repetition = match profile {
        Profile::Quick => 20_u32,
        Profile::Cert => 100_u32,
    };
    #[cfg(target_os = "macos")]
    let counter_cadence = Duration::from_millis(plan.macos_rusage_cadence_ms);
    #[cfg(target_os = "linux")]
    let counter_cadence = Duration::from_millis(plan.linux_smaps_cadence_ms);
    let per_invocation = per_invocation_topology(manifest);
    let mut evidence = MemoryTimeIntegralEvidence {
        sampler_cadence_ns: duration_ns(counter_cadence),
        ..MemoryTimeIntegralEvidence::default()
    };
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    for repetition in 1..=plan.repetitions {
        let profile_root = run_profile_root.join(format!("derived-row46-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-46 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let workflow =
            memory_time_integral_workflow(&profile_root, repetition, turns_per_repetition);
        let actor_name = format!("r46-r{repetition}");
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("row-46 repetition {repetition} actor disappeared"))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row46-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            per_invocation,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        let daemon_roots = if per_invocation {
            Vec::new()
        } else {
            let roots = driver.owned_pids();
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-46 repetition {repetition} daemon exposed no owned root PID"
                )));
            }
            roots
        };
        let sampler = start_memory_time_sampler(repetition, daemon_roots, counter_cadence)?;
        let session = driver
            .create_session(&format!("{}:row46", workflow.scenario))
            .await?;
        let session_id_hash = stable_evidence_hash(&session.0);

        let warmup_boundary_count = driver.completed_turn_boundaries().len();
        driver
            .submit(
                &session,
                &actor.prompt,
                &format!("row-46-r{repetition}-warmup"),
            )
            .await?;
        if per_invocation {
            let roots = driver.session_pids(&session);
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-46 repetition {repetition} warm-up exposed no process root"
                )));
            }
            sampler.set_roots(&roots)?;
            let sampling_boundary = monotonic_timestamp_ns();
            sampler
                .wait_for_sample_after(sampling_boundary, true, outer_turn_timeout(manifest))
                .await?;
            driver.release_invocations().await?;
        }
        let warmup_events =
            collect_session_terminal(&mut driver, &session, None, outer_turn_timeout(manifest))
                .await?;
        let mut after = warmup_events.iter().map(|event| Cursor(event.cursor)).max();
        if per_invocation {
            let boundary = await_completed_turn_boundary(
                &mut driver,
                &session,
                after,
                warmup_boundary_count,
                outer_turn_timeout(manifest),
            )
            .await?;
            sampler.set_roots(&[])?;
            sampler
                .wait_for_sample_after(boundary.exit_ns, false, outer_turn_timeout(manifest))
                .await?;
        }
        if warmup_events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count()
            != 1
        {
            return Err(AhrbError::Protocol(format!(
                "row-46 repetition {repetition} warm-up did not terminalize exactly once"
            )));
        }

        if per_invocation {
            evidence.warm_idle_baseline_bytes.insert(repetition, 0);
        } else {
            let baseline_start_ns = monotonic_timestamp_ns();
            tokio::time::sleep(Duration::from_millis(plan.idle_baseline_ms)).await;
            let baseline_end_ns = monotonic_timestamp_ns();
            sampler
                .wait_for_sample_after(baseline_end_ns, true, outer_turn_timeout(manifest))
                .await?;
            let mut baseline_values = sampler
                .snapshot()?
                .into_iter()
                .filter(|sample| {
                    sample.monotonic_ns >= baseline_start_ns
                        && sample.monotonic_ns <= baseline_end_ns
                        && sample.owned_processes > 0
                })
                .map(|sample| sample.effective_memory_bytes)
                .collect::<Vec<_>>();
            baseline_values.sort_unstable();
            let baseline = match baseline_values.len() {
                0 => {
                    return Err(AhrbError::Protocol(format!(
                        "row-46 repetition {repetition} warm-idle baseline has no samples"
                    )));
                }
                length if length % 2 == 1 => baseline_values[length / 2],
                length => {
                    baseline_values[length / 2 - 1].saturating_add(baseline_values[length / 2]) / 2
                }
            };
            evidence
                .warm_idle_baseline_bytes
                .insert(repetition, baseline);
        }

        for turn in 1..=turns_per_repetition {
            let prompt = format!(
                "AHRB memory-time-integral direct terminal turn {turn} {}",
                route_marker(&workflow.scenario, &actor_name, &format!("turn-{turn:03}"))
            );
            let previous_boundary_count = driver.completed_turn_boundaries().len();
            let submit_ns = monotonic_timestamp_ns();
            driver
                .submit(
                    &session,
                    &prompt,
                    &format!("row-46-r{repetition}-turn-{turn:03}"),
                )
                .await?;
            if per_invocation {
                let roots = driver.session_pids(&session);
                if roots.is_empty() {
                    return Err(AhrbError::Protocol(format!(
                        "row-46 repetition {repetition} turn {turn} exposed no process root"
                    )));
                }
                sampler.set_roots(&roots)?;
                let sampling_boundary = monotonic_timestamp_ns();
                sampler
                    .wait_for_sample_after(sampling_boundary, true, outer_turn_timeout(manifest))
                    .await?;
                driver.release_invocations().await?;
            }
            let suffix = collect_session_terminal_with_poll(
                &mut driver,
                &session,
                after,
                outer_turn_timeout(manifest),
                Duration::from_millis(1),
            )
            .await?;
            let terminal_ns = monotonic_timestamp_ns();
            after = suffix
                .iter()
                .map(|event| Cursor(event.cursor))
                .max()
                .or(after);
            if suffix
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count()
                != 1
            {
                return Err(AhrbError::Protocol(format!(
                    "row-46 repetition {repetition} turn {turn} did not terminalize exactly once"
                )));
            }
            all_events.extend(suffix);
            let boundary = if per_invocation {
                Some(
                    await_completed_turn_boundary(
                        &mut driver,
                        &session,
                        after,
                        previous_boundary_count,
                        outer_turn_timeout(manifest),
                    )
                    .await?,
                )
            } else {
                None
            };
            let (launch_ns, exit_ns, turn_wall_ns, sample_after_ns) =
                if let Some(boundary) = boundary {
                    sampler.set_roots(&[])?;
                    (
                        Some(boundary.launch_ns),
                        Some(boundary.exit_ns),
                        boundary.exit_ns.checked_sub(boundary.launch_ns),
                        boundary.exit_ns,
                    )
                } else {
                    (None, None, terminal_ns.checked_sub(submit_ns), terminal_ns)
                };
            sampler
                .wait_for_sample_after(sample_after_ns, false, outer_turn_timeout(manifest))
                .await?;
            evidence.turns.push(TurnObservation {
                repetition,
                turn_index: turn,
                actor: actor_name.clone(),
                session_id_hash: session_id_hash.clone(),
                phase: "memory-time-integral".to_owned(),
                launch_ns,
                submit_ns: Some(submit_ns),
                first_model_request_ns: None,
                terminal_ns: Some(terminal_ns),
                exit_ns,
                turn_wall_ns,
            });
        }
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        driver.shutdown().await?;
        server.shutdown().await?;
        all_requests.extend(engine.request_records().await);
        let collection = sampler.finish()?;
        if collection.samples.len() < 2 {
            return Err(AhrbError::Protocol(format!(
                "row-46 repetition {repetition} sampler produced fewer than two samples"
            )));
        }
        let first_ns = collection
            .samples
            .first()
            .map_or(0, |sample| sample.monotonic_ns);
        let last_ns = collection
            .samples
            .last()
            .map_or(first_ns, |sample| sample.monotonic_ns);
        evidence.sampler_observation_wall_ns = evidence
            .sampler_observation_wall_ns
            .saturating_add(last_ns.saturating_sub(first_ns));
        evidence.sampler_collection_cpu_ns = collection
            .samples
            .iter()
            .fold(evidence.sampler_collection_cpu_ns, |total, sample| {
                total.saturating_add(sample.collection_cpu_ns)
            });
        evidence.samples.extend(collection.samples);
    }
    all_requests.sort_by(|left, right| {
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
    Ok(MemoryTimeIntegralTrials {
        events: all_events,
        requests: all_requests,
        evidence,
    })
}

struct StreamingCaseResult {
    actor: String,
    response_headers_ns: u64,
    terminal_ns: Option<u64>,
    outer_kill_used: bool,
    outer_kill_ns: Option<u64>,
    cpu_ns: u64,
    cpu_samples: Vec<ModelWaitCpuSample>,
    frames: Vec<crate::fake_model::ModelFrameObservation>,
    events: Vec<NormalizedEvent>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
}

fn streaming_workflow(
    row: u8,
    case: &str,
    profile_root: &Path,
    repetition: u32,
    count: u32,
) -> Workflow {
    let scenario = format!("ahrb-row{row}-{case}-r{repetition}");
    let actor = format!("r{row}-{case}-r{repetition}");
    let fault = if case == "stall" {
        Fault::Stall
    } else {
        Fault::Trickle {
            cadence_ms: 1_000,
            count,
        }
    };
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB row {row} {case} trial {}",
                    route_marker(&scenario, &actor, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![ScriptedResponse {
            scenario,
            actor,
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: success_value(),
            fault: Some(fault),
            barrier: None,
        }],
    }
}

async fn wait_for_response_headers(
    engine: &FakeModelEngine,
    actor: &str,
    deadline: Duration,
) -> Result<crate::fake_model::ModelRequestRecord> {
    let started = Instant::now();
    loop {
        if let Some(record) = engine.request_records().await.into_iter().find(|record| {
            record.request.actor == actor
                && record.request.checkpoint == "start"
                && record.response_headers_ns.is_some()
        }) {
            return Ok(record);
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "streaming provider did not publish response headers for {actor}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

struct PacedFrameWait<'a> {
    engine: &'a FakeModelEngine,
    actor: &'a str,
    expected_count: u32,
    response_headers_ns: u64,
    outer_deadline_ms: u64,
    sampler: Option<&'a mut Box<dyn Sampler>>,
    roots: &'a [u32],
    cpu_samples: &'a mut Vec<ModelWaitCpuSample>,
}

async fn wait_for_paced_frames(
    mut wait: PacedFrameWait<'_>,
) -> Result<Vec<crate::fake_model::ModelFrameObservation>> {
    let deadline_ns = wait
        .response_headers_ns
        .checked_add(wait.outer_deadline_ms.saturating_mul(1_000_000))
        .ok_or_else(|| AhrbError::Validation("streaming row deadline overflow".to_owned()))?;
    let mut next_cpu_sample = Instant::now();
    loop {
        let frames = wait
            .engine
            .frame_observations()?
            .into_iter()
            .filter(|frame| frame.actor == wait.actor)
            .collect::<Vec<_>>();
        if frames.len() == wait.expected_count as usize {
            if let Some(sampler) = wait.sampler.as_deref_mut() {
                wait.cpu_samples
                    .push(sample_streaming_cpu(sampler.as_mut(), wait.roots)?);
            }
            return Ok(frames);
        }
        if frames.len() > wait.expected_count as usize {
            return Err(AhrbError::Protocol(format!(
                "streaming provider yielded {} paced frames for {}; expected {}",
                frames.len(),
                wait.actor,
                wait.expected_count
            )));
        }
        if monotonic_timestamp_ns() >= deadline_ns {
            return Err(AhrbError::Timeout(format!(
                "streaming provider yielded {} of {} paced frames for {}",
                frames.len(),
                wait.expected_count,
                wait.actor
            )));
        }
        if let Some(sampler) = wait.sampler.as_deref_mut()
            && Instant::now() >= next_cpu_sample
        {
            wait.cpu_samples
                .push(sample_streaming_cpu(sampler.as_mut(), wait.roots)?);
            next_cpu_sample = Instant::now() + Duration::from_millis(100);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn sample_streaming_cpu(sampler: &mut dyn Sampler, roots: &[u32]) -> Result<ModelWaitCpuSample> {
    if roots.is_empty() {
        return Err(AhrbError::Protocol(
            "streaming row exposed no owned process root".to_owned(),
        ));
    }
    let sample_started_ns = monotonic_timestamp_ns();
    let tree = sampler.discover(roots)?;
    let cpu_ns = sampler.sample(&tree, "model-wait")?.cpu_ns;
    let sample_finished_ns = monotonic_timestamp_ns();
    Ok(ModelWaitCpuSample {
        sample_started_ns,
        sample_finished_ns,
        cpu_ns,
    })
}

async fn cleanup_streaming_outer_kill(
    driver: &mut HarnessDriver,
    roots: &[u32],
    grace: Duration,
) -> Result<()> {
    if roots.is_empty() {
        return Err(AhrbError::Protocol(
            "streaming outer deadline has no verified owned root to kill".to_owned(),
        ));
    }
    let mut sampler = platform_sampler();
    let tree = sampler.discover(roots)?;
    signal_owned_tree(&tree)?;
    match tokio::time::timeout(grace, driver.reap_after_external_kill()).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(AhrbError::Timeout(
                "streaming outer-kill launcher reap exceeded grace".to_owned(),
            ));
        }
    }
    if !await_owned_tree_empty(sampler.as_mut(), roots, grace).await? {
        return Err(AhrbError::Protocol(
            "streaming outer-kill left owned-tree residue".to_owned(),
        ));
    }
    Ok(())
}

fn remaining_row_deadline(response_headers_ns: u64, outer_deadline_ms: u64) -> Result<Duration> {
    let deadline_ns = response_headers_ns
        .checked_add(outer_deadline_ms.saturating_mul(1_000_000))
        .ok_or_else(|| AhrbError::Validation("streaming row deadline overflow".to_owned()))?;
    let now_ns = monotonic_timestamp_ns();
    if now_ns >= deadline_ns {
        return Err(AhrbError::Timeout(
            "streaming row-local outer deadline elapsed".to_owned(),
        ));
    }
    Ok(Duration::from_nanos(deadline_ns - now_ns))
}

fn workspace_fault_workflow(
    profile_root: &Path,
    repetition: u32,
    manifest: &Manifest,
) -> Result<Workflow> {
    let scenario = format!("ahrb-row61-r{repetition}");
    let actor_name = format!("r61-workspace-r{repetition}");
    let terminal = route_marker(&scenario, &actor_name, "terminal");
    let call = mapped_tool_call(
        manifest,
        "write",
        format!("call-workspace-fault-r{repetition}"),
        json!({
            "path":"row-61-denied.txt",
            "content":format!("must-not-be-written{terminal}")
        }),
    )?;
    Ok(Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor_name.clone(),
            Actor {
                id: actor_name.clone(),
                parent: None,
                prompt: format!(
                    "AHRB workspace fault repetition {repetition} {}",
                    route_marker(&scenario, &actor_name, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![
            ScriptedResponse {
                scenario: scenario.clone(),
                actor: actor_name.clone(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({"tool_calls":[call]}),
                fault: None,
                barrier: None,
            },
            ScriptedResponse {
                scenario,
                actor: actor_name,
                checkpoint: "terminal".to_owned(),
                request_hash: String::new(),
                response: json!({"text":"{\"status\":\"FAILURE\",\"category\":\"workspace\"}"}),
                fault: None,
                barrier: None,
            },
        ],
    })
}

#[cfg(unix)]
fn set_directory_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let mut permissions = std::fs::metadata(path)?.permissions();
    permissions.set_mode(mode);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_directory_mode(_path: &Path, _mode: u32) -> Result<()> {
    Err(AhrbError::Unsupported(
        "read-only workspace fixture requires Unix mode bits".to_owned(),
    ))
}

fn workspace_result_errno(
    events: &[NormalizedEvent],
    expected_call_id: &str,
    expected_path: &str,
) -> Result<i32> {
    let calls = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolCall)
        .collect::<Vec<_>>();
    let results = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolResult)
        .collect::<Vec<_>>();
    if calls.len() != 1 || results.len() != 1 {
        return Err(AhrbError::Protocol(format!(
            "row-61 expected exactly one correlated call/result for {expected_call_id}; observed {}/{}",
            calls.len(),
            results.len()
        )));
    }
    for event in [calls[0], results[0]] {
        if event.payload.get("call_id").and_then(Value::as_str) != Some(expected_call_id)
            || event.payload.get("name").and_then(Value::as_str) != Some("write_fixture")
            || event
                .payload
                .pointer("/arguments/path")
                .and_then(Value::as_str)
                != Some(expected_path)
        {
            return Err(AhrbError::Protocol(
                "row-61 correlated fixture name or target path did not match the scripted write"
                    .to_owned(),
            ));
        }
    }
    let result = results[0];
    let ordinary = result.payload.get("result").ok_or_else(|| {
        AhrbError::Protocol(
            "row-61 correlated result omitted its ordinary result object".to_owned(),
        )
    })?;
    if ordinary.get("path").and_then(Value::as_str) != Some(expected_path)
        || ordinary.get("ok").and_then(Value::as_bool) != Some(false)
    {
        return Err(AhrbError::Protocol(
            "row-61 ordinary result path/status contradicted the denied write".to_owned(),
        ));
    }
    let errno = ordinary
        .get("write_errno")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| {
            AhrbError::Protocol("row-61 ordinary result omitted write_errno".to_owned())
        })?;
    if let Some(native) = result.payload.get("native_result") {
        let native_errno = native
            .get("write_errno")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok());
        if native.get("path").and_then(Value::as_str) != Some(expected_path)
            || native_errno != Some(errno)
        {
            return Err(AhrbError::Protocol(
                "row-61 normalized and native structured results disagreed".to_owned(),
            ));
        }
    }
    Ok(errno)
}

fn workspace_result_claimed_success(events: &[NormalizedEvent]) -> bool {
    events
        .iter()
        .filter(|event| event.event == EventVocab::ToolResult)
        .any(|event| {
            event.payload.pointer("/result/ok").and_then(Value::as_bool) == Some(true)
                || event
                    .payload
                    .pointer("/native_result/ok")
                    .and_then(Value::as_bool)
                    == Some(true)
        })
}

async fn collect_large_output_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<LargeOutputTrials> {
    const PRODUCED_BYTES: u64 = 10_485_760;
    let marker = manifest.capture.truncation_marker.as_ref().ok_or_else(|| {
        AhrbError::Validation(
            "row-60 requires capture.truncation_marker with named fields".to_owned(),
        )
    })?;
    let marker_regex = regex::Regex::new(&marker.regex).map_err(|error| {
        AhrbError::Validation(format!(
            "row-60 truncation marker regex is invalid: {error}"
        ))
    })?;
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let per_invocation = per_invocation_topology(manifest);
    let mut collected = LargeOutputTrials {
        events: Vec::new(),
        requests: Vec::new(),
        evidence: Vec::new(),
    };

    for repetition in 1..=repetitions {
        let profile_root = run_profile_root.join(format!("derived-row60-r{repetition}"));
        prepare_profile(manifest, &profile_root).map_err(|error| {
            AhrbError::Protocol(format!(
                "prepare row-60 repetition {repetition} fresh profile: {error}"
            ))
        })?;
        let (workflow, actor_name, barrier_name) =
            large_output_workflow(&profile_root, repetition, manifest)?;
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "row-60 repetition {repetition} actor {actor_name:?} disappeared"
            ))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row60-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let deadline = outer_turn_timeout(manifest);
        // A journal record wraps the bounded model-visible result in JSONL
        // metadata. That protocol envelope is not AHRB artifact capture, so
        // parse it with a separately bounded fixed overhead while preserving
        // the declared capture limit in row-60 evidence and metrics.
        let mut driver_manifest = manifest.clone();
        if per_invocation {
            driver_manifest.capture.max_bytes = manifest
                .resources
                .max_output_bytes
                .saturating_add(64 * 1024);
        }
        let mut driver = make_driver_with_timeout(
            &driver_manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
            deadline,
        )?;
        driver.start().await?;
        driver.await_readiness().await?;
        let mut sampler = platform_sampler();
        let daemon_roots = if per_invocation {
            Vec::new()
        } else {
            verified_process_roots(
                manifest,
                sampler.as_mut(),
                driver.owned_pids(),
                driver.daemon_pid(),
            )?
        };
        let session = driver
            .create_session(&format!("{}:{actor_name}", workflow.scenario))
            .await?;
        let baseline_bytes = if per_invocation {
            if !driver.session_pids(&session).is_empty() || !driver.owned_pids().is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-60 repetition {repetition} per-invocation topology was not at verified zero-process idle"
                )));
            }
            0_u64
        } else {
            let samples = baseline_samples(sampler.as_mut(), &daemon_roots, profile).await?;
            median_effective_sample_bytes(&samples).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "row-60 repetition {repetition} daemon warm-idle baseline was empty"
                ))
            })?
        };
        driver
            .submit(&session, &actor.prompt, &format!("row-60-r{repetition}"))
            .await?;
        tokio::time::timeout(deadline, engine.barriers().wait_until_ready(&barrier_name))
            .await
            .map_err(|_| {
                AhrbError::Timeout(format!(
                    "row-60 repetition {repetition} did not reach its pre-tool provider barrier"
                ))
            })??;
        let roots = if per_invocation {
            let roots = driver.session_pids(&session);
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-60 repetition {repetition} exposed no active invocation root"
                )));
            }
            roots
        } else {
            daemon_roots.clone()
        };
        let initial_memory = await_large_output_memory_sample(
            sampler.as_mut(),
            &roots,
            deadline.min(Duration::from_secs(1)),
        )
        .await?;
        engine.barriers().release(&barrier_name).await?;
        let (events, peak_bytes) = collect_large_output_terminal_with_memory(
            &mut driver,
            &session,
            sampler.as_mut(),
            &roots,
            deadline,
            initial_memory,
        )
        .await?;
        let terminal_success = events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count()
            == 1
            && events
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count()
                == 1;
        let oom = if per_invocation {
            !matches!(
                driver.client_exit(&session),
                crate::driver::ClientExit::Exited(Some(0))
            )
        } else {
            sampler.discover(&roots)?.members.is_empty()
        };
        let requests = engine.request_records().await;
        let trial = build_large_output_trial(
            repetition,
            manifest,
            &actor_name,
            &events,
            &requests,
            &marker_regex,
            baseline_bytes,
            peak_bytes,
            terminal_success,
            oom,
            &variables,
            deadline,
            PRODUCED_BYTES,
        )
        .await?;
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        driver.shutdown().await?;
        server.shutdown().await?;
        collected.events.extend(events);
        collected.requests.extend(requests);
        collected.evidence.push(trial);
    }
    Ok(collected)
}

fn large_output_workflow(
    profile_root: &Path,
    repetition: u32,
    manifest: &Manifest,
) -> Result<(Workflow, String, String)> {
    let scenario = format!("ahrb-row60-r{repetition}");
    let actor_name = format!("r60-r{repetition}");
    let barrier_name = format!("row60-r{repetition}-pre-tool");
    let actor = Actor {
        id: actor_name.clone(),
        parent: None,
        prompt: format!(
            "AHRB large tool output {}",
            route_marker(&scenario, &actor_name, "start")
        ),
        workspace: profile_root
            .join("workspace")
            .to_string_lossy()
            .into_owned(),
    };
    let mut responses = scripted_row(60, &scenario, &actor_name, manifest)?;
    let first = responses.first_mut().ok_or_else(|| {
        AhrbError::Protocol("row-60 workflow omitted its tool-call response".to_owned())
    })?;
    first.barrier = Some(barrier_name.clone());
    Ok((
        Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario,
            actors: BTreeMap::from([(actor_name.clone(), actor)]),
            barriers: BTreeMap::from([(
                barrier_name.clone(),
                Barrier {
                    name: barrier_name.clone(),
                    actors: vec![actor_name.clone()],
                    checkpoint: "start".to_owned(),
                },
            )]),
            responses,
        },
        actor_name,
        barrier_name,
    ))
}

fn median_effective_sample_bytes(samples: &[Sample]) -> Option<u64> {
    let mut values = samples
        .iter()
        .map(effective_sample_bytes)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        Some(values[middle])
    } else {
        Some(values[middle - 1].saturating_add(values[middle]) / 2)
    }
}

fn sample_large_output_memory(sampler: &mut dyn Sampler, roots: &[u32]) -> Result<u64> {
    let tree = sampler.discover(roots)?;
    if tree.members.is_empty() {
        return Err(AhrbError::Protocol(
            "row-60 memory sample resolved an empty owned tree".to_owned(),
        ));
    }
    let sample = sampler.sample(&tree, "large-tool-output")?;
    Ok(effective_sample_bytes(&sample))
}

async fn await_large_output_memory_sample(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    deadline: Duration,
) -> Result<u64> {
    let started = Instant::now();
    loop {
        match sample_large_output_memory(sampler, roots) {
            Ok(bytes) => return Ok(bytes),
            Err(error) if started.elapsed() >= deadline => return Err(error),
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
    }
}

async fn collect_large_output_terminal_with_memory(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    sampler: &mut dyn Sampler,
    roots: &[u32],
    deadline: Duration,
    mut peak_bytes: u64,
) -> Result<(Vec<NormalizedEvent>, u64)> {
    let started = Instant::now();
    loop {
        if let Ok(bytes) = sample_large_output_memory(sampler, roots) {
            peak_bytes = peak_bytes.max(bytes);
        }
        let events = driver.attach(session, None).await?;
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Ok((events, peak_bytes));
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "row-60 session {} did not terminalize",
                session.0
            )));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[derive(Debug)]
struct LargeOutputMarkerObservation {
    truncated: bool,
    original_bytes: u64,
    payload_bytes: u64,
    actual_payload_bytes: u64,
    sha256: String,
}

#[derive(Debug)]
struct ToolResultCandidate {
    call_id: String,
    content: String,
}

#[allow(clippy::too_many_arguments)]
async fn build_large_output_trial(
    repetition: u32,
    manifest: &Manifest,
    actor: &str,
    events: &[NormalizedEvent],
    requests: &[crate::fake_model::ModelRequestRecord],
    marker_regex: &regex::Regex,
    baseline_bytes: u64,
    peak_bytes: u64,
    terminal_success: bool,
    oom: bool,
    variables: &BTreeMap<String, String>,
    deadline: Duration,
    expected_produced_bytes: u64,
) -> Result<LargeOutputTrial> {
    let call_ids = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolCall)
        .filter_map(|event| event.payload.get("call_id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    if call_ids.len() != 1 {
        return Err(AhrbError::Protocol(format!(
            "row-60 repetition {repetition} observed {} tool-call IDs; expected one",
            call_ids.len()
        )));
    }
    let call_id = call_ids[0].to_owned();
    let results = events
        .iter()
        .filter(|event| event.event == EventVocab::ToolResult)
        .collect::<Vec<_>>();
    if results.len() != 1 {
        return Err(AhrbError::Protocol(format!(
            "row-60 repetition {repetition} observed {} tool results; expected one",
            results.len()
        )));
    }
    let result = results[0];
    let result_call_id = result
        .payload
        .get("call_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "row-60 repetition {repetition} tool result omitted call_id"
            ))
        })?
        .to_owned();
    let event_result = result.payload.get("result").ok_or_else(|| {
        AhrbError::Protocol(format!(
            "row-60 repetition {repetition} tool result omitted result content"
        ))
    })?;
    // Native-fixture adapters may retain a richer normalized event envelope
    // while passing only the worker's result to the next model request.
    let model_visible_event_result = if let Some(value) = result
        .payload
        .get("native_result")
        .or_else(|| event_result.get("native"))
    {
        value
    } else {
        event_result
    };
    let event_encoded = serde_json::to_string(model_visible_event_result)?;
    let next = requests
        .iter()
        .find(|record| {
            record.request.actor == actor
                && record.request.checkpoint == "terminal"
                && record.accepted
        })
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "row-60 repetition {repetition} lacks the next accepted model request"
            ))
        })?;
    let candidates = tool_result_candidates(&next.request.canonical)?;
    let correlated = candidates
        .iter()
        .find(|candidate| candidate.call_id == result_call_id);
    let encoded = if let Some(candidate) = correlated {
        candidate.content.as_str()
    } else {
        event_encoded.as_str()
    };
    let marker = parse_large_output_marker(marker_regex, encoded)?;
    let fixture = collect_large_output_fixture_evidence(
        manifest,
        variables,
        expected_produced_bytes,
        deadline,
    )
    .await?;
    let result_in_next_request = correlated.is_some_and(|candidate| {
        candidate.call_id == call_id
            && candidate.call_id == result_call_id
            && candidate.content == event_encoded
    });
    let encoded_bytes = u64::try_from(encoded.len()).map_err(|_| {
        AhrbError::Protocol("row-60 model-visible content length overflowed u64".to_owned())
    })?;
    Ok(LargeOutputTrial {
        repetition,
        produced_bytes: fixture.produced_bytes,
        model_visible_bytes: marker.actual_payload_bytes,
        model_visible_encoded_bytes: encoded_bytes,
        harness_output_limit_bytes: u64::try_from(manifest.resources.max_output_bytes).map_err(
            |_| AhrbError::Protocol("row-60 harness output limit overflowed u64".to_owned()),
        )?,
        evidence_captured_bytes: fixture.captured_bytes,
        evidence_capture_limit_bytes: u64::try_from(manifest.capture.max_bytes).map_err(|_| {
            AhrbError::Protocol("row-60 evidence capture limit overflowed u64".to_owned())
        })?,
        truncated: marker.truncated,
        marker_original_bytes: marker.original_bytes,
        marker_payload_bytes: marker.payload_bytes,
        marker_sha256: marker.sha256,
        external_sha256: fixture.sha256,
        call_id,
        result_call_id,
        result_in_next_request,
        peak_rss_delta_mib: peak_bytes.saturating_sub(baseline_bytes) as f64 / 1_048_576.0,
        terminal_success,
        oom,
    })
}

fn tool_result_candidates(canonical: &Value) -> Result<Vec<ToolResultCandidate>> {
    fn visit(value: &Value, output: &mut Vec<ToolResultCandidate>) -> Result<()> {
        match value {
            Value::Array(items) => {
                for item in items {
                    visit(item, output)?;
                }
            }
            Value::Object(object) => {
                let candidate = object
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .zip(object.get("content"))
                    .or_else(|| {
                        object
                            .get("type")
                            .and_then(Value::as_str)
                            .filter(|kind| *kind == "function_call_output")
                            .and_then(|_| object.get("call_id").and_then(Value::as_str))
                            .zip(object.get("output"))
                    })
                    .or_else(|| {
                        object
                            .get("type")
                            .and_then(Value::as_str)
                            .filter(|kind| *kind == "tool_result")
                            .and_then(|_| object.get("tool_use_id").and_then(Value::as_str))
                            .zip(object.get("content"))
                    });
                if let Some((call_id, content)) = candidate {
                    output.push(ToolResultCandidate {
                        call_id: call_id.to_owned(),
                        content: normalized_tool_result_content(content)?,
                    });
                    return Ok(());
                }
                for child in object.values() {
                    visit(child, output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut output = Vec::new();
    visit(canonical, &mut output)?;
    Ok(output)
}

fn normalized_tool_result_content(value: &Value) -> Result<String> {
    match value {
        Value::String(content) => Ok(content.clone()),
        Value::Array(blocks)
            if blocks.iter().all(|block| {
                block.get("text").and_then(Value::as_str).is_some() || block.as_str().is_some()
            }) =>
        {
            Ok(blocks
                .iter()
                .filter_map(|block| {
                    block
                        .get("text")
                        .and_then(Value::as_str)
                        .or_else(|| block.as_str())
                })
                .collect::<String>())
        }
        _ => Ok(serde_json::to_string(value)?),
    }
}

fn parse_large_output_marker(
    marker_regex: &regex::Regex,
    encoded: &str,
) -> Result<LargeOutputMarkerObservation> {
    let decoded = match serde_json::from_str::<String>(encoded) {
        Ok(content) => content,
        Err(_) => encoded.to_owned(),
    };
    let captures = marker_regex.captures(&decoded).ok_or_else(|| {
        AhrbError::Protocol("row-60 model-visible result lacks its declared marker".to_owned())
    })?;
    let marker_match = captures.get(0).ok_or_else(|| {
        AhrbError::Protocol("row-60 marker regex returned no complete match".to_owned())
    })?;
    if marker_match.end() != decoded.len() {
        return Err(AhrbError::Protocol(
            "row-60 marker is not the exact final content suffix".to_owned(),
        ));
    }
    let payload = decoded
        .get(..marker_match.start())
        .and_then(|prefix| prefix.strip_suffix('\n'))
        .ok_or_else(|| {
            AhrbError::Protocol(
                "row-60 marker lacks its exact retained-payload separator".to_owned(),
            )
        })?;
    let named = |name: &str| {
        captures
            .name(name)
            .map(|value| value.as_str())
            .ok_or_else(|| {
                AhrbError::Protocol(format!("row-60 marker omitted named capture {name:?}"))
            })
    };
    Ok(LargeOutputMarkerObservation {
        truncated: named("truncated")? == "true",
        original_bytes: named("original_bytes")?.parse::<u64>().map_err(|_| {
            AhrbError::Protocol("row-60 marker original_bytes is invalid".to_owned())
        })?,
        payload_bytes: named("payload_bytes")?.parse::<u64>().map_err(|_| {
            AhrbError::Protocol("row-60 marker payload_bytes is invalid".to_owned())
        })?,
        actual_payload_bytes: u64::try_from(payload.len()).map_err(|_| {
            AhrbError::Protocol("row-60 retained payload length overflowed u64".to_owned())
        })?,
        sha256: named("sha256")?.to_owned(),
    })
}

#[derive(Debug)]
struct LargeOutputFixtureEvidence {
    produced_bytes: u64,
    captured_bytes: u64,
    sha256: String,
}

async fn collect_large_output_fixture_evidence(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    bytes: u64,
    deadline: Duration,
) -> Result<LargeOutputFixtureEvidence> {
    let template = manifest.tools.fixtures.get("large_output").ok_or_else(|| {
        AhrbError::Validation("row-60 requires tools.fixtures.large_output".to_owned())
    })?;
    let mut fixture_variables = variables.clone();
    fixture_variables.insert("bytes".to_owned(), bytes.to_string());
    fixture_variables.insert("ahrb_fixture".to_owned(), fixture_program()?);
    fixture_variables.insert("workspace".to_owned(), ".".to_owned());
    let rendered = template
        .iter()
        .map(|argument| crate::manifest::render_template(argument, &fixture_variables))
        .collect::<Result<Vec<_>>>()?;
    let argv = resolve_local_program(&rendered)?;
    let (program, arguments) = argv.split_first().ok_or_else(|| {
        AhrbError::Validation("row-60 large-output fixture command is empty".to_owned())
    })?;
    let mut child = tokio::process::Command::new(program)
        .args(arguments)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| AhrbError::Protocol("row-60 fixture stdout pipe disappeared".to_owned()))?;
    let started = Instant::now();
    let mut buffer = [0_u8; 16 * 1024];
    let mut produced_bytes = 0_u64;
    let mut digest = Sha256::new();
    let capture_limit = manifest.capture.max_bytes;
    let mut retained = Vec::with_capacity(capture_limit.min(64 * 1024));
    loop {
        let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
            AhrbError::Timeout("row-60 external fixture stream exceeded its deadline".to_owned())
        })?;
        let count = tokio::time::timeout(remaining, stdout.read(&mut buffer))
            .await
            .map_err(|_| {
                AhrbError::Timeout(
                    "row-60 external fixture stream exceeded its deadline".to_owned(),
                )
            })??;
        if count == 0 {
            break;
        }
        produced_bytes = produced_bytes.saturating_add(u64::try_from(count).map_err(|_| {
            AhrbError::Protocol("row-60 fixture read length overflowed u64".to_owned())
        })?);
        digest.update(&buffer[..count]);
        if retained.len() < capture_limit {
            let remaining_capture = capture_limit.saturating_sub(retained.len());
            retained.extend_from_slice(&buffer[..count.min(remaining_capture)]);
        }
    }
    let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
        AhrbError::Timeout("row-60 external fixture exit exceeded its deadline".to_owned())
    })?;
    let status = tokio::time::timeout(remaining, child.wait())
        .await
        .map_err(|_| {
            AhrbError::Timeout("row-60 external fixture exit exceeded its deadline".to_owned())
        })??;
    if !status.success() {
        return Err(AhrbError::Protocol(format!(
            "row-60 external fixture exited with {status}"
        )));
    }
    Ok(LargeOutputFixtureEvidence {
        produced_bytes,
        captured_bytes: u64::try_from(retained.len()).map_err(|_| {
            AhrbError::Protocol("row-60 retained capture length overflowed u64".to_owned())
        })?,
        sha256: format!("{:x}", digest.finalize()),
    })
}

#[cfg(test)]
mod row60_collector_tests {
    use super::*;

    #[test]
    fn extracts_correlated_openai_tool_result_without_losing_json_encoding() -> Result<()> {
        let result = Value::String(
            "abc\nTRUNCATED truncated=true original=10485760 payload=3 sha256=415b6d9db784e1d225cdf51aada0316c4c78c1b925a7fe59d45d78404a02668c"
                .to_owned(),
        );
        let encoded = serde_json::to_string(&result)?;
        let canonical = json!({
            "messages": [{
                "role": "tool",
                "tool_call_id": "call-large-output",
                "content": encoded,
            }]
        });
        let candidates = tool_result_candidates(&canonical)?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].call_id, "call-large-output");
        assert_eq!(candidates[0].content, serde_json::to_string(&result)?);
        Ok(())
    }

    #[test]
    fn marker_parser_cross_checks_actual_retained_payload_bytes() -> Result<()> {
        let regex = regex::Regex::new(
            r"TRUNCATED truncated=(?P<truncated>true) original=(?P<original_bytes>[0-9]+) payload=(?P<payload_bytes>[0-9]+) sha256=(?P<sha256>[0-9a-f]{64})",
        )
        .map_err(|error| AhrbError::Validation(error.to_string()))?;
        let raw = "abc\nTRUNCATED truncated=true original=10485760 payload=3 sha256=415b6d9db784e1d225cdf51aada0316c4c78c1b925a7fe59d45d78404a02668c";
        let encoded = serde_json::to_string(raw)?;
        let marker = parse_large_output_marker(&regex, &encoded)?;
        assert!(marker.truncated);
        assert_eq!(marker.original_bytes, 10_485_760);
        assert_eq!(marker.payload_bytes, 3);
        assert_eq!(marker.actual_payload_bytes, 3);
        let trailing = serde_json::to_string(&format!("{raw}trailing"))?;
        assert!(parse_large_output_marker(&regex, &trailing).is_err());
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct Row61TreeSnapshot {
    root: String,
    exists: bool,
    entries: Vec<Row61SnapshotEntry>,
    sha256: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize)]
struct Row61SnapshotEntry {
    kind: String,
    path: String,
    size_bytes: Option<u64>,
    sha256: Option<String>,
    target: Option<String>,
}

fn row61_tree_snapshot(root: &Path) -> Result<Row61TreeSnapshot> {
    const MAX_ENTRIES: usize = 100_000;
    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let entries = vec![Row61SnapshotEntry {
                kind: "absent".to_owned(),
                path: ".".to_owned(),
                size_bytes: None,
                sha256: None,
                target: None,
            }];
            let encoded = serde_json::to_vec(&entries)?;
            return Ok(Row61TreeSnapshot {
                root: root.to_string_lossy().into_owned(),
                exists: false,
                entries,
                sha256: format!("{:x}", Sha256::digest(encoded)),
            });
        }
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_dir() {
        return Err(AhrbError::Protocol(format!(
            "row-61 snapshot root {} is not a directory",
            root.display()
        )));
    }
    let mut pending = vec![root.to_path_buf()];
    let mut entries = vec![Row61SnapshotEntry {
        kind: "directory".to_owned(),
        path: ".".to_owned(),
        size_bytes: None,
        sha256: None,
        target: None,
    }];
    while let Some(directory) = pending.pop() {
        let mut children =
            std::fs::read_dir(&directory)?.collect::<std::result::Result<Vec<_>, _>>()?;
        children.sort_by_key(std::fs::DirEntry::path);
        for child in children {
            if entries.len() >= MAX_ENTRIES {
                return Err(AhrbError::Protocol(format!(
                    "row-61 snapshot root {} exceeds {MAX_ENTRIES} entries",
                    root.display()
                )));
            }
            let path = child.path();
            let relative = path.strip_prefix(root).map_err(|_| {
                AhrbError::Protocol(format!(
                    "row-61 snapshot entry {} escaped root {}",
                    path.display(),
                    root.display()
                ))
            })?;
            let relative = relative.to_string_lossy();
            let file_type = child.file_type()?;
            if file_type.is_dir() {
                entries.push(Row61SnapshotEntry {
                    kind: "directory".to_owned(),
                    path: relative.into_owned(),
                    size_bytes: None,
                    sha256: None,
                    target: None,
                });
                pending.push(path);
            } else if file_type.is_file() {
                let metadata = child.metadata()?;
                entries.push(Row61SnapshotEntry {
                    kind: "file".to_owned(),
                    path: relative.into_owned(),
                    size_bytes: Some(metadata.len()),
                    sha256: Some(row47_hash_file(&path)?),
                    target: None,
                });
            } else if file_type.is_symlink() {
                entries.push(Row61SnapshotEntry {
                    kind: "symlink".to_owned(),
                    path: relative.into_owned(),
                    size_bytes: None,
                    sha256: None,
                    target: Some(std::fs::read_link(&path)?.to_string_lossy().into_owned()),
                });
            } else {
                entries.push(Row61SnapshotEntry {
                    kind: "other".to_owned(),
                    path: relative.into_owned(),
                    size_bytes: None,
                    sha256: None,
                    target: None,
                });
            }
        }
    }
    entries.sort();
    let encoded = serde_json::to_vec(&entries)?;
    Ok(Row61TreeSnapshot {
        root: root.to_string_lossy().into_owned(),
        exists: true,
        entries,
        sha256: format!("{:x}", Sha256::digest(encoded)),
    })
}

fn row61_unexpected_profile_changes(
    before: &Row61TreeSnapshot,
    after: &Row61TreeSnapshot,
    allowed_roots: &[PathBuf],
) -> u32 {
    let before = before.entries.iter().cloned().collect::<BTreeSet<_>>();
    let after = after.entries.iter().cloned().collect::<BTreeSet<_>>();
    let unexpected = before
        .symmetric_difference(&after)
        .filter(|entry| {
            let path = Path::new(&entry.path);
            !allowed_roots
                .iter()
                .any(|allowed| path == allowed || path.starts_with(allowed))
        })
        .map(|entry| entry.path.as_str())
        .collect::<BTreeSet<_>>();
    u32::try_from(unexpected.len()).map_or(u32::MAX, |value| value)
}

fn row61_allowed_profile_write_roots(
    environment: &BTreeMap<String, String>,
    profile_root: &Path,
) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    for name in ["XDG_STATE_HOME", "XDG_RUNTIME_DIR", "TMPDIR"] {
        if let Some(value) = environment.get(name) {
            let path = Path::new(value);
            let relative = path.strip_prefix(profile_root).map_err(|_| {
                AhrbError::Validation(format!(
                    "row-61 writable state root {name}={} is outside {}",
                    path.display(),
                    profile_root.display()
                ))
            })?;
            roots.push(relative.to_path_buf());
        }
    }
    // These are AHRB-owned transport evidence roots, not actor workspaces.
    roots.push(PathBuf::from("logs"));
    roots.push(PathBuf::from("ahrb-exec-sessions"));
    roots.sort();
    roots.dedup();
    Ok(roots)
}

fn row61_forbidden_roots(manifest: &Manifest) -> Result<Vec<PathBuf>> {
    let home = std::env::var_os("HOME").map(PathBuf::from).ok_or_else(|| {
        AhrbError::Validation("row-61 cannot resolve forbidden roots without HOME".to_owned())
    })?;
    manifest
        .isolation
        .forbidden_roots
        .iter()
        .map(|declared| {
            if declared == "~" {
                Ok(home.clone())
            } else if let Some(relative) = declared.strip_prefix("~/") {
                Ok(home.join(relative))
            } else {
                let path = PathBuf::from(declared);
                if path.is_absolute() {
                    Ok(path)
                } else {
                    Err(AhrbError::Validation(format!(
                        "row-61 forbidden root must be absolute or home-relative: {declared}"
                    )))
                }
            }
        })
        .collect()
}

fn row61_snapshot_set(roots: &[PathBuf]) -> Result<(String, Value)> {
    let snapshots = roots
        .iter()
        .map(|root| row61_tree_snapshot(root))
        .collect::<Result<Vec<_>>>()?;
    let encoded = serde_json::to_vec(&snapshots)?;
    Ok((
        format!("{:x}", Sha256::digest(&encoded)),
        serde_json::to_value(snapshots)?,
    ))
}

fn row61_write_snapshot_ledger(
    profile_root: &Path,
    repetition: u32,
    name: &str,
    value: &Value,
    raw: &mut Vec<FilesystemSnapshot>,
) -> Result<()> {
    let directory = profile_root.join("row61-snapshot-evidence");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join(format!("{name}.json"));
    std::fs::write(&path, serde_json::to_vec(value)?)?;
    row47_snapshot_file(
        &path,
        profile_root,
        repetition,
        name,
        "workspace-fault-snapshot-ledger",
        raw,
    )?
    .ok_or_else(|| {
        AhrbError::Protocol(format!(
            "row-61 snapshot ledger {} disappeared",
            path.display()
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod row61_collector_tests {
    use super::*;

    fn event(cursor: u64, event: EventVocab, payload: Value) -> NormalizedEvent {
        NormalizedEvent {
            id: format!("row61-test-{cursor}"),
            cursor,
            session_id: "row61-session".to_owned(),
            actor: "row61-actor".to_owned(),
            event,
            payload,
        }
    }

    #[test]
    fn workspace_errno_requires_one_exactly_correlated_ordinary_result() -> Result<()> {
        let call_id = "call-workspace-fault-r1";
        let call = event(
            1,
            EventVocab::ToolCall,
            json!({
                "call_id":call_id,
                "name":"write_fixture",
                "arguments":{"path":"row-61-denied.txt"}
            }),
        );
        let result = event(
            2,
            EventVocab::ToolResult,
            json!({
                "call_id":call_id,
                "name":"write_fixture",
                "arguments":{"path":"row-61-denied.txt"},
                "result":{"ok":false,"path":"row-61-denied.txt","write_errno":libc::EACCES}
            }),
        );
        assert_eq!(
            workspace_result_errno(
                &[call.clone(), result.clone()],
                call_id,
                "row-61-denied.txt"
            )?,
            libc::EACCES
        );
        let unrelated = event(
            3,
            EventVocab::ToolResult,
            json!({
                "call_id":"other",
                "name":"write_fixture",
                "arguments":{"path":"other"},
                "result":{"ok":false,"path":"other","write_errno":libc::EACCES}
            }),
        );
        assert!(
            workspace_result_errno(&[call, result, unrelated], call_id, "row-61-denied.txt")
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn profile_snapshot_diff_counts_only_changes_outside_state_roots() {
        let root = Row61SnapshotEntry {
            kind: "directory".to_owned(),
            path: ".".to_owned(),
            size_bytes: None,
            sha256: None,
            target: None,
        };
        let state = Row61SnapshotEntry {
            kind: "file".to_owned(),
            path: "state/session.json".to_owned(),
            size_bytes: Some(1),
            sha256: Some("a".repeat(64)),
            target: None,
        };
        let outside = Row61SnapshotEntry {
            kind: "file".to_owned(),
            path: "home/unexpected".to_owned(),
            size_bytes: Some(1),
            sha256: Some("b".repeat(64)),
            target: None,
        };
        let before = Row61TreeSnapshot {
            root: "profile".to_owned(),
            exists: true,
            entries: vec![root.clone()],
            sha256: "0".repeat(64),
        };
        let allowed_after = Row61TreeSnapshot {
            root: "profile".to_owned(),
            exists: true,
            entries: vec![root.clone(), state.clone()],
            sha256: "1".repeat(64),
        };
        assert_eq!(
            row61_unexpected_profile_changes(&before, &allowed_after, &[PathBuf::from("state")]),
            0
        );
        let unexpected_after = Row61TreeSnapshot {
            entries: vec![root, state, outside],
            ..allowed_after
        };
        assert_eq!(
            row61_unexpected_profile_changes(&before, &unexpected_after, &[PathBuf::from("state")]),
            1
        );
    }
}

async fn collect_workspace_fault_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<WorkspaceFaultTrials> {
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let mut all_events = Vec::new();
    let mut all_requests = Vec::new();
    let mut evidence = Vec::new();
    let mut filesystem_snapshots = Vec::new();
    for repetition in 1..=repetitions {
        let profile_root = run_profile_root.join(format!("derived-row61-r{repetition}"));
        prepare_profile(manifest, &profile_root)?;
        let workflow = workspace_fault_workflow(&profile_root, repetition, manifest)?;
        workflow.validate()?;
        let actor_name = format!("r61-workspace-r{repetition}");
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("row-61 repetition {repetition} actor disappeared"))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row61-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        environment.insert(
            "AHRB_MOCK_TURN_TIMEOUT_MS".to_owned(),
            manifest.resources.turn_timeout_ms.to_string(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        // Prove the fault before the harness is launched. Both daemon and
        // per-invocation mocks receive this exact actor workspace.
        let workspace = profile_root.join("workspace-fault-root");
        std::fs::create_dir_all(&workspace)?;
        set_directory_mode(&workspace, 0o555)?;
        let target = workspace.join("row-61-denied.txt");
        let target_existed_before = std::fs::symlink_metadata(&target).is_ok();
        if target_existed_before {
            set_directory_mode(&workspace, 0o700)?;
            return Err(AhrbError::Protocol(format!(
                "row-61 repetition {repetition} target existed before the control boundary"
            )));
        }
        let control_errno = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
        {
            Ok(file) => {
                drop(file);
                let _ = std::fs::remove_file(&target);
                set_directory_mode(&workspace, 0o700)?;
                return Err(AhrbError::Protocol(format!(
                    "row-61 repetition {repetition} read-only chmod control was ineffective"
                )));
            }
            Err(error) => error.raw_os_error(),
        };
        if !matches!(control_errno, Some(errno) if matches!(errno, libc::EACCES | libc::EROFS | libc::ENOSPC))
        {
            set_directory_mode(&workspace, 0o700)?;
            return Err(AhrbError::Protocol(format!(
                "row-61 repetition {repetition} control returned unrelated errno {control_errno:?}"
            )));
        }
        let workspace_before = row61_tree_snapshot(&workspace)?;
        let profile_before = row61_tree_snapshot(&profile_root)?;
        let forbidden_roots = row61_forbidden_roots(manifest)?;
        let (forbidden_before_sha256, forbidden_before) = row61_snapshot_set(&forbidden_roots)?;
        let allowed_profile_write_roots =
            row61_allowed_profile_write_roots(&environment, &profile_root)?;
        environment.insert(
            "AHRB_MOCK_WORKSPACE_OVERRIDE".to_owned(),
            workspace.to_string_lossy().into_owned(),
        );
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let per_invocation = per_invocation_topology(manifest);
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            per_invocation,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        driver.await_readiness().await?;
        let session = driver
            .create_session(&format!("{}:row61", workflow.scenario))
            .await?;
        let state_evidence_path = if per_invocation {
            profile_root
                .join("ahrb-exec-sessions")
                .join(&session.0)
                .join("session.json")
        } else {
            profile_root
                .join("state")
                .join("sessions")
                .join(&session.0)
                .join("journal.jsonl")
        };
        let _ = row47_snapshot_file(
            &state_evidence_path,
            &profile_root,
            repetition,
            "workspace-fault-before",
            "workspace-fault-state",
            &mut filesystem_snapshots,
        )?;
        let operation_start_ns = monotonic_timestamp_ns();
        driver
            .submit(&session, &actor.prompt, &format!("row-61-r{repetition}"))
            .await?;
        let roots = if per_invocation {
            let roots = driver.session_pids(&session);
            if roots.is_empty() {
                set_directory_mode(&workspace, 0o700)?;
                return Err(AhrbError::Protocol(format!(
                    "row-61 repetition {repetition} exposed no owned invocation root"
                )));
            }
            driver.release_invocations().await?;
            roots
        } else {
            let mut ownership_sampler = platform_sampler();
            verified_process_roots(
                manifest,
                ownership_sampler.as_mut(),
                driver.owned_pids(),
                driver.daemon_pid(),
            )?
        };
        let events_result =
            collect_session_terminal(&mut driver, &session, None, outer_turn_timeout(manifest))
                .await;
        let terminal_received_ns = monotonic_timestamp_ns();
        let (events, hung, crashed) = match events_result {
            Ok(events) => (events, false, false),
            Err(AhrbError::Timeout(_)) => (Vec::new(), true, false),
            Err(error) => {
                let exit = if per_invocation {
                    driver.client_exit(&session)
                } else {
                    driver.harness_exit()?
                };
                if matches!(exit, ClientExit::Exited(_)) {
                    (Vec::new(), false, true)
                } else {
                    return Err(error);
                }
            }
        };
        let target_written = std::fs::symlink_metadata(&target).is_ok();
        let workspace_after = row61_tree_snapshot(&workspace)?;
        set_directory_mode(&workspace, 0o700)?;
        let expected_call_id = format!("call-workspace-fault-r{repetition}");
        let write_errno = if crashed || hung {
            None
        } else {
            Some(workspace_result_errno(
                &events,
                &expected_call_id,
                "row-61-denied.txt",
            )?)
        };
        let success_contradiction = workspace_result_claimed_success(&events)
            || events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess);
        let terminal_count = events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count();
        let failure_count = events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalFailure)
            .count();
        let _ = row47_snapshot_file(
            &state_evidence_path,
            &profile_root,
            repetition,
            "workspace-fault-after",
            "workspace-fault-state",
            &mut filesystem_snapshots,
        )?;
        if (manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty())
            && let Err(error) = driver.close(&session).await
            && !crashed
            && !hung
        {
            return Err(error);
        }
        if let Err(error) = driver.shutdown().await
            && !crashed
            && !hung
        {
            return Err(error);
        }
        let mut sampler = platform_sampler();
        let cleared = if roots.is_empty() {
            false
        } else {
            await_owned_tree_empty(sampler.as_mut(), &roots, Duration::from_secs(2)).await?
        };
        let residue_processes = if cleared {
            0_u32
        } else {
            u32::try_from(sampler.discover(&roots)?.members.len()).map_err(|_| {
                AhrbError::Protocol("row-61 residue count does not fit u32".to_owned())
            })?
        };
        server.shutdown().await?;
        let profile_after = row61_tree_snapshot(&profile_root)?;
        let (forbidden_after_sha256, forbidden_after) = row61_snapshot_set(&forbidden_roots)?;
        let outside_writes = row61_unexpected_profile_changes(
            &profile_before,
            &profile_after,
            &allowed_profile_write_roots,
        )
        .saturating_add(u32::from(
            workspace_before.sha256 != workspace_after.sha256
                || forbidden_before_sha256 != forbidden_after_sha256
                || target_written,
        ));
        let ledgers = [
            (
                "workspace-fault-workspace-before",
                serde_json::to_value(&workspace_before)?,
            ),
            (
                "workspace-fault-workspace-after",
                serde_json::to_value(&workspace_after)?,
            ),
            (
                "workspace-fault-profile-before",
                serde_json::to_value(&profile_before)?,
            ),
            (
                "workspace-fault-profile-after",
                serde_json::to_value(&profile_after)?,
            ),
            ("workspace-fault-forbidden-before", forbidden_before),
            ("workspace-fault-forbidden-after", forbidden_after),
        ];
        for (name, ledger) in ledgers {
            row61_write_snapshot_ledger(
                &profile_root,
                repetition,
                name,
                &ledger,
                &mut filesystem_snapshots,
            )?;
        }
        all_requests.extend(engine.request_records().await);
        all_events.extend(events.clone());
        evidence.push(WorkspaceFaultTrial {
            repetition,
            kind: "read-only-directory".to_owned(),
            write_errno,
            control_write_errno: control_errno,
            structured_failure: failure_count == 1 && !success_contradiction,
            terminal_count: u32::try_from(terminal_count).map_err(|_| {
                AhrbError::Protocol("row-61 terminal count does not fit u32".to_owned())
            })?,
            terminal_ms: terminal_received_ns.saturating_sub(operation_start_ns) as f64
                / 1_000_000.0,
            outside_writes,
            residue_processes,
            target_written,
            target_existed_before,
            snapshot_complete: true,
            workspace_snapshot_before_sha256: workspace_before.sha256,
            workspace_snapshot_after_sha256: workspace_after.sha256,
            profile_snapshot_before_sha256: profile_before.sha256,
            profile_snapshot_after_sha256: profile_after.sha256,
            forbidden_snapshot_before_sha256: forbidden_before_sha256,
            forbidden_snapshot_after_sha256: forbidden_after_sha256,
            success_contradiction,
            crashed,
            hung,
        });
    }
    Ok(WorkspaceFaultTrials {
        events: all_events,
        requests: all_requests,
        evidence,
        filesystem_snapshots,
    })
}

fn offline_mode_workflow(
    profile_root: &Path,
    repetition: u32,
    manifest: &Manifest,
) -> Result<Workflow> {
    let scenario = format!("ahrb-row62-r{repetition}");
    let actor_name = format!("r62-offline-r{repetition}");
    let terminal = route_marker(&scenario, &actor_name, "terminal");
    let call = mapped_tool_call(
        manifest,
        "write",
        format!("call-offline-r{repetition}"),
        json!({
            "path":"row-62-provider-only.txt",
            "content":format!("provider-only{terminal}")
        }),
    )?;
    Ok(Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.clone(),
        actors: BTreeMap::from([(
            actor_name.clone(),
            Actor {
                id: actor_name.clone(),
                parent: None,
                prompt: format!(
                    "AHRB offline provider-only tool turn {}",
                    route_marker(&scenario, &actor_name, "start")
                ),
                workspace: profile_root
                    .join("workspace")
                    .to_string_lossy()
                    .into_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: vec![
            ScriptedResponse {
                scenario: scenario.clone(),
                actor: actor_name.clone(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({"tool_calls":[call]}),
                fault: None,
                barrier: None,
            },
            ScriptedResponse {
                scenario,
                actor: actor_name,
                checkpoint: "terminal".to_owned(),
                request_hash: String::new(),
                response: success_value(),
                fault: None,
                barrier: None,
            },
        ],
    })
}

const ROW62_FORBIDDEN_ADDRESS: &str = "203.0.113.1:9";
const ROW62_OWNED_BOUNDARY: &str = "reference-mock-loopback-connector-v1";

struct ReviewedOwnedEgressBoundary {
    confinement_identity: String,
    ledger_path: PathBuf,
    nonce: String,
}

fn reference_mock_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let parent = current
        .parent()
        .ok_or_else(|| AhrbError::Protocol("AHRB executable has no parent directory".to_owned()))?;
    let mut candidates = vec![parent.join("ahrb-mock-harness")];
    if parent.file_name().and_then(|name| name.to_str()) == Some("deps")
        && let Some(target_directory) = parent.parent()
    {
        candidates.push(target_directory.join("ahrb-mock-harness"));
    }
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(std::fs::canonicalize(candidate)?);
        }
    }
    Err(AhrbError::Protocol(
        "reviewed reference mock executable is not installed beside AHRB".to_owned(),
    ))
}

fn reviewed_owned_egress_boundary(
    manifest: &Manifest,
    command: &[String],
    profile_root: &Path,
    manifest_hash: &str,
    repetition: u32,
) -> Result<ReviewedOwnedEgressBoundary> {
    if !matches!(
        manifest.identity.id.as_str(),
        "ahrb-mock" | "ahrb-mock-exec"
    ) || !matches!(
        manifest.transport.kind,
        TransportKind::Exec | TransportKind::StdinRpc
    ) {
        return Err(AhrbError::Protocol(
            "reviewed same-confinement OS guard is unavailable and this adapter is not the owned reference mock"
                .to_owned(),
        ));
    }
    let resolved = resolve_local_program(command)?;
    let program = resolved
        .first()
        .ok_or_else(|| AhrbError::Validation("row-62 harness command is empty".to_owned()))?;
    let actual = std::fs::canonicalize(program).map_err(|error| {
        AhrbError::Protocol(format!(
            "resolve row-62 harness executable {program:?}: {error}"
        ))
    })?;
    let expected = reference_mock_executable()?;
    if actual != expected {
        return Err(AhrbError::Protocol(format!(
            "row-62 owned boundary requires reviewed reference mock {}, resolved {}",
            expected.display(),
            actual.display()
        )));
    }
    let executable_sha256 = row47_hash_file(&actual)?;
    let nonce = format!(
        "{:x}",
        Sha256::digest(
            format!("row62-owned-egress-v1:{manifest_hash}:{executable_sha256}:{repetition}")
                .as_bytes()
        )
    );
    Ok(ReviewedOwnedEgressBoundary {
        confinement_identity: format!("owned-reference-mock-v1:sha256:{executable_sha256}"),
        ledger_path: profile_root.join("owned-egress.jsonl"),
        nonce,
    })
}

fn read_owned_egress_ledger(path: &Path) -> Result<Vec<OwnedEgressLedgerRecord>> {
    let bytes = std::fs::read(path).map_err(|error| {
        AhrbError::Protocol(format!(
            "read row-62 owned egress ledger {}: {error}",
            path.display()
        ))
    })?;
    if bytes.last() != Some(&b'\n') {
        return Err(AhrbError::Protocol(
            "row-62 owned egress ledger has a torn final record".to_owned(),
        ));
    }
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_slice(line).map_err(|error| {
                AhrbError::Protocol(format!(
                    "row-62 owned egress ledger contains invalid JSON: {error}"
                ))
            })
        })
        .collect()
}

fn verify_owned_egress_records<'a>(
    records: &'a [OwnedEgressLedgerRecord],
    boundary: &ReviewedOwnedEgressBoundary,
    owned_root_pids: &BTreeSet<u32>,
    provider_requests: u64,
) -> Result<&'a OwnedEgressLedgerRecord> {
    if records.is_empty() {
        return Err(AhrbError::Protocol(
            "row-62 owned egress ledger is empty".to_owned(),
        ));
    }
    for (index, record) in records.iter().enumerate() {
        let expected_sequence = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        if record.schema != 1
            || record.sequence != expected_sequence
            || record.nonce != boundary.nonce
            || record.boundary != ROW62_OWNED_BOUNDARY
            || !owned_root_pids.contains(&record.pid)
        {
            return Err(AhrbError::Protocol(
                "row-62 owned egress ledger is not bound to the challenged owned process"
                    .to_owned(),
            ));
        }
    }
    let provider_records = records
        .iter()
        .filter(|record| record.category == "provider")
        .collect::<Vec<_>>();
    if u64::try_from(provider_records.len()).ok() != Some(provider_requests)
        || provider_records.iter().any(|record| {
            !record.allowed
                || !matches!(
                    record.outcome.as_str(),
                    "connected-loopback" | "connected-local-ipc"
                )
        })
    {
        return Err(AhrbError::Protocol(
            "row-62 provider observations did not traverse the owned boundary".to_owned(),
        ));
    }
    let control_records = records
        .iter()
        .filter(|record| record.category == "control-probe")
        .collect::<Vec<_>>();
    if control_records.len() != 1
        || records
            .iter()
            .any(|record| !matches!(record.category.as_str(), "provider" | "control-probe"))
    {
        return Err(AhrbError::Protocol(
            "row-62 owned boundary emitted an incomplete attempt audit".to_owned(),
        ));
    }
    let control = control_records.first().copied().ok_or_else(|| {
        AhrbError::Protocol("row-62 owned boundary omitted its control record".to_owned())
    })?;
    if control.destination != ROW62_FORBIDDEN_ADDRESS
        || control.allowed
        || control.outcome != "blocked-permission-denied"
    {
        return Err(AhrbError::Protocol(
            "row-62 public control connect was not refused by the owned boundary".to_owned(),
        ));
    }
    Ok(control)
}

#[cfg(test)]
mod owned_egress_record_tests {
    use super::*;

    fn boundary() -> ReviewedOwnedEgressBoundary {
        ReviewedOwnedEgressBoundary {
            confinement_identity: "owned-reference-mock-v1:sha256:test".to_owned(),
            ledger_path: PathBuf::from("owned-egress.jsonl"),
            nonce: "b".repeat(64),
        }
    }

    fn records() -> Vec<OwnedEgressLedgerRecord> {
        vec![
            OwnedEgressLedgerRecord {
                schema: 1,
                sequence: 1,
                nonce: "b".repeat(64),
                pid: 42,
                boundary: ROW62_OWNED_BOUNDARY.to_owned(),
                destination: ROW62_FORBIDDEN_ADDRESS.to_owned(),
                category: "control-probe".to_owned(),
                allowed: false,
                outcome: "blocked-permission-denied".to_owned(),
            },
            OwnedEgressLedgerRecord {
                schema: 1,
                sequence: 2,
                nonce: "b".repeat(64),
                pid: 42,
                boundary: ROW62_OWNED_BOUNDARY.to_owned(),
                destination: "127.0.0.1:1234".to_owned(),
                category: "provider".to_owned(),
                allowed: true,
                outcome: "connected-loopback".to_owned(),
            },
        ]
    }

    #[test]
    fn owned_egress_oracle_accepts_challenged_root_and_provider_records() -> Result<()> {
        let records = records();
        let control = verify_owned_egress_records(&records, &boundary(), &BTreeSet::from([42]), 1)?;
        assert_eq!(control.category, "control-probe");
        Ok(())
    }

    #[test]
    fn owned_egress_oracle_rejects_ledger_from_outside_owned_root() {
        let records = records();
        let error = verify_owned_egress_records(&records, &boundary(), &BTreeSet::from([99]), 1)
            .expect_err("foreign PID must not satisfy owned confinement");
        assert!(error.to_string().contains("owned process"));
    }
}

async fn collect_offline_mode_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<OfflineModeTrials> {
    let repetitions = match profile {
        Profile::Quick => 1_u32,
        Profile::Cert => 3_u32,
    };
    let mut trials = OfflineModeTrials {
        events: Vec::new(),
        requests: Vec::new(),
        evidence: Vec::new(),
        egress_attempts: Vec::new(),
    };
    for repetition in 1..=repetitions {
        let profile_root = run_profile_root.join(format!("derived-row62-r{repetition}"));
        prepare_profile(manifest, &profile_root)?;
        let workflow = offline_mode_workflow(&profile_root, repetition, manifest)?;
        workflow.validate()?;
        let actor_name = format!("r62-offline-r{repetition}");
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("row-62 repetition {repetition} actor disappeared"))
        })?;
        let engine = Arc::new(FakeModelEngine::with_request_roles(
            &workflow,
            &manifest.model_roles,
            &manifest.request_role_rules,
        )?);
        let (server, model_environment) = start_model(
            Arc::clone(&engine),
            &workflow,
            &profile_root,
            false,
            &manifest.fake_model.base_url_env,
        )
        .await?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                profile_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let credential = format!(
            "ahrb-{}-row62-r{repetition}-{}",
            &manifest_hash[..16],
            std::process::id()
        );
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment);
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.clone(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential);
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &profile_root)?;
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let boundary = reviewed_owned_egress_boundary(
            manifest,
            &command,
            &profile_root,
            manifest_hash,
            repetition,
        )?;
        environment.insert(
            "AHRB_MOCK_OWNED_EGRESS_LEDGER".to_owned(),
            boundary.ledger_path.to_string_lossy().into_owned(),
        );
        environment.insert(
            "AHRB_MOCK_OWNED_EGRESS_NONCE".to_owned(),
            boundary.nonce.clone(),
        );
        environment.insert(
            "AHRB_MOCK_OWNED_EGRESS_FORBIDDEN".to_owned(),
            ROW62_FORBIDDEN_ADDRESS.to_owned(),
        );
        let per_invocation = per_invocation_topology(manifest);
        let mut driver = make_driver_with_timeout(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            per_invocation,
            outer_turn_timeout(manifest),
        )?;
        driver.start().await?;
        driver.await_readiness().await?;
        let session = driver
            .create_session(&format!("{}:row62", workflow.scenario))
            .await?;
        driver
            .submit(&session, &actor.prompt, &format!("row-62-r{repetition}"))
            .await?;
        let owned_roots = if per_invocation {
            let roots = driver.session_pids(&session);
            if roots.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "row-62 repetition {repetition} exposed no owned invocation root"
                )));
            }
            driver.release_invocations().await?;
            roots
        } else {
            let mut ownership_sampler = platform_sampler();
            verified_process_roots(
                manifest,
                ownership_sampler.as_mut(),
                driver.owned_pids(),
                driver.daemon_pid(),
            )?
        };
        let owned_root_pids = owned_roots.into_iter().collect::<BTreeSet<_>>();
        let events =
            collect_session_terminal(&mut driver, &session, None, outer_turn_timeout(manifest))
                .await?;
        let requests = engine.request_records().await;
        let provider_requests = u64::try_from(
            requests
                .iter()
                .filter(|record| record.request.actor == actor_name)
                .count(),
        )
        .map_err(|_| AhrbError::Protocol("row-62 provider request count overflow".to_owned()))?;
        let terminal_success = events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count()
            == 1
            && events
                .iter()
                .filter(|event| is_terminal(&event.event))
                .count()
                == 1;
        let ledger_records = read_owned_egress_ledger(&boundary.ledger_path)?;
        let probe = verify_owned_egress_records(
            &ledger_records,
            &boundary,
            &owned_root_pids,
            provider_requests,
        )?;
        let identity = boundary.confinement_identity.clone();
        let attempt = OfflineAttempt {
            repetition,
            destination: probe.destination.clone(),
            category: "control-probe".to_owned(),
            outcome: probe.outcome.clone(),
            allowed: false,
            confinement_identity: identity.clone(),
        };
        trials.egress_attempts.push(EgressAttempt {
            repetition,
            monotonic_ns: monotonic_timestamp_ns(),
            destination: probe.destination.clone(),
            category: "control-probe".to_owned(),
            allowed: false,
            outcome: attempt.outcome.clone(),
            enforcement: "reviewed reference-mock owned loopback connector".to_owned(),
            confinement_identity: identity.clone(),
        });
        trials.evidence.push(OfflineTrial {
            repetition,
            provider_requests,
            terminal_success,
            control_probe_blocked: true,
            harness_confinement_identity: identity.clone(),
            probe_confinement_identity: identity,
            egress_enforcement: "reviewed reference-mock owned loopback connector".to_owned(),
            attempts: vec![attempt],
        });
        if manifest.transport.kind == TransportKind::Exec
            || !manifest.sessions.close_delete.is_empty()
        {
            driver.close(&session).await?;
        }
        driver.shutdown().await?;
        server.shutdown().await?;
        trials.events.extend(events);
        trials.requests.extend(requests);
    }
    Ok(trials)
}

#[allow(clippy::too_many_arguments)]
async fn collect_streaming_case(
    manifest: &Manifest,
    run_profile_root: &Path,
    manifest_hash: &str,
    row: u8,
    case: &str,
    repetition: u32,
    count: u32,
    outer_deadline_ms: u64,
    measure_cpu: bool,
) -> Result<StreamingCaseResult> {
    let profile_root = run_profile_root.join(format!("derived-row{row}-{case}-r{repetition}"));
    prepare_profile(manifest, &profile_root).map_err(|error| {
        AhrbError::Protocol(format!(
            "prepare row-{row} {case} repetition {repetition} fresh profile: {error}"
        ))
    })?;
    let workflow = streaming_workflow(row, case, &profile_root, repetition, count);
    let actor_name = format!("r{row}-{case}-r{repetition}");
    let actor = workflow
        .actors
        .get(&actor_name)
        .ok_or_else(|| AhrbError::Protocol(format!("row-{row} {case} actor disappeared")))?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        &workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let (server, model_environment) =
        start_streaming_model(Arc::clone(&engine), &manifest.fake_model.base_url_env).await?;
    let mut variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);
    let credential = format!(
        "ahrb-{}-row{row}-{case}-r{repetition}-{}",
        &manifest_hash[..16],
        std::process::id()
    );
    let mut environment = isolated_environment(manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        credential.clone(),
    );
    environment.insert(
        "AHRB_MOCK_MODEL".to_owned(),
        manifest.fake_model.model.clone(),
    );
    variables.insert(
        "base_url".to_owned(),
        environment
            .get(&manifest.fake_model.base_url_env)
            .cloned()
            .unwrap_or_default(),
    );
    variables.insert("credential".to_owned(), credential);
    variables.insert("model".to_owned(), manifest.fake_model.model.clone());
    write_generated_files(manifest, &variables, &profile_root)?;
    let command = if manifest.transport.kind == TransportKind::Exec {
        manifest.transport.command.clone()
    } else {
        render_argv(&manifest.transport.command, &variables)?
    };
    let row_deadline = Duration::from_millis(outer_deadline_ms);
    let per_invocation = per_invocation_topology(manifest);
    // Per-invocation drivers start their process timeout at launch, before the
    // provider header boundary that defines these rows. Keep that internal I/O
    // guard strictly outside the row-local clock; `remaining_row_deadline`
    // below enforces the exact header-anchored outer deadline.
    let transport_deadline =
        Duration::from_millis(outer_deadline_ms.saturating_add(manifest.resources.turn_timeout_ms));
    let mut driver = make_driver_with_timeout(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        measure_cpu && per_invocation,
        transport_deadline,
    )?;
    driver.start().await?;
    driver.await_readiness().await?;
    let session = driver
        .create_session(&format!("{}:row{row}-{case}", workflow.scenario))
        .await?;
    let mut sampler = measure_cpu.then(platform_sampler);
    let mut cpu_samples = Vec::new();
    let mut roots = if per_invocation {
        Vec::new()
    } else {
        let mut roots = driver.owned_pids();
        if let Some(pid) = driver.daemon_pid() {
            roots.push(pid);
        }
        roots.sort_unstable();
        roots.dedup();
        roots
    };
    if let Some(sampler) = sampler.as_deref_mut()
        && !per_invocation
    {
        cpu_samples.push(sample_streaming_cpu(sampler, &roots)?);
    }
    driver
        .submit(
            &session,
            &actor.prompt,
            &format!("row-{row}-{case}-r{repetition}"),
        )
        .await?;
    if per_invocation {
        roots = driver.session_pids(&session);
        if let Some(sampler) = sampler.as_deref_mut() {
            cpu_samples.push(sample_streaming_cpu(sampler, &roots)?);
            // Release only after the pre-header CPU point is durable, proving
            // that the header boundary cannot precede the left bracket.
            driver.release_invocations().await?;
        }
    }
    let header_record = wait_for_response_headers(&engine, &actor_name, row_deadline).await?;
    let response_headers_ns = header_record
        .response_headers_ns
        .ok_or_else(|| AhrbError::Protocol("provider header boundary disappeared".to_owned()))?;
    if let Some(sampler) = sampler.as_deref_mut() {
        cpu_samples.push(sample_streaming_cpu(sampler, &roots)?);
    }
    let frames = if case == "stall" {
        Vec::new()
    } else {
        wait_for_paced_frames(PacedFrameWait {
            engine: &engine,
            actor: &actor_name,
            expected_count: count,
            response_headers_ns,
            outer_deadline_ms,
            sampler: sampler.as_mut(),
            roots: &roots,
            cpu_samples: &mut cpu_samples,
        })
        .await?
    };
    let cpu_ns = if measure_cpu {
        let final_frame_yield_ns = frames
            .last()
            .map(|frame| frame.frame_yielded_ns)
            .ok_or_else(|| AhrbError::Protocol("model-wait final frame disappeared".to_owned()))?;
        interpolated_model_wait_cpu_ns(&cpu_samples, response_headers_ns, final_frame_yield_ns)
            .map_err(AhrbError::Protocol)?
    } else {
        0
    };
    let terminal_result = match remaining_row_deadline(response_headers_ns, outer_deadline_ms) {
        Ok(remaining) => {
            collect_session_terminal_with_poll(
                &mut driver,
                &session,
                None,
                remaining,
                Duration::from_millis(2),
            )
            .await
        }
        Err(error) => Err(error),
    };
    let (events, terminal_ns, outer_kill_used, outer_kill_ns) = match terminal_result {
        Ok(events) => {
            let terminal_ns = monotonic_timestamp_ns();
            if manifest.transport.kind == TransportKind::Exec
                || !manifest.sessions.close_delete.is_empty()
            {
                driver.close(&session).await?;
            }
            driver.shutdown().await?;
            (events, Some(terminal_ns), false, None)
        }
        Err(AhrbError::Timeout(_)) => {
            let outer_kill_ns = monotonic_timestamp_ns();
            cleanup_streaming_outer_kill(
                &mut driver,
                &roots,
                Duration::from_millis(manifest.daemon.grace_ms.max(500)),
            )
            .await?;
            (Vec::new(), None, true, Some(outer_kill_ns))
        }
        Err(error) => return Err(error),
    };
    server.shutdown().await?;
    let requests = engine.request_records().await;
    Ok(StreamingCaseResult {
        actor: actor_name,
        response_headers_ns,
        terminal_ns,
        outer_kill_used,
        outer_kill_ns,
        cpu_ns,
        cpu_samples,
        frames,
        events,
        requests,
    })
}

fn streaming_chunks(
    repetition: u32,
    case: &str,
    actor: &str,
    frames: &[crate::fake_model::ModelFrameObservation],
) -> Vec<StreamChunkObservation> {
    frames
        .iter()
        .map(|frame| StreamChunkObservation {
            repetition,
            actor: actor.to_owned(),
            case: case.to_owned(),
            ordinal: frame.ordinal,
            scheduled_ns: frame.scheduled_ns,
            frame_yielded_ns: frame.frame_yielded_ns,
            bytes: frame.bytes,
        })
        .collect()
}

async fn collect_model_wait_cpu_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<ModelWaitCpuTrials> {
    let repetitions = ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).repetitions;
    let count = match profile {
        Profile::Quick => 5_u32,
        Profile::Cert => 20_u32,
    };
    let outer_deadline_ms = u64::from(count)
        .saturating_mul(1_000)
        .saturating_add(manifest.resources.idle_timeout_ms)
        .saturating_add(3_000);
    let mut trials = ModelWaitCpuTrials {
        events: Vec::new(),
        requests: Vec::new(),
        evidence: Vec::new(),
        stream_chunks: Vec::new(),
    };
    for repetition in 1..=repetitions {
        let trial = collect_streaming_case(
            manifest,
            run_profile_root,
            manifest_hash,
            48,
            "model-wait",
            repetition,
            count,
            outer_deadline_ms,
            true,
        )
        .await?;
        let success_terminals = trial
            .events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count() as u32;
        let terminal_count = trial
            .events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count() as u32;
        trials.stream_chunks.extend(streaming_chunks(
            repetition,
            "model-wait",
            &trial.actor,
            &trial.frames,
        ));
        trials.evidence.push(ModelWaitCpuTrialEvidence {
            repetition,
            response_headers_ns: trial.response_headers_ns,
            terminal_ns: trial.terminal_ns,
            cpu_ns: trial.cpu_ns,
            cpu_samples: trial.cpu_samples,
            frames: trial.frames,
            terminal_count,
            success_terminals,
            outer_kill_used: trial.outer_kill_used,
            outer_kill_ns: trial.outer_kill_ns,
            outer_deadline_ms,
        });
        trials.events.extend(trial.events);
        trials.requests.extend(trial.requests);
    }
    Ok(trials)
}

async fn collect_slow_stream_stall_trials(
    manifest: &Manifest,
    profile: Profile,
    run_profile_root: &Path,
    manifest_hash: &str,
) -> Result<SlowStreamStallTrials> {
    if manifest.resources.idle_timeout_ms < 1_250 {
        return Err(AhrbError::Validation(format!(
            "row-59 requires idle_timeout_ms>=1250; got {}",
            manifest.resources.idle_timeout_ms
        )));
    }
    let repetitions = ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).repetitions;
    let count = match profile {
        Profile::Quick => 5_u32,
        Profile::Cert => 20_u32,
    };
    let slow_outer_deadline_ms = u64::from(count)
        .saturating_mul(1_000)
        .saturating_add(manifest.resources.idle_timeout_ms)
        .saturating_add(3_000);
    let stall_outer_deadline_ms = manifest.resources.idle_timeout_ms.saturating_add(3_000);
    let mut trials = SlowStreamStallTrials {
        events: Vec::new(),
        requests: Vec::new(),
        evidence: Vec::new(),
        stream_chunks: Vec::new(),
    };
    for repetition in 1..=repetitions {
        let slow = collect_streaming_case(
            manifest,
            run_profile_root,
            manifest_hash,
            59,
            "slow",
            repetition,
            count,
            slow_outer_deadline_ms,
            false,
        )
        .await?;
        let stall = collect_streaming_case(
            manifest,
            run_profile_root,
            manifest_hash,
            59,
            "stall",
            repetition,
            0,
            stall_outer_deadline_ms,
            false,
        )
        .await?;
        let slow_success_terminals = slow
            .events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count() as u32;
        let slow_terminal_count = slow
            .events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count() as u32;
        let slow_idle_timeout_fired = slow.events.iter().any(|event| {
            event.event == EventVocab::TerminalTimeout
                || event.event == EventVocab::TerminalFailure
                    && event.payload.get("category").and_then(Value::as_str) == Some("idle-timeout")
        });
        let stall_terminal_count = stall
            .events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count() as u32;
        let stall_failure_terminals = stall
            .events
            .iter()
            .filter(|event| {
                event.event == EventVocab::TerminalTimeout
                    || event.event == EventVocab::TerminalFailure
                        && event.payload.get("category").and_then(Value::as_str)
                            == Some("idle-timeout")
            })
            .count() as u32;
        trials.stream_chunks.extend(streaming_chunks(
            repetition,
            "slow",
            &slow.actor,
            &slow.frames,
        ));
        trials.evidence.push(SlowStreamStallTrialEvidence {
            repetition,
            slow_response_headers_ns: slow.response_headers_ns,
            slow_terminal_ns: slow.terminal_ns,
            slow_frames: slow.frames,
            slow_terminal_count,
            slow_success_terminals,
            slow_idle_timeout_fired,
            slow_outer_kill_used: slow.outer_kill_used,
            slow_outer_kill_ns: slow.outer_kill_ns,
            slow_outer_deadline_ms,
            stall_response_headers_ns: stall.response_headers_ns,
            stall_terminal_ns: stall.terminal_ns,
            stall_bytes_yielded: stall.frames.iter().map(|frame| frame.bytes).sum(),
            stall_terminal_count,
            stall_failure_terminals,
            stall_outer_kill_used: stall.outer_kill_used,
            stall_outer_kill_ns: stall.outer_kill_ns,
            stall_outer_deadline_ms,
        });
        trials.events.extend(slow.events);
        trials.events.extend(stall.events);
        trials.requests.extend(slow.requests);
        trials.requests.extend(stall.requests);
    }
    Ok(trials)
}

fn add_resource_workflow(
    scenario: &str,
    profile_root: &Path,
    timing: ResourceTimingPlan,
    manifest: &Manifest,
    actors: &mut BTreeMap<String, Actor>,
    responses: &mut Vec<ScriptedResponse>,
) -> Result<()> {
    for repetition in 0..timing.repetitions {
        for turn in 1..=timing.warmup_turns {
            let actor = resource_warmup_actor(repetition, turn);
            let terminal = route_marker(scenario, &actor, "terminal");
            actors.insert(
                actor.clone(),
                Actor {
                    id: actor.clone(),
                    parent: None,
                    prompt: format!(
                        "AHRB resource warm-up {}",
                        route_marker(scenario, &actor, "start")
                    ),
                    workspace: profile_root
                        .join("resource-workspaces")
                        .join(&actor)
                        .to_string_lossy()
                        .into_owned(),
                },
            );
            let arguments = serde_json::Map::from_iter([
                (
                    "path".to_owned(),
                    Value::String(format!("warmup-{turn}.txt")),
                ),
                (
                    "content".to_owned(),
                    Value::String(format!("warmup {terminal}")),
                ),
            ]);
            let call = mapped_tool_call(
                manifest,
                "write",
                format!("resource-warmup-r{repetition}-t{turn}"),
                Value::Object(arguments),
            )?;
            responses.extend([
                ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint: "start".to_owned(),
                    request_hash: String::new(),
                    response: json!({"tool_calls":[call]}),
                    fault: None,
                    barrier: None,
                },
                ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor,
                    checkpoint: "terminal".to_owned(),
                    request_hash: String::new(),
                    response: success_value(),
                    fault: None,
                    barrier: None,
                },
            ]);
        }
        for agents in &timing.sweep_widths {
            for index in 0..*agents {
                let actor = resource_sweep_actor(repetition, *agents, index);
                let terminal = route_marker(scenario, &actor, "terminal");
                actors.insert(
                    actor.clone(),
                    Actor {
                        id: actor.clone(),
                        parent: None,
                        prompt: format!(
                            "AHRB resource sweep {}",
                            route_marker(scenario, &actor, "start")
                        ),
                        workspace: profile_root
                            .join("resource-workspaces")
                            .join(&actor)
                            .to_string_lossy()
                            .into_owned(),
                    },
                );
                let arguments = serde_json::Map::from_iter([
                    (
                        "path".to_owned(),
                        Value::String("resource-fixture.txt".to_owned()),
                    ),
                    (
                        "content".to_owned(),
                        Value::String(format!("resource {terminal}")),
                    ),
                ]);
                let call = mapped_tool_call(
                    manifest,
                    "write",
                    format!("resource-r{repetition}-n{agents}-a{}", index + 1),
                    Value::Object(arguments),
                )?;
                responses.extend([
                    ScriptedResponse {
                        scenario: scenario.to_owned(),
                        actor: actor.clone(),
                        checkpoint: "start".to_owned(),
                        request_hash: String::new(),
                        response: json!({"tool_calls":[call]}),
                        fault: None,
                        barrier: None,
                    },
                    ScriptedResponse {
                        scenario: scenario.to_owned(),
                        actor,
                        checkpoint: "terminal".to_owned(),
                        request_hash: String::new(),
                        response: success_value(),
                        fault: None,
                        barrier: None,
                    },
                ]);
            }
        }

        let actor = resource_long_actor(repetition);
        actors.insert(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: format!(
                    "AHRB long horizon {}",
                    route_marker(scenario, &actor, &resource_long_checkpoint(repetition, 1))
                ),
                workspace: profile_root
                    .join("resource-workspaces")
                    .join(&actor)
                    .to_string_lossy()
                    .into_owned(),
            },
        );
        for turn in 1..=timing.long_horizon_turns {
            let checkpoint = resource_long_checkpoint(repetition, turn);
            if turn % 10 == 0 {
                let terminal_checkpoint = format!("{checkpoint}-terminal");
                let terminal = route_marker(scenario, &actor, &terminal_checkpoint);
                let call = mapped_tool_call(
                    manifest,
                    "write",
                    format!("resource-long-r{repetition}-t{turn}"),
                    json!({
                        "path":format!("turn-{turn}.txt"),
                        "content":format!("turn {turn} {terminal}")
                    }),
                )?;
                responses.push(ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint: checkpoint.clone(),
                    request_hash: String::new(),
                    response: json!({"tool_calls":[call]}),
                    fault: None,
                    barrier: None,
                });
                responses.push(ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint: terminal_checkpoint,
                    request_hash: String::new(),
                    response: success_value(),
                    fault: None,
                    barrier: None,
                });
            } else {
                responses.push(ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint,
                    request_hash: String::new(),
                    response: success_value(),
                    fault: None,
                    barrier: None,
                });
            }
        }
    }
    Ok(())
}

fn resource_sweep_actor(repetition: u32, agents: u32, index: u32) -> String {
    format!("resource-r{repetition}-n{agents}-a{}", index + 1)
}

fn resource_warmup_actor(repetition: u32, turn: u32) -> String {
    format!("resource-r{repetition}-warmup-{turn}")
}

fn resource_barrier_checkpoint(repetition: u32, agents: u32) -> String {
    format!("resource-r{repetition}-n{agents}-steady")
}

fn resource_long_actor(repetition: u32) -> String {
    format!("resource-r{repetition}-long")
}

fn resource_long_checkpoint(repetition: u32, turn: u32) -> String {
    format!("long-r{repetition}-t{turn}")
}

fn mapped_tool_call(
    manifest: &Manifest,
    semantic: &str,
    call_id: String,
    semantic_arguments: Value,
) -> Result<Value> {
    let abstract_name = format!("{semantic}_fixture");
    let object = semantic_arguments.as_object().ok_or_else(|| {
        AhrbError::Validation(format!(
            "semantic tool {semantic:?} arguments are not an object"
        ))
    })?;
    let Some(alias) = manifest.tools.aliases.get(semantic) else {
        return Ok(json!({
            "id": call_id,
            "name": abstract_name,
            "arguments": semantic_arguments
        }));
    };
    let template = manifest.tools.fixtures.get(semantic).ok_or_else(|| {
        AhrbError::Validation(format!(
            "tool {semantic:?} declares a native alias but has no fixture argv"
        ))
    })?;
    let mut variables = BTreeMap::from([
        ("ahrb_fixture".to_owned(), fixture_program()?),
        ("workspace".to_owned(), ".".to_owned()),
    ]);
    for (key, value) in object {
        let rendered = match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        };
        if let Some(value) = rendered {
            variables.insert(key.clone(), value);
        }
    }
    let argv = template
        .iter()
        .map(|argument| crate::manifest::render_template(argument, &variables))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "id": call_id,
        "name": abstract_name,
        "arguments": semantic_arguments,
        "_ahrb_native": {
            "semantic": semantic,
            "aliases": alias.candidates(),
            "bindings": manifest.tools.bindings,
            "argv": argv
        }
    }))
}

fn fixture_program() -> Result<String> {
    let executable = std::env::current_exe()?;
    if let Some(parent) = executable.parent() {
        let sibling = parent.join("ahrb-fixture");
        if sibling.is_file() {
            return Ok(sibling.to_string_lossy().into_owned());
        }
    }
    Ok("ahrb-fixture".to_owned())
}

fn scripted_row(
    row: u8,
    scenario: &str,
    actor: &str,
    manifest: &Manifest,
) -> Result<Vec<ScriptedResponse>> {
    let response = |checkpoint: &str, value: Value, fault: Option<Fault>| ScriptedResponse {
        scenario: scenario.to_owned(),
        actor: actor.to_owned(),
        checkpoint: checkpoint.to_owned(),
        request_hash: String::new(),
        response: value,
        fault,
        barrier: None,
    };
    let terminal = route_marker(scenario, actor, "terminal");
    let scripts = match row {
        67 => {
            let tool_turn = actor.ends_with("t3") || actor.ends_with("t20");
            if tool_turn {
                let call = mapped_tool_call(
                    manifest,
                    "write",
                    format!("row67-{actor}"),
                    json!({
                        "path": format!("{actor}.txt"),
                        "content": format!("usage-tool-turn{terminal}")
                    }),
                )?;
                vec![
                    response(
                        "start",
                        json!({
                            "tool_calls": [call],
                            "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
                        }),
                        None,
                    ),
                    response(
                        "terminal",
                        json!({
                            "text": "SUCCESS",
                            "_ahrb_usage": {"input_tokens": 140, "output_tokens": 30}
                        }),
                        None,
                    ),
                ]
            } else {
                vec![response(
                    "start",
                    json!({
                        "text": "SUCCESS",
                        "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
                    }),
                    None,
                )]
            }
        }
        69 if actor.ends_with('s') => {
            let call = mapped_tool_call(
                manifest,
                "write",
                format!("row69-{actor}"),
                json!({
                    "path": format!("{actor}.txt"),
                    "content": format!("event-stream{terminal}")
                }),
            )?;
            vec![
                response(
                    "start",
                    json!({
                        "tool_calls": [call],
                        "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
                    }),
                    None,
                ),
                response(
                    "terminal",
                    json!({
                        "text": "SUCCESS",
                        "_ahrb_usage": {"input_tokens": 140, "output_tokens": 30}
                    }),
                    None,
                ),
            ]
        }
        69 => vec![response(
            "start",
            json!({
                "text": "{\"status\":\"FAILURE\",\"category\":\"scripted\"}",
                "_ahrb_usage": {"input_tokens": 100, "output_tokens": 20}
            }),
            None,
        )],
        4 => {
            let first = mapped_tool_call(
                manifest,
                "write",
                "parallel-a".to_owned(),
                json!({"path":"parallel-a.txt","content":format!("A{terminal}")}),
            )?;
            let second = mapped_tool_call(
                manifest,
                "write",
                "parallel-b".to_owned(),
                json!({"path":"parallel-b.txt","content":format!("B{terminal}")}),
            )?;
            vec![
                response("start", json!({"tool_calls":[first, second]}), None),
                response("terminal", success_value(), None),
            ]
        }
        2 | 8 | 13 | 15 | 37 => {
            let call = mapped_tool_call(
                manifest,
                "write",
                format!("call-r{row}"),
                json!({"path":format!("row-{row}.txt"),"content":format!("row-{row}{terminal}")}),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        3 => {
            let second = route_marker(scenario, actor, "second");
            let first_call = mapped_tool_call(
                manifest,
                "write",
                "call-a".to_owned(),
                json!({"path":"a.txt","content":"A","route":second}),
            )?;
            let second_call = mapped_tool_call(
                manifest,
                "read",
                "call-b".to_owned(),
                json!({"path":"a.txt","expected_from_a":"A","route":terminal}),
            )?;
            vec![
                response("start", json!({"tool_calls":[first_call]}), None),
                response("second", json!({"tool_calls":[second_call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        5 => vec![response(
            "start",
            success_value(),
            Some(Fault::Fragment {
                boundaries: vec![1, 7, 13],
            }),
        )],
        6 => {
            let call = mapped_tool_call(
                manifest,
                "fail",
                "call-fail".to_owned(),
                json!({"message":format!("expected failure {terminal}")}),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        7 => vec![response(
            "start",
            json!({"tool_calls":[{"id":"call-malformed","name":"unknown_fixture","arguments":"{not-json"}]}),
            None,
        )],
        10 => vec![response(
            "start",
            json!({"text":"{\"status\":\"FAILURE\",\"category\":\"scripted\"}"}),
            None,
        )],
        11 => vec![response(
            "start",
            success_value(),
            Some(Fault::HttpStatus {
                status: 429,
                body: "{\"error\":\"transient\"}".to_owned(),
            }),
        )],
        12 | 36 => vec![response("start", success_value(), Some(Fault::Stall))],
        35 => {
            let call = mapped_tool_call(
                manifest,
                "write",
                "call-crash-recovery".to_owned(),
                json!({
                    "path": "row-35-committed.txt",
                    "content": format!("committed-once{terminal}"),
                    "ahrb_checkpoint": {
                        "name": "row-35-post-commit",
                        "phase": "after-commit"
                    }
                }),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        40 => {
            let call = mapped_tool_call(
                manifest,
                "write",
                "call-durable-journal".to_owned(),
                json!({
                    "path": "row-40-committed.txt",
                    "content": format!("durable{terminal}"),
                    "ahrb_checkpoint": {
                        "name": "row-40-post-commit",
                        "phase": "after-commit"
                    }
                }),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        60 => {
            let call = mapped_tool_call(
                manifest,
                "large_output",
                "call-large-output".to_owned(),
                json!({"bytes":10_485_760_u64,"route":terminal}),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        61 => {
            let call = mapped_tool_call(
                manifest,
                "write",
                "call-workspace-fault".to_owned(),
                json!({"path":"row-61-denied.txt","content":format!("must-not-be-written{terminal}")}),
            )?;
            vec![
                response("start", json!({"tool_calls":[call]}), None),
                response(
                    "terminal",
                    json!({"text":"{\"status\":\"FAILURE\",\"category\":\"workspace\"}"}),
                    None,
                ),
            ]
        }
        71 if actor.ends_with('2') || actor.ends_with('4') || actor.ends_with('6') => {
            vec![response(
                "start",
                json!({"text":"{\"status\":\"FAILURE\",\"category\":\"provider\"}"}),
                None,
            )]
        }
        72 => {
            let after_success = route_marker(scenario, actor, "after-success");
            let success_call = mapped_tool_call(
                manifest,
                "write",
                format!("row72-success-{actor}"),
                json!({
                    "path": "row-72-success.txt",
                    "content": format!("structured success {after_success}"),
                }),
            )?;
            let failed_call = mapped_tool_call(
                manifest,
                "fail",
                format!("row72-failure-{actor}"),
                json!({"message": format!("expected structured failure {terminal}")}),
            )?;
            vec![
                response("start", json!({"tool_calls":[success_call]}), None),
                response("after-success", json!({"tool_calls":[failed_call]}), None),
                response("terminal", success_value(), None),
            ]
        }
        _ => vec![response("start", success_value(), None)],
    };
    Ok(scripts)
}

fn success_value() -> Value {
    json!({"text":"{\"status\":\"SUCCESS\"}"})
}

fn route_marker(scenario: &str, actor: &str, checkpoint: &str) -> String {
    format!("[[AHRB:scenario={scenario};actor={actor};checkpoint={checkpoint}]]")
}

async fn collect_terminals(
    driver: &mut HarnessDriver,
    sessions: &BTreeMap<u8, Vec<crate::driver::SessionId>>,
    deadline: Duration,
    progress: &RunProgress,
) -> Result<(BTreeMap<u8, Vec<NormalizedEvent>>, BTreeMap<u8, String>)> {
    let started = Instant::now();
    let mut complete: BTreeSet<(u8, String)> = BTreeSet::new();
    let mut evidence = BTreeMap::new();
    let mut errors = BTreeMap::new();
    loop {
        for (row, row_sessions) in sessions {
            if errors.contains_key(row) {
                continue;
            }
            for session in row_sessions {
                if complete.contains(&(*row, session.0.clone())) {
                    continue;
                }
                let events = match driver.attach(session, None).await {
                    Ok(events) => events,
                    Err(AhrbError::Timeout(detail)) => {
                        let detail = format!("turn timeout: {detail}");
                        errors.insert(*row, detail.clone());
                        progress.update(|state| {
                            state.row_errors.insert(*row, detail);
                        })?;
                        for row_session in row_sessions {
                            let _ = driver.cancel(row_session).await;
                        }
                        break;
                    }
                    Err(error) => return Err(error),
                };
                if events.iter().any(|event| is_terminal(&event.event)) {
                    complete.insert((*row, session.0.clone()));
                    evidence.entry(*row).or_insert_with(Vec::new).extend(events);
                    let row_complete = row_sessions
                        .iter()
                        .all(|item| complete.contains(&(*row, item.0.clone())));
                    if row_complete && let Some(row_events) = evidence.get(row) {
                        progress.update(|state| {
                            state.completed.insert(*row);
                            state.events.insert(*row, row_events.clone());
                        })?;
                    }
                }
            }
        }
        let expected: usize = sessions.values().map(Vec::len).sum();
        let errored_sessions: usize = errors
            .keys()
            .filter_map(|row| sessions.get(row))
            .map(Vec::len)
            .sum();
        if complete.len().saturating_add(errored_sessions) == expected {
            progress.update(|state| {
                for (row, events) in &evidence {
                    state.completed.insert(*row);
                    state.events.insert(*row, events.clone());
                }
            })?;
            return Ok((evidence, errors));
        }
        if started.elapsed() >= deadline {
            let detail = format!(
                "only {}/{} sessions terminalized within {} ms",
                complete.len(),
                expected,
                deadline.as_millis()
            );
            for (row, row_sessions) in sessions {
                if errors.contains_key(row) {
                    continue;
                }
                let row_complete = row_sessions
                    .iter()
                    .all(|session| complete.contains(&(*row, session.0.clone())));
                if row_complete {
                    if let Some(events) = evidence.get(row) {
                        progress.update(|state| {
                            state.completed.insert(*row);
                            state.events.insert(*row, events.clone());
                        })?;
                    }
                } else {
                    let row_detail = format!("turn timeout: {detail}");
                    errors.insert(*row, row_detail.clone());
                    progress.update(|state| {
                        state.row_errors.insert(*row, row_detail);
                    })?;
                    for session in row_sessions {
                        let _ = driver.cancel(session).await;
                    }
                }
            }
            return Ok((evidence, errors));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn collect_session_terminal(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<Cursor>,
    deadline: Duration,
) -> Result<Vec<NormalizedEvent>> {
    collect_session_terminal_with_poll(driver, session, after, deadline, Duration::from_millis(10))
        .await
}

async fn collect_session_terminal_with_poll(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<Cursor>,
    deadline: Duration,
    poll_interval: Duration,
) -> Result<Vec<NormalizedEvent>> {
    let started = Instant::now();
    loop {
        let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
            AhrbError::Timeout(format!("session {} did not terminalize", session.0))
        })?;
        let events = tokio::time::timeout(remaining, driver.attach(session, after))
            .await
            .map_err(|_| {
                AhrbError::Timeout(format!(
                    "session {} attach exceeded its terminal deadline",
                    session.0
                ))
            })??;
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Ok(events);
        }
        let remaining = deadline.checked_sub(started.elapsed()).ok_or_else(|| {
            AhrbError::Timeout(format!("session {} did not terminalize", session.0))
        })?;
        tokio::time::sleep(poll_interval.min(remaining)).await;
    }
}

#[cfg(test)]
mod terminal_deadline_tests {
    use super::*;
    use crate::driver::{DriverFuture, SessionId, TransportRequest, TransportResponse};

    struct BlockingAttachTransport;

    impl Transport for BlockingAttachTransport {
        fn start(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn request(&mut self, _request: TransportRequest) -> DriverFuture<'_, TransportResponse> {
            Box::pin(std::future::pending())
        }

        fn stop(&mut self) -> DriverFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn terminal_collector_bounds_an_inflight_attach_by_the_absolute_deadline() {
        let mut driver: HarnessDriver = Box::new(GenericDriver::new(BlockingAttachTransport));
        let session = SessionId("blocked-attach".to_owned());
        let started = Instant::now();
        let result = collect_session_terminal_with_poll(
            &mut driver,
            &session,
            None,
            Duration::from_millis(20),
            Duration::from_millis(1),
        )
        .await;
        assert!(matches!(result, Err(AhrbError::Timeout(_))));
        assert!(started.elapsed() < Duration::from_millis(250));
    }
}

async fn wait_for_session_event(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    expected: EventVocab,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let events = driver.attach(session, None).await?;
        if events.iter().any(|event| event.event == expected) {
            return Ok(());
        }
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Err(AhrbError::Protocol(format!(
                "session {} terminalized before {:?}",
                session.0, expected
            )));
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "session {} did not emit {:?}",
                session.0, expected
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn collect_session_checkpoint(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    checkpoint: &str,
    deadline: Duration,
) -> Result<Vec<NormalizedEvent>> {
    let started = Instant::now();
    loop {
        let events = driver.attach(session, None).await?;
        if events.iter().any(|event| {
            event.event == EventVocab::BarrierReached
                && event.payload.get("name").and_then(Value::as_str) == Some(checkpoint)
        }) {
            return Ok(events);
        }
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Err(AhrbError::Protocol(format!(
                "session {} terminalized before named checkpoint {checkpoint:?}",
                session.0
            )));
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "session {} did not reach named checkpoint {checkpoint:?}",
                session.0
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

fn outer_turn_timeout(manifest: &Manifest) -> Duration {
    Duration::from_millis(manifest.resources.turn_timeout_ms.saturating_add(3_000))
}

fn fixture_effect_matches(
    profile_root: &Path,
    state: &RunState,
    row: u8,
    relative: &str,
    expected: &str,
) -> bool {
    state
        .sessions
        .get(&row)
        .into_iter()
        .flatten()
        .any(|session| {
            [
                profile_root
                    .join("ahrb-exec-sessions")
                    .join(&session.0)
                    .join("workspace")
                    .join(relative),
                profile_root
                    .join("state")
                    .join("workspaces")
                    .join(&session.0)
                    .join(relative),
            ]
            .iter()
            .any(|path| {
                std::fs::read_to_string(path)
                    .map(|content| content == expected)
                    .unwrap_or(false)
            })
        })
}

fn capability_for_profile(
    manifest: &Manifest,
    row: u8,
    profile: Profile,
) -> crate::matrix_evidence::CapabilityStatus {
    let capability = crate::matrix_evidence::capability_for_row(manifest, row);
    if matches!(
        capability,
        crate::matrix_evidence::CapabilityStatus::Supported
    ) && matches!(row, 54 | 55)
        && profile == Profile::Cert
        && manifest
            .concurrency
            .max_agents
            .is_some_and(|value| value < 32)
    {
        crate::matrix_evidence::CapabilityStatus::Unsupported(
            "fanout architecture explicitly declares concurrency below cert N=32".to_owned(),
        )
    } else {
        capability
    }
}

fn capability_result(
    definition: &crate::scenarios::TestDefinition,
    capability: crate::matrix_evidence::CapabilityStatus,
) -> Option<TestResult> {
    if matches!(
        capability,
        crate::matrix_evidence::CapabilityStatus::Supported
    ) {
        return None;
    }
    let (declaration, reason) = match capability {
        crate::matrix_evidence::CapabilityStatus::Unsupported(reason) => (Some(false), reason),
        crate::matrix_evidence::CapabilityStatus::Absent(reason) => (None, reason),
        crate::matrix_evidence::CapabilityStatus::Supported => (Some(true), String::new()),
    };
    let mut result = classify(
        definition.row,
        definition.id,
        definition.pillar,
        declaration,
        &[],
        None,
    );
    if matches!(definition.row, 65 | 66 | 67 | 68 | 70) {
        result.metadata.score = Some(0.0);
    }
    result.evidence.push(format!("capability: {reason}"));
    Some(result)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_rows(
    selected: &[&crate::scenarios::TestDefinition],
    state: &RunState,
    requests: &[crate::fake_model::ModelRequestRecord],
    manifest: &Manifest,
    resources: &ResourceCertification,
    profile_root: &Path,
    derived: &DerivedRowEvaluations<'_>,
    profile: Profile,
) -> Vec<TestResult> {
    let row65 = derived.injection_surface;
    let row66 = derived.budget_enforcement;
    let row67 = derived.usage_reporting;
    let row68 = derived.session_ops_cli;
    let row69 = derived.event_stream_completeness;
    let row70 = derived.headless_permissions;
    let row71 = derived.secrets_hygiene;
    let row42 = derived.model_request_efficiency;
    let row43 = derived.turn_latency;
    let row49 = derived.latency_vs_turn_index;
    let row50 = derived.session_residue;
    let row51 = derived.context_limit_recovery;
    let row52 = derived.resume_latency;
    let row53 = derived.journal_torn_tail;
    let row54 = derived.fanout_cliff;
    let row55 = derived.fairness;
    let row44 = derived.process_hygiene;
    let row45 = derived.time_to_first_model_request;
    let row46 = derived.memory_time_integral;
    let row48 = derived.model_wait_cpu;
    let row56 = derived.child_failure_propagation;
    let row57 = derived.signal_matrix;
    let row59 = derived.slow_stream_vs_stall;
    let row60 = derived.large_tool_output;
    let row61 = derived.workspace_fault;
    let row62 = derived.offline_mode;
    let row47 = derived.disk_io_per_turn;
    let row58 = derived.retry_budget;
    let row63 = derived.nondeterministic_fields;
    let row64 = derived.cross_run_reproducibility;
    let row72 = evaluate_tool_result_role_fidelity(
        requests,
        match profile {
            Profile::Quick => 1,
            Profile::Cert => 5,
        },
    );
    selected
        .iter()
        .map(|definition| {
            if let Some(error) = state.row_errors.get(&definition.row) {
                return TestResult {
                    row: definition.row,
                    id: definition.id.to_owned(),
                    pillar: definition.pillar,
                    outcome: TestOutcome::Error(error.clone()),
                    evidence: vec![error.clone()],
                    metadata: TestResultMetadata::for_row(
                        definition.row,
                        &TestOutcome::Error(error.clone()),
                    ),
                };
            }
            let capability = capability_for_profile(manifest, definition.row, profile);
            if let Some(result) = capability_result(definition, capability) {
                return result;
            }
            if (20..=29).contains(&definition.row) {
                return resources
                    .rows
                    .iter()
                    .find(|result| result.row == definition.row)
                    .cloned()
                    .unwrap_or_else(|| {
                        classify(
                            definition.row,
                            definition.id,
                            definition.pillar,
                            Some(true),
                            &[],
                            Some("resource certification omitted this matrix row".to_owned()),
                        )
                    });
            }
            if definition.row == 42 {
                if !row42.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some("model-request-efficiency evidence is incomplete".to_owned()),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row42.reference_envelope_pass,
                        detail: format!(
                            "requests/turn={:.3}, primary/turn={:.3}, side-channel/turn={:.3}, retries/turn={:.3}, context p95={:.0} bytes, slope={:.3} bytes/turn",
                            row42.metrics["model_request_efficiency.requests_per_semantic_turn"],
                            row42.metrics["model_request_efficiency.primary_requests_per_turn"],
                            row42.metrics["model_request_efficiency.side_channel_requests_per_turn"],
                            row42.metrics["model_request_efficiency.retry_attempts_per_turn"],
                            row42.metrics["model_request_efficiency.context_tax_bytes_p95"],
                            row42.metrics["model_request_efficiency.context_tax_slope_bytes_per_turn"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 43 {
                if !row43.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row43.measurement_error.clone().unwrap_or_else(|| {
                            "turn-latency-distribution evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row43.reference_envelope_pass,
                        detail: format!(
                            "p50={:.3}ms, p95={:.3}ms, max={:.3}ms, MAD={:.3}ms, jitter={:.3}, class={}",
                            row43.wall_per_turn_p50_ms,
                            row43.wall_per_turn_p95_ms,
                            row43.wall_per_turn_max_ms,
                            row43.wall_per_turn_mad_ms,
                            row43.wall_per_turn_jitter_ratio,
                            row43.latency_class,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 44 {
                if !row44.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row44.measurement_error.clone().unwrap_or_else(|| {
                            "process-hygiene evidence is incomplete".to_owned()
                        })),
                    );
                }
                let sampler_detail = if row44.sampler_warnings.is_empty() {
                    format!(
                        "out-of-band sampler CPU={}ns, wall={}ns, active overhead={:.3}%, warnings=0",
                        row44.sampler_collection_cpu_ns,
                        row44.sampler_collection_wall_ns,
                        row44.sampler_overhead_pct,
                    )
                } else {
                    format!(
                        "out-of-band sampler CPU={}ns, wall={}ns, active overhead={:.3}%, warnings={}",
                        row44.sampler_collection_cpu_ns,
                        row44.sampler_collection_wall_ns,
                        row44.sampler_overhead_pct,
                        row44.sampler_warnings.join(" | ")
                    )
                };
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[
                        Assertion {
                            name: definition.metric.to_owned(),
                            passed: row44.passed,
                            detail: row44.failure_detail.clone().unwrap_or_else(|| {
                                format!(
                                    "residue processes={:.0}, thread delta={:.0}, FD delta={:.0}; peak live={:.0}, threads={:.0}, FDs={:.0}",
                                    row44.metrics["process_hygiene.residue_processes"],
                                    row44.metrics["process_hygiene.residue_threads_delta"],
                                    row44.metrics["process_hygiene.residue_fds_delta"],
                                    row44.metrics["process_hygiene.peak_live_processes"],
                                    row44.metrics["process_hygiene.peak_threads"],
                                    row44.metrics["process_hygiene.peak_fds"],
                                )
                            }),
                        },
                        Assertion {
                            name: "out-of-band sampler evidence".to_owned(),
                            passed: true,
                            detail: sampler_detail,
                        },
                    ],
                    None,
                );
            }
            if definition.row == 45 {
                if !row45.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row45.measurement_error.clone().unwrap_or_else(|| {
                            "time-to-first-model-request evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row45.reference_envelope_pass,
                        detail: format!(
                            "cold launch to first completed request body p50={:.3}ms, p95={:.3}ms, max={:.3}ms",
                            row45.p50_ms, row45.p95_ms, row45.max_ms,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 46 {
                if !row46.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row46.measurement_error.clone().unwrap_or_else(|| {
                            "memory-time-integral evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row46.reference_envelope_pass,
                        detail: format!(
                            "integral={:.6} MiB*s/turn, coverage={:.6}, max gap={:.3}ms, CPU p50={:.3}ms, p95={:.3}ms, class={}",
                            row46.memory_time_integral_mib_s_per_turn,
                            row46.memory_time_integral_coverage_ratio,
                            row46.memory_time_integral_max_sample_gap_ms,
                            row46.cpu_per_turn_p50_ms,
                            row46.cpu_per_turn_p95_ms,
                            row46.cpu_class,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 47 {
                if !row47.measurement_complete {
                    let measurement_error = match row47.measurement_error.clone() {
                        Some(error) => error,
                        None => "disk-io-per-turn evidence is incomplete".to_owned(),
                    };
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(measurement_error),
                    );
                }
                let disk_metric = |name: &str| {
                    row47.resource_values.get(name).copied().map_or(0.0, |value| value)
                };
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row47.reference_envelope_pass,
                        detail: format!(
                            "disk p50={:.0} bytes/turn, p95={:.0}, max={:.0}, journal={:.0}, log={}, slope={:.3} bytes/turn^2, complete={}, unbounded={}",
                            disk_metric("disk_write_bytes_per_turn_p50"),
                            disk_metric("disk_write_bytes_per_turn_p95"),
                            disk_metric("disk_write_bytes_per_turn_max"),
                            disk_metric("session_journal_growth_bytes_per_turn"),
                            row47.log_growth_bytes_per_turn.map_or_else(
                                || "verified-no-log".to_owned(),
                                |value| format!("{value:.0}"),
                            ),
                            disk_metric("disk_write_growth_slope_bytes_per_turn2"),
                            row47.disk_io_counter_complete,
                            row47.unbounded_disk_growth,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 49 {
                if !row49.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row49.measurement_error.clone().unwrap_or_else(|| {
                            "latency-vs-turn-index evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row49.passed,
                        detail: row49.failure_detail.clone().unwrap_or_else(|| {
                            format!(
                                "first decile p50={:.6}ms, last={:.6}ms, Theil-Sen={:.9}ms/turn, slope100={:.6}ms, ratio={}",
                                row49.first_decile_p50_ms,
                                row49.last_decile_p50_ms,
                                row49.theil_sen_ms_per_turn,
                                row49.latency_slope_ms_per_100_turns,
                                row49.latency_last_first_decile_ratio.map_or_else(
                                    || "null".to_owned(),
                                    |value| format!("{value:.6}"),
                                ),
                            )
                        }),
                    }],
                    None,
                );
            }
            if definition.row == 50 {
                if !row50.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row50.measurement_error.clone().unwrap_or_else(|| {
                            "session-residue-sweep evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row50.passed,
                        detail: format!(
                            "memory slope={:.6} MiB/session, final={:.3} MiB, store slope={:.3} bytes/session, final store={:.0} bytes/{:.0} files",
                            row50.resource_values["session_residue_slope_mib_per_session"],
                            row50.resource_values["session_residue_final_mib"],
                            row50.resource_values["session_store_byte_slope_per_session"],
                            row50.resource_values["session_store_final_residue_bytes"],
                            row50.session_store_final_residue_files,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 51 {
                if !row51.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row51.measurement_error.clone().unwrap_or_else(|| {
                            "context-limit-recovery evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row51.passed,
                        detail: format!(
                            "errors={:.0}, extra requests={:.0}, recovery={:.3}ms, tool pairs={:.0}/{:.0}, orphan calls/results={:.0}/{:.0}",
                            row51.metrics["context_limit_recovery.context_errors"],
                            row51.metrics["context_limit_recovery.extra_requests"],
                            row51.metrics["context_limit_recovery.recovery_ms"],
                            row51.metrics["context_limit_recovery.tool_pairs_before"],
                            row51.metrics["context_limit_recovery.tool_pairs_after"],
                            row51.metrics["context_limit_recovery.orphan_tool_calls"],
                            row51.metrics["context_limit_recovery.orphan_tool_results"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 52 {
                if !row52.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row52.measurement_error.clone().unwrap_or_else(|| {
                            "resume-latency-vs-length evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row52.passed,
                        detail: format!(
                            "longest p50={:.3}ms, p95={:.3}ms, slope={:.6}ms/turn, ratio={}",
                            row52.resource_values["resume_latency_p50_ms"],
                            row52.resource_values["resume_latency_p95_ms"],
                            row52.resource_values["resume_latency_slope_ms_per_turn"],
                            row52.long_short_ratio.map_or_else(|| "null".to_owned(), |value| format!("{value:.6}")),
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 53 {
                if !row53.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row53.measurement_error.clone().unwrap_or_else(|| {
                            "journal-torn-tail-sweep evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row53.passed,
                        detail: format!(
                            "trials={:.0}, growth-observed kills={:.0}, clean={:.0}, corrupt={:.0}, recovery p95={:.3}ms",
                            row53.metrics["journal_torn_tail_sweep.trials"],
                            row53.metrics["journal_torn_tail_sweep.kill_after_growth_observed_trials"],
                            row53.metrics["journal_torn_tail_sweep.clean_recoveries"],
                            row53.metrics["journal_torn_tail_sweep.corrupt_recoveries"],
                            row53.metrics["journal_torn_tail_sweep.recovery_p95_ms"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 54 {
                if !row54.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row54.measurement_error.clone().unwrap_or_else(|| {
                            "fanout-cliff evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row54.passed,
                        detail: format!(
                            "RSS cliff={:?}, wall cliff={:?}, max local alpha RSS={:?}/wall={:?}, global RSS alpha={:?}",
                            row54.fanout_cliff_n_rss,
                            row54.fanout_cliff_n_wall,
                            row54.fanout_max_local_rss_alpha,
                            row54.fanout_max_local_wall_alpha,
                            row54.fanout_global_rss_alpha,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 55 {
                if !row55.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row55.measurement_error.clone().unwrap_or_else(|| {
                            "fairness-under-fanout evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row55.passed,
                        detail: format!(
                            "worst CV={:?}, ratio={:?}, spread={:?}ms, starved={:?}",
                            row55.fairness_latency_cv,
                            row55.fairness_latency_max_min_ratio,
                            row55.fairness_latency_spread_ms,
                            row55.fairness_starved_agents,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 56 {
                if !row56.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row56.measurement_error.clone().unwrap_or_else(|| {
                            "child-failure-propagation evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row56.passed,
                        detail: format!(
                            "crash terminal={:.3}ms, hang terminal={:.3}ms, parent failures={:.0}/{:.0}, hang deadlines={:.0}, child terminals={:.0}, residue={:.0}, outer kills={:.0}",
                            row56.metrics["child_failure_propagation.crash_parent_terminal_ms"],
                            row56.metrics["child_failure_propagation.hang_parent_terminal_ms"],
                            row56.metrics["child_failure_propagation.crash_parent_failure_terminals"],
                            row56.metrics["child_failure_propagation.hang_parent_failure_terminals"],
                            row56.metrics["child_failure_propagation.hang_deadline_fired"],
                            row56.metrics["child_failure_propagation.child_terminal_count"],
                            row56.metrics["child_failure_propagation.child_residue_count"],
                            row56.metrics["child_failure_propagation.outer_kill_used"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 57 {
                if !row57.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row57.measurement_error.clone().unwrap_or_else(|| {
                            "signal-matrix evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row57.passed,
                        detail: format!(
                            "applicable={:.0}, passed={:.0}, SIGTERM={:.3}ms, SIGINTx2={:.3}ms, SIGHUP={:.3}ms, stdin-EOF={}",
                            row57.metrics["signal_matrix.applicable_cases"],
                            row57.metrics["signal_matrix.passed_cases"],
                            row57.metrics["signal_matrix.sigterm_terminal_ms"],
                            row57.metrics["signal_matrix.sigint2_terminal_ms"],
                            row57.metrics["signal_matrix.sighup_terminal_ms"],
                            row57
                                .metrics
                                .get("signal_matrix.stdin_eof_terminal_ms")
                                .map_or_else(|| "not-applicable".to_owned(), |value| format!("{value:.3}ms")),
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 58 {
                if !row58.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row58.measurement_error.clone().unwrap_or_else(|| {
                            "retry-budget evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row58.reference_envelope_pass,
                        detail: format!(
                            "max requests={:.0}/{:.0}, elapsed={:.3}/{:.3}ms, jittered={}, failure terminals={:.0}, effects={:.0}",
                            row58.metrics["retry_budget.requests_total"],
                            row58.metrics["retry_budget.declared_max_requests"],
                            row58.metrics["retry_budget.elapsed_ms"],
                            row58.metrics["retry_budget.declared_worst_case_ms"],
                            row58.metrics["retry_budget.backoff_jittered"],
                            row58.metrics["retry_budget.failure_terminals"],
                            row58.metrics["retry_budget.committed_effects"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 48 {
                if !row48.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row48.measurement_error.clone().unwrap_or_else(|| {
                            "model-wait-cpu evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row48.passed,
                        detail: format!(
                            "CPU p50={:.3}ms, wall p50={:.3}ms, max one-core ratio={:.6}, bytes={:.0}, max gap={:.3}ms",
                            row48.cpu_p50_ms,
                            row48.wall_p50_ms,
                            row48.one_core_max_ratio,
                            row48.metrics["model_wait_cpu.bytes_yielded"],
                            row48.metrics["model_wait_cpu.max_inter_frame_ms"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 59 {
                if !row59.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row59.measurement_error.clone().unwrap_or_else(|| {
                            "slow-stream-vs-stall evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row59.passed,
                        detail: format!(
                            "slow bytes={:.0}, max gap={:.3}ms, slow successes={:.0}, idle fires={:.0}; stall bytes={:.0}, max timeout={:.3}ms, failures={:.0}, outer kills={:.0}",
                            row59.metrics["slow_stream_vs_stall.slow_bytes_yielded"],
                            row59.metrics["slow_stream_vs_stall.slow_max_inter_frame_ms"],
                            row59.metrics["slow_stream_vs_stall.slow_terminal_success"],
                            row59.metrics["slow_stream_vs_stall.slow_idle_timeout_fired"],
                            row59.metrics["slow_stream_vs_stall.stall_bytes_yielded"],
                            row59.metrics["slow_stream_vs_stall.stall_own_timeout_ms"],
                            row59.metrics["slow_stream_vs_stall.stall_structured_failure"],
                            row59.metrics["slow_stream_vs_stall.stall_outer_kill_used"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 60 {
                if !row60.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row60.measurement_error.clone().unwrap_or_else(|| {
                            "large-tool-output evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row60.passed,
                        detail: format!(
                            "produced={:.0}, visible={:.0}/{:.0}, truncated={:.0}, terminal={:.0}, correlated={:.0}, peak delta={:.3} MiB",
                            row60.metrics["large_tool_output.produced_bytes"],
                            row60.metrics["large_tool_output.model_visible_encoded_bytes"],
                            row60.metrics["large_tool_output.harness_output_limit_bytes"],
                            row60.metrics["large_tool_output.truncated"],
                            row60.metrics["large_tool_output.terminal_success"],
                            row60.metrics["large_tool_output.tool_result_correlated"],
                            row60.peak_rss_delta_mib.map_or(0.0, |value| value),
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 61 {
                if !row61.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row61.measurement_error.clone().unwrap_or_else(|| {
                            "workspace-fault evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row61.passed,
                        detail: format!(
                            "structured failures={:.0}, terminals={:.0}, max terminal={:.3}ms, outside writes={:.0}, residue={:.0}",
                            row61.metrics["workspace_fault.structured_failure"],
                            row61.metrics["workspace_fault.terminal_count"],
                            row61.metrics["workspace_fault.terminal_ms"],
                            row61.metrics["workspace_fault.outside_writes"],
                            row61.metrics["workspace_fault.residue_processes"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 62 {
                if !row62.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row62.measurement_error.clone().unwrap_or_else(|| {
                            "egress enforcement unavailable".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row62.passed,
                        detail: format!(
                            "provider requests={:.0}, blocked auxiliary attempts={:.0}, successful non-provider connections={:.0}, offline success={:.0}, control probe blocked={:.0}",
                            row62.metrics["offline_mode.provider_requests"],
                            row62.metrics["offline_mode.blocked_egress_attempts"],
                            row62.metrics["offline_mode.successful_non_provider_connections"],
                            row62.metrics["offline_mode.offline_run_success"],
                            row62.metrics["offline_mode.control_probe_blocked"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 63 {
                if !row63.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row63.measurement_error.clone().unwrap_or_else(|| {
                            "nondeterministic-field-report evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row63.reference_envelope_pass,
                        detail: format!(
                            "score={:.6}, comparable={}, varying={}, pointers={}, critical={}",
                            row63.score,
                            row63.comparable_leaf_occurrences,
                            row63.varying_leaf_occurrences,
                            row63.varying_pointer_count,
                            row63.varying_critical_field_count,
                        ),
                    }],
                    None,
                );
                result.metadata.score = Some(row63.score);
                return result;
            }
            if definition.row == 64 {
                if !row64.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row64.measurement_error.clone().unwrap_or_else(|| {
                            "cross-run-reproducibility evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row64.identical,
                        detail: format!(
                            "identical={}, streams={}, baseline physical attempts={}",
                            row64.identical,
                            row64.request_stream_count,
                            row64.attempt_count,
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 65 {
                if !row65.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row65.measurement_error.clone().unwrap_or_else(|| {
                            "injection-surface evidence is incomplete".to_owned()
                        })),
                    );
                }
                if !row65.baseline_runnable {
                    let mut result = classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(false),
                        &[],
                        None,
                    );
                    result.metadata.score = Some(0.0);
                    result.evidence.push(
                        "fresh baseline could not prove both fake-provider routing and credential injection"
                            .to_owned(),
                    );
                    return result;
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row65.passed,
                        detail: format!(
                            "provider={:.2}, base-url={:.2}, credential={:.2}, verified={:.0}/3, aggregate={:.6}",
                            row65.metrics["injection_surface.provider_score"],
                            row65.metrics["injection_surface.base_url_score"],
                            row65.metrics["injection_surface.credential_score"],
                            row65.metrics["injection_surface.verified_components"],
                            row65.score,
                        ),
                    }],
                    None,
                );
                result.metadata.score = Some(row65.score);
                return result;
            }
            if definition.row == 66 {
                if !row66.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row66.measurement_error.clone().unwrap_or_else(|| {
                            "budget-enforcement evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row66.passed,
                        detail: format!(
                            "tokens={:.0}/{:.0}, cost={:.0}/{:.0}, time={:.3}/{:.0}ms, overruns={:.0}, terminals={:.0}, score={:.6}",
                            row66.metrics["budget_enforcement.token_observed"],
                            row66.metrics["budget_enforcement.token_limit"],
                            row66.metrics["budget_enforcement.cost_observed_microusd"],
                            row66.metrics["budget_enforcement.cost_limit_microusd"],
                            row66.metrics["budget_enforcement.time_observed_ms"],
                            row66.metrics["budget_enforcement.time_limit_ms"],
                            row66.metrics["budget_enforcement.overrun_count"],
                            row66.metrics["budget_enforcement.structured_failures"],
                            row66.metrics["budget_enforcement.score"],
                        ),
                    }],
                    None,
                );
                result.metadata.score = row66.metrics.get("budget_enforcement.score").copied();
                return result;
            }
            if definition.row == 67 {
                if !row67.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row67.measurement_error.clone().unwrap_or_else(|| {
                            "usage-reporting evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row67.passed,
                        detail: format!(
                            "input={:.0}, output={:.0}, total={:.0}, cost={:.0}, turns={:.0}, crosschecks={:.0}",
                            row67.metrics["usage_reporting.input_tokens"],
                            row67.metrics["usage_reporting.output_tokens"],
                            row67.metrics["usage_reporting.total_tokens"],
                            row67.metrics["usage_reporting.cost_microusd"],
                            row67.metrics["usage_reporting.turns"],
                            row67.metrics["usage_reporting.crosscheck_errors"],
                        ),
                    }],
                    None,
                );
                result.metadata.score = row67.metrics.get("usage_reporting.score").copied();
                return result;
            }
            if definition.row == 68 {
                if !row68.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row68.measurement_error.clone().unwrap_or_else(|| {
                            "session-ops-cli evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row68.passed,
                        detail: format!(
                            "create={:.0}, list={:.0}, resume={:.0}, fork={:.0}, delete={:.0}, score={:.6}",
                            row68.metrics["session_ops_cli.create_ok"],
                            row68.metrics["session_ops_cli.list_ok"],
                            row68.metrics["session_ops_cli.resume_ok"],
                            row68.metrics["session_ops_cli.fork_ok"],
                            row68.metrics["session_ops_cli.delete_ok"],
                            row68.metrics["session_ops_cli.score"],
                        ),
                    }],
                    None,
                );
                result.metadata.score = row68.metrics.get("session_ops_cli.score").copied();
                return result;
            }
            if definition.row == 69 {
                if !row69.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row69.measurement_error.clone().unwrap_or_else(|| {
                            "event-stream-completeness evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row69.passed,
                        detail: format!(
                            "tool-id={:.0}, correlation={:.0}, timestamps={:.0}, usage={:.0}, terminal={:.0}, schema={:.0}, score={:.6}",
                            row69.metrics["event_stream_completeness.tool_call_id"],
                            row69.metrics["event_stream_completeness.correlated_result"],
                            row69.metrics["event_stream_completeness.timestamps"],
                            row69.metrics["event_stream_completeness.usage"],
                            row69.metrics["event_stream_completeness.terminal_typing"],
                            row69.metrics["event_stream_completeness.schema_version"],
                            row69.metrics["event_stream_completeness.score"],
                        ),
                    }],
                    None,
                );
                result.metadata.score = row69
                    .metrics
                    .get("event_stream_completeness.score")
                    .copied();
                return result;
            }
            if definition.row == 70 {
                if !row70.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row70.measurement_error.clone().unwrap_or_else(|| {
                            "headless-permission evidence is incomplete".to_owned()
                        })),
                    );
                }
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row70.passed,
                        detail: format!(
                            "score={:.2}, prompts={:.0}, allowed={:.0}, fs-denied={:.0}, network-denied={:.0}, scope={:.0}",
                            row70.metrics["headless_permission_model.score"],
                            row70.metrics["headless_permission_model.tty_prompts"],
                            row70.metrics["headless_permission_model.allowed_effects"],
                            row70.metrics["headless_permission_model.denied_filesystem_effects"],
                            row70.metrics["headless_permission_model.denied_network_effects"],
                            row70.metrics["headless_permission_model.scope_violations"],
                        ),
                    }],
                    None,
                );
                result.metadata.score = row70
                    .metrics
                    .get("headless_permission_model.score")
                    .copied();
                return result;
            }
            if definition.row == 71 {
                if !row71.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row71.measurement_error.clone().unwrap_or_else(|| {
                            "secrets-hygiene-on-disk evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row71.passed,
                        detail: format!(
                            "files={:.0}, bytes={:.0}, stdout={:.0}, stderr={:.0}, journal={:.0}, session={:.0}, log={:.0}, carriers={:.0}",
                            row71.metrics["secrets_hygiene_on_disk.files_scanned"],
                            row71.metrics["secrets_hygiene_on_disk.bytes_scanned"],
                            row71.metrics["secrets_hygiene_on_disk.stdout_matches"],
                            row71.metrics["secrets_hygiene_on_disk.stderr_matches"],
                            row71.metrics["secrets_hygiene_on_disk.journal_matches"],
                            row71.metrics["secrets_hygiene_on_disk.session_matches"],
                            row71.metrics["secrets_hygiene_on_disk.log_matches"],
                            row71.metrics["secrets_hygiene_on_disk.declared_carrier_files"],
                        ),
                    }],
                    None,
                );
            }
            if definition.row == 72 {
                if !row72.measurement_complete {
                    return classify(
                        definition.row,
                        definition.id,
                        definition.pillar,
                        Some(true),
                        &[],
                        Some(row72.measurement_error.clone().unwrap_or_else(|| {
                            "tool-result-role-fidelity evidence is incomplete".to_owned()
                        })),
                    );
                }
                return classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(true),
                    &[Assertion {
                        name: definition.metric.to_owned(),
                        passed: row72.passed,
                        detail: format!(
                            "checks={:.0}, violations={:.0}, plain-user={:.0}, missing={:.0}, duplicate={:.0}",
                            row72.metrics["tool_result_role_fidelity.checks"],
                            row72.metrics["tool_result_role_fidelity.violations"],
                            row72.metrics["tool_result_role_fidelity.plain_user_text_violations"],
                            row72.metrics["tool_result_role_fidelity.missing_results"],
                            row72.metrics["tool_result_role_fidelity.duplicate_results"],
                        ),
                    }],
                    None,
                );
            }
            let events = state.events.get(&definition.row).map_or(&[][..], Vec::as_slice);
            let success_count = events
                .iter()
                .filter(|event| event.event == EventVocab::TerminalSuccess)
                .count();
            let failure_count = events
                .iter()
                .filter(|event| event.event == EventVocab::TerminalFailure)
                .count();
            let tool_calls = events
                .iter()
                .filter(|event| event.event == EventVocab::ToolCall)
                .count();
            let tool_results = events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count();
            let row_actor_prefix = format!("r{:02}", definition.row);
            let row_requests = requests
                .iter()
                .filter(|record| record.request.actor.starts_with(&row_actor_prefix))
                .count();
            let row_primary_requests = requests
                .iter()
                .filter(|record| {
                    record.request.actor.starts_with(&row_actor_prefix)
                        && record.request.model == manifest.fake_model.model
                })
                .count();
            let (passed, detail) = match definition.row {
                1 => (
                    (row_primary_requests == 1
                        && requests.iter().any(|record| {
                            record.request.actor == "r01"
                                && record.request.model == manifest.fake_model.model
                                && (!manifest.fake_model.auth_required
                                    || record.request.credential_fingerprint != "absent")
                        }))
                        || events.iter().any(|event| {
                            event.event == EventVocab::ModelRequest
                                && event.payload.get("model").and_then(Value::as_str)
                                    == Some(manifest.fake_model.model.as_str())
                                && event.payload.get("endpoint").and_then(Value::as_str)
                                    == Some("/v1/chat/completions")
                        }),
                    format!(
                        "observed {} exact configured model route",
                        row_requests.max(1)
                    ),
                ),
                2 => {
                    let call = events.iter().find(|event| {
                        event.event == EventVocab::ToolCall
                            && event.payload.get("call_id").and_then(Value::as_str)
                                == Some("call-r2")
                    });
                    let result = events.iter().find(|event| {
                        event.event == EventVocab::ToolResult
                            && event.payload.get("call_id").and_then(Value::as_str)
                                == Some("call-r2")
                    });
                    let content = call
                        .and_then(|event| event.payload.pointer("/arguments/content"))
                        .and_then(Value::as_str);
                    let exact_call = call.is_some_and(|event| {
                        event.payload.get("name").and_then(Value::as_str)
                            == Some("write_fixture")
                            && event
                                .payload
                                .pointer("/arguments/path")
                                .and_then(Value::as_str)
                                == Some("row-2.txt")
                    });
                    let correlated = result.is_some_and(|event| {
                        event.payload.pointer("/result/ok").and_then(Value::as_bool) == Some(true)
                    });
                    let effect = content.is_some_and(|content| {
                        fixture_effect_matches(profile_root, state, 2, "row-2.txt", content)
                    });
                    (
                        tool_calls == 1
                            && tool_results == 1
                            && success_count == 1
                            && exact_call
                            && correlated
                            && effect,
                        format!(
                            "observed {tool_calls} exact abstract call, {tool_results} correlated result, filesystem effect={effect}"
                        ),
                    )
                }
                3 => {
                    let positions = [
                        (EventVocab::ToolCall, "call-a"),
                        (EventVocab::ToolResult, "call-a"),
                        (EventVocab::ToolCall, "call-b"),
                        (EventVocab::ToolResult, "call-b"),
                    ]
                    .map(|(kind, call_id)| {
                        events.iter().position(|event| {
                            event.event == kind
                                && event.payload.get("call_id").and_then(Value::as_str)
                                    == Some(call_id)
                        })
                    });
                    let ordered = positions
                        .iter()
                        .all(Option::is_some)
                        && positions
                            .windows(2)
                            .all(|pair| pair[0].zip(pair[1]).is_some_and(|(a, b)| a < b));
                    let write = positions[0].and_then(|position| events.get(position));
                    let read = positions[2].and_then(|position| events.get(position));
                    let read_result = positions[3].and_then(|position| events.get(position));
                    let semantic = write.is_some_and(|event| {
                        event.payload.get("name").and_then(Value::as_str)
                            == Some("write_fixture")
                            && event
                                .payload
                                .pointer("/arguments/path")
                                .and_then(Value::as_str)
                                == Some("a.txt")
                            && event
                                .payload
                                .pointer("/arguments/content")
                                .and_then(Value::as_str)
                                == Some("A")
                    }) && read.is_some_and(|event| {
                        event.payload.get("name").and_then(Value::as_str) == Some("read_fixture")
                            && event
                                .payload
                                .pointer("/arguments/path")
                                .and_then(Value::as_str)
                                == Some("a.txt")
                            && event
                                .payload
                                .pointer("/arguments/expected_from_a")
                                .and_then(Value::as_str)
                                == Some("A")
                    });
                    let dependency = read_result.is_some_and(|event| {
                        // The read-back content may sit directly on the result, or
                        // inside a harness exec record that wraps tool stdout
                        // (verified: haider 0.0.967 surfaces it as `output` on the
                        // structured record parsed into `preview_record`). Trim so
                        // a trailing newline from the exec capture does not matter.
                        [
                            "/result/content",
                            "/result/stdout",
                            "/result/aggregated_output",
                            "/result/output",
                            "/result/preview_record/output",
                        ]
                        .iter()
                        .any(|pointer| {
                            event
                                .payload
                                .pointer(pointer)
                                .and_then(Value::as_str)
                                .map(str::trim)
                                == Some("A")
                        })
                    });
                    let effect = fixture_effect_matches(profile_root, state, 3, "a.txt", "A");
                    (
                        tool_calls == 2
                            && tool_results == 2
                            && success_count == 1
                            && ordered
                            && semantic
                            && dependency
                            && effect,
                        format!(
                            "observed exact A/result/B/result order={ordered}, dependency={dependency}, filesystem effect={effect}"
                        ),
                    )
                }
                4 => {
                    let first_result = events
                        .iter()
                        .position(|event| event.event == EventVocab::ToolResult);
                    let last_call = events
                        .iter()
                        .rposition(|event| event.event == EventVocab::ToolCall);
                    (
                        tool_calls == 2
                            && tool_results == 2
                            && success_count == 1
                            && last_call
                                .zip(first_result)
                                .is_some_and(|(call, result)| call < result),
                        "two independent calls were live before either result committed".to_owned(),
                    )
                }
                7 => (
                    failure_count == 1 && tool_calls == 0,
                    "malformed arguments produced one structured failure and no effect".to_owned(),
                ),
                9 => (success_count == 1 && failure_count == 0, "exactly one structural SUCCESS".to_owned()),
                10 => (failure_count == 1 && success_count == 0, "exactly one structural FAILURE".to_owned()),
                11 => (failure_count == 1 && tool_calls == 0, "bounded provider failure terminalized without an effect".to_owned()),
                12 => {
                    let elapsed_ms = events.iter().find_map(|event| {
                        (event.event == EventVocab::TerminalFailure
                            && event.payload.get("category").and_then(Value::as_str)
                                == Some("idle-timeout"))
                        .then(|| {
                            event
                                .payload
                                .get("client_turn_wall_ms")
                                .or_else(|| event.payload.get("elapsed_ms"))
                                .and_then(Value::as_u64)
                        })
                        .flatten()
                    });
                    let bound_ms = manifest.resources.idle_timeout_ms.saturating_add(2_000);
                    (
                        elapsed_ms.is_some_and(|elapsed| elapsed <= bound_ms),
                        format!(
                            "harness emitted its own idle-timeout terminal at {} ms (bound {bound_ms} ms), before the {} ms supervisor deadline",
                            elapsed_ms.map_or_else(|| "missing".to_owned(), |value| value.to_string()),
                            manifest.resources.turn_timeout_ms
                        ),
                    )
                }
                16 => (
                    success_count == 3 && failure_count == 0,
                    format!(
                        "three turns reopened one persisted session and emitted {success_count} terminals"
                    ),
                ),
                30 => (
                    state.session_replay_valid == Some(true),
                    state.session_replay_detail.clone().unwrap_or_else(|| {
                        "no disk-backed attach-after-cursor replay evidence was recorded".to_owned()
                    }),
                ),
                35 => (
                    state.crash_recovery_tree_cleared == Some(true)
                        && state.crash_recovery_valid == Some(true)
                        && state
                            .crash_recovery_ms
                            .is_some_and(|milliseconds| milliseconds <= 10_000.0),
                    format!(
                        "whole owned tree cleared={} before restart readiness {:.3} ms; {}",
                        state.crash_recovery_tree_cleared.unwrap_or(false),
                        state.crash_recovery_ms.unwrap_or(f64::MAX),
                        state.crash_recovery_detail.as_deref().unwrap_or(
                            "no post-commit attach/resume/idempotency evidence was recorded"
                        )
                    ),
                ),
                36 => {
                    let cancelled = events
                        .iter()
                        .filter(|event| event.event == EventVocab::TerminalCancelled)
                        .count();
                    (
                        cancelled == 1 && state.cancel_cleanup_valid == Some(true),
                        format!(
                            "observed {cancelled} cancellation terminal; {}",
                            state.cancel_cleanup_detail.as_deref().unwrap_or(
                                "no process/workspace cleanup evidence was recorded"
                            )
                        ),
                    )
                }
                37 => (
                    state.resume_idempotency_valid == Some(true),
                    state.resume_idempotency_detail.clone().unwrap_or_else(|| {
                        "no disk-reopen duplicate-submit evidence was recorded".to_owned()
                    }),
                ),
                39 => {
                    let hooks = events.iter().filter(|event| event.event == EventVocab::HookCompleted).count();
                    (hooks >= 2, format!("observed {hooks} fsync-ordered hook completions"))
                }
                40 => (
                    state.journal_recovery_valid == Some(true)
                        && (state.journal_torn_tail_injected == Some(true)
                            || state.journal_native_replay_valid == Some(true))
                        && state.journal_recovered_events.is_some_and(|count| count > 0),
                    state.journal_recovery_detail.clone().unwrap_or_else(|| {
                        "journal recovery trial produced no validation evidence".to_owned()
                    }),
                ),
                6 => {
                    let call = events.iter().find(|event| {
                        event.event == EventVocab::ToolCall
                            && event.payload.get("call_id").and_then(Value::as_str)
                                == Some("call-fail")
                            && event.payload.get("name").and_then(Value::as_str)
                                == Some("fail_fixture")
                    });
                    let result = events.iter().find(|event| {
                        event.event == EventVocab::ToolResult
                            && event.payload.get("call_id").and_then(Value::as_str)
                                == Some("call-fail")
                    });
                    let structured_failure = result.is_some_and(|event| {
                        event.payload.pointer("/result/ok").and_then(Value::as_bool) == Some(false)
                    });
                    (
                        call.is_some()
                            && structured_failure
                            && tool_calls == 1
                            && tool_results == 1
                            && success_count == 1,
                        format!(
                            "observed one exact fail_fixture call with correlated structured failure={structured_failure}"
                        ),
                    )
                }
                _ => {
                    let terminal_ok =
                        success_count == state.sessions.get(&definition.row).map_or(1, Vec::len);
                    (
                        terminal_ok,
                        format!(
                            "observed metric '{}': {} normalized events and {row_requests} model requests",
                            definition.metric,
                            events.len()
                        ),
                    )
                }
            };
            classify(
                definition.row,
                definition.id,
                definition.pillar,
                Some(true),
                &[Assertion {
                    name: definition.metric.to_owned(),
                    passed,
                    detail,
                }],
                None,
            )
        })
        .collect()
}

async fn await_owned_pid(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
) -> Result<Option<u32>> {
    if !manifest.daemon.readiness.pid_pointer.is_empty() {
        return Err(AhrbError::Protocol(
            "readiness pid_pointer must use the PID retained by Driver::daemon_pid".to_owned(),
        ));
    }
    let template = manifest
        .process
        .pid_files
        .first()
        .map(String::as_str)
        .or_else(|| {
            (!manifest.daemon.pid_locator.trim().is_empty())
                .then_some(manifest.daemon.pid_locator.as_str())
        });
    let Some(template) = template else {
        return Ok(None);
    };
    let path = PathBuf::from(crate::manifest::render_template(template, variables)?);
    let started = Instant::now();
    let timeout = Duration::from_millis(manifest.daemon.readiness.timeout_ms.max(1));
    let maximum_backoff = Duration::from_millis(25);
    let mut backoff = Duration::from_millis(2);
    let mut last_invalid = None;
    while started.elapsed() < timeout {
        match std::fs::read_to_string(&path) {
            Ok(text) => match text.trim().parse::<u32>() {
                Ok(pid) => return Ok(Some(pid)),
                Err(_) => last_invalid = Some(text),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if started.elapsed() < timeout {
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(maximum_backoff);
        }
    }
    if let Some(text) = last_invalid {
        return Err(AhrbError::Protocol(format!(
            "PID locator {} remained invalid for {} ms (last value {:?})",
            path.display(),
            timeout.as_millis(),
            text.trim()
        )));
    }
    Err(AhrbError::Timeout(format!(
        "PID locator {} did not appear within {} ms",
        path.display(),
        timeout.as_millis()
    )))
}

fn platform_sampler() -> Box<dyn Sampler> {
    #[cfg(target_os = "macos")]
    {
        Box::new(crate::process::macos::MacOsSampler::default())
    }
    #[cfg(target_os = "linux")]
    {
        Box::new(crate::process::linux::LinuxSampler::default())
    }
}

struct ResourceCollector {
    sampler: Option<Box<dyn Sampler>>,
    membership_sampler: Option<Box<dyn Sampler>>,
    series: SampleSeries,
    membership_samples_by_phase: BTreeMap<String, Vec<u64>>,
    membership_refreshes_by_phase: BTreeMap<String, Vec<MembershipRefreshEvidence>>,
    started: Instant,
    /// Interval for each of four staggered membership threads. Each thread starts
    /// once per certification cadence, yielding an aggregate quarter-cadence
    /// observation start interval with scheduler-delay redundancy.
    membership_thread_interval: Duration,
    membership_cadence: Duration,
    counter_sample_interval: Duration,
    counter_cadence: Duration,
}

struct PhaseSampling {
    sampler: Box<dyn Sampler>,
    samples: Vec<Sample>,
}

struct MembershipSampling {
    sampler: Box<dyn Sampler>,
    refreshes: Vec<MembershipRefreshEvidence>,
    collection_ns: u64,
}

type SharedMembershipTree = Arc<std::sync::Mutex<Option<(u64, ProcessTree)>>>;
type SharedResourceRoots = Arc<std::sync::Mutex<Vec<u32>>>;

struct MembershipCompletion(Arc<AtomicU64>);

impl Drop for MembershipCompletion {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[cfg(target_os = "macos")]
fn prioritize_sampler_thread() {
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    unsafe extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    // SAFETY: this changes only the calling sampler thread's QoS class. Failure
    // leaves the default scheduler policy in place and is reflected by cadence evidence.
    let _ = unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
}

#[cfg(not(target_os = "macos"))]
fn prioritize_sampler_thread() {}

#[cfg(target_os = "macos")]
fn prioritize_time_constrained_sampler(period_ns: u64, computation_ns: u64, constraint_ns: u64) {
    #[repr(C)]
    struct MachTimebaseInfo {
        numer: u32,
        denom: u32,
    }
    #[repr(C)]
    struct ThreadTimeConstraintPolicy {
        period: u32,
        computation: u32,
        constraint: u32,
        preemptible: i32,
    }
    const THREAD_TIME_CONSTRAINT_POLICY: i32 = 2;
    unsafe extern "C" {
        static mach_task_self_: u32;
        fn mach_thread_self() -> u32;
        fn mach_port_deallocate(task: u32, name: u32) -> i32;
        fn mach_timebase_info(info: *mut MachTimebaseInfo) -> i32;
        fn thread_policy_set(thread: u32, flavor: i32, policy: *const i32, count: u32) -> i32;
    }

    prioritize_sampler_thread();
    let mut timebase = MachTimebaseInfo { numer: 0, denom: 0 };
    // SAFETY: `timebase` is a valid writable output and the remaining Mach
    // calls operate only on the calling thread's send right.
    if unsafe { mach_timebase_info(&mut timebase) } != 0 || timebase.numer == 0 {
        return;
    }
    let to_ticks = |nanoseconds: u64| {
        nanoseconds
            .saturating_mul(u64::from(timebase.denom))
            .checked_div(u64::from(timebase.numer))
            .and_then(|ticks| u32::try_from(ticks).ok())
    };
    let (Some(period), Some(computation), Some(constraint)) = (
        to_ticks(period_ns),
        to_ticks(computation_ns),
        to_ticks(constraint_ns),
    ) else {
        return;
    };
    let policy = ThreadTimeConstraintPolicy {
        period,
        computation,
        constraint,
        preemptible: 1,
    };
    // SAFETY: the policy is four naturally aligned integer words, exactly the
    // layout/count required by THREAD_TIME_CONSTRAINT_POLICY.
    let thread = unsafe { mach_thread_self() };
    let _ = unsafe {
        thread_policy_set(
            thread,
            THREAD_TIME_CONSTRAINT_POLICY,
            (&policy as *const ThreadTimeConstraintPolicy).cast::<i32>(),
            4,
        )
    };
    // SAFETY: `thread` is the send right returned by `mach_thread_self` above.
    let _ = unsafe { mach_port_deallocate(mach_task_self_, thread) };
}

#[cfg(target_os = "macos")]
fn prioritize_membership_thread() {
    prioritize_time_constrained_sampler(10_000_000, 250_000, 2_000_000);
}

#[cfg(target_os = "macos")]
fn prioritize_counter_thread() {
    prioritize_time_constrained_sampler(5_000_000, 350_000, 2_000_000);
}

#[cfg(not(target_os = "macos"))]
fn prioritize_membership_thread() {
    prioritize_sampler_thread();
}

#[cfg(not(target_os = "macos"))]
fn prioritize_counter_thread() {
    prioritize_sampler_thread();
}

fn sampler_thread_cpu_ns() -> Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid writable timespec and the thread CPU clock
    // measures sampler work without charging scheduler descheduling as CPU cost.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let seconds = u64::try_from(time.tv_sec).map_err(|_| {
        AhrbError::Protocol("thread CPU clock returned negative seconds".to_owned())
    })?;
    let nanoseconds = u64::try_from(time.tv_nsec).map_err(|_| {
        AhrbError::Protocol("thread CPU clock returned negative nanoseconds".to_owned())
    })?;
    Ok(seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(nanoseconds))
}

fn reject_membership_overrun(collection_wall_ns: u64, cadence: Duration) -> Result<()> {
    let cadence_ns = duration_ns(cadence);
    if collection_wall_ns > cadence_ns {
        return Err(AhrbError::Validation(format!(
            "sampler overload: membership discovery consumed {collection_wall_ns} wall ns at a {cadence_ns} ns cadence"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_membership_phase(
    mut sampler: Box<dyn Sampler>,
    roots: SharedResourceRoots,
    membership_interval: Duration,
    membership_cadence: Duration,
    lane: u32,
    initial_delay: Duration,
    collector_started: Instant,
    stop: Arc<AtomicBool>,
    completed_samplers: Arc<AtomicU64>,
    published_sequence: Arc<AtomicU64>,
    initialized_samplers: Arc<AtomicU64>,
    shared_tree: SharedMembershipTree,
) -> Result<MembershipSampling> {
    prioritize_membership_thread();
    let _completion = MembershipCompletion(completed_samplers);
    if !initial_delay.is_zero() && !stop.load(Ordering::Acquire) {
        std::thread::sleep(initial_delay);
    }
    let mut refreshes = Vec::new();
    let mut total_collection_ns = 0_u64;
    let mut deadline = Instant::now();
    loop {
        let force_boundary = !refreshes.is_empty() && stop.load(Ordering::Acquire);
        let now = Instant::now();
        if !force_boundary && now < deadline {
            std::thread::sleep(deadline.duration_since(now));
        }
        let due = Instant::now();
        // Timestamp the observation boundary, not the end of discovery. Process
        // membership is observed as `discover` begins; its CPU cost is accounted
        // independently below and rejected if it consumes a complete cadence.
        let elapsed_ns = duration_ns(collector_started.elapsed());
        let wall_started = Instant::now();
        let collection_started = sampler_thread_cpu_ns()?;
        let current_roots = roots
            .lock()
            .map_err(|_| AhrbError::Protocol("resource roots lock poisoned".to_owned()))?
            .clone();
        let tree = sampler.discover(&current_roots)?;
        let collection_ns = sampler_thread_cpu_ns()?.saturating_sub(collection_started);
        let collection_wall_ns = duration_ns(wall_started.elapsed());
        reject_membership_overrun(collection_wall_ns, membership_cadence)?;
        total_collection_ns = total_collection_ns.saturating_add(collection_ns);
        let first_refresh = refreshes.is_empty();
        refreshes.push(MembershipRefreshEvidence {
            elapsed_ns,
            discovery_wall_ns: collection_wall_ns,
            discovery_cpu_ns: collection_ns,
            lane,
        });
        let sequence = published_sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let mut latest = shared_tree
            .lock()
            .map_err(|_| AhrbError::Protocol("membership snapshot lock poisoned".to_owned()))?;
        if latest
            .as_ref()
            .is_none_or(|(published, _)| sequence > *published)
        {
            *latest = Some((sequence, tree));
        }
        if first_refresh {
            initialized_samplers.fetch_add(1, Ordering::AcqRel);
        }
        while deadline <= due {
            deadline += membership_interval;
        }
        if force_boundary {
            break;
        }
    }
    Ok(MembershipSampling {
        sampler,
        refreshes,
        collection_ns: total_collection_ns,
    })
}

fn collect_resource_phase(
    mut sampler: Box<dyn Sampler>,
    phase: String,
    counter_interval: Duration,
    stop: Arc<AtomicBool>,
    completed_samplers: Arc<AtomicU64>,
    required_samplers: u64,
    shared_tree: SharedMembershipTree,
) -> Result<PhaseSampling> {
    prioritize_counter_thread();
    let mut samples = Vec::new();
    let mut tree = None;
    let mut consumed_sequence = 0_u64;
    let mut deadline = Instant::now();
    loop {
        let stopping = stop.load(Ordering::Acquire);
        let discovery_complete = completed_samplers.load(Ordering::Acquire) >= required_samplers;
        let force_boundary = !samples.is_empty() && stopping && discovery_complete;
        let now = Instant::now();
        if !force_boundary && now < deadline {
            std::thread::sleep(deadline.duration_since(now));
        }
        let latest = shared_tree
            .lock()
            .map_err(|_| AhrbError::Protocol("membership snapshot lock poisoned".to_owned()))?
            .clone();
        if let Some((sequence, latest_tree)) = latest {
            if sequence > consumed_sequence {
                consumed_sequence = sequence;
                tree = Some(latest_tree);
            }
        }
        if tree.is_none() {
            if stopping && discovery_complete {
                return Err(AhrbError::Protocol(
                    "resource membership did not reach the counter sampler".to_owned(),
                ));
            }
            std::thread::yield_now();
            continue;
        }
        let current = tree
            .as_ref()
            .ok_or_else(|| AhrbError::Protocol("resource membership disappeared".to_owned()))?;
        let collection_wall_started = Instant::now();
        let collection_started = sampler_thread_cpu_ns()?;
        let mut sample = sampler.sample(current, &phase)?;
        sample.collection_ns = sampler_thread_cpu_ns()?.saturating_sub(collection_started);
        sample.collection_wall_ns = duration_ns(collection_wall_started.elapsed());
        samples.push(sample);
        let due = Instant::now();
        while deadline <= due {
            deadline += counter_interval;
        }
        if force_boundary {
            break;
        }
    }
    Ok(PhaseSampling { sampler, samples })
}

impl ResourceCollector {
    fn new(timing: &ResourceTimingPlan) -> Self {
        #[cfg(target_os = "macos")]
        let counter_ms = timing.macos_rusage_cadence_ms;
        #[cfg(target_os = "linux")]
        let counter_ms = timing.linux_smaps_cadence_ms;
        let membership_cadence = Duration::from_millis(timing.membership_cadence_ms);
        let membership_thread_interval = membership_cadence;
        let counter_cadence = Duration::from_millis(counter_ms);
        let counter_sample_interval = counter_cadence
            .checked_div(4)
            .filter(|interval| !interval.is_zero())
            .unwrap_or(counter_cadence);
        Self {
            sampler: Some(platform_sampler()),
            membership_sampler: Some(platform_sampler()),
            series: SampleSeries::default(),
            membership_samples_by_phase: BTreeMap::new(),
            membership_refreshes_by_phase: BTreeMap::new(),
            started: Instant::now(),
            membership_thread_interval,
            membership_cadence,
            counter_sample_interval,
            counter_cadence,
        }
    }

    async fn sample_phase(&mut self, roots: &[u32], phase: &str, duration: Duration) -> Result<()> {
        self.sample_until(roots, phase, async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
        .await
    }

    async fn sample_until<T, F>(&mut self, roots: &[u32], phase: &str, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let roots = Arc::new(std::sync::Mutex::new(roots.to_vec()));
        self.sample_until_dynamic(roots, phase, operation).await
    }

    async fn sample_until_dynamic<T, F>(
        &mut self,
        roots: SharedResourceRoots,
        phase: &str,
        operation: F,
    ) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let sampler = self.sampler.take().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let membership_sampler = self.membership_sampler.take().ok_or_else(|| {
            AhrbError::Protocol("membership sampler is already collecting a phase".to_owned())
        })?;
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let completed_samplers = Arc::new(AtomicU64::new(0));
        let published_sequence = Arc::new(AtomicU64::new(0));
        let initialized_samplers = Arc::new(AtomicU64::new(0));
        let shared_tree: SharedMembershipTree = Arc::new(std::sync::Mutex::new(None));
        let staggered_interval = self.membership_thread_interval;
        let membership = std::thread::Builder::new()
            .name("ahrb-membership-sampler".to_owned())
            .spawn({
                let roots = Arc::clone(&roots);
                let stop = Arc::clone(&sampler_stop);
                let membership_interval = staggered_interval;
                let membership_cadence = self.membership_cadence;
                let collector_started = self.started;
                let completed_samplers = Arc::clone(&completed_samplers);
                let published_sequence = Arc::clone(&published_sequence);
                let initialized_samplers = Arc::clone(&initialized_samplers);
                let shared_tree = Arc::clone(&shared_tree);
                move || {
                    collect_membership_phase(
                        membership_sampler,
                        roots,
                        membership_interval,
                        membership_cadence,
                        0,
                        Duration::ZERO,
                        collector_started,
                        stop,
                        completed_samplers,
                        published_sequence,
                        initialized_samplers,
                        shared_tree,
                    )
                }
            })?;
        let stagger = staggered_interval
            .checked_div(4)
            .filter(|delay| !delay.is_zero())
            .unwrap_or(staggered_interval);
        let mut backup_memberships = Vec::new();
        for index in 1_u32..4 {
            let backup = std::thread::Builder::new()
                .name(format!("ahrb-membership-sampler-{index}"))
                .spawn({
                    let backup_sampler = platform_sampler();
                    let roots = Arc::clone(&roots);
                    let stop = Arc::clone(&sampler_stop);
                    let membership_cadence = self.membership_cadence;
                    let initial_delay = stagger.checked_mul(index).unwrap_or(stagger);
                    let collector_started = self.started;
                    let completed_samplers = Arc::clone(&completed_samplers);
                    let published_sequence = Arc::clone(&published_sequence);
                    let initialized_samplers = Arc::clone(&initialized_samplers);
                    let shared_tree = Arc::clone(&shared_tree);
                    move || {
                        collect_membership_phase(
                            backup_sampler,
                            roots,
                            staggered_interval,
                            membership_cadence,
                            index,
                            initial_delay,
                            collector_started,
                            stop,
                            completed_samplers,
                            published_sequence,
                            initialized_samplers,
                            shared_tree,
                        )
                    }
                });
            match backup {
                Ok(handle) => backup_memberships.push(handle),
                Err(error) => {
                    sampler_stop.store(true, Ordering::Release);
                    let _ = membership.join();
                    for handle in backup_memberships {
                        let _ = handle.join();
                    }
                    return Err(error.into());
                }
            }
        }
        let sampling = match std::thread::Builder::new()
            .name("ahrb-resource-sampler".to_owned())
            .spawn({
                let phase = phase.to_owned();
                let stop = Arc::clone(&sampler_stop);
                let counter_interval = self.counter_sample_interval;
                let completed_samplers = Arc::clone(&completed_samplers);
                let shared_tree = Arc::clone(&shared_tree);
                move || {
                    collect_resource_phase(
                        sampler,
                        phase,
                        counter_interval,
                        stop,
                        completed_samplers,
                        4,
                        shared_tree,
                    )
                }
            }) {
            Ok(sampling) => sampling,
            Err(error) => {
                sampler_stop.store(true, Ordering::Release);
                let _ = membership.join();
                for handle in backup_memberships {
                    let _ = handle.join();
                }
                return Err(error.into());
            }
        };
        let readiness_deadline = Instant::now()
            + self
                .membership_cadence
                .checked_mul(5)
                .unwrap_or(Duration::from_secs(1));
        while initialized_samplers.load(Ordering::Acquire) < 4
            && Instant::now() < readiness_deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let operation_result = if initialized_samplers.load(Ordering::Acquire) < 4 {
            Err(AhrbError::Protocol(
                "resource membership samplers did not initialize before the workload".to_owned(),
            ))
        } else {
            operation.await
        };
        sampler_stop.store(true, Ordering::Release);
        let membership_sampling = membership
            .join()
            .map_err(|_| AhrbError::Protocol("membership sampler thread panicked".to_owned()))?;
        let mut backup_samples = Vec::new();
        for handle in backup_memberships {
            let joined = handle.join().map_err(|_| {
                AhrbError::Protocol("backup membership sampler thread panicked".to_owned())
            })?;
            backup_samples.push(joined?);
        }
        let phase_sampling = sampling
            .join()
            .map_err(|_| AhrbError::Protocol("resource sampler thread panicked".to_owned()))?;
        let mut phase_sampling = phase_sampling?;
        let mut membership_sampling = membership_sampling?;
        for backup in backup_samples {
            membership_sampling.refreshes.extend(backup.refreshes);
            membership_sampling.collection_ns = membership_sampling
                .collection_ns
                .saturating_add(backup.collection_ns);
        }
        membership_sampling
            .refreshes
            .sort_by_key(|refresh| (refresh.elapsed_ns, refresh.lane));
        let mut membership_times = membership_sampling
            .refreshes
            .iter()
            .map(|refresh| refresh.elapsed_ns)
            .collect::<Vec<_>>();
        membership_times.dedup();
        self.sampler = Some(phase_sampling.sampler);
        self.membership_sampler = Some(membership_sampling.sampler);
        let sample_count = u64::try_from(phase_sampling.samples.len())
            .unwrap_or(u64::MAX)
            .max(1);
        let share = membership_sampling.collection_ns / sample_count;
        let mut remainder = membership_sampling.collection_ns % sample_count;
        for sample in &mut phase_sampling.samples {
            let extra = u64::from(remainder > 0);
            remainder = remainder.saturating_sub(extra);
            sample.collection_ns = sample
                .collection_ns
                .saturating_add(share)
                .saturating_add(extra);
        }
        for sample in phase_sampling.samples {
            self.series.push(sample)?;
        }
        self.membership_samples_by_phase
            .insert(phase.to_owned(), membership_times);
        self.membership_refreshes_by_phase
            .insert(phase.to_owned(), membership_sampling.refreshes);
        operation_result
    }

    fn sample_once(&mut self, roots: &[u32], phase: &str) -> Result<Sample> {
        let sample = self.observe_once(roots, phase)?;
        self.series.push(sample.clone())?;
        Ok(sample)
    }

    fn observe_once(&mut self, roots: &[u32], phase: &str) -> Result<Sample> {
        let collection_wall_started = Instant::now();
        let collection_started = sampler_thread_cpu_ns()?;
        let sampler = self.sampler.as_deref_mut().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let tree = sampler.discover(roots)?;
        let mut sample = sampler.sample(&tree, phase)?;
        sample.collection_ns = sampler_thread_cpu_ns()?.saturating_sub(collection_started);
        sample.collection_wall_ns = duration_ns(collection_wall_started.elapsed());
        Ok(sample)
    }
}

struct GroupEvidence {
    sweep: SweepObservation,
    ordinary_return: ReturnToIdleObservation,
    single_agent: Option<SingleAgentObservation>,
    cleanup: CleanupObservation,
}

/// Measure client-process fan-out as transient process trees. There is no
/// resident baseline in this architecture: every width launches one fresh CLI
/// process per turn, samples the complete trees until terminal exit, and then
/// verifies that all roots disappeared through the driver's terminal contract.
#[allow(clippy::too_many_arguments)]
async fn collect_per_invocation_resource_observations(
    manifest: &Manifest,
    profile: Profile,
    profile_root: &Path,
    workflow: &Workflow,
    model_environment: &BTreeMap<String, String>,
    credential: &str,
    collect_long_horizon_latency: bool,
) -> Result<PerInvocationResourceCollection> {
    let timing = ResourceTimingPlan::for_profile(ResourceProfile::from(profile));
    let mut observations = Vec::new();
    let mut collector = ResourceCollector::new(&timing);
    let mut turn_wall_ns = Vec::new();
    let mut long_horizon_turns = Vec::new();

    for repetition in 0..timing.repetitions {
        let repetition_root = profile_root.join(format!("pr{repetition}"));
        prepare_profile(manifest, &repetition_root)?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                repetition_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment.clone());
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.to_owned(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential.to_owned());
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &repetition_root)?;
        let command = manifest.transport.command.clone();
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &repetition_root,
            true,
        )?;
        driver.start().await?;

        for agents in &timing.sweep_widths {
            let mut sessions = Vec::new();
            for index in 0..*agents {
                let actor_name = resource_sweep_actor(repetition, *agents, index);
                let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
                    AhrbError::Protocol(format!("resource actor {actor_name:?} is absent"))
                })?;
                let session = driver.create_session(&actor_name).await?;
                driver
                    .submit(
                        &session,
                        &actor.prompt,
                        &format!("resource-r{repetition}-n{agents}-turn-{}", index + 1),
                    )
                    .await?;
                sessions.push(session);
            }

            let launcher_pids = driver.owned_pids();
            if launcher_pids.len() != usize::try_from(*agents).unwrap_or(usize::MAX) {
                return Err(AhrbError::Protocol(format!(
                    "N={agents} launched {} live CLI processes, expected {agents}",
                    launcher_pids.len()
                )));
            }
            let sampler = collector.sampler.as_deref_mut().ok_or_else(|| {
                AhrbError::Protocol("per-invocation sampler is unavailable".to_owned())
            })?;
            let roots = verified_process_roots(manifest, sampler, launcher_pids.clone(), None)?;
            let phase = format!("per-invocation-r{repetition}-n{agents}-active");
            let deadline = Duration::from_millis(manifest.resources.turn_timeout_ms);
            let completed_processes = collector
                .sample_until(&roots, &phase, async {
                    driver.release_invocations().await?;
                    let started = Instant::now();
                    let mut complete = BTreeSet::new();
                    loop {
                        for session in &sessions {
                            if complete.contains(&session.0) {
                                continue;
                            }
                            let events = driver.attach(session, None).await?;
                            if events.iter().any(|event| is_terminal(&event.event)) {
                                complete.insert(session.0.clone());
                            }
                        }
                        if complete.len() == sessions.len() {
                            return u32::try_from(complete.len()).map_err(|_| {
                                AhrbError::Protocol(
                                    "per-invocation process count exceeds u32".to_owned(),
                                )
                            });
                        }
                        if started.elapsed() >= deadline {
                            return Err(AhrbError::Timeout(format!(
                                "only {}/{} per-invocation processes terminalized",
                                complete.len(),
                                sessions.len()
                            )));
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await?;

            // The membership samplers were armed while every launch gate was
            // still holding its unique process-group root. A final discovery
            // therefore retains ordinary orphan/reparent workers even after the
            // CLI launcher has exited.
            tokio::time::sleep(Duration::from_millis(
                timing.membership_cadence_ms.saturating_mul(2),
            ))
            .await;
            let residual_tree = {
                let sampler = collector.membership_sampler.as_deref_mut().ok_or_else(|| {
                    AhrbError::Protocol(
                        "per-invocation membership sampler is unavailable".to_owned(),
                    )
                })?;
                sampler.discover(&roots)?
            };
            let residual_processes = u32::try_from(residual_tree.members.len()).unwrap_or(u32::MAX);
            if residual_processes > 0 {
                let residual_phase = format!("{phase}-residual");
                let sample = {
                    let sampler = collector.membership_sampler.as_deref_mut().ok_or_else(|| {
                        AhrbError::Protocol(
                            "per-invocation membership sampler is unavailable".to_owned(),
                        )
                    })?;
                    sampler.sample(&residual_tree, &residual_phase)?
                };
                collector.series.push(sample)?;
                terminate_owned_tree(
                    collector.membership_sampler.as_deref_mut().ok_or_else(|| {
                        AhrbError::Protocol(
                            "per-invocation membership sampler is unavailable".to_owned(),
                        )
                    })?,
                    &roots,
                    &residual_tree,
                )
                .await?;
            }

            let phase_samples = phase_samples(&collector.series, &phase);
            if phase_samples.is_empty() {
                return Err(AhrbError::Protocol(format!(
                    "N={agents} per-invocation trial produced no samples"
                )));
            }
            let observed_root_pids: BTreeSet<u32> = phase_samples
                .iter()
                .flat_map(|sample| sample.processes.iter())
                .map(|process| process.identity.pid)
                .filter(|pid| launcher_pids.contains(pid))
                .collect();
            if observed_root_pids.len() != launcher_pids.len() {
                return Err(AhrbError::Protocol(format!(
                    "N={agents} sampler observed {}/{} declared CLI roots",
                    observed_root_pids.len(),
                    launcher_pids.len()
                )));
            }
            let peak_bytes = phase_samples
                .iter()
                .map(|sample| effective_sample_bytes(sample))
                .max()
                .unwrap_or(0);
            let cpu_ns = phase_samples
                .first()
                .zip(phase_samples.last())
                .map_or(0, |(first, last)| last.cpu_ns.saturating_sub(first.cpu_ns));
            observations.push(PerInvocationObservation {
                repetition,
                agents: *agents,
                peak_bytes,
                cold_peak_bytes: peak_bytes,
                cpu_ns,
                completed_processes,
                residual_processes,
            });
            for session in &sessions {
                driver.close(session).await?;
            }
        }
        if collect_long_horizon_latency {
            long_horizon_turns.extend(
                collect_per_invocation_long_horizon_turns(
                    &mut driver,
                    workflow,
                    repetition,
                    &timing,
                    !manifest.hooks.completion.is_empty(),
                )
                .await?,
            );
        }
        turn_wall_ns.extend(driver.completed_turn_wall_ns());
        driver.shutdown().await?;
    }

    let membership = membership_report_samples(&collector.membership_refreshes_by_phase);
    Ok(PerInvocationResourceCollection {
        observations,
        samples: collector.series.samples,
        membership,
        turn_wall_ns,
        long_horizon_turns,
    })
}

async fn collect_per_invocation_long_horizon_turns(
    driver: &mut HarnessDriver,
    workflow: &Workflow,
    repetition: u32,
    timing: &ResourceTimingPlan,
    completion_hook_required: bool,
) -> Result<Vec<TurnObservation>> {
    let actor_name = resource_long_actor(repetition);
    if !workflow.actors.contains_key(&actor_name) {
        return Err(AhrbError::Protocol(format!(
            "per-invocation long-horizon actor {actor_name:?} is absent"
        )));
    }
    let session = driver.create_session(&actor_name).await?;
    let session_id_hash = stable_evidence_hash(&session.0);
    let mut after = None;
    let mut turns = Vec::with_capacity(timing.long_horizon_turns as usize);
    for turn in 1..=timing.long_horizon_turns {
        let checkpoint = resource_long_checkpoint(repetition, turn);
        let prompt = format!(
            "AHRB long horizon turn {turn} {}",
            route_marker(&workflow.scenario, &actor_name, &checkpoint)
        );
        let turn_key = format!("resource-long-r{repetition}-turn-{turn}");
        let previous_boundary_count = driver.completed_turn_boundaries().len();
        let submit_ns = monotonic_timestamp_ns();
        let started = Instant::now();
        driver.submit(&session, &prompt, &turn_key).await?;
        driver.release_invocations().await?;
        let (terminal_cursor, tool_results, terminal_ns) = wait_one_terminal(
            driver,
            &session,
            after,
            &turn_key,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
            started,
        )
        .await?;
        after = Some(terminal_cursor);
        let boundary = await_completed_turn_boundary(
            driver,
            &session,
            after,
            previous_boundary_count,
            Duration::from_millis(timing.reclaim_deadline_ms),
        )
        .await?;
        let wall_ns = boundary
            .exit_ns
            .checked_sub(boundary.launch_ns)
            .ok_or_else(|| {
                AhrbError::Protocol(
                    "per-invocation long-horizon launch/exit boundaries are reversed".to_owned(),
                )
            })?;
        let fixture_ok = if turn % 10 == 0 {
            let expected_call_id = format!("resource-long-r{repetition}-t{turn}");
            tool_results.len() == 1
                && tool_results.first().is_some_and(|result| {
                    result.call_id == expected_call_id && result.name == "write_fixture"
                })
        } else {
            tool_results.is_empty()
        };
        if !fixture_ok {
            return Err(AhrbError::Protocol(format!(
                "per-invocation long-horizon turn {turn} violated the fixture-tool cadence: {tool_results:?}"
            )));
        }
        turns.push(TurnObservation {
            repetition: repetition.saturating_add(1),
            turn_index: turn,
            actor: actor_name.clone(),
            session_id_hash: session_id_hash.clone(),
            phase: "latency-vs-turn-index".to_owned(),
            launch_ns: Some(boundary.launch_ns),
            submit_ns: Some(submit_ns),
            first_model_request_ns: None,
            terminal_ns: Some(terminal_ns),
            exit_ns: Some(boundary.exit_ns),
            turn_wall_ns: Some(wall_ns),
        });
    }
    driver.close(&session).await?;
    Ok(turns)
}

async fn terminate_owned_tree(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    tree: &ProcessTree,
) -> Result<()> {
    let owned: BTreeSet<_> = tree.members.keys().copied().collect();
    for identity in &owned {
        let pid = i32::try_from(identity.pid)
            .map_err(|_| AhrbError::Protocol("owned PID exceeds pid_t".to_owned()))?;
        // SAFETY: the PID is a freshly rediscovered member of this invocation's
        // isolated process group. ESRCH only means it exited between discovery
        // and cleanup.
        if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let remaining = sampler.discover(roots)?;
    for identity in remaining
        .members
        .keys()
        .filter(|identity| owned.contains(identity))
    {
        let pid = i32::try_from(identity.pid)
            .map_err(|_| AhrbError::Protocol("owned PID exceeds pid_t".to_owned()))?;
        // SAFETY: start-time identity was revalidated by the immediately
        // preceding discovery, so this cannot target a PID-reuse occupant.
        if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn collect_resource_evidence(
    manifest: &Manifest,
    profile: Profile,
    profile_root: &Path,
    workflow: &Workflow,
    model_environment: &BTreeMap<String, String>,
    credential: &str,
) -> Result<ResourceEvidence> {
    let resource_profile = ResourceProfile::from(profile);
    let timing = ResourceTimingPlan::for_profile(resource_profile);
    let mut collector = ResourceCollector::new(&timing);
    let mut idle_repetitions = Vec::new();
    let mut sweep = Vec::new();
    let mut ordinary_returns = Vec::new();
    let mut cold_starts = Vec::new();
    let mut warmups = Vec::new();
    let mut single_agents = Vec::new();
    let mut cleanups = Vec::new();
    let mut long_horizons = Vec::new();
    let mut lifecycle_notes = Vec::new();
    let mut turn_wall_ns = Vec::new();
    let mut initial_idle_workers = None;
    let mut final_idle_workers = None;
    let mut initial_idle_threads = None;
    let mut final_idle_threads = None;
    let rotation_seed = 0_u64;

    for repetition in 0..timing.repetitions {
        // The guard window is outside the measured tree. It also prevents one
        // repetition's process teardown from overlapping the next cold launch.
        tokio::time::sleep(Duration::from_millis(timing.load_guard_ms)).await;
        let repetition_root = profile_root.join(format!("rr{repetition}"));
        prepare_profile(manifest, &repetition_root)?;
        let mut variables = BTreeMap::from([
            (
                "profile".to_owned(),
                repetition_root.to_string_lossy().into_owned(),
            ),
            ("endpoint".to_owned(), String::new()),
        ]);
        let mut environment = isolated_environment(manifest, &variables)?;
        environment.extend(model_environment.clone());
        environment.insert(
            manifest.fake_model.credential_env.clone(),
            credential.to_owned(),
        );
        environment.insert(
            "AHRB_MOCK_MODEL".to_owned(),
            manifest.fake_model.model.clone(),
        );
        variables.insert(
            "base_url".to_owned(),
            environment
                .get(&manifest.fake_model.base_url_env)
                .cloned()
                .unwrap_or_default(),
        );
        variables.insert("credential".to_owned(), credential.to_owned());
        variables.insert("model".to_owned(), manifest.fake_model.model.clone());
        write_generated_files(manifest, &variables, &repetition_root)?;
        if !manifest.hooks.acceptance.is_empty() {
            environment.insert(
                "AHRB_MOCK_ACCEPTANCE_HOOK".to_owned(),
                serde_json::to_string(&render_argv(&manifest.hooks.acceptance, &variables)?)?,
            );
        }
        if !manifest.hooks.completion.is_empty() {
            environment.insert(
                "AHRB_MOCK_COMPLETION_HOOK".to_owned(),
                serde_json::to_string(&render_argv(&manifest.hooks.completion, &variables)?)?,
            );
        }
        let command = if manifest.transport.kind == TransportKind::Exec {
            manifest.transport.command.clone()
        } else {
            render_argv(&manifest.transport.command, &variables)?
        };
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &repetition_root,
            false,
        )?;
        let cold_phase = format!("resource-r{repetition}-cold-start");
        let dynamic_roots: SharedResourceRoots = Arc::new(std::sync::Mutex::new(Vec::new()));
        let operation_roots = Arc::clone(&dynamic_roots);
        let settle = collector
            .membership_cadence
            .checked_mul(2)
            .unwrap_or(Duration::from_millis(100));
        let (daemon_pid, readiness_ms) = collector
            .sample_until_dynamic(dynamic_roots, &cold_phase, async {
                // All membership and counter threads are initialized before
                // this future is polled, so cold launch is inside the measured
                // interval. Roots change from launcher/client PIDs to the
                // readiness-declared daemon PID without a sampling subprocess.
                let launch_started = Instant::now();
                driver.start().await?;
                {
                    let mut roots = operation_roots.lock().map_err(|_| {
                        AhrbError::Protocol("resource roots lock poisoned".to_owned())
                    })?;
                    *roots = driver.owned_pids();
                }
                driver.await_readiness().await?;
                let daemon_pid = if manifest.daemon.readiness.pid_pointer.is_empty() {
                    await_owned_pid(manifest, &variables).await?
                } else {
                    Some(driver.daemon_pid().ok_or_else(|| {
                        AhrbError::Protocol(
                            "resource driver lost the readiness-declared daemon PID".to_owned(),
                        )
                    })?)
                };
                {
                    let mut roots = operation_roots.lock().map_err(|_| {
                        AhrbError::Protocol("resource roots lock poisoned".to_owned())
                    })?;
                    *roots = driver.owned_pids();
                    roots.extend(daemon_pid);
                    roots.sort_unstable();
                    roots.dedup();
                }
                // Retain the ready root for two discovery cadences so at least
                // one staggered lane publishes its `(pid,start_time)` tree.
                tokio::time::sleep(settle).await;
                Ok((daemon_pid, duration_millis(launch_started.elapsed())))
            })
            .await?;
        let cold_sampling_started_after_launch_ms = 0;
        let sampler = collector.sampler.as_deref_mut().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let roots = verified_process_roots(manifest, sampler, driver.owned_pids(), daemon_pid)?;
        let identity = RepetitionIdentity {
            repetition,
            profile: resource_profile,
            isolation_token: repetition_root.to_string_lossy().into_owned(),
        };
        warmups.push(
            run_resource_warmup(
                &mut driver,
                workflow,
                &identity,
                timing.warmup_turns,
                &timing,
                !manifest.hooks.completion.is_empty(),
            )
            .await?,
        );
        let idle_phase = format!("resource-r{repetition}-idle");
        collector
            .sample_phase(
                &roots,
                &idle_phase,
                Duration::from_millis(timing.idle_drift_ms),
            )
            .await?;
        let idle_samples = phase_samples(&collector.series, &idle_phase);
        let first_idle = idle_samples.first().copied();
        let last_idle = idle_samples.last().copied();
        let workers = |sample: Option<&Sample>| {
            sample.map_or(0, |sample| sample.processes.len().saturating_sub(1))
        };
        initial_idle_workers.get_or_insert_with(|| workers(first_idle));
        final_idle_workers = Some(workers(last_idle));
        initial_idle_threads.get_or_insert_with(|| {
            first_idle
                .and_then(|sample| sample.thread_count)
                .unwrap_or(0)
        });
        final_idle_threads = Some(
            last_idle
                .and_then(|sample| sample.thread_count)
                .unwrap_or(0),
        );
        idle_repetitions.push(IdlePhaseRepetition {
            identity: identity.clone(),
            warm_idle: idle_phase.clone(),
            idle_cpu: idle_phase.clone(),
            idle_drift: idle_phase.clone(),
        });
        cold_starts.push(ColdStartObservation {
            identity: identity.clone(),
            cold_phase,
            ready_idle_phase: idle_phase,
            sampling_started_after_launch_ms: cold_sampling_started_after_launch_ms,
            readiness_ms,
            startup_bound_ms: manifest.daemon.readiness.timeout_ms,
            minimum_idle_processes: 1,
        });

        let mut widths = timing.sweep_widths.clone();
        if !widths.is_empty() {
            let offset = (usize::try_from(rotation_seed)
                .unwrap_or(usize::MAX)
                .wrapping_add(repetition as usize))
                % widths.len();
            widths.rotate_left(offset);
        }
        for (order, agents) in widths.into_iter().enumerate() {
            let group = run_resource_group(
                &mut collector,
                &mut driver,
                &roots,
                workflow,
                &identity,
                agents,
                u32::try_from(order).unwrap_or(u32::MAX),
                rotation_seed,
                &timing,
                !manifest.hooks.completion.is_empty(),
            )
            .await?;
            if agents == 1 {
                ordinary_returns.push(group.ordinary_return);
                if let Some(single) = group.single_agent {
                    single_agents.push(single);
                }
            }
            if agents == *timing.sweep_widths.last().unwrap_or(&agents) {
                cleanups.push(group.cleanup);
            }
            sweep.push(group.sweep);
        }
        let (long_horizon, long_turn_wall_ns) = run_long_horizon(
            &mut collector,
            &mut driver,
            &roots,
            workflow,
            &identity,
            &timing,
            !manifest.hooks.completion.is_empty(),
        )
        .await?;
        long_horizons.push(long_horizon);
        turn_wall_ns.extend(long_turn_wall_ns);
        driver.shutdown().await?;
        lifecycle_notes.extend(driver.lifecycle_notes());
    }

    #[cfg(target_os = "macos")]
    let (counter_kind, counter_cadence_ms) = (
        ResourceCounterKind::MacOsRusage,
        timing.macos_rusage_cadence_ms,
    );
    #[cfg(target_os = "linux")]
    let (counter_kind, counter_cadence_ms) = (
        ResourceCounterKind::LinuxSmapsRollup,
        timing.linux_smaps_cadence_ms,
    );
    let counter_cadence_ns = counter_cadence_ms.saturating_mul(1_000_000);
    let busy_polling_detected =
        detect_busy_polling(&collector.series, &idle_repetitions, counter_cadence_ns)?;
    Ok(ResourceEvidence {
        completed_repetitions: timing.repetitions,
        lifecycle_notes,
        series: collector.series,
        turn_wall_ns,
        phases: ResourcePhases {
            warm_idle: "resource-idle".to_owned(),
            idle_cpu: "resource-idle".to_owned(),
            idle_drift: "resource-idle".to_owned(),
            repetitions: idle_repetitions,
            cadence: Some(ResourceCadenceEvidence {
                membership_cadence_ns: timing.membership_cadence_ms.saturating_mul(1_000_000),
                counter_cadence_ns,
                counter_kind,
                membership_samples_by_phase: collector.membership_samples_by_phase,
                membership_refreshes_by_phase: collector.membership_refreshes_by_phase,
            }),
        },
        memory_metric: Some(MemoryMetric::Effective),
        sampler_cadence_ns: Some(counter_cadence_ns),
        idle: Some(IdleObservation {
            declared_model: if manifest.daemon.persistent {
                IdleProcessModel::PersistentTree
            } else {
                IdleProcessModel::ZeroProcessBetweenTurns
            },
            busy_polling_detected: Some(busy_polling_detected),
            initial_workers: initial_idle_workers.unwrap_or(0),
            final_workers: final_idle_workers.unwrap_or(0),
            initial_threads: initial_idle_threads,
            final_threads: final_idle_threads,
        }),
        warmup: Some(warmups),
        sweep,
        ordinary_return: Some(ordinary_returns),
        cold_start: Some(cold_starts),
        single_agent: Some(single_agents),
        cleanup: Some(cleanups),
        long_horizon: Some(long_horizons),
    })
}

async fn run_resource_warmup(
    driver: &mut HarnessDriver,
    workflow: &Workflow,
    identity: &RepetitionIdentity,
    warmup_turns: u32,
    timing: &ResourceTimingPlan,
    completion_hook_required: bool,
) -> Result<WarmupObservation> {
    let mut completed_turns = 0_u32;
    for turn in 1..=warmup_turns {
        let actor_name = resource_warmup_actor(identity.repetition, turn);
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("resource warm-up actor {actor_name:?} is absent"))
        })?;
        let turn_key = format!("resource-r{}-warmup-{turn}", identity.repetition);
        let session = driver.create_session(&actor_name).await?;
        driver.submit(&session, &actor.prompt, &turn_key).await?;
        let sessions = vec![(actor_name.clone(), actor.prompt.clone(), session.clone())];
        let completion_turn_keys = BTreeMap::from([(session.0.clone(), turn_key)]);
        let _ = driver
            .wait_ready(
                std::slice::from_ref(&session),
                Duration::from_millis(timing.reclaim_deadline_ms.min(200)),
            )
            .await;
        wait_resource_terminals(
            driver,
            &sessions,
            &completion_turn_keys,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
        )
        .await?;
        driver.close(&session).await?;
        completed_turns = completed_turns.saturating_add(1);
    }
    Ok(WarmupObservation {
        identity: identity.clone(),
        completed_turns,
        terminalized: true,
        closed: true,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_resource_group(
    collector: &mut ResourceCollector,
    driver: &mut HarnessDriver,
    roots: &[u32],
    workflow: &Workflow,
    identity: &RepetitionIdentity,
    agents: u32,
    width_order_index: u32,
    width_rotation_seed: u64,
    timing: &ResourceTimingPlan,
    completion_hook_required: bool,
) -> Result<GroupEvidence> {
    let prefix = format!("resource-r{}-n{agents}", identity.repetition);
    let checkpoint = resource_barrier_checkpoint(identity.repetition, agents);
    let baseline_phase = format!("{prefix}-baseline");
    let workload_phase = format!("{prefix}-workload");
    let cold_phase = format!("{prefix}-cold");
    let steady_phase = format!("{prefix}-steady");
    let turn_cpu_phase = format!("{prefix}-complete-turn-cpu");
    let post_turn_phase = format!("{prefix}-post-turn");
    let post_close_phase = format!("{prefix}-post-close");
    collector
        .sample_phase(
            roots,
            &baseline_phase,
            // Collect one complete discardable baseline window before the
            // required trailing window. macOS may asynchronously reclaim a
            // just-closed session's allocator pages even after warm idle; the
            // certified plateau remains the full normative trailing duration.
            Duration::from_millis(timing.idle_baseline_ms.saturating_mul(2)),
        )
        .await?;
    let baseline_sample = phase_samples(&collector.series, &baseline_phase)
        .last()
        .copied()
        .ok_or_else(|| AhrbError::Protocol("resource baseline sample is absent".to_owned()))?;
    let baseline_processes = baseline_sample
        .processes
        .iter()
        .map(|process| process.identity)
        .collect();
    let baseline_threads = baseline_sample.thread_count;
    let mut sessions = Vec::new();
    let mut expected_actors = BTreeSet::new();
    for index in 0..agents {
        let actor_name = resource_sweep_actor(identity.repetition, agents, index);
        let actor = workflow.actors.get(&actor_name).ok_or_else(|| {
            AhrbError::Protocol(format!("resource actor {actor_name:?} is absent"))
        })?;
        let session = driver.create_session(&actor_name).await?;
        sessions.push((actor_name.clone(), actor.prompt.clone(), session));
        expected_actors.insert(actor_name);
    }
    let expected_actor_sessions: BTreeMap<String, String> = sessions
        .iter()
        .map(|(actor, _, session)| (actor.clone(), session.0.clone()))
        .collect();
    let completion_turn_keys: BTreeMap<String, String> = sessions
        .iter()
        .enumerate()
        .map(|(index, (_, _, session))| {
            (
                session.0.clone(),
                format!(
                    "resource-r{}-n{agents}-turn-{}",
                    identity.repetition,
                    index + 1
                ),
            )
        })
        .collect();
    if agents == 1 {
        collector.sample_once(roots, &turn_cpu_phase)?;
    }
    let operation = async {
        for (index, (_, prompt, session)) in sessions.iter().enumerate() {
            driver
                .submit(
                    session,
                    prompt,
                    &format!(
                        "resource-r{}-n{agents}-turn-{}",
                        identity.repetition,
                        index + 1
                    ),
                )
                .await?;
        }
        let cohort = sessions
            .iter()
            .map(|(_, _, session)| session.clone())
            .collect::<Vec<_>>();
        let _ = driver
            .wait_ready(
                &cohort,
                Duration::from_millis(timing.reclaim_deadline_ms.min(200)),
            )
            .await;
        wait_resource_terminals(
            driver,
            &sessions,
            &completion_turn_keys,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
        )
        .await
    };
    let sampling = collector.sample_phase(
        roots,
        &workload_phase,
        Duration::from_millis(timing.barrier_hold_ms),
    );
    let (barrier_result, sample_result) = tokio::join!(operation, sampling);
    barrier_result?;
    sample_result?;
    collector
        .sample_phase(roots, &cold_phase, collector.counter_cadence)
        .await?;
    collector
        .sample_phase(
            roots,
            &steady_phase,
            Duration::from_millis(timing.barrier_hold_ms),
        )
        .await?;
    let mut observed_actors = BTreeSet::new();
    for (actor, _, session) in &sessions {
        let _ = session;
        observed_actors.insert(actor.clone());
    }
    if agents == 1 {
        collector.sample_once(roots, &turn_cpu_phase)?;
    }
    collector
        .sample_phase(
            roots,
            &post_turn_phase,
            Duration::from_millis(
                timing
                    .barrier_discard_ms
                    .saturating_add(timing.barrier_steady_ms),
            ),
        )
        .await?;
    let close_started = Instant::now();
    let mut closed_actor_sessions = BTreeMap::new();
    for (actor, _, session) in &sessions {
        driver.close(session).await?;
        closed_actor_sessions.insert(actor.clone(), session.0.clone());
    }
    let close_elapsed_ms = duration_millis(close_started.elapsed());
    collector
        .sample_phase(
            roots,
            &post_close_phase,
            Duration::from_millis(
                timing
                    .barrier_discard_ms
                    .saturating_add(timing.barrier_steady_ms),
            ),
        )
        .await?;
    let settled_after_ms = close_elapsed_ms.saturating_add(timing.barrier_discard_ms);
    let post_close = phase_samples(&collector.series, &post_close_phase);
    let post_close_sample = post_close
        .last()
        .copied()
        .ok_or_else(|| AhrbError::Protocol("resource post-close sample is absent".to_owned()))?;
    let post_close_processes: BTreeSet<_> = post_close_sample
        .processes
        .iter()
        .map(|process| process.identity)
        .collect();
    let remaining_workers = post_close_processes.difference(&baseline_processes).count();
    let post_close_threads = post_close_sample.thread_count;
    Ok(GroupEvidence {
        sweep: SweepObservation {
            identity: identity.clone(),
            agents,
            expected_barrier_actors: expected_actors,
            observed_barrier_actors: observed_actors,
            barrier_checkpoint: checkpoint,
            baseline_phase: baseline_phase.clone(),
            workload_phase: workload_phase.clone(),
            cold_phase,
            steady_phase: steady_phase.clone(),
            post_turn_phase,
            post_close_phase: post_close_phase.clone(),
            minimum_steady_processes: 1,
            minimum_baseline_processes: 1,
            minimum_post_turn_processes: 1,
            minimum_post_close_processes: 1,
            post_close_settled_after_ms: settled_after_ms,
            width_rotation_seed,
            width_order_index,
        },
        ordinary_return: ReturnToIdleObservation {
            identity: identity.clone(),
            baseline_phase,
            active_phase: workload_phase.clone(),
            returned_phase: post_close_phase,
            settled_after_ms,
            remaining_workers,
            minimum_baseline_processes: 1,
            minimum_returned_processes: 1,
        },
        single_agent: (agents == 1).then(|| SingleAgentObservation {
            identity: identity.clone(),
            turn_phase: turn_cpu_phase,
            scripted_turns: 1,
            barrier_phase: steady_phase,
        }),
        cleanup: CleanupObservation {
            identity: identity.clone(),
            reclaim_after_ms: settled_after_ms,
            remaining_workers,
            expected_actor_sessions,
            closed_actor_sessions,
            baseline_processes,
            post_close_processes,
            baseline_threads,
            post_close_threads,
        },
    })
}

async fn wait_resource_terminals(
    driver: &mut HarnessDriver,
    sessions: &[(String, String, crate::driver::SessionId)],
    completion_turn_keys: &BTreeMap<String, String>,
    completion_hook_required: bool,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        let mut complete = true;
        for (_, _, session) in sessions {
            let events = driver.attach(session, None).await?;
            let terminal = events.iter().any(resource_terminal_or_idle);
            let completion_hook = if completion_hook_required {
                let turn_key = completion_turn_keys.get(&session.0).ok_or_else(|| {
                    AhrbError::Protocol(format!(
                        "resource session {} omitted its completion turn key",
                        session.0
                    ))
                })?;
                events.iter().any(|event| {
                    event.event == EventVocab::HookCompleted
                        && event.payload.get("kind").and_then(Value::as_str) == Some("completion")
                        && event.payload.get("turn_key").and_then(Value::as_str)
                            == Some(turn_key.as_str())
                })
            } else {
                true
            };
            let client_exit = driver.client_exit(session);
            if !resource_session_fence(terminal, completion_hook, client_exit) {
                complete = false;
            }
        }
        if complete {
            return Ok(());
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(
                "resource sessions did not terminalize".to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn resource_terminal_or_idle(event: &NormalizedEvent) -> bool {
    event.event == EventVocab::TerminalSuccess
        || event.payload.get("state").and_then(Value::as_str) == Some("idle")
        || event.payload.get("run_state").and_then(Value::as_str) == Some("idle")
}

fn resource_session_fence(
    terminal_or_idle: bool,
    completion_hook: bool,
    client_exit: crate::driver::ClientExit,
) -> bool {
    terminal_or_idle
        && completion_hook
        && matches!(
            client_exit,
            crate::driver::ClientExit::NotApplicable | crate::driver::ClientExit::Exited(Some(0))
        )
}

async fn run_long_horizon(
    collector: &mut ResourceCollector,
    driver: &mut HarnessDriver,
    roots: &[u32],
    workflow: &Workflow,
    identity: &RepetitionIdentity,
    timing: &ResourceTimingPlan,
    completion_hook_required: bool,
) -> Result<(LongHorizonObservation, Vec<u64>)> {
    if timing.long_horizon_sample_turns == 0 {
        return Err(AhrbError::Validation(
            "long-horizon checkpoint cadence must be nonzero".to_owned(),
        ));
    }
    let baseline_phase = format!("resource-r{}-long-baseline", identity.repetition);
    collector
        .sample_phase(
            roots,
            &baseline_phase,
            Duration::from_millis(timing.idle_baseline_ms),
        )
        .await?;
    let baseline_bytes = phase_median_effective(&collector.series, &baseline_phase)?;
    let baseline_sample = phase_samples(&collector.series, &baseline_phase)
        .last()
        .copied()
        .ok_or_else(|| AhrbError::Protocol("long-horizon baseline sample is absent".to_owned()))?;
    let baseline_open_fds = baseline_sample.open_fds.ok_or_else(|| {
        AhrbError::Protocol("long-horizon baseline FD count is unavailable".to_owned())
    })?;
    let baseline_threads = baseline_sample.thread_count.ok_or_else(|| {
        AhrbError::Protocol("long-horizon baseline thread count is unavailable".to_owned())
    })?;
    let baseline_processes = baseline_sample
        .processes
        .iter()
        .map(|process| process.identity)
        .collect();
    let actor_name = resource_long_actor(identity.repetition);
    let session = driver.create_session(&actor_name).await?;
    let expected_session_id = session.0.clone();
    let session_id_hash = stable_evidence_hash(&session.0);
    let mut after = None;
    let mut points = vec![LongHorizonPoint {
        turn: 0,
        memory_bytes: baseline_bytes,
        open_fds: baseline_open_fds,
        threads: baseline_threads,
    }];
    let mut tool_results_by_turn = BTreeMap::new();
    let mut completed_turns = 0_u32;
    let mut turn_wall_ns = Vec::new();
    let mut turns = Vec::with_capacity(timing.long_horizon_turns as usize);
    for turn in 1..=timing.long_horizon_turns {
        let checkpoint = resource_long_checkpoint(identity.repetition, turn);
        let prompt = format!(
            "AHRB long horizon turn {turn} {}",
            route_marker(&workflow.scenario, &actor_name, &checkpoint)
        );
        let turn_key = format!("resource-long-r{}-turn-{turn}", identity.repetition);
        let submit_ns = monotonic_timestamp_ns();
        let turn_started = Instant::now();
        driver.submit(&session, &prompt, &turn_key).await?;
        let (terminal_cursor, tool_results, terminal_ns) = wait_one_terminal(
            driver,
            &session,
            after,
            &turn_key,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
            turn_started,
        )
        .await?;
        after = Some(terminal_cursor);
        let wall_ns = terminal_ns.checked_sub(submit_ns).ok_or_else(|| {
            AhrbError::Protocol("long-horizon submit/terminal boundaries are reversed".to_owned())
        })?;
        turn_wall_ns.push(wall_ns);
        turns.push(TurnObservation {
            repetition: identity.repetition.saturating_add(1),
            turn_index: turn,
            actor: actor_name.clone(),
            session_id_hash: session_id_hash.clone(),
            phase: "latency-vs-turn-index".to_owned(),
            launch_ns: None,
            submit_ns: Some(submit_ns),
            first_model_request_ns: None,
            terminal_ns: Some(terminal_ns),
            exit_ns: None,
            turn_wall_ns: Some(wall_ns),
        });
        completed_turns = completed_turns.saturating_add(1);
        tool_results_by_turn.insert(turn, tool_results);
        if turn % timing.long_horizon_sample_turns == 0 {
            let phase = format!("resource-r{}-long-turn-{turn}", identity.repetition);
            let settle_deadline = Instant::now()
                .checked_add(Duration::from_millis(timing.barrier_discard_ms))
                .unwrap_or_else(Instant::now);
            let mut settle_attempt = 0_u32;
            let sample = loop {
                let mut observed = collector.observe_once(roots, &phase)?;
                let threads_settled = observed
                    .thread_count
                    .is_some_and(|threads| threads <= baseline_threads);
                let fds_settled = observed
                    .open_fds
                    .is_some_and(|fds| fds <= baseline_open_fds);
                if (threads_settled && fds_settled) || Instant::now() >= settle_deadline {
                    collector.series.push(observed.clone())?;
                    break observed;
                }
                let settle_phase = format!("{phase}-settle-{settle_attempt}");
                observed.phase.clone_from(&settle_phase);
                for process in &mut observed.process_samples {
                    process.phase.clone_from(&settle_phase);
                }
                collector.series.push(observed)?;
                settle_attempt = settle_attempt.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            points.push(LongHorizonPoint {
                turn,
                memory_bytes: effective_sample_bytes(&sample),
                open_fds: sample.open_fds.ok_or_else(|| {
                    AhrbError::Protocol(format!(
                        "long-horizon FD count is unavailable at turn {turn}"
                    ))
                })?,
                threads: sample.thread_count.ok_or_else(|| {
                    AhrbError::Protocol(format!(
                        "long-horizon thread count is unavailable at turn {turn}"
                    ))
                })?,
            });
        }
    }
    driver.close(&session).await?;
    let closed_session_id = Some(session.0.clone());
    let final_post_close_phase = format!("resource-r{}-long-final", identity.repetition);
    // The quick profile's generic 100 ms discard can straddle delayed allocator
    // reclamation after a 100-turn session on macOS. Keep the normative trailing
    // steady window unchanged, but collect a full second of discardable,
    // out-of-band post-close evidence before it so a reclaim step is not
    // misclassified as an unstable steady plateau.
    let final_discard_ms = timing.barrier_discard_ms.max(1_000);
    collector
        .sample_phase(
            roots,
            &final_post_close_phase,
            Duration::from_millis(final_discard_ms.saturating_add(timing.barrier_steady_ms)),
        )
        .await?;
    let final_plateau = collector.series.trailing_plateau(
        &final_post_close_phase,
        MemoryMetric::Effective,
        1,
        timing.barrier_steady_ms.saturating_mul(1_000_000),
    )?;
    let final_sample = phase_samples(&collector.series, &final_post_close_phase)
        .last()
        .copied()
        .ok_or_else(|| AhrbError::Protocol("long-horizon final sample is absent".to_owned()))?;
    let final_post_close_open_fds = final_sample.open_fds.ok_or_else(|| {
        AhrbError::Protocol("long-horizon final FD count is unavailable".to_owned())
    })?;
    let final_post_close_threads = final_sample.thread_count.ok_or_else(|| {
        AhrbError::Protocol("long-horizon final thread count is unavailable".to_owned())
    })?;
    let final_post_close_processes = final_sample
        .processes
        .iter()
        .map(|process| process.identity)
        .collect();
    Ok((
        LongHorizonObservation {
            identity: identity.clone(),
            baseline_phase,
            final_post_close_phase,
            points,
            completed_turns,
            turns,
            tool_results_by_turn,
            expected_session_id,
            closed_session_id,
            baseline_bytes,
            final_post_close_bytes: final_plateau.median_bytes,
            baseline_open_fds,
            final_post_close_open_fds,
            baseline_threads,
            final_post_close_threads,
            baseline_processes,
            final_post_close_processes,
        },
        turn_wall_ns,
    ))
}

async fn wait_one_terminal(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<crate::driver::Cursor>,
    turn_key: &str,
    completion_hook_required: bool,
    deadline: Duration,
    started: Instant,
) -> Result<(crate::driver::Cursor, Vec<LongHorizonToolResult>, u64)> {
    let mut observed_tool_results = BTreeMap::new();
    let mut terminal_observed_ns = None;
    loop {
        let events = driver.attach(session, after).await?;
        for event in events
            .iter()
            .filter(|event| event.event == EventVocab::ToolResult)
        {
            let call_id = event
                .payload
                .get("call_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AhrbError::Protocol("long-horizon tool result omitted call_id".to_owned())
                })?;
            // Some harnesses (verified: haider 0.0.967) omit the tool name on the
            // result envelope; correlate it from the matching tool-call event.
            let correlated_name = events
                .iter()
                .filter(|candidate| candidate.event == EventVocab::ToolCall)
                .find(|candidate| {
                    candidate.payload.get("call_id").and_then(Value::as_str) == Some(call_id)
                })
                .and_then(|candidate| candidate.payload.get("name").and_then(Value::as_str));
            let name = event
                .payload
                .get("name")
                .and_then(Value::as_str)
                .or(correlated_name)
                .ok_or_else(|| {
                    AhrbError::Protocol(
                        "long-horizon tool result omitted name and no correlated call named it"
                            .to_owned(),
                    )
                })?;
            observed_tool_results.insert(
                event.cursor,
                LongHorizonToolResult {
                    event_id: event.id.clone(),
                    cursor: event.cursor,
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                },
            );
        }
        let terminal = events.iter().any(|event| is_terminal(&event.event));
        if terminal && terminal_observed_ns.is_none() {
            terminal_observed_ns = Some(monotonic_timestamp_ns());
        }
        let completion_hook = events.iter().any(|event| {
            event.event == EventVocab::HookCompleted
                && event.payload.get("kind").and_then(Value::as_str) == Some("completion")
                && event.payload.get("turn_key").and_then(Value::as_str) == Some(turn_key)
        });
        if terminal && (!completion_hook_required || completion_hook) {
            let cursor = events
                .iter()
                .map(|event| event.cursor)
                .max()
                .ok_or_else(|| AhrbError::Protocol("terminal attach was empty".to_owned()))?;
            return Ok((
                crate::driver::Cursor(cursor),
                observed_tool_results.into_values().collect(),
                terminal_observed_ns.ok_or_else(|| {
                    AhrbError::Protocol("long-horizon terminal clock disappeared".to_owned())
                })?,
            ));
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout("long-horizon turn".to_owned()));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn verified_process_roots(
    manifest: &Manifest,
    sampler: &mut dyn Sampler,
    launcher_pids: Vec<u32>,
    daemon_pid: Option<u32>,
) -> Result<Vec<u32>> {
    let mut roots: BTreeSet<u32> = launcher_pids.into_iter().collect();
    if let Some(pid) = daemon_pid {
        roots.insert(pid);
    }
    if roots.is_empty() {
        return Err(AhrbError::Protocol(
            "persistent harness exposed no launcher or daemon PID".to_owned(),
        ));
    }
    let roots: Vec<u32> = roots.into_iter().collect();
    let tree = sampler.discover(&roots)?;
    if let Some(pid) = daemon_pid {
        let process = tree
            .members
            .values()
            .find(|process| process.identity.pid == pid)
            .ok_or_else(|| {
                AhrbError::Protocol(format!("daemon PID locator {pid} is not inspectable"))
            })?;
        if !manifest.process.executable_names.is_empty()
            && !manifest
                .process
                .executable_names
                .iter()
                .any(|name| name == &process.command)
        {
            return Err(AhrbError::Protocol(format!(
                "daemon PID {pid} executable {:?} did not match {:?}",
                process.command, manifest.process.executable_names
            )));
        }
    }
    Ok(roots)
}

fn phase_samples<'a>(series: &'a SampleSeries, phase: &str) -> Vec<&'a Sample> {
    series
        .samples
        .iter()
        .filter(|sample| sample.phase == phase)
        .collect()
}

fn effective_sample_bytes(sample: &Sample) -> u64 {
    sample
        .pss_bytes
        .or(sample.footprint_bytes)
        .unwrap_or(sample.rss_bytes)
}

fn phase_median_effective(series: &SampleSeries, phase: &str) -> Result<u64> {
    let mut values: Vec<u64> = phase_samples(series, phase)
        .into_iter()
        .map(effective_sample_bytes)
        .collect();
    if values.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "phase {phase:?} has no samples"
        )));
    }
    values.sort_unstable();
    let middle = values.len() / 2;
    Ok(if values.len() % 2 == 1 {
        values[middle]
    } else {
        values[middle - 1].saturating_add(values[middle]) / 2
    })
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

async fn baseline_samples(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    profile: Profile,
) -> Result<Vec<Sample>> {
    if roots.is_empty() {
        return Ok(Vec::new());
    };
    let duration = match profile {
        Profile::Quick => Duration::from_millis(100),
        Profile::Cert => Duration::from_millis(580),
    };
    let started = Instant::now();
    let mut samples = Vec::new();
    loop {
        let tree = sampler.discover(roots)?;
        samples.push(sampler.sample(&tree, "warm-idle")?);
        if started.elapsed() >= duration {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(samples)
}

fn signal_owned_tree(tree: &ProcessTree) -> Result<()> {
    let mut identities = tree.members.keys().copied().collect::<Vec<_>>();
    identities.sort_by_key(|identity| (tree.roots.contains(identity), *identity));
    for identity in identities {
        let pid = i32::try_from(identity.pid)
            .map_err(|_| AhrbError::Validation("PID exceeds platform range".to_owned()))?;
        // SAFETY: every identity was freshly rediscovered from AHRB's verified root,
        // descendant, or isolated process-group membership. The stable start time is
        // retained by the sampler to prevent a reused PID from joining the owned set.
        let result = unsafe { libc::kill(pid, libc::SIGKILL) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    Ok(())
}

async fn await_owned_tree_empty(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    timeout: Duration,
) -> Result<bool> {
    await_owned_tree_settled(sampler, roots, &[], timeout).await
}

/// Wait until the tree rooted at `roots` holds no member other than the
/// `excluded` PIDs (e.g. a persistent daemon that is expected to linger).
async fn await_owned_tree_settled(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    excluded: &[u32],
    timeout: Duration,
) -> Result<bool> {
    let started = Instant::now();
    loop {
        let members = sampler.discover(roots)?.members;
        let remaining = members
            .keys()
            .filter(|identity| !excluded.contains(&identity.pid))
            .count();
        if remaining == 0 {
            return Ok(true);
        }
        if started.elapsed() >= timeout {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn per_invocation_workspaces_clean(
    profile_root: &Path,
    session: &crate::driver::SessionId,
) -> Result<bool> {
    let paths = [
        profile_root
            .join("ahrb-exec-sessions")
            .join(&session.0)
            .join("workspace"),
        profile_root
            .join("state")
            .join("workspaces")
            .join(&session.0),
    ];
    for path in paths {
        match std::fs::read_dir(&path) {
            Ok(mut entries) => {
                if entries.next().is_some() {
                    return Ok(false);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(true)
}

fn inject_torn_journal_tail(
    manifest: &Manifest,
    variables: &BTreeMap<String, String>,
    session: &crate::driver::SessionId,
) -> Result<()> {
    use std::io::Write as _;

    let mut rendered_variables = variables.clone();
    rendered_variables.insert("session_id".to_owned(), session.0.clone());
    let rendered = crate::manifest::render_template(&manifest.events.path, &rendered_variables)?;
    let journal_path = PathBuf::from(rendered);
    let profile = variables
        .get("profile")
        .ok_or_else(|| AhrbError::Protocol("profile variable is absent".to_owned()))?;
    let canonical_profile = Path::new(profile).canonicalize()?;
    let canonical_journal = journal_path.canonicalize()?;
    if !canonical_journal.starts_with(&canonical_profile) || !canonical_journal.is_file() {
        return Err(AhrbError::Validation(format!(
            "durable journal {} is not a regular file inside cold profile {}",
            canonical_journal.display(),
            canonical_profile.display()
        )));
    }
    let contents = std::fs::read(&canonical_journal)?;
    if contents.is_empty() || !contents.ends_with(b"\n") {
        return Err(AhrbError::Protocol(format!(
            "durable journal {} lacked a complete committed tail before fault injection",
            canonical_journal.display()
        )));
    }
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&canonical_journal)?;
    journal.write_all(br#"{"id":"ahrb-induced-torn-tail","cursor":18446744073709551615"#)?;
    journal.sync_all()?;
    Ok(())
}

fn validate_recovered_suffix(
    original: &[NormalizedEvent],
    after: Option<Cursor>,
    recovered: &[NormalizedEvent],
) -> std::result::Result<(), String> {
    let after_cursor = after.map_or(0, |cursor| cursor.0);
    let mut expected = original
        .iter()
        .filter(|event| event.cursor > after_cursor)
        .collect::<Vec<_>>();
    // A live thin client may emit a nondurable acceptance announcement before
    // the daemon journal begins. Replay is allowed to omit only that leading
    // transport acknowledgement; every durable event remains exact.
    if expected
        .first()
        .is_some_and(|event| event.event == EventVocab::TurnAccepted)
        && recovered
            .first()
            .is_none_or(|event| event.event != EventVocab::TurnAccepted)
    {
        expected.remove(0);
    }
    if recovered.is_empty() {
        return Err("journal replay returned an empty suffix".to_owned());
    }
    if recovered.len() != expected.len() {
        let describe = |events: &[&NormalizedEvent]| {
            events
                .iter()
                .map(|event| format!("{:?}@{}", event.event, event.cursor))
                .collect::<Vec<_>>()
                .join(",")
        };
        let recovered_refs: Vec<&NormalizedEvent> = recovered.iter().collect();
        return Err(format!(
            "journal replay length mismatch: expected {} [{}], recovered {} [{}]",
            expected.len(),
            describe(&expected),
            recovered.len(),
            describe(&recovered_refs)
        ));
    }
    let mut ids = BTreeSet::new();
    let mut expected_cursor = expected
        .first()
        .map(|event| event.cursor)
        .ok_or_else(|| "journal replay had no expected durable suffix".to_owned())?;
    for (index, (actual, expected_event)) in recovered.iter().zip(expected).enumerate() {
        if actual.cursor != expected_cursor {
            return Err(format!(
                "journal replay cursor gap at suffix index {index}: expected {expected_cursor}, got {}",
                actual.cursor
            ));
        }
        if !ids.insert(actual.id.as_str()) {
            return Err(format!(
                "journal replay duplicated event id {:?} at suffix index {index}",
                actual.id
            ));
        }
        let mut actual_durable = actual.clone();
        let mut expected_durable = expected_event.clone();
        for event in [&mut actual_durable, &mut expected_durable] {
            if let Some(payload) = event.payload.as_object_mut() {
                // Row 69 attaches collector receipt bounds while normalizing
                // each carrier observation. They are deliberately external to
                // the harness journal and therefore cannot participate in a
                // durable replay equality check.
                payload.remove("_ahrb_receipt");
            }
            if matches!(
                event.event,
                EventVocab::TerminalSuccess
                    | EventVocab::TerminalFailure
                    | EventVocab::TerminalCancelled
                    | EventVocab::TerminalTimeout
            ) && let Some(payload) = event.payload.as_object_mut()
            {
                // These fields are added by the observing client at exit or by
                // the live stream carrier and are intentionally absent from the
                // daemon-owned journal (verified on haider 0.0.967: the durable
                // terminal is `{"state": done|errored|cancelled}`; `terminal_kind`
                // and `error_code` are derived on the jsonl carrier only).
                for key in [
                    "client_turn_wall_ms",
                    "exit_code",
                    "status",
                    "category",
                    "failure_marker",
                    "terminal_kind",
                    "error_code",
                ] {
                    payload.remove(key);
                }
            }
        }
        if actual_durable != expected_durable {
            return Err(format!(
                "journal replay event at suffix index {index} disagrees with the pre-crash durable journal"
            ));
        }
        expected_cursor = expected_cursor.saturating_add(1);
    }
    Ok(())
}

/// A structured "no crash window to reconcile" probe answer: `error.code ==
/// "no_recovery"` without a success claim. Verified on haider 0.0.967
/// (`haider.session_recovery.v1`, `completed:false`).
fn typed_no_recovery_response(value: &Value) -> bool {
    value
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        == Some("no_recovery")
        && value.get("completed").and_then(Value::as_bool) != Some(true)
}

fn control_response_succeeded(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.is_empty() || object.get("error").is_some_and(|error| !error.is_null()) {
        return false;
    }
    if ["ok", "success", "recovered", "recoverable"]
        .iter()
        .any(|key| object.get(*key).and_then(Value::as_bool) == Some(false))
    {
        return false;
    }
    !["status", "state", "outcome"]
        .iter()
        .filter_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::to_ascii_lowercase)
        .any(|status| {
            status.contains("error")
                || status.contains("fail")
                || status.contains("not_found")
                || status.contains("rejected")
                || status.contains("invalid")
                || status == "did_not_stop"
        })
}

fn incomplete_resource_evidence(state: &RunState, manifest: &Manifest) -> ResourceEvidence {
    let idle_samples: Vec<&Sample> = state
        .samples
        .iter()
        .filter(|sample| sample.phase == "warm-idle")
        .collect();
    let process_count = |sample: Option<&&Sample>| {
        sample.map_or(0, |sample| {
            if manifest.daemon.persistent {
                sample.processes.len().saturating_sub(1)
            } else {
                sample.processes.len()
            }
        })
    };
    ResourceEvidence {
        // The current runner has one process launch, but does not yet perform one
        // complete fresh-profile N=1,2,4,8 repetition. Calling it a repetition
        // would let partial evidence certify, so completeness remains zero.
        completed_repetitions: 0,
        lifecycle_notes: Vec::new(),
        series: SampleSeries {
            samples: state.samples.clone(),
        },
        turn_wall_ns: Vec::new(),
        phases: ResourcePhases::default(),
        memory_metric: (!state.samples.is_empty()).then_some(MemoryMetric::Effective),
        sampler_cadence_ns: Some(20_000_000),
        idle: Some(IdleObservation {
            declared_model: if manifest.daemon.persistent {
                IdleProcessModel::PersistentTree
            } else {
                IdleProcessModel::ZeroProcessBetweenTurns
            },
            busy_polling_detected: None,
            initial_workers: process_count(idle_samples.first()),
            final_workers: process_count(idle_samples.last()),
            initial_threads: None,
            final_threads: None,
        }),
        warmup: None,
        sweep: Vec::new(),
        ordinary_return: None,
        cold_start: None,
        single_agent: None,
        cleanup: None,
        long_horizon: None,
    }
}

fn membership_report_samples(
    refreshes_by_phase: &BTreeMap<String, Vec<MembershipRefreshEvidence>>,
) -> Vec<MembershipSample> {
    let mut membership = refreshes_by_phase
        .iter()
        .flat_map(|(phase, refreshes)| {
            refreshes.iter().map(|refresh| MembershipSample {
                elapsed_ns: refresh.elapsed_ns,
                phase: phase.clone(),
                discovery_wall_ns: refresh.discovery_wall_ns,
                discovery_cpu_ns: refresh.discovery_cpu_ns,
                lane: refresh.lane,
            })
        })
        .collect::<Vec<_>>();
    membership.sort_by_key(|sample| (sample.elapsed_ns, sample.lane, sample.phase.clone()));
    membership
}

fn resource_evidence_turns(evidence: &ResourceEvidence) -> u64 {
    let warmup = evidence
        .warmup
        .as_deref()
        .unwrap_or_default()
        .iter()
        .fold(0_u64, |total, observation| {
            total.saturating_add(u64::from(observation.completed_turns))
        });
    let sweep = evidence.sweep.iter().fold(0_u64, |total, observation| {
        total.saturating_add(u64::from(observation.agents))
    });
    let long_horizon = evidence
        .long_horizon
        .as_deref()
        .unwrap_or_default()
        .iter()
        .fold(0_u64, |total, observation| {
            total.saturating_add(u64::from(observation.completed_turns))
        });
    warmup.saturating_add(sweep).saturating_add(long_horizon)
}

fn enforce_sampler_overhead(rows: &mut [TestResult], sampler_overhead_pct: f64) {
    if sampler_overhead_pct <= 10.0 {
        return;
    }
    let error = format!("sampler overload: {sampler_overhead_pct:.3}% membership discovery CPU");
    for row in rows {
        row.outcome = TestOutcome::Error(error.clone());
        row.evidence.push(error.clone());
    }
}

fn process_observations(samples: &[Sample]) -> Vec<ProcessSample> {
    let mut processes = Vec::new();
    for sample in samples {
        processes.extend(sample.process_samples.iter().cloned());
    }
    processes.sort_by(|left, right| {
        left.elapsed_ns
            .cmp(&right.elapsed_ns)
            .then_with(|| left.process.identity.cmp(&right.process.identity))
    });
    processes
}

fn deterministic_run_id(manifest_hash: &str, rows: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest_hash.as_bytes());
    for row in rows {
        digest.update([*row]);
    }
    let value = format!("{:x}", digest.finalize());
    format!("ahrb-{}", &value[..16])
}

fn workflow_hash() -> String {
    let mut digest = Sha256::new();
    for definition in crate::scenarios::all() {
        digest.update([definition.row]);
        digest.update(definition.id.as_bytes());
        digest.update(definition.metric.as_bytes());
        digest.update(definition.pass_criteria.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn stable_evidence_hash(value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(value.as_bytes());
    format!("{:x}", digest.finalize())
}

fn host_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        return std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    line.strip_prefix("MemTotal:")
                        .and_then(|value| value.split_whitespace().next())
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(|kib| kib.saturating_mul(1024))
                })
            })
            .unwrap_or(0);
    }
    #[cfg(target_os = "macos")]
    {
        let output = crate::process::owned_command_output(
            std::process::Command::new("sysctl").args(["-n", "hw.memsize"]),
        );
        match output {
            Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .unwrap_or(0),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod resource_sampler_tests {
    use super::*;

    #[test]
    fn row49_resource_mirror_excludes_nullable_ratio() {
        let evaluation = LatencyVsTurnIndexEvaluation {
            metrics: BTreeMap::new(),
            first_decile_p50_ms: 1.0,
            last_decile_p50_ms: 2.0,
            theil_sen_ms_per_turn: 0.01,
            latency_slope_ms_per_100_turns: 1.0,
            latency_last_first_decile_ratio: Some(2.0),
            details: json!({}),
            measurement_complete: true,
            passed: false,
            measurement_error: None,
            failure_detail: Some("fixture".to_owned()),
        };
        let metrics = latency_vs_turn_index_resource_metrics(&evaluation);
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics["latency_slope_ms_per_100_turns"], 1.0);
        assert!(!metrics.contains_key("latency_last_first_decile_ratio"));
    }

    #[test]
    fn cert_fanout_result_preserves_absent_vs_explicit_low_width() {
        let base = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))
            .expect("load mock manifest");
        for row in [54, 55] {
            let definition = crate::scenarios::all()
                .iter()
                .find(|definition| definition.row == row)
                .expect("fanout definition");

            let mut missing = base.clone();
            missing.concurrency.max_agents = None;
            let missing_result = capability_result(
                definition,
                capability_for_profile(&missing, row, Profile::Cert),
            )
            .expect("missing width must classify without measurement");
            assert!(matches!(missing_result.outcome, TestOutcome::Absent(_)));

            let mut explicit_low = base.clone();
            explicit_low.concurrency.max_agents = Some(8);
            let low_result = capability_result(
                definition,
                capability_for_profile(&explicit_low, row, Profile::Cert),
            )
            .expect("explicit low cert width must classify without measurement");
            assert!(matches!(low_result.outcome, TestOutcome::Unsupported(_)));
        }
    }

    struct LateChurnSampler {
        refreshes: u32,
        started: Instant,
    }

    impl Default for LateChurnSampler {
        fn default() -> Self {
            Self {
                refreshes: 0,
                started: Instant::now(),
            }
        }
    }

    impl Sampler for LateChurnSampler {
        fn discover(&mut self, _roots: &[u32]) -> Result<ProcessTree> {
            self.refreshes = self.refreshes.saturating_add(1);
            let root = crate::process::ProcIdentity {
                pid: 100,
                start_time: 1,
            };
            let mut tree = ProcessTree::default();
            tree.roots.insert(root);
            tree.members.insert(
                root,
                crate::process::ProcessInfo {
                    identity: root,
                    ppid: 0,
                    command: "late-root".to_owned(),
                    ownership: crate::process::ProcOwnership::DeclaredRoot,
                },
            );
            if self.refreshes >= 2 {
                let child = crate::process::ProcIdentity {
                    pid: 101,
                    start_time: 2,
                };
                tree.members.insert(
                    child,
                    crate::process::ProcessInfo {
                        identity: child,
                        ppid: 100,
                        command: "late-child".to_owned(),
                        ownership: crate::process::ProcOwnership::Descendant,
                    },
                );
            }
            Ok(tree)
        }

        fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample> {
            let elapsed_ns = duration_ns(self.started.elapsed());
            let wall_time = std::time::SystemTime::now();
            let process_samples = tree
                .members
                .values()
                .cloned()
                .map(|process| {
                    let late_child = process.identity.pid == 101;
                    ProcessSample {
                        elapsed_ns,
                        wall_time,
                        phase: phase.to_owned(),
                        process,
                        rss_bytes: 0,
                        pss_bytes: None,
                        private_bytes: None,
                        footprint_bytes: None,
                        rss_crosscheck_bytes: None,
                        cpu_ns: 0,
                        open_fds: Some(if late_child { 5 } else { 3 }),
                        thread_count: Some(if late_child { 2 } else { 1 }),
                    }
                })
                .collect::<Vec<_>>();
            Ok(Sample {
                elapsed_ns,
                wall_time,
                phase: phase.to_owned(),
                rss_bytes: 0,
                pss_bytes: None,
                private_bytes: None,
                footprint_bytes: None,
                rss_crosscheck_bytes: None,
                cgroup_memory_bytes: None,
                cgroup_peak_bytes: None,
                cpu_ns: 0,
                open_fds: Some(
                    process_samples
                        .iter()
                        .filter_map(|sample| sample.open_fds)
                        .sum(),
                ),
                thread_count: Some(
                    process_samples
                        .iter()
                        .filter_map(|sample| sample.thread_count)
                        .sum(),
                ),
                collection_ns: 1,
                collection_wall_ns: 1,
                processes: tree.members.values().cloned().collect(),
                process_samples,
                cpu_accounting_warnings: Vec::new(),
            })
        }
    }

    #[test]
    fn process_hygiene_cadence_collector_captures_late_child_thread_and_fd_churn() {
        let sampler = start_process_hygiene_turn_sampler(
            Box::new(LateChurnSampler::default()),
            vec![100],
            "row44-late-churn".to_owned(),
            Duration::from_millis(10),
        )
        .expect("start deterministic cadence collector");
        // Keep the synthetic active window long enough that debug-build thread
        // accounting remains below the normative 10% sampler-overhead ceiling
        // even when the host is scheduling other compiler/test work.
        std::thread::sleep(Duration::from_millis(50));
        let collection = sampler.finish().expect("finish cadence collector");
        assert_eq!(collection.samples[0].processes.len(), 1);
        assert!(
            collection
                .samples
                .iter()
                .skip(1)
                .any(|sample| sample.processes.len() == 2)
        );
        let mut evidence = ProcessHygieneEvidence::default();
        record_process_hygiene_turn(&mut evidence, 1, 1, 10_000_000, &collection);
        evidence.per_turn_audits.push(ProcessHygieneAudit {
            repetition: 1,
            turn_index: Some(1),
            waited_ms: 2_000,
            processes: Vec::new(),
        });
        evidence.growth_checkpoints.extend([
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 0,
                processes: Vec::new(),
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 1,
                processes: Vec::new(),
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
        ]);
        let evaluation = evaluate_process_hygiene(&evidence, 1, 1, true);
        assert!(
            evaluation.measurement_complete,
            "late-churn measurement error: {:?}",
            evaluation.measurement_error
        );
        assert_eq!(
            evaluation.metrics["process_hygiene.observed_processes_spawned_per_turn_max"],
            2.0
        );
        assert_eq!(
            evaluation.metrics["process_hygiene.observed_threads_created_per_turn_max"],
            3.0
        );
        assert_eq!(
            evaluation.metrics["process_hygiene.observed_fds_opened_per_turn_max"],
            8.0
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_hygiene_actual_residual_child_fixture_fails() {
        use std::os::unix::process::CommandExt as _;

        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "sleep 5 & sleep 0.2"]);
        command.process_group(0);
        let mut leader = command
            .spawn()
            .expect("spawn isolated residual-child fixture");
        let root_pid = leader.id();
        let sampler = platform_sampler();
        let mut evidence = ProcessHygieneEvidence::default();
        let turn_sampler = start_process_hygiene_turn_sampler(
            sampler,
            vec![root_pid],
            "row44-residual-fixture-active".to_owned(),
            Duration::from_millis(10),
        )
        .expect("start residual fixture cadence sampler");
        leader.wait().expect("reap fixture group leader");
        let mut collection = turn_sampler
            .finish()
            .expect("finish residual fixture cadence sampler");
        record_process_hygiene_turn(&mut evidence, 1, 1, 10_000_000, &collection);
        let (waited_ms, residue) = collect_process_hygiene_audit(
            collection.sampler.as_mut(),
            &[root_pid],
            "row44-residual-fixture-audit",
            &mut evidence,
        )
        .await
        .expect("audit detached fixture child");
        evidence.per_turn_audits.push(ProcessHygieneAudit {
            repetition: 1,
            turn_index: Some(1),
            waited_ms,
            processes: residue.clone(),
        });
        evidence.growth_checkpoints.extend([
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 0,
                processes: Vec::new(),
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 1,
                processes: residue,
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
        ]);

        let group = i32::try_from(root_pid).expect("fixture PID fits pid_t");
        // SAFETY: `process_group(0)` above made the just-spawned leader's PID the
        // fixture-only process group; the negative target cannot address AHRB.
        let _ = unsafe { libc::kill(-group, libc::SIGKILL) };

        let evaluation = evaluate_process_hygiene(&evidence, 1, 1, true);
        assert!(
            evaluation.measurement_complete,
            "residual fixture measurement error: {:?}",
            evaluation.measurement_error
        );
        assert!(!evaluation.passed);
        assert!(evaluation.metrics["process_hygiene.residue_processes"] >= 1.0);
        assert!(
            evaluation.details["residue_identities"]
                .as_array()
                .is_some_and(|identities| !identities.is_empty())
        );
    }

    #[test]
    fn deadline_report_preserves_completed_rows_and_marks_only_pending_rows_deadline() {
        let output = std::env::temp_dir().join(format!(
            "ahrb-partial-deadline-report-{}",
            std::process::id()
        ));
        if output.exists() {
            std::fs::remove_dir_all(&output).expect("remove stale partial deadline output");
        }
        let options = RunOptions {
            manifest: PathBuf::from("adapters/mock/manifest.toml"),
            output: output.clone(),
            profile: Profile::Quick,
            tests: vec![1, 2, 3],
            junit: false,
            deadline_secs: Some(1),
            no_save: true,
            harness_version: Some("mock-harness 0.1.0".to_owned()),
        };
        let manifest = crate::manifest::load(&options.manifest).expect("load mock manifest");
        let persistence =
            crate::results::prepare(&options, &manifest).expect("prepare test persistence");
        let selected = selected_definitions(&options).expect("select partial deadline rows");
        let progress = RunProgress::default();
        progress
            .update(|state| {
                state.launched.extend([1, 2]);
                state.completed.insert(1);
                state.results.insert(
                    1,
                    TestResult {
                        row: 1,
                        id: selected[0].id.to_owned(),
                        pillar: selected[0].pillar,
                        outcome: TestOutcome::Pass,
                        evidence: vec!["completed evidence".to_owned()],
                        metadata: TestResultMetadata::for_row(1, &TestOutcome::Pass),
                    },
                );
            })
            .expect("record partial progress");
        write_deadline_report(
            &options,
            &manifest,
            &selected,
            &progress,
            &persistence,
            "deadline after 1s",
        )
        .expect("write partial deadline report");
        let report: Report = serde_json::from_slice(
            &std::fs::read(output.join("report.json")).expect("read partial deadline report"),
        )
        .expect("parse partial deadline report");
        assert!(matches!(report.results[0].outcome, TestOutcome::Pass));
        assert!(matches!(
            &report.results[1].outcome,
            TestOutcome::Error(detail) if detail == "deadline"
        ));
        assert!(matches!(
            &report.results[2].outcome,
            TestOutcome::Error(detail) if detail == "deadline"
        ));
        assert!(report.results[1].evidence[0].contains("active"));
        assert!(report.results[2].evidence[0].contains("not launched"));
        std::fs::remove_dir_all(output).expect("remove partial deadline output");
    }

    #[test]
    fn row_timeout_is_recorded_without_becoming_a_run_error() -> Result<()> {
        let mut errors = BTreeMap::new();
        let result: Result<()> = Err(AhrbError::Timeout("one turn".to_owned()));
        let progress = RunProgress::default();
        assert!(row_timeout(7, result, &mut errors, &progress)?.is_none());
        assert_eq!(
            errors.get(&7).map(String::as_str),
            Some("turn timeout: one turn")
        );
        assert_eq!(
            progress.snapshot()?.row_errors.get(&7).map(String::as_str),
            Some("turn timeout: one turn")
        );
        Ok(())
    }

    fn generated_file_variables(profile: &Path) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("profile".to_owned(), profile.to_string_lossy().into_owned()),
            ("base_url".to_owned(), "http://127.0.0.1:12345".to_owned()),
            ("credential".to_owned(), "test-credential".to_owned()),
            ("model".to_owned(), "ahrb-fake-v1".to_owned()),
        ])
    }

    #[test]
    fn opencode_file_valued_environment_does_not_create_config_as_directory() {
        let manifest = crate::manifest::load(Path::new("adapters/opencode/manifest.toml"))
            .expect("load OpenCode manifest");
        let profile =
            std::env::temp_dir().join(format!("ahrb-opencode-config-fixed-{}", std::process::id()));
        if profile.exists() {
            std::fs::remove_dir_all(&profile).expect("remove stale OpenCode profile");
        }
        prepare_profile(&manifest, &profile).expect("prepare OpenCode profile");
        let variables = generated_file_variables(&profile);
        let config_path = profile.join("config/opencode/opencode.json");
        assert!(!config_path.exists());
        write_generated_files(&manifest, &variables, &profile)
            .expect("write OpenCode generated configuration");
        assert!(config_path.is_file());
        let environment = isolated_environment(&manifest, &variables)
            .expect("render OpenCode isolated environment");
        assert_eq!(
            environment.get("OPENCODE_CONFIG").map(String::as_str),
            config_path.to_str()
        );
        std::fs::remove_dir_all(profile).expect("remove OpenCode profile");
    }

    #[test]
    fn haider_profile_prepares_declared_runtime_root() {
        let manifest = crate::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))
            .expect("load Haider manifest");
        let profile = PathBuf::from(format!("/tmp/ahrb-hr-{:x}", std::process::id()));
        if profile.exists() {
            std::fs::remove_dir_all(&profile).expect("remove stale Haider profile");
        }
        prepare_profile(&manifest, &profile).expect("prepare Haider profile");
        let variables = generated_file_variables(&profile);
        let environment = isolated_environment(&manifest, &variables)
            .expect("render Haider isolated environment");
        assert_eq!(
            environment.get("XDG_RUNTIME_DIR").map(String::as_str),
            profile.join("run").to_str()
        );
        assert!(profile.join("run").is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(profile.join("run"))
                    .expect("runtime metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        let repetition = profile.join("rr0");
        prepare_profile(&manifest, &repetition).expect("prepare short repetition profile");
        std::fs::remove_dir_all(profile).expect("remove Haider profile");
    }

    #[test]
    fn generated_file_io_error_names_the_colliding_path() {
        let mut manifest = crate::manifest::load(Path::new("adapters/opencode/manifest.toml"))
            .expect("load OpenCode manifest");
        let binding = manifest
            .isolation
            .environment
            .remove("OPENCODE_CONFIG")
            .expect("OpenCode config environment binding");
        manifest
            .isolation
            .roots
            .insert("OPENCODE_CONFIG".to_owned(), binding);
        let profile = std::env::temp_dir().join(format!(
            "ahrb-opencode-config-collision-{}",
            std::process::id()
        ));
        if profile.exists() {
            std::fs::remove_dir_all(&profile).expect("remove stale collision profile");
        }
        prepare_profile(&manifest, &profile).expect("reproduce directory collision");
        let config_path = profile.join("config/opencode/opencode.json");
        assert!(config_path.is_dir());
        let error = write_generated_files(&manifest, &generated_file_variables(&profile), &profile)
            .expect_err("directory collision must fail as a generated-file write");
        let message = error.to_string();
        assert!(message.contains("write generated file"));
        assert!(message.contains(config_path.to_string_lossy().as_ref()));
        assert!(message.contains("Is a directory"));
        std::fs::remove_dir_all(profile).expect("remove collision profile");
    }

    fn recovery_event(cursor: u64) -> NormalizedEvent {
        NormalizedEvent {
            id: format!("event-{cursor}"),
            cursor,
            session_id: "session-recovery".to_owned(),
            actor: "root".to_owned(),
            event: EventVocab::ToolResult,
            payload: json!({"cursor": cursor}),
        }
    }

    #[tokio::test]
    async fn owned_pid_locator_retries_transient_invalid_contents() {
        let mut manifest = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))
            .expect("load mock manifest");
        manifest.daemon.readiness.pid_pointer.clear();
        manifest.daemon.readiness.ready_pointer.clear();
        manifest.daemon.readiness.kind = "file".to_owned();
        manifest.daemon.readiness.command.clear();
        let root =
            std::env::temp_dir().join(format!("ahrb-pid-locator-retry-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("remove stale PID-locator test directory");
        }
        let state = root.join("state");
        std::fs::create_dir_all(&state).expect("create PID-locator test state");
        let locator = state.join("daemon.pid");
        std::fs::write(&locator, []).expect("publish transient empty PID locator");
        let expected_pid = std::process::id();
        let writer_locator = locator.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            std::fs::write(writer_locator, expected_pid.to_string())
                .expect("publish complete PID locator");
        });
        let variables =
            BTreeMap::from([("profile".to_owned(), root.to_string_lossy().into_owned())]);
        let observed = await_owned_pid(&manifest, &variables)
            .await
            .expect("retry transient PID locator")
            .expect("mock manifest declares a PID locator");
        writer.await.expect("PID-locator writer task");
        assert_eq!(observed, expected_pid);
        std::fs::remove_dir_all(root).expect("remove PID-locator test directory");
    }

    #[test]
    fn codex_fixture_scripts_defer_native_selection_with_executable_argv() {
        let manifest = crate::manifest::load(Path::new("adapters/codex/manifest.toml"))
            .expect("load Codex manifest");
        let write = mapped_tool_call(
            &manifest,
            "write",
            "call-write".to_owned(),
            json!({"path": "fixture.txt", "content": "fixture payload"}),
        )
        .expect("map Codex fixture write");
        assert_eq!(
            write.get("name").and_then(Value::as_str),
            Some("write_fixture")
        );
        assert_eq!(
            write
                .pointer("/_ahrb_native/aliases/0")
                .and_then(Value::as_str),
            Some("shell_command")
        );
        let command = write
            .pointer("/_ahrb_native/argv")
            .and_then(Value::as_array)
            .expect("deferred Codex shell command keeps argv");
        let command: Vec<_> = command.iter().filter_map(Value::as_str).collect();
        assert!(command.first().is_some_and(|program| {
            program.ends_with("ahrb-fixture") || *program == "ahrb-fixture"
        }));
        assert_eq!(
            &command[1..],
            [
                "write",
                "--path",
                "fixture.txt",
                "--content",
                "fixture payload"
            ]
        );

        let read = mapped_tool_call(
            &manifest,
            "read",
            "call-read".to_owned(),
            json!({"path": "fixture.txt"}),
        )
        .expect("map Codex fixture read");
        assert_eq!(
            read.get("name").and_then(Value::as_str),
            Some("read_fixture")
        );
        let read_command: Vec<_> = read
            .pointer("/_ahrb_native/argv")
            .and_then(Value::as_array)
            .expect("deferred Codex read command keeps argv")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(&read_command[1..], ["read", "--path", "fixture.txt"]);

        let fail = mapped_tool_call(
            &manifest,
            "fail",
            "call-fail".to_owned(),
            json!({"message": "expected failure"}),
        )
        .expect("map Codex fixture failure");
        assert_eq!(
            fail.get("name").and_then(Value::as_str),
            Some("fail_fixture")
        );
        let fail_command: Vec<_> = fail
            .pointer("/_ahrb_native/argv")
            .and_then(Value::as_array)
            .expect("deferred Codex fail command keeps argv")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(
            &fail_command[1..],
            ["fail", "--message", "expected failure"]
        );
    }

    #[test]
    fn individual_membership_discovery_overrun_is_rejected() {
        assert!(reject_membership_overrun(10_000_000, Duration::from_millis(10)).is_ok());
        let error = reject_membership_overrun(10_000_001, Duration::from_millis(10))
            .expect_err("membership collection beyond its cadence must fail");
        assert!(error.to_string().contains("sampler overload"));
    }

    #[test]
    fn aggregate_membership_cpu_over_ten_percent_errors_resource_rows() {
        let mut rows = vec![classify(
            20,
            "idle-footprint",
            crate::evaluate::Pillar::Resource,
            Some(true),
            &[Assertion {
                name: "measured".to_owned(),
                passed: true,
                detail: "external evidence".to_owned(),
            }],
            None,
        )];
        enforce_sampler_overhead(&mut rows, 10.001);
        assert!(matches!(
            &rows[0].outcome,
            TestOutcome::Error(detail) if detail.contains("sampler overload")
        ));
    }

    #[test]
    fn resource_sweep_scripts_one_terminal_segment_per_actor() {
        let manifest = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))
            .expect("load mock manifest");
        let timing = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let mut actors = BTreeMap::new();
        let mut responses = Vec::new();
        add_resource_workflow(
            "terminal-segment-test",
            Path::new("/tmp/ahrb-script-test"),
            timing.clone(),
            &manifest,
            &mut actors,
            &mut responses,
        )
        .expect("build resource scripts");
        for repetition in 0..timing.repetitions {
            for agents in &timing.sweep_widths {
                let prefix = format!("resource-r{repetition}-n{agents}-a");
                let terminals = responses
                    .iter()
                    .filter(|response| {
                        response.actor.starts_with(&prefix) && response.checkpoint == "terminal"
                    })
                    .count();
                assert_eq!(terminals, *agents as usize);
            }
        }
    }

    #[test]
    fn resource_fence_requires_success_or_idle_and_zero_client_exit() {
        use crate::driver::ClientExit;
        assert!(resource_session_fence(
            true,
            true,
            ClientExit::NotApplicable
        ));
        assert!(resource_session_fence(
            true,
            true,
            ClientExit::Exited(Some(0))
        ));
        assert!(!resource_session_fence(true, true, ClientExit::Running));
        assert!(!resource_session_fence(
            true,
            true,
            ClientExit::Exited(Some(1))
        ));
        assert!(!resource_session_fence(
            true,
            true,
            ClientExit::Exited(None)
        ));
        assert!(!resource_session_fence(
            false,
            true,
            ClientExit::Exited(Some(0))
        ));
    }

    #[test]
    fn profile_roots_are_create_once_and_cannot_be_prewarmed() {
        let manifest = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))
            .expect("load mock manifest");
        let root = std::env::temp_dir().join(format!("ahrb-cold-profile-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("remove stale cold profile");
        }
        prepare_profile(&manifest, &root).expect("create cold profile once");
        let error = prepare_profile(&manifest, &root)
            .expect_err("a second launch must not reuse a warmed profile");
        assert!(
            error
                .to_string()
                .contains("refusing to reuse non-cold profile")
        );
        std::fs::remove_dir_all(root).expect("remove cold profile");
    }

    #[test]
    fn recovered_suffix_requires_exact_contiguous_identity_agreement() {
        let original = vec![recovery_event(1), recovery_event(2), recovery_event(3)];
        assert!(validate_recovered_suffix(&original, Some(Cursor(1)), &original[1..]).is_ok());

        let mut gap = original[1..].to_vec();
        gap[0].cursor = 3;
        assert!(validate_recovered_suffix(&original, Some(Cursor(1)), &gap).is_err());

        let mut duplicate = original[1..].to_vec();
        duplicate[1].id = duplicate[0].id.clone();
        assert!(validate_recovered_suffix(&original, Some(Cursor(1)), &duplicate).is_err());

        let mut changed = original[1..].to_vec();
        changed[1].payload = json!({"torn": true});
        assert!(validate_recovered_suffix(&original, Some(Cursor(1)), &changed).is_err());
    }

    #[test]
    fn durable_replay_may_omit_only_live_acceptance_and_exit_augmentation() {
        let accepted = NormalizedEvent {
            id: "accepted".to_owned(),
            cursor: 1,
            session_id: "session-recovery".to_owned(),
            actor: "root".to_owned(),
            event: EventVocab::TurnAccepted,
            payload: json!({}),
        };
        let mut terminal = recovery_event(2);
        terminal.event = EventVocab::TerminalSuccess;
        terminal.payload = json!({
            "state":"done",
            "terminal_kind":"success",
            "status":"success",
            "exit_code":0,
            "client_turn_wall_ms":42
        });
        let mut durable_terminal = terminal.clone();
        durable_terminal.payload = json!({"state":"done","terminal_kind":"success"});
        assert!(
            validate_recovered_suffix(&[accepted, terminal], None, &[durable_terminal]).is_ok()
        );
    }
}
