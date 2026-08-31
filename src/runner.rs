//! End-to-end benchmark orchestration.

use crate::cli::{Profile, RunOptions};
use crate::determinism::{
    CrossRunReproducibilityEvaluation, DeterminismRun, NondeterministicFieldEvaluation,
    NormalizationContext, evaluate_cross_run_reproducibility, evaluate_nondeterministic_fields,
};
use crate::driver::{
    Cursor, Driver, DriverOperations, GenericDriver, HttpTransport, ManagedDaemonConfig,
    ManagedDaemonTransport, PerInvocationConfig, PerInvocationDriver, SocketJsonRpcTransport,
    StdinRpcTransport, Transport,
};
use crate::evaluate::{
    Assertion, TestOutcome, TestResult, TestResultMetadata, automation_score, badge_label, certify,
    classify, suite_exit_code,
};
use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{
    FakeModelEngine, FakeModelMailboxServer, FakeModelServer, FakeModelUnixServer,
    is_transient_bind_error, monotonic_timestamp_ns,
};
use crate::manifest::{Manifest, TransportKind};
use crate::process::{ProcessSample, ProcessTree, Sample, Sampler};
use crate::report::{
    Fingerprint, MembershipSample, MemoryTimeIntegralEvaluation, MemoryTimeIntegralEvidence,
    MemoryTimeIntegralSample, ProcessHygieneAudit, ProcessHygieneCadenceSample,
    ProcessHygieneCheckpoint, ProcessHygieneEvaluation, ProcessHygieneEvidence,
    ProcessHygieneProcess, Report, ResourceSummary, TimeToFirstModelRequestEvaluation,
    TopologyMetric, TurnLatencyEvaluation, TurnObservation, evaluate_memory_time_integral,
    evaluate_process_hygiene, evaluate_time_to_first_model_request, evaluate_turn_latency,
    render_resource_summary, summarize_resources,
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
use crate::workflow::{Actor, Barrier, Fault, ScriptedResponse, WORKFLOW_SCHEMA_VERSION, Workflow};
use crate::{AhrbError, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type HarnessDriver = Box<dyn Driver>;

enum ModelServer {
    Tcp(FakeModelServer),
    Unix {
        server: FakeModelUnixServer,
        directory: PathBuf,
    },
    Mailbox(FakeModelMailboxServer),
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

struct DeterminismTrials {
    events: Vec<NormalizedEvent>,
    runs: Vec<DeterminismRun>,
    requests: Vec<crate::fake_model::ModelRequestRecord>,
}

struct DerivedRowEvaluations<'a> {
    model_request_efficiency: &'a crate::fake_model::ModelRequestEfficiencyEvaluation,
    turn_latency: &'a TurnLatencyEvaluation,
    process_hygiene: &'a ProcessHygieneEvaluation,
    time_to_first_model_request: &'a TimeToFirstModelRequestEvaluation,
    memory_time_integral: &'a MemoryTimeIntegralEvaluation,
    nondeterministic_fields: &'a NondeterministicFieldEvaluation,
    cross_run_reproducibility: &'a CrossRunReproducibilityEvaluation,
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
    summary.wall_per_turn_p50_ms = evaluation.wall_per_turn_p50_ms;
    summary.wall_per_turn_p95_ms = evaluation.wall_per_turn_p95_ms;
    summary.wall_per_turn_max_ms = evaluation.wall_per_turn_max_ms;
    summary.wall_per_turn_mad_ms = evaluation.wall_per_turn_mad_ms;
    summary.wall_per_turn_jitter_ratio = evaluation.wall_per_turn_jitter_ratio;
    summary.latency_class.clone_from(&evaluation.latency_class);
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
    let results: Vec<TestResult> = selected
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
    let mut raw_events = Vec::new();
    for events in progress.events.values() {
        for event in events {
            raw_events.push(serde_json::to_value(event)?);
        }
    }
    let automation = automation_score(&results);
    let details = BTreeMap::from([
        (
            "automation-score".to_owned(),
            json!({
                "topology": manifest.concurrency.topology.clone(),
                "comparison_scope": "within-topology-only",
                "score": automation.score,
            }),
        ),
        (
            "resource-summary".to_owned(),
            json!({"measurement_complete": false}),
        ),
    ]);
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
            | crate::matrix_evidence::CapabilityStatus::Absent(reason) = capability
            {
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(false),
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
        (20..=29).contains(row)
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
        )
        .await
        {
            Ok(collection) => {
                progress.update(|state| {
                    for row in selected_rows
                        .iter()
                        .copied()
                        .filter(|row| (20..=29).contains(row))
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
                    .filter(|row| (20..=29).contains(row))
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
                        .filter(|row| (20..=29).contains(row))
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
                    .filter(|row| (20..=29).contains(row))
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
        if (20..=29).contains(row) || matches!(*row, 42..=46 | 63 | 64) {
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
                    let process_cleared = !roots.is_empty()
                        && await_owned_tree_empty(
                            platform_sampler.as_mut(),
                            &roots,
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
                        "two resume attempts produced {} successful JSON control responses",
                        new_control
                            .iter()
                            .filter(|value| control_response_succeeded(value))
                            .count()
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
                        if !control_response_succeeded(response) {
                            return Err(AhrbError::Protocol(format!(
                                "native recovery probe reported failure: {response}"
                            )));
                        }
                        control_evidence.push(json!({
                            "session_id": session.0,
                            "action": "recover-probe",
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
    if let Some(trials) = &row45_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &row46_trials {
        request_records.extend(trials.requests.clone());
    }
    if let Some(trials) = &determinism_trials {
        request_records.extend(trials.requests.clone());
    }
    if row42_trials.is_some()
        || row43_trials.is_some()
        || row45_trials.is_some()
        || row46_trials.is_some()
        || determinism_trials.is_some()
    {
        request_records.sort_by(|left, right| {
            (
                &left.request.scenario,
                &left.request.actor,
                &left.request.checkpoint,
                left.semantic_ordinal,
                left.attempt,
            )
                .cmp(&(
                    &right.request.scenario,
                    &right.request.actor,
                    &right.request.checkpoint,
                    right.semantic_ordinal,
                    right.attempt,
                ))
        });
    }
    server.shutdown().await?;
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
    let row42_completed_turns = state
        .events
        .get(&42)
        .into_iter()
        .flatten()
        .filter(|event| is_terminal(&event.event))
        .count() as u64;
    let row42_expected_turns = match options.profile {
        Profile::Quick => 40,
        Profile::Cert => 200,
    };
    let row42_evaluation = crate::fake_model::evaluate_model_request_efficiency(
        &row42_records,
        row42_completed_turns,
        row42_expected_turns,
    );
    let row43_expected_turns = match options.profile {
        Profile::Quick => 100,
        Profile::Cert => 1_000,
    };
    let row43_observations = row43_trials
        .as_ref()
        .map_or(&[][..], |trials| trials.turns.as_slice());
    let row43_evaluation = evaluate_turn_latency(
        row43_observations,
        row43_expected_turns,
        per_invocation_topology(&manifest),
        manifest.resources.turn_timeout_ms,
    );
    if selected_rows.contains(&43) {
        apply_turn_latency_summary(&mut resource_summary, &row43_evaluation);
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
            model_request_efficiency: &row42_evaluation,
            turn_latency: &row43_evaluation,
            process_hygiene: &row44_evaluation,
            time_to_first_model_request: &row45_evaluation,
            memory_time_integral: &row46_evaluation,
            nondeterministic_fields: &row63_evaluation,
            cross_run_reproducibility: &row64_evaluation,
        },
    );
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
    if selected_rows.contains(&43) {
        resource_metric_values.extend(turn_latency_resource_metrics(&row43_evaluation));
    }
    if selected_rows.contains(&45) && row45_evaluation.measurement_complete {
        resource_metric_values.extend(time_to_first_model_request_resource_metrics(
            &row45_evaluation,
        ));
    }
    if selected_rows.contains(&46) && row46_evaluation.measurement_complete {
        resource_metric_values.extend(memory_time_integral_resource_metrics(&row46_evaluation));
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
    if selected_rows.contains(&42) {
        metrics.extend(row42_evaluation.metrics.clone());
    }
    if selected_rows.contains(&44) {
        metrics.extend(row44_evaluation.metrics.clone());
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
    let mut details = BTreeMap::new();
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
    let automation = automation_score(&results);
    let automation_provisional = automation.provisional;
    details.insert(
        "automation-score".to_owned(),
        json!({
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
    let badge = certify(
        &results,
        &manifest,
        std::env::consts::OS,
        state.parallel_agents,
        marginal_bytes,
        &resource_summary.latency_class,
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
    let mut model_requests = request_records
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if model_requests.is_empty() {
        for row_events in state.events.values() {
            for event in row_events {
                if event.event == EventVocab::ModelRequest {
                    if event.payload.get("request").is_some() {
                        model_requests.push(event.payload.clone());
                    } else {
                        model_requests.push(json!({
                            "event": event,
                            "model": event.payload.get("model").cloned().unwrap_or(Value::Null),
                            "endpoint": event.payload.get("endpoint").cloned().unwrap_or(Value::Null),
                            "credential_fingerprint": "embedded-redacted"
                        }));
                    }
                }
            }
        }
    }
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
    };
    crate::results::persist_report(&persistence, &report, options.junit, false)?;
    println!("{}", render_resource_summary(&report.resource_summary));
    match &report.badge {
        Some(badge) => println!("badge {}", badge_label(badge)),
        None => println!("badge none"),
    }
    if automation_provisional {
        println!("badge_note A provisional until rows 65-72 are measured");
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
        Err(AhrbError::Io(error))
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || is_transient_bind_error(&error) =>
        {
            let tcp_error = error.to_string();
            match start_unix_model(Arc::clone(&engine)).await {
                Ok(started) => Ok(started),
                Err(unix_error) => start_mailbox_model(engine).await.map_err(|mailbox_error| {
                    AhrbError::Protocol(format!(
                        "TCP fake-model bind failed after bounded retries ({tcp_error}); Unix-socket fallback failed ({unix_error}); provider mailbox fallback failed: {mailbox_error}"
                    ))
                }),
            }
        }
        Err(error) => Err(error),
    }
}

async fn start_mailbox_model(
    engine: Arc<FakeModelEngine>,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    #[cfg(target_os = "macos")]
    let root = PathBuf::from("/private/tmp");
    #[cfg(not(target_os = "macos"))]
    let root = PathBuf::from("/tmp");
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = root.join(format!("ahrb-fmb-{}-{sequence}", std::process::id()));
    let server = FakeModelMailboxServer::bind(directory.clone(), engine).await?;
    let environment = BTreeMap::from([(
        "AHRB_MOCK_PROVIDER_MAILBOX".to_owned(),
        directory.to_string_lossy().into_owned(),
    )]);
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
    let timeout = Duration::from_millis(manifest.transport.timeout_ms);
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
        let count = if *row == 26 { 8 } else { 1 };
        let mut row_actors = Vec::new();
        for index in 0..count {
            let actor = if count == 1 {
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
    if rows.iter().any(|row| (20..=29).contains(row)) {
        add_resource_workflow(
            scenario,
            profile_root,
            ResourceTimingPlan::for_profile(ResourceProfile::from(profile)),
            manifest,
            &mut actors,
            &mut responses,
        )?;
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
    let wall_started = Instant::now();
    let cpu_started = sampler_thread_cpu_ns()?;
    let tree = sampler.discover(roots)?;
    let sample = sampler.sample(&tree, phase)?;
    let collection_cpu_ns = sampler_thread_cpu_ns()?.saturating_sub(cpu_started);
    let collection_wall_ns = duration_ns(wall_started.elapsed());
    let mut warnings = Vec::new();
    for warning in sample.cpu_accounting_warnings {
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
    let started = Instant::now();
    let stop = Arc::new((Mutex::new(false), Condvar::new()));
    let thread_stop = Arc::clone(&stop);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let join = std::thread::Builder::new()
        .name("ahrb-row44-sampler".to_owned())
        .spawn(move || {
            prioritize_counter_thread();
            let mut samples = Vec::new();
            let mut warnings = Vec::new();
            let first = sample_process_hygiene(
                sampler.as_mut(),
                &roots,
                &phase,
                duration_ns(started.elapsed()),
            );
            let ready_result = first.as_ref().map(|_| ()).map_err(ToString::to_string);
            let _ = ready_tx.send(ready_result);
            let (sample, first_warnings) = first?;
            samples.push(sample);
            warnings.extend(first_warnings);
            let mut deadline = Instant::now() + sample_interval;
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
                let mut elapsed_ns = duration_ns(started.elapsed());
                while elapsed_ns <= previous_elapsed {
                    std::thread::yield_now();
                    elapsed_ns = duration_ns(started.elapsed());
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
        Ok(Ok(())) => Ok(ProcessHygieneTurnSampler {
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
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            collect_process_hygiene && per_invocation,
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
                Duration::from_millis(manifest.resources.turn_timeout_ms),
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
            evidence.warm_baselines.push(ProcessHygieneCheckpoint {
                repetition,
                turn_index: 0,
                processes,
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            });
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
                Duration::from_millis(manifest.resources.turn_timeout_ms),
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
                && per_invocation
            {
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
                    processes,
                });
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
            &left.request.checkpoint,
            left.semantic_ordinal,
            left.attempt,
        )
            .cmp(&(
                &right.request.scenario,
                &right.request.actor,
                &right.request.checkpoint,
                right.semantic_ordinal,
                right.attempt,
            ))
    });
    Ok(ModelRequestEfficiencyTrials {
        events: all_events,
        requests: all_requests,
        process_hygiene,
    })
}

fn turn_latency_workflow(profile_root: &Path, turns: u32) -> Workflow {
    let scenario = "ahrb-row43".to_owned();
    let actor = "r43-latency".to_owned();
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
    let profile_root = run_profile_root.join("derived-row43");
    prepare_profile(manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare row-43 fresh profile: {error}")))?;
    let workflow = turn_latency_workflow(&profile_root, turns);
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
    let credential = format!("ahrb-{}-row43-{}", &manifest_hash[..16], std::process::id());
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
    let mut driver = make_driver(
        manifest,
        &command,
        &environment,
        &variables,
        &profile_root,
        false,
    )?;
    driver.start().await?;
    let session = driver
        .create_session(&format!("{}:row43", workflow.scenario))
        .await?;
    let session_id_hash = stable_evidence_hash(&session.0);
    let actor = "r43-latency";
    let mut events = Vec::new();
    let mut observations = Vec::with_capacity(turns as usize);
    let warmup_prompt = workflow
        .actors
        .get(actor)
        .map(|actor| actor.prompt.as_str())
        .ok_or_else(|| AhrbError::Protocol("row-43 actor disappeared".to_owned()))?;
    let warmup_boundary_count = driver.completed_turn_boundaries().len();
    driver
        .submit(&session, warmup_prompt, "row-43-warmup")
        .await?;
    let warmup_events = collect_session_terminal(
        &mut driver,
        &session,
        None,
        Duration::from_millis(manifest.resources.turn_timeout_ms),
    )
    .await?;
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
            Duration::from_millis(manifest.resources.turn_timeout_ms),
        )
        .await?;
    }
    for turn in 1..=turns {
        let prompt = format!(
            "AHRB turn latency direct terminal turn {turn} {}",
            route_marker(&workflow.scenario, actor, &format!("turn-{turn:04}"))
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
            Duration::from_millis(manifest.resources.turn_timeout_ms),
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
                Duration::from_millis(manifest.resources.turn_timeout_ms),
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
            repetition: 1,
            turn_index: turn,
            actor: actor.to_owned(),
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
    if manifest.transport.kind == TransportKind::Exec || !manifest.sessions.close_delete.is_empty()
    {
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
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
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
            Duration::from_millis(manifest.resources.turn_timeout_ms),
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
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
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
            &left.request.checkpoint,
            left.semantic_ordinal,
            left.attempt,
        )
            .cmp(&(
                &right.request.scenario,
                &right.request.actor,
                &right.request.checkpoint,
                right.semantic_ordinal,
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
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            false,
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
            let events = collect_session_terminal(
                &mut driver,
                &session,
                None,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
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
        let primary_checkpoints = records
            .iter()
            .filter(|record| record.role == "primary")
            .map(|record| {
                (
                    record.request.actor.as_str(),
                    record.request.checkpoint.as_str(),
                    record.semantic_ordinal,
                    record.attempt,
                )
            })
            .collect::<Vec<_>>();
        let expected_checkpoints = vec![
            ("d63-direct", "start", 1_u64, 1_u64),
            ("d63-tool", "start", 1_u64, 1_u64),
            ("d63-tool", "terminal", 2_u64, 1_u64),
        ];
        if primary_checkpoints != expected_checkpoints {
            return Err(AhrbError::Protocol(format!(
                "row-63 execution {execution} primary request sequence was {primary_checkpoints:?}, expected {expected_checkpoints:?}"
            )));
        }
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
            &left.request.checkpoint,
            left.semantic_ordinal,
            left.attempt,
        )
            .cmp(&(
                &right.request.scenario,
                &right.request.actor,
                &right.request.checkpoint,
                right.semantic_ordinal,
                right.attempt,
            ))
    });
    Ok(DeterminismTrials {
        events: all_events,
        runs,
        requests: all_requests,
    })
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
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &profile_root,
            per_invocation,
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
                .wait_for_sample_after(
                    sampling_boundary,
                    true,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?;
            driver.release_invocations().await?;
        }
        let warmup_events = collect_session_terminal(
            &mut driver,
            &session,
            None,
            Duration::from_millis(manifest.resources.turn_timeout_ms),
        )
        .await?;
        let mut after = warmup_events.iter().map(|event| Cursor(event.cursor)).max();
        if per_invocation {
            let boundary = await_completed_turn_boundary(
                &mut driver,
                &session,
                after,
                warmup_boundary_count,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
            )
            .await?;
            sampler.set_roots(&[])?;
            sampler
                .wait_for_sample_after(
                    boundary.exit_ns,
                    false,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
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
                .wait_for_sample_after(
                    baseline_end_ns,
                    true,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
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
                    .wait_for_sample_after(
                        sampling_boundary,
                        true,
                        Duration::from_millis(manifest.resources.turn_timeout_ms),
                    )
                    .await?;
                driver.release_invocations().await?;
            }
            let suffix = collect_session_terminal_with_poll(
                &mut driver,
                &session,
                after,
                Duration::from_millis(manifest.resources.turn_timeout_ms),
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
                        Duration::from_millis(manifest.resources.turn_timeout_ms),
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
                .wait_for_sample_after(
                    sample_after_ns,
                    false,
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
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
            &left.request.checkpoint,
            left.semantic_ordinal,
            left.attempt,
        )
            .cmp(&(
                &right.request.scenario,
                &right.request.actor,
                &right.request.checkpoint,
                right.semantic_ordinal,
                right.attempt,
            ))
    });
    Ok(MemoryTimeIntegralTrials {
        events: all_events,
        requests: all_requests,
        evidence,
    })
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
        if let Some(value) = value.as_str() {
            variables.insert(key.clone(), value.to_owned());
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
        let events = driver.attach(session, after).await?;
        if events.iter().any(|event| is_terminal(&event.event)) {
            return Ok(events);
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "exec session {} did not terminalize",
                session.0
            )));
        }
        tokio::time::sleep(poll_interval).await;
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

fn evaluate_rows(
    selected: &[&crate::scenarios::TestDefinition],
    state: &RunState,
    requests: &[crate::fake_model::ModelRequestRecord],
    manifest: &Manifest,
    resources: &ResourceCertification,
    profile_root: &Path,
    derived: &DerivedRowEvaluations<'_>,
) -> Vec<TestResult> {
    let row42 = derived.model_request_efficiency;
    let row43 = derived.turn_latency;
    let row44 = derived.process_hygiene;
    let row45 = derived.time_to_first_model_request;
    let row46 = derived.memory_time_integral;
    let row63 = derived.nondeterministic_fields;
    let row64 = derived.cross_run_reproducibility;
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
            let capability =
                crate::matrix_evidence::capability_for_row(manifest, definition.row);
            if !matches!(
                capability,
                crate::matrix_evidence::CapabilityStatus::Supported
            ) {
                let reason = match capability {
                    crate::matrix_evidence::CapabilityStatus::Unsupported(reason)
                    | crate::matrix_evidence::CapabilityStatus::Absent(reason) => reason,
                    crate::matrix_evidence::CapabilityStatus::Supported => String::new(),
                };
                let mut result = classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(false),
                    &[],
                    None,
                );
                result.evidence.push(format!("capability: {reason}"));
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
                        ["content", "stdout", "aggregated_output"]
                            .iter()
                            .any(|field| {
                                event
                                    .payload
                                    .pointer(&format!("/result/{field}"))
                                    .and_then(Value::as_str)
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
) -> Result<PerInvocationResourceCollection> {
    let timing = ResourceTimingPlan::for_profile(ResourceProfile::from(profile));
    let mut observations = Vec::new();
    let mut collector = ResourceCollector::new(&timing);
    let mut turn_wall_ns = Vec::new();

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
        turn_wall_ns.extend(driver.completed_turn_wall_ns());
        driver.shutdown().await?;
    }

    let membership = membership_report_samples(&collector.membership_refreshes_by_phase);
    Ok(PerInvocationResourceCollection {
        observations,
        samples: collector.series.samples,
        membership,
        turn_wall_ns,
    })
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
    for turn in 1..=timing.long_horizon_turns {
        let checkpoint = resource_long_checkpoint(identity.repetition, turn);
        let prompt = format!(
            "AHRB long horizon turn {turn} {}",
            route_marker(&workflow.scenario, &actor_name, &checkpoint)
        );
        let turn_key = format!("resource-long-r{}-turn-{turn}", identity.repetition);
        // Move the existing external deadline clock to the submit boundary so
        // it covers submit plus daemon handling without adding another timer.
        let turn_started = Instant::now();
        driver.submit(&session, &prompt, &turn_key).await?;
        let (terminal_cursor, tool_results, turn_wall) = wait_one_terminal(
            driver,
            &session,
            after,
            &turn_key,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
            turn_started,
        )
        .await?;
        turn_wall_ns.push(duration_ns(turn_wall));
        after = Some(terminal_cursor);
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
    collector
        .sample_phase(
            roots,
            &final_post_close_phase,
            Duration::from_millis(
                timing
                    .barrier_discard_ms
                    .saturating_add(timing.barrier_steady_ms),
            ),
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
) -> Result<(crate::driver::Cursor, Vec<LongHorizonToolResult>, Duration)> {
    let mut observed_tool_results = BTreeMap::new();
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
            let name = event
                .payload
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AhrbError::Protocol("long-horizon tool result omitted name".to_owned())
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
                started.elapsed(),
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
    let started = Instant::now();
    loop {
        if sampler.discover(roots)?.members.is_empty() {
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
        return Err(format!(
            "journal replay length mismatch: expected {}, recovered {}",
            expected.len(),
            recovered.len()
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
            if matches!(
                event.event,
                EventVocab::TerminalSuccess
                    | EventVocab::TerminalFailure
                    | EventVocab::TerminalCancelled
                    | EventVocab::TerminalTimeout
            ) && let Some(payload) = event.payload.as_object_mut()
            {
                // These fields are added by the observing client at exit and
                // are intentionally absent from the daemon-owned journal.
                for key in [
                    "client_turn_wall_ms",
                    "exit_code",
                    "status",
                    "category",
                    "failure_marker",
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
        std::thread::sleep(Duration::from_millis(12));
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
            processes: residue,
        });

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
