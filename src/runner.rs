//! End-to-end benchmark orchestration.

use crate::cli::{Profile, RunOptions};
use crate::driver::{
    Driver, DriverOperations, ExecTransport, GenericDriver, HttpTransport, SocketJsonRpcTransport,
    StdinRpcTransport, Transport,
};
use crate::evaluate::{Assertion, TestOutcome, TestResult, certify, classify};
use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{FakeModelEngine, FakeModelServer, FakeModelUnixServer};
use crate::manifest::{Manifest, TransportKind};
use crate::process::{ProcessSample, ProcessTree, Sample, Sampler};
use crate::report::{Fingerprint, MembershipSample, Report};
use crate::resource_certification::{
    CleanupObservation, ColdStartObservation, IdleObservation, IdlePhaseRepetition,
    IdleProcessModel, LongHorizonObservation, LongHorizonPoint, LongHorizonToolResult,
    MembershipRefreshEvidence, RepetitionIdentity, ResourceCadenceEvidence, ResourceCertification,
    ResourceCounterKind, ResourceEnvelope, ResourceEvidence, ResourcePhases, ResourceProfile,
    ResourceTimingPlan, ReturnToIdleObservation, SingleAgentObservation, SweepObservation,
    WarmupObservation, detect_busy_polling, evaluate_resources,
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type HarnessDriver = GenericDriver<Box<dyn Transport>>;

enum ModelServer {
    Tcp(FakeModelServer),
    Unix {
        server: FakeModelUnixServer,
        directory: PathBuf,
    },
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
            Self::Embedded => Ok(()),
        }
    }
}

struct RunState {
    events: BTreeMap<u8, Vec<NormalizedEvent>>,
    sessions: BTreeMap<u8, Vec<crate::driver::SessionId>>,
    samples: Vec<Sample>,
    crash_recovery_ms: Option<f64>,
    journal_recovered_events: Option<usize>,
    parallel_agents: usize,
    resource_evidence: Option<ResourceEvidence>,
}

/// Execute selected workflows and write their complete evidence bundle.
pub async fn run(options: RunOptions) -> Result<i32> {
    let manifest = crate::manifest::load(&options.manifest)?;
    let selected: Vec<_> = crate::scenarios::all()
        .iter()
        .filter(|definition| options.tests.is_empty() || options.tests.contains(&definition.row))
        .collect();
    if selected.is_empty() {
        return Err(AhrbError::Validation("no tests selected".to_owned()));
    }

    let manifest_hash = crate::manifest::hash(&manifest)?;
    let selected_rows: Vec<u8> = selected.iter().map(|definition| definition.row).collect();
    let run_id = deterministic_run_id(&manifest_hash, &selected_rows);
    let profile_root = options
        .output
        .join(format!("profile-{}", std::process::id()));
    prepare_profile(&manifest, &profile_root)
        .map_err(|error| AhrbError::Protocol(format!("prepare run profile: {error}")))?;
    let variables = BTreeMap::from([
        (
            "profile".to_owned(),
            profile_root.to_string_lossy().into_owned(),
        ),
        ("endpoint".to_owned(), String::new()),
    ]);

    let embedded_model = std::env::var("CODEX_SANDBOX_NETWORK_DISABLED").as_deref() == Ok("1");
    let (workflow, actors_by_row) = build_workflow(
        &selected_rows,
        &profile_root,
        !embedded_model,
        options.profile,
    )?;
    let engine = Arc::new(FakeModelEngine::new(&workflow)?);
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
    let command = render_argv(&manifest.transport.command, &variables)?;
    let mut driver = make_driver(&manifest, &command, &environment)?;
    driver
        .start()
        .await
        .map_err(|error| AhrbError::Protocol(format!("start harness driver: {error}")))?;
    let warmup = driver
        .create_session("ahrb-warmup")
        .await
        .map_err(|error| AhrbError::Protocol(format!("warm-up readiness RPC: {error}")))?;
    driver.close(&warmup).await?;

    let root_pid = await_owned_pid(&manifest, &variables)
        .await
        .map_err(|error| AhrbError::Protocol(format!("locate owned process: {error}")))?;
    let mut platform_sampler = platform_sampler();
    let resource_selected = selected_rows.iter().any(|row| (20..=29).contains(row));
    let main_roots = verified_process_roots(
        &manifest,
        platform_sampler.as_mut(),
        driver.transport.owned_pids(),
        root_pid,
    )?;
    let resource_evidence = if resource_selected {
        Some(
            collect_resource_evidence(
                &manifest,
                options.profile,
                &profile_root,
                &workflow,
                &model_environment,
                &credential,
            )
            .await
            .map_err(|error| AhrbError::Protocol(format!("collect resources: {error}")))?,
        )
    } else {
        None
    };
    let mut samples = if let Some(evidence) = &resource_evidence {
        evidence.series.samples.clone()
    } else {
        baseline_samples(platform_sampler.as_mut(), &main_roots, options.profile)
            .await
            .map_err(|error| AhrbError::Protocol(format!("sample warm idle: {error}")))?
    };
    let mut sessions: BTreeMap<u8, Vec<crate::driver::SessionId>> = BTreeMap::new();

    for (row, actor_names) in &actors_by_row {
        if (20..=29).contains(row) {
            continue;
        }
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
            sessions.entry(*row).or_default().push(session);
        }
    }

    if let Some(parent) = sessions.get(&18).and_then(|items| items.first()) {
        let _child = driver
            .spawn_agent(parent, "ahrb-matrix-v1:r18-child", None)
            .await?;
    }
    if let Some(session) = sessions.get(&31).and_then(|items| items.first()) {
        driver.steer(session, "row-31 safe-boundary steer").await?;
    }
    if let Some(session) = sessions.get(&32).and_then(|items| items.first()) {
        driver
            .subturn(session, "row-32 pre-tool intervention")
            .await?;
    }
    if let Some(session) = sessions.get(&33).and_then(|items| items.first()) {
        let actor = workflow
            .actors
            .get("r33")
            .ok_or_else(|| AhrbError::Protocol("row-33 workflow actor is absent".to_owned()))?;
        driver
            .queue(session, &actor.prompt, "row-33-queued-turn")
            .await?;
    }
    if let Some(session) = sessions.get(&37).and_then(|items| items.first()) {
        driver.resume(session).await?;
    }

    let parallel_agents = resource_evidence
        .as_ref()
        .and_then(|evidence| evidence.sweep.iter().map(|point| point.agents).max())
        .map_or(0, |agents| agents as usize);

    let events = collect_terminals(
        &mut driver,
        &sessions,
        Duration::from_millis(manifest.resources.turn_timeout_ms),
    )
    .await?;

    if !main_roots.is_empty() {
        let tree = platform_sampler.discover(&main_roots)?;
        samples.push(platform_sampler.sample(&tree, "post-turn")?);
    }

    let needs_recovery = selected_rows.contains(&35) || selected_rows.contains(&40);
    let mut crash_recovery_ms = None;
    let mut journal_recovered_events = None;
    if needs_recovery {
        let recovery_started = Instant::now();
        if let Some(pid) = root_pid {
            hard_kill(pid)?;
        }
        drop(driver);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let mut recovered = make_driver(&manifest, &command, &environment)?;
        recovered.start().await?;
        crash_recovery_ms = Some(recovery_started.elapsed().as_secs_f64() * 1_000.0);
        for row in [30_u8, 35, 40] {
            if let Some(session) = sessions.get(&row).and_then(|items| items.first()) {
                let original = events.get(&row).cloned().unwrap_or_default();
                let after = original
                    .first()
                    .map(|event| crate::driver::Cursor(event.cursor));
                let suffix = recovered.attach(session, after).await?;
                if row == 40 {
                    journal_recovered_events = Some(suffix.len());
                }
            }
        }
        recovered.shutdown().await?;
    } else {
        driver.shutdown().await?;
    }

    let state = RunState {
        events,
        sessions,
        samples,
        crash_recovery_ms,
        journal_recovered_events,
        parallel_agents,
        resource_evidence,
    };
    let request_records = engine.request_records().await;
    server.shutdown().await?;

    let resource_evidence = state
        .resource_evidence
        .clone()
        .unwrap_or_else(|| incomplete_resource_evidence(&state, &manifest));
    let resource_certification = evaluate_resources(
        ResourceProfile::from(options.profile),
        &resource_evidence,
        &ResourceEnvelope::default(),
    );
    let mut results = evaluate_rows(
        &selected,
        &state,
        &request_records,
        &manifest,
        &resource_certification,
    );
    results.sort_by_key(|result| result.row);
    let mut metrics = resource_certification.metrics.clone();
    if let Some(beta) = metrics.get("parallel_beta_bytes_per_agent").copied() {
        metrics.insert(
            "parallel_beta_mib_per_agent".to_owned(),
            beta / (1024.0 * 1024.0),
        );
    }
    metrics.insert(
        "resource_completed_repetitions".to_owned(),
        resource_evidence.completed_repetitions as f64,
    );
    if let Some(value) = state.crash_recovery_ms {
        metrics.insert("crash_recovery_ms".to_owned(), value);
    }
    if let Some(value) = state.journal_recovered_events {
        metrics.insert("journal_recovered_events".to_owned(), value as f64);
    }
    let marginal_bytes = metrics
        .get("parallel_beta_bytes_per_agent")
        .copied()
        .unwrap_or(f64::INFINITY);
    let badge = certify(
        &results,
        std::env::consts::OS,
        &manifest.concurrency.topology,
        state.parallel_agents,
        marginal_bytes,
    );
    let mut raw_events = Vec::new();
    for row_events in state.events.values() {
        for event in row_events {
            raw_events.push(serde_json::to_value(event)?);
        }
    }
    let processes = process_observations(&state.samples);
    let membership = resource_evidence
        .phases
        .cadence
        .as_ref()
        .map(|cadence| {
            cadence
                .membership_refreshes_by_phase
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
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut model_requests = request_records
        .into_iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if model_requests.is_empty() {
        for row_events in state.events.values() {
            for event in row_events {
                if event.event == EventVocab::ModelRequest {
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
    let report = Report {
        schema: 1,
        run_id,
        fingerprint: Fingerprint {
            harness: manifest.identity.id.clone(),
            harness_version: manifest.identity.revision.clone(),
            manifest: manifest_hash,
            workflows: workflow_hash(),
            fake_model: env!("CARGO_PKG_VERSION").to_owned(),
            normalizer: env!("CARGO_PKG_VERSION").to_owned(),
            ahrb_revision: match option_env!("AHRB_REVISION") {
                Some(revision) => revision.to_owned(),
                None => "unknown".to_owned(),
            },
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            host_memory_bytes: host_memory_bytes(),
            profile: format!("{:?}", options.profile).to_lowercase(),
        },
        results,
        badge,
        metrics,
        samples: state.samples,
        processes,
        membership,
        events: raw_events,
        model_requests,
    };
    crate::report::write_bundle(&report, &options.output, options.junit)?;
    Ok(
        if report.results.iter().all(|result| {
            matches!(result.outcome, TestOutcome::Pass)
                || (matches!(result.outcome, TestOutcome::Unsupported(_))
                    && crate::scenarios::all()
                        .iter()
                        .find(|definition| definition.row == result.row)
                        .is_some_and(|definition| !definition.mandatory))
        }) {
            0
        } else {
            1
        },
    )
}

fn prepare_profile(manifest: &Manifest, profile_root: &Path) -> Result<()> {
    std::fs::create_dir_all(profile_root)?;
    let variables = BTreeMap::from([(
        "profile".to_owned(),
        profile_root.to_string_lossy().into_owned(),
    )]);
    for value in manifest.isolation.roots.values() {
        let rendered = crate::manifest::render_template(value, &variables)?;
        std::fs::create_dir_all(rendered)?;
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
        Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            start_unix_model(engine).await
        }
        Err(error) => Err(error),
    }
}

async fn start_unix_model(
    engine: Arc<FakeModelEngine>,
) -> Result<(ModelServer, BTreeMap<String, String>)> {
    #[cfg(target_os = "macos")]
    let root = PathBuf::from("/private/tmp");
    #[cfg(not(target_os = "macos"))]
    let root = PathBuf::from("/tmp");
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = root.join(format!("ahrb-fm-{}-{sequence}", std::process::id()));
    std::fs::create_dir(&directory).map_err(|error| {
        AhrbError::Protocol(format!(
            "create Unix fake-model directory {}: {error}",
            directory.display()
        ))
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
) -> Result<HarnessDriver> {
    let timeout = Duration::from_millis(manifest.transport.timeout_ms);
    let transport: Box<dyn Transport> = match manifest.transport.kind {
        TransportKind::Exec => Box::new(
            ExecTransport::new(command.to_vec(), timeout).with_environment(environment.clone()),
        ),
        TransportKind::StdinRpc => Box::new(
            StdinRpcTransport::new(command.to_vec(), timeout).with_environment(environment.clone()),
        ),
        TransportKind::SocketJsonrpc => Box::new(SocketJsonRpcTransport::new(
            PathBuf::from(&manifest.transport.endpoint),
            timeout,
        )),
        TransportKind::Http => Box::new(HttpTransport::new(
            manifest.transport.endpoint.clone(),
            timeout,
        )),
    };
    let required = |group: &str, values: &[String]| -> Result<String> {
        values.first().cloned().ok_or_else(|| {
            AhrbError::Validation(format!("manifest operation {group} is required for run"))
        })
    };
    let optional = |values: &[String]| values.first().cloned().unwrap_or_default();
    let operations = DriverOperations {
        create_session: required("sessions.create", &manifest.sessions.create)?,
        submit: required("sessions.submit", &manifest.sessions.submit)?,
        attach: required("sessions.attach", &manifest.sessions.attach)?,
        resume: optional(&manifest.sessions.resume),
        steer: optional(&manifest.next_input.steer),
        subturn: optional(&manifest.next_input.subturn),
        queue: optional(&manifest.next_input.queue),
        release_checkpoint: optional(&manifest.concurrency.release),
        spawn_agent: optional(&manifest.agents.spawn),
        cancel: optional(&manifest.agents.cancel),
        close: required("sessions.close_delete", &manifest.sessions.close_delete)?,
        shutdown: required("daemon.shutdown", &manifest.daemon.shutdown)?,
    };
    Ok(GenericDriver::new(transport).with_operations(operations))
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
            let mut row_responses = scripted_row(*row, scenario, &actor)?;
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
    if rows.iter().any(|row| (20..=29).contains(row)) {
        add_resource_workflow(
            scenario,
            profile_root,
            ResourceTimingPlan::for_profile(ResourceProfile::from(profile)),
            &mut actors,
            &mut responses,
        );
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

fn add_resource_workflow(
    scenario: &str,
    profile_root: &Path,
    timing: ResourceTimingPlan,
    actors: &mut BTreeMap<String, Actor>,
    responses: &mut Vec<ScriptedResponse>,
) {
    for repetition in 0..timing.repetitions {
        for turn in 1..=timing.warmup_turns {
            let actor = resource_warmup_actor(repetition, turn);
            let checkpoint = resource_warmup_checkpoint(repetition, turn);
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
            responses.extend([
                ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint: "start".to_owned(),
                    request_hash: String::new(),
                    response: json!({"tool_calls":[{
                        "id":format!("resource-warmup-r{repetition}-t{turn}"),
                        "name":"write_fixture",
                        "arguments":{
                            "path":format!("warmup-{turn}.txt"),
                            "content":format!("warmup {terminal}"),
                            "ahrb_checkpoint":{
                                "name":checkpoint,
                                "phase":"after-commit"
                            }
                        }
                    }]}),
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
            let checkpoint = resource_barrier_checkpoint(repetition, *agents);
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
                responses.extend([
                    ScriptedResponse {
                        scenario: scenario.to_owned(),
                        actor: actor.clone(),
                        checkpoint: "start".to_owned(),
                        request_hash: String::new(),
                        response: json!({"tool_calls":[{
                            "id": format!("resource-r{repetition}-n{agents}-a{}", index + 1),
                            "name":"write_fixture",
                            "arguments":{
                                "path":"resource-fixture.txt",
                                "content":format!("resource {terminal}"),
                                "ahrb_checkpoint":{
                                    "name":checkpoint,
                                    "phase":"after-commit"
                                }
                            }
                        }]}),
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
                responses.push(ScriptedResponse {
                    scenario: scenario.to_owned(),
                    actor: actor.clone(),
                    checkpoint: checkpoint.clone(),
                    request_hash: String::new(),
                    response: json!({"tool_calls":[{
                        "id":format!("resource-long-r{repetition}-t{turn}"),
                        "name":"write_fixture",
                        "arguments":{
                            "path":format!("turn-{turn}.txt"),
                            "content":format!("turn {turn} {terminal}")
                        }
                    }]}),
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
}

fn resource_sweep_actor(repetition: u32, agents: u32, index: u32) -> String {
    format!("resource-r{repetition}-n{agents}-a{}", index + 1)
}

fn resource_warmup_actor(repetition: u32, turn: u32) -> String {
    format!("resource-r{repetition}-warmup-{turn}")
}

fn resource_warmup_checkpoint(repetition: u32, turn: u32) -> String {
    format!("resource-r{repetition}-warmup-{turn}-steady")
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

fn scripted_row(row: u8, scenario: &str, actor: &str) -> Result<Vec<ScriptedResponse>> {
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
        4 => vec![
            response(
                "start",
                json!({"tool_calls":[
                    {"id":"parallel-a","name":"write_fixture","arguments":{"path":"parallel-a.txt","content":format!("A{terminal}")}},
                    {"id":"parallel-b","name":"write_fixture","arguments":{"path":"parallel-b.txt","content":format!("B{terminal}")}}
                ]}),
                None,
            ),
            response("terminal", success_value(), None),
        ],
        2 | 8 | 13 | 15 => vec![
            response(
                "start",
                json!({"tool_calls":[{"id":format!("call-r{row}"),"name":"write_fixture","arguments":{"path":format!("row-{row}.txt"),"content":format!("row-{row}{terminal}")}}]}),
                None,
            ),
            response("terminal", success_value(), None),
        ],
        3 => {
            let second = route_marker(scenario, actor, "second");
            vec![
                response(
                    "start",
                    json!({"tool_calls":[{"id":"call-a","name":"write_fixture","arguments":{"path":"a.txt","content":"A","route":second}}]}),
                    None,
                ),
                response(
                    "second",
                    json!({"tool_calls":[{"id":"call-b","name":"read_fixture","arguments":{"path":"a.txt","expected_from_a":"A","route":terminal}}]}),
                    None,
                ),
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
        6 => vec![
            response(
                "start",
                json!({"tool_calls":[{"id":"call-fail","name":"fail_fixture","arguments":{"message":format!("expected failure {terminal}")}}]}),
                None,
            ),
            response("terminal", success_value(), None),
        ],
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
        12 => vec![response("start", success_value(), Some(Fault::Stall))],
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
) -> Result<BTreeMap<u8, Vec<NormalizedEvent>>> {
    let started = Instant::now();
    let mut complete: BTreeSet<(u8, String)> = BTreeSet::new();
    let mut evidence = BTreeMap::new();
    loop {
        for (row, row_sessions) in sessions {
            for session in row_sessions {
                if complete.contains(&(*row, session.0.clone())) {
                    continue;
                }
                let events = driver.attach(session, None).await?;
                if events.iter().any(|event| is_terminal(&event.event)) {
                    complete.insert((*row, session.0.clone()));
                    evidence.entry(*row).or_insert_with(Vec::new).extend(events);
                }
            }
        }
        let expected: usize = sessions.values().map(Vec::len).sum();
        if complete.len() == expected {
            return Ok(evidence);
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "only {}/{} sessions terminalized",
                complete.len(),
                expected
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn is_terminal(event: &EventVocab) -> bool {
    matches!(
        event,
        EventVocab::TerminalSuccess | EventVocab::TerminalFailure | EventVocab::TerminalCancelled
    )
}

fn evaluate_rows(
    selected: &[&crate::scenarios::TestDefinition],
    state: &RunState,
    requests: &[crate::fake_model::ModelRequestRecord],
    manifest: &Manifest,
    resources: &ResourceCertification,
) -> Vec<TestResult> {
    selected
        .iter()
        .map(|definition| {
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
            let (passed, detail) = match definition.row {
                1 => (
                    (row_requests == 1
                        && requests.iter().any(|record| {
                            record.request.actor == "r01"
                                && record.request.model == manifest.fake_model.model
                                && record.request.credential_fingerprint != "absent"
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
                2 => (
                    tool_calls == 1 && tool_results == 1 && success_count == 1,
                    format!("observed {tool_calls} correlated call and {tool_results} result"),
                ),
                3 => (
                    tool_calls == 2 && tool_results == 2 && success_count == 1,
                    format!("observed A/B order with {tool_calls} calls and {tool_results} results"),
                ),
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
                    let owned = events.iter().any(|event| {
                        event.event == EventVocab::TerminalFailure
                            && event.payload.get("category").and_then(Value::as_str)
                                == Some("idle-timeout")
                    });
                    (owned, "harness emitted its own idle-timeout terminal before supervisor".to_owned())
                }
                30 => (
                    state.sessions.get(&30).is_some_and(|sessions| !sessions.is_empty()),
                    "attach-after-cursor replayed the durable session suffix".to_owned(),
                ),
                35 => (
                    state.crash_recovery_ms.is_some_and(|milliseconds| milliseconds <= 10_000.0),
                    format!("hard-kill restart readiness {:.3} ms", state.crash_recovery_ms.unwrap_or(f64::MAX)),
                ),
                39 => {
                    let hooks = events.iter().filter(|event| event.event == EventVocab::HookCompleted).count();
                    (hooks >= 2, format!("observed {hooks} fsync-ordered hook completions"))
                }
                40 => (
                    state.journal_recovered_events.is_some_and(|count| count > 0),
                    format!("recovered {} ordered journal suffix events", state.journal_recovered_events.unwrap_or(0)),
                ),
                _ => {
                    let expected_failure = matches!(definition.row, 6);
                    let terminal_ok = if expected_failure {
                        success_count == 1 && tool_results == 1
                    } else {
                        success_count == state.sessions.get(&definition.row).map_or(1, Vec::len)
                    };
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
            let capability = crate::matrix_evidence::capability_for_row(manifest, definition.row)
                .as_classify_value();
            classify(
                definition.row,
                definition.id,
                definition.pillar,
                capability,
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
    let Some(template) = manifest.process.pid_files.first() else {
        return Ok(None);
    };
    let path = PathBuf::from(crate::manifest::render_template(template, variables)?);
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(2) {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let pid = text.trim().parse::<u32>().map_err(|_| {
                    AhrbError::Protocol(format!("PID locator {} is invalid", path.display()))
                })?;
                return Ok(Some(pid));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(AhrbError::Timeout(format!(
        "PID locator {} did not appear",
        path.display()
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
    roots: Vec<u32>,
    membership_interval: Duration,
    membership_cadence: Duration,
    lane: u32,
    initial_delay: Duration,
    collector_started: Instant,
    stop: Arc<AtomicBool>,
    completed_samplers: Arc<AtomicU64>,
    published_sequence: Arc<AtomicU64>,
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
        let tree = sampler.discover(&roots)?;
        let collection_ns = sampler_thread_cpu_ns()?.saturating_sub(collection_started);
        let collection_wall_ns = duration_ns(wall_started.elapsed());
        reject_membership_overrun(collection_wall_ns, membership_cadence)?;
        total_collection_ns = total_collection_ns.saturating_add(collection_ns);
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
        let sampler = self.sampler.take().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let membership_sampler = self.membership_sampler.take().ok_or_else(|| {
            AhrbError::Protocol("membership sampler is already collecting a phase".to_owned())
        })?;
        let sampler_stop = Arc::new(AtomicBool::new(false));
        let completed_samplers = Arc::new(AtomicU64::new(0));
        let published_sequence = Arc::new(AtomicU64::new(0));
        let shared_tree: SharedMembershipTree = Arc::new(std::sync::Mutex::new(None));
        let staggered_interval = self.membership_thread_interval;
        let membership = std::thread::Builder::new()
            .name("ahrb-membership-sampler".to_owned())
            .spawn({
                let roots = roots.to_vec();
                let stop = Arc::clone(&sampler_stop);
                let membership_interval = staggered_interval;
                let membership_cadence = self.membership_cadence;
                let collector_started = self.started;
                let completed_samplers = Arc::clone(&completed_samplers);
                let published_sequence = Arc::clone(&published_sequence);
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
                    let roots = roots.to_vec();
                    let stop = Arc::clone(&sampler_stop);
                    let membership_cadence = self.membership_cadence;
                    let initial_delay = stagger.checked_mul(index).unwrap_or(stagger);
                    let collector_started = self.started;
                    let completed_samplers = Arc::clone(&completed_samplers);
                    let published_sequence = Arc::clone(&published_sequence);
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
        let operation_result = operation.await;
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
    let mut initial_idle_workers = None;
    let mut final_idle_workers = None;
    let mut initial_idle_threads = None;
    let mut final_idle_threads = None;
    let rotation_seed = 0_u64;

    for repetition in 0..timing.repetitions {
        // The guard window is outside the measured tree. It also prevents one
        // repetition's process teardown from overlapping the next cold launch.
        tokio::time::sleep(Duration::from_millis(timing.load_guard_ms)).await;
        let repetition_root = profile_root.join(format!("resource-repetition-{repetition}"));
        prepare_profile(manifest, &repetition_root)?;
        let variables = BTreeMap::from([
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
        let command = render_argv(&manifest.transport.command, &variables)?;
        let launch_started = Instant::now();
        let mut driver = make_driver(manifest, &command, &environment)?;
        driver.start().await?;
        let cold_phase = format!("resource-r{repetition}-cold-start");
        let sampler = collector.sampler.as_deref_mut().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let cold_roots =
            verified_process_roots(manifest, sampler, driver.transport.owned_pids(), None)?;
        let cold_sampling_started_after_launch_ms = duration_millis(launch_started.elapsed());
        let daemon_pid = collector
            .sample_until(
                &cold_roots,
                &cold_phase,
                await_owned_pid(manifest, &variables),
            )
            .await?;
        let readiness_ms = duration_millis(launch_started.elapsed());
        let sampler = collector.sampler.as_deref_mut().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let roots =
            verified_process_roots(manifest, sampler, driver.transport.owned_pids(), daemon_pid)?;
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
        long_horizons.push(
            run_long_horizon(
                &mut collector,
                &mut driver,
                &roots,
                workflow,
                &identity,
                &timing,
                !manifest.hooks.completion.is_empty(),
            )
            .await?,
        );
        driver.shutdown().await?;
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
        series: collector.series,
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
        let checkpoint = resource_warmup_checkpoint(identity.repetition, turn);
        let turn_key = format!("resource-r{}-warmup-{turn}", identity.repetition);
        let session = driver.create_session(&actor_name).await?;
        driver.submit(&session, &actor.prompt, &turn_key).await?;
        let sessions = vec![(actor_name.clone(), actor.prompt.clone(), session.clone())];
        let completion_turn_keys = BTreeMap::from([(session.0.clone(), turn_key)]);
        let events = wait_for_resource_barrier(
            driver,
            &sessions,
            &checkpoint,
            Duration::from_millis(timing.reclaim_deadline_ms),
        )
        .await?;
        let barrier = events
            .get(&session.0)
            .and_then(|items| {
                items.iter().find(|event| {
                    event.event == EventVocab::BarrierReached
                        && event.payload.get("name").and_then(Value::as_str)
                            == Some(checkpoint.as_str())
                })
            })
            .ok_or_else(|| AhrbError::Protocol("warm-up barrier evidence is absent".to_owned()))?;
        let release_token = barrier
            .payload
            .get("release_token")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("warm-up release token is absent".to_owned()))?;
        driver.release_checkpoint(&session, release_token).await?;
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
            Duration::from_millis(timing.idle_baseline_ms),
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
    let checkpoint = resource_barrier_checkpoint(identity.repetition, agents);
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
        wait_for_resource_barrier(
            driver,
            &sessions,
            &checkpoint,
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
    let barrier_events = barrier_result?;
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
        let events = barrier_events.get(&session.0).ok_or_else(|| {
            AhrbError::Protocol(format!("session {} omitted barrier evidence", session.0))
        })?;
        let barrier = events
            .iter()
            .find(|event| {
                event.event == EventVocab::BarrierReached
                    && event.payload.get("name").and_then(Value::as_str)
                        == Some(checkpoint.as_str())
            })
            .ok_or_else(|| AhrbError::Protocol("barrier event disappeared".to_owned()))?;
        let release_token = barrier
            .payload
            .get("release_token")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("barrier omitted release token".to_owned()))?;
        driver.release_checkpoint(session, release_token).await?;
        observed_actors.insert(actor.clone());
    }
    wait_resource_terminals(
        driver,
        &sessions,
        &completion_turn_keys,
        completion_hook_required,
        Duration::from_millis(timing.reclaim_deadline_ms),
    )
    .await?;
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

async fn wait_for_resource_barrier(
    driver: &mut HarnessDriver,
    sessions: &[(String, String, crate::driver::SessionId)],
    checkpoint: &str,
    deadline: Duration,
) -> Result<BTreeMap<String, Vec<NormalizedEvent>>> {
    let started = Instant::now();
    loop {
        let mut complete = true;
        let mut evidence = BTreeMap::new();
        for (_, _, session) in sessions {
            let events = driver.attach(session, None).await?;
            let tool_cursor = events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .map(|event| event.cursor)
                .max();
            let barrier_cursor = events
                .iter()
                .filter(|event| {
                    event.event == EventVocab::BarrierReached
                        && event.payload.get("name").and_then(Value::as_str) == Some(checkpoint)
                })
                .map(|event| event.cursor)
                .max();
            if !matches!((tool_cursor, barrier_cursor), (Some(tool), Some(barrier)) if tool < barrier)
            {
                complete = false;
            }
            evidence.insert(session.0.clone(), events);
        }
        if complete {
            return Ok(evidence);
        }
        if started.elapsed() >= deadline {
            return Err(AhrbError::Timeout(format!(
                "resource barrier {checkpoint:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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
            let terminal = events.iter().any(|event| is_terminal(&event.event));
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
            if !terminal || !completion_hook {
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

async fn run_long_horizon(
    collector: &mut ResourceCollector,
    driver: &mut HarnessDriver,
    roots: &[u32],
    workflow: &Workflow,
    identity: &RepetitionIdentity,
    timing: &ResourceTimingPlan,
    completion_hook_required: bool,
) -> Result<LongHorizonObservation> {
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
    for turn in 1..=timing.long_horizon_turns {
        let checkpoint = resource_long_checkpoint(identity.repetition, turn);
        let prompt = format!(
            "AHRB long horizon turn {turn} {}",
            route_marker(&workflow.scenario, &actor_name, &checkpoint)
        );
        let turn_key = format!("resource-long-r{}-turn-{turn}", identity.repetition);
        driver.submit(&session, &prompt, &turn_key).await?;
        let (terminal_cursor, tool_results) = wait_one_terminal(
            driver,
            &session,
            after,
            &turn_key,
            completion_hook_required,
            Duration::from_millis(timing.reclaim_deadline_ms),
        )
        .await?;
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
    Ok(LongHorizonObservation {
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
    })
}

async fn wait_one_terminal(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<crate::driver::Cursor>,
    turn_key: &str,
    completion_hook_required: bool,
    deadline: Duration,
) -> Result<(crate::driver::Cursor, Vec<LongHorizonToolResult>)> {
    let started = Instant::now();
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

fn hard_kill(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid)
        .map_err(|_| AhrbError::Validation("PID exceeds platform range".to_owned()))?;
    // SAFETY: the PID comes from the run-local, fsync'd locator written by the child
    // AHRB launched. The signal is the explicit crash-recovery workload.
    let result = unsafe { libc::kill(pid, libc::SIGKILL) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
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
        series: SampleSeries {
            samples: state.samples.clone(),
        },
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
        let output = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output();
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
    fn individual_membership_discovery_overrun_is_rejected() {
        assert!(reject_membership_overrun(10_000_000, Duration::from_millis(10)).is_ok());
        let error = reject_membership_overrun(10_000_001, Duration::from_millis(10))
            .expect_err("membership collection beyond its cadence must fail");
        assert!(error.to_string().contains("sampler overload"));
    }
}
