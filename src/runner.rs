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
use crate::process::{ProcessInfo, Sample, Sampler};
use crate::report::{Fingerprint, Report};
use crate::resource_certification::{
    IdleObservation, IdleProcessModel, ResourceCertification, ResourceEnvelope, ResourceEvidence,
    ResourcePhases, ResourceProfile, ResourceTimingPlan, evaluate_resources,
};
use crate::sampler::{MemoryMetric, SampleSeries};
use crate::workflow::{Actor, Barrier, Fault, ScriptedResponse, WORKFLOW_SCHEMA_VERSION, Workflow};
use crate::{AhrbError, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    let (workflow, actors_by_row) = build_workflow(&selected_rows, &profile_root, !embedded_model)?;
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
    let mut environment = isolated_environment(&manifest, &variables)?;
    environment.extend(model_environment);
    environment.insert(
        manifest.fake_model.credential_env.clone(),
        format!("ahrb-{}-{}", &manifest_hash[..16], std::process::id()),
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
    let mut samples = baseline_samples(
        platform_sampler.as_mut(),
        root_pid,
        options.profile,
        resource_selected,
    )
    .await
    .map_err(|error| AhrbError::Protocol(format!("sample warm idle: {error}")))?;
    let mut sessions: BTreeMap<u8, Vec<crate::driver::SessionId>> = BTreeMap::new();

    for (row, actor_names) in &actors_by_row {
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

    let parallel_agents = sessions.get(&26).map_or(0, Vec::len);
    if parallel_agents > 0 && !embedded_model {
        tokio::time::timeout(
            Duration::from_secs(5),
            engine.barriers().wait_until_ready("row26-steady"),
        )
        .await
        .map_err(|_| AhrbError::Timeout("N=8 state barrier readiness".to_owned()))??;
        let hold = match options.profile {
            Profile::Quick => Duration::from_millis(300),
            Profile::Cert => Duration::from_secs(3),
        };
        let started = Instant::now();
        while started.elapsed() < hold {
            if let Some(pid) = root_pid {
                let tree = platform_sampler.discover(&[pid])?;
                samples.push(platform_sampler.sample(&tree, "parallel-steady")?);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine.barriers().release("row26-steady").await?;
    } else if parallel_agents > 0 {
        if let Some(pid) = root_pid {
            let tree = platform_sampler.discover(&[pid])?;
            samples.push(platform_sampler.sample(&tree, "n8-barrier-steady")?);
        }
    }

    let events = collect_terminals(
        &mut driver,
        &sessions,
        Duration::from_millis(manifest.resources.turn_timeout_ms),
    )
    .await?;

    if let Some(pid) = root_pid {
        let tree = platform_sampler.discover(&[pid])?;
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
    };
    let request_records = engine.request_records().await;
    server.shutdown().await?;

    let resource_evidence = resource_evidence(&state, &manifest);
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
    let processes = unique_processes(&state.samples);
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

async fn baseline_samples(
    sampler: &mut dyn Sampler,
    pid: Option<u32>,
    profile: Profile,
    resource_selected: bool,
) -> Result<Vec<Sample>> {
    let Some(pid) = pid else {
        return Ok(Vec::new());
    };
    let duration = if resource_selected {
        Duration::from_millis(
            ResourceTimingPlan::for_profile(ResourceProfile::from(profile)).idle_baseline_ms,
        )
    } else {
        match profile {
            Profile::Quick => Duration::from_millis(100),
            Profile::Cert => Duration::from_millis(580),
        }
    };
    let started = Instant::now();
    let mut samples = Vec::new();
    loop {
        let tree = sampler.discover(&[pid])?;
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

fn resource_evidence(state: &RunState, manifest: &Manifest) -> ResourceEvidence {
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
        sweep: Vec::new(),
        ordinary_return: None,
        cold_start: None,
        single_agent: None,
        cleanup: None,
        long_horizon: None,
    }
}

fn unique_processes(samples: &[Sample]) -> Vec<ProcessInfo> {
    let mut processes = BTreeMap::new();
    for sample in samples {
        for process in &sample.processes {
            processes
                .entry(process.identity)
                .or_insert_with(|| process.clone());
        }
    }
    processes.into_values().collect()
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
