//! End-to-end benchmark orchestration.

use crate::cli::{Profile, RunOptions};
use crate::driver::{
    Cursor, Driver, DriverOperations, GenericDriver, HttpTransport, ManagedDaemonConfig,
    ManagedDaemonTransport, PerInvocationConfig, PerInvocationDriver, SocketJsonRpcTransport,
    StdinRpcTransport, Transport,
};
use crate::evaluate::{Assertion, TestResult, certify, classify, suite_exit_code};
use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{FakeModelEngine, FakeModelServer, FakeModelUnixServer};
use crate::manifest::{Manifest, TransportKind};
use crate::process::{ProcessSample, ProcessTree, Sample, Sampler};
use crate::report::{Fingerprint, MembershipSample, Report, TopologyMetric};
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type HarnessDriver = Box<dyn Driver>;

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
    cancel_cleanup_valid: Option<bool>,
    cancel_cleanup_detail: Option<String>,
    resume_idempotency_valid: Option<bool>,
    resume_idempotency_detail: Option<String>,
    parallel_agents: usize,
    resource_evidence: Option<ResourceEvidence>,
    per_invocation_resources: Vec<PerInvocationObservation>,
}

struct PerInvocationResourceCollection {
    observations: Vec<PerInvocationObservation>,
    samples: Vec<Sample>,
}

fn per_invocation_topology(manifest: &Manifest) -> bool {
    !manifest.daemon.persistent
        && matches!(
            crate::manifest::topology_family(&manifest.concurrency.topology),
            Some(crate::manifest::TopologyFamily::PerInvocation)
        )
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
    let mut variables = BTreeMap::from([
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
        &manifest,
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

    let root_pid = await_owned_pid(&manifest, &variables)
        .await
        .map_err(|error| AhrbError::Protocol(format!("locate owned process: {error}")))?;
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
        Some(
            collect_per_invocation_resource_observations(
                &manifest,
                options.profile,
                &profile_root,
                &workflow,
                &model_environment,
                &credential,
            )
            .await
            .map_err(|error| {
                AhrbError::Protocol(format!("collect per-invocation resources: {error}"))
            })?,
        )
    } else {
        None
    };
    let resource_evidence = if resource_selected && per_invocation_collection.is_none() {
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

    for (row, actor_names) in &actors_by_row {
        if (20..=29).contains(row) {
            continue;
        }
        if !matches!(
            crate::matrix_evidence::capability_for_row(&manifest, *row),
            crate::matrix_evidence::CapabilityStatus::Supported
        ) {
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

    let mut precollected_events = BTreeMap::new();
    let mut session_replay_valid = None;
    let mut session_replay_detail = None;
    let mut cancel_cleanup_valid = None;
    let mut cancel_cleanup_detail = None;
    let mut resume_idempotency_valid = None;
    let mut resume_idempotency_detail = None;
    if let Some(session) = sessions.get(&36).and_then(|items| items.first()) {
        wait_for_session_event(
            &mut driver,
            session,
            EventVocab::TurnAccepted,
            Duration::from_millis(manifest.resources.turn_timeout_ms),
        )
        .await?;
        let roots = driver.session_pids(session);
        driver.cancel(session).await?;
        if per_invocation_topology(&manifest) {
            let process_cleared = !roots.is_empty()
                && await_owned_tree_empty(
                    platform_sampler.as_mut(),
                    &roots,
                    Duration::from_millis(manifest.daemon.grace_ms.max(100)),
                )
                .await?;
            let workspace_cleared = per_invocation_workspaces_clean(&profile_root, session)?;
            cancel_cleanup_valid = Some(process_cleared && workspace_cleared);
            cancel_cleanup_detail = Some(format!(
                "terminated {} one-shot process root(s): cleared={process_cleared}; driver and harness workspaces clean={workspace_cleared}",
                roots.len()
            ));
        } else {
            cancel_cleanup_valid = Some(true);
            cancel_cleanup_detail = Some(
                "shared controller acknowledged session cancellation; terminal evidence verifies cleanup"
                    .to_owned(),
            );
        }
    }
    if let Some(session) = sessions.get(&16).and_then(|items| items.first()) {
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
        precollected_events.insert(16_u8, transcript);
    }

    if let Some(session) = sessions.get(&30).and_then(|items| items.first()) {
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
        driver.resume(session).await?;
        let suffix = driver.attach(session, Some(after)).await?;
        let suffix_validation = validate_recovered_suffix(&original, Some(after), &suffix);
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
        precollected_events.insert(30_u8, transcript);
    }

    if let Some(session) = sessions.get(&37).and_then(|items| items.first()) {
        let original = collect_session_terminal(
            &mut driver,
            session,
            None,
            Duration::from_millis(manifest.resources.turn_timeout_ms),
        )
        .await?;
        driver.resume(session).await?;
        let actor = workflow
            .actors
            .get("r37")
            .ok_or_else(|| AhrbError::Protocol("row-37 workflow actor is absent".to_owned()))?;
        driver
            .submit(session, &actor.prompt, "row-37-turn-1")
            .await?;
        let replayed = collect_session_terminal(
            &mut driver,
            session,
            None,
            Duration::from_millis(manifest.resources.turn_timeout_ms),
        )
        .await?;
        let accepted = replayed
            .iter()
            .filter(|event| event.event == EventVocab::TurnAccepted)
            .count();
        let effects = replayed
            .iter()
            .filter(|event| event.event == EventVocab::ToolResult)
            .count();
        let terminals = replayed
            .iter()
            .filter(|event| is_terminal(&event.event))
            .count();
        let unchanged = replayed == original;
        let valid = unchanged && accepted == 1 && effects == 1 && terminals == 1;
        resume_idempotency_valid = Some(valid);
        resume_idempotency_detail = Some(format!(
            "disk reopen plus duplicate submit preserved the exact journal: unchanged={unchanged}, accepted={accepted}, committed_effects={effects}, terminals={terminals}"
        ));
        precollected_events.insert(37_u8, replayed);
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
    let needs_recovery = [35_u8, 40].iter().any(|row| {
        selected_rows.contains(row)
            && matches!(
                crate::matrix_evidence::capability_for_row(&manifest, *row),
                crate::matrix_evidence::CapabilityStatus::Supported
            )
    });
    let crash_pre_events = if needs_recovery {
        if let Some(session) = sessions.get(&35).and_then(|items| items.first()) {
            Some(
                collect_session_checkpoint(
                    &mut driver,
                    session,
                    "row-35-post-commit",
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?,
            )
        } else {
            None
        }
    } else {
        None
    };
    let journal_pre_events = if needs_recovery {
        if let Some(session) = sessions.get(&40).and_then(|items| items.first()) {
            Some(
                collect_session_checkpoint(
                    &mut driver,
                    session,
                    "row-40-post-commit",
                    Duration::from_millis(manifest.resources.turn_timeout_ms),
                )
                .await?,
            )
        } else {
            None
        }
    } else {
        None
    };

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
    let mut events = collect_terminals(
        &mut driver,
        &sessions_to_collect,
        Duration::from_millis(manifest.resources.turn_timeout_ms),
    )
    .await?;
    events.extend(precollected_events);
    if let Some(pre_crash) = &crash_pre_events {
        events.insert(35, pre_crash.clone());
    }
    if let Some(pre_crash) = &journal_pre_events {
        events.insert(40, pre_crash.clone());
    }

    if !main_roots.is_empty() {
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
        drop(driver);
        let tree_cleared = had_owned_process
            && await_owned_tree_empty(
                platform_sampler.as_mut(),
                &recovery_roots,
                Duration::from_millis(manifest.daemon.grace_ms.max(100)),
            )
            .await?;
        crash_recovery_tree_cleared = Some(tree_cleared);
        if tree_cleared {
            if let Some(session) = sessions.get(&40).and_then(|items| items.first()) {
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
            let mut recovered = make_driver(
                &manifest,
                &command,
                &environment,
                &variables,
                &profile_root,
                false,
            )?;
            recovered.start().await?;
            crash_recovery_ms = Some(recovery_started.elapsed().as_secs_f64() * 1_000.0);
            if let Some(session) = sessions.get(&35).and_then(|items| items.first()) {
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
                    recovered.resume(session).await?;
                    let actor = workflow.actors.get("r35").ok_or_else(|| {
                        AhrbError::Protocol("row-35 workflow actor is absent".to_owned())
                    })?;
                    recovered
                        .submit(session, &actor.prompt, "row-35-turn-1")
                        .await?;
                    recovered.release_checkpoint(session, release_token).await?;
                    let recovered_events = collect_session_terminal(
                        &mut recovered,
                        session,
                        None,
                        Duration::from_millis(manifest.resources.turn_timeout_ms),
                    )
                    .await?;
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
                    events.insert(35, recovered_events);
                } else {
                    crash_recovery_valid = Some(false);
                    crash_recovery_detail = Some(
                        "named post-commit checkpoint omitted its durable release token".to_owned(),
                    );
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
                                if journal_torn_tail_injected == Some(true) {
                                    journal_recovery_valid = Some(true);
                                    journal_recovery_detail = Some(format!(
                                        "replayed {} exact, contiguous, duplicate-free events and cleanly ignored the induced torn tail",
                                        suffix.len()
                                    ));
                                }
                            }
                            Err(detail) => {
                                journal_recovery_valid = Some(false);
                                journal_recovery_detail = Some(detail);
                            }
                        }
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
    }

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
        cancel_cleanup_valid,
        cancel_cleanup_detail,
        resume_idempotency_valid,
        resume_idempotency_detail,
        parallel_agents,
        resource_evidence,
        per_invocation_resources: per_invocation_collection
            .map(|collection| collection.observations)
            .unwrap_or_default(),
    };
    let request_records = engine.request_records().await;
    server.shutdown().await?;

    let resource_evidence = state
        .resource_evidence
        .clone()
        .unwrap_or_else(|| incomplete_resource_evidence(&state, &manifest));
    let resource_certification =
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
    let mut results = evaluate_rows(
        &selected,
        &state,
        &request_records,
        &manifest,
        &resource_certification,
    );
    results.sort_by_key(|result| result.row);
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
    let badge = certify(
        &results,
        &manifest,
        std::env::consts::OS,
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
        resource_metrics,
        samples: state.samples,
        processes,
        membership,
        events: raw_events,
        model_requests,
    };
    crate::report::write_bundle(&report, &options.output, options.junit)?;
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
            std::fs::create_dir_all(parent)?;
        }
        let content = crate::manifest::render_template(&specification.content, variables)?;
        std::fs::write(&path, content.as_bytes())?;
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
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
        }
        let file = std::fs::OpenOptions::new().write(true).open(&path)?;
        file.sync_all()?;
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
    variables: &BTreeMap<String, String>,
    profile_root: &Path,
    gate_exec_launch: bool,
) -> Result<HarnessDriver> {
    let timeout = Duration::from_millis(manifest.transport.timeout_ms);
    if manifest.transport.kind == TransportKind::Exec {
        let first_command = resolve_local_program(&manifest.transport.command)?;
        let resume_command = resolve_local_program(&manifest.sessions.resume)?;
        return Ok(Box::new(PerInvocationDriver::new(PerInvocationConfig {
            command: first_command,
            resume_command,
            release_command: resolve_local_program(&manifest.concurrency.release)?,
            cancel_command: resolve_local_program(&manifest.agents.cancel)?,
            replay_command: resolve_local_program(&manifest.events.replay_command)?,
            environment: environment.clone(),
            profile_root: profile_root.to_path_buf(),
            events: manifest.events.clone(),
            exit: manifest.exit.clone(),
            session_id_pointer: manifest.sessions.id_pointer.clone(),
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
        TransportKind::StdinRpc => Box::new(
            StdinRpcTransport::new(command.to_vec(), timeout).with_environment(environment.clone()),
        ),
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
        let mut readiness = manifest.daemon.readiness.clone();
        readiness.target = crate::manifest::render_template(&readiness.target, variables)?;
        transport = Box::new(ManagedDaemonTransport::new(
            transport,
            ManagedDaemonConfig {
                command: resolve_local_program(&render_argv(&manifest.daemon.start, variables)?)?,
                environment: environment.clone(),
                readiness,
                grace: Duration::from_millis(manifest.daemon.grace_ms.max(1)),
                log_directory: profile_root.join("daemon-logs"),
            },
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
    };
    Ok(Box::new(
        GenericDriver::new(transport).with_operations(operations),
    ))
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
            let mut arguments = serde_json::Map::from_iter([
                (
                    "path".to_owned(),
                    Value::String(format!("warmup-{turn}.txt")),
                ),
                (
                    "content".to_owned(),
                    Value::String(format!("warmup {terminal}")),
                ),
            ]);
            if manifest.daemon.persistent {
                arguments.insert(
                    "ahrb_checkpoint".to_owned(),
                    json!({"name":checkpoint, "phase":"after-commit"}),
                );
            }
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
                let mut arguments = serde_json::Map::from_iter([
                    (
                        "path".to_owned(),
                        Value::String("resource-fixture.txt".to_owned()),
                    ),
                    (
                        "content".to_owned(),
                        Value::String(format!("resource {terminal}")),
                    ),
                ]);
                if manifest.daemon.persistent {
                    arguments.insert(
                        "ahrb_checkpoint".to_owned(),
                        json!({"name":checkpoint, "phase":"after-commit"}),
                    );
                }
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

fn mapped_tool_call(
    manifest: &Manifest,
    semantic: &str,
    call_id: String,
    semantic_arguments: Value,
) -> Result<Value> {
    let name = manifest
        .tools
        .aliases
        .get(semantic)
        .cloned()
        .unwrap_or_else(|| format!("{semantic}_fixture"));
    let object = semantic_arguments.as_object().ok_or_else(|| {
        AhrbError::Validation(format!(
            "semantic tool {semantic:?} arguments are not an object"
        ))
    })?;
    let arguments = if let Some(command_field) = manifest.tools.bindings.get("command") {
        let template = manifest.tools.fixtures.get(semantic).ok_or_else(|| {
            AhrbError::Validation(format!(
                "tool {semantic:?} binds a command field but has no fixture argv"
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
        let mut mapped = serde_json::Map::new();
        mapped.insert(command_field.clone(), Value::String(shell_join(&argv)));
        Value::Object(mapped)
    } else if let Some(command_field) = manifest.tools.bindings.get("command_argv") {
        let template = manifest.tools.fixtures.get(semantic).ok_or_else(|| {
            AhrbError::Validation(format!(
                "tool {semantic:?} binds an argv command field but has no fixture argv"
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
        let mut mapped = serde_json::Map::new();
        mapped.insert(
            command_field.clone(),
            Value::Array(argv.into_iter().map(Value::String).collect()),
        );
        Value::Object(mapped)
    } else {
        let mut mapped = serde_json::Map::new();
        for (key, value) in object {
            let target = manifest
                .tools
                .bindings
                .get(key)
                .cloned()
                .unwrap_or_else(|| key.clone());
            mapped.insert(target, value.clone());
        }
        Value::Object(mapped)
    };
    Ok(json!({"id":call_id, "name":name, "arguments":arguments}))
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

fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|argument| format!("'{}'", argument.replace('\'', "'\"'\"'")))
        .collect::<Vec<_>>()
        .join(" ")
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

async fn collect_session_terminal(
    driver: &mut HarnessDriver,
    session: &crate::driver::SessionId,
    after: Option<Cursor>,
    deadline: Duration,
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
        tokio::time::sleep(Duration::from_millis(10)).await;
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
                        && state.journal_torn_tail_injected == Some(true)
                        && state.journal_recovered_events.is_some_and(|count| count > 0),
                    state.journal_recovery_detail.clone().unwrap_or_else(|| {
                        "journal recovery trial produced no validation evidence".to_owned()
                    }),
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
        let tree = sampler.discover(&roots)?;
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
                let roots = roots.to_vec();
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
                    let roots = roots.to_vec();
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
    let mut collected_samples = Vec::new();

    for repetition in 0..timing.repetitions {
        let repetition_root =
            profile_root.join(format!("per-invocation-resource-repetition-{repetition}"));
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
            let mut collector = ResourceCollector::new(&timing);
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
                .iter()
                .map(|sample| sample.cpu_ns)
                .max()
                .unwrap_or(0);
            observations.push(PerInvocationObservation {
                repetition,
                agents: *agents,
                peak_bytes,
                cold_peak_bytes: peak_bytes,
                cpu_ns,
                completed_processes,
                residual_processes,
            });
            collected_samples.extend(collector.series.samples);
            for session in &sessions {
                driver.close(session).await?;
            }
        }
        driver.shutdown().await?;
    }

    Ok(PerInvocationResourceCollection {
        observations,
        samples: collected_samples,
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
        let launch_started = Instant::now();
        let mut driver = make_driver(
            manifest,
            &command,
            &environment,
            &variables,
            &repetition_root,
            false,
        )?;
        driver.start().await?;
        let cold_phase = format!("resource-r{repetition}-cold-start");
        let sampler = collector.sampler.as_deref_mut().ok_or_else(|| {
            AhrbError::Protocol("resource sampler is already collecting a phase".to_owned())
        })?;
        let cold_roots = verified_process_roots(manifest, sampler, driver.owned_pids(), None)?;
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
    let expected = original
        .iter()
        .filter(|event| event.cursor > after_cursor)
        .collect::<Vec<_>>();
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
    let mut expected_cursor = after_cursor.checked_add(1).ok_or_else(|| {
        "journal replay cursor overflowed after the requested attachment point".to_owned()
    })?;
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
        if actual != expected_event {
            return Err(format!(
                "journal replay event at suffix index {index} disagrees with the pre-crash durable journal"
            ));
        }
        expected_cursor = expected_cursor.saturating_add(1);
    }
    Ok(())
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

    #[test]
    fn individual_membership_discovery_overrun_is_rejected() {
        assert!(reject_membership_overrun(10_000_000, Duration::from_millis(10)).is_ok());
        let error = reject_membership_overrun(10_000_001, Duration::from_millis(10))
            .expect_err("membership collection beyond its cadence must fail");
        assert!(error.to_string().contains("sampler overload"));
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
}
