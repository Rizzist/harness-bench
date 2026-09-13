//! Separate instrumented workload; never feed S2 samples into S1/S3 scalars.
use super::*;
use crate::storage::durability as collector;

pub(super) async fn preflight(
    manifest: &Manifest,
    config: &StorageConfig,
    progress: &mut Progress,
) -> Result<()> {
    let command = resolve_local_program(
        &manifest
            .availability
            .exec_paths
            .iter()
            .find_map(|p| {
                let resolved = resolve_local_program(std::slice::from_ref(p)).ok()?;
                let path = PathBuf::from(&resolved[0]);
                if path.is_file() {
                    Some(resolved)
                } else {
                    std::env::var_os("PATH").and_then(|paths| {
                        std::env::split_paths(&paths)
                            .map(|dir| dir.join(p))
                            .find(|p| p.is_file())
                            .map(|p| vec![p.to_string_lossy().into_owned()])
                    })
                }
            })
            .ok_or_else(|| {
                AhrbError::Unsupported("os-limited: executable resolution failed".into())
            })?,
    )?;
    let executable = std::fs::canonicalize(&command[0])?;
    let probe = collector::probe(
        &progress.output,
        &executable,
        manifest
            .availability
            .version_probe
            .get(1..)
            .unwrap_or_default(),
    )
    .await?;
    progress.report.fsync_events.extend(probe.events);
    let mut details = DurabilityDetails {
        measurement_label: "instrumented durability calls; wall cost is an estimate at 4 ms/call"
            .into(),
        reason: probe.instrumentation.reason.clone(),
        instrumentation: vec![probe.instrumentation],
        trials: Vec::new(),
    };
    if let Some(reason) = &details.reason {
        progress.finalized.insert(1);
        progress.report.results[1] = row(1, TestOutcome::Unsupported(reason.clone()), config);
        details.trials.push(Trial {
            repetition: 0,
            outcome: TestOutcome::Unsupported(reason.clone()),
            measurement_complete: true,
            reason: Some(reason.clone()),
            summary: DurabilitySummary {
                assumed_fsync_cost_ms: Some(4.0),
                ..Default::default()
            },
            diagnostics: DurabilityDiagnostics {
                instrumentation_ref: Some(0),
                ..Default::default()
            },
            evidence_refs: Vec::new(),
        });
    }
    progress
        .details
        .insert(ROWS[1].into(), serde_json::to_value(&details)?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    manifest: &Manifest,
    config: &StorageConfig,
    manifest_hash: &str,
    workflow: &Workflow,
    run_root: &Path,
    repetitions: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<()> {
    let mut details: DurabilityDetails = serde_json::from_value(progress.details[ROWS[1]].clone())?;
    if details.reason.is_some() {
        return Ok(());
    }
    let executable = details
        .instrumentation
        .first()
        .ok_or_else(|| AhrbError::Protocol("S2 preflight missing".into()))?
        .executable_sha256
        .clone();
    for repetition in 1..=repetitions {
        let hook = collector::Collector::new(&progress.output, &format!("r{repetition}"))?;
        let result = collect(
            manifest,
            config,
            manifest_hash,
            workflow,
            run_root,
            repetition,
            n,
            &hook,
            progress,
        )
        .await;
        let capture = collector::read_trace(&hook.trace, repetition, true);
        let primitive_applicability = capture
            .as_ref()
            .map(collector::applicability)
            .unwrap_or_default();
        let mut instrumentation = Instrumentation {
            repetition,
            backend: collector::BACKEND.into(),
            version: collector::VERSION.into(),
            executable_sha256: executable.clone(),
            shim_sha256: Some(collector::digest(&hook.shim)?),
            environment_keys: hook.environment().into_keys().collect(),
            drop_count: collector::observed_drop_count(&hook.trace)?,
            ..Default::default()
        };
        let evaluated = match (result, capture) {
            (Ok(intervals), Ok(mut capture)) => {
                instrumentation.images = capture.images;
                let outcome = if !capture.gaps.is_empty() {
                    Err(AhrbError::Protocol(format!(
                        "durability coverage lost after preflight: {}",
                        capture.gaps.join("; ")
                    )))
                } else if instrumentation.images.is_empty() {
                    Err(AhrbError::Protocol(
                        "durability missing image receipts after successful preflight".into(),
                    ))
                } else {
                    attribute_turns(&mut capture.events, &intervals)
                        .and_then(|()| collector::summarize(&capture.events, n))
                };
                progress.report.fsync_events.extend(capture.events);
                outcome
            }
            (Err(e), Ok(capture)) => {
                instrumentation.images = capture.images;
                progress.report.fsync_events.extend(capture.events);
                Err(e)
            }
            (_, Err(e)) => Err(e),
        };
        let index = details.instrumentation.len() as u64;
        let (outcome, summary, failed) = match evaluated {
            Ok((summary, failed)) => (TestOutcome::Pass, summary, Some(failed)),
            Err(e) => {
                let reason = e.to_string();
                instrumentation.reason = Some(reason.clone());
                (
                    TestOutcome::Error(reason),
                    DurabilitySummary {
                        assumed_fsync_cost_ms: Some(4.0),
                        ..Default::default()
                    },
                    None,
                )
            }
        };
        details.instrumentation.push(instrumentation);
        details.trials.push(Trial {
            repetition,
            measurement_complete: outcome == TestOutcome::Pass,
            reason: outcome_reason(&outcome),
            outcome: outcome.clone(),
            diagnostics: DurabilityDiagnostics {
                failed_calls: failed,
                primitive_applicability,
                instrumentation_ref: Some(index),
            },
            summary,
            evidence_refs: Vec::new(),
        });
        if outcome != TestOutcome::Pass {
            details.reason = outcome_reason(&outcome);
        }
        progress
            .details
            .insert(ROWS[1].into(), serde_json::to_value(&details)?);
        progress.active_engine = None;
        if outcome != TestOutcome::Pass {
            progress.finalized.insert(1);
            progress.report.results[1] = row(1, outcome, config);
            ensure_owned_cleanup()?;
            return Ok(());
        }
    }
    let summary = progress
        .report
        .storage_summary
        .as_mut()
        .expect("storage summary");
    let median_field = |get: fn(&DurabilitySummary) -> Option<f64>| {
        details
            .trials
            .iter()
            .map(|t| get(&t.summary))
            .collect::<Option<Vec<_>>>()
            .and_then(median)
    };
    summary.fsync_calls_per_turn = median_field(|s| s.fsync_calls_per_turn);
    summary.fdatasync_calls_per_turn = median_field(|s| s.fdatasync_calls_per_turn);
    summary.fullfsync_calls_per_turn = median_field(|s| s.fullfsync_calls_per_turn);
    summary.durability_calls_per_turn = median_field(|s| s.durability_calls_per_turn);
    summary.estimated_durability_wall_ms_per_turn =
        median_field(|s| s.estimated_durability_wall_ms_per_turn);
    summary.durability_class = summary
        .durability_calls_per_turn
        .map(|v| collector::class(v).into());
    progress.finalized.insert(1);
    progress.report.results[1] = row(1, TestOutcome::Pass, config);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn collect(
    manifest: &Manifest,
    config: &StorageConfig,
    manifest_hash: &str,
    workflow: &Workflow,
    run_root: &Path,
    repetition: u32,
    n: u32,
    hook: &collector::Collector,
    progress: &mut Progress,
) -> Result<Vec<(u32, u64, u64)>> {
    let profile = run_root.join(format!("s2-r{repetition}"));
    prepare_profile(manifest, &profile)?;
    let initial_workspace = profile.join("storage-workspace");
    std::fs::create_dir(&initial_workspace)?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        workflow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    let ScriptedPillarRuntime {
        engine,
        server,
        mut driver,
        ..
    } = start_scripted_pillar_runtime_with_environment(
        manifest,
        manifest_hash,
        workflow,
        &profile,
        "storage",
        Some(&initial_workspace),
        engine,
        false,
        &hook.environment(),
    )
    .await?;
    // Distinct repetition ordinals preserve request/turn identity without
    // merging the instrumented run into the uninstrumented physical totals.
    let evidence_repetition = 1_000_000 + repetition;
    engine.enable_storage_body_capture(&progress.output.join("request-bodies"))?;
    progress.active_engine = Some((evidence_repetition, Arc::clone(&engine), 0, 0));
    let session = driver.create_session(&format!("{TASK}:storage")).await?;
    let workspace = if let Some(template) = &config.workspace_path {
        PathBuf::from(crate::manifest::render_template(
            template,
            &BTreeMap::from([
                ("profile".into(), profile.to_string_lossy().into_owned()),
                ("session_id".into(), session.0.clone()),
            ]),
        )?)
    } else {
        driver
            .session_workspace(&session)
            .ok_or_else(|| AhrbError::Validation("missing storage workspace locator".into()))?
    };
    validate_scripted_workspace_path(&profile, &workspace, "storage")?;
    std::fs::create_dir_all(&workspace)?;
    fixture::seed(&workspace)?;
    let mut intervals = Vec::new();
    let mut after = None;
    let mut owned = BTreeSet::new();
    let mut sampler = platform_sampler();
    let baseline = accounting::settle(&profile, config, monotonic_timestamp_ns()).await?;
    record_settle(progress, config, repetition, 0, &baseline);
    for turn in 1..=n {
        let key = format!("storage-s2-r{repetition}-t{turn:04}");
        let boundaries = driver.completed_turn_boundaries().len();
        let submit_ns = monotonic_timestamp_ns();
        let started = Instant::now();
        driver
            .submit(&session, &fixture::prompt(turn)?, &key)
            .await?;
        let roots = driver.owned_pids();
        if roots.is_empty() {
            return Err(AhrbError::Protocol("S2 missing owned process roots".into()));
        }
        observe_owned(
            sampler.as_mut(),
            &roots,
            &mut owned,
            progress,
            repetition,
            turn,
        )?;
        let (cursor, tools, terminal_ns) = {
            let terminal = wait_one_terminal(
                &mut driver,
                &session,
                after,
                &key,
                false,
                outer_turn_timeout(manifest),
                started,
            );
            tokio::pin!(terminal);
            loop {
                tokio::select! {
                    r=&mut terminal=>break r?,
                    _=tokio::time::sleep(Duration::from_millis(10))=>{observe_owned(sampler.as_mut(), &roots, &mut owned, progress, repetition, turn)?;}
                }
            }
        };
        let events = driver.attach(&session, after).await?;
        if events.iter().filter(|e| is_terminal(&e.event)).count() != 1
            || !events
                .iter()
                .any(|e| e.event == EventVocab::TerminalSuccess)
            || (turn % 10 == 0
                && !tools
                    .iter()
                    .any(|t| t.call_id == format!("storage-read-{turn:04}")))
        {
            return Err(AhrbError::Protocol(
                "S2 task-incomplete: terminal or correlated read missing".into(),
            ));
        }
        if manifest.transport.kind == TransportKind::Exec {
            await_completed_turn_boundary(
                &mut driver,
                &session,
                Some(cursor),
                boundaries,
                outer_turn_timeout(manifest),
            )
            .await?;
        }
        after = Some(cursor);
        let settled = accounting::settle(&profile, config, terminal_ns).await?;
        let settled_ns = monotonic_timestamp_ns();
        if driver
            .attach(&session, after)
            .await?
            .iter()
            .any(|event| is_terminal(&event.event))
        {
            return Err(AhrbError::Protocol(
                "S2 task-incomplete: extra terminal after settled turn".into(),
            ));
        }
        record_settle(progress, config, repetition, turn, &settled);
        intervals.push((turn, submit_ns, settled_ns));
        progress.report.turns.push(TurnObservation {
            repetition: evidence_repetition,
            turn_index: turn,
            actor: "storage".into(),
            session_id_hash: stable_evidence_hash(&session.0),
            phase: "storage-durability-instrumented".into(),
            submit_ns: Some(submit_ns),
            terminal_ns: Some(terminal_ns),
            turn_wall_ns: Some(terminal_ns - submit_ns),
            ..Default::default()
        });
        progress.report.events.extend(
            events
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        );
        capture_provider(progress).await?;
        let checkpoint = if turn % 10 == 0 {
            format!("t{turn:04}-terminal")
        } else {
            format!("t{turn:04}")
        };
        if !progress.report.model_requests.iter().any(|r| {
            r["repetition"] == evidence_repetition
                && r["accepted"] == true
                && r["request"]["checkpoint"] == checkpoint
                && r["request"]["scenario"] == TASK
        }) {
            return Err(AhrbError::Protocol(
                "S2 task-incomplete: missing accepted terminal provider response".into(),
            ));
        }
        if turn % 10 == 0 {
            println!("storage S2 repetition {repetition} turn {turn}/{n}");
        }
    }
    driver.shutdown().await?;
    server.shutdown().await?;
    capture_provider(progress).await?;
    // Keep the active repetition until its typed trial is saved; an error while
    // reading collector files must still receive an interrupted/error trial.
    let capture = collector::read_trace(&hook.trace, repetition, true)?;
    if owned.iter().any(|id| {
        !capture
            .images
            .iter()
            .any(|i| i.pid == id.pid && i.start_time == id.start_time)
    }) {
        return Err(AhrbError::Protocol(
            "durability owned identity lacks image load/control/exit receipt".into(),
        ));
    }
    progress
        .report
        .lifecycle_notes
        .extend(driver.lifecycle_notes());
    Ok(intervals)
}
fn record_settle(
    progress: &mut Progress,
    config: &StorageConfig,
    repetition: u32,
    turn: u32,
    settled: &accounting::SettledInventory,
) {
    let old = progress.report.storage_samples.len();
    record_boundary(
        &mut progress.report,
        config,
        repetition,
        turn,
        settled,
        &Counters::default(),
    );
    let sample = &mut progress.report.storage_samples[old];
    sample.row_id = ROWS[1].into();
    sample.boundary = format!("s2-r{repetition}-t{turn:04}");
    sample.counter_source = "instrumented-no-physical-io".into();
    sample.counter_complete = false;
    sample.physical_write_bytes = None;
    sample.physical_read_bytes = None;
    // record_boundary also supplies file inventories: explicitly mark S2 scope.
    for file in progress
        .report
        .storage_files
        .iter_mut()
        .rev()
        .take(settled.inventory.entries.len())
    {
        file.row_id = ROWS[1].into();
        file.boundary = sample.boundary.clone();
    }
}

/// On cancellation, preserve whatever the raw collector actually delivered.
/// Missing clean ends remain an interrupted trial, never a complete aggregate.
pub(super) fn preserve_partial(progress: &mut Progress) -> Result<()> {
    let Some((ordinal, _, _, _)) = &progress.active_engine else {
        return Ok(());
    };
    if *ordinal < 1_000_000 {
        return Ok(());
    }
    let repetition = *ordinal - 1_000_000;
    let mut details: DurabilityDetails = serde_json::from_value(progress.details[ROWS[1]].clone())?;
    if details.trials.iter().any(|t| t.repetition == repetition) {
        return Ok(());
    }
    let trace = progress
        .output
        .join(format!("durability-support/r{repetition}"));
    let capture = collector::read_trace(&trace, repetition, false);
    let primitive_applicability = capture
        .as_ref()
        .map(collector::applicability)
        .unwrap_or_default();
    let reason = outcome_reason(&progress.report.results[1].outcome)
        .unwrap_or_else(|| "interrupted durability collection".into());
    let mut instrumentation = details.instrumentation.first().cloned().unwrap_or_default();
    instrumentation.repetition = repetition;
    instrumentation.drop_count = collector::observed_drop_count(&trace)?;
    instrumentation.images.clear();
    instrumentation.reason = Some(reason.clone());
    match capture {
        Ok(capture) => {
            instrumentation.images = capture.images;
            progress.report.fsync_events.extend(capture.events);
        }
        Err(e) => {
            instrumentation.reason = Some(format!(
                "{reason}; {e}; raw records retained in {}",
                trace.display()
            ))
        }
    }
    let index = details.instrumentation.len() as u64;
    details.instrumentation.push(instrumentation);
    details.trials.push(Trial {
        repetition,
        outcome: TestOutcome::Error(reason.clone()),
        measurement_complete: false,
        reason: Some(reason),
        summary: DurabilitySummary {
            assumed_fsync_cost_ms: Some(4.0),
            ..Default::default()
        },
        diagnostics: DurabilityDiagnostics {
            instrumentation_ref: Some(index),
            primitive_applicability,
            ..Default::default()
        },
        evidence_refs: Vec::new(),
    });
    progress
        .details
        .insert(ROWS[1].into(), serde_json::to_value(details)?);
    Ok(())
}

fn observe_owned(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    owned: &mut BTreeSet<crate::process::ProcIdentity>,
    progress: &mut Progress,
    repetition: u32,
    turn: u32,
) -> Result<()> {
    let start = monotonic_timestamp_ns();
    let cpu = sampler_thread_cpu_ns()?;
    let tree = sampler.discover(roots)?;
    owned.extend(tree.members.keys().copied());
    let phase = format!("storage-s2-r{repetition}-t{turn}");
    progress
        .report
        .membership
        .push(crate::report::MembershipSample {
            elapsed_ns: start,
            phase: phase.clone(),
            discovery_wall_ns: monotonic_timestamp_ns().saturating_sub(start),
            discovery_cpu_ns: sampler_thread_cpu_ns()?.saturating_sub(cpu),
            lane: 0,
        });
    let sample = sampler.sample(&tree, &phase)?;
    progress
        .report
        .processes
        .extend(sample.process_samples.clone());
    progress.report.samples.push(sample);
    Ok(())
}

#[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
#[path = "../../../tests/common/mod.rs"]
mod test_common;

#[cfg(all(test, target_os = "macos", target_arch = "aarch64"))]
mod tests {
    use super::*;

    // A short internal integration fixture exercises the real driver and OS
    // hooks, including the tenth-turn tool. Production quick/cert budgets are
    // untouched; full profile evidence is collected separately through the CLI.
    #[tokio::test]
    async fn both_transports_measure_real_extra_calls_without_polluting_s1() {
        const TEST: &str = "runner::storage::durability::tests::both_transports_measure_real_extra_calls_without_polluting_s1";
        // Sampler unit tests intentionally observe this test process and retain
        // those identities in the process-wide cleanup registry. Exercise the
        // integration fixture and its cleanup in a fresh process, just as a CLI
        // invocation does. Acquire the shared test lock only in that child.
        if std::env::var("AHRB_S2_TEST_ISOLATED").as_deref() != Ok(TEST) {
            use std::os::unix::process::CommandExt as _;
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
                .env("AHRB_S2_TEST_ISOLATED", TEST)
                .process_group(0)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated durability fixture {}\nstdout: {}\nstderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let _guard = test_common::serialize_ahrb_subprocesses();
        for adapter in ["mock", "mock-exec"] {
            let mut totals = Vec::new();
            for mode in ["per-turn", "extra"] {
                let root = std::env::temp_dir().join(format!(
                    "ahrb-s2-{}-{}",
                    std::process::id(),
                    monotonic_timestamp_ns()
                ));
                std::fs::create_dir(&root).unwrap();
                let profiles = root.join("profiles");
                std::fs::create_dir(&profiles).unwrap();
                std::fs::File::create(root.join("storage-request-bodies.jsonl")).unwrap();
                let mut manifest =
                    crate::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))
                        .unwrap();
                manifest
                    .isolation
                    .environment
                    .insert("AHRB_MOCK_STORAGE_FSYNC_MODE".into(), mode.into());
                let config = manifest.storage.clone().unwrap_or_default();
                let hash = crate::manifest::hash(&manifest).unwrap();
                let workflow = workflow(&manifest, 10).unwrap();
                let mut progress = Progress {
                    output: root.clone(),
                    finalized: BTreeSet::new(),
                    active_engine: None,
                    report: Report {
                        storage_summary: Some(StorageSummary::default()),
                        results: (0..10)
                            .map(|i| row(i, TestOutcome::Error("pending".into()), &config))
                            .collect(),
                        ..Default::default()
                    },
                    write_trials: Vec::new(),
                    curve_trials: Vec::new(),
                    auxiliary_trials: Vec::new(),
                    retention_trials: Vec::new(),
                    details: (0..10)
                        .map(|i| (ROWS[i].into(), pending_details(i, "pending", &config)))
                        .collect(),
                };
                preflight(&manifest, &config, &mut progress).await.unwrap();
                let result = run(
                    &manifest,
                    &config,
                    &hash,
                    &workflow,
                    &profiles,
                    1,
                    10,
                    &mut progress,
                )
                .await;
                ensure_owned_cleanup().unwrap();
                result.unwrap_or_else(|e| {
                    panic!("{adapter}/{mode} {e}; evidence {}", root.display())
                });
                assert_eq!(
                    progress.report.results[1].outcome,
                    TestOutcome::Pass,
                    "{adapter}/{mode}: {} at {}",
                    progress.details[ROWS[1]],
                    root.display()
                );
                let summary = progress.report.storage_summary.as_ref().unwrap();
                assert_eq!(summary.completed_turns, 0);
                assert_eq!(summary.physical_requests, 0);
                assert!(summary.write_bytes_per_turn_p95.is_none());
                assert!(
                    progress
                        .report
                        .storage_samples
                        .iter()
                        .all(|s| s.row_id == ROWS[1] && !s.counter_complete)
                );
                assert!(
                    progress
                        .report
                        .fsync_events
                        .iter()
                        .any(|e| !e.self_test && e.turn == 10)
                );
                totals.push(summary.durability_calls_per_turn.unwrap());
                // Exercise the real S2 completion path before a later lifecycle timeout.
                let completed = serde_json::to_value(&progress.report).unwrap();
                let completed_details = progress.details[ROWS[1]].clone();
                assert!(progress.finalized.contains(&1));
                mark_interrupted_rows(&mut progress, &config, 1);
                preserve_partial(&mut progress).unwrap();
                let interrupted = serde_json::to_value(&progress.report).unwrap();
                assert_eq!(completed["results"][1], interrupted["results"][1]);
                assert_eq!(completed["storage_summary"], interrupted["storage_summary"]);
                assert_eq!(completed_details, progress.details[ROWS[1]]);
                std::fs::remove_dir_all(root).unwrap();
            }
            assert!(
                (totals[1] - totals[0] - 11.0).abs() < 0.0001,
                "{adapter}: {totals:?}"
            );
        }
    }
}

fn attribute_turns(events: &mut [FsyncEvent], intervals: &[(u32, u64, u64)]) -> Result<()> {
    for event in events.iter_mut().filter(|event| !event.self_test) {
        for (turn, start, end) in intervals {
            if event.enter_ns >= *start && event.exit_ns <= *end {
                event.turn = *turn;
                break;
            }
            if event.enter_ns < *end && event.exit_ns > *start {
                return Err(AhrbError::Protocol(
                    "durability call straddles turn boundary".into(),
                ));
            }
        }
    }
    Ok(())
}
