//! Lifecycle storage collectors. Public verbs act only on fresh benchmark profiles.
use super::*;
use crate::storage::lifecycle::{aggregate_outcome, compaction_summary, evaluate_close};

pub(crate) struct CompactionCapture {
    pub config: StorageConfig,
    pub output: PathBuf,
    pub before: Option<accounting::SettledInventory>,
    pub after: Option<accounting::SettledInventory>,
    pub before_ns: Option<u64>,
    pub after_ns: Option<u64>,
    pub engine: Option<Arc<FakeModelEngine>>,
    pub events: Vec<Value>,
}

fn file_ref(output: &Path, name: &str) -> Result<EvidenceRef> {
    Ok(EvidenceRef {
        file: name.into(),
        sha256: format!("{:x}", Sha256::digest(std::fs::read(output.join(name))?)),
        first_record: None,
        last_record: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn boundary(
    report: &mut Report,
    config: &StorageConfig,
    id: usize,
    repetition: u32,
    ordinal: u32,
    name: &str,
    sample: &accounting::SettledInventory,
    observed_ns: Option<u64>,
) {
    report.storage_samples.push(StorageSample {
        row_id: ROWS[id].into(),
        repetition,
        session_ordinal: ordinal,
        turn: if id == 4 && ordinal != 0 { 1 } else { 0 },
        boundary: name.into(),
        monotonic_ns: observed_ns.unwrap_or_else(monotonic_timestamp_ns),
        settle_ms: sample.settle_ms,
        sync_start_ns: sample.sync_start_ns,
        sync_end_ns: sample.sync_end_ns,
        allocated_bytes: sample.inventory.allocated_bytes(),
        apparent_bytes: sample.inventory.apparent_bytes(),
        regular_files: sample.inventory.regular_files(),
        families: sample.inventory.families(config),
        physical_write_bytes: None,
        physical_read_bytes: None,
        counter_source: "not-collected".into(),
        counter_complete: false,
    });
    report
        .storage_files
        .extend(
            sample
                .inventory
                .entries
                .iter()
                .cloned()
                .map(|entry| StorageFile {
                    row_id: ROWS[id].into(),
                    repetition,
                    boundary: name.into(),
                    entry,
                }),
        );
}

async fn provider(
    progress: &mut Progress,
    id: usize,
    repetition: u32,
    engine: &Arc<FakeModelEngine>,
) -> Result<()> {
    let records = engine.request_records().await;
    for record in &records {
        let mut value = serde_json::to_value(record)?;
        value["row_id"] = json!(ROWS[id]);
        value["repetition"] = json!(repetition);
        progress.report.model_requests.push(value);
    }
    let mut mapping = std::fs::OpenOptions::new()
        .append(true)
        .open(progress.output.join("storage-request-bodies.jsonl"))?;
    for mut receipt in engine.take_storage_body_receipts()? {
        let record = records
            .iter()
            .find(|r| Some(r.received_ns) == receipt["received_ns"].as_u64());
        receipt["row_id"] = json!(ROWS[id]);
        receipt["repetition"] = json!(repetition);
        receipt["canonical_sha256"] = json!(record.map(|r| &r.canonical_hash));
        receipt["semantic_ordinal"] = json!(record.map(|r| r.semantic_ordinal));
        receipt["attempt"] = json!(record.map(|r| r.attempt));
        receipt["role"] = json!(record.map(|r| &r.role));
        writeln!(mapping, "{receipt}")?;
    }
    // The shared-task physical request headline remains scoped to the standardized N-turn task.
    Ok(())
}

fn unavailable<S: Default, D: Default>(repetition: u32, outcome: TestOutcome) -> Trial<S, D> {
    Trial {
        repetition,
        measurement_complete: matches!(outcome, TestOutcome::Unsupported(_)),
        reason: outcome_reason(&outcome),
        outcome,
        summary: S::default(),
        diagnostics: D::default(),
        evidence_refs: Vec::new(),
    }
}

fn close_prerequisite(manifest: &Manifest, config: &StorageConfig) -> Option<TestOutcome> {
    if config.session_close.as_ref().is_none_or(Vec::is_empty) {
        Some(TestOutcome::Unsupported("no-close-without-delete".into()))
    } else if [
        manifest.sessions.close_delete.as_slice(),
        manifest.sessions.delete.as_slice(),
        config.session_delete.as_deref().unwrap_or_default(),
    ]
    .iter()
    .any(|deletion| {
        if deletion.is_empty() {
            return false;
        }
        let normalize = |argv: &[String]| {
            argv.iter()
                .map(|arg| {
                    arg.replace(
                        "{{harness}}",
                        manifest
                            .availability
                            .exec_paths
                            .first()
                            .map_or("{{harness}}", String::as_str),
                    )
                })
                .collect::<Vec<_>>()
        };
        normalize(config.session_close.as_deref().unwrap_or_default()) == normalize(deletion)
    }) {
        Some(TestOutcome::Error(
            "session_close aliases a declared deleting operation".into(),
        ))
    } else if manifest.sessions.store_paths.is_empty() {
        Some(TestOutcome::Absent(
            "close declared without sessions.store_paths".into(),
        ))
    } else {
        None
    }
}
fn compaction_prerequisite(manifest: &Manifest) -> Option<TestOutcome> {
    if !manifest
        .capabilities
        .required
        .contains_key("context_limit_recovery")
        && !manifest
            .capabilities
            .optional
            .contains_key("context_limit_recovery")
    {
        Some(TestOutcome::Absent(
            "missing context_limit_recovery declaration".into(),
        ))
    } else if manifest.resources.context_window.is_none() {
        Some(TestOutcome::Absent("missing context-window binding".into()))
    } else {
        None
    }
}

/// Called after the shared task, under the same absolute storage deadline.
#[allow(clippy::too_many_arguments)]
pub(super) async fn collect(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    root: &Path,
    profile: Profile,
    repetitions: u32,
    deadline: tokio::time::Instant,
    progress: &mut Progress,
) -> Result<bool> {
    for id in [3, 4] {
        if tokio::time::Instant::now() >= deadline {
            return Ok(true);
        }
        let preflight = if id == 3 {
            compaction_prerequisite(manifest)
        } else {
            close_prerequisite(manifest, config)
        };
        if let Some(outcome) = preflight {
            progress.report.results[id] = row(id, outcome.clone(), config);
            progress.details.insert(
                ROWS[id].into(),
                pending_details(id, &outcome_reason(&outcome).unwrap_or_default()),
            );
            progress.finalized.insert(id);
            continue;
        }
        let mut trials = Vec::<Value>::new();
        let mut outcomes = Vec::new();
        let mut interrupted = false;
        for repetition in 1..=repetitions {
            if tokio::time::Instant::now() >= deadline {
                interrupted = true;
                break;
            }
            let (value, outcome) = if id == 3 {
                let mut trial: Trial<CompactionSummary, CompactionDiagnostics> =
                    unavailable(repetition, TestOutcome::Error("pending".into()));
                let mut capture = CompactionCapture {
                    config: config.clone(),
                    output: progress.output.clone(),
                    before: None,
                    after: None,
                    before_ns: None,
                    after_ns: None,
                    engine: None,
                    events: Vec::new(),
                };
                let mut result = tokio::time::timeout_at(
                    deadline,
                    compact(
                        manifest,
                        hash,
                        root,
                        profile,
                        repetition,
                        &mut capture,
                        &mut trial,
                        progress,
                    ),
                )
                .await;
                if let Some(engine) = &capture.engine {
                    if let Err(error) = provider(progress, id, repetition, engine).await {
                        if result.is_ok() {
                            result = Ok(Err(error));
                        } else {
                            progress
                                .report
                                .lifecycle_notes
                                .push(format!("provider capture during deadline: {error}"));
                        }
                    }
                }
                progress.report.events.append(&mut capture.events);
                // Capture inventories even if cancellation or a later context oracle interrupted the pair.
                if let Some(before) = &capture.before {
                    trial.diagnostics.before_boundary = Some(format!("s4-r{repetition}-before"));
                    boundary(
                        &mut progress.report,
                        config,
                        id,
                        repetition,
                        1,
                        &format!("s4-r{repetition}-before"),
                        before,
                        capture.before_ns,
                    );
                }
                if let Some(after) = &capture.after {
                    trial.diagnostics.after_boundary = Some(format!("s4-r{repetition}-after"));
                    boundary(
                        &mut progress.report,
                        config,
                        id,
                        repetition,
                        1,
                        &format!("s4-r{repetition}-after"),
                        after,
                        capture.after_ns,
                    );
                }
                apply_result(result, &mut trial, &mut interrupted);
                let outcome = trial.outcome.clone();
                (serde_json::to_value(trial)?, outcome)
            } else {
                let mut trial: Trial<CloseSummary, CloseDiagnostics> =
                    unavailable(repetition, TestOutcome::Error("pending".into()));
                let mut engine = None;
                let mut result = tokio::time::timeout_at(
                    deadline,
                    close(
                        manifest,
                        config,
                        hash,
                        root,
                        profile,
                        repetition,
                        &mut trial,
                        &mut engine,
                        progress,
                    ),
                )
                .await;
                if let Some(engine) = engine {
                    if let Err(error) = provider(progress, id, repetition, &engine).await {
                        if result.is_ok() {
                            result = Ok(Err(error));
                        } else {
                            progress
                                .report
                                .lifecycle_notes
                                .push(format!("provider capture during deadline: {error}"));
                        }
                    }
                }
                apply_result(result, &mut trial, &mut interrupted);
                let outcome = trial.outcome.clone();
                (serde_json::to_value(trial)?, outcome)
            };
            outcomes.push(outcome);
            trials.push(value);
            progress.details.insert(
                ROWS[id].into(),
                json!({"measurement_label":MEASUREMENT_LABEL,"reason":null,"trials":trials}),
            );
            ensure_owned_cleanup()?;
            if interrupted {
                break;
            }
        }
        if interrupted || tokio::time::Instant::now() >= deadline {
            return Ok(true);
        }
        let outcome = aggregate_outcome(outcomes);
        progress.report.results[id] = row(id, outcome.clone(), config);
        progress.details.get_mut(ROWS[id]).expect("inserted trials")["reason"] =
            json!(outcome_reason(&outcome));
        if !matches!(outcome, TestOutcome::Error(_) | TestOutcome::Absent(_)) {
            aggregate(progress, id, &trials, config)?;
        }
        progress.finalized.insert(id);
    }
    Ok(false)
}

fn apply_result<S: Default, D>(
    result: std::result::Result<Result<()>, tokio::time::error::Elapsed>,
    trial: &mut Trial<S, D>,
    interrupted: &mut bool,
) {
    let reason = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => format!("task-incomplete: {error}"),
        Err(_) => {
            *interrupted = true;
            "deadline".into()
        }
    };
    trial.outcome = TestOutcome::Error(reason.clone());
    trial.reason = Some(reason);
    trial.measurement_complete = false;
    trial.summary = S::default();
}

fn aggregate(
    progress: &mut Progress,
    id: usize,
    trials: &[Value],
    config: &StorageConfig,
) -> Result<()> {
    // All trials must provide a scalar; never filter missing evidence out of the median.
    let scalar = |key: &str| {
        trials
            .iter()
            .map(|t| t["summary"][key].as_f64())
            .collect::<Option<Vec<_>>>()
            .and_then(median)
    };
    let s = progress
        .report
        .storage_summary
        .as_mut()
        .ok_or_else(|| AhrbError::Protocol("missing summary".into()))?;
    if id == 3 {
        s.compaction_before_allocated_bytes = scalar("compaction_before_allocated_bytes");
        s.compaction_after_allocated_bytes = scalar("compaction_after_allocated_bytes");
        s.compaction_freed_pct = scalar("compaction_freed_pct");
    } else {
        s.closed_sessions = trials
            .iter()
            .map(|t| t["summary"]["closed_sessions"].as_u64())
            .collect::<Option<Vec<_>>>()
            .map(|v| v.iter().sum());
        s.close_retained_bytes_per_session = scalar("close_retained_bytes_per_session");
        s.close_retained_after_sweep_bytes_per_session =
            scalar("close_retained_after_sweep_bytes_per_session");
        s.retention_cap_bytes = config.retention_cap_bytes;
        s.close_retention_class = if trials
            .iter()
            .any(|t| t["summary"]["close_retention_class"].is_null())
        {
            None
        } else if trials
            .iter()
            .any(|t| t["summary"]["close_retention_class"] == "unbounded")
        {
            Some(BoundClass::Unbounded)
        } else {
            Some(BoundClass::Bounded)
        };
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn compact(
    manifest: &Manifest,
    hash: &str,
    root: &Path,
    profile: Profile,
    repetition: u32,
    capture: &mut CompactionCapture,
    trial: &mut Trial<CompactionSummary, CompactionDiagnostics>,
    progress: &mut Progress,
) -> Result<()> {
    let (history, pairs, window) = match profile {
        Profile::Quick => (16, 4, 4096),
        Profile::Cert => (128, 32, 16384),
    };
    let observation = collect_context_recovery_repetition(
        manifest,
        root,
        hash,
        repetition,
        history,
        pairs,
        window,
        Some(capture),
    )
    .await?;
    let receipt_name = format!("s4-r{repetition}-context.json");
    std::fs::write(
        progress.output.join(&receipt_name),
        serde_json::to_vec_pretty(
            &json!({"row51_trials":observation.trials,"compaction_trials":observation.compaction_trials.iter().map(|t| json!({"repetition":t.repetition,"compaction_observed":t.compaction_observed,"omitted_markers":t.omitted_markers})).collect::<Vec<_>>()}),
        )?,
    )?;
    trial
        .evidence_refs
        .push(file_ref(&progress.output, &receipt_name)?);
    trial.diagnostics.before_boundary = capture
        .before
        .as_ref()
        .map(|_| format!("s4-r{repetition}-before"));
    trial.diagnostics.after_boundary = capture
        .after
        .as_ref()
        .map(|_| format!("s4-r{repetition}-after"));
    // No-compaction is completed feasibility evidence only after the expected
    // provider error gate and terminal inventories were actually captured.
    let before = capture
        .before
        .as_ref()
        .ok_or_else(|| AhrbError::Protocol("missing gated pre-error inventory".into()))?;
    let after = capture
        .after
        .as_ref()
        .ok_or_else(|| AhrbError::Protocol("missing post-terminal inventory".into()))?;
    let compacted = observation
        .compaction_trials
        .iter()
        .any(|t| t.compaction_observed);
    if !compacted || observation.trials.is_empty() {
        trial.outcome = TestOutcome::Unsupported("no-compaction-observed".into());
        trial.reason = outcome_reason(&trial.outcome);
        trial.measurement_complete = true;
        trial.diagnostics.recovery_outcome = Some(CompactionOutcome::NotCompacted);
        return Ok(());
    }
    let context = &observation.trials[0];
    // Reuse row 51's identity/content oracle with original observed timestamps.
    // Its matrix latency threshold is not an S4 oracle: the storage provider hold
    // is deliberate. The real outer-turn and whole-run deadlines still apply.
    let mut checked = context.clone();
    checked.repetition = 1;
    let evaluated = evaluate_context_limit_recovery(&[checked], 1, window, pairs, u64::MAX);
    if !evaluated.measurement_complete || !evaluated.passed {
        return Err(AhrbError::Protocol(format!(
            "row-51 compacted candidate failed identity/content recovery judgement: {}",
            evaluated.details
        )));
    }
    trial.summary = compaction_summary(
        before.inventory.allocated_bytes(),
        after.inventory.allocated_bytes(),
    );
    trial.diagnostics.compaction_request_sha256 = Some(context.compacted_request_hash.clone());
    trial.diagnostics.recovery_outcome = Some(CompactionOutcome::Compacted);
    trial.diagnostics.accepted_input_tokens = Some(context.accepted_input_tokens);
    trial.diagnostics.accepted_body_bytes = Some(context.accepted_body_bytes);
    trial.outcome = TestOutcome::Pass;
    trial.measurement_complete = true;
    trial.reason = trial
        .summary
        .compaction_freed_pct
        .is_none()
        .then(|| "zero baseline".into());
    Ok(())
}

fn store_bytes(inventory: &accounting::Inventory, profile: &Path, roots: &[PathBuf]) -> u64 {
    inventory
        .entries
        .iter()
        .filter(|entry| {
            entry.kind == "regular"
                && roots
                    .iter()
                    .any(|r| profile.join(&entry.path).starts_with(r))
        })
        .map(|entry| entry.allocated_bytes)
        .sum()
}

#[allow(clippy::too_many_arguments)]
async fn close(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    root: &Path,
    profile: Profile,
    repetition: u32,
    trial: &mut Trial<CloseSummary, CloseDiagnostics>,
    active_engine: &mut Option<Arc<FakeModelEngine>>,
    progress: &mut Progress,
) -> Result<()> {
    let sessions = match profile {
        Profile::Quick => 20,
        Profile::Cert => 200,
    };
    let profile = root.join(format!("s5-r{repetition}"));
    prepare_profile(manifest, &profile)?;
    let workspace = profile.join("storage-workspace");
    std::fs::create_dir(&workspace)?;
    let fixture_workflow = workflow(manifest, sessions)?;
    let ScriptedPillarRuntime {
        engine,
        server,
        mut driver,
    } = start_scripted_pillar_runtime(
        manifest,
        hash,
        &fixture_workflow,
        &profile,
        "storage",
        Some(&workspace),
    )
    .await?;
    engine.enable_storage_body_capture(&progress.output.join("request-bodies"))?;
    *active_engine = Some(Arc::clone(&engine));
    // Retain the engine outside the cancelable future for interrupted body evidence.
    let result = close_sessions(
        manifest,
        config,
        &profile,
        &workspace,
        sessions,
        repetition,
        &mut driver,
        trial,
        progress,
    )
    .await;
    if result.is_ok() {
        driver.shutdown().await?;
        server.shutdown().await?;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn close_sessions(
    manifest: &Manifest,
    config: &StorageConfig,
    profile: &Path,
    workspace: &Path,
    sessions: u32,
    repetition: u32,
    driver: &mut HarnessDriver,
    trial: &mut Trial<CloseSummary, CloseDiagnostics>,
    progress: &mut Progress,
) -> Result<()> {
    let mut variables = BTreeMap::from([
        ("profile".into(), profile.to_string_lossy().into_owned()),
        ("workspace".into(), workspace.to_string_lossy().into_owned()),
    ]);
    let roots = crate::manifest::render_session_store_paths(manifest, &variables, profile)?;
    let baseline = accounting::settle(profile, config, monotonic_timestamp_ns()).await?;
    let baseline_bytes = store_bytes(&baseline.inventory, profile, &roots);
    trial.diagnostics.store_baseline_allocated_bytes = Some(baseline_bytes);
    trial.diagnostics.checkpoints.push(CloseCheckpoint {
        closed_sessions: 0,
        elapsed_s: 0.0,
        phase: ClosePhase::Immediate,
        retained_allocated_bytes: 0,
    });
    boundary(
        &mut progress.report,
        config,
        4,
        repetition,
        0,
        &format!("s5-r{repetition}-c0000"),
        &baseline,
        None,
    );
    let mut final_close_ns = None;
    let mut seeded_workspaces = BTreeSet::new();
    for ordinal in 1..=sessions {
        let session = driver
            .create_session(&format!("{TASK}:close-{ordinal:03}"))
            .await?;
        let task_workspace = if let Some(template) = &config.workspace_path {
            let mut vars = variables.clone();
            vars.insert("session_id".into(), session.0.clone());
            PathBuf::from(crate::manifest::render_template(template, &vars)?)
        } else {
            driver
                .session_workspace(&session)
                .ok_or_else(|| AhrbError::Validation("missing storage workspace locator".into()))?
        };
        validate_scripted_workspace_path(profile, &task_workspace, "storage")?;
        std::fs::create_dir_all(&task_workspace)?;
        if seeded_workspaces.insert(std::fs::canonicalize(&task_workspace)?) {
            fixture::seed(&task_workspace)?;
        }
        let key = format!("s5-r{repetition}-c{ordinal:04}");
        driver
            .submit(&session, &fixture::prompt(ordinal)?, &key)
            .await?;
        let events =
            collect_session_terminal(driver, &session, None, outer_turn_timeout(manifest)).await?;
        if events.iter().filter(|e| is_terminal(&e.event)).count() != 1
            || !events
                .iter()
                .any(|e| e.event == EventVocab::TerminalSuccess)
            || events.iter().any(|e| e.session_id != session.0)
            || events
                .iter()
                .filter(|e| e.event == EventVocab::ToolCall)
                .count()
                != usize::from(ordinal % 10 == 0)
            || events
                .iter()
                .filter(|e| e.event == EventVocab::ToolResult)
                .count()
                != usize::from(ordinal % 10 == 0)
        {
            return Err(AhrbError::Protocol(
                "S5 tiny turn lacks successful same-session terminal".into(),
            ));
        }
        for event in events {
            progress.report.events.push(serde_json::to_value(event)?);
        }
        if manifest.transport.kind == TransportKind::Exec
            && !matches!(driver.client_exit(&session), ClientExit::Exited(Some(0)))
        {
            return Err(AhrbError::Protocol(
                "S5 client has not exited successfully before public close".into(),
            ));
        }
        variables.insert(
            "workspace".into(),
            task_workspace.to_string_lossy().into_owned(),
        );
        variables.insert("session_id".into(), session.0.clone());
        let observation = public_close(manifest, config, profile, &variables).await?;
        let name = format!("s5-r{repetition}-c{ordinal:04}-close.json");
        let bytes = serde_json::to_vec_pretty(
            &json!({"argv":observation.argv,"exit_code":observation.exit_code,"stdout":String::from_utf8_lossy(&observation.stdout),"stderr":String::from_utf8_lossy(&observation.stderr),"operation_start_ns":observation.operation_start_ns,"terminal_receipt_ns":observation.terminal_receipt_ns,"outer_kill":observation.outer_kill}),
        )?;
        std::fs::write(progress.output.join(&name), &bytes)?;
        trial.evidence_refs.push(file_ref(&progress.output, &name)?);
        if observation.exit_code != Some(0)
            || observation.outer_kill
            || observation.terminal_receipt_ns.is_none()
            || (!observation.terminal.is_null()
                && !control_response_succeeded(&observation.terminal))
            || observation.terminal.get("closed").and_then(Value::as_bool) == Some(false)
            || observation.terminal.get("deleted").and_then(Value::as_bool) == Some(true)
            || observation
                .terminal
                .get("session_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id != session.0)
        {
            return Err(AhrbError::Protocol(
                "declared close failed or missing/contradictory close receipt".into(),
            ));
        }
        let closed_ns = observation.terminal_receipt_ns.expect("validated receipt");
        final_close_ns = Some(closed_ns);
        trial.diagnostics.close_receipts.push(CloseReceipt {
            session_id_hash: stable_evidence_hash(&session.0),
            exit_code: 0,
            receipt_sha256: format!("{:x}", Sha256::digest(&bytes)),
        });
        if ordinal % 10 == 0 || ordinal == sessions {
            let sample = accounting::settle(profile, config, closed_ns).await?;
            trial.diagnostics.checkpoints.push(CloseCheckpoint {
                closed_sessions: ordinal as u64,
                elapsed_s: monotonic_timestamp_ns().saturating_sub(closed_ns) as f64 / 1e9,
                phase: ClosePhase::Immediate,
                retained_allocated_bytes: store_bytes(&sample.inventory, profile, &roots)
                    .saturating_sub(baseline_bytes),
            });
            boundary(
                &mut progress.report,
                config,
                4,
                repetition,
                ordinal,
                &key,
                &sample,
                None,
            );
        }
    }
    if let Some(interval) = config.sweep_interval_s {
        let closed =
            final_close_ns.ok_or_else(|| AhrbError::Protocol("missing final close".into()))?;
        let elapsed = monotonic_timestamp_ns().saturating_sub(closed);
        let interval_ns = interval
            .checked_mul(1_000_000_000)
            .ok_or_else(|| AhrbError::Validation("sweep interval overflow".into()))?;
        tokio::time::sleep(Duration::from_nanos(interval_ns.saturating_sub(elapsed))).await;
        let sample = accounting::settle(profile, config, monotonic_timestamp_ns()).await?;
        trial.diagnostics.checkpoints.push(CloseCheckpoint {
            closed_sessions: sessions as u64,
            elapsed_s: monotonic_timestamp_ns().saturating_sub(closed) as f64 / 1e9,
            phase: ClosePhase::PostSweep,
            retained_allocated_bytes: store_bytes(&sample.inventory, profile, &roots)
                .saturating_sub(baseline_bytes),
        });
        boundary(
            &mut progress.report,
            config,
            4,
            repetition,
            sessions,
            &format!("s5-r{repetition}-post-sweep"),
            &sample,
            None,
        );
    }
    let (summary, outcome) = evaluate_close(config, sessions as u64, &trial.diagnostics)?;
    trial.summary = summary;
    trial.reason = outcome_reason(&outcome);
    trial.outcome = outcome;
    trial.measurement_complete = true;
    Ok(())
}

async fn public_close(
    manifest: &Manifest,
    config: &StorageConfig,
    profile: &Path,
    variables: &BTreeMap<String, String>,
) -> Result<DirectCommandObservation> {
    let mut variables = variables.clone();
    let executable = manifest
        .availability
        .exec_paths
        .iter()
        .find_map(|p| {
            let path = PathBuf::from(p);
            if path.is_file() {
                return std::fs::canonicalize(path).ok();
            }
            std::env::var_os("PATH")
                .into_iter()
                .flat_map(|v| std::env::split_paths(&v).collect::<Vec<_>>())
                .map(|dir| dir.join(p))
                .find(|p| p.is_file())
        })
        .ok_or_else(|| {
            AhrbError::Validation("storage public verb executable unavailable".into())
        })?;
    variables.insert("harness".into(), executable.to_string_lossy().into_owned());
    let session = variables
        .get("session_id")
        .ok_or_else(|| AhrbError::Validation("missing session identity".into()))?;
    if session.contains(['/', '\\', '\0', '\n', '\r']) || matches!(session.as_str(), "." | "..") {
        return Err(AhrbError::Validation(
            "unsafe session identity in public close".into(),
        ));
    }
    let argv = render_argv(
        config.session_close.as_deref().unwrap_or_default(),
        &variables,
    )?;
    for arg in argv.iter().skip(1) {
        let value = arg.split_once('=').map_or(arg.as_str(), |(_, v)| v);
        if Path::new(value).is_absolute() {
            validate_close_path(profile, Path::new(value))?;
        }
    }
    let mut environment = isolated_environment(manifest, &variables)?;
    // Fake-only behavior knobs are forwarded to this env-cleared public command;
    // driver launches already inherit them. No provider credential is needed to close.
    for key in [
        "AHRB_MOCK_STORAGE_CLOSE_RETENTION",
        "AHRB_MOCK_STORAGE_SWEEP",
    ] {
        if let Ok(value) = std::env::var(key) {
            environment.insert(key.into(), value);
        }
    }
    let argv = resolve_local_program(&argv)?;
    for deletion in [
        manifest.sessions.close_delete.as_slice(),
        manifest.sessions.delete.as_slice(),
        config.session_delete.as_deref().unwrap_or_default(),
    ] {
        if !deletion.is_empty() {
            if let Ok(command) =
                render_argv(deletion, &variables).and_then(|v| resolve_local_program(&v))
            {
                if command == argv {
                    return Err(AhrbError::Validation(
                        "session_close aliases a declared deleting operation".into(),
                    ));
                }
            }
        }
    }
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| AhrbError::Validation("empty public close argv".into()))?;
    let mut command = tokio::process::Command::new(program);
    command
        .args(args)
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
        .ok_or_else(|| AhrbError::Protocol("public close has no launch PID".into()))?;
    crate::process::register_child(&child)?;
    let output = tokio::time::timeout(outer_turn_timeout(manifest), child.wait_with_output()).await;
    crate::process::retire_process(pid)?;
    let output =
        output.map_err(|_| AhrbError::Timeout("public close command deadline".into()))??;
    let terminal_receipt_ns = Some(monotonic_timestamp_ns());
    if output.stdout.len() > manifest.resources.max_output_bytes
        || output.stderr.len() > manifest.resources.max_output_bytes
    {
        return Err(AhrbError::Protocol(
            "public close output exceeds capture limit".into(),
        ));
    }
    let terminal = output
        .stdout
        .split(|b| *b == b'\n')
        .rev()
        .find(|line| line.iter().any(|b| !b.is_ascii_whitespace()))
        .and_then(|line| serde_json::from_slice(line).ok())
        .unwrap_or(Value::Null);
    Ok(DirectCommandObservation {
        argv,
        exit_code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
        operation_start_ns,
        terminal_receipt_ns,
        terminal,
        outer_kill: false,
    })
}

fn validate_close_path(profile: &Path, path: &Path) -> Result<()> {
    // A public verb may take the disposable profile itself as its state root;
    // only actor workspaces are required to be nonempty children of that root.
    if path == profile {
        if !std::fs::symlink_metadata(profile)?.file_type().is_dir() {
            return Err(AhrbError::Validation(
                "storage public close profile must be a real directory".into(),
            ));
        }
        Ok(())
    } else {
        validate_scripted_workspace_path(profile, path, "storage public close")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_close_accepts_profile_root_and_rejects_escapes() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "ahrb-close-path-{}-{}",
            std::process::id(),
            monotonic_timestamp_ns()
        ));
        std::fs::create_dir(&root)?;
        validate_close_path(&root, &root)?;
        validate_close_path(&root, &root.join("state"))?;
        assert!(validate_close_path(&root, &root.join("../outside")).is_err());
        assert!(validate_close_path(&root, root.parent().unwrap()).is_err());
        #[cfg(unix)]
        {
            let link = root.join("link");
            std::os::unix::fs::symlink(&root, &link)?;
            assert!(validate_close_path(&root, &link).is_err());
            assert!(validate_close_path(&link, &link).is_err());
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    // Sampler unit tests intentionally observe the test runner itself. Keep real
    // lifecycle drivers and their process-wide cleanup in a fresh test process,
    // so their ownership registry contains only this fixture's children.
    async fn run_in_isolated_test_process(
        name: &str,
        environment: &[(&str, &str)],
    ) -> Result<bool> {
        const CHILD: &str = "AHRB_L4_LIFECYCLE_TEST_CHILD";
        if std::env::var(CHILD).as_deref() == Ok(name) {
            return Ok(false);
        }
        let mut command = tokio::process::Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                &format!("runner::storage::lifecycle::tests::{name}"),
                "--test-threads=1",
                "--nocapture",
            ])
            .env(CHILD, name)
            .envs(environment.iter().copied())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let output = tokio::time::timeout(Duration::from_secs(120), command.output())
            .await
            .map_err(|_| AhrbError::Timeout("isolated lifecycle test deadline".into()))??;
        assert!(
            output.status.success(),
            "isolated {name}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        print!("{}", String::from_utf8_lossy(&output.stdout));
        Ok(true)
    }

    #[tokio::test]
    async fn close_completes_twenty_sessions_in_reused_workspace_on_both_transports() -> Result<()>
    {
        if run_in_isolated_test_process(
            "close_completes_twenty_sessions_in_reused_workspace_on_both_transports",
            &[],
        )
        .await?
        {
            return Ok(());
        }
        for adapter in ["mock", "mock-exec"] {
            let manifest =
                crate::manifest::load(Path::new(&format!("adapters/{adapter}/manifest.toml")))?;
            let mut config = manifest.storage.clone().expect("mock storage declaration");
            config.sweep_interval_s = None;
            let base = std::fs::canonicalize(std::env::temp_dir())?.join(format!(
                "ahrb-s5-workspace-{adapter}-{}-{}",
                std::process::id(),
                monotonic_timestamp_ns()
            ));
            let output = base.join("output");
            std::fs::create_dir_all(&output)?;
            let mut progress = Progress {
                output,
                finalized: BTreeSet::new(),
                active_engine: None,
                report: Report::default(),
                write_trials: Vec::new(),
                curve_trials: Vec::new(),
                auxiliary_trials: Vec::new(),
                retention_trials: Vec::new(),
                details: BTreeMap::new(),
            };
            let mut trial = unavailable(1, TestOutcome::Error("pending".into()));
            let result = close(
                &manifest,
                &config,
                &crate::manifest::hash(&manifest)?,
                &base.join("profiles"),
                Profile::Quick,
                1,
                &mut trial,
                &mut None,
                &mut progress,
            )
            .await;
            ensure_owned_cleanup()?;
            result?;
            assert_eq!(
                trial.outcome,
                TestOutcome::Pass,
                "{adapter}: {:?}",
                trial.reason
            );
            assert_eq!(trial.summary.closed_sessions, Some(20));
            assert_eq!(trial.diagnostics.close_receipts.len(), 20);
            assert_eq!(trial.diagnostics.checkpoints.len(), 3);
            std::fs::remove_dir_all(base)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn real_context_fixture_without_accepted_candidate_is_unsupported_not_zero_pass()
    -> Result<()> {
        if run_in_isolated_test_process(
            "real_context_fixture_without_accepted_candidate_is_unsupported_not_zero_pass",
            &[],
        )
        .await?
        {
            return Ok(());
        }
        // Keep the verifier's external fault-injection command usable: its failed
        // stimulus must now report ERROR instead of asserting completed feasibility.
        let failed_stimulus =
            std::env::var("AHRB_MOCK_STORAGE_COMPACTION_DISK").as_deref() == Ok("invalid");
        check_context_fixture_without_accepted_candidate(true, failed_stimulus).await
    }

    #[tokio::test]
    async fn failed_context_stimulus_is_error_with_incomplete_aggregates() -> Result<()> {
        if run_in_isolated_test_process(
            "failed_context_stimulus_is_error_with_incomplete_aggregates",
            &[("AHRB_MOCK_STORAGE_COMPACTION_DISK", "invalid")],
        )
        .await?
        {
            return Ok(());
        }
        check_context_fixture_without_accepted_candidate(false, true).await
    }

    async fn check_context_fixture_without_accepted_candidate(
        disable_compaction: bool,
        failed_stimulus: bool,
    ) -> Result<()> {
        let mut manifest = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))?;
        if disable_compaction {
            manifest
                .transport
                .command
                .push("--disable-compaction".into());
        }
        let base = std::fs::canonicalize(std::env::temp_dir())?.join(format!(
            "ahrb-s4-no-candidate-{}-{}",
            std::process::id(),
            monotonic_timestamp_ns()
        ));
        let output = base.join("output");
        std::fs::create_dir_all(&output)?;
        let mut progress = Progress {
            output: output.clone(),
            finalized: BTreeSet::new(),
            active_engine: None,
            report: Report::default(),
            write_trials: Vec::new(),
            curve_trials: Vec::new(),
            auxiliary_trials: Vec::new(),
            retention_trials: Vec::new(),
            details: BTreeMap::new(),
        };
        let mut capture = CompactionCapture {
            config: StorageConfig::default(),
            output,
            before: None,
            after: None,
            before_ns: None,
            after_ns: None,
            engine: None,
            events: Vec::new(),
        };
        let mut trial: Trial<CompactionSummary, CompactionDiagnostics> =
            unavailable(1, TestOutcome::Error("pending".into()));
        let result = compact(
            &manifest,
            &crate::manifest::hash(&manifest)?,
            &base.join("profiles"),
            Profile::Quick,
            1,
            &mut capture,
            &mut trial,
            &mut progress,
        )
        .await;
        ensure_owned_cleanup()?;
        let mut interrupted = false;
        apply_result(Ok(result), &mut trial, &mut interrupted);
        assert!(!interrupted);
        if failed_stimulus {
            assert!(
                matches!(trial.outcome, TestOutcome::Error(ref reason) if reason.contains("missing provider context-error record")),
                "unexpected failed-stimulus outcome: {:?}",
                trial.outcome
            );
            assert!(!trial.measurement_complete);
            assert!(trial.diagnostics.recovery_outcome.is_none());
            assert!(capture.before.is_none());
            let requests = capture.engine.as_ref().unwrap().request_records().await;
            assert!(!requests.iter().any(|r| r.response_status == Some(400)));
            assert!(
                capture
                    .events
                    .iter()
                    .any(|e| e["event"] == "terminal-failure")
            );
        } else {
            assert!(
                matches!(trial.outcome,TestOutcome::Unsupported(ref reason) if reason == "no-compaction-observed")
            );
            assert!(trial.measurement_complete);
            assert_eq!(
                trial.diagnostics.recovery_outcome,
                Some(CompactionOutcome::NotCompacted)
            );
            assert!(
                capture.before.is_some(),
                "the actual context error was gated"
            );
        }
        assert!(trial.summary.compaction_before_allocated_bytes.is_none());
        assert!(trial.summary.compaction_after_allocated_bytes.is_none());
        assert!(trial.summary.compaction_freed_pct.is_none());
        assert!(capture.after.is_some(), "the failed terminal was observed");
        assert!(
            !capture.events.is_empty(),
            "completed turn evidence survives independently of the collector result"
        );
        let value = serde_json::to_value(&trial)?;
        progress.report.storage_summary = Some(StorageSummary::default());
        aggregate(
            &mut progress,
            3,
            std::slice::from_ref(&value),
            &capture.config,
        )?;
        let summary = progress.report.storage_summary.as_ref().unwrap();
        assert!(summary.compaction_before_allocated_bytes.is_none());
        assert!(summary.compaction_after_allocated_bytes.is_none());
        assert!(summary.compaction_freed_pct.is_none());
        println!("S4 observed trial: {value}");
        std::fs::remove_dir_all(base)?;
        Ok(())
    }

    #[test]
    fn missing_close_and_store_follow_distinct_feasibility_rules() {
        let mut manifest = crate::manifest::load(Path::new("adapters/mock/manifest.toml")).unwrap();
        let mut config = manifest.storage.clone().unwrap();
        config.session_close = None;
        assert!(matches!(
            close_prerequisite(&manifest, &config),
            Some(TestOutcome::Unsupported(_))
        ));
        config.session_close = manifest.storage.as_ref().unwrap().session_close.clone();
        manifest.sessions.close_delete = config.session_close.clone().unwrap();
        assert!(matches!(
            close_prerequisite(&manifest, &config),
            Some(TestOutcome::Error(_))
        ));
        manifest.sessions.close_delete.clear();
        manifest.sessions.store_paths.clear();
        assert!(matches!(
            close_prerequisite(&manifest, &config),
            Some(TestOutcome::Absent(_))
        ));
        manifest
            .capabilities
            .required
            .remove("context_limit_recovery");
        assert!(matches!(
            compaction_prerequisite(&manifest),
            Some(TestOutcome::Absent(_))
        ));
    }

    #[test]
    fn incomplete_pair_and_missing_repetition_scalars_do_not_aggregate_to_zero() {
        let config = StorageConfig::default();
        let mut progress = Progress {
            output: PathBuf::new(),
            finalized: BTreeSet::new(),
            active_engine: None,
            report: Report {
                storage_summary: Some(StorageSummary::default()),
                ..Default::default()
            },
            write_trials: Vec::new(),
            curve_trials: Vec::new(),
            auxiliary_trials: Vec::new(),
            retention_trials: Vec::new(),
            details: BTreeMap::new(),
        };
        let positive = json!({"summary":compaction_summary(100, 50)});
        let unavailable = json!({"summary":CompactionSummary::default()});
        aggregate(&mut progress, 3, &[positive, unavailable], &config).unwrap();
        let s = progress.report.storage_summary.as_ref().unwrap();
        assert!(s.compaction_before_allocated_bytes.is_none());
        assert!(s.compaction_after_allocated_bytes.is_none());
        assert!(s.compaction_freed_pct.is_none());
    }

    #[test]
    fn store_scope_is_a_union_and_does_not_count_workspaces_or_metadata() {
        let profile = Path::new("/disposable");
        let mut inventory = accounting::Inventory::default();
        for (path, kind, bytes) in [
            ("state/sessions/a/file", "regular", 4096),
            ("workspace/file", "regular", 8192),
            ("state/sessions/a", "directory", 4096),
        ] {
            inventory.entries.push(accounting::FileEntry {
                path: path.into(),
                kind: kind.into(),
                device_id: 1,
                inode_or_file_id: 2,
                allocated_bytes: bytes,
                apparent_bytes: 1,
                sha256: None,
                family: "other".into(),
            });
        }
        assert_eq!(
            store_bytes(
                &inventory,
                profile,
                &[profile.join("state"), profile.join("state/sessions")]
            ),
            4096
        );
    }
}
