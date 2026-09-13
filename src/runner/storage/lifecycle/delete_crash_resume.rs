//! Independent disposable-profile delete, crash and resume experiments.
use super::*;
use crate::storage::lifecycle::{contained_path, declared_outcome, render_verb, residue};

fn vars(
    profile: &Path,
    workspace: &Path,
    session: &crate::driver::SessionId,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("profile".into(), profile.to_string_lossy().into_owned()),
        ("workspace".into(), workspace.to_string_lossy().into_owned()),
        ("session_id".into(), session.0.clone()),
    ])
}

fn boundary(
    progress: &mut Progress,
    config: &StorageConfig,
    row_id: usize,
    rep: u32,
    turn: u32,
    name: &str,
    b: &accounting::SettledInventory,
) {
    let samples = progress.report.storage_samples.len();
    let files = progress.report.storage_files.len();
    record_boundary(
        &mut progress.report,
        config,
        rep,
        turn,
        b,
        &Counters::default(),
    );
    for sample in &mut progress.report.storage_samples[samples..] {
        sample.row_id = ROWS[row_id].into();
        sample.boundary = name.into();
        sample.physical_read_bytes = None;
        sample.physical_write_bytes = None;
        sample.counter_complete = false;
        sample.counter_source = "unavailable: inventory-only boundary".into();
    }
    for file in &mut progress.report.storage_files[files..] {
        file.row_id = ROWS[row_id].into();
        file.boundary = name.into();
    }
}

struct SessionRun {
    runtime: ScriptedPillarRuntime,
    profile: PathBuf,
    workspace: PathBuf,
    session: crate::driver::SessionId,
    after: Option<Cursor>,
    empty_store: accounting::SettledInventory,
}

async fn start(
    manifest: &Manifest,
    hash: &str,
    flow: &Workflow,
    root: &Path,
    label: &str,
    output: &Path,
) -> Result<SessionRun> {
    std::fs::create_dir_all(root)?;
    let profile = std::fs::canonicalize(root)?.join(label);
    prepare_profile(manifest, &profile)?;
    let workspace = profile.join("storage-workspace");
    std::fs::create_dir(&workspace)?;
    fixture::seed(&workspace)?;
    let engine = Arc::new(FakeModelEngine::with_request_roles(
        flow,
        &manifest.model_roles,
        &manifest.request_role_rules,
    )?);
    engine.enable_storage_body_capture(&output.join("request-bodies"))?;
    let mut runtime = start_scripted_pillar_runtime_with_engine(
        manifest,
        hash,
        flow,
        &profile,
        "storage",
        Some(&workspace),
        engine,
        true,
    )
    .await?;
    let empty_store = accounting::settle(
        &profile,
        &manifest.storage.clone().unwrap_or_default(),
        monotonic_timestamp_ns(),
    )
    .await?;
    let session = runtime
        .driver
        .create_session(&format!("{TASK}:storage"))
        .await?;
    let actual = if let Some(template) = manifest
        .storage
        .as_ref()
        .and_then(|s| s.workspace_path.as_ref())
    {
        PathBuf::from(crate::manifest::render_template(
            template,
            &vars(&profile, &workspace, &session),
        )?)
    } else {
        runtime
            .driver
            .session_workspace(&session)
            .ok_or_else(|| AhrbError::Validation("missing storage workspace locator".into()))?
    };
    contained_path(&profile, &actual)?;
    if actual != workspace {
        std::fs::create_dir_all(&actual)?;
        fixture::seed(&actual)?;
    }
    Ok(SessionRun {
        runtime,
        profile,
        workspace: actual,
        session,
        after: None,
        empty_store,
    })
}

async fn grow(
    run: &mut SessionRun,
    manifest: &Manifest,
    n: u32,
    progress: &mut Progress,
    rep: u32,
    row_id: usize,
) -> Result<()> {
    for turn in 1..=n {
        let count = run.runtime.driver.completed_turn_boundaries().len();
        run.runtime
            .driver
            .submit(
                &run.session,
                &fixture::prompt(turn)?,
                &format!("storage-t{turn:04}"),
            )
            .await?;
        let events = collect_session_terminal(
            &mut run.runtime.driver,
            &run.session,
            run.after,
            outer_turn_timeout(manifest),
        )
        .await?;
        if events.iter().filter(|e| is_terminal(&e.event)).count() != 1
            || !events
                .iter()
                .any(|e| e.event == EventVocab::TerminalSuccess)
        {
            return Err(AhrbError::Protocol(
                "storage lifecycle preparation task-incomplete".into(),
            ));
        }
        run.after = events.iter().map(|e| Cursor(e.cursor)).max().or(run.after);
        if manifest.transport.kind == TransportKind::Exec {
            await_completed_turn_boundary(
                &mut run.runtime.driver,
                &run.session,
                run.after,
                count,
                outer_turn_timeout(manifest),
            )
            .await?;
        }
        progress.report.events.extend(
            events
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        );
    }
    // Preparation is not a read/write measurement. Settle the final durable state;
    // the S6/S9/S10 measurement boundaries each have their own full settle window.
    let settled = accounting::settle(
        &run.profile,
        &manifest.storage.clone().unwrap_or_default(),
        monotonic_timestamp_ns(),
    )
    .await?;
    boundary(
        progress,
        &manifest.storage.clone().unwrap_or_default(),
        row_id,
        rep,
        n,
        &format!("s{}-r{rep}-prepared", row_id + 1),
        &settled,
    );
    Ok(())
}

async fn provider_evidence(
    run: &SessionRun,
    progress: &mut Progress,
    rep: u32,
    row_id: usize,
) -> Result<()> {
    for r in run.runtime.engine.request_records().await {
        let mut v = serde_json::to_value(r)?;
        v["row_id"] = json!(ROWS[row_id]);
        v["repetition"] = json!(rep);
        progress.report.model_requests.push(v);
    }
    let mut mapping = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(progress.output.join("storage-request-bodies.jsonl"))?;
    for mut receipt in run.runtime.engine.take_storage_body_receipts()? {
        receipt["row_id"] = json!(ROWS[row_id]);
        receipt["repetition"] = json!(rep);
        writeln!(mapping, "{receipt}")?;
    }
    Ok(())
}

fn missing_operation(operation: OperationKind) -> Operation {
    let reason = match operation {
        OperationKind::SessionDelete => "no way to delete a session",
        OperationKind::UninstallCleanup => "no disposable-profile uninstall cleanup",
    }
    .to_string();
    Operation {
        repetition: None,
        operation,
        declared: false,
        scope: vec![],
        baseline_boundary: None,
        after_boundary: None,
        exit_code: None,
        receipt_sha256: None,
        outcome: TestOutcome::Unsupported(reason.clone()),
        reason: Some(reason),
        residue_paths: vec![],
        whole_root_residue_allocated_bytes: None,
        whole_root_residue_files: None,
    }
}

async fn execute_verb(
    manifest: &Manifest,
    run: &SessionRun,
    name: &str,
    argv: &[String],
    output: &Path,
) -> Result<(i32, String)> {
    let mut variables = vars(&run.profile, &run.workspace, &run.session);
    let mut executable = None;
    for candidate in &manifest.availability.exec_paths {
        let resolved = resolve_local_program(std::slice::from_ref(candidate))?;
        if let Some(path) = crate::manifest::resolve_executable(&resolved[0]) {
            executable = Some(std::fs::canonicalize(path)?);
            break;
        }
    }
    let executable = executable.ok_or_else(|| {
        AhrbError::Validation("storage verb has no discovered harness executable".into())
    })?;
    variables.insert("harness".into(), executable.to_string_lossy().into_owned());
    let argv = resolve_local_program(&render_verb(name, argv, &variables, &run.profile)?)?;
    // Public verbs can derive store paths from a profile-only argument. Check
    // the complete profile immediately before execution, not only rendered argv.
    accounting::inventory(
        &run.profile,
        &manifest.storage.clone().unwrap_or_default(),
        false,
    )?;
    let mut env = isolated_environment(manifest, &variables)?;
    // Never inherit a real provider/account environment into destructive verbs.
    for name in [
        "PATH",
        "AHRB_MOCK_STORAGE_DELETE_MODE",
        "AHRB_MOCK_STORAGE_UNINSTALL_MODE",
    ] {
        if let Ok(v) = std::env::var(name) {
            env.insert(name.into(), v);
        }
    }
    let mut command = tokio::process::Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .env_clear()
        .envs(env)
        .current_dir(&run.profile)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn()?;
    crate::process::register_child(&child)?;
    let pid = child
        .id()
        .ok_or_else(|| AhrbError::Protocol("storage verb has no owned PID".into()))?;
    let started = monotonic_timestamp_ns();
    let result = tokio::time::timeout(outer_turn_timeout(manifest), child.wait_with_output())
        .await
        .map_err(|_| AhrbError::Timeout("storage verb deadline".into()))??;
    crate::process::retire_process(pid)?;
    let code = result.status.code().unwrap_or(-1);
    let receipt = json!({"argv":argv,"pid":pid,"start_ns":started,"exit_ns":monotonic_timestamp_ns(),"exit_code":code,"stdout":String::from_utf8_lossy(&result.stdout),"stderr":String::from_utf8_lossy(&result.stderr)});
    let bytes = serde_json::to_vec(&receipt)?;
    std::fs::write(output, &bytes)?;
    Ok((code, format!("{:x}", Sha256::digest(bytes))))
}

#[allow(clippy::too_many_arguments)]
async fn delete_operation(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    flow: &Workflow,
    root: &Path,
    rep: u32,
    n: u32,
    kind: OperationKind,
    argv: &[String],
    progress: &mut Progress,
) -> Result<(Operation, DeleteSummary)> {
    let label = if kind == OperationKind::SessionDelete {
        "session-delete"
    } else {
        "uninstall-cleanup"
    };
    let mut op = missing_operation(kind);
    op.declared = true;
    op.repetition = Some(rep);
    op.reason = None;
    let mut summary = DeleteSummary::default();
    if kind == OperationKind::UninstallCleanup
        && manifest.daemon.persistent
        && manifest.daemon.shutdown.is_empty()
    {
        op.outcome =
            TestOutcome::Unsupported("no declared daemon shutdown before uninstall".into());
        op.reason = outcome_reason(&op.outcome);
        return Ok((op, summary));
    }
    if kind == OperationKind::SessionDelete && manifest.sessions.store_paths.is_empty() {
        op.outcome = TestOutcome::Absent("declared delete has no sessions.store_paths".into());
        op.reason = outcome_reason(&op.outcome);
        return Ok((op, summary));
    }
    let mut run = start(
        manifest,
        hash,
        flow,
        root,
        &format!("s6-{label}-r{rep}"),
        &progress.output,
    )
    .await?;
    let result = async {
        if kind==OperationKind::UninstallCleanup && run.workspace!=run.profile.join("storage-workspace") {
            return Ok(TestOutcome::Unsupported("uninstall-baseline-unavailable: public workspace cannot be seeded before harness initialization".into()));
        }
        let variables = vars(&run.profile, &run.workspace, &run.session);
        op.scope = if kind == OperationKind::SessionDelete {
            manifest
                .sessions
                .store_paths
                .iter()
                .map(|p| {
                    let path = PathBuf::from(crate::manifest::render_template(p, &variables)?);
                    Ok(contained_path(&run.profile, &path)?
                        .to_string_lossy()
                        .into_owned())
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![String::new()]
        };
        let before = if kind == OperationKind::SessionDelete {
            run.empty_store.clone()
        } else {
            run.runtime
                .storage_pre_start
                .clone()
                .ok_or_else(|| AhrbError::Protocol("missing pre-initialization baseline".into()))?
        };
        let baseline_name = format!("s6-{label}-r{rep}-baseline");
        boundary(progress, config, 5, rep, 0, &baseline_name, &before);
        op.baseline_boundary = Some(baseline_name);
        grow(&mut run, manifest, n, progress, rep, 5).await?;
        // Delete uses the live public surface. Only uninstall stops writers first;
        // close and generic teardown could erase the residue being measured.
        if kind == OperationKind::UninstallCleanup {
            run.runtime.driver.shutdown().await?;
        }
        let name = if kind == OperationKind::SessionDelete {
            "session_delete"
        } else {
            "uninstall_cleanup"
        };
        let receipt_file = progress
            .output
            .join(format!("s6-{label}-r{rep}-command.json"));
        let (exit, receipt) = execute_verb(manifest, &run, name, argv, &receipt_file).await?;
        op.exit_code = Some(exit);
        op.receipt_sha256 = Some(receipt);
        if exit != 0 {
            return Err(AhrbError::Protocol(format!(
                "declared {label} exited {exit}"
            )));
        }
        contained_path(&run.profile, &run.profile)?;
        let after = accounting::settle(&run.profile, config, monotonic_timestamp_ns()).await?;
        let after_name = format!("s6-{label}-r{rep}-after");
        boundary(progress, config, 5, rep, n, &after_name, &after);
        op.after_boundary = Some(after_name);
        let leftovers = residue(&before.inventory, &after.inventory, &op.scope);
        let bytes = leftovers.iter().map(|f| f.allocated_bytes).sum::<u64>();
        let files = leftovers.len() as u64;
        op.residue_paths = leftovers.iter().map(|f| f.path.clone()).collect();
        let whole = residue(&before.inventory, &after.inventory, &[String::new()]);
        op.whole_root_residue_allocated_bytes = Some(whole.iter().map(|f| f.allocated_bytes).sum());
        op.whole_root_residue_files = Some(whole.len() as u64);
        if kind == OperationKind::SessionDelete {
            summary.delete_residue_allocated_bytes = Some(bytes);
            summary.delete_residue_files = Some(files);
        } else {
            summary.uninstall_residue_allocated_bytes = Some(bytes);
            summary.uninstall_residue_files = Some(files);
        }
        Ok(if bytes > 0 || files > 0 {
            TestOutcome::Fail(format!(
                "declared {label} left {files} regular files ({bytes} allocated bytes)"
            ))
        } else {
            TestOutcome::Pass
        })
    }
    .await;
    provider_evidence(&run, progress, rep, 5).await?;
    let _ = run.runtime.driver.shutdown().await;
    run.runtime.server.shutdown().await?;
    op.outcome = result.unwrap_or_else(|e| TestOutcome::Error(e.to_string()));
    op.reason = outcome_reason(&op.outcome);
    Ok((op, summary))
}

pub(in crate::runner::storage) fn missing_operations(config: &StorageConfig) -> Vec<Operation> {
    [
        (
            OperationKind::SessionDelete,
            config.session_delete.as_deref().unwrap_or(&[]),
        ),
        (
            OperationKind::UninstallCleanup,
            config.uninstall_cleanup.as_deref().unwrap_or(&[]),
        ),
    ]
    .into_iter()
    .filter(|(_, argv)| argv.is_empty())
    .map(|(kind, _)| missing_operation(kind))
    .collect()
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runner::storage) async fn collect_delete(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    flow: &Workflow,
    root: &Path,
    reps: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<()> {
    let mut details = DeleteDetails {
        measurement_label: MEASUREMENT_LABEL.into(),
        reason: None,
        trials: vec![],
        operations: missing_operations(config),
    };
    if config.delete_declared() {
        for rep in 1..=reps {
            let mut summary = DeleteSummary::default();
            let mut refs = Vec::new();
            let mut command_refs = Vec::new();
            for (kind, argv) in [
                (
                    OperationKind::SessionDelete,
                    config.session_delete.as_deref().unwrap_or(&[]),
                ),
                (
                    OperationKind::UninstallCleanup,
                    config.uninstall_cleanup.as_deref().unwrap_or(&[]),
                ),
            ] {
                if argv.is_empty() {
                    continue;
                }
                let (op, values) = match delete_operation(
                    manifest, config, hash, flow, root, rep, n, kind, argv, progress,
                )
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let mut op = missing_operation(kind);
                        op.declared = true;
                        op.repetition = Some(rep);
                        op.outcome = TestOutcome::Error(e.to_string());
                        op.reason = outcome_reason(&op.outcome);
                        (op, DeleteSummary::default())
                    }
                };
                if kind == OperationKind::SessionDelete {
                    summary.delete_residue_allocated_bytes = values.delete_residue_allocated_bytes;
                    summary.delete_residue_files = values.delete_residue_files;
                } else {
                    summary.uninstall_residue_allocated_bytes =
                        values.uninstall_residue_allocated_bytes;
                    summary.uninstall_residue_files = values.uninstall_residue_files;
                }
                if let Some(sha256) = op.receipt_sha256.clone() {
                    let label = if kind == OperationKind::SessionDelete {
                        "session-delete"
                    } else {
                        "uninstall-cleanup"
                    };
                    command_refs.push(EvidenceRef {
                        file: format!("s6-{label}-r{rep}-command.json"),
                        sha256,
                        first_record: None,
                        last_record: None,
                    });
                }
                refs.push(details.operations.len() as u64);
                details.operations.push(op);
                progress
                    .details
                    .insert(ROWS[5].into(), serde_json::to_value(&details)?);
            }
            let outcome = declared_outcome(
                refs.iter()
                    .map(|i| &details.operations[*i as usize].outcome),
            );
            details.trials.push(Trial {
                repetition: rep,
                measurement_complete: !matches!(
                    outcome,
                    TestOutcome::Error(_) | TestOutcome::Absent(_)
                ),
                reason: outcome_reason(&outcome),
                outcome,
                summary,
                diagnostics: DeleteDiagnostics {
                    operation_refs: refs,
                },
                evidence_refs: command_refs,
            });
            progress
                .details
                .insert(ROWS[5].into(), serde_json::to_value(&details)?);
        }
    }
    let outcome = declared_outcome(
        details
            .operations
            .iter()
            .filter(|o| o.declared)
            .map(|o| &o.outcome),
    );
    details.reason = outcome_reason(&outcome);
    if details.trials.len() == reps as usize {
        let s = progress.report.storage_summary.as_mut().unwrap();
        s.delete_residue_allocated_bytes = details
            .trials
            .iter()
            .map(|t| t.summary.delete_residue_allocated_bytes)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
        s.delete_residue_files = details
            .trials
            .iter()
            .map(|t| t.summary.delete_residue_files)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
        s.uninstall_residue_allocated_bytes = details
            .trials
            .iter()
            .map(|t| t.summary.uninstall_residue_allocated_bytes)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
        s.uninstall_residue_files = details
            .trials
            .iter()
            .map(|t| t.summary.uninstall_residue_files)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
    }
    progress.report.results[5] = row(5, outcome, config);
    progress
        .details
        .insert(ROWS[5].into(), serde_json::to_value(details)?);
    Ok(())
}

fn held_workflow(manifest: &Manifest, n: u32, checkpoint: &str) -> Result<Workflow> {
    let mut flow = workflow(manifest, n)?;
    flow.responses.push(ScriptedResponse {
        scenario: TASK.into(),
        actor: "storage".into(),
        checkpoint: checkpoint.into(),
        request_hash: String::new(),
        response: success_value(),
        fault: None,
        barrier: Some(checkpoint.into()),
    });
    flow.barriers.insert(
        checkpoint.into(),
        crate::workflow::Barrier {
            name: checkpoint.into(),
            actors: vec!["storage".into()],
            checkpoint: checkpoint.into(),
        },
    );
    flow.validate()?;
    Ok(flow)
}

fn resume_diagnostics(manifest: &Manifest) -> ResumeDiagnostics {
    let p = per_invocation_topology(manifest);
    let e = manifest.transport.kind == TransportKind::Exec;
    let c = !manifest.sessions.resume_control.is_empty();
    ResumeDiagnostics {
        per_invocation_topology: p,
        transport_kind: manifest.transport.kind,
        resume_control_declared: c,
        resume_path: crate::storage::lifecycle::resume_path(p, e, c),
        reattach_start_ns: None,
        read_start_ns: None,
        control_start_ns: None,
        control_end_ns: None,
        continuation_start_ns: None,
        counter_start_ns: None,
        resume_start_ns: None,
        first_request_ns: None,
        counter_end_ns: None,
        start_skew_ns: None,
        end_skew_ns: None,
        first_read_bytes: None,
        last_read_bytes: None,
        cursor: None,
        expected_cursor: None,
        total_resume_latency_ms: None,
        session_id_hash: None,
        expected_session_id_hash: None,
        identities: vec![],
    }
}

async fn resume_trial(
    manifest: &Manifest,
    hash: &str,
    root: &Path,
    rep: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<Trial<ResumeSummary, ResumeDiagnostics>> {
    let mut d = resume_diagnostics(manifest);
    let mut trial = Trial {
        repetition: rep,
        outcome: TestOutcome::Pass,
        measurement_complete: true,
        reason: None,
        summary: ResumeSummary::default(),
        diagnostics: d.clone(),
        evidence_refs: vec![],
    };
    if d.resume_path.is_none() {
        trial.outcome=TestOutcome::Unsupported("resume-boundary-unavailable: nonpersistent non-exec driver has no continuation launch receipt".into());
        trial.reason = outcome_reason(&trial.outcome);
        return Ok(trial);
    }
    let mut sampler = platform_sampler();
    if let Err(e) = sampler.disk_counter_preflight() {
        trial.outcome = match e {
            AhrbError::Unsupported(r) => TestOutcome::Unsupported(format!("os-limited: {r}")),
            e => TestOutcome::Error(e.to_string()),
        };
        trial.measurement_complete = !matches!(trial.outcome, TestOutcome::Error(_));
        trial.reason = outcome_reason(&trial.outcome);
        return Ok(trial);
    }
    let flow = held_workflow(manifest, n, "resume")?;
    let mut run = start(
        manifest,
        hash,
        &flow,
        root,
        &format!("s10-r{rep}"),
        &progress.output,
    )
    .await?;
    let result=async {
        grow(&mut run,manifest,n,progress,rep,9).await?;
        let detached=run.after.ok_or_else(||AhrbError::Protocol("resume preparation has no committed cursor".into()))?;
        d.expected_cursor=detached.0.checked_add(1);d.expected_session_id_hash=Some(stable_evidence_hash(&run.session.0));
        let warm=if d.per_invocation_topology {vec![]} else {verified_process_roots(manifest,sampler.as_mut(),run.runtime.driver.owned_pids(),run.runtime.driver.daemon_pid())?};
        let mut counters=Counters::default();
        let mut initial_reads=BTreeMap::new();
        // For warm topology and exec-control, arm before resume. The no-control
        // per-invocation path arms only after AHRB metadata preparation.
        let arm_before=!d.per_invocation_topology || d.resume_control_declared;
        if arm_before {
            d.counter_start_ns=Some(monotonic_timestamp_ns());
            counters.observe(sampler.as_mut(),&warm,&mut progress.report,rep,0)?;
            d.first_read_bytes=counters.reads.snapshot().cumulative_write_bytes;
            initial_reads=counters.reads.snapshot().identities.into_iter().filter_map(|r|r.write_bytes.map(|v|(r.identity,v))).collect();
        }
        run.runtime.driver.enable_storage_resume_receipts();
        d.reattach_start_ns=Some(monotonic_timestamp_ns());
        let resume_result={
            let resume=run.runtime.driver.resume(&run.session);tokio::pin!(resume);
            loop {tokio::select! {
                result=&mut resume=>break result,
                _=tokio::time::sleep(Duration::from_millis(2)),if !warm.is_empty()=>{counters.observe(sampler.as_mut(),&warm,&mut progress.report,rep,1)?;}
            }}
        };
        if let Err(e)=resume_result {
            if matches!(e,AhrbError::Timeout(_)) || (!d.resume_control_declared && !e.to_string().contains("receipt")) {
                return Ok((ResumeSummary {resume_outcome:Some(ResumeOutcome::Failed),..Default::default()},Some(format!("public resume refused: {e}"))));
            }
            return Err(e);
        }
        let controls=run.runtime.driver.storage_control_receipts();
        if manifest.transport.kind==TransportKind::Exec && d.resume_control_declared {
            if controls.len()!=1 {return Err(AhrbError::Protocol("missing or duplicate resume control retirement receipt".into()));}
            d.control_start_ns=Some(controls[0].launch_ns);d.control_end_ns=Some(controls[0].exit_ns);
            if controls[0].identities.is_empty() || controls[0].identities.iter().any(|i|!i.complete) {return Err(AhrbError::Protocol("incomplete resume control identities".into()));}
            if controls[0].exit_code!=Some(0) {return Ok((ResumeSummary {resume_outcome:Some(ResumeOutcome::Failed),..Default::default()},Some("public control returned nonzero".into())));}
        }
        if !arm_before {
            d.counter_start_ns=Some(monotonic_timestamp_ns());
            counters.observe(sampler.as_mut(),&[],&mut progress.report,rep,0)?;
            d.first_read_bytes=counters.reads.snapshot().cumulative_write_bytes;
        }
        let previous=run.runtime.driver.completed_turn_boundaries().len();
        let prompt=format!("AHRB storage resume continuation {}",route_marker(TASK,"storage","resume"));
        run.runtime.driver.submit(&run.session,&prompt,"storage-resume").await?;
        d.continuation_start_ns=run.runtime.driver.active_launch_ns(&run.session);
        if manifest.transport.kind==TransportKind::Exec && d.continuation_start_ns.is_none() {return Err(AhrbError::Protocol("missing continuation launch receipt".into()));}
        d.read_start_ns=if !d.per_invocation_topology {d.reattach_start_ns} else if d.resume_control_declared {d.control_start_ns} else {d.continuation_start_ns};
        d.resume_start_ns=if d.per_invocation_topology {d.continuation_start_ns} else {d.reattach_start_ns};
        let mut roots=warm.clone();roots.extend(run.runtime.driver.owned_pids());roots.sort_unstable();roots.dedup();
        if roots.is_empty() {return Err(AhrbError::Protocol("resume has no owned roots".into()));}
        let wait=run.runtime.engine.barriers().wait_until_ready("resume");tokio::pin!(wait);
        let deadline=tokio::time::sleep(outer_turn_timeout(manifest));tokio::pin!(deadline);
        loop {tokio::select! {
            result=&mut wait=>{result?;break;},
            _=&mut deadline=>return Ok((ResumeSummary {resume_outcome:Some(ResumeOutcome::Failed),..Default::default()},Some("resume provider deadline".into()))),
            _=tokio::time::sleep(Duration::from_millis(2))=>{counters.observe(sampler.as_mut(),&roots,&mut progress.report,rep,1)?;}
        }}
        d.first_request_ns=run.runtime.engine.request_records().await.iter().filter(|r|r.request.checkpoint=="resume").map(|r|r.received_ns).min();
        counters.observe(sampler.as_mut(),&roots,&mut progress.report,rep,1)?;
        d.counter_end_ns=Some(monotonic_timestamp_ns());
        let reads=counters.reads.snapshot();
        if !reads.counter_complete || counters.unavailable.is_some() {return Err(AhrbError::Protocol("incomplete resume read bracket".into()));}
        let control_bytes=controls.iter().flat_map(|c|&c.identities).try_fold(0u64,|sum,i|sum.checked_add(i.last_bytes?)).ok_or_else(||AhrbError::Protocol("missing/overflow control read counters".into()))?;
        d.last_read_bytes=reads.cumulative_write_bytes.and_then(|v|v.checked_add(control_bytes));
        d.identities=reads.identities.into_iter().map(|r|IdentityReceipt {pid:r.identity.pid,start_time:r.identity.start_time,source:crate::driver::storage_control::read_source().into(),first_bytes:Some(initial_reads.get(&r.identity).copied().unwrap_or(0)),last_bytes:r.write_bytes,retirement_method:format!("{:?}",r.status),complete:true}).collect();
        d.identities.extend(controls.iter().flat_map(|c|c.identities.clone()));
        let first=d.first_request_ns.ok_or_else(||AhrbError::Protocol("missing provider body receipt".into()))?;
        let read_start=d.read_start_ns.ok_or_else(||AhrbError::Protocol("missing read origin".into()))?;
        let start=d.resume_start_ns.ok_or_else(||AhrbError::Protocol("missing resume origin".into()))?;
        d.start_skew_ns=read_start.checked_sub(d.counter_start_ns.unwrap());d.end_skew_ns=d.counter_end_ns.unwrap().checked_sub(first);
        let latency=first.checked_sub(start).ok_or_else(||AhrbError::Protocol("provider preceded resume origin".into()))? as f64/1e6;
        d.total_resume_latency_ms=first.checked_sub(read_start).map(|v|v as f64/1e6);
        if d.start_skew_ns.is_none()||d.end_skew_ns.is_none()||d.total_resume_latency_ms.is_none() {return Err(AhrbError::Protocol("read counter bracket does not enclose resume".into()));}
        crate::storage::lifecycle::validate_resume_bracket(&d)?;
        let bytes=d.last_read_bytes.and_then(|last|last.checked_sub(d.first_read_bytes?)).ok_or_else(||AhrbError::Protocol("resume reads regressed or absent".into()))?;
        if !controls.is_empty() {
            let path=progress.output.join(format!("s10-r{rep}-control.json"));
            let bytes=serde_json::to_vec(&controls)?;std::fs::write(&path,&bytes)?;
            trial.evidence_refs.push(EvidenceRef {file:path.file_name().unwrap().to_string_lossy().into_owned(),sha256:format!("{:x}",Sha256::digest(bytes)),first_record:None,last_record:None});
        }
        // Nothing above this gate settles/scans the filesystem or releases the
        // provider. Terminal and post-response reads are outside the interval.
        run.runtime.engine.barriers().release("resume").await?;
        let events=match collect_session_terminal(&mut run.runtime.driver,&run.session,Some(detached),outer_turn_timeout(manifest)).await {
            Ok(v)=>v,Err(e)=>return Ok((ResumeSummary {resume_outcome:Some(ResumeOutcome::Failed),..Default::default()},Some(format!("resume continuation failed: {e}")))),
        };
        d.cursor=events.iter().map(|e|e.cursor).min();d.session_id_hash=events.first().map(|e|stable_evidence_hash(&e.session_id));
        let preserved=d.cursor==d.expected_cursor && d.session_id_hash==d.expected_session_id_hash && events.iter().all(|e|e.session_id==run.session.0) && events.iter().any(|e|e.event==EventVocab::TerminalSuccess) && events.iter().filter(|e|is_terminal(&e.event)).count()==1;
        progress.report.events.extend(events.into_iter().map(serde_json::to_value).collect::<std::result::Result<Vec<_>,_>>()?);
        if manifest.transport.kind==TransportKind::Exec {
            let boundary=await_completed_turn_boundary(&mut run.runtime.driver,&run.session,Some(detached),previous,outer_turn_timeout(manifest)).await?;
            if Some(boundary.launch_ns)!=d.continuation_start_ns {return Err(AhrbError::Protocol("continuation launch receipt changed".into()));}
            let v=json!({"launch_ns":boundary.launch_ns,"exit_ns":boundary.exit_ns,"identities":d.identities});let raw=serde_json::to_vec(&v)?;
            let file=format!("s10-r{rep}-continuation.json");std::fs::write(progress.output.join(&file),&raw)?;
            trial.evidence_refs.push(EvidenceRef {file,sha256:format!("{:x}",Sha256::digest(raw)),first_record:None,last_record:None});
        }
        let after_resume=accounting::settle(&run.profile,&manifest.storage.clone().unwrap_or_default(),monotonic_timestamp_ns()).await?;
        boundary(progress,&manifest.storage.clone().unwrap_or_default(),9,rep,n+1,&format!("s10-r{rep}-post-resume"),&after_resume);
        Ok((if preserved {ResumeSummary {resume_read_bytes_p50:Some(bytes as f64),resume_read_bytes_p95:Some(bytes as f64),resume_latency_p50_ms:Some(latency),resume_latency_p95_ms:Some(latency),resume_outcome:Some(ResumeOutcome::Preserved)}} else {ResumeSummary {resume_outcome:Some(ResumeOutcome::Failed),..Default::default()}},if preserved {Some("warm-cache OS physical reads; background owned I/O included; no cache eviction".into())}else{Some("same-session/exact-next-cursor oracle contradicted".into())}))
    }.await;
    provider_evidence(&run, progress, rep, 9).await?;
    let _ = run.runtime.engine.barriers().release("resume").await;
    let _ = run.runtime.driver.shutdown().await;
    run.runtime.server.shutdown().await?;
    trial.diagnostics = d;
    match result {
        Ok((summary, reason)) => {
            trial.summary = summary;
            trial.reason = reason;
        }
        Err(e) => {
            trial.outcome = TestOutcome::Error(e.to_string());
            trial.reason = outcome_reason(&trial.outcome);
            trial.measurement_complete = false;
        }
    }
    Ok(trial)
}

pub(in crate::runner::storage) async fn collect_resume(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    root: &Path,
    reps: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<()> {
    let mut details = RowDetails {
        measurement_label: MEASUREMENT_LABEL.into(),
        reason: None,
        trials: vec![],
    };
    for rep in 1..=reps {
        details
            .trials
            .push(resume_trial(manifest, hash, root, rep, n, progress).await?);
        progress
            .details
            .insert(ROWS[9].into(), serde_json::to_value(&details)?);
    }
    let outcome = declared_outcome(details.trials.iter().map(|t| &t.outcome));
    details.reason = outcome_reason(&outcome);
    if details.trials.iter().all(|t| t.measurement_complete) {
        let s = progress.report.storage_summary.as_mut().unwrap();
        let reads = details
            .trials
            .iter()
            .map(|t| t.summary.resume_read_bytes_p50)
            .collect::<Option<Vec<_>>>();
        let latency = details
            .trials
            .iter()
            .map(|t| t.summary.resume_latency_p50_ms)
            .collect::<Option<Vec<_>>>();
        s.resume_read_bytes_p50 = reads.clone().and_then(median);
        s.resume_read_bytes_p95 = reads.and_then(p95);
        s.resume_latency_p50_ms = latency.clone().and_then(median);
        s.resume_latency_p95_ms = latency.and_then(p95);
        s.resume_outcome = details
            .trials
            .iter()
            .map(|t| t.summary.resume_outcome)
            .collect::<Option<Vec<_>>>()
            .map(|v| {
                if v.contains(&ResumeOutcome::Failed) {
                    ResumeOutcome::Failed
                } else {
                    ResumeOutcome::Preserved
                }
            });
    }
    progress.report.results[9] = row(9, outcome, config);
    progress
        .details
        .insert(ROWS[9].into(), serde_json::to_value(details)?);
    Ok(())
}

async fn crash_trial(
    manifest: &Manifest,
    hash: &str,
    root: &Path,
    rep: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<Trial<CrashSummary, CrashDiagnostics>> {
    let config = manifest.storage.clone().unwrap_or_default();
    let mut trial = Trial {
        repetition: rep,
        outcome: TestOutcome::Pass,
        measurement_complete: true,
        reason: None,
        summary: CrashSummary::default(),
        diagnostics: CrashDiagnostics::default(),
        evidence_refs: vec![],
    };
    if (!manifest
        .capabilities
        .required
        .contains_key("durable_journal")
        && !manifest
            .capabilities
            .optional
            .contains_key("durable_journal"))
        || manifest.events.path.is_empty()
    {
        trial.outcome = TestOutcome::Absent(
            "crash recovery needs durable_journal and a journal locator".into(),
        );
        trial.reason = outcome_reason(&trial.outcome);
        trial.measurement_complete = false;
        return Ok(trial);
    }
    let flow = held_workflow(manifest, n, "crash")?;
    let mut run = start(
        manifest,
        hash,
        &flow,
        root,
        &format!("s9-r{rep}"),
        &progress.output,
    )
    .await?;
    let mut d = CrashDiagnostics::default();
    let result=async {
        grow(&mut run,manifest,n,progress,rep,8).await?;
        let pre=accounting::settle(&run.profile,&config,monotonic_timestamp_ns()).await?;
        boundary(progress,&config,8,rep,n,&format!("s9-r{rep}-pre-hold"),&pre);
        let prompt=format!("AHRB storage crash held turn {}",route_marker(TASK,"storage","crash"));
        run.runtime.driver.submit(&run.session,&prompt,"storage-crash").await?;
        run.runtime.driver.release_invocations().await?;
        tokio::time::timeout(outer_turn_timeout(manifest),run.runtime.engine.barriers().wait_until_ready("crash")).await.map_err(|_|AhrbError::Timeout("crash held request not received".into()))??;
        d.hold_ns=Some(monotonic_timestamp_ns());
        let variables=vars(&run.profile,&run.workspace,&run.session);
        let journal=PathBuf::from(crate::manifest::render_template(&manifest.events.path,&variables)?);contained_path(&run.profile,&journal)?;
        let committed=std::fs::read(&journal)?;
        let before_events=read_signal_matrix_journal(manifest,&variables,&run.profile,&run.session)?;
        d.committed_cursor=before_events.iter().map(|e|e.cursor).max();
        let cursor=d.committed_cursor.ok_or_else(||AhrbError::Protocol("kill has no durable activity".into()))?;
        if cursor<=run.after.map_or(0,|c|c.0) {return Err(AhrbError::Protocol("held turn has no new durable activity".into()));}
        d.committed_prefix_sha256=Some(format!("{:x}",Sha256::digest(&committed)));
        let prefix_file=format!("s9-r{rep}-committed-journal.bin");std::fs::write(progress.output.join(&prefix_file),&committed)?;
        trial.evidence_refs.push(EvidenceRef {file:prefix_file,sha256:d.committed_prefix_sha256.clone().unwrap(),first_record:None,last_record:None});
        d.expected_session_id_hash=Some(stable_evidence_hash(&run.session.0));d.expected_cursor=cursor.checked_add(1);
        let mut sampler=platform_sampler();
        let roots=verified_process_roots(manifest,sampler.as_mut(),run.runtime.driver.owned_pids(),run.runtime.driver.daemon_pid())?;
        let tree=sampler.discover(&roots)?;
        let target=signal_target_identity(&tree,&roots)?;
        #[cfg(unix)] crate::process::deliver_registered_tree_signal(target,SignalMatrixCase::Sigkill.unix_signal().unwrap())?;
        #[cfg(not(unix))] return Err(AhrbError::Unsupported("SIGKILL unavailable".into()));
        d.kill_ns=Some(monotonic_timestamp_ns());
        // No catchable-signal terminal is expected from SIGKILL. Confirm the
        // registered owned tree exits, then reap without sending a close/delete.
        let deadline=Instant::now()+outer_turn_timeout(manifest);
        while crate::process::registered_tree_is_live(target)? {
            if Instant::now()>=deadline {return Err(AhrbError::Timeout("SIGKILL tree did not exit".into()));}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        run.runtime.driver.reap_after_external_kill().await?;
        d.exit_ns=Some(monotonic_timestamp_ns());
        let kill_receipt=json!({"signal":"SIGKILL","target":target,"owned_identities":tree.members.keys().collect::<Vec<_>>(),"delivery_succeeded":true,"hold_ns":d.hold_ns,"kill_ns":d.kill_ns,"exit_ns":d.exit_ns,"tree_exited":true});
        let file=format!("s9-r{rep}-kill.json");let bytes=serde_json::to_vec(&kill_receipt)?;std::fs::write(progress.output.join(&file),&bytes)?;
        trial.evidence_refs.push(EvidenceRef {file,sha256:format!("{:x}",Sha256::digest(bytes)),first_record:None,last_record:None});
        let post=accounting::settle(&run.profile,&config,monotonic_timestamp_ns()).await?;
        boundary(progress,&config,8,rep,n,&format!("s9-r{rep}-post-kill"),&post);
        let candidates=residue(&pre.inventory,&post.inventory,&[String::new()]);
        let bytes=candidates.iter().map(|e|e.allocated_bytes).sum();let files=candidates.len() as u64;
        d.residue_paths=candidates.iter().map(|e|e.path.clone()).collect();
        // Restart using the same fake engine and disposable root; generic setup
        // does not remove journals. The held response can now complete recovery.
        let engine=Arc::clone(&run.runtime.engine);
        let replacement=start_scripted_pillar_runtime_with_engine(manifest,hash,&flow,&run.profile,"storage",Some(&run.workspace),engine,false).await;
        let replacement=match replacement {Ok(v)=>v,Err(e)=>return Ok((CrashSummary {crash_residue_allocated_bytes:Some(bytes),crash_residue_files:Some(files),crash_resume_outcome:Some(CrashOutcome::Failed)},format!("restart refused: {e}")))};
        let old=std::mem::replace(&mut run.runtime,replacement);drop(old.driver);
        run.runtime.engine.barriers().release("crash").await?;
        old.server.shutdown().await?;
        let resumed=async {
            run.runtime.driver.resume(&run.session).await?;
            if manifest.transport.kind==TransportKind::Exec {run.runtime.driver.submit(&run.session,&prompt,"storage-crash").await?;}
            collect_session_terminal(&mut run.runtime.driver,&run.session,Some(Cursor(cursor)),outer_turn_timeout(manifest)).await
        }.await;
        let after=accounting::settle(&run.profile,&config,monotonic_timestamp_ns()).await?;
        boundary(progress,&config,8,rep,n,&format!("s9-r{rep}-post-resume"),&after);
        let events=match resumed {Ok(v)=>v,Err(e)=>return Ok((CrashSummary {crash_residue_allocated_bytes:Some(bytes),crash_residue_files:Some(files),crash_resume_outcome:Some(CrashOutcome::Failed)},format!("resume refused/deadline: {e}")))};
        d.resumed_cursor=events.iter().map(|e|e.cursor).min();d.resumed_session_id_hash=events.first().map(|e|stable_evidence_hash(&e.session_id));
        let after_bytes=std::fs::read(&journal)?;
        let prefix_ok=after_bytes.starts_with(&committed);
        let all=read_signal_matrix_journal(manifest,&variables,&run.profile,&run.session)?;
        let before_ids=before_events.iter().map(|e|e.cursor).collect::<BTreeSet<_>>();
        let after_ids=all.iter().map(|e|e.cursor).collect::<BTreeSet<_>>();
        d.lost_events=Some(before_ids.difference(&after_ids).count() as u64);
        let unique_event_ids=all.iter().map(|e|&e.id).collect::<BTreeSet<_>>();
        d.duplicate_events=Some(all.len().saturating_sub(after_ids.len()).max(all.len().saturating_sub(unique_event_ids.len())) as u64);
        let effects=all.iter().filter(|e|e.event==EventVocab::ToolResult).filter_map(|e|e.payload.get("call_id").and_then(Value::as_str)).collect::<Vec<_>>();
        d.duplicate_effects=Some(effects.len().saturating_sub(effects.iter().collect::<BTreeSet<_>>().len()) as u64);
        let contiguous=events.iter().map(|e|e.cursor).collect::<BTreeSet<_>>().iter().copied().eq((cursor+1)..=(all.iter().map(|e|e.cursor).max().unwrap_or(cursor)));
        let preserved=prefix_ok&&contiguous&&d.resumed_cursor==d.expected_cursor&&d.resumed_session_id_hash==d.expected_session_id_hash&&events.iter().all(|e|e.session_id==run.session.0)&&d.lost_events==Some(0)&&d.duplicate_events==Some(0)&&d.duplicate_effects==Some(0);
        let resume_outcome=if events.iter().any(|e|is_terminal(&e.event)&&e.event!=EventVocab::TerminalSuccess) {CrashOutcome::Failed}
            else if preserved&&events.iter().filter(|e|is_terminal(&e.event)).count()==1 {CrashOutcome::Preserved}else{CrashOutcome::Corrupt};
        progress.report.events.extend(events.into_iter().map(serde_json::to_value).collect::<std::result::Result<Vec<_>,_>>()?);
        Ok((CrashSummary {crash_residue_allocated_bytes:Some(bytes),crash_residue_files:Some(files),crash_resume_outcome:Some(resume_outcome)},format!("committed-prefix unchanged={prefix_ok}; residue paths are surviving candidates, not proven orphans")))
    }.await;
    if result.is_ok()
        && !progress
            .report
            .storage_samples
            .iter()
            .any(|s| s.row_id == ROWS[8] && s.boundary == format!("s9-r{rep}-post-resume"))
    {
        let after = accounting::settle(&run.profile, &config, monotonic_timestamp_ns()).await?;
        boundary(
            progress,
            &config,
            8,
            rep,
            n,
            &format!("s9-r{rep}-post-resume"),
            &after,
        );
    }
    provider_evidence(&run, progress, rep, 8).await?;
    let _ = run.runtime.engine.barriers().release("crash").await;
    let _ = run.runtime.driver.shutdown().await;
    run.runtime.server.shutdown().await?;
    trial.diagnostics = d;
    match result {
        Ok((summary, reason)) => {
            trial.summary = summary;
            trial.reason = Some(reason);
        }
        Err(e) => {
            trial.outcome = TestOutcome::Error(e.to_string());
            trial.reason = outcome_reason(&trial.outcome);
            trial.measurement_complete = false;
        }
    }
    Ok(trial)
}

pub(in crate::runner::storage) async fn collect_crash(
    manifest: &Manifest,
    config: &StorageConfig,
    hash: &str,
    root: &Path,
    reps: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<()> {
    let mut details = RowDetails {
        measurement_label: MEASUREMENT_LABEL.into(),
        reason: None,
        trials: vec![],
    };
    for rep in 1..=reps {
        details
            .trials
            .push(crash_trial(manifest, hash, root, rep, n, progress).await?);
        progress
            .details
            .insert(ROWS[8].into(), serde_json::to_value(&details)?);
    }
    let outcome = declared_outcome(details.trials.iter().map(|t| &t.outcome));
    details.reason = outcome_reason(&outcome);
    if details.trials.iter().all(|t| t.measurement_complete) {
        let s = progress.report.storage_summary.as_mut().unwrap();
        s.crash_residue_allocated_bytes = details
            .trials
            .iter()
            .map(|t| t.summary.crash_residue_allocated_bytes)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
        s.crash_residue_files = details
            .trials
            .iter()
            .map(|t| t.summary.crash_residue_files)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().max());
        s.crash_resume_outcome = details
            .trials
            .iter()
            .map(|t| t.summary.crash_resume_outcome)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| {
                v.into_iter().max_by_key(|v| match v {
                    CrashOutcome::Preserved => 0,
                    CrashOutcome::Corrupt => 1,
                    CrashOutcome::Failed => 2,
                })
            });
    }
    progress.report.results[8] = row(8, outcome, config);
    progress
        .details
        .insert(ROWS[8].into(), serde_json::to_value(details)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Each trial shuts down its own driver and server. Process-wide cleanup is
    // unsafe here: earlier sampler unit tests intentionally observe this runner.
    fn progress(root: &Path) -> Progress {
        let output = root.join("output");
        std::fs::create_dir_all(&output).unwrap();
        Progress {
            output,
            active_engine: None,
            report: Report {
                storage_summary: Some(StorageSummary::default()),
                results: (0..10)
                    .map(|i| {
                        row(
                            i,
                            TestOutcome::Error("pending".into()),
                            &StorageConfig::default(),
                        )
                    })
                    .collect(),
                ..Report::default()
            },
            write_trials: vec![],
            curve_trials: vec![],
            auxiliary_trials: vec![],
            retention_trials: vec![],
            finalized: BTreeSet::new(),
            details: BTreeMap::new(),
        }
    }
    fn root(label: &str) -> PathBuf {
        // Short enough for native Unix sockets; fixtures own only this directory.
        std::env::temp_dir().join(format!("s6-{label}-{}", monotonic_timestamp_ns()))
    }
    #[tokio::test]
    async fn storage_lifecycle_real_drivers_cover_all_resume_brackets() {
        for (name, control) in [
            ("mock", false),
            ("mock-exec", false),
            ("mock-exec", true),
            ("mock-storage-daemon-exec", false),
            ("mock-storage-daemon-exec", true),
        ] {
            let root = root("resume");
            let mut progress = progress(&root);
            let mut manifest =
                crate::manifest::load(Path::new(&format!("adapters/{name}/manifest.toml")))
                    .unwrap();
            if control {
                manifest.sessions.resume_control = vec![
                    "target/debug/ahrb-mock-harness".into(),
                    "session-resume".into(),
                    "--state-dir".into(),
                    "{{profile}}/state".into(),
                    "--session-id".into(),
                    "{{session_id}}".into(),
                ];
            }
            let trial = resume_trial(
                &manifest,
                &crate::manifest::hash(&manifest).unwrap(),
                &root,
                1,
                2,
                &mut progress,
            )
            .await
            .unwrap();
            assert_eq!(
                trial.outcome,
                TestOutcome::Pass,
                "{name} control={control}: {trial:?}"
            );
            assert_eq!(
                trial.summary.resume_outcome,
                Some(ResumeOutcome::Preserved),
                "{trial:?}"
            );
            let d = &trial.diagnostics;
            assert!(d.first_read_bytes.is_some() && d.last_read_bytes.is_some());
            assert_eq!(d.control_start_ns.is_some(), control);
            assert_eq!(d.control_end_ns.is_some(), control);
            assert_eq!(d.continuation_start_ns.is_some(), name != "mock");
            assert!(
                d.counter_start_ns <= d.read_start_ns && d.first_request_ns <= d.counter_end_ns
            );
            assert_eq!(
                d.resume_start_ns,
                if d.per_invocation_topology {
                    d.continuation_start_ns
                } else {
                    d.reattach_start_ns
                }
            );
            if control {
                assert!(
                    d.identities
                        .iter()
                        .any(|i| i.retirement_method == "RetiredAfterFinalSample")
                );
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn storage_lifecycle_sigkill_recovers_committed_prefix_for_both_transports() {
        for name in ["mock", "mock-exec"] {
            let root = root("crash");
            let mut progress = progress(&root);
            let manifest =
                crate::manifest::load(Path::new(&format!("adapters/{name}/manifest.toml")))
                    .unwrap();
            let trial = crash_trial(
                &manifest,
                &crate::manifest::hash(&manifest).unwrap(),
                &root,
                1,
                2,
                &mut progress,
            )
            .await
            .unwrap();
            assert_eq!(trial.outcome, TestOutcome::Pass, "{name}: {trial:?}");
            assert_eq!(
                trial.summary.crash_resume_outcome,
                Some(CrashOutcome::Preserved),
                "{name}: {trial:?}"
            );
            assert!(trial.diagnostics.kill_ns >= trial.diagnostics.hold_ns);
            assert!(trial.diagnostics.exit_ns >= trial.diagnostics.kill_ns);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn storage_lifecycle_adverse_resume_and_crash_are_informational() {
        for name in ["mock", "mock-exec"] {
            let root = root("adverse");
            let mut progress = progress(&root);
            let mut manifest =
                crate::manifest::load(Path::new(&format!("adapters/{name}/manifest.toml")))
                    .unwrap();
            manifest
                .isolation
                .environment
                .insert("AHRB_MOCK_STORAGE_RESUME_READ".into(), "extra".into());
            manifest
                .isolation
                .environment
                .insert("AHRB_MOCK_STORAGE_RESUME_ID".into(), "wrong".into());
            manifest
                .isolation
                .environment
                .insert("AHRB_MOCK_STORAGE_CRASH_RECOVERY".into(), "corrupt".into());
            manifest
                .isolation
                .environment
                .insert("AHRB_MOCK_STORAGE_CRASH_TEMP".into(), "leave".into());
            let hash = crate::manifest::hash(&manifest).unwrap();
            let resume = resume_trial(&manifest, &hash, &root, 1, 2, &mut progress)
                .await
                .unwrap();
            assert_eq!(resume.outcome, TestOutcome::Pass, "{name}: {resume:?}");
            assert_eq!(
                resume.summary.resume_outcome,
                Some(ResumeOutcome::Failed),
                "{resume:?}"
            );
            assert!(
                resume.summary.resume_read_bytes_p50.is_none()
                    && resume.summary.resume_latency_p50_ms.is_none()
            );
            let crash = crash_trial(&manifest, &hash, &root, 1, 2, &mut progress)
                .await
                .unwrap();
            assert_eq!(crash.outcome, TestOutcome::Pass, "{name}: {crash:?}");
            assert_eq!(
                crash.summary.crash_resume_outcome,
                Some(CrashOutcome::Corrupt),
                "{crash:?}"
            );
            assert!(
                crash
                    .diagnostics
                    .residue_paths
                    .iter()
                    .any(|p| p.ends_with("storage-orphan.tmp"))
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn storage_lifecycle_declared_delete_and_uninstall_execute_independently() {
        for name in ["mock", "mock-exec"] {
            let root = root("delete");
            let mut progress = progress(&root);
            let mut manifest =
                crate::manifest::load(Path::new(&format!("adapters/{name}/manifest.toml")))
                    .unwrap();
            let config = StorageConfig {
                session_delete: Some(vec![
                    "{{harness}}".into(),
                    "storage-cleanup".into(),
                    "--profile".into(),
                    "{{profile}}".into(),
                    "--operation".into(),
                    "delete".into(),
                    "--session-id".into(),
                    "{{session_id}}".into(),
                ]),
                uninstall_cleanup: Some(vec![
                    "{{harness}}".into(),
                    "storage-cleanup".into(),
                    "--profile".into(),
                    "{{profile}}".into(),
                    "--operation".into(),
                    "uninstall".into(),
                ]),
                ..Default::default()
            };
            manifest.storage = Some(config.clone());
            manifest
                .availability
                .exec_paths
                .insert(0, "ahrb-deliberately-missing-executable".into());
            collect_delete(
                &manifest,
                &config,
                &crate::manifest::hash(&manifest).unwrap(),
                &workflow(&manifest, 2).unwrap(),
                &root,
                1,
                2,
                &mut progress,
            )
            .await
            .unwrap();
            assert_eq!(
                progress.report.results[5].outcome,
                TestOutcome::Pass,
                "{name}: {:?}",
                progress.details
            );
            if name == "mock" {
                for (label, expected) in [("session-delete", true), ("uninstall-cleanup", false)] {
                    let receipt: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(progress.output.join(format!("s6-{label}-r1-command.json")))
                            .unwrap(),
                    )
                    .unwrap();
                    let stdout: serde_json::Value =
                        serde_json::from_str(receipt["stdout"].as_str().unwrap()).unwrap();
                    assert_eq!(
                        stdout["controller_live"].as_bool().unwrap_or(false),
                        expected
                    );
                }
            }
            let summary = progress.report.storage_summary.unwrap();
            assert_eq!(summary.delete_residue_files, Some(0));
            assert_eq!(summary.uninstall_residue_files, Some(0));
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
