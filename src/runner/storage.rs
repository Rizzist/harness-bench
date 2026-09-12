//! Serialized storage workload. This child module reuses the matrix's public
//! session driver and terminal collector, but not its close/delete oracle.
pub(crate) mod lifecycle;

use super::*;
use crate::process::{DiskIdentityStatus, ProcessDiskObservation, TreeDiskTracker};
use crate::storage::{self as contract, accounting, evidence::*, *};
use std::io::Write as _;

fn workflow(manifest: &Manifest, n: u32) -> Result<Workflow> {
    fixture::validate_renderer_pins()?;
    let mut responses = Vec::new();
    for turn in 1..=n {
        let checkpoint = format!("t{turn:04}");
        let terminal = if turn % 10 == 0 {
            format!("{checkpoint}-terminal")
        } else {
            checkpoint.clone()
        };
        if turn % 10 == 0 {
            let call = mapped_tool_call(
                manifest,
                "read",
                format!("storage-read-{turn:04}"),
                json!({"path":"context-a.txt","route":route_marker(TASK,"storage",&terminal)}),
            )?;
            responses.push(ScriptedResponse {
                scenario: TASK.into(),
                actor: "storage".into(),
                checkpoint: checkpoint.clone(),
                request_hash: String::new(),
                response: json!({"tool_calls":[call]}),
                fault: None,
                barrier: None,
            });
        }
        responses.push(ScriptedResponse {
            scenario: TASK.into(),
            actor: "storage".into(),
            checkpoint: terminal,
            request_hash: String::new(),
            response: json!({"text":crate::economy::ECONOMY_OUTPUT_CONTENT}),
            fault: None,
            barrier: None,
        });
    }
    let workflow = Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: TASK.into(),
        actors: BTreeMap::from([(
            "storage".into(),
            Actor {
                id: "storage".into(),
                parent: None,
                prompt: fixture::prompt(1)?,
                workspace: "storage".into(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses,
    };
    workflow.validate()?;
    Ok(workflow)
}

fn row(id: usize, outcome: TestOutcome, config: &StorageConfig) -> TestResult {
    TestResult {
        row: (id + 1) as u8,
        id: ROWS[id].into(),
        pillar: crate::evaluate::Pillar::Storage,
        evidence: vec![format!("details.{}", ROWS[id])],
        metadata: TestResultMetadata {
            requirement: if id == 2 || (id == 5 && config.delete_declared()) {
                "core"
            } else {
                "informational"
            }
            .into(),
            measurement_complete: !matches!(
                outcome,
                TestOutcome::Error(_) | TestOutcome::Absent(_)
            ),
            ..TestResultMetadata::default()
        },
        outcome,
    }
}

fn pending_details(id: usize, reason: &str) -> Value {
    let mut value = json!({"measurement_label":MEASUREMENT_LABEL,"reason":reason,"trials":[]});
    if let Some(key) = match id {
        1 => Some("instrumentation"),
        5 => Some("operations"),
        7 => Some("matches"),
        _ => None,
    } {
        value[key] = json!([]);
    }
    value
}

struct Progress {
    output: PathBuf,
    finalized: BTreeSet<usize>,
    active_engine: Option<(u32, Arc<FakeModelEngine>, usize, usize)>,
    report: Report,
    write_trials: Vec<Trial<WriteSummary, WriteDiagnostics>>,
    curve_trials: Vec<Trial<FootprintSummary, CurveEvaluation>>,
    auxiliary_trials: Vec<Trial<AuxiliarySummary, AuxiliaryDiagnostics>>,
    retention_trials: Vec<Trial<RetentionSummary, RetentionDiagnostics>>,
    details: BTreeMap<String, Value>,
}

/// Run the storage pillar with explicit interrupted-report semantics.
pub async fn run_storage(mut options: RunOptions) -> Result<i32> {
    if !options.tests.is_empty() {
        return Err(AhrbError::Usage("storage rejects --tests".into()));
    }
    let manifest = crate::manifest::load(&options.manifest)?;
    let config = manifest.storage.clone().unwrap_or_default();
    let env = std::env::var_os("AHRB_DEADLINE").map(|v| v.to_string_lossy().into_owned());
    let budget = resolve_deadline(
        options.profile,
        options.deadline_secs,
        env.as_deref(),
        config.sweep_interval_s,
    )?;
    println!("storage deadline {}", serde_json::to_string(&budget)?);
    let persistence = crate::results::prepare(&options, &manifest)?;
    options.output = persistence.output.clone();
    if let Some(parent) = options.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir(&options.output).map_err(|e| {
        AhrbError::Validation(format!(
            "storage requires a fresh output {}: {e}",
            options.output.display()
        ))
    })?;
    let deadline_at = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(budget.seconds))
        .ok_or_else(|| AhrbError::Usage("storage deadline overflows".into()))?;
    let (n, repetitions) = match options.profile {
        Profile::Quick => (100, 3),
        Profile::Cert => (1000, 7),
    };
    let workflow = workflow(&manifest, n)?;
    std::fs::write(
        options.output.join("storage-deadline.json"),
        serde_json::to_vec_pretty(&budget)?,
    )?;
    write_fixture_receipts(&options.output, &workflow, &manifest)?;
    let manifest_hash = crate::manifest::hash(&manifest)?;
    let profile = format!("{:?}", options.profile).to_lowercase();
    let source = if cfg!(target_os = "macos") {
        "macos-ri_diskio_byteswritten"
    } else if cfg!(target_os = "linux") {
        "linux-proc-pid-io-write_bytes"
    } else {
        "unavailable"
    };
    let summary = StorageSummary {
        schema: 1,
        task: TASK.into(),
        profile: profile.clone(),
        os: std::env::consts::OS.into(),
        topology: manifest.concurrency.topology.clone(),
        comparison_scope: "within-topology-only".into(),
        turn_budget: n,
        repetitions,
        measurement_label: MEASUREMENT_LABEL.into(),
        counter_source: source.into(),
        allocation_source: if cfg!(unix) {
            "stat-st_blocks-512"
        } else {
            "unavailable"
        }
        .into(),
        declarations_sha256: config.declarations_sha256()?,
        assumed_fsync_cost_ms: Some(4.0),
        ..StorageSummary::default()
    };
    let pending = "collector-not-implemented: scheduled for a later storage lane; no feasibility or zero measurement claimed";
    std::fs::File::create(options.output.join("storage-request-bodies.jsonl"))?;
    std::fs::File::create(options.output.join("storage-log-audit.jsonl"))?;
    let mut progress = Progress {
        output: options.output.clone(),
        finalized: BTreeSet::new(),
        active_engine: None,
        report: Report {
            schema: 4,
            spec_version: 4,
            pillar: Some("storage".into()),
            run_id: format!("ahrb-storage-{}", persistence.timestamp),
            profile_path: persistence.profile_path.to_string_lossy().into_owned(),
            fingerprint: Fingerprint {
                harness: manifest.identity.id.clone(),
                harness_version: persistence.harness_version.clone(),
                manifest: manifest_hash.clone(),
                workflows: format!("{:x}", Sha256::digest(serde_json::to_vec(&workflow)?)),
                fake_model: env!("CARGO_PKG_VERSION").into(),
                normalizer: env!("CARGO_PKG_VERSION").into(),
                ahrb_revision: crate::results::ahrb_revision(),
                platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
                host_memory_bytes: host_memory_bytes(),
                profile: profile.clone(),
            },
            results: (0..10)
                .map(|i| row(i, TestOutcome::Error(pending.into()), &config))
                .collect(),
            resource_summary: ResourceSummary {
                topology: manifest.concurrency.topology.clone(),
                profile,
                comparison_scope: "within-topology-only".into(),
                ..ResourceSummary::default()
            },
            storage_summary: Some(summary),
            ..Report::default()
        },
        write_trials: Vec::new(),
        curve_trials: Vec::new(),
        auxiliary_trials: Vec::new(),
        retention_trials: Vec::new(),
        details: (0..10)
            .map(|i| (ROWS[i].into(), pending_details(i, pending)))
            .collect(),
    };
    let prerequisite = common_prerequisite(&manifest);
    let mut interrupted = false;
    let mut collection_failed = false;
    if budget.seconds == 0 {
        interrupted = true;
    } else if let Some(reason) = prerequisite {
        for (i, _slug) in ROWS.iter().enumerate() {
            progress.report.results[i] = row(i, TestOutcome::Absent(reason.clone()), &config);
            progress
                .details
                .insert((*_slug).into(), pending_details(i, &reason));
        }
    } else if let Some(reason) = pre_reap_unavailability(&manifest) {
        // This is declared transport feasibility, established before launching
        // the workload. Never route a failed collection through this branch.
        for i in [0, 2, 6, 7] {
            progress.report.results[i] = row(i, TestOutcome::Unsupported(reason.clone()), &config);
            progress
                .details
                .insert(ROWS[i].into(), pending_details(i, &reason));
        }
        progress.report.lifecycle_notes.push(reason);
    } else {
        for repetition in 1..=repetitions {
            let outcome = tokio::time::timeout_at(
                deadline_at,
                collect_repetition(
                    &manifest,
                    &config,
                    &manifest_hash,
                    &workflow,
                    &persistence.profile_path,
                    repetition,
                    n,
                    &mut progress,
                ),
            )
            .await;
            match outcome {
                Err(_) => {
                    interrupted = true;
                    break;
                }
                Ok(Err(error)) => {
                    collection_failed = true;
                    if error
                        .to_string()
                        .contains("missing storage workspace locator")
                    {
                        let reason = "missing required storage workspace locator";
                        for (i, _slug) in ROWS.iter().enumerate() {
                            progress.report.results[i] =
                                row(i, TestOutcome::Absent(reason.into()), &config);
                            progress
                                .details
                                .insert(ROWS[i].into(), pending_details(i, reason));
                        }
                        break;
                    }
                    if matches!(&error, AhrbError::Unsupported(reason) if reason.starts_with("storage stat-st_blocks-512 unavailable"))
                    {
                        let reason = format!("os-limited: {error}");
                        for i in [0, 2, 6, 7] {
                            progress.report.results[i] =
                                row(i, TestOutcome::Unsupported(reason.clone()), &config);
                            progress
                                .details
                                .insert(ROWS[i].into(), pending_details(i, &reason));
                        }
                        if let Some(summary) = &mut progress.report.storage_summary {
                            summary.allocation_source = "unavailable".into();
                        }
                        break;
                    }
                    let reason = format!("task-incomplete: {error}");
                    for i in [0, 2, 6, 7] {
                        progress.report.results[i] =
                            row(i, TestOutcome::Error(reason.clone()), &config);
                        progress
                            .details
                            .insert(ROWS[i].into(), pending_details(i, &reason));
                    }
                    progress.report.lifecycle_notes.push(reason);
                    break;
                }
                Ok(Ok(())) => {}
            }
            if tokio::time::Instant::now() >= deadline_at {
                interrupted = true;
                break;
            }
        }
        if !interrupted && !collection_failed && progress.curve_trials.len() == repetitions as usize
        {
            finalize(&mut progress, &config)?;
            progress.finalized.extend([0, 2]);
        }
    }
    capture_provider(&mut progress).await?;
    if !interrupted
        && common_prerequisite(&manifest).is_none()
        && !progress
            .report
            .results
            .iter()
            .all(|r| matches!(r.outcome, TestOutcome::Absent(_)))
    {
        preserve_partial_trial(&mut progress, &config)?;
        progress.active_engine = None;
        let lifecycle_result = lifecycle::collect(
            &manifest,
            &config,
            &manifest_hash,
            &persistence.profile_path,
            options.profile,
            repetitions,
            deadline_at,
            &mut progress,
        )
        .await;
        match lifecycle_result {
            Ok(expired) => interrupted = expired,
            Err(error) => {
                let reason = format!("lifecycle collection aborted: {error}");
                for id in [3, 4] {
                    if !progress.finalized.contains(&id) {
                        progress.report.results[id] =
                            row(id, TestOutcome::Error(reason.clone()), &config);
                        progress
                            .details
                            .get_mut(ROWS[id])
                            .expect("storage detail exists")["reason"] = json!(reason);
                    }
                }
                progress.report.lifecycle_notes.push(reason);
            }
        }
    }
    if interrupted {
        for (i, _slug) in ROWS.iter().enumerate() {
            if progress.finalized.contains(&i) {
                continue;
            }
            let reason = if ([0, 2, 6, 7].contains(&i)
                && progress.curve_trials.len() == repetitions as usize)
                || ([3, 4].contains(&i)
                    && progress
                        .details
                        .get(ROWS[i])
                        .and_then(|d| d["trials"].as_array())
                        .is_some_and(|trials| {
                            trials.len() == repetitions as usize
                                && trials.iter().all(|t| t["measurement_complete"] == true)
                        })) {
                "deadline interrupted final evaluation"
            } else {
                "deadline"
            };
            progress.report.results[i] = row(i, TestOutcome::Error(reason.into()), &config);
            progress
                .details
                .entry(ROWS[i].into())
                .and_modify(|v| v["reason"] = json!(reason));
        }
        crate::report::write_failure_diagnostic(
            &options.output,
            &options.manifest,
            &AhrbError::Timeout(format!("storage deadline after {}s", budget.seconds)),
        )?;
    }
    if let Err(error) = ensure_owned_cleanup() {
        let reason = format!("owned cleanup: {error}");
        progress.report.results[0] = row(0, TestOutcome::Error(reason.clone()), &config);
        progress.report.lifecycle_notes.push(reason);
    }
    capture_provider(&mut progress).await?;
    preserve_partial_trial(&mut progress, &config)?;
    // Keep completed trial evidence even when later repetitions or collectors failed.
    if !progress.write_trials.is_empty() {
        let reason = outcome_reason(&progress.report.results[0].outcome);
        progress.details.insert(
            ROWS[0].into(),
            serde_json::to_value(RowDetails {
                measurement_label: MEASUREMENT_LABEL.into(),
                reason,
                trials: progress.write_trials,
            })?,
        );
    }
    if !progress.curve_trials.is_empty() {
        let reason = outcome_reason(&progress.report.results[2].outcome);
        progress.details.insert(
            ROWS[2].into(),
            serde_json::to_value(RowDetails {
                measurement_label: MEASUREMENT_LABEL.into(),
                reason,
                trials: progress.curve_trials,
            })?,
        );
    }
    for (id, trials) in [
        (6, serde_json::to_value(&progress.auxiliary_trials)?),
        (7, serde_json::to_value(&progress.retention_trials)?),
    ] {
        let detail = progress.details.get_mut(ROWS[id]).expect("storage detail");
        detail["trials"] = trials;
        detail["reason"] = json!(outcome_reason(&progress.report.results[id].outcome));
    }
    progress.details.get_mut(ROWS[7]).expect("S8 detail")["matches"] =
        serde_json::to_value(&progress.report.request_body_matches)?;
    bind_evidence(&mut progress.report, &mut progress.details, &options.output)?;
    progress.report.details = progress.details.into();
    let summary = progress
        .report
        .storage_summary
        .as_ref()
        .ok_or_else(|| AhrbError::Protocol("storage summary disappeared".into()))?;
    progress.report.metrics = summary.numeric_metrics()?;
    progress.report.badge = storage_badge(summary, &progress.report.results, &config)
        .map(crate::report::ReportBadge::Storage);
    write_area_receipts(&options.output, &progress.report, &config)?;
    let code = if interrupted {
        2
    } else {
        suite_exit_code(&progress.report.results, None, &manifest)
    };
    crate::results::persist_report(&persistence, &progress.report, options.junit, interrupted)?;
    println!("{}", contract::render_markdown(summary, &progress.report));
    println!("storage bundle {} exit {code}", options.output.display());
    Ok(code)
}

fn common_prerequisite(manifest: &Manifest) -> Option<String> {
    for name in ["sessions", "resume"] {
        if !manifest.capabilities.required.contains_key(name)
            && !manifest.capabilities.optional.contains_key(name)
        {
            return Some(format!("missing required {name} declaration"));
        }
    }
    if manifest.transport.kind == TransportKind::Exec {
        if manifest.sessions.continue_turn.is_empty() && manifest.sessions.resume.is_empty() {
            return Some("missing public continuation binding".into());
        }
    } else if manifest.sessions.create.is_empty()
        || manifest.sessions.submit.is_empty()
        || manifest.sessions.attach.is_empty()
        || manifest.sessions.resume.is_empty()
    {
        return Some("missing required session operation binding".into());
    }
    None
}

/// Exec clients must expose a declared structured terminal before attach can
/// reap them. Exit-only synthesis happens after reap in the current driver and
/// cannot establish the final owned-process counter receipt. S3 shares this
/// task, so neither row starts a partial workload for an unavailable boundary.
fn pre_reap_unavailability(manifest: &Manifest) -> Option<String> {
    if manifest.transport.kind != TransportKind::Exec {
        return None;
    }
    let events = &manifest.events;
    let terminal_rules = events
        .rules
        .iter()
        .filter(|rule| {
            matches!(
                rule.event.as_str(),
                "terminal-success" | "terminal-failure" | "terminal-cancelled" | "terminal-timeout"
            )
        })
        .count();
    let source_available = matches!(events.source.as_str(), "stdout" | "journal-file");
    let framing_available = matches!(events.framing.as_str(), "jsonl" | "json-seq" | "json");
    // The ordinary task requires successful turns. Failure-only mappings cannot
    // expose their boundary, and the out-of-band parser does not expand arrays.
    let success_rules = events
        .rules
        .iter()
        .filter(|rule| rule.event == "terminal-success" && rule.expand_pointer.is_empty())
        .count();
    if source_available && framing_available && success_rules > 0 {
        return None;
    }
    Some(format!(
        "pre-reap-observation-unavailable: transport=exec source={} framing={} terminal_rules={terminal_rules} compatible_success_rules={success_rules}; no compatible declared structured success terminal before client reap; driver exit-terminal synthesis occurs after reap and cannot supply mandatory final owned-process counter receipts; shared S1/S3/S7/S8 task not started",
        events.source, events.framing
    ))
}

fn outcome_reason(outcome: &TestOutcome) -> Option<String> {
    match outcome {
        TestOutcome::Pass => None,
        TestOutcome::Error(r)
        | TestOutcome::Absent(r)
        | TestOutcome::Unsupported(r)
        | TestOutcome::Fail(r) => Some(r.clone()),
    }
}

#[derive(Default)]
struct Counters {
    unavailable: Option<String>,
    writes: TreeDiskTracker,
    reads: TreeDiskTracker,
    first: BTreeMap<crate::process::ProcIdentity, u64>,
    last_capture_ns: u64,
}

impl Counters {
    fn observe(
        &mut self,
        sampler: &mut dyn Sampler,
        roots: &[u32],
        report: &mut Report,
        repetition: u32,
        turn: u32,
    ) -> Result<()> {
        if self.unavailable.is_some() {
            return Ok(());
        }
        if turn == 0 {
            match sampler.disk_counter_preflight() {
                Err(AhrbError::Unsupported(reason)) => {
                    let reason = format!("os-limited: {reason}");
                    report
                        .lifecycle_notes
                        .push(format!("S1 preflight: {reason}"));
                    self.unavailable = Some(reason);
                    return Ok(());
                }
                result => result?,
            }
        }
        let start = monotonic_timestamp_ns();
        let cpu = sampler_thread_cpu_ns()?;
        let tree = sampler.discover(roots)?;
        report.membership.push(crate::report::MembershipSample {
            elapsed_ns: start,
            phase: format!("storage-r{repetition}-t{turn}"),
            discovery_wall_ns: monotonic_timestamp_ns().saturating_sub(start),
            discovery_cpu_ns: sampler_thread_cpu_ns()?.saturating_sub(cpu),
            lane: 0,
        });
        let mut observation = match sampler.disk_counters(&tree) {
            Err(AhrbError::Unsupported(reason)) if turn == 0 => {
                let reason = format!("os-limited: {reason}");
                report
                    .lifecycle_notes
                    .push(format!("S1 preflight: {reason}"));
                self.unavailable = Some(reason);
                return Ok(());
            }
            result => result?,
        };
        // Select one complete source: per-process. Never sum a cgroup and processes.
        observation.cgroup_write_bytes = None;
        for (id, bytes) in &observation.write_bytes_by_identity {
            self.first.entry(*id).or_insert(*bytes);
        }
        let mut reads = ProcessDiskObservation {
            expected_identities: observation.expected_identities.clone(),
            ..ProcessDiskObservation::default()
        };
        for id in &reads.expected_identities {
            if let Some(bytes) = sampler.disk_read_counter_for_identity(*id)? {
                reads.write_bytes_by_identity.insert(*id, bytes);
            }
        }
        self.writes.observe(&observation)?;
        self.reads.observe(&reads)?;
        let now = monotonic_timestamp_ns();
        if now.saturating_sub(self.last_capture_ns) >= 100_000_000 {
            let sample = sampler.sample(&tree, &format!("storage-r{repetition}-t{turn}"))?;
            report.processes.extend(sample.process_samples.clone());
            report.samples.push(sample);
            self.last_capture_ns = now;
        }
        Ok(())
    }
    fn retire_clients(
        &mut self,
        sampler: &mut dyn Sampler,
        clients: &BTreeSet<crate::process::ProcIdentity>,
    ) -> Result<()> {
        if self.unavailable.is_some() {
            return Ok(());
        }
        for identity in clients {
            self.writes.note_structured_terminal(*identity)?;
            self.reads.note_structured_terminal(*identity)?;
            let missing = || {
                AhrbError::Protocol(format!(
                    "storage missing client terminal-before-reap receipt ({},{})",
                    identity.pid, identity.start_time,
                ))
            };
            let bytes = sampler
                .disk_counter_for_identity(*identity)?
                .ok_or_else(missing)?;
            self.writes
                .record_final_sample_before_reap(*identity, bytes)?;
            self.writes.retire_after_final_sample(*identity)?;
            let bytes = sampler
                .disk_read_counter_for_identity(*identity)?
                .ok_or_else(missing)?;
            self.reads
                .record_final_sample_before_reap(*identity, bytes)?;
            self.reads.retire_after_final_sample(*identity)?;
        }
        Ok(())
    }
    fn receipts(&self, source: &str) -> Vec<IdentityReceipt> {
        self.writes
            .snapshot()
            .identities
            .into_iter()
            .map(|r| IdentityReceipt {
                pid: r.identity.pid,
                start_time: r.identity.start_time,
                source: source.into(),
                first_bytes: self.first.get(&r.identity).copied(),
                last_bytes: r.write_bytes,
                retirement_method: format!("{:?}", r.status),
                complete: matches!(
                    r.status,
                    DiskIdentityStatus::Live
                        | DiskIdentityStatus::RetiredAfterFinalSample
                        | DiskIdentityStatus::RetiredByDurableCgroup
                ),
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_repetition(
    manifest: &Manifest,
    config: &StorageConfig,
    manifest_hash: &str,
    workflow: &Workflow,
    run_root: &Path,
    repetition: u32,
    n: u32,
    progress: &mut Progress,
) -> Result<()> {
    let profile = run_root.join(format!("s-r{repetition}"));
    prepare_profile(manifest, &profile)?;
    let initial_workspace = profile.join("storage-workspace");
    std::fs::create_dir(&initial_workspace)?;
    let ScriptedPillarRuntime {
        engine,
        server,
        mut driver,
    } = start_scripted_pillar_runtime(
        manifest,
        manifest_hash,
        workflow,
        &profile,
        "storage",
        Some(&initial_workspace),
    )
    .await?;
    engine.enable_storage_body_capture(&progress.output.join("request-bodies"))?;
    progress.active_engine = Some((repetition, Arc::clone(&engine), 0, 0));
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if std::fs::metadata(&workspace)?.dev() != std::fs::metadata(&profile)?.dev() {
            return Err(AhrbError::Validation(
                "storage workspace crosses device".into(),
            ));
        }
    }
    fixture::seed(&workspace)?;
    let per_invocation = per_invocation_topology(manifest);
    let exec_clients = manifest.transport.kind == TransportKind::Exec;
    let mut sampler = platform_sampler();
    let mut counters = Counters::default();
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
    // An explicit backend UNSUPPORTED is a pre-stimulus feasibility result.
    // Per-invocation has an empty owned tree here; it still probes backend support.
    counters.observe(
        sampler.as_mut(),
        &daemon_roots,
        &mut progress.report,
        repetition,
        0,
    )?;
    let baseline = accounting::settle(&profile, config, monotonic_timestamp_ns()).await?;
    if !per_invocation {
        counters.observe(
            sampler.as_mut(),
            &daemon_roots,
            &mut progress.report,
            repetition,
            0,
        )?;
    }
    record_boundary(
        &mut progress.report,
        config,
        repetition,
        0,
        &baseline,
        &counters,
    );
    audit_log_declarations(
        manifest,
        &profile,
        &workspace,
        &session,
        repetition,
        0,
        &baseline.inventory,
        progress,
    )?;
    let initial_allocated = baseline.inventory.allocated_bytes();
    let baseline_content = retention::baseline_bytes(&profile, &baseline.inventory)?;
    let baseline_inventory = baseline.inventory.clone();
    let fixture_paths = (b'a'..=b'e')
        .map(|letter| {
            workspace
                .join(format!("context-{}.txt", letter as char))
                .strip_prefix(&profile)
                .map(|p| p.to_string_lossy().into_owned())
                .map_err(|e| AhrbError::Protocol(e.to_string()))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut family_snapshots = vec![(0, baseline.inventory.clone())];
    let mut previous = baseline.inventory;
    let mut points = vec![Checkpoint {
        turn: 0,
        allocated_bytes: initial_allocated,
    }];
    let mut writes = Vec::new();
    let mut growth = 0_u64;
    let mut after = None;
    let mut counter_error = None;

    for turn in 1..=n {
        let before_disk = counters.writes.snapshot();
        let previous_boundary_count = driver.completed_turn_boundaries().len();
        let key = format!("storage-r{repetition}-t{turn:04}");
        let submit_ns = monotonic_timestamp_ns();
        let started = Instant::now();
        driver
            .submit(&session, &fixture::prompt(turn)?, &key)
            .await?;
        // Exec clients are direct owned roots even when a warm daemon persists.
        let mut roots = daemon_roots.clone();
        roots.extend(driver.owned_pids());
        roots.sort_unstable();
        roots.dedup();
        let client_roots = if exec_clients {
            driver.session_pids(&session)
        } else {
            Vec::new()
        };
        let mut client_sampler = platform_sampler();
        let mut clients = BTreeSet::new();
        if exec_clients && client_roots.is_empty() {
            return Err(AhrbError::Protocol(
                "storage missing owned exec client root".into(),
            ));
        }
        if roots.is_empty() {
            return Err(AhrbError::Protocol(
                "storage missing owned process roots".into(),
            ));
        }
        let pre_reap = if exec_clients {
            if manifest.events.source == "journal-file" {
                Some(PathBuf::from(crate::manifest::render_template(
                    &manifest.events.path,
                    &BTreeMap::from([
                        ("profile".into(), profile.to_string_lossy().into_owned()),
                        ("session_id".into(), session.0.clone()),
                    ]),
                )?))
            } else {
                driver.live_event_path(&session)
            }
        } else {
            None
        };
        if exec_clients && pre_reap.is_none() {
            return Err(AhrbError::Protocol(
                "storage missing client terminal-before-reap event path".into(),
            ));
        }
        if let Some(path) = pre_reap {
            loop {
                // This independent discovery follows only the active client roots,
                // including descendants/groups, never the still-live daemon tree.
                let tree = client_sampler.discover(&client_roots)?;
                clients.extend(tree.members.keys().copied());
                if client_roots
                    .iter()
                    .any(|pid| !clients.iter().any(|id| id.pid == *pid))
                {
                    return Err(AhrbError::Protocol(
                        "storage missing client launch identity receipt".into(),
                    ));
                }
                counters.observe(
                    sampler.as_mut(),
                    &roots,
                    &mut progress.report,
                    repetition,
                    turn,
                )?;
                if row47_terminal_record_count(&path, manifest)?
                    >= if manifest.events.source == "journal-file" {
                        turn as u64
                    } else {
                        1
                    }
                {
                    counters.retire_clients(sampler.as_mut(), &clients)?;
                    progress.report.lifecycle_notes.push(format!(
                        "storage-client-retirement {}", serde_json::to_string(&json!({
                            "repetition":repetition,"turn":turn,"identities":counters.receipts(counter_source()).into_iter().filter(|r| clients.iter().any(|id| id.pid == r.pid && id.start_time == r.start_time)).collect::<Vec<_>>()
                        }))?
                    ));
                    break;
                }
                if started.elapsed() >= outer_turn_timeout(manifest) {
                    counter_error = Some("missing terminal-before-reap receipt".into());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        // The same cursor-based submit/terminal loop as run_long_horizon; storage
        // additionally checks successful terminal, exact cadence and tool call ID.
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
                    result=&mut terminal => break result?,
                    _=tokio::time::sleep(Duration::from_millis(10)), if !exec_clients => {counters.observe(sampler.as_mut(),&roots,&mut progress.report,repetition,turn)?;}
                }
            }
        };
        let events = driver.attach(&session, after).await?;
        if events.iter().filter(|e| is_terminal(&e.event)).count() != 1
            || !events
                .iter()
                .any(|e| e.event == EventVocab::TerminalSuccess)
        {
            return Err(AhrbError::Protocol(
                "storage task-incomplete: expected exactly one successful terminal".into(),
            ));
        }
        if turn % 10 == 0
            && !tools
                .iter()
                .any(|t| t.call_id == format!("storage-read-{turn:04}"))
        {
            return Err(AhrbError::Protocol(
                "storage task-incomplete: missing correlated fixture read".into(),
            ));
        }
        if exec_clients {
            // Retirement was captured before any attach can reap the client.
            // Wait for that reap before sampling the warm tree again, otherwise
            // the sampler can rediscover a terminal client during its exit tail.
            await_completed_turn_boundary(
                &mut driver,
                &session,
                Some(cursor),
                previous_boundary_count,
                outer_turn_timeout(manifest),
            )
            .await?;
        }
        after = Some(cursor);
        progress.report.events.extend(
            events
                .into_iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        );
        progress.report.turns.push(TurnObservation {
            repetition,
            turn_index: turn,
            actor: "storage".into(),
            session_id_hash: stable_evidence_hash(&session.0),
            phase: TASK.into(),
            launch_ns: None,
            submit_ns: Some(submit_ns),
            first_model_request_ns: None,
            terminal_ns: Some(terminal_ns),
            exit_ns: None,
            turn_wall_ns: Some(terminal_ns.saturating_sub(submit_ns)),
        });
        if let Some(s) = &mut progress.report.storage_summary {
            s.completed_turns += 1;
        }
        let boundary = {
            let settled = accounting::settle(&profile, config, terminal_ns);
            tokio::pin!(settled);
            loop {
                tokio::select! {
                    result=&mut settled => break result?,
                    _=tokio::time::sleep(Duration::from_millis(20)), if !per_invocation => {counters.observe(sampler.as_mut(),&daemon_roots,&mut progress.report,repetition,turn)?;}
                }
            }
        };
        capture_provider(progress).await?;
        let expected_checkpoint = if turn % 10 == 0 {
            format!("t{turn:04}-terminal")
        } else {
            format!("t{turn:04}")
        };
        if !progress.report.model_requests.iter().any(|record| {
            record["repetition"].as_u64() == Some(repetition as u64)
                && record["accepted"] == true
                && record["request"]["scenario"] == TASK
                && record["request"]["actor"] == "storage"
                && record["request"]["checkpoint"] == expected_checkpoint
        }) {
            return Err(AhrbError::Protocol(
                "storage task-incomplete: missing accepted terminal provider response".into(),
            ));
        }
        if driver
            .attach(&session, after)
            .await?
            .iter()
            .any(|event| is_terminal(&event.event))
        {
            return Err(AhrbError::Protocol(
                "storage task-incomplete: extra terminal after the completed turn".into(),
            ));
        }
        if !per_invocation {
            counters.observe(
                sampler.as_mut(),
                &daemon_roots,
                &mut progress.report,
                repetition,
                turn,
            )?;
        }
        let current = counters.writes.snapshot();
        if counters.unavailable.is_some() {
            // Preserve null physical measurements; allocation collection continues.
        } else if let Some(delta) = before_disk
            .cumulative_write_bytes
            .zip(current.cumulative_write_bytes)
            .and_then(|(b, a)| a.checked_sub(b))
        {
            writes.push(delta);
        } else {
            counter_error = Some("incomplete live/retired physical counters".into());
        }
        growth = growth
            .checked_add(boundary.inventory.growth_since(&previous))
            .ok_or_else(|| AhrbError::Protocol("storage growth overflow".into()))?;
        if checkpoints(n).contains(&turn) {
            family_snapshots.push((turn, boundary.inventory.clone()));
            points.push(Checkpoint {
                turn,
                allocated_bytes: boundary.inventory.allocated_bytes(),
            });
        }
        record_boundary(
            &mut progress.report,
            config,
            repetition,
            turn,
            &boundary,
            &counters,
        );
        audit_log_declarations(
            manifest,
            &profile,
            &workspace,
            &session,
            repetition,
            turn,
            &boundary.inventory,
            progress,
        )?;
        previous = boundary.inventory;
        if turn % 10 == 0 {
            println!(
                "storage repetition {repetition} turn {turn}/{n} allocated {}",
                previous.allocated_bytes()
            );
        }
    }
    capture_provider(progress).await?;
    let audited_receipts = captured_body_receipts(&progress.output, repetition)?;
    progress.auxiliary_trials.push(auxiliaries::evaluate(
        repetition,
        n,
        &family_snapshots,
        config,
    )?);
    match retention::collect(
        repetition,
        &profile,
        config,
        &baseline_inventory,
        &baseline_content,
        &previous,
        &fixture_paths,
        &engine.request_records().await,
        &progress.output,
    ) {
        Ok((mut trial, matches)) => {
            let first = progress.report.request_body_matches.len() as u64;
            trial.diagnostics.match_refs = (first..first + matches.len() as u64).collect();
            progress.report.request_body_matches.extend(matches);
            progress.retention_trials.push(trial);
        }
        Err(error) => {
            let reason = format!("S8 capture/audit error: {error}");
            progress.retention_trials.push(Trial {
                repetition,
                outcome: TestOutcome::Error(reason.clone()),
                measurement_complete: false,
                reason: Some(reason),
                summary: RetentionSummary::default(),
                diagnostics: RetentionDiagnostics {
                    coverage: Coverage::CaptureError,
                    body_blobs: captured_body_blobs(&progress.output, repetition)?,
                    baseline_exclusions: Vec::new(),
                    match_refs: Vec::new(),
                },
                evidence_refs: Vec::new(),
            });
        }
    }
    let (class, slope, diagnostics) = evaluate_curve(&points, n)?;
    let curve = points
        .iter()
        .map(|p| CurvePoint {
            turn: p.turn,
            allocated_bytes: p.allocated_bytes as f64,
            mad_bytes: 0.0,
        })
        .collect();
    progress.curve_trials.push(Trial {
        repetition,
        outcome: if class == GrowthClass::Superlinear {
            TestOutcome::Fail("superlinear sampled allocated growth".into())
        } else {
            TestOutcome::Pass
        },
        measurement_complete: true,
        reason: None,
        summary: FootprintSummary {
            footprint_curve: curve,
            first_turn_allocated_bytes: Some(points[1].allocated_bytes as f64),
            footprint_slope_bytes_per_turn: Some(slope),
            growth_class: Some(class),
        },
        diagnostics,
        evidence_refs: Vec::new(),
    });
    let complete = counter_error.is_none()
        && writes.len() == n as usize
        && counters.writes.snapshot().counter_complete;
    let physical = writes
        .iter()
        .try_fold(0_u64, |total, value| total.checked_add(*value))
        .ok_or_else(|| AhrbError::Protocol("storage physical-write total overflow".into()))?;
    let distribution = writes.iter().map(|v| *v as f64).collect::<Vec<_>>();
    let net = i64::try_from(previous.allocated_bytes())
        .ok()
        .zip(i64::try_from(initial_allocated).ok())
        .and_then(|(a, b)| a.checked_sub(b));
    let write_summary = if complete {
        WriteSummary {
            write_bytes_per_turn_p50: median(distribution.clone()),
            write_bytes_per_turn_p95: p95(distribution.clone()),
            write_bytes_per_turn_max: writes.iter().max().map(|v| *v as f64),
            logical_growth_bytes_per_turn: Some(growth as f64 / n as f64),
            net_growth_bytes_per_turn: net.map(|v| v as f64 / n as f64),
            write_amplification_ratio: (growth > 0).then(|| physical as f64 / growth as f64),
            disk_class: p95(distribution).map(|b| disk_class(b).into()),
        }
    } else {
        WriteSummary::default()
    };
    progress.write_trials.push(Trial {
        repetition,
        outcome: if let Some(reason) = &counters.unavailable {
            TestOutcome::Unsupported(reason.clone())
        } else if complete {
            TestOutcome::Pass
        } else {
            TestOutcome::Error(
                counter_error
                    .clone()
                    .unwrap_or_else(|| "incomplete retirement".into()),
            )
        },
        measurement_complete: complete || counters.unavailable.is_some(),
        reason: counters.unavailable.clone().or(counter_error),
        summary: write_summary,
        diagnostics: WriteDiagnostics {
            physical_write_bytes: complete.then_some(physical),
            logical_growth_bytes: Some(growth),
            net_growth_bytes: net,
            amplification_reason: (growth == 0).then(|| "zero-denominator".into()),
            counter_complete: complete,
            identities: counters.receipts(counter_source()),
        },
        evidence_refs: Vec::new(),
    });
    // Shutdown is teardown, after the final evidence; never call close/delete.
    driver.shutdown().await?;
    server.shutdown().await?;
    capture_provider(progress).await?;
    let final_receipts = captured_body_receipts(&progress.output, repetition)?;
    if let Some(trial) = progress.retention_trials.last_mut() {
        finalize_retention_capture(trial, &audited_receipts, &final_receipts);
    }
    progress
        .report
        .lifecycle_notes
        .extend(driver.lifecycle_notes());
    Ok(())
}

fn counter_source() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos-ri_diskio_byteswritten"
    } else if cfg!(target_os = "linux") {
        "linux-proc-pid-io-write_bytes"
    } else {
        "unavailable"
    }
}

fn record_boundary(
    report: &mut Report,
    config: &StorageConfig,
    repetition: u32,
    turn: u32,
    b: &accounting::SettledInventory,
    counters: &Counters,
) {
    let boundary = format!("r{repetition}-t{turn:04}");
    let writes = counters.writes.snapshot();
    let reads = counters.reads.snapshot();
    report.storage_samples.push(StorageSample {
        row_id: ROWS[0].into(),
        repetition,
        session_ordinal: 1,
        turn,
        boundary: boundary.clone(),
        monotonic_ns: monotonic_timestamp_ns(),
        settle_ms: b.settle_ms,
        sync_start_ns: b.sync_start_ns,
        sync_end_ns: b.sync_end_ns,
        allocated_bytes: b.inventory.allocated_bytes(),
        apparent_bytes: b.inventory.apparent_bytes(),
        regular_files: b.inventory.regular_files(),
        families: b.inventory.families(config),
        physical_write_bytes: counters
            .unavailable
            .is_none()
            .then_some(writes.cumulative_write_bytes)
            .flatten(),
        physical_read_bytes: counters
            .unavailable
            .is_none()
            .then_some(reads.cumulative_write_bytes)
            .flatten(),
        counter_source: if counters.unavailable.is_some() {
            "unavailable"
        } else {
            counter_source()
        }
        .into(),
        counter_complete: counters.unavailable.is_none() && writes.counter_complete,
    });
    for entry in &b.inventory.entries {
        report.storage_files.push(StorageFile {
            row_id: ROWS[0].into(),
            repetition,
            boundary: boundary.clone(),
            entry: entry.clone(),
        });
    }
}

fn finalize(progress: &mut Progress, config: &StorageConfig) -> Result<()> {
    let summary = progress
        .report
        .storage_summary
        .as_mut()
        .ok_or_else(|| AhrbError::Protocol("storage summary absent".into()))?;
    summary.auxiliaries = auxiliaries::aggregate(&progress.auxiliary_trials);
    let retained = retention::aggregate(&progress.retention_trials)?;
    summary.request_retention_class = retained.request_retention_class;
    summary.stored_request_bytes = retained.stored_request_bytes;
    summary.unique_request_content_bytes = retained.unique_request_content_bytes;
    summary.stored_unique_ratio = retained.stored_unique_ratio;
    for (id, outcomes) in [
        (
            6,
            progress
                .auxiliary_trials
                .iter()
                .map(|t| t.outcome.clone())
                .collect::<Vec<_>>(),
        ),
        (
            7,
            progress
                .retention_trials
                .iter()
                .map(|t| t.outcome.clone())
                .collect::<Vec<_>>(),
        ),
    ] {
        let outcome = outcomes
            .iter()
            .find(|o| matches!(o, TestOutcome::Error(_)))
            .or_else(|| {
                outcomes
                    .iter()
                    .find(|o| matches!(o, TestOutcome::Unsupported(_)))
            })
            .cloned()
            .unwrap_or(TestOutcome::Pass);
        progress.report.results[id] = row(id, outcome, config);
    }
    let classes = progress
        .curve_trials
        .iter()
        .filter_map(|t| t.summary.growth_class)
        .collect::<Vec<_>>();
    summary.growth_class = classes.iter().max().copied();
    for turn in checkpoints(summary.turn_budget) {
        let values = progress
            .curve_trials
            .iter()
            .filter_map(|t| {
                t.diagnostics
                    .checkpoints
                    .iter()
                    .find(|p| p.turn == turn)
                    .map(|p| p.allocated_bytes as f64)
            })
            .collect::<Vec<_>>();
        let center = median(values.clone())
            .ok_or_else(|| AhrbError::Protocol("storage missing curve aggregate".into()))?;
        summary.footprint_curve.push(CurvePoint {
            turn,
            allocated_bytes: center,
            mad_bytes: median(values.iter().map(|v| (v - center).abs()).collect()).unwrap_or(0.0),
        });
    }
    summary.first_turn_allocated_bytes = median(
        progress
            .curve_trials
            .iter()
            .filter_map(|t| t.summary.first_turn_allocated_bytes)
            .collect(),
    );
    summary.footprint_slope_bytes_per_turn = median(
        progress
            .curve_trials
            .iter()
            .filter_map(|t| t.summary.footprint_slope_bytes_per_turn)
            .collect(),
    );
    progress.report.results[2] = row(
        2,
        if classes.contains(&GrowthClass::Superlinear) {
            TestOutcome::Fail("superlinear in at least one repetition".into())
        } else {
            TestOutcome::Pass
        },
        config,
    );
    let error = progress
        .write_trials
        .iter()
        .any(|t| !t.measurement_complete || matches!(t.outcome, TestOutcome::Error(_)));
    let unavailable = progress.write_trials.iter().find_map(|t| match &t.outcome {
        TestOutcome::Unsupported(reason) => Some(reason.clone()),
        _ => None,
    });
    let outcome = if error {
        TestOutcome::Error("incomplete live/retired counter evidence; see trials".into())
    } else if let Some(reason) = unavailable {
        summary.counter_source = "unavailable".into();
        TestOutcome::Unsupported(reason)
    } else {
        TestOutcome::Pass
    };
    let complete = outcome == TestOutcome::Pass;
    progress.report.results[0] = row(0, outcome, config);
    if complete {
        let trials = &progress.write_trials;
        summary.write_bytes_per_turn_p50 = median(
            trials
                .iter()
                .filter_map(|t| t.summary.write_bytes_per_turn_p50)
                .collect(),
        );
        summary.write_bytes_per_turn_p95 = median(
            trials
                .iter()
                .filter_map(|t| t.summary.write_bytes_per_turn_p95)
                .collect(),
        );
        summary.write_bytes_per_turn_max = trials
            .iter()
            .filter_map(|t| t.summary.write_bytes_per_turn_max)
            .reduce(f64::max);
        summary.logical_growth_bytes_per_turn = median(
            trials
                .iter()
                .filter_map(|t| t.summary.logical_growth_bytes_per_turn)
                .collect(),
        );
        summary.net_growth_bytes_per_turn = median(
            trials
                .iter()
                .filter_map(|t| t.summary.net_growth_bytes_per_turn)
                .collect(),
        );
        summary.write_amplification_ratio = if trials
            .iter()
            .all(|t| t.summary.write_amplification_ratio.is_some())
        {
            median(
                trials
                    .iter()
                    .filter_map(|t| t.summary.write_amplification_ratio)
                    .collect(),
            )
        } else {
            None
        };
        summary.disk_class = summary
            .write_bytes_per_turn_p95
            .map(|b| disk_class(b).into());
    }
    Ok(())
}

pub(crate) fn storage_badge(
    summary: &StorageSummary,
    rows: &[TestResult],
    config: &StorageConfig,
) -> Option<StorageBadge> {
    if rows.len() != 10
        || rows
            .iter()
            .any(|r| matches!(r.outcome, TestOutcome::Error(_)))
        || !rows
            .iter()
            .any(|r| r.id == ROWS[2] && r.outcome == TestOutcome::Pass)
        || (config.delete_declared()
            && !rows
                .iter()
                .any(|r| r.id == ROWS[5] && r.outcome == TestOutcome::Pass))
    {
        return None;
    }
    let disk = summary.disk_class.clone()?;
    let growth = summary.growth_class?;
    let measured = |id: &str| {
        rows.iter().any(|r| {
            r.id == id && r.outcome == TestOutcome::Pass && r.metadata.measurement_complete
        })
    };
    let mut facets = Vec::new();
    if let Some(f) = &summary.durability_class {
        if measured(ROWS[1]) {
            facets.push(format!("F{f}"));
        }
    }
    if measured(ROWS[4]) && summary.close_retention_class == Some(BoundClass::Bounded) {
        facets.push("close-bounded".into());
    }
    if measured(ROWS[5])
        && config
            .session_delete
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        && summary.delete_residue_files == Some(0)
        && summary.delete_residue_allocated_bytes == Some(0)
    {
        facets.push("delete-clean".into());
    }
    if measured(ROWS[5])
        && config
            .uninstall_cleanup
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        && summary.uninstall_residue_files == Some(0)
        && summary.uninstall_residue_allocated_bytes == Some(0)
    {
        facets.push("uninstall-clean".into());
    }
    if measured(ROWS[6])
        && !summary.auxiliaries.is_empty()
        && summary
            .auxiliaries
            .iter()
            .all(|a| a.class == Some(BoundClass::Bounded))
    {
        facets.push("aux-bounded".into());
    }
    if let Some(r) = summary.request_retention_class {
        if measured(ROWS[7]) {
            facets.push(format!("requests-{r:?}").to_lowercase());
        }
    }
    if measured(ROWS[8]) && summary.crash_resume_outcome == Some(CrashOutcome::Preserved) {
        facets.push("crash-preserved".into());
    }
    let mut label = format!(
        "Storage v4 · {} · {} · D{} · G{}",
        summary.os,
        summary.topology,
        disk,
        format!("{growth:?}").to_lowercase()
    );
    if !facets.is_empty() {
        label.push_str(" · ");
        label.push_str(&facets.join("+"));
    }
    Some(StorageBadge {
        spec_version: 4,
        os: summary.os.clone(),
        topology: summary.topology.clone(),
        profile: summary.profile.clone(),
        comparison_scope: summary.comparison_scope.clone(),
        disk_class: disk,
        growth_class: growth,
        facets,
        label,
    })
}

fn bind_evidence(
    report: &mut Report,
    details: &mut BTreeMap<String, Value>,
    output: &Path,
) -> Result<()> {
    fn receipt<T: serde::Serialize>(file: &str, records: &[T]) -> Result<EvidenceRef> {
        let mut bytes = Vec::new();
        for value in records {
            serde_json::to_writer(&mut bytes, value)?;
            bytes.push(b'\n');
        }
        Ok(EvidenceRef {
            file: file.into(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            first_record: None,
            last_record: None,
        })
    }
    let mut refs = vec![
        EvidenceRef {
            file: "storage-log-audit.jsonl".into(),
            sha256: format!(
                "{:x}",
                Sha256::digest(std::fs::read(output.join("storage-log-audit.jsonl"))?)
            ),
            first_record: None,
            last_record: None,
        },
        receipt("storage-samples.jsonl", &report.storage_samples)?,
        receipt("storage-files.jsonl", &report.storage_files)?,
        receipt("processes.jsonl", &report.processes)?,
        receipt("turns.jsonl", &report.turns)?,
        receipt("model-requests.jsonl", &report.model_requests)?,
        receipt("events.jsonl", &report.events)?,
    ];
    refs.push(receipt(
        "request-body-matches.jsonl",
        &report.request_body_matches,
    )?);
    refs.push(receipt("model-requests.jsonl", &report.model_requests)?);
    refs.push(EvidenceRef {
        file: "storage-request-bodies.jsonl".into(),
        sha256: retention::digest(&std::fs::read(output.join("storage-request-bodies.jsonl"))?),
        first_record: None,
        last_record: None,
    });
    for id in [ROWS[0], ROWS[2], ROWS[3], ROWS[4], ROWS[6], ROWS[7]] {
        if let Some(trials) = details
            .get_mut(id)
            .and_then(|d| d.get_mut("trials"))
            .and_then(Value::as_array_mut)
        {
            for trial in trials {
                let mut trial_refs = refs.clone();
                if id == ROWS[7] {
                    if let Some(blobs) = trial["diagnostics"]["body_blobs"].as_array() {
                        for blob in blobs {
                            let path = blob["path"].as_str().ok_or_else(|| {
                                AhrbError::Protocol("S8 missing blob path".into())
                            })?;
                            let bytes = match std::fs::read(output.join(path)) {
                                Ok(bytes) => bytes,
                                Err(_) if trial["measurement_complete"] == false => continue,
                                Err(error) => return Err(error.into()),
                            };
                            trial_refs.push(EvidenceRef {
                                file: path.into(),
                                sha256: retention::digest(&bytes),
                                first_record: None,
                                last_record: None,
                            });
                        }
                    }
                }
                trial["evidence_refs"] = serde_json::to_value(&trial_refs)?;
            }
        }
    }
    for (id, value) in details {
        validate_details(id, value)?;
    }
    Ok(())
}

fn write_fixture_receipts(output: &Path, workflow: &Workflow, manifest: &Manifest) -> Result<()> {
    use crate::fake_model::{
        AnthropicMessagesFrontend, ModelResponse, OpenAiChatFrontend, OpenAiResponsesFrontend,
        ProtocolFrontend,
    };
    let mut file = std::fs::File::create(output.join("storage-fixture.jsonl"))?;
    for (suffix, content) in fixture::contents()? {
        writeln!(
            file,
            "{}",
            json!({"kind":"seed","path":format!("context-{suffix}.txt"),"bytes":content.len(),"sha256":format!("{:x}",Sha256::digest(content))})
        )?;
    }
    for turn in 1..=if workflow.responses.len() > 110 {
        1000
    } else {
        100
    } {
        let prompt = fixture::prompt(turn)?;
        writeln!(
            file,
            "{}",
            json!({"kind":"prompt","turn":turn,"bytes":prompt.len(),"sha256":format!("{:x}",Sha256::digest(prompt))})
        )?;
    }
    // Stable renderer contract uses a pinned request identity. Actual HTTP bodies
    // retain their request-dependent IDs, native paths, tools and protocol framing.
    for response in &workflow.responses {
        for frontend in [
            &OpenAiChatFrontend as &dyn ProtocolFrontend,
            &OpenAiResponsesFrontend,
            &AnthropicMessagesFrontend,
        ] {
            let model = ModelResponse {
                dialect: frontend.dialect().into(),
                model: manifest.fake_model.model.clone(),
                scenario: TASK.into(),
                actor: "storage".into(),
                checkpoint: response.checkpoint.clone(),
                request_hash: "storage-fixture-renderer-v1".into(),
                attempt: 1,
                value: response.response.clone(),
                fault: None,
                retry: false,
                stream: false,
            };
            let rendered = frontend.render(&model)?;
            writeln!(
                file,
                "{}",
                json!({"kind":"rendered-response-contract","dialect":frontend.dialect(),"checkpoint":response.checkpoint,"request_identity":"storage-fixture-renderer-v1","bytes":rendered.body.len(),"sha256":format!("{:x}",Sha256::digest(&rendered.body)),"semantic_bytes":serde_json::to_vec(&response.response)?.len(),"semantic_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&response.response)?))})
            )?;
        }
    }
    Ok(())
}

async fn capture_provider(progress: &mut Progress) -> Result<()> {
    let Some((repetition, engine, request_count, response_count)) = &mut progress.active_engine
    else {
        return Ok(());
    };
    let records = engine.request_records().await;
    if let Some(summary) = &mut progress.report.storage_summary {
        summary.physical_requests += (records.len() - *request_count) as u64;
    }
    // The engine sorts by semantic route; late auxiliary traffic may insert
    // before prior records, and acceptance/attempt totals can change in place.
    progress
        .report
        .model_requests
        .retain(|r| r["repetition"].as_u64() != Some(*repetition as u64));
    for record in &records {
        let mut value = serde_json::to_value(record)?;
        value["repetition"] = json!(*repetition);
        progress.report.model_requests.push(value);
    }
    let mut body_mapping = std::fs::OpenOptions::new()
        .append(true)
        .open(progress.output.join("storage-request-bodies.jsonl"))?;
    for mut receipt in engine.take_storage_body_receipts()? {
        receipt["repetition"] = json!(*repetition);
        let record = records
            .iter()
            .find(|r| Some(r.received_ns) == receipt["received_ns"].as_u64());
        receipt["canonical_sha256"] = json!(
            record
                .map(|r| serde_json::to_vec(&r.request.canonical).map(|b| retention::digest(&b)))
                .transpose()?
        );
        receipt["semantic_ordinal"] = json!(record.map(|r| r.semantic_ordinal));
        receipt["attempt"] = json!(record.map(|r| r.attempt));
        receipt["role"] = json!(record.map(|r| &r.role));
        writeln!(body_mapping, "{receipt}")?;
    }
    *request_count = records.len();
    let responses = engine.storage_response_receipts()?;
    progress
        .report
        .storage_responses
        .extend(
            responses[*response_count..]
                .iter()
                .cloned()
                .map(|response| ResponseReceipt {
                    repetition: *repetition,
                    response,
                }),
        );
    *response_count = responses.len();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn audit_log_declarations(
    manifest: &Manifest,
    profile: &Path,
    workspace: &Path,
    session: &crate::driver::SessionId,
    repetition: u32,
    turn: u32,
    inventory: &accounting::Inventory,
    progress: &mut Progress,
) -> Result<()> {
    let variables = BTreeMap::from([
        ("profile".into(), profile.to_string_lossy().into_owned()),
        ("workspace".into(), workspace.to_string_lossy().into_owned()),
        ("session_id".into(), session.0.clone()),
    ]);
    let journals = row47_render_paths(
        std::iter::once((manifest.events.path.clone(), "journal")).chain(
            manifest
                .resources
                .journal_paths
                .iter()
                .flatten()
                .cloned()
                .map(|p| (p, "journal")),
        ),
        &variables,
        profile,
    )?;
    let logs = row47_render_paths(
        manifest
            .resources
            .log_paths
            .iter()
            .flatten()
            .cloned()
            .map(|p| (p, "log")),
        &variables,
        profile,
    )?;
    let no_log = manifest
        .resources
        .log_paths
        .as_ref()
        .is_some_and(Vec::is_empty);
    let mut contradictions = Vec::new();
    let mut matches = Vec::new();
    for entry in inventory.entries.iter().filter(|e| e.kind == "regular") {
        let path = profile.join(&entry.path);
        let journal = journals.iter().any(|(p, _)| *p == path);
        let log = logs.iter().any(|(p, _)| *p == path);
        if no_log {
            if let Some(reason) =
                no_log_contradiction(Path::new(&entry.path), journal, entry.family == "logs")
            {
                contradictions.push(reason);
            }
        }
        if journal || log || entry.family == "logs" || row47_looks_like_log(Path::new(&entry.path))
        {
            matches.push(json!({"path":entry.path,"journal_locator":journal,"log_locator":log,"family":entry.family}));
        }
    }
    let locator_receipts = journals.iter().chain(&logs).map(|(path, kind)| {
        let relative = path.strip_prefix(profile).map_err(|_| AhrbError::Validation("storage log locator escapes profile".into()))?.to_string_lossy();
        Ok(json!({"path":relative,"kind":kind,"matched":inventory.entries.iter().any(|e| e.path == relative && e.kind == "regular")}))
    }).collect::<Result<Vec<_>>>()?;
    let receipt = json!({
        "repetition":repetition,"turn":turn,"boundary":format!("r{repetition}-t{turn:04}"),
        "log_paths":manifest.resources.log_paths,"journal_paths":manifest.resources.journal_paths,
        "events_path":manifest.events.path,"exhaustive_regular_files":inventory.entries.iter().filter(|e| e.kind=="regular").count(),
        "status":if !contradictions.is_empty() {"contradicted"} else if no_log {"corroborated-no-log"} else if manifest.resources.log_paths.is_none() {"omitted"} else {"declared-paths"},
        "locators":locator_receipts,"matches":matches,"contradictions":contradictions,
    });
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(progress.output.join("storage-log-audit.jsonl"))?;
    writeln!(file, "{receipt}")?;
    if !contradictions.is_empty() {
        return Err(AhrbError::Protocol(contradictions.join("; ")));
    }
    Ok(())
}

fn write_area_receipts(output: &Path, report: &Report, config: &StorageConfig) -> Result<()> {
    let mut file = std::fs::File::create(output.join("storage-area-matches.jsonl"))?;
    for sample in &report.storage_samples {
        let files = report
            .storage_files
            .iter()
            .filter(|f| {
                f.repetition == sample.repetition
                    && f.boundary == sample.boundary
                    && f.entry.kind == "regular"
            })
            .collect::<Vec<_>>();
        for (family, globs) in config.areas.iter().flat_map(|a| a.iter()) {
            for glob in globs {
                let matched = files
                    .iter()
                    .filter(|f| glob_matches(glob, &f.entry.path))
                    .count();
                writeln!(
                    file,
                    "{}",
                    json!({"repetition":sample.repetition,"boundary":sample.boundary,"family":family,"glob":glob,"matched_files":matched,"empty_match":matched==0})
                )?;
            }
        }
    }
    Ok(())
}

fn captured_body_receipts(output: &Path, repetition: u32) -> Result<Vec<Value>> {
    Ok(
        std::fs::read_to_string(output.join("storage-request-bodies.jsonl"))?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|r| r["repetition"].as_u64() == Some(repetition as u64))
            .collect(),
    )
}

fn body_blobs_from_receipts(receipts: &[Value]) -> Vec<BodyBlob> {
    receipts
        .iter()
        .filter_map(|r| {
            Some(BodyBlob {
                semantic_ordinal: r["semantic_ordinal"].as_u64()?,
                attempt: r["attempt"].as_u64()?,
                role: r["role"].as_str()?.into(),
                raw_sha256: r["raw_sha256"].as_str()?.into(),
                canonical_sha256: r["canonical_sha256"].as_str()?.into(),
                path: r["path"].as_str()?.into(),
            })
        })
        .collect()
}

fn captured_body_blobs(output: &Path, repetition: u32) -> Result<Vec<BodyBlob>> {
    Ok(body_blobs_from_receipts(&captured_body_receipts(
        output, repetition,
    )?))
}

fn finalize_retention_capture(
    trial: &mut Trial<RetentionSummary, RetentionDiagnostics>,
    audited_receipts: &[Value],
    final_receipts: &[Value],
) {
    // Raw receipts precede parsing. A malformed late body has no typed blob,
    // but must still invalidate the audit of the earlier request set.
    let captured = body_blobs_from_receipts(final_receipts);
    if audited_receipts != final_receipts
        || final_receipts.len() != captured.len()
        || final_receipts.len() != trial.diagnostics.body_blobs.len()
    {
        let reason = "S8 capture-error: provider capture changed or is incomplete after the final content audit";
        trial.outcome = TestOutcome::Error(reason.into());
        trial.measurement_complete = false;
        trial.reason = Some(reason.into());
        trial.summary = RetentionSummary::default();
        trial.diagnostics.coverage = Coverage::CaptureError;
        trial.diagnostics.body_blobs = captured;
    }
}

/// Retain the last settled checkpoints when a repetition cannot complete. No
/// aggregate or counter total is inferred from incomplete turn coverage.
fn preserve_partial_trial(progress: &mut Progress, config: &StorageConfig) -> Result<()> {
    let Some((repetition, _, _, _)) = &progress.active_engine else {
        return Ok(());
    };
    let repetition = *repetition;
    if matches!(progress.report.results[6].outcome, TestOutcome::Error(_))
        && !progress
            .auxiliary_trials
            .iter()
            .any(|t| t.repetition == repetition)
    {
        let snapshots = progress
            .report
            .storage_samples
            .iter()
            .filter(|s| {
                s.repetition == repetition
                    && contract::checkpoints(
                        progress
                            .report
                            .storage_summary
                            .as_ref()
                            .unwrap()
                            .turn_budget,
                    )
                    .contains(&s.turn)
            })
            .map(|s| {
                let mut inventory = accounting::Inventory::default();
                inventory.entries = progress
                    .report
                    .storage_files
                    .iter()
                    .filter(|f| f.repetition == repetition && f.boundary == s.boundary)
                    .map(|f| f.entry.clone())
                    .collect();
                (s.turn, inventory)
            })
            .collect::<Vec<_>>();
        progress.auxiliary_trials.push(Trial {
            repetition,
            outcome: progress.report.results[6].outcome.clone(),
            measurement_complete: false,
            reason: outcome_reason(&progress.report.results[6].outcome),
            summary: AuxiliarySummary::default(),
            diagnostics: auxiliaries::checkpoint_diagnostics(&snapshots, config),
            evidence_refs: Vec::new(),
        });
    }
    if matches!(progress.report.results[7].outcome, TestOutcome::Error(_))
        && !progress
            .retention_trials
            .iter()
            .any(|t| t.repetition == repetition)
    {
        let body_blobs = captured_body_blobs(&progress.output, repetition)?;
        progress.retention_trials.push(Trial {
            repetition,
            outcome: progress.report.results[7].outcome.clone(),
            measurement_complete: false,
            reason: outcome_reason(&progress.report.results[7].outcome),
            summary: RetentionSummary::default(),
            diagnostics: RetentionDiagnostics {
                coverage: Coverage::CaptureError,
                body_blobs,
                baseline_exclusions: Vec::new(),
                match_refs: Vec::new(),
            },
            evidence_refs: Vec::new(),
        });
    }
    if matches!(progress.report.results[0].outcome, TestOutcome::Error(_))
        && !progress
            .write_trials
            .iter()
            .any(|t| t.repetition == repetition)
    {
        let reason = outcome_reason(&progress.report.results[0].outcome);
        progress.write_trials.push(Trial {
            repetition,
            outcome: progress.report.results[0].outcome.clone(),
            measurement_complete: false,
            reason,
            summary: WriteSummary::default(),
            diagnostics: WriteDiagnostics::default(),
            evidence_refs: Vec::new(),
        });
    }
    if matches!(progress.report.results[2].outcome, TestOutcome::Error(_))
        && !progress
            .curve_trials
            .iter()
            .any(|t| t.repetition == repetition)
    {
        let checkpoints = progress
            .report
            .storage_samples
            .iter()
            .filter(|s| {
                s.repetition == repetition
                    && checkpoints(
                        progress
                            .report
                            .storage_summary
                            .as_ref()
                            .map_or(100, |summary| summary.turn_budget),
                    )
                    .contains(&s.turn)
            })
            .map(|s| Checkpoint {
                turn: s.turn,
                allocated_bytes: s.allocated_bytes,
            })
            .collect();
        progress.curve_trials.push(Trial {
            repetition,
            outcome: progress.report.results[2].outcome.clone(),
            measurement_complete: false,
            reason: outcome_reason(&progress.report.results[2].outcome),
            summary: FootprintSummary::default(),
            diagnostics: CurveEvaluation {
                checkpoints,
                early_bytes_per_turn: None,
                late_bytes_per_turn: None,
                late_early_ratio: None,
                late_range_bytes: None,
            },
            evidence_refs: Vec::new(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn final_capture_rejects_late_unparsed_raw_receipt() {
        let receipt = serde_json::json!({
            "repetition": 1, "semantic_ordinal": 1, "attempt": 1, "role": "primary",
            "raw_sha256": "raw", "canonical_sha256": "canonical", "path": "request-bodies/raw.bin",
            "received_ns": 1
        });
        let audited = vec![receipt.clone()];
        let trial = Trial {
            repetition: 1,
            outcome: TestOutcome::Pass,
            measurement_complete: true,
            reason: None,
            summary: RetentionSummary {
                stored_request_bytes: Some(100),
                ..Default::default()
            },
            diagnostics: RetentionDiagnostics {
                body_blobs: body_blobs_from_receipts(&audited),
                coverage: Coverage::Complete,
                baseline_exclusions: Vec::new(),
                match_refs: Vec::new(),
            },
            evidence_refs: Vec::new(),
        };
        let mut unchanged = trial.clone();
        finalize_retention_capture(&mut unchanged, &audited, &audited);
        assert!(matches!(unchanged.outcome, TestOutcome::Pass));
        assert_eq!(unchanged.summary.stored_request_bytes, Some(100));
        for late in [
            receipt,
            serde_json::json!({
                "repetition": 1, "semantic_ordinal": null, "canonical_sha256": null,
                "raw_sha256": "malformed", "path": "request-bodies/malformed.bin", "received_ns": 2
            }),
        ] {
            let mut final_receipts = audited.clone();
            final_receipts.push(late);
            let mut changed = trial.clone();
            finalize_retention_capture(&mut changed, &audited, &final_receipts);
            assert!(matches!(changed.outcome, TestOutcome::Error(_)));
            assert!(!changed.measurement_complete);
            assert!(changed.summary.stored_request_bytes.is_none());
            assert!(matches!(
                changed.diagnostics.coverage,
                Coverage::CaptureError
            ));
        }
    }

    use super::*;

    #[test]
    fn pre_reap_feasibility_uses_transport_and_declared_terminal_rules() {
        let mut manifest = crate::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
            .expect("mock manifest");
        for source in ["stdout", "journal-file"] {
            manifest.events.source = source.into();
            for framing in ["jsonl", "json-seq", "json"] {
                manifest.events.framing = framing.into();
                assert!(pre_reap_unavailability(&manifest).is_none());
            }
        }
        manifest.events.source = "http".into();
        assert!(pre_reap_unavailability(&manifest).is_some());
        manifest.events.source = "stdout".into();
        manifest.events.framing = "plaintext".into();
        assert!(pre_reap_unavailability(&manifest).is_some());
        manifest.events.framing = "jsonl".into();
        for rule in &mut manifest.events.rules {
            if rule.event == "terminal-success" {
                rule.expand_pointer = "/content".into();
            }
        }
        assert!(pre_reap_unavailability(&manifest).is_some());
        manifest
            .events
            .rules
            .retain(|r| r.event != "terminal-success");
        assert!(pre_reap_unavailability(&manifest).is_some());
        manifest
            .events
            .rules
            .retain(|r| !r.event.starts_with("terminal-"));
        assert!(pre_reap_unavailability(&manifest).is_some());
        // A daemon behind an exec transport still has retiring exec clients.
        manifest.daemon.persistent = true;
        assert!(pre_reap_unavailability(&manifest).is_some());
        let daemon = crate::manifest::load(Path::new("adapters/mock/manifest.toml"))
            .expect("daemon manifest");
        assert!(pre_reap_unavailability(&daemon).is_none());
    }

    struct UnavailableSampler;
    impl Sampler for UnavailableSampler {
        fn discover(&mut self, _roots: &[u32]) -> Result<crate::process::ProcessTree> {
            Ok(crate::process::ProcessTree::default())
        }
        fn sample(
            &mut self,
            _tree: &crate::process::ProcessTree,
            _phase: &str,
        ) -> Result<crate::process::Sample> {
            Err(AhrbError::Protocol(
                "unexpected sample after failed feasibility".into(),
            ))
        }
        fn disk_counters(
            &mut self,
            _tree: &crate::process::ProcessTree,
        ) -> Result<ProcessDiskObservation> {
            Err(AhrbError::Unsupported(
                "explicit test backend unavailability".into(),
            ))
        }
    }

    #[test]
    fn preflight_unavailability_is_null_and_late_loss_remains_an_error() {
        let mut counters = Counters::default();
        let mut report = Report::default();
        counters
            .observe(&mut UnavailableSampler, &[], &mut report, 1, 0)
            .expect("verified feasibility");
        assert!(
            counters
                .unavailable
                .as_deref()
                .expect("reason")
                .starts_with("os-limited:")
        );
        record_boundary(
            &mut report,
            &StorageConfig::default(),
            1,
            0,
            &accounting::SettledInventory {
                inventory: accounting::Inventory::default(),
                settle_ms: 2100.0,
                sync_start_ns: 1,
                sync_end_ns: 2,
            },
            &counters,
        );
        let sample = &report.storage_samples[0];
        assert!(sample.physical_write_bytes.is_none() && sample.physical_read_bytes.is_none());
        assert!(!sample.counter_complete);
        assert_eq!(sample.counter_source, "unavailable");
        assert!(
            Counters::default()
                .observe(&mut UnavailableSampler, &[], &mut report, 1, 1)
                .is_err()
        );
    }

    #[test]
    fn client_retirement_preserves_warm_daemon_and_rejects_lost_receipts() {
        use crate::process::ProcIdentity;
        struct ReceiptSampler(bool);
        impl Sampler for ReceiptSampler {
            fn discover(&mut self, _: &[u32]) -> Result<crate::process::ProcessTree> {
                unreachable!()
            }
            fn sample(
                &mut self,
                _: &crate::process::ProcessTree,
                _: &str,
            ) -> Result<crate::process::Sample> {
                unreachable!()
            }
            fn disk_counter_for_identity(&mut self, _: ProcIdentity) -> Result<Option<u64>> {
                Ok(self.0.then_some(100))
            }
            fn disk_read_counter_for_identity(&mut self, _: ProcIdentity) -> Result<Option<u64>> {
                Ok(self.0.then_some(50))
            }
        }
        let daemon = ProcIdentity {
            pid: 1,
            start_time: 1,
        };
        let client = ProcIdentity {
            pid: 2,
            start_time: 2,
        };
        let child = ProcIdentity {
            pid: 3,
            start_time: 3,
        };
        for available in [true, false] {
            let mut counters = Counters::default();
            let observation = ProcessDiskObservation {
                expected_identities: BTreeSet::from([daemon, client, child]),
                write_bytes_by_identity: BTreeMap::from([(daemon, 10), (client, 20), (child, 30)]),
                ..Default::default()
            };
            counters.writes.observe(&observation).expect("writes");
            counters.reads.observe(&observation).expect("reads");
            let result = counters.retire_clients(
                &mut ReceiptSampler(available),
                &BTreeSet::from([client, child]),
            );
            let snapshot = counters.writes.snapshot();
            assert_eq!(
                snapshot
                    .identities
                    .iter()
                    .find(|r| r.identity == daemon)
                    .expect("daemon")
                    .status,
                DiskIdentityStatus::Live
            );
            if available {
                result.expect("retire clients");
                assert!(
                    snapshot
                        .identities
                        .iter()
                        .filter(|r| r.identity != daemon)
                        .all(|r| r.status == DiskIdentityStatus::RetiredAfterFinalSample)
                );
                assert!(snapshot.counter_complete);
            } else {
                assert!(
                    result
                        .expect_err("lost receipt")
                        .to_string()
                        .contains("missing client terminal-before-reap receipt")
                );
                assert!(!snapshot.counter_complete);
            }
        }
    }

    #[test]
    fn no_log_audit_reuses_journal_exemptions_but_never_exempts_declared_log_families() {
        assert!(no_log_contradiction(Path::new("state/journal.log"), true, false).is_none());
        assert!(no_log_contradiction(Path::new("state/journal.log"), false, false).is_some());
        assert!(no_log_contradiction(Path::new("state/payload.bin"), true, true).is_some());
        assert!(no_log_contradiction(Path::new("state/payload.bin"), false, false).is_none());
    }

    #[test]
    fn badge_facets_require_completed_measurement_and_declared_operations() {
        let config = StorageConfig::default();
        let mut rows = (0..10)
            .map(|i| row(i, TestOutcome::Pass, &config))
            .collect::<Vec<_>>();
        let mut summary = StorageSummary {
            os: "macos".into(),
            topology: "per-invocation".into(),
            profile: "quick".into(),
            comparison_scope: "within-topology-only".into(),
            disk_class: Some("64".into()),
            growth_class: Some(GrowthClass::Bounded),
            durability_class: Some("1".into()),
            delete_residue_files: Some(0),
            delete_residue_allocated_bytes: Some(0),
            ..StorageSummary::default()
        };
        let badge = storage_badge(&summary, &rows, &config).expect("eligible");
        assert_eq!(
            badge.label,
            "Storage v4 · macos · per-invocation · D64 · Gbounded · F1"
        );
        assert_eq!(badge.facets, vec!["F1"]);
        rows[1] = row(
            1,
            TestOutcome::Unsupported("verified unavailable".into()),
            &config,
        );
        assert!(
            storage_badge(&summary, &rows, &config)
                .expect("informational unavailable")
                .facets
                .is_empty()
        );
        rows[2].outcome = TestOutcome::Fail("superlinear".into());
        assert!(storage_badge(&summary, &rows, &config).is_none());
        rows[2].outcome = TestOutcome::Pass;
        rows[9].outcome = TestOutcome::Error("incomplete".into());
        assert!(storage_badge(&summary, &rows, &config).is_none());
        rows[9].outcome = TestOutcome::Pass;
        summary.disk_class = None;
        assert!(storage_badge(&summary, &rows, &config).is_none());
    }
}
