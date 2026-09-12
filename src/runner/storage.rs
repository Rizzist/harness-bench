//! Serialized storage workload. This child module reuses the matrix's public
//! session driver and terminal collector, but not its close/delete oracle.
use super::*;
use crate::process::{DiskIdentityStatus, ProcessDiskObservation, TreeDiskTracker};
use crate::storage::{self as contract, accounting, evidence::*, *};
use std::io::Write as _;

#[allow(dead_code)]
pub(crate) mod lifecycle;

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
    active_engine: Option<(u32, Arc<FakeModelEngine>, usize, usize)>,
    report: Report,
    write_trials: Vec<Trial<WriteSummary, WriteDiagnostics>>,
    curve_trials: Vec<Trial<FootprintSummary, CurveEvaluation>>,
    #[allow(dead_code)]
    auxiliary_trials: Vec<Trial<AuxiliarySummary, AuxiliaryDiagnostics>>,
    #[allow(dead_code)]
    retention_trials: Vec<Trial<RetentionSummary, RetentionDiagnostics>>,
    details: BTreeMap<String, Value>,
    #[allow(dead_code)]
    finalized: BTreeSet<usize>,
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
        details: (0..10)
            .map(|i| (ROWS[i].into(), pending_details(i, pending)))
            .collect(),
        finalized: BTreeSet::new(),
        auxiliary_trials: Vec::new(),
        retention_trials: Vec::new(),
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
                        for i in [0, 2] {
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
                    record_collection_failure(&mut progress, &config, &error);
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
        preserve_partial_trial(&mut progress, &config);
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
            let reason =
                if [0, 2].contains(&i) && progress.curve_trials.len() == repetitions as usize {
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
    if matches!(
        progress.report.results[6].outcome,
        TestOutcome::Unsupported(_)
    ) {
        progress.report.results[3] = row(
            3,
            TestOutcome::Error("row-51 growing session committed 0/0 tool calls/results".into()),
            &config,
        );
        progress.report.results[4] = row(
            4,
            TestOutcome::Unsupported("no-close-without-delete".into()),
            &config,
        );
    }
    if let Err(error) = ensure_owned_cleanup() {
        let reason = format!("owned cleanup: {error}");
        progress.report.results[0] = row(0, TestOutcome::Error(reason.clone()), &config);
        progress.report.lifecycle_notes.push(reason);
    }
    capture_provider(&mut progress).await?;
    preserve_partial_trial(&mut progress, &config);
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
    if !progress.auxiliary_trials.is_empty() {
        progress.details.insert(
            ROWS[6].into(),
            serde_json::to_value(RowDetails::<AuxiliarySummary, AuxiliaryDiagnostics> {
                measurement_label: MEASUREMENT_LABEL.into(),
                reason: outcome_reason(&progress.report.results[6].outcome),
                trials: progress.auxiliary_trials.clone(),
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
    collection_error: Option<String>,
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
        live_descendants: &BTreeSet<crate::process::ProcIdentity>,
    ) -> Result<()> {
        if self.unavailable.is_some() {
            return Ok(());
        }
        let mut errors = Vec::new();
        for identity in clients {
            // An ancestor's terminal does not end a still-live helper's writes.
            // Keep its tracker live for subsequent discovery, but permanently
            // withhold physical aggregates without its own final receipt.
            if live_descendants.contains(identity) {
                errors.push(format!(
                    "storage live owned descendant at client terminal lacks final retirement receipt ({},{})",
                    identity.pid, identity.start_time,
                ));
                continue;
            }
            // A failed direction or earlier child must not suppress later receipts.
            let write = Self::retire_counter(&mut self.writes, *identity, || {
                sampler.disk_counter_for_identity(*identity)
            });
            let read = Self::retire_counter(&mut self.reads, *identity, || {
                sampler.disk_read_counter_for_identity(*identity)
            });
            for (direction, result) in [("write", write), ("read", read)] {
                if let Err(error) = result {
                    errors.push(format!("{direction}: {error}"));
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            let reason = errors.join("; ");
            self.collection_error.get_or_insert_with(|| reason.clone());
            Err(AhrbError::Protocol(reason))
        }
    }
    fn retire_counter(
        tracker: &mut TreeDiskTracker,
        identity: crate::process::ProcIdentity,
        sample: impl FnOnce() -> Result<Option<u64>>,
    ) -> Result<()> {
        tracker.note_structured_terminal(identity)?;
        let bytes = sample()?.ok_or_else(|| {
            AhrbError::Protocol(format!(
                "storage missing client terminal-before-reap receipt ({},{})",
                identity.pid, identity.start_time,
            ))
        })?;
        tracker.record_final_sample_before_reap(identity, bytes)?;
        tracker.retire_after_final_sample(identity)
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
                let discovered = tree
                    .members
                    .iter()
                    .filter(|(id, _)| !clients.contains(*id))
                    .map(|(_, info)| info)
                    .collect::<Vec<_>>();
                if !discovered.is_empty() {
                    progress.report.lifecycle_notes.push(format!("storage-client-discovery {}", serde_json::to_string(&json!({
                        "repetition":repetition,"turn":turn,"monotonic_ns":monotonic_timestamp_ns(),"client_roots":client_roots,"processes":discovered
                    }))?));
                }
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
                    let live_descendants = tree
                        .members
                        .keys()
                        .filter(|identity| !client_roots.contains(&identity.pid))
                        .copied()
                        .collect::<BTreeSet<_>>();
                    let retirement =
                        counters.retire_clients(sampler.as_mut(), &clients, &live_descendants);
                    progress.report.lifecycle_notes.push(format!(
                        "storage-client-retirement {}", serde_json::to_string(&json!({
                            "repetition":repetition,"turn":turn,"live_descendants":live_descendants,"error":retirement.as_ref().err().map(ToString::to_string),"read_identities":counters.reads.snapshot().identities.into_iter().filter(|r| clients.contains(&r.identity)).collect::<Vec<_>>(),"identities":counters.receipts(counter_source()).into_iter().filter(|r| clients.iter().any(|id| id.pid == r.pid && id.start_time == r.start_time)).collect::<Vec<_>>()
                        }))?
                    ));
                    // S1 is permanently incomplete; independent allocation/task
                    // evidence can continue, with the failure already preserved.
                    if let Err(error) = retirement {
                        counter_error.get_or_insert_with(|| error.to_string());
                    }
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
        if counters.unavailable.is_some() || counters.collection_error.is_some() {
            // Preserve null physical measurements; allocation collection continues.
        } else if let Some(delta) = before_disk
            .cumulative_write_bytes
            .zip(current.cumulative_write_bytes)
            .and_then(|(b, a)| a.checked_sub(b))
        {
            writes.push(delta);
        } else {
            counter_error.get_or_insert_with(|| "incomplete live/retired physical counters".into());
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
    progress.auxiliary_trials.push(auxiliaries::evaluate(
        repetition,
        n,
        &family_snapshots,
        config,
    )?);
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
        && counters.collection_error.is_none()
        && writes.len() == n as usize
        && counters.writes.snapshot().counter_complete
        && counters.reads.snapshot().counter_complete;
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
    if !b.inventory.capture_retries.is_empty() {
        report.lifecycle_notes.push(format!(
            "storage transient re-inventory boundary={boundary} paths={:?}",
            b.inventory.capture_retries
        ));
    }
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
        physical_write_bytes: (counters.unavailable.is_none()
            && counters.collection_error.is_none())
        .then_some(writes.cumulative_write_bytes)
        .flatten(),
        physical_read_bytes: (counters.unavailable.is_none()
            && counters.collection_error.is_none())
        .then_some(reads.cumulative_write_bytes)
        .flatten(),
        counter_source: if counters.unavailable.is_some() {
            "unavailable"
        } else {
            counter_source()
        }
        .into(),
        counter_complete: counters.unavailable.is_none()
            && counters.collection_error.is_none()
            && writes.counter_complete
            && reads.counter_complete,
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
    let refs = vec![
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
    ];
    for id in [ROWS[0], ROWS[2]] {
        if let Some(trials) = details
            .get_mut(id)
            .and_then(|d| d.get_mut("trials"))
            .and_then(Value::as_array_mut)
        {
            for trial in trials {
                trial["evidence_refs"] = serde_json::to_value(&refs)?;
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
        receipt["canonical_sha256"] = json!(record.map(|r| &r.canonical_hash));
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

/// Retain the last settled checkpoints when a repetition cannot complete. No
/// aggregate or counter total is inferred from incomplete turn coverage.
fn preserve_partial_trial(progress: &mut Progress, config: &StorageConfig) {
    let Some((repetition, _, _, _)) = &progress.active_engine else {
        return;
    };
    let repetition = *repetition;
    if !progress
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
                    && checkpoints(
                        progress
                            .report
                            .storage_summary
                            .as_ref()
                            .map_or(100, |x| x.turn_budget),
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
            outcome: TestOutcome::Error("deadline".into()),
            measurement_complete: false,
            reason: Some("deadline".into()),
            summary: AuxiliarySummary::default(),
            diagnostics: auxiliaries::checkpoint_diagnostics(&snapshots, config),
            evidence_refs: Vec::new(),
        });
    }
    if !progress
        .retention_trials
        .iter()
        .any(|t| t.repetition == repetition)
    {
        let blobs: Vec<BodyBlob> =
            std::fs::read_to_string(progress.output.join("storage-request-bodies.jsonl"))
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|r| r["repetition"].as_u64() == Some(repetition as u64))
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
                .collect();
        let evidence_refs = blobs
            .iter()
            .map(|b| EvidenceRef {
                file: b.path.clone(),
                sha256: b.raw_sha256.clone(),
                first_record: None,
                last_record: None,
            })
            .collect();
        progress.retention_trials.push(Trial {
            repetition,
            outcome: TestOutcome::Error("deadline".into()),
            measurement_complete: false,
            reason: Some("deadline".into()),
            summary: RetentionSummary::default(),
            diagnostics: RetentionDiagnostics {
                coverage: Coverage::CaptureError,
                body_blobs: blobs,
                baseline_exclusions: Vec::new(),
                match_refs: Vec::new(),
            },
            evidence_refs,
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
}

fn record_collection_failure(progress: &mut Progress, config: &StorageConfig, error: &AhrbError) {
    let reason = format!("task-incomplete: {error}");
    for i in [0, 2] {
        progress.report.results[i] = row(i, TestOutcome::Error(reason.clone()), config);
        progress
            .details
            .insert(ROWS[i].into(), pending_details(i, &reason));
    }
    progress.report.lifecycle_notes.push(reason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn transient_then_fatal_capture_reaches_error_report_without_aggregates() {
        let config = StorageConfig::default();
        let mut progress = Progress {
            output: PathBuf::new(),
            active_engine: None,
            report: Report {
                results: (0..10)
                    .map(|i| row(i, TestOutcome::Error("pending".into()), &config))
                    .collect(),
                storage_summary: Some(StorageSummary::default()),
                ..Default::default()
            },
            write_trials: Vec::new(),
            curve_trials: Vec::new(),
            details: BTreeMap::new(),
            finalized: BTreeSet::new(),
            auxiliary_trials: Vec::new(),
            retention_trials: Vec::new(),
        };
        let error = accounting::transient_then_fatal_fixture(2).await;
        record_collection_failure(&mut progress, &config, &error);
        let note = &progress.report.lifecycle_notes[0];
        eprintln!("{note}");
        for path in ["cache/pack-1", "cache/pack-2", "state/link"] {
            assert!(note.contains(path), "{note}");
        }
        for i in [0, 2] {
            assert!(
                matches!(&progress.report.results[i].outcome, TestOutcome::Error(reason) if reason == note)
            );
            assert!(!progress.report.results[i].metadata.measurement_complete);
            assert_eq!(progress.details[ROWS[i]]["reason"], *note);
            assert_eq!(progress.details[ROWS[i]]["trials"], json!([]));
        }
        let summary = progress.report.storage_summary.unwrap();
        assert!(summary.write_bytes_per_turn_p50.is_none());
        assert!(summary.disk_class.is_none());
        assert!(summary.footprint_curve.is_empty());
        assert!(summary.growth_class.is_none());
        assert!(progress.report.storage_samples.is_empty());
        assert!(progress.report.storage_files.is_empty());
    }

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
                &BTreeSet::new(),
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

#[cfg(test)]
mod retirement_error_tests {
    use super::*;
    use crate::process::ProcIdentity;

    #[derive(Default)]
    struct FinalSampler {
        calls: Vec<(ProcIdentity, &'static str)>,
    }
    impl Sampler for FinalSampler {
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
        fn disk_counter_for_identity(&mut self, id: ProcIdentity) -> Result<Option<u64>> {
            self.calls.push((id, "write"));
            Ok(Some(100))
        }
        fn disk_read_counter_for_identity(&mut self, id: ProcIdentity) -> Result<Option<u64>> {
            self.calls.push((id, "read"));
            Ok((id.pid != 1).then_some(100))
        }
    }

    #[test]
    fn lost_read_receipt_masks_both_directions_and_keeps_later_retirements() -> Result<()> {
        let id = |pid| ProcIdentity {
            pid,
            start_time: u64::from(pid),
        };
        let mut counters = Counters::default();
        let observation = ProcessDiskObservation {
            expected_identities: BTreeSet::from([id(1), id(2)]),
            write_bytes_by_identity: BTreeMap::from([(id(1), 10), (id(2), 20)]),
            ..Default::default()
        };
        counters.writes.observe(&observation)?;
        counters.reads.observe(&observation)?;
        let mut sampler = FinalSampler::default();
        counters
            .retire_clients(
                &mut sampler,
                &observation.expected_identities,
                &BTreeSet::new(),
            )
            .expect_err("missing read receipt");
        assert_eq!(
            sampler.calls,
            vec![
                (id(1), "write"),
                (id(1), "read"),
                (id(2), "write"),
                (id(2), "read")
            ]
        );
        assert!(counters.writes.snapshot().counter_complete);
        assert!(!counters.reads.snapshot().counter_complete);
        let first_error = counters.collection_error.clone().expect("sticky failure");
        assert!(
            first_error.contains("read:")
                && first_error.contains("missing client terminal-before-reap receipt"),
            "{first_error}"
        );
        let later = ProcessDiskObservation {
            expected_identities: BTreeSet::from([id(3)]),
            write_bytes_by_identity: BTreeMap::from([(id(3), 30)]),
            ..Default::default()
        };
        counters.writes.observe(&later)?;
        counters.reads.observe(&later)?;
        counters.retire_clients(&mut sampler, &later.expected_identities, &BTreeSet::new())?;
        assert_eq!(
            counters.collection_error.as_deref(),
            Some(first_error.as_str())
        );
        let mut report = Report::default();
        record_boundary(
            &mut report,
            &StorageConfig::default(),
            1,
            2,
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
        assert_eq!(sample.counter_source, counter_source());
        Ok(())
    }

    #[test]
    fn live_descendant_is_not_retired_and_can_be_sampled_on_the_next_turn() -> Result<()> {
        let root = ProcIdentity {
            pid: 2,
            start_time: 2,
        };
        let child = ProcIdentity {
            pid: 3,
            start_time: 3,
        };
        let mut counters = Counters::default();
        let mut observation = ProcessDiskObservation {
            expected_identities: BTreeSet::from([root, child]),
            write_bytes_by_identity: BTreeMap::from([(root, 10), (child, 20)]),
            ..Default::default()
        };
        counters.writes.observe(&observation)?;
        counters.reads.observe(&observation)?;
        let mut sampler = FinalSampler::default();
        counters
            .retire_clients(
                &mut sampler,
                &observation.expected_identities,
                &BTreeSet::from([child]),
            )
            .expect_err("live child lacks its final receipt");
        assert_eq!(sampler.calls, [(root, "write"), (root, "read")]);
        let error = counters.collection_error.clone().expect("sticky error");
        assert!(error.contains("live owned descendant"));
        observation.expected_identities = BTreeSet::from([child]);
        observation.write_bytes_by_identity = BTreeMap::from([(child, 30)]);
        for tracker in [&mut counters.writes, &mut counters.reads] {
            let snapshot = tracker.observe(&observation)?;
            assert_eq!(
                snapshot
                    .identities
                    .iter()
                    .find(|r| r.identity == child)
                    .unwrap()
                    .status,
                DiskIdentityStatus::Live
            );
            assert_eq!(
                snapshot
                    .identities
                    .iter()
                    .find(|r| r.identity == root)
                    .unwrap()
                    .status,
                DiskIdentityStatus::RetiredAfterFinalSample
            );
        }
        assert_eq!(counters.collection_error.as_deref(), Some(error.as_str()));
        let mut report = Report::default();
        record_boundary(
            &mut report,
            &StorageConfig::default(),
            1,
            2,
            &accounting::SettledInventory {
                inventory: accounting::Inventory::default(),
                settle_ms: 2100.0,
                sync_start_ns: 1,
                sync_end_ns: 2,
            },
            &counters,
        );
        assert!(!report.storage_samples[0].counter_complete);
        assert!(report.storage_samples[0].physical_write_bytes.is_none());
        assert!(report.storage_samples[0].physical_read_bytes.is_none());
        Ok(())
    }

    #[test]
    fn missing_physical_evidence_never_hides_a_complete_curve_or_yields_a_disk_class() -> Result<()>
    {
        let config = StorageConfig::default();
        for class in [GrowthClass::Bounded, GrowthClass::Superlinear] {
            let mut progress = Progress {
                output: PathBuf::new(),
                active_engine: None,
                report: Report {
                    results: (0..10)
                        .map(|i| row(i, TestOutcome::Error("pending".into()), &config))
                        .collect(),
                    storage_summary: Some(StorageSummary {
                        turn_budget: 100,
                        repetitions: 3,
                        counter_source: counter_source().into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                write_trials: Vec::new(),
                curve_trials: Vec::new(),
                details: BTreeMap::new(),
                finalized: BTreeSet::new(),
                auxiliary_trials: Vec::new(),
                retention_trials: Vec::new(),
            };
            for repetition in 1..=3 {
                progress.write_trials.push(Trial {
                    repetition,
                    outcome: TestOutcome::Error("missing receipt".into()),
                    measurement_complete: false,
                    reason: Some("missing receipt".into()),
                    summary: WriteSummary::default(),
                    diagnostics: WriteDiagnostics::default(),
                    evidence_refs: Vec::new(),
                });
                let points = checkpoints(100)
                    .into_iter()
                    .map(|turn| Checkpoint {
                        turn,
                        allocated_bytes: 4096
                            + if class == GrowthClass::Superlinear {
                                4096 * u64::from(turn).pow(2)
                            } else {
                                0
                            },
                    })
                    .collect::<Vec<_>>();
                let (evaluated, slope, diagnostics) = evaluate_curve(&points, 100)?;
                assert_eq!(evaluated, class);
                progress.curve_trials.push(Trial {
                    repetition,
                    outcome: if class == GrowthClass::Bounded {
                        TestOutcome::Pass
                    } else {
                        TestOutcome::Fail("shape".into())
                    },
                    measurement_complete: true,
                    reason: None,
                    summary: FootprintSummary {
                        growth_class: Some(class),
                        footprint_slope_bytes_per_turn: Some(slope),
                        first_turn_allocated_bytes: Some(points[1].allocated_bytes as f64),
                        ..Default::default()
                    },
                    diagnostics,
                    evidence_refs: Vec::new(),
                });
            }
            finalize(&mut progress, &config)?;
            assert!(matches!(
                progress.report.results[0].outcome,
                TestOutcome::Error(_)
            ));
            assert_eq!(
                matches!(progress.report.results[2].outcome, TestOutcome::Fail(_)),
                class == GrowthClass::Superlinear
            );
            let summary = progress.report.storage_summary.as_ref().unwrap();
            assert_eq!(summary.footprint_curve.len(), checkpoints(100).len());
            assert!(
                summary.write_bytes_per_turn_p95.is_none()
                    && summary.disk_class.is_none()
                    && summary.write_amplification_ratio.is_none()
            );
            assert!(storage_badge(summary, &progress.report.results, &config).is_none());
        }
        Ok(())
    }
}
