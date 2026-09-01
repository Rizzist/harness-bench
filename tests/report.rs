use ahrb::evaluate::{
    Assertion, Badge, Pillar, TestOutcome, badge_label, certify, classify, suite_exit_code,
};
use ahrb::process::{ProcIdentity, ProcOwnership, Sample};
use ahrb::report::{
    MembershipSample, MemoryTimeIntegralEvidence, MemoryTimeIntegralSample, ProcessHygieneAudit,
    ProcessHygieneCadenceSample, ProcessHygieneCheckpoint, ProcessHygieneEvidence,
    ProcessHygieneProcess, Report, ResourceSummary, TurnObservation, evaluate_memory_time_integral,
    evaluate_process_hygiene, evaluate_time_to_first_model_request, evaluate_turn_latency,
    evaluate_turn_latency_repetitions, record_capability_declarations, render_markdown,
    render_resource_summary, summarize_resources,
};
use std::path::Path;
use std::time::SystemTime;

#[test]
fn report_records_optional_capability_declaration_from_manifest() {
    let mut manifest =
        ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("load mock manifest");
    let mut results = vec![classify(
        4,
        "parallel-tools",
        Pillar::Functionality,
        Some(false),
        &[],
        None,
    )];
    record_capability_declarations(&mut results, &manifest);
    assert_eq!(results[0].metadata.capability_declared, Some(true));

    manifest
        .capabilities
        .optional
        .remove("parallel_tool_execution");
    record_capability_declarations(&mut results, &manifest);
    assert_eq!(results[0].metadata.capability_declared, Some(false));
}

#[test]
fn markdown_sorts_rows_and_names_outcomes() {
    let mut report = Report {
        schema: 1,
        run_id: "deterministic".to_owned(),
        ..Report::default()
    };
    report.results.push(classify(
        2,
        "single-tool-call",
        Pillar::ToolCallCorrectness,
        Some(true),
        &[Assertion {
            name: "effect".to_owned(),
            passed: true,
            detail: "once".to_owned(),
        }],
        None,
    ));
    let markdown = render_markdown(&report);
    assert!(markdown.contains("| 2 | ToolCallCorrectness | `single-tool-call` | PASS |"));
}

#[test]
fn report_details_validate_known_blocks_and_retain_future_blocks() {
    let mut value = serde_json::to_value(Report::default()).expect("serialize default report");
    value["details"] = serde_json::json!({
        "automation-score": {
            "profile": "quick",
            "topology": "client-process-fanout",
            "comparison_scope": "within-topology-only",
            "score": null
        },
        "future-row": {"opaque": [1, 2, 3]}
    });
    let report: Report = serde_json::from_value(value.clone()).expect("typed details parse");
    assert_eq!(report.details["future-row"]["opaque"][2], 3);

    value["details"]["automation-score"]["score"] = serde_json::json!("not-a-number");
    let error = serde_json::from_value::<Report>(value)
        .expect_err("known detail block must reject the wrong field type");
    assert!(error.to_string().contains("details.automation-score"));
}

#[test]
fn incomplete_resource_summary_omits_non_nullable_wave_one_derivatives() {
    let value =
        serde_json::to_value(ResourceSummary::default()).expect("serialize empty resource summary");
    for field in [
        "wall_per_turn_p50_ms",
        "wall_per_turn_p95_ms",
        "wall_per_turn_max_ms",
        "wall_per_turn_mad_ms",
        "wall_per_turn_jitter_ratio",
        "latency_class",
        "time_to_first_model_request_p50_ms",
        "time_to_first_model_request_p95_ms",
        "time_to_first_model_request_max_ms",
        "memory_time_integral_mib_s_per_turn",
        "memory_time_integral_coverage_ratio",
        "memory_time_integral_max_sample_gap_ms",
        "cpu_per_turn_p50_ms",
        "cpu_per_turn_p95_ms",
        "cpu_class",
    ] {
        assert!(value.get(field).is_none(), "{field} must be omitted");
    }
}

fn latency_turn(index: u32, start_ns: u64, wall_ns: u64) -> TurnObservation {
    TurnObservation {
        repetition: 1,
        turn_index: index,
        actor: "latency".to_owned(),
        session_id_hash: "digest".to_owned(),
        phase: "turn-latency".to_owned(),
        launch_ns: None,
        submit_ns: Some(start_ns),
        first_model_request_ns: None,
        terminal_ns: Some(start_ns + wall_ns),
        exit_ns: None,
        turn_wall_ns: Some(wall_ns),
    }
}

fn first_request_turn(repetition: u32, launch_ns: u64, latency_ns: u64) -> TurnObservation {
    TurnObservation {
        repetition,
        turn_index: 1,
        actor: format!("cold-{repetition}"),
        session_id_hash: "digest".to_owned(),
        phase: "time-to-first-model-request".to_owned(),
        launch_ns: Some(launch_ns),
        submit_ns: Some(launch_ns.saturating_add(1)),
        first_model_request_ns: Some(launch_ns.saturating_add(latency_ns)),
        terminal_ns: Some(launch_ns.saturating_add(latency_ns).saturating_add(1)),
        exit_ns: None,
        turn_wall_ns: None,
    }
}

fn memory_time_sample(
    repetition: u32,
    monotonic_ns: u64,
    memory_mib: u64,
    cpu_ns: u64,
) -> MemoryTimeIntegralSample {
    MemoryTimeIntegralSample {
        repetition,
        monotonic_ns,
        effective_memory_bytes: memory_mib * 1_048_576,
        cpu_ns,
        owned_processes: 1,
        collection_cpu_ns: 1_000,
        collection_wall_ns: 2_000,
        cpu_accounting_warnings: Vec::new(),
    }
}

fn complete_memory_time_evidence(final_cpu_ns: u64) -> MemoryTimeIntegralEvidence {
    MemoryTimeIntegralEvidence {
        samples: vec![
            memory_time_sample(1, 1_000_000_000, 0, 0),
            memory_time_sample(1, 2_000_000_000, 100, final_cpu_ns / 2),
            memory_time_sample(1, 3_000_000_000, 0, final_cpu_ns),
        ],
        turns: vec![TurnObservation {
            repetition: 1,
            turn_index: 1,
            actor: "integral".to_owned(),
            session_id_hash: "digest".to_owned(),
            phase: "memory-time-integral".to_owned(),
            launch_ns: Some(1_000_000_000),
            submit_ns: Some(1_000_000_000),
            first_model_request_ns: None,
            terminal_ns: Some(3_000_000_000),
            exit_ns: Some(3_000_000_000),
            turn_wall_ns: Some(2_000_000_000),
        }],
        warm_idle_baseline_bytes: std::collections::BTreeMap::from([(1, 0)]),
        sampler_cadence_ns: 1_000_000_000,
        sampler_collection_cpu_ns: 3_000,
        sampler_observation_wall_ns: 2_000_000_000,
    }
}

fn hygiene_process(pid: u32, threads: u64, fds: u64) -> ProcessHygieneProcess {
    ProcessHygieneProcess {
        identity: ProcIdentity {
            pid,
            start_time: u64::from(pid) * 10,
        },
        command: format!("fixture-{pid}"),
        ownership: ProcOwnership::DeclaredRoot,
        thread_count: Some(threads),
        open_fds: Some(fds),
    }
}

fn hygiene_checkpoint(
    repetition: u32,
    turn_index: u32,
    processes: Vec<ProcessHygieneProcess>,
) -> ProcessHygieneCheckpoint {
    let sample = |elapsed_ns| ProcessHygieneCadenceSample {
        elapsed_ns,
        collection_cpu_ns: 100,
        collection_wall_ns: 1_000,
        processes: processes.clone(),
    };
    let cadence_samples = vec![sample(1), sample(1_000_000)];
    ProcessHygieneCheckpoint {
        repetition,
        turn_index,
        processes,
        cadence_samples,
        sampled_wall_ns: 1_000_000,
        required_cadence_ns: 10_000_000,
    }
}

fn finish_hygiene_accounting(mut evidence: ProcessHygieneEvidence) -> ProcessHygieneEvidence {
    evidence.active_sampler_collection_cpu_ns = evidence
        .checkpoints
        .iter()
        .flat_map(|checkpoint| &checkpoint.cadence_samples)
        .fold(0_u64, |total, sample| {
            total.saturating_add(sample.collection_cpu_ns)
        });
    evidence.sampled_turn_wall_ns = evidence
        .checkpoints
        .iter()
        .fold(0_u64, |total, checkpoint| {
            total.saturating_add(checkpoint.sampled_wall_ns)
        });
    evidence
}

fn clean_per_invocation_hygiene(turns: u32) -> ProcessHygieneEvidence {
    finish_hygiene_accounting(ProcessHygieneEvidence {
        checkpoints: (1..=turns)
            .map(|turn| hygiene_checkpoint(1, turn, vec![hygiene_process(1_000 + turn, 1, 3)]))
            .collect(),
        per_turn_audits: (1..=turns)
            .map(|turn| ProcessHygieneAudit {
                repetition: 1,
                turn_index: Some(turn),
                waited_ms: 2_000,
                processes: Vec::new(),
            })
            .collect(),
        growth_checkpoints: (0..=turns)
            .map(|turn| ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: turn,
                processes: Vec::new(),
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            })
            .collect(),
        ..ProcessHygieneEvidence::default()
    })
}

fn daemon_hygiene(
    post_close: Vec<ProcessHygieneProcess>,
    shutdown: Vec<ProcessHygieneProcess>,
) -> ProcessHygieneEvidence {
    let baseline = hygiene_process(100, 10, 20);
    finish_hygiene_accounting(ProcessHygieneEvidence {
        checkpoints: vec![hygiene_checkpoint(1, 1, vec![baseline.clone()])],
        warm_baselines: vec![ProcessHygieneCheckpoint {
            repetition: 1,
            turn_index: 0,
            processes: vec![baseline],
            cadence_samples: Vec::new(),
            sampled_wall_ns: 0,
            required_cadence_ns: 0,
        }],
        growth_checkpoints: vec![
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 0,
                processes: vec![hygiene_process(100, 10, 20)],
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
            ProcessHygieneCheckpoint {
                repetition: 1,
                turn_index: 1,
                processes: vec![hygiene_process(100, 10, 20)],
                cadence_samples: Vec::new(),
                sampled_wall_ns: 0,
                required_cadence_ns: 0,
            },
        ],
        post_close_audits: vec![ProcessHygieneAudit {
            repetition: 1,
            turn_index: None,
            waited_ms: 2_000,
            processes: post_close,
        }],
        shutdown_audits: vec![ProcessHygieneAudit {
            repetition: 1,
            turn_index: None,
            waited_ms: 2_000,
            processes: shutdown,
        }],
        ..ProcessHygieneEvidence::default()
    })
}

#[test]
fn process_hygiene_emits_exact_metrics_from_observed_churn() {
    let evaluation = evaluate_process_hygiene(&clean_per_invocation_hygiene(3), 1, 3, true);
    assert!(evaluation.measurement_complete);
    assert!(evaluation.passed);
    assert_eq!(
        evaluation
            .metrics
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec![
            "process_hygiene.observed_fds_opened_per_turn_max",
            "process_hygiene.observed_fds_opened_per_turn_p50",
            "process_hygiene.observed_processes_spawned_per_turn_max",
            "process_hygiene.observed_processes_spawned_per_turn_p50",
            "process_hygiene.observed_threads_created_per_turn_max",
            "process_hygiene.observed_threads_created_per_turn_p50",
            "process_hygiene.peak_fds",
            "process_hygiene.peak_live_processes",
            "process_hygiene.peak_threads",
            "process_hygiene.residue_fds_delta",
            "process_hygiene.residue_processes",
            "process_hygiene.residue_threads_delta",
            "process_hygiene.unique_process_identities",
        ]
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_processes_spawned_per_turn_p50"],
        1.0
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_threads_created_per_turn_max"],
        1.0
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_fds_opened_per_turn_p50"],
        3.0
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.unique_process_identities"],
        3.0
    );
}

#[test]
fn process_hygiene_intentional_residual_child_fails_with_sorted_identity_details() {
    let mut evidence = clean_per_invocation_hygiene(2);
    evidence.per_turn_audits[0].processes =
        vec![hygiene_process(900, 2, 5), hygiene_process(800, 1, 4)];
    let evaluation = evaluate_process_hygiene(&evidence, 1, 2, true);
    assert!(evaluation.measurement_complete);
    assert!(!evaluation.passed);
    assert_eq!(evaluation.metrics["process_hygiene.residue_processes"], 2.0);
    assert_eq!(
        evaluation.details["residue_identities"][0]["pid"],
        serde_json::json!(800)
    );
    assert_eq!(
        evaluation.details["residue_identities"][1]["pid"],
        serde_json::json!(900)
    );
    assert_eq!(
        evaluation.details["residue_identities"][0]["ownership"],
        serde_json::json!("declared-root")
    );
}

#[test]
fn process_hygiene_daemon_tolerances_are_inclusive_and_residue_is_strict() {
    let exact = evaluate_process_hygiene(
        &daemon_hygiene(vec![hygiene_process(100, 12, 24)], Vec::new()),
        1,
        1,
        false,
    );
    assert!(exact.passed);
    assert_eq!(exact.metrics["process_hygiene.residue_threads_delta"], 2.0);
    assert_eq!(exact.metrics["process_hygiene.residue_fds_delta"], 4.0);

    for post_close in [
        vec![hygiene_process(100, 13, 24)],
        vec![hygiene_process(100, 12, 25)],
        vec![hygiene_process(100, 10, 20), hygiene_process(101, 1, 1)],
    ] {
        let over = evaluate_process_hygiene(&daemon_hygiene(post_close, Vec::new()), 1, 1, false);
        assert!(!over.passed);
    }
    let shutdown_residue = evaluate_process_hygiene(
        &daemon_hygiene(
            vec![hygiene_process(100, 10, 20)],
            vec![hygiene_process(100, 1, 1)],
        ),
        1,
        1,
        false,
    );
    assert!(!shutdown_residue.passed);
}

#[test]
fn process_hygiene_adjacent_increase_threshold_is_exact() {
    let with_counts = |counts: &[u32]| {
        let mut evidence = clean_per_invocation_hygiene(counts.len() as u32);
        for (checkpoint, count) in evidence.growth_checkpoints.iter_mut().skip(1).zip(counts) {
            let processes = (0..*count)
                .map(|offset| hygiene_process(2_000 + checkpoint.turn_index * 10 + offset, 1, 1))
                .collect::<Vec<_>>();
            checkpoint.processes.clone_from(&processes);
        }
        evidence
    };
    let first = evaluate_process_hygiene(&with_counts(&[1, 1, 1, 1]), 1, 4, true);
    let second = evaluate_process_hygiene(&with_counts(&[1, 2, 1, 2]), 1, 4, true);
    assert!(first.passed && second.passed, "growth is informational");
    assert_eq!(
        first.details["monotonic_growth_ok"],
        serde_json::json!(true)
    );
    assert_eq!(
        second.details["monotonic_growth_ok"],
        serde_json::json!(false)
    );
}

#[test]
fn process_hygiene_requires_every_ordered_two_second_audit() {
    let mut short = clean_per_invocation_hygiene(2);
    short.per_turn_audits[1].waited_ms = 1_999;
    assert!(!evaluate_process_hygiene(&short, 1, 2, true).measurement_complete);

    let mut misordered = clean_per_invocation_hygiene(2);
    misordered.per_turn_audits[1].turn_index = Some(1);
    assert!(!evaluate_process_hygiene(&misordered, 1, 2, true).measurement_complete);
}

#[test]
fn process_hygiene_cadence_captures_identity_threads_and_fds_after_initial_sample() {
    let mut evidence = clean_per_invocation_hygiene(1);
    evidence.checkpoints[0].cadence_samples[0].processes.clear();
    evidence.checkpoints[0].cadence_samples[1].processes = vec![hygiene_process(7_001, 3, 9)];
    let evaluation = evaluate_process_hygiene(&evidence, 1, 1, true);
    assert!(evaluation.measurement_complete);
    assert!(evaluation.passed);
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_processes_spawned_per_turn_max"],
        1.0
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_threads_created_per_turn_max"],
        3.0
    );
    assert_eq!(
        evaluation.metrics["process_hygiene.observed_fds_opened_per_turn_max"],
        9.0
    );
}

#[test]
fn process_hygiene_rejects_untrustworthy_cadence_and_missing_counters() {
    let mut gap = clean_per_invocation_hygiene(1);
    gap.checkpoints[0].cadence_samples[1].elapsed_ns = 25_000_000;
    gap.checkpoints[0].sampled_wall_ns = 25_000_000;
    gap.sampled_turn_wall_ns = 25_000_000;
    let gap_evaluation = evaluate_process_hygiene(&gap, 1, 1, true);
    assert!(!gap_evaluation.measurement_complete);
    assert!(
        gap_evaluation
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("untrustworthy cadence"))
    );

    let mut missing = clean_per_invocation_hygiene(1);
    missing.checkpoints[0].cadence_samples[1].processes[0].thread_count = None;
    assert!(!evaluate_process_hygiene(&missing, 1, 1, true).measurement_complete);
}

#[test]
fn process_hygiene_sampler_overhead_boundary_is_inclusive() {
    let with_cpu = |per_sample_cpu_ns| {
        let mut evidence = clean_per_invocation_hygiene(1);
        for sample in &mut evidence.checkpoints[0].cadence_samples {
            sample.collection_cpu_ns = per_sample_cpu_ns;
        }
        evidence.active_sampler_collection_cpu_ns = per_sample_cpu_ns * 2;
        evidence
    };
    let exact = evaluate_process_hygiene(&with_cpu(50_000), 1, 1, true);
    assert!(exact.measurement_complete);
    assert_eq!(exact.sampler_overhead_pct, 10.0);
    let over = evaluate_process_hygiene(&with_cpu(50_001), 1, 1, true);
    assert!(!over.measurement_complete);
    assert!(
        over.measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("sampler overload"))
    );
}

#[test]
fn turn_latency_uses_nearest_rank_mad_and_exact_classes() {
    let turns = [50_u64, 50, 100, 150, 150]
        .into_iter()
        .enumerate()
        .map(|(index, milliseconds)| {
            latency_turn(index as u32 + 1, 1_000, milliseconds * 1_000_000)
        })
        .collect::<Vec<_>>();
    let evaluation = evaluate_turn_latency(&turns, 5, false, 1_000);
    assert!(evaluation.measurement_complete);
    assert_eq!(evaluation.wall_per_turn_p50_ms, 100.0);
    assert_eq!(evaluation.wall_per_turn_p95_ms, 150.0);
    assert_eq!(evaluation.wall_per_turn_max_ms, 150.0);
    assert_eq!(evaluation.wall_per_turn_mad_ms, 50.0);
    assert_eq!(evaluation.wall_per_turn_jitter_ratio, 0.5);
    assert_eq!(evaluation.latency_class, "L250");
    assert!(!evaluation.reference_envelope_pass);
}

#[test]
fn turn_latency_classes_include_every_exact_boundary() {
    for (wall_ns, expected_class) in [
        (100_000_000_u64, "L100"),
        (250_000_000, "L250"),
        (500_000_000, "L500"),
        (1_000_000_000, "L1000"),
        (1_000_000_001, "L1000+"),
    ] {
        let evaluation = evaluate_turn_latency(&[latency_turn(1, 1_000, wall_ns)], 1, false, 2_000);
        assert!(evaluation.measurement_complete);
        assert_eq!(evaluation.latency_class, expected_class);
    }
}

#[test]
fn turn_latency_reference_envelope_includes_exact_edges_only() {
    let evaluate = |walls: &[u64]| {
        let turns = walls
            .iter()
            .enumerate()
            .map(|(index, wall_ns)| latency_turn(index as u32 + 1, 1_000, *wall_ns))
            .collect::<Vec<_>>();
        evaluate_turn_latency(&turns, walls.len() as u32, false, 2_000)
    };

    let exact = evaluate(&[
        600_000_000,
        600_000_000,
        800_000_000,
        1_000_000_000,
        1_000_000_000,
    ]);
    assert_eq!(exact.wall_per_turn_p95_ms, 1_000.0);
    assert_eq!(exact.wall_per_turn_jitter_ratio, 0.25);
    assert!(exact.reference_envelope_pass);

    let p95_over = evaluate(&[
        600_000_000,
        600_000_000,
        800_000_000,
        1_000_000_000,
        1_000_000_001,
    ]);
    assert!(p95_over.wall_per_turn_p95_ms > 1_000.0);
    assert_eq!(p95_over.wall_per_turn_jitter_ratio, 0.25);
    assert!(!p95_over.reference_envelope_pass);

    let jitter_over = evaluate(&[
        524_999_999,
        524_999_999,
        700_000_000,
        875_000_001,
        875_000_001,
    ]);
    assert!(jitter_over.wall_per_turn_p95_ms < 1_000.0);
    assert!(jitter_over.wall_per_turn_jitter_ratio > 0.25);
    assert!(!jitter_over.reference_envelope_pass);
}

#[test]
fn turn_latency_aggregates_repetitions_without_hiding_a_failed_repetition() {
    let mut turns = Vec::new();
    for (repetition, milliseconds) in [(1, 100_u64), (2, 300), (3, 500)] {
        for index in 1..=5 {
            let mut turn = latency_turn(index, 1_000, milliseconds * 1_000_000);
            turn.repetition = repetition;
            turns.push(turn);
        }
    }
    let aggregate = evaluate_turn_latency_repetitions(&turns, 3, 5, false, 2_000);
    assert!(aggregate.measurement_complete);
    assert_eq!(aggregate.wall_per_turn_p50_ms, 300.0);
    assert_eq!(aggregate.wall_per_turn_p95_ms, 300.0);
    assert_eq!(aggregate.wall_per_turn_max_ms, 500.0);
    assert_eq!(aggregate.latency_class, "L500");
    assert!(aggregate.reference_envelope_pass);

    turns[9] = {
        let mut turn = latency_turn(5, 1_000, 2_000_000_000);
        turn.repetition = 2;
        turn
    };
    let failed = evaluate_turn_latency_repetitions(&turns, 3, 5, false, 2_000);
    assert!(failed.measurement_complete);
    assert_eq!(failed.wall_per_turn_p95_ms, 500.0);
    assert_eq!(failed.wall_per_turn_max_ms, 2_000.0);
    assert!(!failed.reference_envelope_pass);
}

#[test]
fn per_invocation_turn_latency_requires_launch_and_exit_boundaries() {
    let complete = TurnObservation {
        repetition: 1,
        turn_index: 1,
        actor: "latency".to_owned(),
        session_id_hash: "digest".to_owned(),
        phase: "turn-latency".to_owned(),
        launch_ns: Some(1_000),
        submit_ns: None,
        first_model_request_ns: None,
        terminal_ns: None,
        exit_ns: Some(10_001_000),
        turn_wall_ns: Some(10_000_000),
    };
    assert!(
        evaluate_turn_latency(std::slice::from_ref(&complete), 1, true, 1_000).measurement_complete
    );

    let mut missing_launch = complete.clone();
    missing_launch.launch_ns = None;
    assert!(!evaluate_turn_latency(&[missing_launch], 1, true, 1_000).measurement_complete);

    let mut missing_exit = complete;
    missing_exit.exit_ns = None;
    assert!(!evaluate_turn_latency(&[missing_exit], 1, true, 1_000).measurement_complete);
}

#[test]
fn turn_latency_missing_or_zero_boundaries_error_but_measured_timeout_fails_envelope() {
    let mut missing = latency_turn(1, 1_000, 10_000_000);
    missing.terminal_ns = None;
    assert!(!evaluate_turn_latency(&[missing], 1, false, 1_000).measurement_complete);

    let zero = latency_turn(1, 1_000, 0);
    let zero_evaluation = evaluate_turn_latency(&[zero], 1, false, 1_000);
    assert!(!zero_evaluation.measurement_complete);
    assert!(
        zero_evaluation
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("invalid external wall interval"))
    );

    let timeout = latency_turn(1, 1_000, 1_000_000_000);
    let timeout_evaluation = evaluate_turn_latency(&[timeout], 1, false, 1_000);
    assert!(timeout_evaluation.measurement_complete);
    assert!(!timeout_evaluation.reference_envelope_pass);
    assert!(timeout_evaluation.measurement_error.is_none());
}

#[test]
fn time_to_first_model_request_uses_nearest_rank_and_exact_envelope_edges() {
    let roles = vec!["primary".to_owned(); 3];
    let exact = evaluate_time_to_first_model_request(
        &[
            first_request_turn(1, 1_000, 1_000_000_000),
            first_request_turn(2, 2_000, 2_000_000_000),
            first_request_turn(3, 3_000, 1_500_000_000),
        ],
        &roles,
        3,
        2_001,
    );
    assert!(exact.measurement_complete);
    assert!(exact.reference_envelope_pass);
    assert_eq!(exact.p50_ms, 1_500.0);
    assert_eq!(exact.p95_ms, 2_000.0);
    assert_eq!(exact.max_ms, 2_000.0);
    assert_eq!(exact.details["first_request_role"], "primary");

    let p95_over = evaluate_time_to_first_model_request(
        &[first_request_turn(1, 1_000, 2_000_000_001)],
        &["title".to_owned()],
        1,
        10_001,
    );
    assert!(p95_over.measurement_complete);
    assert!(!p95_over.reference_envelope_pass);
}

#[test]
fn time_to_first_model_request_requires_every_ordered_pair_and_strict_timeout() {
    let complete = first_request_turn(1, 1_000, 2_000_000_000);
    let timeout_edge = evaluate_time_to_first_model_request(
        std::slice::from_ref(&complete),
        &["primary".to_owned()],
        1,
        2_000,
    );
    assert!(timeout_edge.measurement_complete);
    assert!(!timeout_edge.reference_envelope_pass);

    let mut missing = complete.clone();
    missing.first_model_request_ns = None;
    assert!(
        !evaluate_time_to_first_model_request(&[missing], &["primary".to_owned()], 1, 10_000,)
            .measurement_complete
    );

    let reversed = TurnObservation {
        first_model_request_ns: Some(9_999),
        ..first_request_turn(1, 10_000, 1)
    };
    assert!(
        !evaluate_time_to_first_model_request(&[reversed], &["primary".to_owned()], 1, 10_000,)
            .measurement_complete
    );
    assert!(
        !evaluate_time_to_first_model_request(&[complete], &[], 1, 10_000,).measurement_complete
    );
}

#[test]
fn memory_time_integral_uses_trapezoids_and_exact_cpu_classes() {
    let evaluation =
        evaluate_memory_time_integral(&complete_memory_time_evidence(20_000_000), 1, 1, true);
    assert!(evaluation.measurement_complete);
    assert!(evaluation.reference_envelope_pass);
    assert_eq!(evaluation.memory_time_integral_mib_s_per_turn, 100.0);
    assert_eq!(evaluation.memory_time_integral_coverage_ratio, 1.0);
    assert_eq!(evaluation.memory_time_integral_max_sample_gap_ms, 1_000.0);
    assert_eq!(evaluation.cpu_per_turn_p50_ms, 20.0);
    assert_eq!(evaluation.cpu_per_turn_p95_ms, 20.0);
    assert_eq!(evaluation.cpu_class, "C50");
    assert_eq!(evaluation.details["integration"], "trapezoidal");

    for (cpu_ns, class) in [
        (10_000_000, "C10"),
        (50_000_000, "C50"),
        (250_000_000, "C250"),
        (250_000_001, "C250+"),
    ] {
        let evaluation =
            evaluate_memory_time_integral(&complete_memory_time_evidence(cpu_ns), 1, 1, true);
        assert!(evaluation.measurement_complete);
        assert_eq!(evaluation.cpu_class, class);
        assert_eq!(evaluation.reference_envelope_pass, cpu_ns <= 250_000_000);
    }
}

#[test]
fn memory_time_integral_bad_gap_bracketing_and_sampler_overhead_are_errors() {
    let mut gap = complete_memory_time_evidence(20_000_000);
    gap.sampler_cadence_ns = 499_999_999;
    let gap = evaluate_memory_time_integral(&gap, 1, 1, true);
    assert!(!gap.measurement_complete);
    assert!(
        gap.measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("maximum sample gap"))
    );

    let mut unbracketed = complete_memory_time_evidence(20_000_000);
    unbracketed.turns[0].launch_ns = Some(500_000_000);
    let unbracketed = evaluate_memory_time_integral(&unbracketed, 1, 1, true);
    assert!(!unbracketed.measurement_complete);
    assert!(
        unbracketed
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("not bracketed before start"))
    );

    let mut overloaded = complete_memory_time_evidence(20_000_000);
    overloaded.samples[0].collection_cpu_ns = 200_000_001;
    overloaded.samples[1].collection_cpu_ns = 0;
    overloaded.samples[2].collection_cpu_ns = 0;
    overloaded.sampler_collection_cpu_ns = 200_000_001;
    let overloaded = evaluate_memory_time_integral(&overloaded, 1, 1, true);
    assert!(!overloaded.measurement_complete);
    assert!(
        overloaded
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("sampler overload"))
    );

    let mut wall_overrun = complete_memory_time_evidence(20_000_000);
    wall_overrun.samples[1].collection_wall_ns = wall_overrun.sampler_cadence_ns.saturating_add(1);
    let wall_overrun = evaluate_memory_time_integral(&wall_overrun, 1, 1, true);
    assert!(!wall_overrun.measurement_complete);
    assert!(
        wall_overrun
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("1 cadence overruns"))
    );

    let mut repeated_jitter = complete_memory_time_evidence(20_000_000);
    repeated_jitter.sampler_cadence_ns = 600_000_000;
    let repeated_jitter = evaluate_memory_time_integral(&repeated_jitter, 1, 1, true);
    assert!(!repeated_jitter.measurement_complete);
    assert!(
        repeated_jitter
            .measurement_error
            .as_deref()
            .is_some_and(|error| error.contains("2 cadence gaps"))
    );
}

#[test]
fn informational_fail_does_not_change_suite_exit_but_error_does() {
    let manifest = ahrb::manifest::load(std::path::Path::new("adapters/mock/manifest.toml"))
        .expect("load mock manifest");
    let informational_fail = classify(
        43,
        "turn-latency-distribution",
        Pillar::Resource,
        Some(true),
        &[Assertion {
            name: "envelope".to_owned(),
            passed: false,
            detail: "outside reference envelope".to_owned(),
        }],
        None,
    );
    assert_eq!(suite_exit_code(&[informational_fail], None, &manifest), 0);
    let informational_error = classify(
        43,
        "turn-latency-distribution",
        Pillar::Resource,
        Some(true),
        &[],
        Some("missing boundary".to_owned()),
    );
    assert_eq!(suite_exit_code(&[informational_error], None, &manifest), 1);
}

#[test]
fn badge_v2_adds_profile_and_latency_without_an_empty_facet_segment() {
    let mut badge = Badge {
        spec_version: 2,
        os: "macos".to_owned(),
        topology: "client-process-fanout".to_owned(),
        profile: "quick".to_owned(),
        parallel_width: 8,
        resource_class: "R96".to_owned(),
        latency_class: "L500".to_owned(),
        cpu_class: "C50".to_owned(),
        automation_score: 82,
        facets: Vec::new(),
        comparison_scope: "within-topology-only".to_owned(),
    };
    assert_eq!(
        badge_label(&badge),
        "Automation Ready v2 · macos · client-process-fanout · quick · N8 · R96 · L500 · C50 · A82"
    );
    let quick = badge_label(&badge);
    badge.profile = "cert".to_owned();
    let cert = badge_label(&badge);
    assert_ne!(quick, cert);
    assert!(cert.contains(" · cert · "));
}

#[test]
fn deserialized_v1_badge_keeps_the_exact_legacy_label() {
    let badge: Badge = serde_json::from_value(serde_json::json!({
        "os": "macos",
        "topology": "client-process-fanout",
        "parallel_width": 4,
        "resource_class": "R32",
        "facets": ["replay", "crash", "resume"]
    }))
    .expect("deserialize v1 badge without additive v2 fields");
    assert_eq!(badge.spec_version, 1);
    assert_eq!(badge.latency_class, "");
    assert_eq!(
        badge_label(&badge),
        "Automation Ready v1 · macos · client-process-fanout · N4 · R32 · replay+crash+resume"
    );
}

#[test]
fn unsupported_is_never_silently_passed() {
    let result = classify(
        18,
        "native-delegation",
        Pillar::Functionality,
        Some(false),
        &[],
        None,
    );
    assert!(matches!(result.outcome, TestOutcome::Unsupported(_)));
}

#[test]
fn missing_g2_components_prevent_resource_only_badge() {
    let manifest = ahrb::manifest::load(std::path::Path::new("adapters/mock/manifest.toml"))
        .expect("load mock manifest");
    let results: Vec<_> = ahrb::scenarios::all()
        .iter()
        .map(|definition| {
            classify(
                definition.row,
                definition.id,
                definition.pillar,
                Some(true),
                &[Assertion {
                    name: "criterion".to_owned(),
                    passed: true,
                    detail: "met".to_owned(),
                }],
                None,
            )
        })
        .collect();
    let badge = certify(
        &results,
        &manifest,
        "macos",
        "quick",
        8,
        64.0 * 1024.0 * 1024.0,
        "L500",
        "C50",
    );
    assert!(badge.is_none());
}

fn sample(elapsed_ns: u64, memory_mib: u64, cpu_ns: u64) -> Sample {
    let bytes = memory_mib * 1024 * 1024;
    Sample {
        elapsed_ns,
        wall_time: SystemTime::UNIX_EPOCH,
        phase: "external-sample".to_owned(),
        rss_bytes: bytes + 1024,
        pss_bytes: Some(bytes),
        private_bytes: None,
        footprint_bytes: Some(bytes),
        rss_crosscheck_bytes: None,
        cgroup_memory_bytes: None,
        cgroup_peak_bytes: None,
        cpu_ns,
        open_fds: None,
        thread_count: None,
        collection_ns: 1,
        collection_wall_ns: 1,
        processes: Vec::new(),
        process_samples: Vec::new(),
        cpu_accounting_warnings: Vec::new(),
    }
}

#[test]
fn resource_summary_is_pure_post_processing_of_existing_external_evidence() {
    // The function accepts immutable samples, membership records, and already
    // captured wall clocks. It has no driver/harness handle, so surfacing the
    // summary cannot insert synchronous work into a harness turn.
    let samples = [
        sample(1_000_000_000, 10, 1_000_000_000),
        sample(3_000_000_000, 30, 5_000_000_000),
    ];
    let membership = [
        MembershipSample {
            elapsed_ns: 1_000_000_000,
            phase: "external-sample".to_owned(),
            discovery_wall_ns: 1,
            discovery_cpu_ns: 50_000_000,
            lane: 0,
        },
        MembershipSample {
            elapsed_ns: 3_000_000_000,
            phase: "external-sample".to_owned(),
            discovery_wall_ns: 1,
            discovery_cpu_ns: 50_000_000,
            lane: 0,
        },
    ];
    let summary = summarize_resources(
        &samples,
        &membership,
        4,
        &[10_000_000, 30_000_000],
        Some(8.0),
        Some(12.0),
        Some(0.75),
    );

    assert_eq!(summary.peak_rss_mib, 30.0);
    assert_eq!(summary.mean_rss_mib, 20.0);
    assert_eq!(summary.median_rss_mib, 20.0);
    assert_eq!(summary.cpu_total_s, 4.0);
    assert_eq!(summary.cpu_per_turn_ms, 1_000.0);
    assert_eq!(summary.wall_per_turn_ms, 20.0);
    assert_eq!(summary.sampler_overhead_pct, 5.0);
    let line = render_resource_summary(&summary);
    assert!(line.starts_with("resource_summary peak_rss_mib=30.000"));
    assert!(line.contains("idle_rss_mib=8.000"));
    assert!(line.contains("parallel_beta_mib_per_agent=12.000"));
    assert!(line.ends_with("sampler_overhead_pct=5.000"));
}
