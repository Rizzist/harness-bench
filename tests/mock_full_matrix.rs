mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn run_directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-full-matrix-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn run_matrix(output: &Path, tests: Option<&str>) -> (std::process::ExitStatus, Report) {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut command = Command::new(env!("CARGO_BIN_EXE_ahrb"));
    command
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(repository.join("adapters/mock/manifest.toml"))
        .arg("--output")
        .arg(output)
        .arg("--profile")
        .arg("quick")
        .arg("--junit");
    if let Some(tests) = tests {
        command.arg("--tests").arg(tests);
    }
    let result = command
        .output()
        .expect("execute AHRB against its built-in mock harness");
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &result,
        &report_path,
        "AHRB full-matrix subprocess did not produce a report",
    );
    let report = serde_json::from_slice(&report_bytes).expect("parse full report");
    (result.status, report)
}

fn run_full_matrix(output: &Path) -> (std::process::ExitStatus, Report) {
    run_matrix(output, None)
}

fn json_text(values: &[Value]) -> String {
    serde_json::to_string(values).expect("serialize evidence for inspection")
}

fn derived_row43_journal(report: &Report) -> String {
    let profiles = [PathBuf::from(&report.profile_path)];
    assert_eq!(profiles.len(), 1, "daemon run uses one canonical profile");
    assert!(profiles[0].is_dir(), "canonical daemon profile exists");
    let sessions = profiles[0].join("derived-row43/state/sessions");
    std::fs::read_dir(sessions)
        .expect("read derived row-43 sessions")
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("journal.jsonl")).ok())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn full_matrix_certifies_the_reference_mock_with_complete_artifacts() {
    let output = run_directory();
    let (status, report) = run_full_matrix(&output);

    assert_eq!(status.code(), Some(0));

    assert_eq!(report.results.len(), 48);
    let mut expected_rows = (1_u8..=46).collect::<Vec<_>>();
    expected_rows.extend([63, 64]);
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| result.row)
            .collect::<Vec<_>>(),
        expected_rows
    );
    for result in &report.results {
        assert!(matches!(result.outcome, TestOutcome::Pass));
        assert!(
            !result.evidence.is_empty(),
            "passing row {} has no evaluated evidence",
            result.row
        );
        assert!(
            result
                .evidence
                .iter()
                .all(|item| !item.contains("runner-wired")
                    && !item.contains("not produced evidence")),
            "row {} retained placeholder evidence",
            result.row
        );
    }

    // Raw events must remain observations, not circular restatements of the already
    // classified row outcome.
    assert!(report.events.iter().all(|event| event.get("row").is_none()));

    let events = json_text(&report.events);
    let requests = json_text(&report.model_requests);
    for token in [
        "turn-accepted",
        "tool-call",
        "tool-result",
        "terminal-success",
        "terminal-failure",
        "input-accepted",
    ] {
        assert!(
            events.contains(token),
            "normalized evidence omitted {token}"
        );
    }
    for token in ["ahrb-fake-v1", "/v1/chat/completions", "credential"] {
        assert!(requests.contains(token), "model evidence omitted {token}");
    }
    assert!(!requests.contains("matrix-secret"));

    // The central resource methodology must be represented by measurements, rather
    // than by prose-only row outcomes.
    assert!(
        !report.samples.is_empty(),
        "resource rows emitted no samples"
    );
    assert!(
        report
            .samples
            .iter()
            .any(|sample| sample.phase.contains("idle"))
    );
    assert!(
        report
            .samples
            .iter()
            .any(|sample| sample.phase.contains("n4") || sample.phase.contains("barrier"))
    );
    for key in [
        "crash_recovery_ms",
        "crash_recovery_tree_cleared",
        "crash_recovery_valid",
        "journal_recovered_events",
        "journal_recovery_valid",
        "journal_torn_tail_injected",
    ] {
        assert!(report.metrics.contains_key(key), "missing metric {key}");
    }
    assert!(
        report
            .resource_metrics
            .contains_key("parallel_beta_mib_per_agent")
    );
    assert!(report.resource_summary.peak_rss_mib > 0.0);
    assert!(report.resource_summary.mean_rss_mib > 0.0);
    assert!(report.resource_summary.wall_per_turn_ms > 0.0);
    assert!(report.resource_summary.idle_rss_mib.is_some());
    assert!(
        report
            .resource_summary
            .parallel_beta_mib_per_agent
            .is_some()
    );
    assert!(report.resource_summary.scaling_alpha.is_some());
    assert!(report.resource_summary.sampler_overhead_pct >= 0.0);
    assert_eq!(report.turns.len(), 163);
    for (phase, expected) in [
        ("turn-latency", 100),
        ("time-to-first-model-request", 3),
        ("memory-time-integral", 60),
    ] {
        assert_eq!(
            report
                .turns
                .iter()
                .filter(|turn| turn.phase == phase)
                .count(),
            expected,
            "unexpected turn count for {phase}"
        );
    }
    assert!(report.turns.iter().all(|turn| {
        turn.submit_ns
            .zip(turn.terminal_ns)
            .zip(turn.turn_wall_ns)
            .is_some_and(|((submit, terminal), wall)| terminal.checked_sub(submit) == Some(wall))
    }));
    assert!(report.turns.iter().all(|turn| {
        if turn.phase == "time-to-first-model-request" {
            turn.launch_ns.is_some() && turn.first_model_request_ns.is_some()
        } else {
            turn.launch_ns.is_none() && turn.exit_ns.is_none()
        }
    }));
    assert_eq!(report.resource_summary.topology, "shared-daemon-sessions");
    assert_eq!(
        report.resource_summary.comparison_scope,
        "within-topology-only"
    );
    assert!(report.resource_summary.wall_per_turn_p95_ms <= 1_000.0);
    assert!(report.resource_summary.wall_per_turn_jitter_ratio <= 0.25);
    let row43_events = report
        .events
        .iter()
        .filter(|event| event.get("actor").and_then(Value::as_str) == Some("ahrb-row43:row43"))
        .collect::<Vec<_>>();
    assert_eq!(
        row43_events
            .iter()
            .filter(|event| {
                event.get("event").and_then(Value::as_str) == Some("terminal-success")
            })
            .count(),
        100
    );
    assert!(row43_events.iter().all(|event| {
        event.pointer("/payload/key").and_then(Value::as_str) != Some("row-43-warmup")
    }));
    let row43_primary_requests = report
        .model_requests
        .iter()
        .filter(|request| {
            request.pointer("/request/scenario").and_then(Value::as_str) == Some("ahrb-row43")
                && request.get("accepted").and_then(Value::as_bool) == Some(true)
                && request.get("role").and_then(Value::as_str) == Some("primary")
        })
        .collect::<Vec<_>>();
    assert_eq!(row43_primary_requests.len(), 100);
    assert!(row43_primary_requests.iter().all(|request| {
        request
            .pointer("/request/checkpoint")
            .and_then(Value::as_str)
            != Some("warmup")
    }));
    let row43_journal = derived_row43_journal(&report);
    assert_eq!(
        row43_journal.matches("\"key\":\"row-43-warmup\"").count(),
        1
    );
    let sampled_peak = report
        .samples
        .iter()
        .map(|sample| {
            #[cfg(target_os = "macos")]
            {
                sample.footprint_bytes.unwrap_or(sample.rss_bytes)
            }
            #[cfg(target_os = "linux")]
            {
                sample.pss_bytes.unwrap_or(sample.rss_bytes)
            }
        })
        .max()
        .unwrap_or(0) as f64
        / (1024.0 * 1024.0);
    assert!((report.resource_summary.peak_rss_mib - sampled_peak).abs() < f64::EPSILON);
    let sampled_cpu_s = report
        .samples
        .first()
        .zip(report.samples.last())
        .map_or(0, |(first, last)| last.cpu_ns.saturating_sub(first.cpu_ns))
        as f64
        / 1_000_000_000.0;
    assert!((report.resource_summary.cpu_total_s - sampled_cpu_s).abs() < f64::EPSILON);
    assert!(report.resource_metrics.values().all(|metric| {
        metric.topology == "shared-daemon-sessions"
            && metric.comparison_scope == "within-topology-only"
    }));
    assert!(report.metrics["crash_recovery_ms"] < 10_000.0);
    assert_eq!(report.metrics["crash_recovery_valid"], 1.0);
    assert!(report.metrics["journal_recovered_events"] >= 1.0);
    assert_eq!(report.metrics["journal_torn_tail_injected"], 1.0);
    let badge = report.badge.as_ref().expect("full matrix badge");
    assert_eq!(badge.os, std::env::consts::OS);
    assert_eq!(badge.topology, "shared-daemon-sessions");
    assert_eq!(badge.parallel_width, 4);
    assert_eq!(badge.spec_version, 2);
    assert!(matches!(
        badge.latency_class.as_str(),
        "L100" | "L250" | "L500" | "L1000"
    ));
    assert_eq!(
        badge.facets,
        vec![
            "replay",
            "crash",
            "steer",
            "queue",
            "native-delegation",
            "subturn",
            "hooks"
        ]
    );

    for artifact in [
        "report.md",
        "report.json",
        "samples.jsonl",
        "processes.jsonl",
        "membership.jsonl",
        "events.jsonl",
        "model-requests.jsonl",
        "turns.jsonl",
        "junit.xml",
    ] {
        assert!(
            output.join(artifact).is_file(),
            "missing artifact {artifact}"
        );
    }

    std::fs::remove_dir_all(&report.profile_path).expect("remove full-matrix profile");
    std::fs::remove_dir_all(&output).expect("remove isolated full-matrix report");
}

#[test]
fn derived_latency_trials_do_not_delay_cancel_cleanup() {
    let output = run_directory();
    let (status, report) = run_matrix(&output, Some("36,43"));

    assert_eq!(status.code(), Some(0));
    assert_eq!(report.results.len(), 2);
    let cancellation = report
        .results
        .iter()
        .find(|result| result.row == 36)
        .expect("row 36 result");
    assert!(matches!(cancellation.outcome, TestOutcome::Pass));
    assert!(
        cancellation
            .evidence
            .iter()
            .any(|item| item.contains("observed 1 cancellation terminal"))
    );
    let latency = report
        .results
        .iter()
        .find(|result| result.row == 43)
        .expect("row 43 result");
    assert!(latency.metadata.measurement_complete);
    assert!(!matches!(
        latency.outcome,
        TestOutcome::Error(_) | TestOutcome::Unsupported(_)
    ));
    assert_eq!(report.turns.len(), 100);

    std::fs::remove_dir_all(&output).expect("remove isolated cancellation-latency report");
}
