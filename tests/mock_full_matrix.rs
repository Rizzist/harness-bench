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

fn run_full_matrix(output: &Path) -> (std::process::ExitStatus, Report) {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(repository.join("adapters/mock/manifest.toml"))
        .arg("--output")
        .arg(output)
        .arg("--profile")
        .arg("quick")
        .arg("--junit")
        .output()
        .expect("execute AHRB against its built-in mock harness");
    let report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read full report"),
    )
    .expect("parse full report");
    (result.status, report)
}

fn json_text(values: &[Value]) -> String {
    serde_json::to_string(values).expect("serialize evidence for inspection")
}

#[test]
fn full_matrix_certifies_the_reference_mock_with_complete_artifacts() {
    let output = run_directory();
    let (status, report) = run_full_matrix(&output);

    assert_eq!(status.code(), Some(0));

    assert_eq!(report.results.len(), 41);
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| result.row)
            .collect::<Vec<_>>(),
        (1_u8..=41).collect::<Vec<_>>()
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
    for key in ["crash_recovery_ms", "journal_recovered_events"] {
        assert!(report.metrics.contains_key(key), "missing metric {key}");
    }
    assert!(report.metrics.contains_key("parallel_beta_mib_per_agent"));
    assert!(report.metrics["crash_recovery_ms"] < 10_000.0);
    assert!(report.metrics["journal_recovered_events"] >= 1.0);
    let badge = report.badge.as_ref().expect("full matrix badge");
    assert_eq!(badge.os, std::env::consts::OS);
    assert_eq!(badge.topology, "shared-daemon-sessions");
    assert_eq!(badge.parallel_width, 4);

    for artifact in [
        "report.md",
        "report.json",
        "samples.jsonl",
        "processes.jsonl",
        "membership.jsonl",
        "events.jsonl",
        "model-requests.jsonl",
        "junit.xml",
    ] {
        assert!(
            output.join(artifact).is_file(),
            "missing artifact {artifact}"
        );
    }

    std::fs::remove_dir_all(&output).expect("remove isolated full-matrix report");
}
