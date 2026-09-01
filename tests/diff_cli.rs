use ahrb::evaluate::{Pillar, TestOutcome, TestResult, TestResultMetadata};
use ahrb::report::{Fingerprint, Report, ResourceSummary};
use ahrb::results::{INDEX_SCHEMA, IndexEntry};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;

fn result(row: u8, id: &str, outcome: TestOutcome) -> TestResult {
    TestResult {
        row,
        id: id.to_owned(),
        pillar: if matches!(row, 42 | 43) {
            Pillar::Resource
        } else {
            Pillar::Functionality
        },
        metadata: TestResultMetadata::for_row(row, &outcome),
        outcome,
        evidence: Vec::new(),
    }
}

fn report(info_outcome: TestOutcome, wall_p95: f64) -> Report {
    Report {
        schema: 3,
        spec_version: 2,
        run_id: "deterministic-shared-id".to_owned(),
        fingerprint: Fingerprint {
            harness: "ahrb-mock".to_owned(),
            harness_version: "mock-v".to_owned(),
            manifest: "manifest".to_owned(),
            workflows: "workflows".to_owned(),
            fake_model: "fake".to_owned(),
            normalizer: "normalizer".to_owned(),
            ahrb_revision: "revision".to_owned(),
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            host_memory_bytes: 1,
            profile: "quick".to_owned(),
        },
        results: vec![
            result(1, "startup", TestOutcome::Pass),
            result(42, "model-request-efficiency", info_outcome),
            result(43, "turn-latency-distribution", TestOutcome::Pass),
        ],
        resource_summary: ResourceSummary {
            topology: "persistent-daemon".to_owned(),
            profile: "quick".to_owned(),
            comparison_scope: "within-topology-only".to_owned(),
            wall_per_turn_p95_ms: Some(wall_p95),
            ..ResourceSummary::default()
        },
        ..Report::default()
    }
}

fn write_report(root: &Path, relative: &str, report: &Report) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().expect("report parent"))
        .expect("create report directory");
    std::fs::write(
        path,
        serde_json::to_vec_pretty(report).expect("serialize report"),
    )
    .expect("write report");
}

#[test]
fn diff_latest_reads_mixed_legacy_and_v2_index_lines() {
    let root = std::env::temp_dir().join(format!("ahrb-diff-mixed-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale diff fixture");
    }
    std::fs::create_dir_all(root.join("results")).expect("create diff fixture");
    let legacy_report_path = "results/ahrb-mock/legacy/report.json";
    let current_report_path = "results/ahrb-mock/current/report.json";
    write_report(&root, legacy_report_path, &report(TestOutcome::Pass, 100.0));
    write_report(
        &root,
        current_report_path,
        &report(TestOutcome::Fail("reference envelope".to_owned()), 150.0),
    );
    let legacy_line = json!({
        "harness_id": "stale-legacy-harness",
        "harness_version": "stale-legacy-version",
        "manifest_hash": "stale-legacy-manifest",
        "ahrb_revision": "stale-legacy-revision",
        "platform": "stale-os-stale-arch",
        "profile": "cert",
        "rows_run": [1, 42],
        "timestamp": "2026-01-01T00:00:00Z",
        "counts": {"PASS": 2, "FAIL": 0, "UNSUPPORTED": 0, "ERROR": 0},
        "badge": null,
        "resource_summary": {
            "peak_rss_mib": 0.0,
            "cpu_per_turn_ms": 0.0,
            "wall_per_turn_ms": 0.0,
            "sampler_overhead_pct": 0.0
        },
        "results_dir": "results/ahrb-mock/legacy",
        "load_avg_1m": null
    });
    let current_line = IndexEntry {
        schema: INDEX_SCHEMA,
        run_key: "run-current".to_owned(),
        completed_at: "2026-01-02T00:00:00Z".to_owned(),
        harness: "ahrb-mock".to_owned(),
        harness_version: "mock-v".to_owned(),
        report_path: current_report_path.to_owned(),
        report_schema: 3,
        spec_version: 2,
        profile: "quick".to_owned(),
        os: std::env::consts::OS.to_owned(),
        topology: "persistent-daemon".to_owned(),
        manifest_sha256: "manifest".to_owned(),
        workflow_sha256: "workflows".to_owned(),
        ahrb_revision: "revision".to_owned(),
    };
    let index = format!(
        "{}\n{}\n",
        serde_json::to_string(&legacy_line).expect("serialize legacy index"),
        serde_json::to_string(&current_line).expect("serialize current index")
    );
    std::fs::write(root.join("results/index.jsonl"), index).expect("write mixed index");

    let run_diff = || {
        Command::new(env!("CARGO_BIN_EXE_hbench"))
            .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
            .env("AHRB_RESULTS_ROOT", &root)
            .args(["diff", "--latest", "ahrb-mock"])
            .output()
            .expect("run hbench diff")
    };
    let output = run_diff();
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let diff: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse diff output");
    assert!(
        diff["left"]["run_key"]
            .as_str()
            .is_some_and(|value| value.starts_with("legacy-"))
    );
    assert_eq!(diff["left"]["harness"], "ahrb-mock");
    assert_eq!(diff["left"]["harness_version"], "mock-v");
    assert_eq!(diff["left"]["ahrb_revision"], "revision");
    assert_eq!(diff["left"]["os"], std::env::consts::OS);
    assert_eq!(diff["left"]["profile"], "quick");
    assert_eq!(diff["right"]["run_key"], "run-current");
    assert_eq!(
        diff["rows"][1]["change"],
        serde_json::Value::String("informational-regression".to_owned())
    );
    assert_eq!(
        diff["resource_summary_deltas"]["wall_per_turn_p95_ms"]["delta"],
        50.0
    );
    let repeated = run_diff();
    assert!(repeated.status.success());
    assert_eq!(output.stdout, repeated.stdout);
    std::fs::remove_dir_all(root).expect("remove diff fixture");
}
