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
        pillar: "matrix".into(),
        storage_summary: None,
        badge_label: None,
        outcome_counts: Default::default(),
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
        resource_summary: ahrb::results::IndexedResourceSummary::default(),
        metrics: std::collections::BTreeMap::new(),
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

#[test]
fn storage_paths_enforce_scope_and_only_behavioral_failures_gate() {
    let root = std::env::temp_dir().join(format!("ahrb-storage-diff-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut left = report(TestOutcome::Pass, 100.0);
    left.schema = 4;
    left.spec_version = 4;
    left.pillar = Some("storage".into());
    left.results = vec![
        result(3, "footprint-curve", TestOutcome::Pass),
        result(1, "write-volume", TestOutcome::Pass),
    ];
    for r in &mut left.results {
        r.pillar = Pillar::Storage;
    }
    left.storage_summary = Some(ahrb::storage::evidence::StorageSummary {
        schema: 1,
        task: ahrb::storage::TASK.into(),
        profile: "quick".into(),
        os: "macos".into(),
        topology: "per-invocation".into(),
        comparison_scope: "within-topology-only".into(),
        allocation_source: "stat-st_blocks-512".into(),
        counter_source: "macos-ri_diskio_byteswritten".into(),
        declarations_sha256: "a".repeat(64),
        write_bytes_per_turn_p50: Some(4096.0),
        ..Default::default()
    });
    let mut right = left.clone();
    right
        .storage_summary
        .as_mut()
        .unwrap()
        .write_bytes_per_turn_p50 = Some(8192.0);
    right.results[1].outcome = TestOutcome::Error("missing receipt".into());
    write_report(&root, "left/report.json", &left);
    let run = |r: &Report| {
        write_report(&root, "right/report.json", r);
        Command::new(env!("CARGO_BIN_EXE_hbench"))
            .arg("diff")
            .arg(root.join("left"))
            .arg(root.join("right/report.json"))
            .env("AHRB_RESULTS_ROOT", &root)
            .output()
            .unwrap()
    };
    let output = run(&right);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        d["storage_summary_deltas"]["values"]["write_bytes_per_turn_p50"]["delta"],
        4096.0
    );
    assert_eq!(d["left"]["outcome_counts"]["PASS"], 2);
    assert_eq!(d["right"]["outcome_counts"]["ERROR"], 1);
    assert_eq!(d["left"]["os"], "macos");
    assert_eq!(d["left"]["completed_at"], "unavailable");
    right.results[0].outcome = TestOutcome::Fail("superlinear".into());
    assert_eq!(run(&right).status.code(), Some(1));
    right.storage_summary.as_mut().unwrap().profile = "cert".into();
    let d: serde_json::Value = serde_json::from_slice(&run(&right).stdout).unwrap();
    assert_eq!(
        d["storage_summary_deltas"]["comparison_scope"],
        "not-comparable-storage-scope"
    );
    assert_eq!(d["comparison_scope"], "not-comparable-storage-scope");
    assert!(
        d["storage_summary_deltas"]["values"]["write_bytes_per_turn_p50"]
            .get("delta")
            .is_none()
    );
    right.storage_summary = None;
    right.schema = 3;
    right.spec_version = 2;
    right.pillar = None;
    let d: serde_json::Value = serde_json::from_slice(&run(&right).stdout).unwrap();
    assert_eq!(
        d["storage_summary_deltas"]["comparison_scope"],
        "unavailable"
    );
    left.storage_summary = None;
    write_report(&root, "left/report.json", &left);
    let d: serde_json::Value = serde_json::from_slice(&run(&right).stdout).unwrap();
    assert_eq!(
        d["storage_summary_deltas"]["comparison_scope"],
        "unavailable"
    );
    assert_eq!(d["storage_summary_deltas"]["values"], json!({}));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cross_pillar_document_refuses_numeric_comparison() {
    fn assert_no_deltas(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    if matches!(key.as_str(), "delta" | "delta_pct") {
                        assert!(value.is_null(), "unexpected {key}: {value}");
                    }
                    assert_no_deltas(value);
                }
            }
            serde_json::Value::Array(values) => values.iter().for_each(assert_no_deltas),
            _ => {}
        }
    }
    let root = std::env::temp_dir().join(format!("ahrb-diff-cross-{}", std::process::id()));
    let mut storage = report(TestOutcome::Pass, 100.0);
    storage.schema = 4;
    storage.spec_version = 4;
    storage.pillar = Some("storage".into());
    storage.results.clear();
    storage.storage_summary = Some(ahrb::storage::evidence::StorageSummary {
        schema: 1,
        task: ahrb::storage::TASK.into(),
        profile: "quick".into(),
        os: std::env::consts::OS.into(),
        topology: "persistent-daemon".into(),
        comparison_scope: "within-topology-only".into(),
        allocation_source: "stat-st_blocks-512".into(),
        counter_source: "test-counter".into(),
        declarations_sha256: "a".repeat(64),
        write_bytes_per_turn_p50: Some(4096.0),
        ..Default::default()
    });
    let run = |left: &Report, right: &Report| {
        write_report(&root, "left/report.json", left);
        write_report(&root, "right/report.json", right);
        Command::new(env!("CARGO_BIN_EXE_hbench"))
            .arg("diff")
            .arg(root.join("left"))
            .arg(root.join("right"))
            .env("AHRB_RESULTS_ROOT", &root)
            .output()
            .unwrap()
    };
    for legacy in [false, true] {
        storage.pillar = (!legacy).then(|| "storage".into());
        let matrix = report(TestOutcome::Pass, 150.0);
        for (left, right) in [(&storage, &matrix), (&matrix, &storage)] {
            let output = run(left, right);
            assert!(
                !output.status.success(),
                "cross-pillar diff unexpectedly succeeded: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            assert!(String::from_utf8_lossy(&output.stderr).contains("different pillars"));
        }
    }
    let same =
        serde_json::from_slice::<serde_json::Value>(&run(&storage, &storage).stdout).unwrap();
    assert_eq!(same["comparison_scope"], "within-topology-only");
    assert_eq!(
        same["storage_summary_deltas"]["values"]["write_bytes_per_turn_p50"]["delta"],
        0.0
    );
    assert_no_deltas(&same["resource_summary_deltas"]);
    let mut missing = storage.clone();
    missing.pillar = Some("storage".into());
    missing.storage_summary = None;
    let unavailable =
        serde_json::from_slice::<serde_json::Value>(&run(&missing, &missing).stdout).unwrap();
    assert_eq!(unavailable["comparison_scope"], "unavailable");
    assert_no_deltas(&unavailable);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn schema_two_pillar_is_recovered_before_latest_selection() {
    let root = std::env::temp_dir().join(format!("ahrb-legacy-pillar-{}", std::process::id()));
    let mut report = report(TestOutcome::Pass, 100.0);
    report.pillar = Some("economy".into());
    write_report(&root, "results/a/report.json", &report);
    write_report(&root, "results/b/report.json", &report);
    let a = json!({"schema":2,"run_key":"a","completed_at":"2026-01-01T00:00:00Z","harness":"ahrb-mock","harness_version":"v","report_path":"results/a/report.json","report_schema":3,"spec_version":2,"profile":"quick","os":"macos","topology":"persistent-daemon","manifest_sha256":"a","workflow_sha256":"b","ahrb_revision":"c"});
    let mut b = a.clone();
    b["schema"] = json!(3);
    b["pillar"] = json!("economy");
    b["run_key"] = json!("b");
    b["completed_at"] = json!("2026-01-02T00:00:00Z");
    b["report_path"] = json!("results/b/report.json");
    std::fs::write(root.join("results/index.jsonl"), format!("{a}\n{b}\n")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .args(["diff", "--latest", "ahrb-mock"])
        .env("AHRB_RESULTS_ROOT", &root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(d["left"]["pillar"], "economy");
    assert_eq!(d["right"]["pillar"], "economy");
    std::fs::remove_dir_all(root).unwrap();
}
