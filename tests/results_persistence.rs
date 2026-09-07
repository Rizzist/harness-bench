mod common;

use ahrb::results::IndexEntry;
use ahrb::{evaluate::TestOutcome, report::Report};
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

#[test]
fn hbench_auto_saves_bundle_indexes_it_and_lists_history() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!("ahrb-results-test-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale results fixture");
    }
    std::fs::create_dir(&root).expect("create results fixture");
    let primary = root.join("primary");
    let run = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .arg("mock")
        .arg("--tests")
        .arg("1,2,3")
        .arg("--output")
        .arg(&primary)
        .arg("--junit")
        .arg("--deadline")
        .arg("120")
        .output()
        .expect("run auto-saved hbench subset");
    assert!(
        run.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    let index_text =
        std::fs::read_to_string(root.join("results/index.jsonl")).expect("read results index");
    let lines = index_text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let entry: IndexEntry = serde_json::from_str(lines[0]).expect("parse results index entry");
    let indexed_value: serde_json::Value =
        serde_json::from_str(lines[0]).expect("parse index object");
    let exact_keys = [
        "schema",
        "run_key",
        "completed_at",
        "harness",
        "harness_version",
        "report_path",
        "report_schema",
        "spec_version",
        "profile",
        "os",
        "topology",
        "resource_summary",
        "metrics",
        "manifest_sha256",
        "workflow_sha256",
        "ahrb_revision",
        "pillar",
        "storage_summary",
        "badge_label",
        "outcome_counts",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        indexed_value
            .as_object()
            .expect("index line is object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        exact_keys
    );
    assert_eq!(entry.schema, ahrb::results::INDEX_SCHEMA);
    assert_eq!(entry.schema, 3);
    assert_eq!(entry.pillar, "matrix");
    assert!(entry.storage_summary.is_none());
    assert!(entry.badge_label.is_none());
    assert_eq!(
        indexed_value["outcome_counts"],
        serde_json::json!({"PASS":3,"FAIL":0,"UNSUPPORTED":0,"ERROR":0})
    );
    let mut legacy = indexed_value.clone();
    let object = legacy.as_object_mut().expect("legacy object");
    object.insert("schema".into(), 2.into());
    for field in ["pillar", "storage_summary", "badge_label", "outcome_counts"] {
        object.remove(field);
    }
    let legacy: IndexEntry =
        serde_json::from_value(legacy).expect("schema-2 additive compatibility");
    assert_eq!(legacy.pillar, "matrix");
    assert!(legacy.storage_summary.is_none());

    assert_eq!(entry.harness, "ahrb-mock");
    assert!(entry.run_key.starts_with("run-"));
    assert_ne!(entry.ahrb_revision, "unknown");
    assert!(!entry.harness_version.contains(['\n', '\r']));
    assert!(entry.harness_version.chars().count() <= 80);
    let saved_report = root.join(&entry.report_path);
    let saved = saved_report.parent().expect("saved report parent");
    let report: Report =
        serde_json::from_slice(&std::fs::read(&saved_report).expect("read saved report"))
            .expect("parse saved report");
    assert_eq!(report.results.len(), 3);
    assert!(
        report
            .results
            .iter()
            .all(|result| matches!(result.outcome, TestOutcome::Pass))
    );
    for file in [
        "report.json",
        "report.md",
        "junit.xml",
        "samples.jsonl",
        "processes.jsonl",
        "membership.jsonl",
        "events.jsonl",
        "model-requests.jsonl",
        "turns.jsonl",
    ] {
        assert!(primary.join(file).is_file(), "primary missing {file}");
        assert!(saved.join(file).is_file(), "saved bundle missing {file}");
    }

    let history = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .arg("results")
        .arg("ahrb-mock")
        .output()
        .expect("list hbench results");
    assert!(history.status.success());
    let history = String::from_utf8_lossy(&history.stdout);
    assert!(history.contains("HARNESS\tVERSION"));
    assert!(history.contains("ahrb-mock\tahrb-mock-harness"));

    let no_save_output = root.join("no-save-output");
    let no_save = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .arg("mock")
        .arg("--tests")
        .arg("1")
        .arg("--output")
        .arg(&no_save_output)
        .arg("--no-save")
        .arg("--deadline")
        .arg("120")
        .output()
        .expect("run no-save hbench subset");
    assert!(no_save.status.success());
    assert!(no_save_output.join("report.json").is_file());
    let unchanged =
        std::fs::read_to_string(root.join("results/index.jsonl")).expect("reread results index");
    assert_eq!(unchanged.lines().count(), 1);

    let deadline_output = root.join("deadline-output");
    let deadline = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .arg("run")
        .arg("--manifest")
        .arg("adapters/mock/manifest.toml")
        .arg("--output")
        .arg(&deadline_output)
        .arg("--tests")
        .arg("1-3")
        .arg("--deadline")
        .arg("0")
        .output()
        .expect("run auto-saved deadline report");
    assert_ne!(deadline.status.code(), Some(0));
    assert!(deadline_output.join("report.json").is_file());
    assert!(deadline_output.join("run-error.txt").is_file());
    let indexed =
        std::fs::read_to_string(root.join("results/index.jsonl")).expect("read deadline index");
    let lines = indexed.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let deadline_entry: IndexEntry =
        serde_json::from_str(lines[1]).expect("parse deadline index entry");
    let deadline_saved_report = root.join(&deadline_entry.report_path);
    let deadline_report: Report = serde_json::from_slice(
        &std::fs::read(&deadline_saved_report).expect("read deadline saved report"),
    )
    .expect("parse deadline saved report");
    assert_eq!(deadline_report.results.len(), 3);
    assert!(deadline_report.results.iter().all(|result| {
        matches!(
            result.outcome,
            TestOutcome::Error(_) | TestOutcome::Absent(_)
        )
    }));
    assert!(
        deadline_saved_report
            .parent()
            .expect("deadline report parent")
            .join("run-error.txt")
            .is_file()
    );
    std::fs::remove_dir_all(root).expect("remove results fixture");
}
