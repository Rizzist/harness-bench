mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use ahrb::results::IndexEntry;
use std::path::Path;
use std::process::Command;

#[test]
fn protocol_abort_is_mirrored_and_indexed_as_all_error() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!(
        "ahrb-protocol-abort-persistence-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale abort fixture");
    }
    std::fs::create_dir_all(&root).expect("create abort fixture");
    let source = std::fs::read_to_string(repository.join("adapters/mock-exec/manifest.toml"))
        .expect("read mock-exec manifest");
    let broken = source.replace(
        "target/debug/ahrb-mock-harness\", \"exec-turn",
        "target/debug/ahrb-mock-harness-missing\", \"exec-turn",
    );
    assert_ne!(source, broken, "mock-exec command was not replaced");
    let manifest = root.join("broken-mock-exec.toml");
    std::fs::write(&manifest, broken).expect("write broken mock-exec manifest");
    let primary = root.join("primary");
    let run = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .arg("run")
        .arg("--manifest")
        .arg(&manifest)
        .arg("--output")
        .arg(&primary)
        .arg("--tests")
        .arg("1,2,3")
        .arg("--deadline")
        .arg("120")
        .output()
        .expect("run protocol-aborting fixture");
    assert_eq!(run.status.code(), Some(2));
    assert!(primary.join("report.json").is_file());
    assert!(primary.join("run-error.txt").is_file());
    let report: Report = serde_json::from_slice(
        &std::fs::read(primary.join("report.json")).expect("read abort report"),
    )
    .expect("parse abort report");
    assert_eq!(report.results.len(), 3);
    assert!(
        report
            .results
            .iter()
            .all(|result| matches!(result.outcome, TestOutcome::Error(_)))
    );

    let index =
        std::fs::read_to_string(root.join("results/index.jsonl")).expect("read abort index");
    let lines = index.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let entry: IndexEntry = serde_json::from_str(lines[0]).expect("parse abort index entry");
    assert_eq!(entry.harness, "ahrb-mock-exec");
    let saved_report = root.join(&entry.report_path);
    let saved = saved_report.parent().expect("saved report parent");
    let saved_report: Report =
        serde_json::from_slice(&std::fs::read(&saved_report).expect("read saved abort report"))
            .expect("parse saved abort report");
    assert_eq!(saved_report.results.len(), 3);
    assert!(
        saved_report
            .results
            .iter()
            .all(|result| matches!(result.outcome, TestOutcome::Error(_)))
    );
    assert!(saved.join("report.json").is_file());
    assert!(saved.join("run-error.txt").is_file());
    std::fs::remove_dir_all(root).expect("remove abort fixture");
}
