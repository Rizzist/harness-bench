mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::Path;
use std::process::Command;

#[test]
fn run_deadline_still_writes_error_report_and_diagnostic() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!("ahrb-deadline-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale deadline output");
    }
    let output = root.join("output");
    let run = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg("adapters/mock/manifest.toml")
        .arg("--output")
        .arg(&output)
        .arg("--tests")
        .arg("1,2,3")
        .arg("--deadline")
        .arg("0")
        .arg("--no-save")
        .output()
        .expect("execute deadline-limited run");
    assert_ne!(run.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(stderr.contains("run deadline reached"), "stderr: {stderr}");
    let bytes = std::fs::read(output.join("report.json")).expect("read deadline report");
    let report: Report = serde_json::from_slice(&bytes).expect("parse deadline report");
    assert_eq!(report.results.len(), 3);
    assert!(report.results.iter().all(|result| {
        matches!(&result.outcome, TestOutcome::Error(detail) if detail == "deadline")
    }));
    let diagnostic =
        std::fs::read_to_string(output.join("run-error.txt")).expect("read deadline diagnostic");
    assert!(diagnostic.contains("deadline after 0s"));
    std::fs::remove_dir_all(root).expect("remove deadline output");
}
