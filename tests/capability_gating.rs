mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::Path;
use std::process::Command;

#[test]
fn undeclared_exec_operations_emit_unsupported_and_junit_skips() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!("ahrb-capability-gating-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale capability output");
    }
    std::fs::create_dir_all(&root).expect("create capability root");
    let source = std::fs::read_to_string(repository.join("adapters/mock-exec/manifest.toml"))
        .expect("read mock exec manifest");
    let source = source
        .lines()
        .filter(|line| {
            !line.starts_with("sessions = ")
                && !line.starts_with("resume = \"")
                && !line.starts_with("durable_journal = ")
                && !line.starts_with("cancel_cleanup = ")
        })
        .map(|line| {
            if line.starts_with("cancel = ") {
                "cancel = []"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let manifest = root.join("manifest.toml");
    std::fs::write(&manifest, source).expect("write under-declared exec manifest");
    let output = root.join("output");
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(&manifest)
        .arg("--output")
        .arg(&output)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg("18,30,31,32,33,35,36,37,39,40")
        .arg("--junit")
        .output()
        .expect("run capability-gated exec rows");
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &command,
        &report_path,
        "under-declared exec subprocess did not produce a report",
    );
    let report: Report = serde_json::from_slice(&report_bytes).expect("parse capability report");
    assert_eq!(report.results.len(), 10);
    assert!(report.results.iter().all(|result| {
        matches!(result.outcome, TestOutcome::Unsupported(_))
            && result
                .evidence
                .iter()
                .any(|item| item.starts_with("capability:"))
    }));
    let junit = std::fs::read_to_string(output.join("junit.xml")).expect("read junit");
    assert!(junit.contains("failures=\"0\""));
    assert!(junit.contains("skipped=\"10\""));
    assert_eq!(junit.matches("<skipped ").count(), 10);
    std::fs::remove_dir_all(root).expect("remove capability output");
}

#[test]
fn missing_daemon_session_surface_reports_unsupported_instead_of_aborting() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!(
        "ahrb-daemon-capability-gating-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale daemon capability root");
    }
    std::fs::create_dir_all(&root).expect("create daemon capability root");
    let source = std::fs::read_to_string(repository.join("adapters/mock/manifest.toml"))
        .expect("read mock manifest");
    let changed = source.replacen("attach = [\"session.attach\"]", "attach = []", 1);
    assert_ne!(source, changed, "session attach declaration was not found");
    let manifest = root.join("manifest.toml");
    std::fs::write(&manifest, changed).expect("write modified manifest");
    let output = root.join("output");
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(&manifest)
        .arg("--output")
        .arg(&output)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg("30")
        .output()
        .expect("run daemon with missing session attach");
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &command,
        &report_path,
        "under-declared daemon subprocess did not produce a report",
    );
    let report: Report =
        serde_json::from_slice(&report_bytes).expect("parse daemon capability report");
    assert!(matches!(
        report.results.first().map(|result| &result.outcome),
        Some(TestOutcome::Unsupported(_))
    ));
    std::fs::remove_dir_all(root).expect("remove daemon capability root");
}

#[test]
fn aborted_run_exits_nonzero_and_persists_a_diagnostic() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!(
        "ahrb-aborted-run-diagnostic-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale aborted-run output");
    }
    let output = root.join("output");
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(root.join("missing-manifest.toml"))
        .arg("--output")
        .arg(&output)
        .output()
        .expect("run AHRB with a missing manifest");
    assert_eq!(result.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("ahrb: run aborted for manifest"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("ahrb: report.json was not written"),
        "stderr: {stderr}"
    );
    assert!(!output.join("report.json").exists());
    let diagnostic = std::fs::read_to_string(output.join("run-error.txt"))
        .expect("read persisted run diagnostic");
    assert!(diagnostic.contains("AHRB run aborted"));
    assert!(diagnostic.contains("missing-manifest.toml"));
    std::fs::remove_dir_all(root).expect("remove aborted-run output");
}
