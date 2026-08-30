use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::Path;
use std::process::Command;

#[test]
fn undeclared_exec_operations_emit_unsupported_and_junit_skips() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output =
        std::env::temp_dir().join(format!("ahrb-capability-gating-{}", std::process::id()));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale capability output");
    }
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(repository.join("adapters/mock-exec/manifest.toml"))
        .arg("--output")
        .arg(&output)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg("18,30,31,32,33,35,36,37,39,40")
        .arg("--junit")
        .output()
        .expect("run capability-gated exec rows");
    assert_eq!(
        command.status.code(),
        Some(1),
        "stderr: {}",
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read capability report"),
    )
    .expect("parse capability report");
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
    std::fs::remove_dir_all(output).expect("remove capability output");
}

#[test]
fn missing_daemon_session_surface_reports_unsupported_instead_of_aborting() {
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
    assert_eq!(
        command.status.code(),
        Some(1),
        "stderr: {}",
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read daemon capability report"),
    )
    .expect("parse daemon capability report");
    assert!(matches!(
        report.results.first().map(|result| &result.outcome),
        Some(TestOutcome::Unsupported(_))
    ));
    std::fs::remove_dir_all(root).expect("remove daemon capability root");
}
