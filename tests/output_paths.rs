mod common;

use ahrb::report::Report;
use std::path::Path;
use std::process::Command;

#[test]
fn mock_exec_succeeds_with_relative_output_from_a_foreign_cwd() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let foreign = std::env::temp_dir().join(format!(
        "ahrb-relative-output-foreign-cwd-{}",
        std::process::id()
    ));
    if foreign.exists() {
        std::fs::remove_dir_all(&foreign).expect("remove stale foreign cwd");
    }
    std::fs::create_dir_all(&foreign).expect("create foreign cwd");
    let run = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(&foreign)
        .arg("run")
        .arg("--manifest")
        .arg(repository.join("adapters/mock-exec/manifest.toml"))
        .arg("--output")
        .arg("relative-output")
        .arg("--tests")
        .arg("1")
        .arg("--no-save")
        .arg("--deadline")
        .arg("120")
        .output()
        .expect("run mock-exec from foreign cwd");
    assert!(
        run.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    let report_path = foreign.join("relative-output/report.json");
    let report: Report =
        serde_json::from_slice(&std::fs::read(&report_path).expect("read foreign-cwd report"))
            .expect("parse foreign-cwd report");
    assert_eq!(report.results.len(), 1);
    std::fs::remove_dir_all(foreign).expect("remove foreign cwd");
}
