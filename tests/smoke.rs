mod common;

use ahrb::Result;
use ahrb::cli::{Profile, RunOptions};
use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::PathBuf;

fn output_directory() -> PathBuf {
    std::env::temp_dir().join(format!("ahrb-full-smoke-test-{}", std::process::id()))
}

#[tokio::test]
async fn mock_harness_exercises_the_non_resource_report_pipeline() -> Result<()> {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let output = output_directory();
    match std::fs::remove_dir_all(&output) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let code = ahrb::runner::run(RunOptions {
        manifest: PathBuf::from("adapters/mock/manifest.toml"),
        output: output.clone(),
        profile: Profile::Quick,
        tests: vec![1, 2, 3, 9, 10, 12, 30, 35, 40],
        junit: true,
        deadline_secs: Some(120),
        no_save: true,
        harness_version: Some("mock-harness 0.1.0".to_owned()),
    })
    .await?;
    assert_eq!(code, 0);
    let report: Report = serde_json::from_slice(&std::fs::read(output.join("report.json"))?)?;
    assert_eq!(report.results.len(), 9);
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| result.row)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 9, 10, 12, 30, 35, 40]
    );
    for row in [1_u8, 2, 3, 9, 10, 12, 30, 35, 40] {
        let outcome = report
            .results
            .iter()
            .find(|result| result.row == row)
            .map(|result| &result.outcome);
        assert!(
            matches!(outcome, Some(TestOutcome::Pass)),
            "selected smoke row {row}"
        );
    }
    assert!(
        report
            .results
            .iter()
            .all(|result| !result.evidence.is_empty())
    );
    assert!(report.badge.is_none());
    assert!(!report.samples.is_empty());
    assert!(!report.processes.is_empty());
    assert!(report.events.len() >= 9);
    assert!(!report.model_requests.is_empty());
    for metric in [
        "crash_recovery_tree_cleared",
        "crash_recovery_valid",
        "journal_recovery_valid",
        "journal_torn_tail_injected",
    ] {
        assert_eq!(report.metrics.get(metric), Some(&1.0), "metric {metric}");
    }
    for file in [
        "report.md",
        "report.json",
        "samples.jsonl",
        "processes.jsonl",
        "events.jsonl",
        "model-requests.jsonl",
        "turns.jsonl",
        "junit.xml",
    ] {
        assert!(
            output.join(file).is_file(),
            "missing report artifact {file}"
        );
    }
    std::fs::remove_dir_all(output)?;
    Ok(())
}
