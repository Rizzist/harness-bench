use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn run_directory(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-mock-exec-{label}-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn run_certification(manifest: &Path, output: &Path) -> (ExitStatus, Report, String) {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(manifest)
        .arg("--output")
        .arg(output)
        .arg("--profile")
        .arg("quick")
        .arg("--junit")
        .output()
        .expect("execute AHRB against the per-invocation reference harness");
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read mock-exec report"),
    )
    .expect("parse mock-exec report");
    let junit = std::fs::read_to_string(output.join("junit.xml")).expect("read mock-exec JUnit");
    (result.status, report, junit)
}

#[test]
fn per_invocation_reference_certifies_and_core_underdeclaration_suppresses_badge() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let reference = repository.join("adapters/mock-exec/manifest.toml");
    let output = run_directory("reference");
    let (status, report, junit) = run_certification(&reference, &output);

    assert_eq!(
        status.code(),
        Some(0),
        "reference per-invocation certification must exit zero"
    );
    assert_eq!(report.results.len(), 41);
    let pass_count = report
        .results
        .iter()
        .filter(|result| matches!(result.outcome, TestOutcome::Pass))
        .count();
    let unsupported_rows = report
        .results
        .iter()
        .filter_map(|result| {
            matches!(result.outcome, TestOutcome::Unsupported(_)).then_some(result.row)
        })
        .collect::<Vec<_>>();
    assert_eq!(pass_count, 35);
    assert_eq!(unsupported_rows, vec![4, 18, 31, 32, 33, 39]);
    assert!(
        report.results.iter().all(|result| {
            !matches!(result.outcome, TestOutcome::Fail(_) | TestOutcome::Error(_))
        })
    );
    let badge = report.badge.as_ref().expect("reduced-facet badge");
    assert_eq!(badge.topology, "client-process-fanout");
    assert_eq!(badge.parallel_width, 4);
    assert_eq!(badge.facets, vec!["replay", "crash", "resume"]);
    assert_eq!(badge.comparison_scope, "within-topology-only");
    assert!(junit.contains("failures=\"0\""));
    assert!(junit.contains("skipped=\"6\""));
    std::fs::remove_dir_all(&output).expect("remove reference mock-exec output");

    let variant_root = run_directory("without-durable-journal");
    std::fs::create_dir_all(&variant_root).expect("create variant root");
    let source = std::fs::read_to_string(&reference).expect("read reference manifest");
    let changed = source
        .lines()
        .filter(|line| !line.starts_with("durable_journal = "))
        .collect::<Vec<_>>()
        .join("\n");
    assert_ne!(source, changed, "durable-journal declaration was not found");
    let variant = variant_root.join("manifest.toml");
    std::fs::write(&variant, changed).expect("write core-underdeclared manifest");
    let variant_output = variant_root.join("output");
    let (status, report, junit) = run_certification(&variant, &variant_output);

    assert_eq!(
        status.code(),
        Some(0),
        "core UNSUPPORTED is nonfatal when no FAIL/ERROR occurred"
    );
    assert!(report.badge.is_none(), "core UNSUPPORTED must block badge");
    assert!(matches!(
        report
            .results
            .iter()
            .find(|result| result.row == 40)
            .map(|result| &result.outcome),
        Some(TestOutcome::Unsupported(_))
    ));
    assert!(
        report.results.iter().all(|result| {
            !matches!(result.outcome, TestOutcome::Fail(_) | TestOutcome::Error(_))
        })
    );
    assert!(junit.contains("failures=\"0\""));
    assert!(junit.contains("skipped=\"7\""));
    std::fs::remove_dir_all(&variant_root).expect("remove variant mock-exec output");
}
