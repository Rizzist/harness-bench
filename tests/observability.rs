mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn run_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-observability-{label}-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn variant_manifest(root: &Path, flag: Option<&str>) -> PathBuf {
    std::fs::create_dir_all(root).expect("create observability variant root");
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml"),
    )
    .expect("read mock manifest");
    let rendered = flag.map_or(source.clone(), |flag| {
        source.replace(
            "\"--retry-max-delay-ms\", \"200\"]",
            &format!("\"--retry-max-delay-ms\", \"200\", \"{flag}\"]"),
        )
    });
    if flag.is_some() {
        assert_ne!(rendered, source, "variant flag must modify daemon argv");
    }
    let path = root.join("manifest.toml");
    std::fs::write(&path, rendered).expect("write observability variant manifest");
    path
}

fn run_variant(manifest: &Path, output: &Path, rows: &str) -> (Output, Report) {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let process = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .arg("run")
        .arg("--manifest")
        .arg(manifest)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg(rows)
        .arg("--output")
        .arg(output)
        .arg("--no-save")
        .output()
        .expect("run observability mock variant");
    let bytes = std::fs::read(output.join("report.json")).unwrap_or_else(|error| {
        panic!(
            "observability mock variant did not produce a report: {error}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&process.stdout),
            String::from_utf8_lossy(&process.stderr),
        )
    });
    let report = serde_json::from_slice(&bytes).expect("parse observability report");
    (process, report)
}

fn row<'a>(report: &'a Report, id: &str) -> &'a ahrb::evaluate::TestResult {
    report
        .results
        .iter()
        .find(|result| result.id == id)
        .unwrap_or_else(|| panic!("report omitted {id}"))
}

fn cleanup(root: &Path, reports: &[&Report]) {
    for report in reports {
        let profile = PathBuf::from(&report.profile_path);
        if profile.is_dir() {
            std::fs::remove_dir_all(profile).expect("remove observability profile");
        }
    }
    std::fs::remove_dir_all(root).expect("remove observability variant root");
}

#[test]
fn mock_journals_discriminate_narrative_and_compaction_transparency() {
    let root = run_root("variants");
    let baseline_root = root.join("baseline");
    let narrative_root = root.join("metadata-only");
    let silent_root = root.join("silent-compaction");
    let absent_root = root.join("no-compaction");
    let baseline_manifest = variant_manifest(&baseline_root, None);
    let narrative_manifest = variant_manifest(&narrative_root, Some("--suppress-narrative"));
    let silent_manifest = variant_manifest(&silent_root, Some("--silent-compaction"));
    let absent_manifest = variant_manifest(&absent_root, Some("--disable-compaction"));

    let (baseline_process, baseline) =
        run_variant(&baseline_manifest, &baseline_root.join("output"), "69,73");
    assert!(baseline_process.status.success());
    assert!(matches!(
        row(&baseline, "event-stream-completeness").outcome,
        TestOutcome::Pass
    ));
    assert_eq!(
        baseline.metrics["event_stream_completeness.narrative_reconstructability"],
        1.0
    );
    assert!(matches!(
        row(&baseline, "compaction-transparency").outcome,
        TestOutcome::Pass
    ));
    assert_eq!(baseline.metrics["compaction_transparency.score"], 1.0);

    let (narrative_process, metadata_only) =
        run_variant(&narrative_manifest, &narrative_root.join("output"), "69");
    assert!(!narrative_process.status.success());
    assert!(matches!(
        row(&metadata_only, "event-stream-completeness").outcome,
        TestOutcome::Fail(_)
    ));
    assert_eq!(
        metadata_only.metrics["event_stream_completeness.narrative_reconstructability"],
        0.0
    );
    assert_eq!(
        metadata_only.metrics["event_stream_completeness.score"],
        6.0 / 7.0
    );

    let (silent_process, silent) = run_variant(&silent_manifest, &silent_root.join("output"), "73");
    assert!(!silent_process.status.success());
    assert!(matches!(
        row(&silent, "compaction-transparency").outcome,
        TestOutcome::Fail(_)
    ));
    assert_eq!(silent.metrics["compaction_transparency.score"], 0.0);

    let (absent_process, absent) = run_variant(&absent_manifest, &absent_root.join("output"), "73");
    assert!(absent_process.status.success());
    assert!(matches!(
        row(&absent, "compaction-transparency").outcome,
        TestOutcome::Unsupported(_)
    ));
    assert_eq!(
        absent.metrics["compaction_transparency.compactions_observed"],
        0.0
    );
    assert!(absent.badge.is_none());

    cleanup(&root, &[&baseline, &metadata_only, &silent, &absent]);
}
