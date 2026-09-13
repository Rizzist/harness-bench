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

fn bundle_hashes(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, String> {
    use sha2::{Digest, Sha256};
    fn visit(
        root: &Path,
        path: &Path,
        hashes: &mut std::collections::BTreeMap<std::path::PathBuf, String>,
    ) {
        for entry in std::fs::read_dir(path).expect("list bundle") {
            let entry = entry.expect("bundle entry");
            let kind = entry.file_type().expect("artifact type");
            if kind.is_dir() {
                visit(root, &entry.path(), hashes);
            } else {
                assert!(kind.is_file(), "bundle contains non-regular artifact");
                hashes.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    format!(
                        "{:x}",
                        Sha256::digest(std::fs::read(entry.path()).expect("artifact bytes"))
                    ),
                );
            }
        }
    }
    let mut hashes = std::collections::BTreeMap::new();
    visit(root, root, &mut hashes);
    hashes
}

#[test]
fn storage_auto_save_preserves_all_files_hashes_and_lifecycle_receipts() {
    let _guard = common::serialize_ahrb_subprocesses();
    let root = std::env::temp_dir().join(format!(
        "ahrb-storage-results-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    let primary = root.join("primary");
    std::fs::create_dir(&root).unwrap();
    let run = Command::new(env!("CARGO_BIN_EXE_hbench"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("AHRB_RESULTS_ROOT", &root)
        .env("AHRB_NO_SAVE", "0")
        .env("AHRB_MOCK_STORAGE_CLOSE_RETENTION", "capped")
        .env("AHRB_MOCK_STORAGE_SWEEP", "on")
        .env("AHRB_MOCK_STORAGE_COMPACTION_DISK", "reclaim")
        .args([
            "storage",
            "mock-exec",
            "--profile",
            "quick",
            "--deadline",
            "10512",
            "--junit",
            "--output",
        ])
        .arg(&primary)
        .output()
        .expect("run auto-saved storage mock");
    std::fs::write(root.join("stdout.log"), &run.stdout).unwrap();
    std::fs::write(root.join("stderr.log"), &run.stderr).unwrap();
    let bytes = std::fs::read(primary.join("report.json")).unwrap_or_else(|error| {
        panic!(
            "storage report: {error}; status={} evidence={} stderr={}",
            run.status,
            root.display(),
            String::from_utf8_lossy(&run.stderr)
        )
    });
    let report: Report = serde_json::from_slice(&bytes).unwrap();
    for i in [0, 2, 3, 4] {
        assert!(
            matches!(report.results[i].outcome, TestOutcome::Pass),
            "{}: {:?}; evidence={}",
            report.results[i].id,
            report.results[i].outcome,
            root.display()
        );
        assert!(report.results[i].metadata.measurement_complete);
    }
    // All ten collectors are implemented; unsupported declarations remain non-failing.
    assert!(
        report
            .results
            .iter()
            .all(|r| !matches!(r.outcome, TestOutcome::Error(_) | TestOutcome::Fail(_))),
        "{:?}",
        report.results
    );
    assert_eq!(run.status.code(), Some(0));
    let summary = report.storage_summary.as_ref().unwrap();
    assert_eq!(summary.closed_sessions, Some(60));
    assert_eq!(
        summary.close_retention_class,
        Some(ahrb::storage::evidence::BoundClass::Bounded)
    );
    assert!(summary.compaction_freed_pct.unwrap() > 0.0);
    let index = std::fs::read_to_string(root.join("results/index.jsonl")).unwrap();
    assert_eq!(index.lines().count(), 1);
    let entry: IndexEntry = serde_json::from_str(index.lines().next().unwrap()).unwrap();
    assert_eq!(entry.pillar, "storage");
    let saved_report = root.join(entry.report_path);
    let saved = saved_report.parent().unwrap();
    let primary_hashes = bundle_hashes(&primary);
    let saved_hashes = bundle_hashes(saved);
    assert_eq!(
        primary_hashes,
        saved_hashes,
        "bundle sets/hashes; evidence={}",
        root.display()
    );
    for repetition in 1..=3 {
        assert!(saved_hashes.contains_key(Path::new(&format!("s4-r{repetition}-context.json"))));
        for close in 1..=20 {
            assert!(saved_hashes.contains_key(Path::new(&format!(
                "s5-r{repetition}-c{close:04}-close.json"
            ))));
        }
    }
    fn check_refs(
        value: &serde_json::Value,
        hashes: &std::collections::BTreeMap<std::path::PathBuf, String>,
    ) -> usize {
        match value {
            serde_json::Value::Object(object) => {
                let mut count = 0;
                if let (Some(file), Some(digest)) = (
                    object.get("file").and_then(|v| v.as_str()),
                    object.get("sha256").and_then(|v| v.as_str()),
                ) {
                    assert_eq!(
                        hashes.get(Path::new(file)).map(String::as_str),
                        Some(digest),
                        "saved evidence reference {file}"
                    );
                    count += 1;
                }
                count
                    + object
                        .values()
                        .map(|v| check_refs(v, hashes))
                        .sum::<usize>()
            }
            serde_json::Value::Array(values) => values.iter().map(|v| check_refs(v, hashes)).sum(),
            _ => 0,
        }
    }
    let references = check_refs(&serde_json::from_slice(&bytes).unwrap(), &saved_hashes);
    assert!(references >= 63, "all S4/S5 references checked");
    println!(
        "storage mock-exec: S1/S3/S4/S5 PASS; 3 S4 and 60 S5 receipts; {} identical file hashes; {references} valid references; evidence={}",
        primary_hashes.len(),
        root.display()
    );
    std::fs::write(
        root.join("bundle-hashes.json"),
        serde_json::to_vec_pretty(&saved_hashes).unwrap(),
    )
    .unwrap();
    // Preserve this substantial real CLI run outside Git for failure diagnosis and audits.
}

// The protected version-probe executable produces real S2 loader-refusal
// receipts. A short deadline bounds the unrelated shared storage workload.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[test]
fn storage_auto_save_preserves_native_s2_evidence_and_all_bundle_hashes() {
    storage_auto_save_preserves_bundle(false);
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[test]
fn storage_auto_save_accepts_version_probe_home_helper_links() {
    storage_auto_save_preserves_bundle(true);
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn storage_auto_save_preserves_bundle(home_helper: bool) {
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    fn file_hashes(root: &Path, directory: &Path) -> BTreeMap<std::path::PathBuf, String> {
        let mut hashes = BTreeMap::new();
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                hashes.extend(file_hashes(root, &entry.path()));
            } else {
                assert!(kind.is_file());
                hashes.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    format!("{:x}", Sha256::digest(std::fs::read(entry.path()).unwrap())),
                );
            }
        }
        hashes
    }

    let _guard = common::serialize_ahrb_subprocesses();
    let root = std::env::temp_dir().join(format!(
        "ahrb-results-s2-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&root).unwrap();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut manifest =
        ahrb::manifest::load(&repository.join("adapters/mock/manifest.toml")).unwrap();
    manifest.availability.exec_paths = vec!["/usr/bin/true".into()];
    manifest.availability.version_probe = vec!["/usr/bin/true".into()];
    if home_helper {
        manifest.availability.exec_paths = vec!["/bin/sh".into()];
        manifest.availability.version_probe = vec![
            "/bin/sh".into(),
            "-c".into(),
            "if [ \"$AHRB_DURABILITY_CONTROL_ONLY\" = 1 ]; then ln -s missing-helper \"$HOME/helper\" || exit 1; fi; printf 'version-probe-ok'".into(),
        ];
    }
    let manifest_path = root.join("manifest.toml");
    std::fs::write(&manifest_path, toml::to_string(&manifest).unwrap()).unwrap();
    let primary = root.join("primary");
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .env("AHRB_RESULTS_ROOT", &root)
        .env("AHRB_NO_SAVE", "0")
        .args(["run", "--pillar", "storage", "--manifest"])
        .arg(&manifest_path)
        .args(["--profile", "quick", "--deadline", "10", "--output"])
        .arg(&primary)
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(2), "{result:?}");
    let index = std::fs::read_to_string(root.join("results/index.jsonl")).unwrap();
    assert_eq!(index.lines().count(), 1);
    let entry: IndexEntry = serde_json::from_str(index.lines().next().unwrap()).unwrap();
    let saved_report = root.join(entry.report_path);
    let saved = saved_report.parent().unwrap();
    let primary_hashes = file_hashes(&primary, &primary);
    assert_eq!(primary_hashes, file_hashes(saved, saved));
    let launch: serde_json::Value = serde_json::from_slice(
        &std::fs::read(saved.join("durability-support/preflight/launch.json")).unwrap(),
    )
    .unwrap();
    let home = Path::new(launch["isolated_home"].as_str().unwrap());
    assert!(home.is_dir() && !home.starts_with(&primary) && !home.starts_with(saved));
    assert!(!primary.join("durability-preflight-home").exists());
    assert!(!saved.join("durability-preflight-home").exists());
    if home_helper {
        assert_eq!(launch["exit_code"], 0);
        assert_eq!(launch["stdout"], "version-probe-ok");
        assert!(home.join("helper").is_symlink());
    }
    for name in [
        "durability-support/libahrb-durability.dylib",
        "durability-support/libahrb-durability-control.dylib",
        "durability-support/preflight/launch.json",
    ] {
        assert!(
            primary_hashes.contains_key(Path::new(name)),
            "missing {name}"
        );
    }
    let report: Report = serde_json::from_slice(&std::fs::read(&saved_report).unwrap()).unwrap();
    assert!(
        matches!(&report.results[1].outcome, TestOutcome::Unsupported(reason)
        if reason.starts_with("os-limited:"))
    );
    let details: ahrb::storage::evidence::DurabilityDetails =
        serde_json::from_value(report.details["durability-cost"].clone()).unwrap();
    assert!(!details.trials.is_empty());
    for trial in details.trials {
        assert!(!trial.evidence_refs.is_empty());
        for reference in trial.evidence_refs {
            assert_eq!(primary_hashes[Path::new(&reference.file)], reference.sha256);
            match (reference.first_record, reference.last_record) {
                (None, None) => {}
                (Some(first), Some(last)) => {
                    let text = std::fs::read_to_string(saved.join(reference.file)).unwrap();
                    assert!(first >= 1 && last >= first && last <= text.lines().count() as u64);
                }
                _ => panic!("incomplete record bounds"),
            }
        }
    }
    std::fs::remove_dir_all(home).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
