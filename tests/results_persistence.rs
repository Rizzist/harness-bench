mod common;

use ahrb::results::IndexEntry;
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
    assert_eq!(entry.harness_id, "ahrb-mock");
    assert_eq!(entry.rows_run, vec![1, 2, 3]);
    assert_eq!(entry.counts.pass, 3);
    assert_eq!(entry.counts.fail, 0);
    assert_eq!(entry.counts.unsupported, 0);
    assert_eq!(entry.counts.error, 0);
    assert!(entry.load_avg_1m.is_some());
    let saved = root.join(&entry.results_dir);
    for file in [
        "report.json",
        "report.md",
        "junit.xml",
        "samples.jsonl",
        "processes.jsonl",
        "membership.jsonl",
        "events.jsonl",
        "model-requests.jsonl",
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
    assert_eq!(deadline_entry.counts.error, 3);
    assert_eq!(deadline_entry.rows_run, vec![1, 2, 3]);
    assert!(
        root.join(&deadline_entry.results_dir)
            .join("run-error.txt")
            .is_file()
    );
    std::fs::remove_dir_all(root).expect("remove results fixture");
}
