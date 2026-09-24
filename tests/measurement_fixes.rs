mod common;

use ahrb::report::Report;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

fn run_mock_rows(test_name: &str, rows: &str) -> (std::path::PathBuf, Command, Report) {
    let output = std::env::temp_dir().join(format!("ahrb-l10-{test_name}-{}", std::process::id()));
    let mut command = Command::new(env!("CARGO_BIN_EXE_ahrb"));
    command
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--manifest",
            "adapters/mock/manifest.toml",
            "--tests",
            rows,
            "--profile",
            "quick",
            "--no-save",
            "--output",
        ])
        .arg(&output);
    let executed = command.output().unwrap();
    let bytes = common::read_ahrb_run_report(
        &executed,
        &output.join("report.json"),
        &format!("L10 {test_name}"),
    );
    let report: Report = serde_json::from_slice(&bytes).unwrap();
    assert!(
        executed.status.success(),
        "{:?} {}",
        report.results,
        String::from_utf8_lossy(&executed.stderr)
    );
    (output, command, report)
}

#[test]
fn daemon_exec_rows25_and46_include_short_lived_client_cpu_and_process_evidence() {
    let _guard = common::serialize_ahrb_subprocesses();
    let root = std::env::temp_dir().join(format!("ahrb-l10-owned-clients-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    // Zero post-output delay exercises the same immediate-exit boundary as L7a.
    let manifest = std::fs::read_to_string("adapters/mock-storage-daemon-exec/manifest.toml")
        .unwrap()
        .replace(
            "\"--post-output-delay-ms\", \"350\"",
            "\"--post-output-delay-ms\", \"0\"",
        );
    let manifest_path = root.join("manifest.toml");
    std::fs::write(&manifest_path, manifest).unwrap();
    let output = root.join("output");
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(["run", "--manifest"])
        .arg(&manifest_path)
        .args([
            "--tests",
            "25,46",
            "--profile",
            "quick",
            "--no-save",
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    let bytes = common::read_ahrb_run_report(
        &command,
        &output.join("report.json"),
        "row46 client ownership",
    );
    let report: Report = serde_json::from_slice(&bytes).unwrap();
    assert!(
        command.status.success(),
        "{:?} {}",
        report.results,
        String::from_utf8_lossy(&command.stderr)
    );
    assert!(report.resource_summary.cpu_per_turn_p50_ms.unwrap() > 0.0);
    for row in &report.results {
        assert!(
            row.metadata.wall_duration_s > 0.0,
            "row {} has no wall duration",
            row.row
        );
        assert_eq!(row.metadata.wall_duration_scope, "submit-to-terminal");
    }
    let row25_final = report
        .processes
        .iter()
        .filter(|sample| sample.phase.ends_with("-client-final-before-reap"))
        .collect::<Vec<_>>();
    assert!(
        !row25_final.is_empty(),
        "row25 final client CPU evidence is absent"
    );
    let mut row25_roots: std::collections::BTreeMap<_, BTreeSet<_>> =
        std::collections::BTreeMap::new();
    for sample in row25_final {
        row25_roots
            .entry(&sample.phase)
            .or_default()
            .insert(sample.process.identity);
    }
    for (phase, roots) in row25_roots {
        let width: usize = phase
            .split("-n")
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            roots.len() > width,
            "{phase}: missing daemon/client roots: {roots:?}"
        );
    }
    for repetition in 1..=3 {
        let phase = format!("memory-time-integral-r{repetition}");
        let processes = report
            .processes
            .iter()
            .filter(|sample| sample.phase == phase)
            .collect::<Vec<_>>();
        let ids = processes
            .iter()
            .map(|sample| sample.process.identity)
            .collect::<BTreeSet<_>>();
        // One daemon plus warmup and twenty independent exec clients.
        assert!(ids.len() >= 22, "missing clients: {ids:?}");
        let mut at_boundary: std::collections::BTreeMap<_, BTreeSet<_>> =
            std::collections::BTreeMap::new();
        for sample in processes {
            at_boundary
                .entry(sample.elapsed_ns)
                .or_default()
                .insert(sample.process.identity);
        }
        assert!(at_boundary.values().any(|ids| ids.len() >= 2));
    }
    std::fs::remove_dir_all(&report.profile_path).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn daemon_signal_row_clock_ends_at_its_durable_terminal_receipt() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = std::env::temp_dir().join(format!("ahrb-l10-signal-clock-{}", std::process::id()));
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--manifest",
            "adapters/mock/manifest.toml",
            "--tests",
            "57",
            "--profile",
            "quick",
            "--no-save",
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    let bytes =
        common::read_ahrb_run_report(&command, &output.join("report.json"), "row57 wall clock");
    let report: Report = serde_json::from_slice(&bytes).unwrap();
    assert!(
        command.status.success(),
        "{:?} {}",
        report.results,
        String::from_utf8_lossy(&command.stderr)
    );
    let row = &report.results[0];
    assert_eq!(row.row, 57);
    assert!(row.metadata.wall_duration_s > 0.0);
    assert_eq!(row.metadata.wall_duration_scope, "submit-to-terminal");
    std::fs::remove_dir_all(&report.profile_path).unwrap();
    std::fs::remove_dir_all(output).unwrap();
}

#[test]
fn daemon_rows50_and53_ignore_retained_zombies_for_live_cleanup() {
    let _guard = common::serialize_ahrb_subprocesses();
    let (output, _, report) = run_mock_rows("live-cleanup", "50,53");
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| result.row)
            .collect::<Vec<_>>(),
        vec![50, 53]
    );
    assert!(
        report
            .results
            .iter()
            .all(|result| matches!(result.outcome, ahrb::evaluate::TestOutcome::Pass))
    );
    assert_eq!(
        report
            .results
            .iter()
            .find(|result| result.row == 50)
            .unwrap()
            .metadata
            .wall_duration_scope,
        "submit-to-terminal"
    );
    std::fs::remove_dir_all(&report.profile_path).unwrap();
    std::fs::remove_dir_all(output).unwrap();
}

#[test]
fn recovery_row_durations_are_frozen_before_unrelated_streaming_work() {
    let _guard = common::serialize_ahrb_subprocesses();
    let (short_output, _, short) = run_mock_rows("recovery-short", "35,37,40");
    let (long_output, _, with_later_row) = run_mock_rows("recovery-with-later-row", "35,37,40,48");
    for row in [35_u8, 37, 40] {
        let short_result = short
            .results
            .iter()
            .find(|result| result.row == row)
            .unwrap();
        let long_result = with_later_row
            .results
            .iter()
            .find(|result| result.row == row)
            .unwrap();
        assert_eq!(
            short_result.metadata.wall_duration_scope,
            "submit-to-terminal"
        );
        assert_eq!(
            long_result.metadata.wall_duration_scope,
            "submit-to-terminal"
        );
        assert!(
            long_result.metadata.wall_duration_s < 2.0,
            "row {row} absorbed later row duration: short={}, with-later={}",
            short_result.metadata.wall_duration_s,
            long_result.metadata.wall_duration_s
        );
        assert!(
            (long_result.metadata.wall_duration_s - short_result.metadata.wall_duration_s).abs()
                < 1.0,
            "row {row} duration changed with unrelated row: short={}, with-later={}",
            short_result.metadata.wall_duration_s,
            long_result.metadata.wall_duration_s
        );
    }
    std::fs::remove_dir_all(&short.profile_path).unwrap();
    std::fs::remove_dir_all(&with_later_row.profile_path).unwrap();
    std::fs::remove_dir_all(short_output).unwrap();
    std::fs::remove_dir_all(long_output).unwrap();
}

#[test]
fn mock_exec_row50_live_counter_retry_survives_combined_load() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = std::env::temp_dir().join(format!(
        "ahrb-l10-row50-combined-load-{}",
        std::process::id()
    ));
    let executed = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--manifest",
            "adapters/mock-exec/manifest.toml",
            "--tests",
            "25,35,37,40,43,46,47,48,50,53,59,60,61",
            "--profile",
            "quick",
            "--no-save",
            "--output",
        ])
        .arg(&output)
        .output()
        .unwrap();
    let bytes = std::fs::read(output.join("report.json")).unwrap_or_else(|error| {
        panic!(
            "row50 combined-load run produced no report: {error}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&executed.stdout),
            String::from_utf8_lossy(&executed.stderr)
        )
    });
    let report: Report = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(report.results.len(), 13);
    let row50 = report
        .results
        .iter()
        .find(|result| result.row == 50)
        .unwrap();
    assert!(
        matches!(row50.outcome, ahrb::evaluate::TestOutcome::Pass),
        "row50 failed under combined load: {:?}\nprocess exit={:?}\nstdout:\n{}\nstderr:\n{}",
        row50,
        executed.status.code(),
        String::from_utf8_lossy(&executed.stdout),
        String::from_utf8_lossy(&executed.stderr)
    );
    assert!(row50.metadata.measurement_complete);
    assert_eq!(row50.metadata.wall_duration_scope, "submit-to-terminal");
    std::fs::remove_dir_all(&report.profile_path).unwrap();
    std::fs::remove_dir_all(output).unwrap();
}
