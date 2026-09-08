#![cfg(unix)]
mod common;
use ahrb::{
    evaluate::TestOutcome,
    report::Report,
    storage::{
        ROWS,
        evidence::{AuxiliaryDiagnostics, AuxiliarySummary, RetentionDetails, RowDetails},
    },
};
use std::{path::PathBuf, process::Command};

fn fresh(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-l3-cli-{label}-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ))
}

#[test]
fn interrupted_both_transport_bundles_preserve_rotations_and_body_mappings() {
    let _guard = common::serialize_ahrb_subprocesses();
    for name in ["mock", "mock-exec"] {
        let root = fresh(name);
        std::fs::create_dir(&root).unwrap();
        let text = std::fs::read_to_string(format!("adapters/{name}/manifest.toml"))
            .unwrap()
            .replace("log_paths = []\n", "");
        let manifest = root.join("manifest.toml");
        std::fs::write(&manifest,format!("{text}\n[storage.areas]\nlogs=['state/storage-aux/logs.log*']\n[storage.auxiliary_cap_bytes]\nlogs=49152\nother=67108864\n")).unwrap();
        let output = root.join("bundle");
        let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
            .args(["run", "--pillar", "storage", "--manifest"])
            .arg(&manifest)
            .args([
                "--profile",
                "quick",
                "--deadline",
                "35",
                "--no-save",
                "--output",
            ])
            .arg(&output)
            .env("AHRB_MOCK_STORAGE_AUX_MODE", "capped")
            .env("AHRB_MOCK_STORAGE_REQUEST_RETENTION", "full")
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: Report =
            serde_json::from_slice(&std::fs::read(output.join("report.json")).unwrap()).unwrap();
        for id in [6, 7] {
            assert!(matches!(report.results[id].outcome, TestOutcome::Error(_)));
        }
        let s7: RowDetails<AuxiliarySummary, AuxiliaryDiagnostics> =
            serde_json::from_value(report.details[ROWS[6]].clone()).unwrap();
        assert!(!s7.trials.is_empty());
        assert!(
            s7.trials[0]
                .diagnostics
                .checkpoints
                .iter()
                .any(|p| p.identity_replacements > 0),
            "{name}: {:?}",
            s7.trials[0]
        );
        let s8: RetentionDetails = serde_json::from_value(report.details[ROWS[7]].clone()).unwrap();
        assert!(!s8.trials[0].diagnostics.body_blobs.is_empty());
        assert!(s8.trials[0].summary.request_retention_class.is_none());
        assert!(!s8.trials[0].measurement_complete);
        for blob in &s8.trials[0].diagnostics.body_blobs {
            let bytes = std::fs::read(output.join(&blob.path)).unwrap();
            assert_eq!(ahrb::storage::retention::digest(&bytes), blob.raw_sha256);
            assert!(
                s8.trials[0]
                    .evidence_refs
                    .iter()
                    .any(|r| r.file == blob.path && r.sha256 == blob.raw_sha256)
            );
        }
        let summary = report.storage_summary.unwrap();
        assert!(
            summary.request_retention_class.is_none()
                && summary.stored_request_bytes.is_none()
                && summary.auxiliaries.is_empty()
        );
        assert!(
            std::fs::read_to_string(output.join("report.md"))
                .unwrap()
                .contains("Privacy limitation")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
