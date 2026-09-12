use ahrb::{
    evaluate::TestOutcome,
    storage::{evidence::*, lifecycle::*, *},
};

fn close_diagnostics(n: u64, immediate: u64, post: Option<(f64, u64)>) -> CloseDiagnostics {
    let mut checkpoints = (0..=n)
        .filter(|v| v % 10 == 0 || *v == n)
        .map(|closed_sessions| CloseCheckpoint {
            closed_sessions,
            elapsed_s: 2.1,
            phase: ClosePhase::Immediate,
            retained_allocated_bytes: if closed_sessions == 0 { 0 } else { immediate },
        })
        .collect::<Vec<_>>();
    if let Some((elapsed_s, retained_allocated_bytes)) = post {
        checkpoints.push(CloseCheckpoint {
            closed_sessions: n,
            elapsed_s,
            phase: ClosePhase::PostSweep,
            retained_allocated_bytes,
        });
    }
    CloseDiagnostics {
        store_baseline_allocated_bytes: Some(4096),
        checkpoints,
        close_receipts: (1..=n)
            .map(|i| CloseReceipt {
                session_id_hash: format!("{i:064x}"),
                exit_code: 0,
                receipt_sha256: "a".repeat(64),
            })
            .collect(),
    }
}

#[test]
fn close_cap_is_total_and_boundary_equality_is_bounded() {
    let config = StorageConfig {
        retention_cap_bytes: Some(100),
        ..Default::default()
    };
    let (summary, outcome) =
        evaluate_close(&config, 20, &close_diagnostics(20, 100, None)).unwrap();
    assert_eq!(summary.close_retention_class, Some(BoundClass::Bounded));
    assert_eq!(summary.close_retained_bytes_per_session, Some(5.0));
    assert_eq!(summary.close_retained_after_sweep_bytes_per_session, None);
    assert!(matches!(outcome, TestOutcome::Pass));
    let (summary, outcome) =
        evaluate_close(&config, 20, &close_diagnostics(20, 101, None)).unwrap();
    assert_eq!(summary.close_retention_class, Some(BoundClass::Unbounded));
    assert!(
        matches!(outcome, TestOutcome::Pass),
        "cap breach is informational"
    );
    let mut early_breach = close_diagnostics(20, 0, None);
    early_breach.checkpoints[1].retained_allocated_bytes = 101;
    assert_eq!(
        evaluate_close(&config, 20, &early_breach)
            .unwrap()
            .0
            .close_retention_class,
        Some(BoundClass::Unbounded)
    );
}

#[test]
fn sweep_requires_elapsed_interval_and_replaces_immediate_cap_assessment() {
    let config = StorageConfig {
        retention_cap_bytes: Some(100),
        sweep_interval_s: Some(5),
        ..Default::default()
    };
    let d = close_diagnostics(20, 5000, Some((7.1, 100)));
    let (s, o) = evaluate_close(&config, 20, &d).unwrap();
    assert!(matches!(o, TestOutcome::Pass));
    assert_eq!(s.close_retention_class, Some(BoundClass::Bounded));
    assert_eq!(s.close_retained_after_sweep_bytes_per_session, Some(5.0));
    for post in [None, Some((4.99, 0))] {
        assert!(evaluate_close(&config, 20, &close_diagnostics(20, 5000, post)).is_err());
    }
    let no_cap = StorageConfig {
        sweep_interval_s: Some(5),
        ..Default::default()
    };
    let (s, o) = evaluate_close(&no_cap, 20, &d).unwrap();
    assert!(matches!(o, TestOutcome::Unsupported(_)));
    assert_eq!(s.close_retention_class, None);
    assert_eq!(s.close_retained_bytes_per_session, Some(250.0));
}

#[test]
fn missing_store_checkpoint_or_public_receipt_cannot_be_measured_zero() {
    let config = StorageConfig {
        retention_cap_bytes: Some(0),
        ..Default::default()
    };
    let good = close_diagnostics(20, 0, None);
    let mut d = good.clone();
    d.store_baseline_allocated_bytes = None;
    assert!(evaluate_close(&config, 20, &d).is_err());
    let mut d = good.clone();
    d.checkpoints.remove(1);
    assert!(evaluate_close(&config, 20, &d).is_err());
    let mut d = good.clone();
    d.close_receipts.pop();
    assert!(evaluate_close(&config, 20, &d).is_err());
    let mut d = good.clone();
    d.close_receipts[0].receipt_sha256.clear();
    assert!(evaluate_close(&config, 20, &d).is_err());
    let mut d = good.clone();
    d.close_receipts[0].exit_code = 1;
    assert!(evaluate_close(&config, 20, &d).is_err());
    let mut d = good.clone();
    d.close_receipts[1] = d.close_receipts[0].clone();
    assert!(evaluate_close(&config, 20, &d).is_err());
}

#[test]
fn compaction_percent_is_signed_and_zero_baseline_is_null() {
    assert_eq!(compaction_summary(100, 25).compaction_freed_pct, Some(75.0));
    assert_eq!(
        compaction_summary(100, 125).compaction_freed_pct,
        Some(-25.0)
    );
    assert_eq!(compaction_summary(100, 100).compaction_freed_pct, Some(0.0));
    assert_eq!(compaction_summary(0, 25).compaction_freed_pct, None);
    assert!(
        compaction_summary(u64::MAX, u64::MAX - 1)
            .compaction_freed_pct
            .unwrap()
            > 0.0
    );
    let summary = StorageSummary {
        compaction_before_allocated_bytes: Some(100.0),
        compaction_after_allocated_bytes: Some(125.0),
        compaction_freed_pct: Some(-25.0),
        ..Default::default()
    };
    assert!(
        render_markdown(&summary, &Default::default())
            .contains("compaction frees -25.000% of allocated disk")
    );
    let zero = StorageSummary {
        compaction_before_allocated_bytes: Some(0.0),
        ..Default::default()
    };
    assert!(
        render_markdown(&zero, &Default::default())
            .contains("compaction frees unavailable% of allocated disk (zero baseline)")
    );
    assert!(
        !render_markdown(&StorageSummary::default(), &Default::default()).contains("frees +0.000%")
    );
    let mixed = StorageSummary {
        compaction_before_allocated_bytes: Some(100.0),
        compaction_after_allocated_bytes: Some(25.0),
        compaction_freed_pct: None,
        ..Default::default()
    };
    let mut report = ahrb::report::Report::default();
    assert!(render_markdown(&mixed, &report).contains("missing compacted pair"));
    report.details.insert("compaction-vs-disk".into(), serde_json::json!({"trials":[{"measurement_complete":true,"summary":{"compaction_before_allocated_bytes":0.0}}]}));
    assert!(render_markdown(&mixed, &report).contains("(zero baseline)"));
}

#[test]
fn lifecycle_outcome_precedence_keeps_missing_evidence_visible() {
    let outcome = aggregate_outcome([
        TestOutcome::Pass,
        TestOutcome::Unsupported("no compaction".into()),
        TestOutcome::Absent("store".into()),
        TestOutcome::Error("missing receipt".into()),
    ]);
    assert!(matches!(outcome, TestOutcome::Error(_)));
}

mod common;
#[test]
fn public_mock_close_retains_session_records_and_automatic_sweep_changes_real_files() {
    use ahrb::events::{EventVocab, NormalizedEvent};
    use std::{
        fs,
        process::Command,
        time::{Duration, Instant},
    };
    let _guard = common::serialize_ahrb_subprocesses();
    for (mode, sweep, expected) in [
        ("capped", "off", 262144),
        ("capped", "on", 0),
        ("grow", "on", 262144),
    ] {
        let root = std::env::temp_dir().join(format!(
            "ahrb-lifecycle-close-{}-{}",
            std::process::id(),
            ahrb::fake_model::monotonic_timestamp_ns()
        ));
        let state = root.join("state");
        let store = state.join("sessions/session-1");
        fs::create_dir_all(&store).unwrap();
        let meta = br#"{"id":"session-1","marker":"storage:test"}"#;
        fs::write(store.join("meta.json"), meta).unwrap();
        let event = NormalizedEvent {
            id: "terminal-1".into(),
            cursor: 1,
            session_id: "session-1".into(),
            actor: "storage".into(),
            event: EventVocab::TerminalSuccess,
            payload: serde_json::json!({"status":"success"}),
        };
        let mut journal = serde_json::to_vec(&event).unwrap();
        journal.push(b'\n');
        fs::write(store.join("journal.jsonl"), &journal).unwrap();
        let started = Instant::now();
        let output = Command::new(env!("CARGO_BIN_EXE_ahrb-mock-harness"))
            .args(["session-close", "--state-dir"])
            .arg(&state)
            .args(["--session-id", "session-1"])
            .env_clear()
            .env("AHRB_MOCK_STORAGE_CLOSE_RETENTION", mode)
            .env("AHRB_MOCK_STORAGE_SWEEP", sweep)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "close blocked until automatic sweep"
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["closed"], true);
        assert_eq!(receipt["deleted"], false);
        assert_eq!(receipt["sweep_interval_s"], 1);
        for adapter in ["mock", "mock-exec"] {
            let manifest = ahrb::manifest::load(std::path::Path::new(&format!(
                "adapters/{adapter}/manifest.toml"
            )))
            .unwrap();
            assert_eq!(manifest.storage.unwrap().sweep_interval_s, Some(1));
        }
        assert!(fs::metadata(store.join("closed-cache.bin")).unwrap().len() > 0);
        if sweep == "on" {
            std::thread::sleep(Duration::from_millis(1500));
        }
        assert_eq!(
            fs::metadata(store.join("closed-cache.bin")).unwrap().len(),
            expected
        );
        assert_eq!(fs::read(store.join("meta.json")).unwrap(), meta);
        assert_eq!(fs::read(store.join("journal.jsonl")).unwrap(), journal);
        fs::remove_dir_all(root).unwrap();
    }
}
