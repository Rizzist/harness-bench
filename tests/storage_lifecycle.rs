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

mod delete_crash_resume {
    // Public cleanup commands operate only on their disposable storage fixtures.
    use super::common;
    use ahrb::storage::{StorageConfig, accounting, lifecycle::residue};
    use std::path::PathBuf;
    use std::process::Command;

    fn root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ahrb-lifecycle-{label}-{}-{}",
            std::process::id(),
            ahrb::fake_model::monotonic_timestamp_ns()
        ));
        std::fs::create_dir_all(path.join("state/sessions/one")).unwrap();
        std::fs::write(
            path.join("state/storage-benchmark-owned"),
            "ahrb-disposable-storage-v1",
        )
        .unwrap();
        std::fs::canonicalize(path).unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_cli_refuses_symlink_paths_and_preserves_sibling_sentinel() {
        let _guard = common::serialize_ahrb_subprocesses();
        for operation in ["delete", "uninstall"] {
            for link in [
                "profile", "state", "sessions", "session", "nested", "ancestor",
            ] {
                let root = root("symlink");
                let sibling = root.join("sibling");
                std::fs::create_dir_all(sibling.join("one")).unwrap();
                let sentinel = sibling.join("one/sentinel.txt");
                std::fs::write(&sentinel, "synthetic sibling sentinel\n").unwrap();
                let profile = root.join("profile");
                std::fs::create_dir_all(profile.join("state/sessions/one")).unwrap();
                std::fs::write(
                    profile.join("state/storage-benchmark-owned"),
                    "ahrb-disposable-storage-v1",
                )
                .unwrap();
                let path = match link {
                    "profile" => profile.clone(),
                    "state" => profile.join("state"),
                    "sessions" => profile.join("state/sessions"),
                    "session" => profile.join("state/sessions/one"),
                    "nested" => profile.join("state/sessions/one/link"),
                    "ancestor" => root.join("alias"),
                    _ => unreachable!(),
                };
                if path.exists() {
                    std::fs::remove_dir_all(&path).unwrap();
                }
                std::os::unix::fs::symlink(
                    if link == "ancestor" { &root } else { &sibling },
                    &path,
                )
                .unwrap();
                let supplied = if link == "ancestor" {
                    path.join("profile")
                } else {
                    profile
                };
                assert!(
                    ahrb::storage::lifecycle::contained_path(
                        &supplied,
                        &supplied.join("state/sessions/one/link")
                    )
                    .is_err()
                );
                let audit = ahrb::storage::lifecycle::contained_path(&supplied, &supplied)
                    .and_then(|_| {
                        accounting::inventory(&supplied, &StorageConfig::default(), true)
                    });
                assert!(audit.is_err());
                let output = Command::new(env!("CARGO_BIN_EXE_ahrb-mock-harness"))
                    .args(["storage-cleanup", "--profile"])
                    .arg(&supplied)
                    .args(["--operation", operation, "--session-id", "one"])
                    .output()
                    .unwrap();
                assert!(!output.status.success(), "{operation}/{link}: {output:?}");
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("symlink"),
                    "{output:?}"
                );
                assert!(!String::from_utf8_lossy(&output.stdout).contains("\"disposable\":true"));
                assert_eq!(
                    std::fs::read_to_string(&sentinel).unwrap(),
                    "synthetic sibling sentinel\n"
                );
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
    #[test]
    fn cleanup_cli_removes_only_disposable_scopes_and_exposes_empty_file_residue() {
        let _guard = common::serialize_ahrb_subprocesses();
        for operation in ["delete", "uninstall"] {
            for mode in ["clean", "residue", "error"] {
                let root = root(mode);
                let baseline =
                    accounting::inventory(&root, &StorageConfig::default(), true).unwrap();
                std::fs::write(
                    root.join("state/sessions/one/journal.jsonl"),
                    "fake journal\n",
                )
                .unwrap();
                let mut command = Command::new(env!("CARGO_BIN_EXE_ahrb-mock-harness"));
                command
                    .args(["storage-cleanup", "--profile"])
                    .arg(&root)
                    .args(["--operation", operation]);
                if operation == "delete" {
                    command.args(["--session-id", "one"]);
                }
                let knob = if operation == "delete" {
                    "AHRB_MOCK_STORAGE_DELETE_MODE"
                } else {
                    "AHRB_MOCK_STORAGE_UNINSTALL_MODE"
                };
                let output = command.env(knob, mode).output().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(if mode == "error" { 7 } else { 0 }),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let after = accounting::inventory(&root, &StorageConfig::default(), true).unwrap();
                let scope = vec![if operation == "delete" {
                    "state/sessions".into()
                } else {
                    String::new()
                }];
                let leftovers = residue(&baseline, &after, &scope);
                assert_eq!(leftovers.len(), if mode == "clean" { 0 } else { 1 });
                if mode == "residue" {
                    assert_eq!(leftovers[0].apparent_bytes, 0);
                }
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
    #[test]
    fn cleanup_cli_refuses_profiles_without_benchmark_ownership() {
        let _guard = common::serialize_ahrb_subprocesses();
        let root = root("unowned");
        std::fs::remove_file(root.join("state/storage-benchmark-owned")).unwrap();
        std::fs::write(root.join("state/sessions/one/keep"), "untouched").unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_ahrb-mock-harness"))
            .args(["storage-cleanup", "--profile"])
            .arg(&root)
            .args(["--operation", "uninstall"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(root.join("state/sessions/one/keep").is_file());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolved_harness_cannot_turn_a_storage_verb_into_a_shell() {
        let root = root("shell");
        let argv = [
            "{{harness}}",
            "-c",
            "echo injected",
            "{{profile}}",
            "{{session_id}}",
        ]
        .map(str::to_owned);
        let vars = std::collections::BTreeMap::from([
            ("harness".into(), "/bin/sh".into()),
            ("profile".into(), root.to_string_lossy().into_owned()),
            ("session_id".into(), "one".into()),
        ]);
        assert!(
            ahrb::storage::lifecycle::render_verb("session_delete", &argv, &vars, &root).is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn declared_operation_aggregation_preserves_exit_rules_and_unavailable_receipts() {
        use ahrb::evaluate::{
            Pillar, TestOutcome, TestResult, TestResultMetadata, suite_exit_code,
        };
        use ahrb::storage::lifecycle::declared_outcome;
        let mut manifest =
            ahrb::manifest::load(std::path::Path::new("adapters/mock/manifest.toml")).unwrap();
        for (declared, operations, expected_class, expected_exit) in [
            (true, vec![TestOutcome::Pass], "PASS", 0),
            (
                true,
                vec![TestOutcome::Fail("empty file".into())],
                "FAIL",
                1,
            ),
            (false, vec![], "UNSUPPORTED", 0),
            (
                true,
                vec![TestOutcome::Absent("no locator".into())],
                "ABSENT",
                0,
            ),
            (
                true,
                vec![
                    TestOutcome::Error("exit 7".into()),
                    TestOutcome::Fail("residue".into()),
                ],
                "ERROR",
                1,
            ),
            (
                true,
                vec![
                    TestOutcome::Absent("no locator".into()),
                    TestOutcome::Fail("residue".into()),
                ],
                "FAIL",
                1,
            ),
        ] {
            manifest.storage = Some(StorageConfig {
                session_delete: declared.then(|| {
                    vec![
                        "harness".into(),
                        "delete".into(),
                        "{{profile}}".into(),
                        "{{session_id}}".into(),
                    ]
                }),
                ..Default::default()
            });
            let outcome = declared_outcome(operations.iter());
            assert_eq!(
                serde_json::to_value(&outcome).unwrap()["class"],
                expected_class
            );
            let row = TestResult {
                row: 6,
                id: "delete-uninstall-residue".into(),
                pillar: Pillar::Storage,
                outcome,
                evidence: vec![],
                metadata: TestResultMetadata::default(),
            };
            assert_eq!(suite_exit_code(&[row], None, &manifest), expected_exit);
        }
    }

    #[test]
    fn resume_timing_table_rejects_lost_and_invented_receipts() {
        use ahrb::manifest::TransportKind;
        use ahrb::storage::{
            evidence::ResumeDiagnostics,
            lifecycle::{resume_path, validate_resume_bracket},
        };
        for (p, e, c) in [
            (true, true, false),
            (true, true, true),
            (false, true, false),
            (false, true, true),
            (false, false, true),
        ] {
            let origin = if p { if c { 20 } else { 40 } } else { 10 };
            let headline = if p { 40 } else { 10 };
            let value = serde_json::json!({
                "per_invocation_topology":p,"transport_kind":if e {"exec"}else{"stdin-rpc"},"resume_control_declared":c,
                "resume_path":resume_path(p,e,c),"reattach_start_ns":10,"read_start_ns":origin,
                "control_start_ns":if e&&c {Some(20)}else{None},"control_end_ns":if e&&c {Some(30)}else{None},
                "continuation_start_ns":if e {Some(40)}else{None},"counter_start_ns":5,"resume_start_ns":headline,
                "first_request_ns":50,"counter_end_ns":60,"start_skew_ns":origin-5,"end_skew_ns":10,
                "total_resume_latency_ms":(50-origin) as f64/1e6,"first_read_bytes":0,"last_read_bytes":7,
                "session_id_hash":"id","expected_session_id_hash":"id","cursor":2,"expected_cursor":2,
                "identities":[{"pid":1,"start_time":1,"source":"fixture","first_bytes":0,"last_bytes":7,"retirement_method":if e&&c {"RetiredAfterFinalSample"}else{"Live"},"complete":true}]
            });
            let d: ResumeDiagnostics = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(d.transport_kind == TransportKind::Exec, e);
            validate_resume_bracket(&d).unwrap();
            for field in [
                "reattach_start_ns",
                "read_start_ns",
                "counter_start_ns",
                "counter_end_ns",
                "first_request_ns",
            ] {
                let mut missing = value.clone();
                missing[field] = serde_json::Value::Null;
                assert!(
                    validate_resume_bracket(&serde_json::from_value(missing).unwrap()).is_err()
                );
            }
            if e && c {
                let mut missing = value.clone();
                missing["control_end_ns"] = serde_json::Value::Null;
                assert!(
                    validate_resume_bracket(&serde_json::from_value(missing).unwrap()).is_err()
                );
            }
            if !e {
                let mut invented = value.clone();
                invented["control_start_ns"] = 20.into();
                assert!(
                    validate_resume_bracket(&serde_json::from_value(invented).unwrap()).is_err()
                );
            }
            let mut lost = value.clone();
            lost["identities"][0]["complete"] = false.into();
            assert!(validate_resume_bracket(&serde_json::from_value(lost).unwrap()).is_err());
        }
    }

    #[test]
    fn cleanup_scope_checks_do_not_strip_equals_from_positional_paths() {
        let root = root("equals=inside");
        let base = [
            "harness",
            "delete",
            "--profile",
            "{{profile}}",
            "{{session_id}}",
        ]
        .map(str::to_owned)
        .to_vec();
        let mut variables = std::collections::BTreeMap::from([
            ("profile".into(), root.to_string_lossy().into_owned()),
            ("session_id".into(), "one".into()),
        ]);
        for outside in ["/outside=part", "outside/path=part", "--path=/outside=part"] {
            let mut argv = base.clone();
            argv.push(outside.into());
            assert!(
                StorageConfig {
                    session_delete: Some(argv.clone()),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
            assert!(
                ahrb::storage::lifecycle::render_verb("session_delete", &argv, &variables, &root)
                    .is_err()
            );
        }
        // These paths are introduced only when templates are rendered, so the
        // runtime guard must independently reject them before spawning a command.
        for outside in ["/outside=part", "outside/path=part"] {
            variables.insert("session_id".into(), outside.into());
            assert!(
                ahrb::storage::lifecycle::render_verb("session_delete", &base, &variables, &root)
                    .is_err()
            );
        }
        variables.insert("session_id".into(), "one".into());
        for inside in ["{{profile}}/file=part", "--path={{profile}}/file=part"] {
            let mut argv = base.clone();
            argv.push(inside.into());
            StorageConfig {
                session_delete: Some(argv.clone()),
                ..Default::default()
            }
            .validate()
            .unwrap();
            ahrb::storage::lifecycle::render_verb("session_delete", &argv, &variables, &root)
                .unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
