mod common;
use ahrb::{
    cli::{self, Command, Profile},
    hbench, manifest,
    storage::*,
};
use std::{path::PathBuf, process::Command as ProcessCommand};

#[test]
fn storage_parser_is_independent() {
    for pillar in ["matrix", "economy", "fidelity", "storage"] {
        let args = [
            "run",
            "--manifest",
            "mock.toml",
            "--pillar",
            pillar,
            "--profile",
            "cert",
            "--deadline",
            "0",
            "--no-save",
            "--junit",
        ]
        .map(str::to_owned);
        let parsed = cli::parse(&args).expect("parse pillar");
        match (pillar, parsed) {
            ("matrix", Command::Run(o))
            | ("economy", Command::Economy(o))
            | ("fidelity", Command::Fidelity(o))
            | ("storage", Command::Storage(o)) => {
                assert_eq!(o.profile, Profile::Cert);
                assert_eq!(o.deadline_secs, Some(0));
                assert!(o.no_save && o.junit);
            }
            _ => panic!("pillar dispatch crossed"),
        }
        if pillar != "matrix" {
            let mut args = args.to_vec();
            args.extend(["--tests".into(), "1".into()]);
            assert!(cli::parse(&args).is_err());
        }
    }
    assert!(matches!(
        cli::parse(&["run", "--manifest", "m"].map(str::to_owned)).expect("matrix default"),
        Command::Run(_)
    ));
    for name in ["mock", "mock-exec", "pi"] {
        assert!(hbench::parse_storage(&[name.to_owned()]).is_ok());
        assert!(hbench::parse_storage(&[name.to_owned(), "--tests".into(), "1".into()]).is_err());
    }
}

#[test]
fn storage_deadlines_follow_serialized_budget_and_precedence() {
    for (profile, r, n, c, q, h, expected, k) in [
        (Profile::Quick, 3, 100, 20, 1, 3600, 10509, 1645),
        (Profile::Cert, 7, 1000, 200, 3, 14400, 174979, 38233),
    ] {
        let computed = 2 * r * (n + 1)
            + 2 * r
            + r * (2 + c / 10)
            + 2 * r * (n + 2)
            + q * (n + 3)
            + r * (n + 2);
        assert_eq!(computed, k);
        let default = resolve_deadline(profile, None, None, Some(60)).expect("default");
        assert_eq!(default.seconds, expected + r * 60);
        assert_eq!((default.k, default.h, default.w), (k, h, 60));
        assert_eq!(default.source, "storage-default");
        assert_eq!(
            resolve_deadline(profile, None, Some("42"), None)
                .expect("environment")
                .seconds,
            42
        );
        assert_eq!(
            resolve_deadline(profile, Some(0), Some("bad"), None)
                .expect("flag precedence")
                .seconds,
            0
        );
        for invalid in ["-1", "1.5", "bad", "18446744073709551616"] {
            assert!(resolve_deadline(profile, None, Some(invalid), None).is_err());
        }
    }
    assert!(resolve_deadline(Profile::Quick, None, None, Some(u64::MAX)).is_err());
    assert!(resolve_deadline(Profile::Quick, Some(u64::MAX), None, None).is_err());
}

#[test]
fn storage_manifests_are_additive_and_strict() {
    let text = std::fs::read_to_string("adapters/mock/manifest.toml").expect("mock");
    let text = text.split("\n[storage]").next().expect("base manifest");
    let omitted: manifest::Manifest = toml::from_str(text).expect("old manifest");
    assert!(omitted.storage.is_none());
    let good = format!(
        "{text}\n[storage]\nsession_close=[]\nsweep_interval_s=1\nretention_cap_bytes=0\nworkspace_path='{{{{profile}}}}/work/{{{{session_id}}}}'\n[storage.areas]\nstore=['state/**']\nlogs=[]\nother=[]\n[storage.auxiliary_cap_bytes]\nother=0\nstore=1024\n"
    );
    let parsed: manifest::Manifest = toml::from_str(&good).expect("storage manifest");
    manifest::validate(&parsed).expect("valid");
    let config = parsed.storage.expect("config");
    assert_eq!(config.session_close, Some(vec![]));
    assert_eq!(config.session_delete, None);
    for bad in [
        "sweep_interval_s=0",
        "unknown=1",
        "retention_cap_bytes=-1",
        "workspace_path='/tmp/outside'",
        "workspace_path='{{profile}}/../outside'",
        "session_delete=['harness','delete','{{session_id}}']",
        "session_delete=['sh','-c','{{profile}}','{{session_id}}']",
        "session_close=['harness','close','{{profile}}','{{credential}}','{{session_id}}']",
    ] {
        let parsed = toml::from_str::<manifest::Manifest>(&format!("{text}\n[storage]\n{bad}\n"));
        assert!(
            parsed.as_ref().is_err() || manifest::validate(&parsed.expect("parsed")).is_err(),
            "accepted {bad}"
        );
    }
}

fn fresh(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-storage-test-{label}-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ))
}

#[test]
fn both_real_cli_forms_preserve_zero_deadline_reports() {
    let _guard = common::serialize_ahrb_subprocesses();
    for (short, name) in [
        (false, "mock"),
        (true, "mock-exec"),
        (true, "mock-exec-plaintext"),
    ] {
        let output = fresh(name);
        let mut command = ProcessCommand::new(if short {
            env!("CARGO_BIN_EXE_hbench")
        } else {
            env!("CARGO_BIN_EXE_ahrb")
        });
        if short {
            command.args(["storage", name]);
        } else {
            command.args([
                "run",
                "--pillar",
                "storage",
                "--manifest",
                &format!("adapters/{name}/manifest.toml"),
            ]);
        }
        let result = command
            .args(["--profile", "quick", "--deadline", "0", "--output"])
            .arg(&output)
            .arg("--no-save")
            .output()
            .expect("CLI");
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let raw = std::fs::read(output.join("report.json")).expect("interrupted report");
        let report: ahrb::report::Report =
            serde_json::from_slice(&raw).expect("typed storage report");
        assert_eq!(report.schema, 4);
        assert_eq!(report.spec_version, 4);
        assert_eq!(report.pillar.as_deref(), Some("storage"));
        assert_eq!(report.results.len(), 10);
        assert!(report.badge.is_none());
        for row in &report.results {
            assert_eq!(row.pillar, ahrb::evaluate::Pillar::Storage);
            assert!(
                matches!(&row.outcome,ahrb::evaluate::TestOutcome::Error(reason) if reason=="deadline")
            );
            assert!(!row.metadata.measurement_complete);
            assert!(row.metadata.score.is_none());
        }
        let summary = report.storage_summary.expect("summary");
        assert_eq!(summary.completed_turns, 0);
        assert!(summary.write_bytes_per_turn_p95.is_none());
        assert!(summary.footprint_curve.is_empty());
        for file in [
            "storage-samples.jsonl",
            "storage-files.jsonl",
            "fsync-events.jsonl",
            "request-body-matches.jsonl",
            "events.jsonl",
            "turns.jsonl",
            "model-requests.jsonl",
            "samples.jsonl",
            "processes.jsonl",
            "membership.jsonl",
            "run-error.txt",
        ] {
            assert!(output.join(file).is_file(), "missing {file}");
        }
        let markdown = std::fs::read_to_string(output.join("report.md")).expect("markdown");
        assert!(markdown.contains("Storage v4"));
        assert!(markdown.contains("unavailable"));
        assert!(!markdown.contains("Automation Ready"));
        std::fs::remove_dir_all(output).expect("remove owned fixture");
    }
}

#[test]
fn plaintext_shared_task_is_unsupported_before_stimulus_in_both_cli_forms() {
    let _guard = common::serialize_ahrb_subprocesses();
    for short in [false, true] {
        let output = fresh("plaintext-preflight");
        let mut command = ProcessCommand::new(if short {
            env!("CARGO_BIN_EXE_hbench")
        } else {
            env!("CARGO_BIN_EXE_ahrb")
        });
        if short {
            command.args(["storage", "mock-exec-plaintext"]);
        } else {
            command.args([
                "run",
                "--pillar",
                "storage",
                "--manifest",
                "adapters/mock-exec-plaintext/manifest.toml",
            ]);
        }
        let result = command
            .args([
                "--profile",
                "quick",
                "--deadline",
                "10509",
                "--no-save",
                "--output",
            ])
            .arg(&output)
            .output()
            .expect("plaintext CLI");
        // Later-lane unimplemented rows retain their ERROR outcomes.
        assert_eq!(
            result.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: ahrb::report::Report =
            serde_json::from_slice(&std::fs::read(output.join("report.json")).expect("report"))
                .expect("typed report");
        for i in [0, 2, 6, 7] {
            let row = &report.results[i];
            assert!(
                matches!(&row.outcome, ahrb::evaluate::TestOutcome::Unsupported(reason)
                if reason.contains("pre-reap-observation-unavailable") && reason.contains("terminal_rules=0"))
            );
            assert!(row.metadata.measurement_complete);
            assert!(
                report.details[&row.id]["trials"]
                    .as_array()
                    .expect("trials")
                    .is_empty()
            );
        }
        assert!(
            report
                .results
                .iter()
                .enumerate()
                .filter(|(i, _)| ![0, 2, 3, 4, 6, 7].contains(i))
                .all(
                    |(_, r)| matches!(&r.outcome, ahrb::evaluate::TestOutcome::Error(_))
                        && !r.metadata.measurement_complete
                )
        );
        assert!(report.turns.is_empty());
        assert!(
            report
                .storage_samples
                .iter()
                .all(|r| r.row_id == "compaction-vs-disk")
        );
        assert!(
            report
                .storage_files
                .iter()
                .all(|r| r.row_id == "compaction-vs-disk")
        );
        assert!(report.request_body_matches.is_empty());
        assert!(
            report.details["request-body-retention"]["matches"]
                .as_array()
                .expect("matches")
                .is_empty()
        );
        // Lifecycle feasibility is independent of S1/S3's pre-reap counter gate.
        // S4 attempts its declared recovery but cannot prove tool correlations
        // from plaintext events; S5 has no declared close-only command.
        assert!(matches!(&report.results[3].outcome,
            ahrb::evaluate::TestOutcome::Error(reason)
            if reason.contains("row-51 growing session committed 0/0 tool calls/results")));
        assert!(!report.results[3].metadata.measurement_complete);
        assert!(matches!(&report.results[4].outcome,
            ahrb::evaluate::TestOutcome::Unsupported(reason)
            if reason == "no-close-without-delete"));
        assert!(report.results[4].metadata.measurement_complete);
        assert!(
            !report.model_requests.is_empty(),
            "S4 attempted recovery evidence survives"
        );
        assert!(
            report
                .model_requests
                .iter()
                .all(|r| r["row_id"] == "compaction-vs-disk")
        );
        assert!(report.badge.is_none());
        let summary = report.storage_summary.expect("summary");
        assert_eq!(summary.completed_turns, 0);
        assert_eq!(summary.physical_requests, 0);
        assert!(summary.disk_class.is_none() && summary.growth_class.is_none());
        assert!(summary.write_bytes_per_turn_p95.is_none() && summary.footprint_curve.is_empty());
        assert!(summary.auxiliaries.is_empty());
        assert!(summary.request_retention_class.is_none());
        assert!(summary.stored_request_bytes.is_none());
        assert!(summary.unique_request_content_bytes.is_none());
        assert!(summary.stored_unique_ratio.is_none());
        assert!(summary.compaction_freed_pct.is_none() && summary.closed_sessions.is_none());
        let markdown = std::fs::read_to_string(output.join("report.md")).expect("markdown");
        assert!(markdown.contains("| S1 `write-volume` | UNSUPPORTED |"));
        assert!(markdown.contains("| S3 `footprint-curve` | UNSUPPORTED |"));
        assert!(markdown.contains("| S7 `bounded-auxiliaries` | UNSUPPORTED |"));
        assert!(markdown.contains("| S8 `request-body-retention` | UNSUPPORTED |"));
        assert!(markdown.contains("shared S1/S3/S7/S8 task not started"));
        std::fs::remove_dir_all(output).expect("remove owned output");
    }
}

#[test]
fn attempted_plaintext_collection_with_a_false_structured_declaration_stays_error() {
    let _guard = common::serialize_ahrb_subprocesses();
    let root = fresh("plaintext-false-declaration");
    std::fs::create_dir(&root).expect("fixture root");
    let mut manifest = manifest::load(std::path::Path::new(
        "adapters/mock-exec-plaintext/manifest.toml",
    ))
    .expect("plaintext manifest");
    let structured = manifest::load(std::path::Path::new("adapters/mock-exec/manifest.toml"))
        .expect("structured manifest");
    manifest.events.rules = structured.events.rules;
    let path = root.join("manifest.toml");
    std::fs::write(
        &path,
        toml::to_string(&manifest).expect("manifest serialization"),
    )
    .expect("manifest");
    let output = root.join("bundle");
    let result = ProcessCommand::new(env!("CARGO_BIN_EXE_ahrb"))
        .args(["run", "--pillar", "storage", "--manifest"])
        .arg(path)
        .args(["--deadline", "30", "--no-save", "--output"])
        .arg(&output)
        .output()
        .expect("CLI");
    assert_eq!(
        result.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: ahrb::report::Report =
        serde_json::from_slice(&std::fs::read(output.join("report.json")).expect("report"))
            .expect("typed report");
    for i in [0, 2] {
        assert!(
            matches!(&report.results[i].outcome, ahrb::evaluate::TestOutcome::Error(reason)
            if reason.contains("task-incomplete") && reason.contains("invalid complete record"))
        );
        assert!(!report.results[i].metadata.measurement_complete);
    }
    assert!(
        !report.storage_samples.is_empty(),
        "collection was attempted"
    );
    assert!(
        report
            .storage_summary
            .expect("summary")
            .write_bytes_per_turn_p95
            .is_none()
    );
    std::fs::remove_dir_all(root).expect("remove owned fixture");
}

#[test]
fn storage_exit_uses_pillar_and_slug_not_matrix_row_number() {
    let mut manifest =
        manifest::load(std::path::Path::new("adapters/mock/manifest.toml")).expect("mock");
    for (id, expected) in [
        ("footprint-curve", 1),
        ("write-volume", 0),
        ("delete-uninstall-residue", 0),
    ] {
        let result = ahrb::evaluate::TestResult {
            row: 47,
            id: id.into(),
            pillar: ahrb::evaluate::Pillar::Storage,
            outcome: ahrb::evaluate::TestOutcome::Fail("measured".into()),
            evidence: vec![],
            metadata: Default::default(),
        };
        assert_eq!(
            ahrb::evaluate::suite_exit_code(&[result], None, &manifest),
            expected
        );
    }
    manifest.storage = Some(StorageConfig {
        session_delete: Some(vec![
            "harness".into(),
            "delete".into(),
            "{{profile}}".into(),
            "{{session_id}}".into(),
        ]),
        ..Default::default()
    });
    let result = ahrb::evaluate::TestResult {
        row: 6,
        id: "delete-uninstall-residue".into(),
        pillar: ahrb::evaluate::Pillar::Storage,
        outcome: ahrb::evaluate::TestOutcome::Fail("residue".into()),
        evidence: vec![],
        metadata: Default::default(),
    };
    assert_eq!(
        ahrb::evaluate::suite_exit_code(&[result], None, &manifest),
        1
    );
}

#[test]
fn insufficient_budget_preserves_partial_mock_evidence_and_null_aggregates() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = fresh("partial");
    let result = ProcessCommand::new(env!("CARGO_BIN_EXE_ahrb"))
        .args([
            "run",
            "--pillar",
            "storage",
            "--manifest",
            "adapters/mock/manifest.toml",
            "--deadline",
            "6",
            "--output",
        ])
        .arg(&output)
        .arg("--no-save")
        .output()
        .expect("CLI");
    assert_eq!(
        result.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: ahrb::report::Report =
        serde_json::from_slice(&std::fs::read(output.join("report.json")).expect("report"))
            .expect("typed report");
    let summary = report.storage_summary.expect("summary");
    assert!(summary.completed_turns > 0 && summary.completed_turns < 100);
    assert!(summary.write_bytes_per_turn_p95.is_none());
    assert!(summary.growth_class.is_none());
    for slug in ["write-volume", "footprint-curve"] {
        let trial = &report.details[slug]["trials"][0];
        assert_eq!(trial["measurement_complete"], false);
        assert_eq!(trial["outcome"]["class"], "ERROR");
        assert_eq!(trial["reason"], "deadline");
    }
    assert!(
        !report.details["footprint-curve"]["trials"][0]["diagnostics"]["checkpoints"]
            .as_array()
            .expect("partial checkpoints")
            .is_empty()
    );
    assert!(!report.storage_samples.is_empty());
    assert!(!report.storage_files.is_empty());
    assert!(!report.model_requests.is_empty());
    let mapping =
        std::fs::read_to_string(output.join("storage-request-bodies.jsonl")).expect("raw mapping");
    assert!(!mapping.is_empty());
    for line in mapping.lines() {
        use sha2::{Digest, Sha256};
        let receipt: serde_json::Value = serde_json::from_str(line).expect("body receipt");
        let bytes = std::fs::read(output.join(receipt["path"].as_str().expect("path")))
            .expect("lossless body");
        assert_eq!(
            bytes.len() as u64,
            receipt["body_bytes"].as_u64().expect("length")
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            receipt["raw_sha256"].as_str().expect("hash")
        );
    }
    assert!(report.storage_samples.iter().all(|s| s.settle_ms >= 2100.0));
    assert!(report.badge.is_none());
    // Driver bookkeeping is outside each measured repetition profile.
    assert!(
        report
            .storage_files
            .iter()
            .all(|e| !e.entry.path.contains("ahrb-exec-sessions")
                && !e.entry.path.ends_with("session.json"))
    );
    std::fs::remove_dir_all(output).expect("cleanup output");
    if !report.profile_path.is_empty() {
        std::fs::remove_dir_all(report.profile_path).expect("cleanup owned profile");
    }
}

#[test]
fn storage_details_reject_unknown_keys_and_invalid_typed_records() {
    use ahrb::storage::evidence::*;
    let details = RowDetails {
        measurement_label: MEASUREMENT_LABEL.into(),
        reason: Some("deadline".into()),
        trials: vec![Trial {
            repetition: 1,
            outcome: ahrb::evaluate::TestOutcome::Error("deadline".into()),
            measurement_complete: false,
            reason: Some("deadline".into()),
            summary: WriteSummary::default(),
            diagnostics: WriteDiagnostics::default(),
            evidence_refs: vec![],
        }],
    };
    let valid = serde_json::to_value(details).expect("typed details");
    validate_details("write-volume", &valid).expect("valid");
    for location in ["root", "summary", "diagnostics"] {
        let mut bad = valid.clone();
        if location == "root" {
            bad["unexpected"] = true.into();
        } else {
            bad["trials"][0][location]["unexpected"] = true.into();
        }
        assert!(validate_details("write-volume", &bad).is_err());
    }
    let mut bad = valid;
    bad["trials"][0]["diagnostics"]["physical_write_bytes"] = (-1).into();
    assert!(validate_details("write-volume", &bad).is_err());
    assert!(
        serde_json::from_value::<FsyncEvent>(serde_json::json!({
            "repetition":1,"turn":1,"pid":1,"start_time":1,"sequence":1,
            "primitive":"invented","enter_ns":1,"exit_ns":2,"return_code":0,"errno":0,
            "backend":"fixture","self_test":false
        }))
        .is_err()
    );
}

#[test]
fn conflicting_no_log_declaration_is_rejected_specifically_by_doctor() {
    let _guard = common::serialize_ahrb_subprocesses();
    let text = std::fs::read_to_string("adapters/mock/manifest.toml").expect("mock");
    let path = fresh("conflicting-log.toml");
    let conflict = format!("{text}\n[storage.areas]\nlogs=['state/storage-payload.bin']\n");
    std::fs::write(&path, &conflict).expect("manifest");
    let result = ProcessCommand::new(env!("CARGO_BIN_EXE_ahrb"))
        .args(["doctor", "--manifest"])
        .arg(&path)
        .output()
        .expect("doctor");
    assert_eq!(result.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(stderr.contains("conflicting-log-declaration: resources.log_paths=[] contradicts nonempty storage.areas.logs"), "{stderr}");
    let omitted = conflict.replace("log_paths = []\n", "");
    manifest::validate(&toml::from_str(&omitted).expect("omitted logs"))
        .expect("omission is not a no-log claim");
    std::fs::remove_file(path).expect("cleanup manifest");
}

#[test]
fn persistent_daemon_exec_clients_are_observed_and_retired_in_real_storage_cli() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = fresh("daemon-exec");
    let result = ProcessCommand::new(env!("CARGO_BIN_EXE_ahrb"))
        .args([
            "run",
            "--pillar",
            "storage",
            "--manifest",
            "adapters/mock-storage-daemon-exec/manifest.toml",
            "--deadline",
            "9",
            "--no-save",
            "--output",
        ])
        .arg(&output)
        .env("AHRB_MOCK_STORAGE_WRITE_MODE", "append")
        .env("AHRB_MOCK_STORAGE_GROWTH", "bounded")
        .output()
        .expect("CLI");
    assert_eq!(
        result.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: ahrb::report::Report =
        serde_json::from_slice(&std::fs::read(output.join("report.json")).expect("report"))
            .expect("typed report");
    let retirements = report
        .lifecycle_notes
        .iter()
        .filter_map(|n| n.strip_prefix("storage-client-retirement "))
        .map(|n| serde_json::from_str::<serde_json::Value>(n).expect("receipt"))
        .collect::<Vec<_>>();
    assert!(
        !retirements.is_empty(),
        "no client retirement: {:?}",
        report.results
    );
    for retirement in retirements {
        let ids = retirement["identities"].as_array().expect("identities");
        assert!(!ids.is_empty());
        assert!(
            ids.iter()
                .all(|i| i["complete"] == true
                    && i["retirement_method"] == "RetiredAfterFinalSample")
        );
        assert!(
            ids.iter()
                .any(|i| i["last_bytes"].as_u64().unwrap_or(0) > 0),
            "client physical writes absent: {ids:?}"
        );
    }
    assert!(report.storage_samples.iter().any(|s| s.turn > 0
        && s.counter_complete
        && s.physical_write_bytes.is_some_and(|b| b > 0)));
    assert!(
        report
            .storage_files
            .iter()
            .any(|f| f.entry.path == "state/storage-payload.bin" && f.entry.allocated_bytes > 0)
    );
    let audit = std::fs::read_to_string(output.join("storage-log-audit.jsonl")).expect("audit");
    assert!(audit.contains("corroborated-no-log"));
    std::fs::remove_dir_all(output).expect("cleanup output");
    std::fs::remove_dir_all(report.profile_path).expect("cleanup owned profile");
}

#[test]
fn exhaustive_storage_audit_reports_an_undeclared_log_contradiction() {
    let _guard = common::serialize_ahrb_subprocesses();
    let text = std::fs::read_to_string("adapters/mock/manifest.toml").expect("mock");
    let path = fresh("runtime-log.toml");
    std::fs::write(&path, format!("{text}\n[[isolation.generated_files]]\npath='{{{{profile}}}}/state/undeclared.log'\ncontent='log fixture'\nmode='0600'\n")).expect("manifest");
    let output = fresh("runtime-log");
    let result = ProcessCommand::new(env!("CARGO_BIN_EXE_ahrb"))
        .args(["run", "--pillar", "storage", "--manifest"])
        .arg(&path)
        .args(["--no-save", "--output"])
        .arg(&output)
        .output()
        .expect("CLI");
    assert_eq!(
        result.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: ahrb::report::Report =
        serde_json::from_slice(&std::fs::read(output.join("report.json")).expect("report"))
            .expect("typed report");
    for slug in ["write-volume", "footprint-curve"] {
        assert!(
            matches!(&report.results.iter().find(|r| r.id == slug).expect("row").outcome, ahrb::evaluate::TestOutcome::Error(r) if r.contains("conflicting-log-declaration") && r.contains("state/undeclared.log"))
        );
    }
    let audit = std::fs::read_to_string(output.join("storage-log-audit.jsonl")).expect("audit");
    assert!(audit.contains("contradicted") && audit.contains("state/undeclared.log"));
    assert!(
        report
            .storage_files
            .iter()
            .any(|f| f.entry.path == "state/undeclared.log")
    );
    assert!(
        report
            .storage_summary
            .expect("summary")
            .write_bytes_per_turn_p95
            .is_none()
    );
    std::fs::remove_dir_all(output).expect("cleanup output");
    std::fs::remove_dir_all(report.profile_path).expect("cleanup profile");
    std::fs::remove_file(path).expect("cleanup manifest");
}
