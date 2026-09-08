use ahrb::storage::{accounting, evidence::*, retention::*};
#[cfg(unix)]
use ahrb::storage::{auxiliaries, *};
#[cfg(unix)]
use ahrb::{evaluate::TestOutcome, fake_model::ModelRequestRecord};
#[cfg(unix)]
use serde_json::json;
#[cfg(unix)]
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn retention_classification_and_ratio_boundaries() {
    for (s, u, bodies, blocks, limited, class, coverage) in [
        (
            0,
            10,
            false,
            false,
            false,
            Some(RetentionClass::None),
            Coverage::Complete,
        ),
        (
            10,
            10,
            false,
            true,
            false,
            Some(RetentionClass::Deduplicated),
            Coverage::Complete,
        ),
        (11, 10, false, true, false, None, Coverage::Partial),
        (
            20,
            10,
            true,
            true,
            false,
            Some(RetentionClass::Full),
            Coverage::Complete,
        ),
        (5, 10, false, false, false, None, Coverage::Partial),
        (
            0,
            10,
            false,
            false,
            true,
            None,
            Coverage::RepresentationLimited,
        ),
        (
            20,
            10,
            true,
            true,
            true,
            Some(RetentionClass::Full),
            Coverage::RepresentationLimited,
        ),
    ] {
        let (summary, c, _) = classify(s, u, bodies, blocks, limited);
        assert_eq!(summary.request_retention_class, class);
        assert_eq!(c, coverage);
        assert_eq!(summary.stored_unique_ratio, Some(s as f64 / u as f64));
    }
    assert_eq!(
        classify(0, 0, false, false, false).0.stored_unique_ratio,
        None
    );
    assert_eq!(
        union_length(&[(0, 10), (5, 10), (30, 5), (0, 10)]).unwrap(),
        20
    );
    assert!(union_length(&[(u64::MAX, 1)]).is_err());
}

#[cfg(unix)]
fn temp(label: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ahrb-l3-{label}-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
#[cfg(unix)]
fn record() -> ModelRequestRecord {
    serde_json::from_value(json!({"request":{"dialect":"openai-chat-completions","endpoint":"/v1/chat/completions","model":"fake","credential_fingerprint":"fake","stream":false,"scenario":TASK,"actor":"storage","checkpoint":"t0001","canonical":{"messages":[{"role":"user","content":"known prompt"},{"role":"assistant","content":"known reply"}]}},"canonical_hash":"unused","attempts":1,"accepted":true,"semantic_ordinal":1,"attempt":1,"received_ns":1,"body_bytes":0,"role":"primary","side_channel_kind":null,"response_status":200,"response_first_frame_yield_ns":null,"response_last_frame_yield_ns":null,"semantic_attempts_total":1})).unwrap()
}

#[test]
#[cfg(unix)]
fn exhaustive_fixture_bundles_cover_modes_missing_capture_and_unrelated_growth() {
    for mode in [
        "none",
        "full",
        "full-binary",
        "deduplicated",
        "partial",
        "opaque",
        "missing",
    ] {
        let root = temp(mode);
        let out = temp("bodies");
        let config = StorageConfig::default();
        std::fs::write(root.join("baseline"), b"unchanged baseline").unwrap();
        let baseline = accounting::inventory(&root, &config, true).unwrap();
        let content = baseline_bytes(&root, &baseline).unwrap();
        let mut r = record();
        let raw = serde_json::to_vec(&r.request.canonical).unwrap();
        r.body_bytes = raw.len() as u64;
        let hash = digest(&raw);
        std::fs::create_dir(out.join("request-bodies")).unwrap();
        std::fs::write(out.join(format!("request-bodies/{hash}.bin")), &raw).unwrap();
        std::fs::write(
            out.join("storage-request-bodies.jsonl"),
            if mode == "missing" {
                String::new()
            } else {
                json!({"repetition":1,"received_ns":1,"raw_sha256":hash}).to_string() + "\n"
            },
        )
        .unwrap();
        let mut side = r.clone();
        side.received_ns = 2;
        side.role = "side-channel".into();
        side.semantic_ordinal = 2;
        let pretty = serde_json::to_vec_pretty(&side.request.canonical).unwrap();
        side.body_bytes = pretty.len() as u64;
        let pretty_hash = digest(&pretty);
        std::fs::write(
            out.join(format!("request-bodies/{pretty_hash}.bin")),
            &pretty,
        )
        .unwrap();
        let mut retry = r.clone();
        retry.received_ns = 3;
        retry.attempt = 2;
        if mode != "missing" {
            std::fs::write(
                out.join("storage-request-bodies.jsonl"),
                [
                    json!({"repetition":1,"received_ns":1,"raw_sha256":hash}),
                    json!({"repetition":1,"received_ns":2,"raw_sha256":pretty_hash}),
                    json!({"repetition":1,"received_ns":3,"raw_sha256":hash}),
                ]
                .iter()
                .map(|r| r.to_string() + "\n")
                .collect::<String>(),
            )
            .unwrap();
        }
        let blocks = r.request.canonical["messages"].as_array().unwrap();
        match mode {
            "full" | "full-binary" => {
                std::fs::write(root.join("actual-bytes"), &raw).unwrap();
                if mode == "full-binary" {
                    std::fs::write(root.join("unrelated-binary"), [0, 255, 128]).unwrap();
                }
            }
            "deduplicated" | "partial" => {
                for (i, b) in blocks
                    .iter()
                    .take(if mode == "partial" { 1 } else { 2 })
                    .enumerate()
                {
                    std::fs::write(
                        root.join(format!("block-{i}")),
                        serde_json::to_vec(b).unwrap(),
                    )
                    .unwrap();
                }
            }
            "opaque" => std::fs::write(root.join("encrypted"), [0, 255, 128]).unwrap(),
            _ => std::fs::write(root.join("growth"), vec![b'X'; 1024 * 1024]).unwrap(),
        }
        if mode == "none" {
            std::fs::write(root.join(&hash), b"unrelated bytes despite digest filename").unwrap();
        }
        let final_inventory = accounting::inventory(&root, &config, true).unwrap();
        let result = collect(
            1,
            &root,
            &config,
            &baseline,
            &content,
            &final_inventory,
            &BTreeSet::new(),
            &[r, side, retry],
            &out,
        );
        if mode == "missing" {
            assert!(result.unwrap_err().to_string().contains("capture-error"));
        } else {
            let (trial, matches) = result.unwrap();
            assert_eq!(trial.diagnostics.body_blobs.len(), 3);
            let expected = match mode {
                "none" => Some(RetentionClass::None),
                "full" | "full-binary" => Some(RetentionClass::Full),
                "deduplicated" => Some(RetentionClass::Deduplicated),
                _ => None,
            };
            assert_eq!(trial.summary.request_retention_class, expected, "{mode}");
            if mode == "deduplicated" {
                assert_eq!(trial.summary.stored_unique_ratio, Some(1.0));
            }
            if mode == "none" {
                assert!(matches.is_empty());
            }
            let details = RetentionDetails {
                measurement_label: MEASUREMENT_LABEL.into(),
                reason: trial.reason.clone(),
                trials: vec![trial],
                matches,
            };
            validate_details(ROWS[7], &serde_json::to_value(details).unwrap()).unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(out).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn auxiliary_caps_include_rotated_siblings_and_all_repetitions() {
    let root = temp("aux");
    let logs = root.join("logs");
    std::fs::create_dir(&logs).unwrap();
    let config = StorageConfig {
        areas: Some(BTreeMap::from([
            ("logs".into(), vec!["logs/**".into()]),
            ("empty".into(), vec!["absent/**".into()]),
        ])),
        auxiliary_cap_bytes: Some(BTreeMap::from([
            ("logs".into(), 8192),
            ("empty".into(), 0),
            ("other".into(), 0),
        ])),
        ..Default::default()
    };
    let mut snapshots = Vec::new();
    for t in checkpoints(100) {
        if logs.join("active").exists() {
            std::fs::rename(logs.join("active"), logs.join(format!("rotated-{t}"))).unwrap();
        }
        std::fs::write(logs.join("active"), vec![b'L'; 4096]).unwrap();
        snapshots.push((t, accounting::inventory(&root, &config, true).unwrap()));
    }
    let trial = auxiliaries::evaluate(1, 100, &snapshots, &config).unwrap();
    assert_eq!(trial.outcome, TestOutcome::Pass);
    let logs = trial
        .summary
        .auxiliaries
        .iter()
        .find(|a| a.name == "logs")
        .unwrap();
    assert_eq!(logs.class, Some(BoundClass::Unbounded));
    assert!(logs.rotation_observed);
    assert!(logs.peak_allocated_bytes > 8192);
    let mut capped = config.clone();
    capped
        .auxiliary_cap_bytes
        .as_mut()
        .unwrap()
        .insert("logs".into(), logs.peak_allocated_bytes);
    let bounded = auxiliaries::evaluate(2, 100, &snapshots, &capped).unwrap();
    assert_eq!(
        bounded
            .summary
            .auxiliaries
            .iter()
            .find(|a| a.name == "logs")
            .unwrap()
            .class,
        Some(BoundClass::Bounded)
    );
    capped.auxiliary_cap_bytes = None;
    assert!(matches!(
        auxiliaries::evaluate(1, 100, &snapshots, &capped)
            .unwrap()
            .outcome,
        TestOutcome::Unsupported(_)
    ));
    assert!(auxiliaries::evaluate(1, 100, &snapshots[..4], &config).is_err());
    assert!(
        auxiliaries::checkpoint_diagnostics(&snapshots[..3], &config)
            .checkpoints
            .iter()
            .any(|p| p.identity_replacements > 0)
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("active", root.join("logs/link")).unwrap();
        assert!(accounting::inventory(&root, &config, true).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn overlapping_representations_copies_and_baseline_fragments_count_exactly() {
    let entry = accounting::FileEntry {
        path: "content".into(),
        kind: "regular".into(),
        device_id: 1,
        inode_or_file_id: 2,
        allocated_bytes: 4096,
        apparent_bytes: 16,
        sha256: None,
        family: "other".into(),
    };
    let needles = vec![
        Needle {
            bytes: b"abcdefgh".to_vec(),
            representation: "raw-body",
            request: Some("request".into()),
            block: None,
        },
        Needle {
            bytes: b"abcdefgh".to_vec(),
            representation: "canonical-body",
            request: Some("request".into()),
            block: None,
        },
        Needle {
            bytes: b"cdef".to_vec(),
            representation: "canonical-block",
            request: None,
            block: Some("block".into()),
        },
    ];
    let matches = scan_file(1, &entry, b"abcdefghabcdefgh", &[], &needles);
    assert_eq!(
        union_length(
            &matches
                .iter()
                .map(|m| (m.offset_bytes, m.length_bytes))
                .collect::<Vec<_>>()
        )
        .unwrap(),
        16
    );
    let matches = scan_file(1, &entry, b"abcdefghabcdefgh", &[(2, 4)], &needles);
    assert_eq!(
        union_length(
            &matches
                .iter()
                .filter(|m| !m.excluded_baseline)
                .map(|m| (m.offset_bytes, m.length_bytes))
                .collect::<Vec<_>>()
        )
        .unwrap(),
        12
    );
    assert!(matches.iter().any(|m| m.excluded_baseline));
    assert!(
        matches
            .iter()
            .filter(|m| !m.excluded_baseline
                && !m.representation.ends_with("-fragment")
                && m.request_sha256.is_some())
            .all(|m| m.offset_bytes == 8)
    );
}
