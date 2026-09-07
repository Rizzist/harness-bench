use ahrb::storage::{accounting::*, *};
use std::collections::BTreeMap;

#[test]
fn glob_grammar_and_family_intersection() {
    for glob in [
        "",
        "/absolute",
        "../escape",
        "a/./b",
        "a/../b",
        "a\\b",
        "{{profile}}/x",
        "a/**b",
        "a//b",
    ] {
        assert!(validate_glob(glob).is_err(), "accepted {glob}");
    }
    for (pattern, path, expected) in [
        ("state/**/journal?.jsonl", "state/journal1.jsonl", true),
        ("state/**/journal?.jsonl", "state/a/b/journalé.jsonl", true),
        ("state/*/x", "state/a/b/x", false),
        ("a[1]", "a1", false),
        ("a[1]", "a[1]", true),
    ] {
        assert_eq!(glob_matches(pattern, path), expected);
    }
    for (a, b, yes) in [
        ("state/**", "state/x", true),
        ("a/*/x", "a/**/x", true),
        ("a/x*", "a/*y", true),
        ("a/x", "a/y", false),
        ("store/**", "logs/**", false),
    ] {
        assert_eq!(globs_overlap(a, b), yes);
    }
    let config = StorageConfig {
        areas: Some(BTreeMap::from([
            ("store".into(), vec!["state/**".into()]),
            ("logs".into(), vec!["state/*.log".into()]),
        ])),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    let no_areas = StorageConfig::default();
    assert_eq!(no_areas.family("state/a.log").expect("family"), "other");
}

#[test]
fn amplification_growth_resets_identity_and_truncation() {
    let entry = FileEntry {
        path: "x".into(),
        kind: "regular".into(),
        device_id: 1,
        inode_or_file_id: 2,
        allocated_bytes: 4096,
        apparent_bytes: 4096,
        sha256: None,
        family: "other".into(),
    };
    let mut after = entry.clone();
    after.allocated_bytes = 8192;
    after.apparent_bytes = 8192;
    assert_eq!(file_growth(Some(&entry), Some(&after)), 4096);
    after.apparent_bytes = 1;
    assert_eq!(file_growth(Some(&entry), Some(&after)), 0);
    after.apparent_bytes = 8192;
    after.inode_or_file_id = 3;
    assert_eq!(file_growth(Some(&entry), Some(&after)), 0);
    assert_eq!(file_growth(None, Some(&entry)), 0);
    assert_eq!(file_growth(Some(&entry), None), 0);
}

#[test]
fn exact_curve_classes_and_threshold_equalities() {
    let points = |f: &dyn Fn(u32) -> u64| {
        checkpoints(100)
            .into_iter()
            .map(|turn| Checkpoint {
                turn,
                allocated_bytes: f(turn),
            })
            .collect::<Vec<_>>()
    };
    for (f, expected) in [
        ((|_| 1_000_000) as fn(u32) -> u64, GrowthClass::Bounded),
        ((|t| 1_000_000 + 65536 * t as u64), GrowthClass::Linear),
        (
            (|t| 1_000_000 + 4096 * t as u64 * t as u64),
            GrowthClass::Superlinear,
        ),
    ] {
        assert_eq!(evaluate_curve(&points(&f), 100).expect("curve").0, expected);
    }
    let equal = vec![
        Checkpoint {
            turn: 0,
            allocated_bytes: 0,
        },
        Checkpoint {
            turn: 1,
            allocated_bytes: 100000,
        },
        Checkpoint {
            turn: 10,
            allocated_bytes: 100000,
        },
        Checkpoint {
            turn: 50,
            allocated_bytes: 100000,
        },
        Checkpoint {
            turn: 100,
            allocated_bytes: 165536,
        },
    ];
    // Global slope includes the late increase; equality is allowed by both rules.
    let (_, slope, d) = evaluate_curve(&equal, 100).expect("equality");
    assert_eq!(d.late_range_bytes, Some(65536));
    assert!(slope >= 0.0);
    // Strict shape inequality: equality at either floor remains linear.
    for early in [8192_u64, 32768] {
        let threshold = (early * 5 / 4).max(early + 4096);
        let make = |extra: u64| {
            points(&|t| {
                if t <= 50 {
                    1_000_000 + early * t as u64
                } else {
                    1_000_000 + early * 50 + (threshold + extra) * (t - 50) as u64
                }
            })
        };
        assert_eq!(
            evaluate_curve(&make(0), 100).expect("equal shape").0,
            GrowthClass::Linear
        );
        assert_eq!(
            evaluate_curve(&make(1), 100).expect("above shape").0,
            GrowthClass::Superlinear
        );
    }
    let mut exact_slope = [100000, 100000, 105830, 132059, 164845]
        .into_iter()
        .zip(checkpoints(100))
        .map(|(allocated_bytes, turn)| Checkpoint {
            turn,
            allocated_bytes,
        })
        .collect::<Vec<_>>();
    let (class, slope, _) = evaluate_curve(&exact_slope, 100).expect("exact slope allowance");
    assert_eq!(slope, 655.36);
    assert_eq!(class, GrowthClass::Bounded);
    let shifted = exact_slope
        .iter()
        .map(|p| Checkpoint {
            turn: p.turn,
            allocated_bytes: (1_u64 << 60) + p.allocated_bytes,
        })
        .collect::<Vec<_>>();
    let shifted_curve =
        evaluate_curve(&shifted, 100).expect("large baseline preserves byte deltas");
    assert_eq!(
        (shifted_curve.0, shifted_curve.1),
        (GrowthClass::Bounded, 655.36)
    );
    exact_slope[4].allocated_bytes += 1;
    assert_eq!(
        evaluate_curve(&exact_slope, 100)
            .expect("above slope allowance")
            .0,
        GrowthClass::Linear
    );
    let mut missing = points(&|t| t as u64);
    missing.remove(2);
    assert!(evaluate_curve(&missing, 100).is_err());
    assert_eq!(disk_class(65536.0), "64");
    assert_eq!(disk_class(65537.0), "256");
    assert_eq!(disk_class(4194304.0), "4096");
    assert_eq!(disk_class(4194305.0), "4096+");
    assert_eq!(median(vec![1.0, 2.0, 3.0, 4.0]), Some(2.5));
    assert_eq!(p95(vec![1.0, 2.0, 3.0]), Some(3.0));
}

#[cfg(unix)]
#[test]
fn sparse_files_and_aliases_use_blocks_and_no_follow() {
    use std::os::unix::fs::MetadataExt;
    let root = std::env::temp_dir().join(format!(
        "ahrb-storage-stat-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&root).expect("root");
    let file = std::fs::File::create(root.join("sparse")).expect("file");
    file.set_len(64 * 1024 * 1024).expect("sparse");
    let config = StorageConfig::default();
    let snap = inventory(&root, &config, true).expect("snapshot");
    assert_eq!(
        snap.allocated_bytes(),
        file.metadata().expect("stat").blocks() * 512
    );
    assert!(snap.allocated_bytes() < snap.apparent_bytes());
    std::fs::hard_link(root.join("sparse"), root.join("alias")).expect("hardlink");
    assert!(inventory(&root, &config, true).is_err());
    std::fs::remove_file(root.join("alias")).expect("remove alias");
    std::os::unix::fs::symlink(root.join("sparse"), root.join("symlink")).expect("symlink");
    assert!(inventory(&root, &config, true).is_err());
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn pinned_seed_contract_matches_economy_bytes() {
    fixture::validate_renderer_pins().expect("dialect response/read-call pins");
    let c = fixture::contents().expect("pins");
    assert_eq!(
        c.iter().map(|(_, c)| c.len()).collect::<Vec<_>>(),
        [768, 752, 768, 736, 752]
    );
    assert_eq!(
        fixture::prompt(1).expect("prompt").len(),
        fixture::prompt(1000).expect("prompt").len()
    );
}

#[cfg(target_os = "macos")]
#[test]
fn apfs_clones_count_each_reported_allocation() {
    use std::os::unix::ffi::OsStrExt;
    let root = std::env::temp_dir().join(format!(
        "ahrb-storage-clone-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&root).expect("root");
    let original = root.join("original");
    let clone = root.join("clone");
    std::fs::write(&original, vec![b'C'; 128 * 1024]).expect("original");
    let source = std::ffi::CString::new(original.as_os_str().as_bytes()).expect("source path");
    let destination = std::ffi::CString::new(clone.as_os_str().as_bytes()).expect("clone path");
    // This is a real APFS clone receipt; unsupported filesystems fail this check.
    let status = unsafe { libc::clonefile(source.as_ptr(), destination.as_ptr(), 0) };
    assert_eq!(status, 0, "clonefile: {}", std::io::Error::last_os_error());
    let snapshot = inventory(&root, &StorageConfig::default(), true).expect("clone inventory");
    assert_eq!(snapshot.regular_files(), 2);
    assert_ne!(
        snapshot.entries[0].inode_or_file_id,
        snapshot.entries[1].inode_or_file_id
    );
    assert_eq!(
        snapshot.allocated_bytes(),
        snapshot
            .entries
            .iter()
            .map(|e| e.allocated_bytes)
            .sum::<u64>()
    );
    assert_eq!(snapshot.entries[0].sha256, snapshot.entries[1].sha256);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn missing_retirement_never_becomes_a_low_write_class() {
    use ahrb::process::{ProcIdentity, ProcessDiskObservation, TreeDiskTracker};
    let identity = ProcIdentity {
        pid: 123,
        start_time: 45,
    };
    let mut tracker = TreeDiskTracker::default();
    tracker
        .observe(&ProcessDiskObservation {
            expected_identities: std::collections::BTreeSet::from([identity]),
            write_bytes_by_identity: BTreeMap::from([(identity, 4096)]),
            cgroup_write_bytes: None,
        })
        .expect("live sample");
    let after = tracker
        .observe(&ProcessDiskObservation::default())
        .expect("disappeared");
    assert!(!after.counter_complete);
    assert!(after.cumulative_write_bytes.is_none());
    assert!(tracker.retire_after_final_sample(identity).is_err());
}
