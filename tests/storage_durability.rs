use ahrb::storage::{durability::*, evidence::*};
use std::path::PathBuf;
fn fresh(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ahrb-durability-{name}-{}-{}",
        std::process::id(),
        ahrb::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
#[test]
fn newline_only_record_file_returns_typed_error() {
    let root = fresh("empty-records");
    let file = root.join("records.jsonl");
    for bytes in [b"\n".as_slice(), b"\n\n", b""] {
        std::fs::write(&file, bytes).unwrap();
        for complete in [false, true] {
            let error = read_trace(&root, 1, complete).unwrap_err();
            assert!(matches!(&error, ahrb::AhrbError::Protocol(reason)
                if reason == "durability trace loss: empty or truncated image stream"));
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn independent_control_covers_success_failure_and_rejects_loss() {
    let p = fresh("control");
    let probe = probe(
        &p,
        std::path::Path::new(env!("CARGO_BIN_EXE_ahrb-fixture")),
        &["--help".into()],
    )
    .await
    .unwrap();
    assert!(
        probe.instrumentation.reason.is_none(),
        "{:?}",
        probe.instrumentation.reason
    );
    let data = probe
        .events
        .iter()
        .any(|e| e.primitive == Primitive::Fdatasync);
    assert_eq!(probe.events.len(), if data { 11 } else { 9 });
    assert!(probe.events.iter().all(|e| e.self_test));
    assert_eq!(
        probe
            .events
            .iter()
            .filter(|e| e.return_code == -1 && e.errno == libc::EBADF as i64)
            .count(),
        if data { 5 } else { 4 }
    );
    assert!(
        probe
            .instrumentation
            .images
            .iter()
            .all(|i| i.load_verified && i.self_test_verified && i.exit_seen)
    );
    let trace = p.join("durability-support/preflight");
    let captured = read_trace(&trace, 1, true).unwrap();
    let available = applicability(&captured);
    assert!(matches!(
        available[&Primitive::Fsync],
        Applicability::Measured
    ));
    assert!(matches!(
        (data, &available[&Primitive::Fdatasync]),
        (true, Applicability::Measured) | (false, Applicability::NotApplicable)
    ));
    // A failed/missing capture has unknown applicability, even though its
    // aggregate counts are also null like an OS-inapplicable primitive's count.
    assert!(applicability(&Capture::default()).is_empty());
    let file = std::fs::read_dir(&trace)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .unwrap();
    let original = std::fs::read_to_string(&file).unwrap();
    for corrupted in [
        original
            .lines()
            .filter(|l| !l.contains("\"sequence\":3,"))
            .map(|l| format!("{l}\n"))
            .collect::<String>(),
        original
            .lines()
            .filter(|l| !l.contains("\"kind\":\"end\""))
            .map(|l| format!("{l}\n"))
            .collect::<String>(),
        original.replace("\"drop_count\":0", "\"drop_count\":1"),
        original.trim_end().to_owned(),
    ] {
        std::fs::write(&file, &corrupted).unwrap();
        assert!(read_trace(&trace, 1, true).is_err());
        if corrupted.contains("\"drop_count\":1") {
            assert_eq!(observed_drop_count(&trace).unwrap(), 1);
        }
    }
    // A PID alone cannot certify a new spawn: this image was already loaded
    // before the recorded spawn began. Reusing it would hide a missing child.
    let mut reused = original
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mut spawn = reused.last().unwrap().clone();
    spawn["kind"] = "spawn".into();
    spawn["child_pid"] = reused[0]["pid"].clone();
    let later = probe
        .events
        .iter()
        .map(|event| event.exit_ns)
        .max()
        .unwrap()
        + 1;
    spawn["enter_ns"] = later.into();
    spawn["exit_ns"] = later.into();
    let end = reused.pop().unwrap();
    reused.push(spawn);
    reused.push(end);
    let reused = reused
        .into_iter()
        .enumerate()
        .map(|(index, mut value)| {
            value["sequence"] = (index + 1).into();
            format!("{}\n", serde_json::to_string(&value).unwrap())
        })
        .collect::<String>();
    let parent = trace.join("parent.jsonl");
    let parent_stream = reused
        .lines()
        .map(|line| {
            let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
            value["pid"] = (value["pid"].as_u64().unwrap() + 1).into();
            value["start_time"] = (value["start_time"].as_u64().unwrap() + 1).into();
            format!("{}\n", serde_json::to_string(&value).unwrap())
        })
        .collect::<String>();
    std::fs::write(&file, &original).unwrap();
    std::fs::write(&parent, parent_stream).unwrap();
    std::fs::copy(
        file.with_extension("jsonl.image"),
        parent.with_extension("jsonl.image"),
    )
    .unwrap();
    assert!(read_trace(&trace, 1, true).is_err());
    std::fs::remove_file(&parent).unwrap();
    std::fs::remove_file(parent.with_extension("jsonl.image")).unwrap();
    // A previous image cannot certify a later image's unfinished exec.
    let shift = probe.events.iter().map(|e| e.exit_ns).max().unwrap()
        - probe.events.iter().map(|e| e.enter_ns).min().unwrap()
        + 1;
    let mut successor = String::new();
    for line in original.lines() {
        let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
        for field in ["enter_ns", "exit_ns"] {
            if let Some(timestamp) = value[field].as_u64() {
                value[field] = (timestamp + shift).into();
            }
        }
        successor.push_str(&serde_json::to_string(&value).unwrap());
        successor.push('\n');
    }
    let second = trace.join("successor.jsonl");
    std::fs::copy(
        file.with_extension("jsonl.image"),
        second.with_extension("jsonl.image"),
    )
    .unwrap();
    let exec = |raw: &str| raw.replace("\"kind\":\"end\"", "\"kind\":\"exec\"");
    std::fs::write(&file, exec(&original)).unwrap();
    std::fs::write(&second, &successor).unwrap();
    assert!(read_trace(&trace, 1, true).is_ok());
    std::fs::write(&second, exec(&successor)).unwrap();
    assert!(read_trace(&trace, 1, true).is_err());
    std::fs::remove_dir_all(p).unwrap();
}
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn protected_binary_refusal_is_observed_not_zero() {
    let p = fresh("refusal");
    let probe = probe(&p, std::path::Path::new("/usr/bin/true"), &[])
        .await
        .unwrap();
    assert!(
        probe
            .instrumentation
            .reason
            .as_deref()
            .unwrap()
            .starts_with("os-limited:")
    );
    assert!(
        probe
            .instrumentation
            .images
            .iter()
            .all(|i| !i.load_verified && !i.self_test_verified)
    );
    assert!(probe.events.is_empty());
    let launch: serde_json::Value = serde_json::from_slice(
        &std::fs::read(p.join("durability-support/preflight/launch.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(launch["executable"], "/usr/bin/true");
    assert_eq!(launch["version_args"], serde_json::json!([]));
    assert!(launch["spawned_pid"].as_u64().unwrap() > 0);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn estimates_exclude_controls_failures_and_inapplicable_primitives() {
    let event = |primitive, turn, return_code, self_test| FsyncEvent {
        repetition: 1,
        turn,
        pid: 1,
        start_time: 1,
        sequence: 1,
        primitive,
        enter_ns: 1,
        exit_ns: 2,
        return_code,
        errno: if return_code < 0 { 9 } else { 0 },
        backend: BACKEND.into(),
        self_test,
    };
    let events = vec![
        event(Primitive::Fsync, 1, 0, false),
        event(Primitive::Fullfsync, 2, 0, false),
        event(Primitive::Fsync, 2, -1, false),
        event(Primitive::Fsync, 0, 0, true),
        event(Primitive::Fsync, 0, 0, false),
    ];
    let (s, failed) = summarize(&events, 2).unwrap();
    assert_eq!(s.fsync_calls_per_turn, Some(0.5));
    assert_eq!(s.fullfsync_calls_per_turn, Some(0.5));
    assert_eq!(s.fdatasync_calls_per_turn, None);
    assert_eq!(s.durability_calls_per_turn, Some(1.0));
    assert_eq!(s.estimated_durability_wall_ms_per_turn, Some(4.0));
    assert_eq!(failed, 1);
    for (v, expected) in [
        (0.0, "0"),
        (0.01, "1"),
        (1.0, "1"),
        (1.01, "10"),
        (10.0, "10"),
        (10.01, "10+"),
    ] {
        assert_eq!(class(v), expected);
    }
    assert!(summarize(&events, 0).is_err());
    assert!(summarize(&[event(Primitive::Fdatasync, 1, 0, false)], 1).is_err());
}

#[test]
fn applicable_sum_preserves_the_exact_one_call_band() {
    let mut events = Vec::new();
    for (primitive, count) in [
        (Primitive::Fsync, 33),
        (Primitive::Fullfsync, 56),
        (Primitive::Fdatasync, 11),
    ] {
        for _ in 0..count {
            events.push(FsyncEvent {
                repetition: 1,
                turn: 1,
                pid: 1,
                start_time: 1,
                sequence: events.len() as u64 + 1,
                primitive,
                enter_ns: 1,
                exit_ns: 2,
                return_code: 0,
                errno: 0,
                backend: BACKEND.into(),
                self_test: false,
            });
        }
    }
    let mut control = events.last().unwrap().clone();
    control.self_test = true;
    control.turn = 0;
    events.push(control);
    let (summary, failed) = summarize(&events, 100).unwrap();
    assert_eq!(summary.durability_calls_per_turn, Some(1.0));
    assert_eq!(summary.durability_class.as_deref(), Some("1"));
    assert_eq!(summary.estimated_durability_wall_ms_per_turn, Some(4.0));
    assert_eq!(failed, 0);
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
async fn launch_spawn_fixture(
    root: &std::path::Path,
    executable: &std::path::Path,
    collector: &Collector,
    args: &[&str],
) {
    let launched = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::process::Command::new(executable)
            .args(args)
            .current_dir(root)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", root)
            .envs(collector.environment())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        launched.status.success(),
        "{}",
        String::from_utf8_lossy(&launched.stderr)
    );
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_spawn_entry_points_produce_distinct_child_receipts() {
    let root = fresh("spawn");
    let executable = root.join("spawn-fixture");
    let compiled = std::process::Command::new("cc")
        .args(["-Wall", "-Wextra", "-Werror", "-O2"])
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/storage_spawn.c"
        ))
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let collector = Collector::new(&root, "spawn").unwrap();
    launch_spawn_fixture(&root, &executable, &collector, &[]).await;
    let captured = read_trace(&collector.trace, 1, true).unwrap();
    assert!(captured.gaps.is_empty());
    assert_eq!(captured.images.len(), 5);
    assert_eq!(captured.spawned.len(), 4);
    assert!(
        captured
            .images
            .iter()
            .all(|image| image.exit_seen && image.self_test_verified)
    );
    let measured = captured
        .events
        .iter()
        .filter(|event| !event.self_test)
        .collect::<Vec<_>>();
    assert_eq!(measured.len(), 4);
    assert!(
        measured
            .iter()
            .all(|event| event.primitive == Primitive::Fsync && event.return_code == 0)
    );
    // A NULL-PID spawn must remain an obligation when the child drops DYLD.
    let stripped = Collector::new(&root, "spawn-stripped").unwrap();
    launch_spawn_fixture(&root, &executable, &stripped, &["strip"]).await;
    let error = read_trace(&stripped.trace, 1, true).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("spawned process lacks a distinct subsequent image control")
    );
    // vfork is another legal child path; stripped injection cannot certify it.
    let forked = Collector::new(&root, "vfork-stripped").unwrap();
    launch_spawn_fixture(&root, &executable, &forked, &["vfork-strip"]).await;
    let capture = read_trace(&forked.trace, 1, true);
    assert!(capture.is_err() || !capture.unwrap().gaps.is_empty());
    std::fs::remove_dir_all(root).unwrap();
}
