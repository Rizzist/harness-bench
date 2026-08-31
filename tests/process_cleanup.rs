use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn read_pid(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

fn pid_exists(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal zero only checks existence.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[test]
fn cleanup_reaps_child_and_grandchild_process_tree() {
    let root = std::env::temp_dir().join(format!("ahrb-cleanup-tree-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale cleanup fixture");
    }
    std::fs::create_dir(&root).expect("create cleanup fixture");
    let child_path = root.join("child.pid");
    let grandchild_path = root.join("grandchild.pid");
    let mut command = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"));
    command
        .process_group(0)
        .current_dir(&root)
        .args([
            "process-tree",
            "--child-pid",
            "child.pid",
            "--grandchild-pid",
            "grandchild.pid",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let root_child = command.spawn().expect("spawn owned process tree");
    let root_pid = root_child.id();
    ahrb::process::register_process(root_pid).expect("register process group");

    let deadline = Instant::now() + Duration::from_secs(5);
    let (child_pid, grandchild_pid) = loop {
        if let (Some(child), Some(grandchild)) = (read_pid(&child_path), read_pid(&grandchild_path))
        {
            break (child, grandchild);
        }
        assert!(
            Instant::now() < deadline,
            "fixture tree did not become ready"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    #[cfg(target_os = "macos")]
    let mut sampler = ahrb::process::macos::MacOsSampler::default();
    #[cfg(target_os = "linux")]
    let mut sampler = ahrb::process::linux::LinuxSampler::default();
    let tree = ahrb::process::Sampler::discover(&mut sampler, &[root_pid])
        .expect("discover complete owned tree");
    assert!(
        tree.members
            .keys()
            .any(|identity| identity.pid == child_pid)
    );
    assert!(
        tree.members
            .keys()
            .any(|identity| identity.pid == grandchild_pid)
    );

    let survivors = ahrb::process::cleanup_owned_processes(Duration::from_millis(100))
        .expect("cleanup owned process tree");
    assert!(survivors.is_empty(), "cleanup survivors: {survivors:?}");
    assert!(!pid_exists(root_pid));
    assert!(!pid_exists(child_pid));
    assert!(!pid_exists(grandchild_pid));
    drop(root_child);
    std::fs::remove_dir_all(root).expect("remove cleanup fixture");
}

fn signal_during_doctor_probe_reaps_probe_descendants(signal: i32, label: &str) {
    let root = std::env::temp_dir().join(format!("ahrb-probe-{label}-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale probe signal fixture");
    }
    std::fs::create_dir(&root).expect("create probe signal fixture");
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = Path::new(env!("CARGO_BIN_EXE_ahrb-fixture"));
    let source = std::fs::read_to_string(repository.join("adapters/mock/manifest.toml"))
        .expect("read mock manifest");
    let replacement = format!(
        "version_probe = [{:?}, \"process-tree\", \"--child-pid\", \"child.pid\", \"--grandchild-pid\", \"grandchild.pid\"]",
        fixture.to_string_lossy()
    );
    let manifest_text = source.replacen(
        "version_probe = [\"target/debug/ahrb-mock-harness\", \"--help\"]",
        &replacement,
        1,
    );
    let manifest = root.join("manifest.toml");
    std::fs::write(&manifest, manifest_text).expect("write probe signal manifest");
    let mut ahrb = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(&root)
        .arg("doctor")
        .arg("--manifest")
        .arg(&manifest)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn doctor with slow version probe");
    let child_path = root.join("child.pid");
    let grandchild_path = root.join("grandchild.pid");
    let deadline = Instant::now() + Duration::from_secs(5);
    let (child_pid, grandchild_pid) = loop {
        if let (Some(child), Some(grandchild)) = (read_pid(&child_path), read_pid(&grandchild_path))
        {
            break (child, grandchild);
        }
        assert!(
            Instant::now() < deadline,
            "version probe tree did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let ahrb_pid = i32::try_from(ahrb.id()).expect("ahrb PID fits pid_t");
    // SAFETY: the child PID is live and owned by this test.
    assert_eq!(unsafe { libc::kill(ahrb_pid, signal) }, 0);
    let status = ahrb.wait().expect("wait for signal-cleanup exit");
    assert_ne!(status.code(), Some(0));
    let gone_deadline = Instant::now() + Duration::from_secs(3);
    while (pid_exists(child_pid) || pid_exists(grandchild_pid)) && Instant::now() < gone_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!pid_exists(child_pid));
    assert!(!pid_exists(grandchild_pid));
    std::fs::remove_dir_all(root).expect("remove probe signal fixture");
}

#[test]
fn sigterm_during_doctor_probe_reaps_probe_descendants() {
    signal_during_doctor_probe_reaps_probe_descendants(libc::SIGTERM, "sigterm");
}

#[test]
fn sigabrt_during_doctor_probe_reaps_probe_descendants() {
    signal_during_doctor_probe_reaps_probe_descendants(libc::SIGABRT, "sigabrt");
}

#[test]
fn normal_doctor_exit_reaps_probe_descendants() {
    let root = std::env::temp_dir().join(format!("ahrb-probe-normal-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale normal probe fixture");
    }
    std::fs::create_dir(&root).expect("create normal probe fixture");
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = Path::new(env!("CARGO_BIN_EXE_ahrb-fixture"));
    let source = std::fs::read_to_string(repository.join("adapters/mock/manifest.toml"))
        .expect("read mock manifest");
    let replacement = format!(
        "version_probe = [{:?}, \"process-tree-launcher\", \"--child-pid\", \"child.pid\", \"--grandchild-pid\", \"grandchild.pid\"]",
        fixture.to_string_lossy()
    );
    let manifest_text = source.replacen(
        "version_probe = [\"target/debug/ahrb-mock-harness\", \"--help\"]",
        &replacement,
        1,
    );
    let manifest = root.join("manifest.toml");
    std::fs::write(&manifest, manifest_text).expect("write normal probe manifest");
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(&root)
        .arg("doctor")
        .arg("--manifest")
        .arg(&manifest)
        .output()
        .expect("run doctor with exiting launcher probe");
    assert!(
        result.status.code().is_some(),
        "doctor did not complete a normal exit: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    for path in [root.join("child.pid"), root.join("grandchild.pid")] {
        if let Some(pid) = read_pid(&path) {
            assert!(!pid_exists(pid), "normal doctor left PID {pid} alive");
        }
    }
    std::fs::remove_dir_all(root).expect("remove normal probe fixture");
}
