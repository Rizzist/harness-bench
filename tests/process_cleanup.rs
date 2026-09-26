use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

static PROCESS_REGISTRY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn lock_process_registry_test() -> std::sync::MutexGuard<'static, ()> {
    PROCESS_REGISTRY_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock process-registry integration test")
}

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
    let _serial = lock_process_registry_test();
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

#[test]
fn profile_owned_reparented_survivor_is_recorded_reaped_and_lock_audited() {
    let sequence = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let _serial = lock_process_registry_test();
    let root = std::env::temp_dir().join(format!(
        "ahrb-profile-owned-{}-{sequence}",
        std::process::id(),
    ));
    assert!(!root.exists(), "profile-owned fixture root must be fresh");
    let profile = root.join("dr57/sigint2-r1");
    std::fs::create_dir_all(&profile).expect("create disposable profile");
    let pid_file = root.join("daemon.pid");
    let lock_file = profile.join("home/.haider/dev-profile/lock");
    let launcher = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"))
        .args([
            "profile-daemon-launcher",
            "--profile",
            profile.to_str().expect("UTF-8 profile"),
            "--pid-file",
            pid_file.to_str().expect("UTF-8 pid file"),
            "--lock-file",
            lock_file.to_str().expect("UTF-8 lock file"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exiting daemon launcher");
    let launcher_pid = launcher.id();
    assert!(
        launcher
            .wait_with_output()
            .expect("wait for daemon launcher")
            .status
            .success()
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let (daemon_pid, observed) = loop {
        if let Some(pid) = read_pid(&pid_file) {
            let found = ahrb::process::discover_profile_owned_processes(
                &root,
                &["ahrb-fixture".to_owned()],
            )
            .expect("discover profile-owned daemon");
            if let Some(process) = found.iter().find(|process| process.identity.pid == pid) {
                break (pid, process.clone());
            }
        }
        assert!(
            Instant::now() < deadline,
            "profile daemon did not become visible"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_ne!(observed.ppid, launcher_pid, "daemon was not reparented");
    assert_eq!(
        observed.ownership,
        ahrb::process::ProcOwnership::ProfilePath
    );

    let held =
        ahrb::process::audit_profile_lock(&profile, &lock_file).expect("audit held profile lock");
    assert!(matches!(
        held,
        ahrb::process::ProfileLockAudit::Held { holder_pid, .. } if holder_pid == Some(daemon_pid)
    ));

    ahrb::process::track_profile_owned_processes(std::slice::from_ref(&observed))
        .expect("register profile-owned daemon");
    let cleanup = ahrb::process::cleanup_owned_processes_with_evidence(Duration::from_millis(100))
        .expect("reap profile-owned daemon");
    assert!(
        cleanup
            .observed
            .iter()
            .any(|process| process.identity == observed.identity),
        "pre-reap observation omitted the detached daemon"
    );
    assert!(cleanup.reaped.contains(&observed.identity));
    assert!(cleanup.survivors.is_empty());
    assert!(!pid_exists(daemon_pid));

    let unlocked = ahrb::process::audit_profile_lock(&profile, &lock_file)
        .expect("audit released profile lock");
    assert!(matches!(
        unlocked,
        ahrb::process::ProfileLockAudit::Unlocked { .. }
    ));
    std::fs::remove_dir_all(root).expect("remove profile-owned fixture");
}

fn fresh_fixture_root(label: &str) -> std::path::PathBuf {
    let sequence = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let root =
        std::env::temp_dir().join(format!("ahrb-{label}-{}-{sequence}", std::process::id(),));
    assert!(!root.exists(), "fixture root must be fresh");
    root
}

/// Launch a detached fixture daemon that `flock(2)`s `lock_file`, naming the
/// profile by its *resolved* path (as Haider does), and wait for its PID.
fn spawn_flock_daemon(profile_arg: &Path, pid_file: &Path, lock_file: &Path) -> u32 {
    let launcher = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"))
        .args([
            "profile-daemon-launcher",
            "--profile",
            profile_arg.to_str().expect("UTF-8 profile"),
            "--pid-file",
            pid_file.to_str().expect("UTF-8 pid file"),
            "--lock-file",
            lock_file.to_str().expect("UTF-8 lock file"),
            "--lock-kind",
            "flock",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run exiting daemon launcher");
    assert!(launcher.success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(pid) = read_pid(pid_file)
            && matches!(
                ahrb::process::audit_profile_lock(Path::new("/"), lock_file),
                Ok(ahrb::process::ProfileLockAudit::Held { .. })
            )
        {
            return pid;
        }
        assert!(Instant::now() < deadline, "flock daemon did not start");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Row-57 regression on Haider 0.0.970/0.0.971: the lock is a `flock(2)` lock
/// (macOS `F_GETLK` reports `l_pid = -1`) and the daemon names the resolved
/// `/private/...` spelling of AHRB's profile. Teardown must still observe the
/// daemon, attribute and reap it, re-audit the lock, and finish `PASS`.
#[cfg(target_os = "macos")]
#[test]
fn teardown_reaps_unknown_owner_flock_holder_and_reaudits() {
    let _serial = lock_process_registry_test();
    let root = fresh_fixture_root("unknown-owner-lock");
    let profile = root.join("dr57/sigint2-r1");
    std::fs::create_dir_all(profile.join("home/.haider/dev-profile")).expect("create profile");
    let resolved_profile = std::fs::canonicalize(&profile).expect("resolve profile");
    assert_ne!(
        resolved_profile, profile,
        "temp dir must be a symlinked alias"
    );
    let lock_file = profile.join("home/.haider/dev-profile/lock");
    let daemon_pid = spawn_flock_daemon(
        &resolved_profile,
        &root.join("daemon.pid"),
        &resolved_profile.join("home/.haider/dev-profile/lock"),
    );

    // The kernel cannot name a flock owner: still held, owner unknown.
    let held = ahrb::process::audit_profile_lock(&profile, &lock_file).expect("audit");
    assert!(matches!(
        held,
        ahrb::process::ProfileLockAudit::Held {
            holder_pid: None,
            ..
        }
    ));
    // The open-file scan identifies the holder instead.
    let openers = ahrb::process::lock_file_openers(&lock_file).expect("scan lock openers");
    assert!(
        openers
            .iter()
            .any(|process| process.identity.pid == daemon_pid)
    );
    // Discovery under AHRB's lexical root matches the resolved argv spelling.
    let discovered =
        ahrb::process::discover_profile_owned_processes(&root, &["ahrb-fixture".to_owned()])
            .expect("discover profile-owned daemon");
    assert!(
        discovered
            .iter()
            .any(|process| process.identity.pid == daemon_pid)
    );

    let teardown = ahrb::process::teardown_owned_processes(
        &root,
        &["ahrb-fixture".to_owned()],
        &[(profile.clone(), lock_file.clone())],
    );
    assert_eq!(teardown.status, "PASS", "errors: {:?}", teardown.errors);
    assert!(
        teardown
            .observed_owned_processes
            .iter()
            .any(|process| process.identity.pid == daemon_pid
                && process.ownership == ahrb::process::ProcOwnership::ProfilePath),
        "daemon must be recorded as observed before reaping"
    );
    assert!(
        teardown
            .reaped_processes
            .iter()
            .any(|identity| identity.pid == daemon_pid)
    );
    assert!(teardown.surviving_processes.is_empty());
    assert!(matches!(
        teardown.profile_locks.as_slice(),
        [ahrb::process::ProfileLockAudit::Unlocked { .. }]
    ));
    assert!(!pid_exists(daemon_pid));
    std::fs::remove_dir_all(root).expect("remove fixture root");
}

/// A declared-executable holder whose argv never names the profile (so profile
/// discovery cannot see it) is identified through the unknown-owner lock,
/// recorded, reaped in a bounded lock round, and the lock is re-audited.
#[cfg(target_os = "macos")]
#[test]
fn teardown_attributes_and_reaps_lock_holder_invisible_to_argv_discovery() {
    let _serial = lock_process_registry_test();
    let root = fresh_fixture_root("argv-invisible-holder");
    let outside = fresh_fixture_root("argv-invisible-outside");
    let profile = root.join("dr57/sigint2-r1");
    std::fs::create_dir_all(&profile).expect("create profile");
    std::fs::create_dir_all(&outside).expect("create outside directory");
    let lock_file = profile.join("lock");
    std::fs::write(&lock_file, b"").expect("create lock file");
    let lock_alias = outside.join("lock-alias");
    std::os::unix::fs::symlink(&lock_file, &lock_alias).expect("link lock alias");
    let holder_pid = spawn_flock_daemon(&outside, &outside.join("holder.pid"), &lock_alias);
    assert!(
        ahrb::process::discover_profile_owned_processes(&root, &["ahrb-fixture".to_owned()])
            .expect("discover")
            .is_empty(),
        "fixture must be invisible to argv discovery"
    );

    let teardown = ahrb::process::teardown_owned_processes(
        &root,
        &["ahrb-fixture".to_owned()],
        &[(profile.clone(), lock_file.clone())],
    );
    let alive = pid_exists(holder_pid);
    if alive {
        // SAFETY: the PID was written by this test's own fixture.
        unsafe {
            libc::kill(holder_pid as i32, libc::SIGKILL);
        }
    }
    assert_eq!(teardown.status, "PASS", "errors: {:?}", teardown.errors);
    assert!(!alive);
    let [evidence] = teardown.lock_holders.as_slice() else {
        panic!(
            "expected one lock-holder round: {:?}",
            teardown.lock_holders
        );
    };
    assert_eq!(evidence.round, 1);
    assert_eq!(evidence.kernel_owner_pid, None);
    assert_eq!(evidence.identified_by, "open-file-scan");
    assert!(evidence.unowned_holders.is_empty());
    assert!(
        evidence
            .holders
            .iter()
            .any(|holder| holder.identity.pid == holder_pid
                && holder.ownership == ahrb::process::ProcOwnership::LockHolder)
    );
    assert!(
        teardown
            .observed_owned_processes
            .iter()
            .any(|process| process.identity.pid == holder_pid)
    );
    assert!(
        teardown
            .reaped_processes
            .iter()
            .any(|identity| identity.pid == holder_pid)
    );
    assert!(matches!(
        teardown.profile_locks.as_slice(),
        [ahrb::process::ProfileLockAudit::Unlocked { .. }]
    ));
    std::fs::remove_dir_all(root).expect("remove fixture root");
    std::fs::remove_dir_all(outside).expect("remove outside directory");
}

/// When the holder of a still-held profile lock is not attributable to the
/// adapter, teardown never signals it, yet it still returns a complete `ERROR`
/// record: the held lock, the identified holder, and the observed evidence.
#[cfg(target_os = "macos")]
#[test]
fn teardown_lock_held_after_bounded_reaping_is_error_with_evidence() {
    let _serial = lock_process_registry_test();
    let root = fresh_fixture_root("held-lock-error");
    let profile = root.join("dr57/sigint2-r1");
    std::fs::create_dir_all(&profile).expect("create profile");
    let lock_file = profile.join("lock");
    let holder_pid = spawn_flock_daemon(&profile, &root.join("holder.pid"), &lock_file);

    // Declared executables do not include the fixture: it is not AHRB-owned.
    let teardown = ahrb::process::teardown_owned_processes(
        &root,
        &["haiderd".to_owned()],
        &[(profile.clone(), lock_file.clone())],
    );
    let alive = pid_exists(holder_pid);
    // Stop exactly the holder this test started before asserting.
    // SAFETY: the PID was written by this test's own fixture.
    unsafe {
        libc::kill(holder_pid as i32, libc::SIGKILL);
    }
    assert!(alive, "an unowned holder must never be signalled");
    assert_eq!(teardown.status, "ERROR");
    assert!(
        teardown.errors.iter().any(
            |error| error.contains("remained held after bounded reaping")
                && error.contains("kernel owner PID unknown")
        ),
        "errors: {:?}",
        teardown.errors
    );
    assert!(matches!(
        teardown.profile_locks.as_slice(),
        [ahrb::process::ProfileLockAudit::Held {
            holder_pid: None,
            ..
        }]
    ));
    assert!(!teardown.lock_holders.is_empty());
    assert!(teardown.lock_holders.iter().all(|evidence| {
        evidence.identified_by == "open-file-scan"
            && evidence
                .unowned_holders
                .iter()
                .any(|identity| identity.pid == holder_pid)
            && evidence
                .holders
                .iter()
                .any(|holder| holder.identity.pid == holder_pid)
    }));
    assert!(
        teardown
            .reaped_processes
            .iter()
            .all(|identity| identity.pid != holder_pid)
    );
    std::fs::remove_dir_all(root).expect("remove fixture root");
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
