use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

static PROCESS_REGISTRY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(target_os = "macos")]
fn local_ipv4_addresses() -> Vec<std::net::Ipv4Addr> {
    let mut head = std::ptr::null_mut::<libc::ifaddrs>();
    // SAFETY: `head` is a valid output pointer and the returned list is freed
    // exactly once below after all entries have been copied.
    assert_eq!(unsafe { libc::getifaddrs(&mut head) }, 0);
    let mut addresses = Vec::new();
    let mut current = head;
    while !current.is_null() {
        // SAFETY: `current` walks the live list and the address family is
        // checked before interpreting an AF_INET entry.
        let entry = unsafe { &*current };
        if !entry.ifa_addr.is_null()
            && unsafe { (*entry.ifa_addr).sa_family as i32 } == libc::AF_INET
        {
            // SAFETY: the family check establishes the sockaddr_in layout.
            let address = unsafe { &*(entry.ifa_addr.cast::<libc::sockaddr_in>()) };
            addresses.push(std::net::Ipv4Addr::from(u32::from_be(
                address.sin_addr.s_addr,
            )));
        }
        current = entry.ifa_next;
    }
    // SAFETY: `head` came from the successful `getifaddrs` call above.
    unsafe { libc::freeifaddrs(head) };
    addresses.sort_unstable();
    addresses.dedup();
    addresses
}

#[cfg(target_os = "macos")]
fn exclusive_listener(address: std::net::Ipv4Addr, port: u16) -> tokio::net::TcpListener {
    let socket = tokio::net::TcpSocket::new_v4().expect("create IPv4 provider socket");
    socket
        .set_reuseaddr(false)
        .expect("disable provider SO_REUSEADDR");
    socket
        .set_reuseport(false)
        .expect("disable provider SO_REUSEPORT");
    socket
        .bind((address, port).into())
        .expect("bind exclusive provider-port listener");
    socket.listen(1024).expect("listen on provider port")
}

/// Exclusive same-port sentinels on every non-loopback local IPv4 address
/// which record each accepted connection, standing in for the provider's
/// row-62 sentinels as the guard's delivery oracle.
#[cfg(target_os = "macos")]
struct TestSentinels {
    arrivals: std::sync::Arc<Mutex<Vec<(String, String)>>>,
    claimed: Mutex<Vec<(String, String)>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

#[cfg(target_os = "macos")]
impl TestSentinels {
    fn start(addresses: &[std::net::Ipv4Addr], port: u16) -> Self {
        let arrivals = std::sync::Arc::new(Mutex::new(Vec::new()));
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut threads = Vec::new();
        for address in addresses
            .iter()
            .copied()
            .filter(|address| !address.is_loopback())
        {
            let listener = exclusive_listener(address, port)
                .into_std()
                .expect("detach sentinel listener");
            listener
                .set_nonblocking(true)
                .expect("nonblocking sentinel listener");
            let arrivals = std::sync::Arc::clone(&arrivals);
            let stop = std::sync::Arc::clone(&stop);
            threads.push(std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, peer)) => {
                            let destination = stream.local_addr().expect("sentinel destination");
                            arrivals
                                .lock()
                                .expect("record sentinel arrival")
                                .push((destination.to_string(), peer.to_string()));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("sentinel accept failed: {error}"),
                    }
                }
            }));
        }
        Self {
            arrivals,
            claimed: Mutex::new(Vec::new()),
            stop,
            threads,
        }
    }

    fn claimed(&self) -> Vec<(String, String)> {
        self.claimed.lock().expect("claimed arrivals").clone()
    }

    fn unclaimed(&self) -> Vec<(String, String)> {
        self.arrivals.lock().expect("sentinel arrivals").clone()
    }
}

#[cfg(target_os = "macos")]
impl ahrb::offline_guard::LocalDeliveryOracle for TestSentinels {
    fn claim_setup_arrival(
        &self,
        destination: std::net::SocketAddrV4,
        peer: std::net::SocketAddrV4,
    ) -> bool {
        let key = (destination.to_string(), peer.to_string());
        let mut arrivals = self.arrivals.lock().expect("sentinel arrivals");
        let Some(index) = arrivals.iter().position(|arrival| arrival == &key) else {
            return false;
        };
        arrivals.remove(index);
        self.claimed.lock().expect("claimed arrivals").push(key);
        true
    }
}

#[cfg(target_os = "macos")]
impl Drop for TestSentinels {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Unconfined control: does a TCP connect to `address` reach this test's own
/// listener on that address (matched by the connecting socket's source)?
#[cfg(target_os = "macos")]
fn unconfined_local_delivery(address: std::net::Ipv4Addr) -> bool {
    let listener = std::net::TcpListener::bind((address, 0)).expect("bind unconfined listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking unconfined listener");
    let target = listener.local_addr().expect("unconfined listener address");
    let Ok(stream) = std::net::TcpStream::connect_timeout(&target, Duration::from_millis(1_000))
    else {
        return false;
    };
    let source = stream.local_addr().expect("unconfined source address");
    let deadline = Instant::now() + Duration::from_millis(1_000);
    while Instant::now() < deadline {
        match listener.accept() {
            Ok((_, peer)) if peer == source => return true,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_) => return false,
        }
    }
    false
}

#[test]
fn guarded_launcher_rejects_profile_hash_mismatch_before_exec() {
    let root = fresh_fixture_root("guard-hash-mismatch");
    std::fs::create_dir(&root).expect("create guard fixture root");
    let profile = root.join("offline.sb");
    std::fs::write(&profile, b"(version 1)\n(allow default)\n")
        .expect("write guard profile fixture");
    let output = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"))
        .args([
            "guarded-launch",
            "--profile",
            profile.to_str().expect("UTF-8 profile path"),
            "--profile-sha256",
            &"0".repeat(64),
            "--launcher",
            "/usr/bin/true",
            "--",
            "/usr/bin/true",
        ])
        .output()
        .expect("run guarded-launch mismatch control");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("offline guard profile hash mismatch before launch")
    );
    std::fs::remove_dir_all(root).expect("remove guard fixture root");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_guard_owns_every_permitted_local_ipv4_destination() {
    let root = fresh_fixture_root("owned-provider-port-guard");
    std::fs::create_dir(&root).expect("create owned-provider fixture root");
    let provider = exclusive_listener(std::net::Ipv4Addr::LOCALHOST, 0);
    let bind_address = provider.local_addr().expect("provider bind address");
    let addresses = local_ipv4_addresses();
    let sentinels = TestSentinels::start(&addresses, bind_address.port());
    let destination = format!("127.0.0.1:{}", bind_address.port());
    let mut guard = ahrb::offline_guard::OfflineGuard::new(
        &root,
        &format!("http://{destination}"),
        &bind_address.to_string(),
        &sentinels,
    )
    .expect("build and prove owned-port Seatbelt guard");
    guard.finish_trial_address_monitor();
    let evidence = guard.evidence();

    // Setup delivery proof: one probe per non-loopback address. An address
    // whose unconfined same-host connect reaches this test's own listener
    // must be credited to the sentinel; one answered elsewhere (for example
    // a VPN tunnel interface) must fail closed, naming it.
    let non_loopback = addresses
        .iter()
        .copied()
        .filter(|address| !address.is_loopback())
        .collect::<Vec<_>>();
    assert_eq!(evidence.local_delivery_probes.len(), non_loopback.len());
    let mut undelivered = Vec::new();
    for address in &non_loopback {
        let probe = evidence
            .local_delivery_probes
            .iter()
            .find(|probe| probe.address == address.to_string())
            .unwrap_or_else(|| panic!("no delivery probe for {address}"));
        assert_eq!(
            probe.destination,
            format!("{address}:{}", bind_address.port())
        );
        if unconfined_local_delivery(*address) {
            assert_eq!(
                probe.outcome,
                ahrb::offline_guard::LOCAL_DELIVERY_OWNED,
                "{address}: {probe:?}"
            );
            assert!(probe.sentinel_recorded);
        } else {
            assert_ne!(
                probe.outcome,
                ahrb::offline_guard::LOCAL_DELIVERY_OWNED,
                "{address}: {probe:?}"
            );
            assert!(!probe.sentinel_recorded);
            undelivered.push(*address);
        }
    }
    let credited = evidence
        .local_delivery_probes
        .iter()
        .filter(|probe| probe.sentinel_recorded)
        .map(|probe| {
            (
                probe.destination.clone(),
                probe.probe_local_address.clone().expect("credited source"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(sentinels.claimed(), credited);
    assert!(
        sentinels.unclaimed().is_empty(),
        "{:?}",
        sentinels.unclaimed()
    );
    if undelivered.is_empty() {
        assert!(evidence.local_delivery_proven);
        assert!(guard.local_delivery_failure().is_none());
    } else {
        assert!(!evidence.local_delivery_proven);
        let failure = guard.local_delivery_failure().expect("delivery failure");
        assert!(failure.starts_with("guard boundary includes a local address AHRB does not own: "));
        for address in &undelivered {
            assert!(failure.contains(&format!("{address} (")), "{failure}");
        }
        let manifest = ahrb::manifest::load(Path::new("adapters/opencode/manifest.toml"))
            .expect("load a real adapter manifest");
        let refused = guard
            .apply_to_manifest(&manifest)
            .expect_err("an unproven guard must refuse to wrap a harness");
        assert!(refused.to_string().contains(failure), "{refused}");
        eprintln!("host delivery proof failed as required: {failure}");
    }
    assert_eq!(evidence.provider_destination, destination);
    assert_eq!(evidence.provider_bind_address, bind_address.to_string());
    assert!(evidence.provider_port_owned);
    assert!(
        evidence
            .owned_ipv4_addresses
            .iter()
            .any(|address| address == "127.0.0.1")
    );
    assert_eq!(
        evidence.provider_rule,
        format!(
            "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:{}\")))",
            bind_address.port()
        )
    );
    assert!(evidence.udp_probes_blocked);
    assert_eq!(
        evidence.udp_probe_destinations.len(),
        evidence.owned_ipv4_addresses.len()
    );
    assert_eq!(evidence.local_ipv4_monitor.sample_interval_ms, 500);
    assert!(evidence.local_ipv4_monitor.samples_completed >= 2);
    assert!(!evidence.local_ipv4_monitor.change_detected);
    assert!(evidence.local_ipv4_monitor.error.is_none());
    assert_eq!(
        evidence.alternate_loopback_destination,
        format!("[::1]:{}", bind_address.port())
    );
    assert!(evidence.provider_probe_allowed);
    assert_eq!(
        evidence.alternate_ipv4_destination,
        format!("127.0.0.2:{}", bind_address.port())
    );
    assert!(evidence.alternate_ipv4_probe_blocked);
    assert!(evidence.alternate_loopback_probe_blocked);
    for address in &evidence.owned_ipv4_addresses {
        let rebound = std::net::TcpListener::bind(format!("{address}:{}", bind_address.port()));
        assert!(
            rebound
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::AddrInUse),
            "provider/sentinel set did not exclusively own {address}:{}: {rebound:?}",
            bind_address.port()
        );
    }
    drop(guard);
    drop(sentinels);
    drop(provider);
    std::fs::remove_dir_all(root).expect("remove exact-provider fixture root");
}

/// The provider rule is TCP-only: a datagram sent from inside the row-62
/// profile to any owned local IPv4 address at the provider port is denied and
/// never reaches an unconfined UDP receiver, while TCP to the provider works.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_guard_denies_udp_to_provider_port_on_every_local_ipv4_address() {
    let root = fresh_fixture_root("udp-provider-port-guard");
    std::fs::create_dir(&root).expect("create UDP guard fixture root");
    let provider = exclusive_listener(std::net::Ipv4Addr::LOCALHOST, 0);
    let bind_address = provider.local_addr().expect("provider bind address");
    let port = bind_address.port();
    let addresses = local_ipv4_addresses();
    let sentinels = TestSentinels::start(&addresses, port);
    let guard = ahrb::offline_guard::OfflineGuard::new(
        &root,
        &format!("http://127.0.0.1:{port}"),
        &bind_address.to_string(),
        &sentinels,
    )
    .expect("build and prove TCP-only Seatbelt guard");
    assert!(guard.evidence().udp_probes_blocked);
    let rendered = &guard.evidence().rendered_profile;
    match guard.local_delivery_failure() {
        // Hosts with a tunnel/proxy interface: the guard refuses to wrap any
        // command, but the rendered profile's UDP denial is still checked.
        Some(failure) => match guard.wrap_argv(&["/usr/bin/true".to_owned()]) {
            Err(ahrb::AhrbError::Protocol(message)) => assert_eq!(message, failure),
            other => panic!("unproven guard must refuse wrap_argv: {other:?}"),
        },
        None => {
            let prefix = guard
                .wrap_argv(&["/usr/bin/true".to_owned()])
                .expect("render guarded argv");
            let profile_path = &prefix[prefix
                .iter()
                .position(|argument| argument == "--profile")
                .expect("guarded argv names its profile")
                + 1];
            let on_disk = std::fs::read_to_string(profile_path).expect("read rendered profile");
            assert_eq!(&on_disk, rendered);
        }
    }
    assert!(rendered.contains(&format!("(remote tcp \"localhost:{port}\")")));
    assert!(!rendered.contains("(remote ip "));
    assert!(!rendered.contains("(remote udp "));
    // Run the probes under a test-owned copy of the exact rendered profile;
    // `guarded-launch` re-hashes it against the guard's evidence before exec.
    let profile_copy = root.join("row62-offline-copy.sb");
    std::fs::write(&profile_copy, rendered).expect("write rendered profile copy");
    let under_profile = |command: &[&str]| -> Vec<String> {
        let mut argv = vec![
            env!("CARGO_BIN_EXE_ahrb-fixture").to_owned(),
            "guarded-launch".to_owned(),
            "--profile".to_owned(),
            profile_copy.to_string_lossy().into_owned(),
            "--profile-sha256".to_owned(),
            guard.evidence().profile_sha256.clone(),
            "--launcher".to_owned(),
            "/usr/bin/sandbox-exec".to_owned(),
            "--".to_owned(),
        ];
        argv.extend(command.iter().map(|argument| (*argument).to_owned()));
        argv
    };

    for address in &addresses {
        let receiver =
            std::net::UdpSocket::bind((*address, port)).expect("bind unconfined UDP receiver");
        receiver
            .set_read_timeout(Some(Duration::from_millis(300)))
            .expect("set UDP receive timeout");
        let destination = format!("{address}:{port}");
        let argv = under_profile(&[
            env!("CARGO_BIN_EXE_ahrb-fixture"),
            "egress-udp-probe",
            "--address",
            &destination,
        ]);
        let output = Command::new(&argv[0])
            .args(&argv[1..])
            .stdin(Stdio::null())
            .output()
            .expect("run guarded UDP probe");
        assert_eq!(
            output.status.code(),
            Some(0),
            "UDP to {destination} was not denied: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let mut buffer = [0_u8; 256];
        let received = receiver.recv_from(&mut buffer);
        assert!(
            received.is_err(),
            "guarded UDP datagram reached {destination}: {received:?}"
        );
    }

    let provider_destination = format!("127.0.0.1:{port}");
    let tcp = under_profile(&[
        env!("CARGO_BIN_EXE_ahrb-fixture"),
        "egress-probe",
        "--address",
        &provider_destination,
        "--timeout-ms",
        "1000",
    ]);
    let tcp_output = Command::new(&tcp[0])
        .args(&tcp[1..])
        .stdin(Stdio::null())
        .output()
        .expect("run guarded TCP provider probe");
    assert_eq!(
        tcp_output.status.code(),
        Some(4),
        "provider TCP not reachable"
    );
    drop(guard);
    drop(sentinels);
    drop(provider);
    std::fs::remove_dir_all(root).expect("remove UDP guard fixture root");
}

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

/// Owns the detached `ahrb-fixture profile-daemon` a test starts through its
/// PID file. The daemon calls `setsid()` and parks forever, so a test that
/// panics before its reap step would otherwise leave it behind. On drop
/// (including unwinding) the guard SIGKILLs exactly the recorded PID, and only
/// while that PID still has the start time observed when the guard bound it.
struct FixtureDaemonGuard {
    pid_file: std::path::PathBuf,
    identity: Option<ahrb::process::ProcIdentity>,
}

impl FixtureDaemonGuard {
    /// Create the guard before launching the daemon that will write `pid_file`.
    fn new(pid_file: &Path) -> Self {
        Self {
            pid_file: pid_file.to_path_buf(),
            identity: None,
        }
    }

    /// Bind to the fixture PID named in the PID file, recording its start time.
    fn bind(&mut self) -> Option<u32> {
        if let Some(identity) = self.identity {
            return Some(identity.pid);
        }
        let pid = read_pid(&self.pid_file)?;
        let process = ahrb::process::live_process_info(pid).ok()??;
        if process.command != "ahrb-fixture" {
            return None;
        }
        self.identity = Some(process.identity);
        Some(pid)
    }

    /// Only the macOS-gated flock tests need the bound PID directly.
    #[cfg(target_os = "macos")]
    fn pid(&mut self) -> u32 {
        self.bind().expect("fixture daemon PID is bound")
    }
}

impl Drop for FixtureDaemonGuard {
    fn drop(&mut self) {
        // A launcher that succeeded may still be about to write the PID file.
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.bind().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let Some(identity) = self.identity else {
            return;
        };
        let Ok(pid) = i32::try_from(identity.pid) else {
            return;
        };
        match ahrb::process::live_process_info(identity.pid) {
            Ok(Some(process)) if process.identity == identity => {
                // SAFETY: the PID still has the start identity this guard
                // recorded for the fixture daemon its own test launched.
                unsafe { libc::kill(pid, libc::SIGKILL) };
                let gone = Instant::now() + Duration::from_secs(2);
                while pid_exists(identity.pid) && Instant::now() < gone {
                    std::thread::sleep(Duration::from_millis(10));
                }
                if pid_exists(identity.pid) {
                    eprintln!("fixture daemon {identity:?} survived SIGKILL");
                }
            }
            Ok(_) => {}
            Err(error) => eprintln!("could not re-verify fixture daemon {identity:?}: {error}"),
        }
    }
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
    let mut daemon = FixtureDaemonGuard::new(&pid_file);
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
        if let Some(pid) = daemon.bind() {
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

/// Start a detached `profile-daemon` fixture under a guard and panic before any
/// reap step, as a failing assertion or discovery error would.
fn start_guarded_daemon_then_panic(root: &Path) {
    let pid_file = root.join("daemon.pid");
    let mut daemon = FixtureDaemonGuard::new(&pid_file);
    let status = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"))
        .args([
            "profile-daemon-launcher",
            "--profile",
            root.to_str().expect("UTF-8 profile"),
            "--pid-file",
            pid_file.to_str().expect("UTF-8 pid file"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run exiting daemon launcher");
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(5);
    let pid = loop {
        if let Some(pid) = daemon.bind() {
            break pid;
        }
        assert!(Instant::now() < deadline, "profile daemon did not start");
        std::thread::sleep(Duration::from_millis(10));
    };
    std::fs::write(root.join("bound.pid"), pid.to_string()).expect("record bound PID");
    panic!("deliberate panic while fixture daemon {pid} is alive");
}

#[test]
fn fixture_daemon_guard_kills_detached_daemon_when_test_panics() {
    let root = fresh_fixture_root("daemon-guard-unwind");
    std::fs::create_dir_all(&root).expect("create fixture root");
    let outcome = std::panic::catch_unwind(|| start_guarded_daemon_then_panic(&root));
    assert!(outcome.is_err(), "helper must panic");
    let pid = read_pid(&root.join("bound.pid")).expect("daemon PID was bound before the panic");
    assert!(
        !pid_exists(pid),
        "guard must kill fixture daemon {pid} on unwind"
    );
    std::fs::remove_dir_all(root).expect("remove fixture root");
}

/// Manual demonstration that a whole test failing (not only a caught unwind)
/// leaves no fixture daemon: run with `--ignored --exact`, then confirm with
/// `ps` that no `ahrb-fixture` from this checkout remains. Ignored because it
/// fails by design; its root is left under the temp dir for inspection.
#[test]
#[ignore = "fails by design; manual unwind demonstration for the fixture daemon guard"]
fn fixture_daemon_guard_demo_failing_test_leaves_no_daemon() {
    let root = fresh_fixture_root("daemon-guard-failing-test");
    std::fs::create_dir_all(&root).expect("create fixture root");
    eprintln!("fixture root: {}", root.display());
    start_guarded_daemon_then_panic(&root);
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
/// profile by its *resolved* path (as Haider does), and wait for its PID. The
/// returned guard kills that exact daemon when dropped.
#[cfg(target_os = "macos")]
fn spawn_flock_daemon(profile_arg: &Path, pid_file: &Path, lock_file: &Path) -> FixtureDaemonGuard {
    let mut daemon = FixtureDaemonGuard::new(pid_file);
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
        if let Some(pid) = daemon.bind()
            && ahrb::process::lock_file_openers(lock_file)
                .is_ok_and(|openers| openers.iter().any(|process| process.identity.pid == pid))
        {
            return daemon;
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
    let mut daemon = spawn_flock_daemon(
        &resolved_profile,
        &root.join("daemon.pid"),
        &resolved_profile.join("home/.haider/dev-profile/lock"),
    );
    let daemon_pid = daemon.pid();

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

/// An opener whose argv never names the profile is not owned merely because
/// its executable basename is declared or it opened the same inode by a
/// symlink spelling.
#[cfg(target_os = "macos")]
#[test]
fn teardown_does_not_reap_unowned_declared_executable_or_symlink_alias_opener() {
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
    let mut holder = spawn_flock_daemon(&outside, &outside.join("holder.pid"), &lock_alias);
    let holder_pid = holder.pid();
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
    // Stop exactly the holder created by this test after teardown has proved it
    // did not signal the unrelated identity.
    drop(holder);
    assert_eq!(teardown.status, "ERROR", "errors: {:?}", teardown.errors);
    assert!(alive, "an unowned same-inode opener must not be signalled");
    assert!(teardown.lock_holders.iter().all(|evidence| {
        evidence
            .unowned_holders
            .iter()
            .any(|identity| identity.pid == holder_pid)
    }));
    assert!(
        teardown
            .reaped_processes
            .iter()
            .all(|identity| identity.pid != holder_pid)
    );
    std::fs::remove_dir_all(root).expect("remove fixture root");
    std::fs::remove_dir_all(outside).expect("remove outside directory");
}

#[test]
fn profile_lock_audit_rejects_symlinked_ancestry_and_hardlinks() {
    let root = fresh_fixture_root("lock-path-identity");
    let profile = root.join("profile");
    let outside = root.join("outside");
    std::fs::create_dir_all(&profile).expect("create profile");
    std::fs::create_dir_all(&outside).expect("create outside");
    let outside_lock = outside.join("lock");
    std::fs::write(&outside_lock, b"").expect("create outside lock");
    let link = profile.join("linked");
    std::os::unix::fs::symlink(&outside, &link).expect("create ancestry symlink");
    let symlink_error = ahrb::process::audit_profile_lock(&profile, &link.join("lock"))
        .expect_err("symlink ancestry must be rejected");
    assert!(symlink_error.to_string().contains("symlink"));

    let lock = profile.join("lock");
    std::fs::write(&lock, b"").expect("create profile lock");
    std::fs::hard_link(&lock, outside.join("hardlink")).expect("create hardlink escape");
    let hardlink_error = ahrb::process::audit_profile_lock(&profile, &lock)
        .expect_err("multiply linked lock must be rejected");
    assert!(hardlink_error.to_string().contains("hard links"));
    std::fs::remove_dir_all(root).expect("remove fixture root");
}

#[test]
fn teardown_never_signals_a_pid_with_a_reused_start_identity() {
    let _serial = lock_process_registry_test();
    let mut child = Command::new("/bin/sleep")
        .arg("30")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn PID-reuse control");
    let pid = child.id();
    let mut process = ahrb::process::live_process_info(pid)
        .expect("inspect control")
        .expect("control is live");
    process.identity.start_time = process.identity.start_time.saturating_add(1);
    ahrb::process::track_profile_owned_processes(&[process])
        .expect("register deliberately stale identity");
    let cleanup = ahrb::process::cleanup_owned_processes_with_evidence(Duration::from_millis(25))
        .expect("run identity-bound cleanup");
    assert!(pid_exists(pid), "PID-reused process must not be signalled");
    assert!(cleanup.reaped.is_empty());
    child.kill().expect("stop control child");
    child.wait().expect("reap control child");
}

#[test]
fn abort_cleanup_never_signals_a_pid_with_a_reused_start_identity() {
    let _serial = lock_process_registry_test();
    let mut victim = Command::new("/bin/sleep")
        .arg("30")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn abort PID-reuse control");
    let output = Command::new(env!("CARGO_BIN_EXE_ahrb-fixture"))
        .args(["abort-stale-identity", "--pid", &victim.id().to_string()])
        .output()
        .expect("run abort cleanup control");
    assert_eq!(output.status.code(), Some(128 + libc::SIGABRT));
    assert!(
        pid_exists(victim.id()),
        "abort cleanup must not signal a process whose start identity changed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("registered stale start"), "{stderr}");
    assert!(stderr.contains("skipped signal"), "{stderr}");
    assert!(stderr.contains("stale PID"), "{stderr}");
    victim.kill().expect("stop abort control victim");
    victim.wait().expect("reap abort control victim");
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
    let mut holder = spawn_flock_daemon(&profile, &root.join("holder.pid"), &lock_file);
    let holder_pid = holder.pid();

    // Declared executables do not include the fixture: it is not AHRB-owned.
    let teardown = ahrb::process::teardown_owned_processes(
        &root,
        &["haiderd".to_owned()],
        &[(profile.clone(), lock_file.clone())],
    );
    let alive = pid_exists(holder_pid);
    // Stop exactly the holder this test started before asserting.
    drop(holder);
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
