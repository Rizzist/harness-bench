use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static GUARD_ROOT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn create_guard_root() -> Result<PathBuf> {
    for _ in 0..16 {
        let sequence = GUARD_ROOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from("/tmp").join(format!(
            "ahrb-offline-guard-{}-{sequence}",
            std::process::id()
        ));
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(AhrbError::Protocol(
        "could not allocate a fresh AHRB-owned offline guard directory".to_owned(),
    ))
}

pub(super) fn build(
    profile_root: &Path,
    base_url: &str,
    provider_bind_address: &str,
    delivery: &dyn LocalDeliveryOracle,
) -> Result<OfflineGuard> {
    let (host, port) = parse_loopback_http(base_url)?;
    let expected_bind = format!("{host}:{port}");
    if provider_bind_address != expected_bind {
        return Err(AhrbError::Protocol(format!(
            "row-62 fake provider must bind the advertised destination at {expected_bind}, found {provider_bind_address}"
        )));
    }
    let interfaces = local_ipv4_interfaces()?;
    let owned_ipv4_address_set = verify_provider_port_ownership(&interfaces, port)?;
    let owned_ipv4_addresses = ipv4_strings(&owned_ipv4_address_set);
    let launcher = PathBuf::from("/usr/bin/sandbox-exec");
    if !launcher.is_file() {
        return Err(AhrbError::Protocol(
            "reviewed macOS offline guard unavailable: /usr/bin/sandbox-exec is absent".to_owned(),
        ));
    }
    let launcher = std::fs::canonicalize(launcher)?;
    let launcher_sha256 = hash_file(&launcher)?;
    let fixture = fixture_executable()?;
    let provider_address = format!("{host}:{port}");
    let canonical_profile_root = std::fs::canonicalize(profile_root)?;
    let guard_root = create_guard_root()?;
    let mut profile_root_guard = crate::results::RunRootGuard::new(guard_root.clone(), false)?;
    // This is an ephemeral guard-profile root rather than a benchmark run
    // root; it has no report bundle and is always eligible for owned cleanup.
    profile_root_guard.confirm_persisted();
    if !crate::results::claim_reserved_run_root(&guard_root)? {
        return Err(AhrbError::Protocol(
            "offline guard directory lost its AHRB reservation".to_owned(),
        ));
    }
    let canonical_guard_root = std::fs::canonicalize(&guard_root)?;
    let profile = canonical_guard_root.join("row62-offline.sb");
    // This macOS Seatbelt compiler rejects numeric hosts in `remote ip`.
    // Pair its required localhost token with both AF_INET and TCP. On this
    // Seatbelt implementation the host token covers the host's local IPv4
    // destinations, so the provider plus per-interface sentinels must own
    // every permitted TCP destination while UDP remains denied. Exclusive
    // binding alone is not proof: a tunnel/proxy interface can answer its
    // own address, so `prove` also requires each sentinel to record a
    // guarded setup connection.
    let provider_rule = format!(
        "(allow network-outbound (require-all (socket-domain AF_INET) (remote tcp \"localhost:{port}\")))"
    );
    let alternate_ipv4_destination = format!("127.0.0.2:{port}");
    let alternate_loopback_destination = format!("[::1]:{port}");
    // The active IPv6 same-port probe proves the rule remains IPv4-only. Unix
    // sockets under the cold profile remain available for harness-local IPC.
    let rendered = format!(
        concat!(
            "(version 1)\n",
            "(allow default)\n",
            "(deny file-write* (literal {:?}))\n",
            "(deny file-write* (subpath {:?}))\n",
            "(deny file-write* (literal {:?}))\n",
            "(deny network-outbound)\n",
            "{}\n",
            "(allow network-outbound (subpath {:?}))\n"
        ),
        canonical_guard_root.to_string_lossy(),
        canonical_guard_root.to_string_lossy(),
        profile.to_string_lossy(),
        provider_rule,
        canonical_profile_root.to_string_lossy()
    );
    let profile_sha256 = write_profile(&profile, rendered.as_bytes())?;
    let confinement_identity = format!(
        "macos-seatbelt-v3:profile-sha256:{profile_sha256}:launcher-sha256:{launcher_sha256}:provider:{provider_address}:bind:{provider_bind_address}"
    );
    let udp_probe_destinations = owned_ipv4_addresses
        .iter()
        .map(|address| format!("{address}:{port}"))
        .collect();
    let evidence = GuardEvidence {
        enforcement: crate::wave2_automation::OFFLINE_SEATBELT_ENFORCEMENT.to_owned(),
        confinement_identity,
        profile_sha256,
        rendered_profile: rendered,
        launcher_sha256,
        provider_destination: provider_address.clone(),
        provider_bind_address: provider_bind_address.to_owned(),
        owned_ipv4_addresses: owned_ipv4_addresses.clone(),
        provider_port_owned: true,
        provider_rule,
        provider_probe_allowed: false,
        udp_probe_destinations,
        udp_probes_blocked: false,
        alternate_ipv4_destination,
        alternate_ipv4_probe_blocked: false,
        alternate_loopback_destination,
        alternate_loopback_probe_blocked: false,
        control_destination: FORBIDDEN_CONTROL_ADDRESS.to_owned(),
        control_probe_blocked: false,
        child_inheritance_proven: false,
        profile_write_blocked: false,
        launch_hash_verified: false,
        local_ipv4_monitor: LocalIpv4MonitorEvidence {
            sample_interval_ms: LOCAL_IPV4_SAMPLE_INTERVAL_MS,
            samples_completed: 1,
            final_addresses: owned_ipv4_addresses.clone(),
            ..LocalIpv4MonitorEvidence::default()
        },
        local_delivery_probes: Vec::new(),
        local_delivery_proven: false,
    };
    let mut guard = OfflineGuard {
        fixture,
        launcher,
        profile,
        _profile_root_guard: profile_root_guard,
        provider_address,
        // Sample from the setup enumeration onward, so an address change
        // during the active probes below is also compared with the setup set.
        address_monitor: Some(LocalIpv4Monitor::start(owned_ipv4_address_set)),
        local_delivery_failure: None,
        evidence,
    };
    guard.prove(delivery, &interfaces)?;
    Ok(guard)
}

fn verify_provider_port_ownership(
    interfaces: &BTreeMap<Ipv4Addr, BTreeSet<String>>,
    port: u16,
) -> Result<BTreeSet<Ipv4Addr>> {
    let mut addresses = interfaces.keys().copied().collect::<BTreeSet<_>>();
    addresses.insert(Ipv4Addr::LOCALHOST);
    for address in addresses.iter().copied() {
        match std::net::TcpListener::bind((address, port)) {
            Ok(listener) => {
                drop(listener);
                return Err(AhrbError::Protocol(format!(
                    "row-62 provider port {port} is not exclusively owned at {address}"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(error) => {
                return Err(AhrbError::Protocol(format!(
                    "could not prove exclusive row-62 provider ownership at {address}:{port}: {error}"
                )));
            }
        }
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard_with_delivery_failure(failure: Option<&str>) -> (OfflineGuard, PathBuf) {
        let guard_root = create_guard_root().expect("allocate guard root");
        let mut root_guard = crate::results::RunRootGuard::new(guard_root.clone(), false)
            .expect("create owned guard root");
        root_guard.confirm_persisted();
        let guard = OfflineGuard {
            fixture: PathBuf::from("/nonexistent/ahrb-fixture"),
            launcher: PathBuf::from("/usr/bin/sandbox-exec"),
            profile: guard_root.join("row62-offline.sb"),
            _profile_root_guard: root_guard,
            provider_address: "127.0.0.1:9".to_owned(),
            address_monitor: None,
            local_delivery_failure: failure.map(str::to_owned),
            evidence: GuardEvidence::default(),
        };
        (guard, guard_root)
    }

    #[test]
    fn wrap_argv_refuses_after_failed_local_delivery_proof() {
        let failure =
            format!("{LOCAL_DELIVERY_FAILURE_PREFIX}: 198.18.0.1 (utun4) answered-by-other");
        let argv = vec!["/bin/echo".to_owned(), "harness".to_owned()];
        let (guard, guard_root) = guard_with_delivery_failure(Some(&failure));
        match guard.wrap_argv(&argv) {
            Err(AhrbError::Protocol(message)) => assert_eq!(message, failure),
            other => panic!("wrap_argv must refuse an unproven boundary: {other:?}"),
        }
        let manifest = crate::manifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml"),
        )
        .expect("load mock manifest");
        match guard.apply_to_manifest(&manifest) {
            Err(AhrbError::Protocol(message)) => assert_eq!(message, failure),
            other => panic!("apply_to_manifest must refuse the same way: {other:?}"),
        }
        drop(guard);
        assert!(!guard_root.exists(), "owned guard root must be removed");

        let (guard, guard_root) = guard_with_delivery_failure(None);
        let wrapped = guard.wrap_argv(&argv).expect("proven guard wraps");
        assert_eq!(wrapped[0], "/nonexistent/ahrb-fixture");
        assert_eq!(wrapped[1], "guarded-launch");
        assert!(wrapped.ends_with(&argv));
        drop(guard);
        assert!(!guard_root.exists(), "owned guard root must be removed");
    }
}
