//! Platform-specific row-62 outbound-network confinement.
//!
//! The guard is deliberately launch based. Environment-only proxy settings are
//! not confinement evidence and are never accepted here.

use crate::manifest::{Manifest, TransportKind};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
#[cfg(target_os = "macos")]
use sha2::{Digest as _, Sha256};
#[cfg(unix)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
#[cfg(target_os = "macos")]
use std::process::{Command, Stdio};

#[cfg(target_os = "macos")]
const LOCAL_IPV4_SAMPLE_INTERVAL_MS: u64 = 500;

#[cfg(target_os = "macos")]
const FORBIDDEN_CONTROL_ADDRESS: &str = "203.0.113.1:9";

/// How long guard setup waits for AHRB's sentinel to record one delivery
/// probe after the guarded connect succeeded.
pub const LOCAL_DELIVERY_WAIT_MS: u64 = 2_000;

/// The TCP connect reached the exact AHRB sentinel on that address; the
/// sentinel recorded the probe's own peer address.
pub const LOCAL_DELIVERY_OWNED: &str = "delivered-to-ahrb-sentinel";
/// Seatbelt denied the TCP connect (EPERM/EACCES): the rule does not permit
/// this address, so no AHRB ownership is required there.
pub const LOCAL_DELIVERY_BLOCKED: &str = "blocked-by-profile";
/// The connect succeeded but AHRB's sentinel recorded no matching arrival:
/// another process or interface (for example a tunnel/proxy extension)
/// answered it.
pub const LOCAL_DELIVERY_ANSWERED_BY_OTHER: &str = "answered-by-other";
/// The connect was refused although AHRB's sentinel holds that address.
pub const LOCAL_DELIVERY_REFUSED: &str = "refused";
/// The connect did not complete within the probe timeout.
pub const LOCAL_DELIVERY_TIMEOUT: &str = "timeout";
/// The probe result could not be attributed (unparseable output, missing
/// local address, or another connect error).
pub const LOCAL_DELIVERY_AMBIGUOUS: &str = "ambiguous";

/// Prefix of the row-62 ERROR raised when a delivery probe does not prove
/// AHRB ownership of a permitted local address.
pub const LOCAL_DELIVERY_FAILURE_PREFIX: &str =
    "guard boundary includes a local address AHRB does not own";

/// Result of one guard-setup TCP probe from inside the row-62 profile to one
/// non-loopback local IPv4 address at the provider port.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalDeliveryProbe {
    pub address: String,
    pub interfaces: Vec<String>,
    pub destination: String,
    pub outcome: String,
    /// Local (source) address the guarded probe socket was bound to; this is
    /// the per-probe token the sentinel must record as its peer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_local_address: Option<String>,
    pub sentinel_recorded: bool,
    pub detail: String,
}

impl LocalDeliveryProbe {
    /// Only an arrival recorded by AHRB's own sentinel, or a Seatbelt denial,
    /// keeps the address inside the proven boundary.
    pub fn proves_boundary(&self) -> bool {
        (self.outcome == LOCAL_DELIVERY_OWNED && self.sentinel_recorded)
            || (self.outcome == LOCAL_DELIVERY_BLOCKED && !self.sentinel_recorded)
    }
}

/// Row-62 ERROR reason naming every probed address (and its interfaces)
/// whose delivery was not proven, or `None` when every probe proves the
/// boundary.
pub fn local_delivery_failure(probes: &[LocalDeliveryProbe]) -> Option<String> {
    let failed = probes
        .iter()
        .filter(|probe| !probe.proves_boundary())
        .map(|probe| {
            format!(
                "{} ({}) {}: {}",
                probe.address,
                if probe.interfaces.is_empty() {
                    "unknown interface".to_owned()
                } else {
                    probe.interfaces.join(", ")
                },
                probe.outcome,
                probe.detail
            )
        })
        .collect::<Vec<_>>();
    if failed.is_empty() {
        None
    } else {
        Some(format!(
            "{LOCAL_DELIVERY_FAILURE_PREFIX}: {}",
            failed.join("; ")
        ))
    }
}

/// Setup-probe arrivals observed by AHRB's own row-62 sentinels.
pub trait LocalDeliveryOracle: Send + Sync {
    /// Return true only if an AHRB sentinel accepted a connection at exactly
    /// `destination` from exactly `peer`. A claimed arrival is moved to the
    /// sentinel's separate setup-probe record so it never counts as a
    /// harness connection.
    fn claim_setup_arrival(&self, destination: SocketAddrV4, peer: SocketAddrV4) -> bool;
}

/// Raw result of one guarded delivery connect, before sentinel matching.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocalDeliveryConnect {
    Connected { local: Option<SocketAddrV4> },
    Blocked,
    Refused,
    TimedOut,
    Failed(String),
}

/// Classify one delivery probe. `sentinel_recorded` must come from the
/// sentinel oracle for exactly the probe's own local address.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn classify_local_delivery(
    address: Ipv4Addr,
    interfaces: &BTreeSet<String>,
    port: u16,
    connect: LocalDeliveryConnect,
    sentinel_recorded: bool,
) -> LocalDeliveryProbe {
    let destination = SocketAddrV4::new(address, port);
    let mut probe_local_address = None;
    let (outcome, detail) = match connect {
        LocalDeliveryConnect::Connected { local: Some(local) } => {
            probe_local_address = Some(local.to_string());
            if sentinel_recorded {
                (
                    LOCAL_DELIVERY_OWNED,
                    format!("AHRB sentinel at {destination} recorded peer {local}"),
                )
            } else {
                (
                    LOCAL_DELIVERY_ANSWERED_BY_OTHER,
                    format!(
                        "connect from inside the row-62 profile succeeded from {local} but AHRB's sentinel at {destination} recorded no such arrival within {LOCAL_DELIVERY_WAIT_MS} ms"
                    ),
                )
            }
        }
        LocalDeliveryConnect::Connected { local: None } => (
            LOCAL_DELIVERY_AMBIGUOUS,
            format!(
                "connect to {destination} succeeded but the probe reported no local address to match"
            ),
        ),
        LocalDeliveryConnect::Blocked => (
            LOCAL_DELIVERY_BLOCKED,
            format!("Seatbelt denied TCP to {destination} with EPERM/EACCES"),
        ),
        LocalDeliveryConnect::Refused => (
            LOCAL_DELIVERY_REFUSED,
            format!("connect to {destination} was refused although AHRB's sentinel holds it"),
        ),
        LocalDeliveryConnect::TimedOut => (
            LOCAL_DELIVERY_TIMEOUT,
            format!("connect to {destination} did not complete"),
        ),
        LocalDeliveryConnect::Failed(error) => (
            LOCAL_DELIVERY_AMBIGUOUS,
            format!("probe to {destination} gave no attributable result: {error}"),
        ),
    };
    // A sentinel record is only meaningful for a connected probe with a
    // known source; anything else is not credited as delivery.
    let sentinel_recorded = sentinel_recorded && outcome == LOCAL_DELIVERY_OWNED;
    LocalDeliveryProbe {
        address: address.to_string(),
        interfaces: interfaces.iter().cloned().collect(),
        destination: destination.to_string(),
        outcome: outcome.to_owned(),
        probe_local_address,
        sentinel_recorded,
        detail,
    }
}

/// Interpret the `ahrb-fixture egress-probe` exit status and JSON line.
#[cfg(target_os = "macos")]
fn parse_delivery_connect(status: Option<i32>, stdout: &[u8]) -> LocalDeliveryConnect {
    let text = String::from_utf8_lossy(stdout);
    let value = serde_json::from_str::<serde_json::Value>(text.trim()).ok();
    let field = |name: &str| value.as_ref().and_then(|value| value.get(name));
    match status {
        Some(4) => LocalDeliveryConnect::Connected {
            local: field("local")
                .and_then(|local| local.as_str())
                .and_then(|local| local.parse::<SocketAddrV4>().ok()),
        },
        Some(0) if field("blocked").and_then(|blocked| blocked.as_bool()) == Some(true) => {
            LocalDeliveryConnect::Blocked
        }
        Some(3) => {
            let errno = field("errno").and_then(|errno| errno.as_i64());
            let kind = field("kind").and_then(|kind| kind.as_str()).unwrap_or("");
            if errno == Some(i64::from(libc::ECONNREFUSED)) || kind == "ConnectionRefused" {
                LocalDeliveryConnect::Refused
            } else if errno == Some(i64::from(libc::ETIMEDOUT)) || kind == "TimedOut" {
                LocalDeliveryConnect::TimedOut
            } else {
                LocalDeliveryConnect::Failed(text.trim().to_owned())
            }
        }
        other => LocalDeliveryConnect::Failed(format!("status={other:?} stdout={}", text.trim())),
    }
}

/// Evidence binding a harness launch and its independent control probe to one
/// reviewed platform guard.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GuardEvidence {
    pub enforcement: String,
    pub confinement_identity: String,
    pub profile_sha256: String,
    /// Exact rendered Seatbelt profile bytes whose SHA-256 is `profile_sha256`.
    pub rendered_profile: String,
    pub launcher_sha256: String,
    pub provider_destination: String,
    pub provider_bind_address: String,
    pub owned_ipv4_addresses: Vec<String>,
    pub provider_port_owned: bool,
    pub provider_rule: String,
    pub provider_probe_allowed: bool,
    pub udp_probe_destinations: Vec<String>,
    pub udp_probes_blocked: bool,
    pub alternate_ipv4_destination: String,
    pub alternate_ipv4_probe_blocked: bool,
    pub alternate_loopback_destination: String,
    pub alternate_loopback_probe_blocked: bool,
    pub control_destination: String,
    pub control_probe_blocked: bool,
    pub child_inheritance_proven: bool,
    pub profile_write_blocked: bool,
    pub launch_hash_verified: bool,
    pub local_ipv4_monitor: LocalIpv4MonitorEvidence,
    /// One setup-time TCP delivery probe per non-loopback owned address.
    pub local_delivery_probes: Vec<LocalDeliveryProbe>,
    /// True only when every non-loopback owned address was probed and each
    /// probe was recorded by AHRB's sentinel (or denied by Seatbelt).
    pub local_delivery_proven: bool,
}

/// Address-set coverage for the local-address sentinels used by row 62.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalIpv4MonitorEvidence {
    pub sample_interval_ms: u64,
    pub samples_completed: u64,
    pub final_addresses: Vec<String>,
    pub change_detected: bool,
    pub first_change_old_addresses: Vec<String>,
    pub first_change_new_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// One rendered, immutable guard reused for the harness and challenged probe.
#[derive(Debug)]
pub struct OfflineGuard {
    fixture: PathBuf,
    launcher: PathBuf,
    profile: PathBuf,
    _profile_root_guard: crate::results::RunRootGuard,
    #[cfg(target_os = "macos")]
    provider_address: String,
    #[cfg(target_os = "macos")]
    address_monitor: Option<LocalIpv4Monitor>,
    /// Set when guard setup could not prove local delivery to AHRB's own
    /// sentinels; the guard then refuses to wrap any harness command.
    local_delivery_failure: Option<String>,
    evidence: GuardEvidence,
}

impl OfflineGuard {
    /// Build the platform guard for the exact loopback listener in `base_url`.
    /// `delivery` is the provider's sentinel record used to prove that every
    /// non-loopback local address the rule permits reaches AHRB. A guard
    /// whose delivery proof failed is still returned, with its evidence, but
    /// [`Self::local_delivery_failure`] is set and it refuses to guard a
    /// harness manifest.
    pub fn new(
        profile_root: &Path,
        base_url: &str,
        provider_bind_address: &str,
        delivery: &dyn LocalDeliveryOracle,
    ) -> Result<Self> {
        #[cfg(target_os = "macos")]
        {
            return macos::build(profile_root, base_url, provider_bind_address, delivery);
        }
        #[cfg(target_os = "linux")]
        {
            return linux::build(profile_root, base_url, provider_bind_address, delivery);
        }
        #[allow(unreachable_code)]
        Err(AhrbError::Unsupported(
            "row-62 OS confinement is implemented only on macOS and Linux".to_owned(),
        ))
    }

    /// Return the immutable evidence after active positive and negative probes.
    pub fn evidence(&self) -> &GuardEvidence {
        &self.evidence
    }

    /// Row-62 ERROR reason when guard setup could not prove that every
    /// permitted non-loopback local address is delivered to AHRB.
    pub fn local_delivery_failure(&self) -> Option<&str> {
        self.local_delivery_failure.as_deref()
    }

    /// Stop periodic local-address sampling and take the required end sample.
    /// A change is retained as evidence instead of returned early so row 62
    /// can report the old and new sets in its infrastructure ERROR. Only the
    /// macOS guard samples addresses; other platforms never construct a guard.
    pub fn finish_trial_address_monitor(&mut self) {
        #[cfg(target_os = "macos")]
        {
            let Some(monitor) = self.address_monitor.take() else {
                self.evidence.local_ipv4_monitor.error =
                    Some("row-62 local IPv4 monitor was unavailable at trial end".to_owned());
                return;
            };
            self.evidence.local_ipv4_monitor = monitor.finish();
        }
    }

    /// Prefix every executable surface which may launch or control the
    /// challenged harness. Descendants inherit the same confinement.
    pub fn apply_to_manifest(&self, manifest: &Manifest) -> Result<Manifest> {
        self.refuse_unproven_local_delivery()?;
        let mut guarded = manifest.clone();
        visit_row62_launch_commands_mut(&mut guarded, |_, argv| self.wrap_nonempty(argv));
        validate_row62_launch_commands(&guarded, |argv| self.is_wrapped(argv))?;
        Ok(guarded)
    }

    /// Prefix an already rendered harness command with this guard. Like
    /// [`Self::apply_to_manifest`], it refuses once the delivery proof failed;
    /// setup probes run before that result is recorded.
    pub fn wrap_argv(&self, argv: &[String]) -> Result<Vec<String>> {
        self.refuse_unproven_local_delivery()?;
        if argv.is_empty() {
            return Err(AhrbError::Validation(
                "offline guard cannot wrap an empty command".to_owned(),
            ));
        }
        let mut wrapped = self.prefix();
        wrapped.extend_from_slice(argv);
        Ok(wrapped)
    }

    fn refuse_unproven_local_delivery(&self) -> Result<()> {
        match &self.local_delivery_failure {
            Some(failure) => Err(AhrbError::Protocol(failure.clone())),
            None => Ok(()),
        }
    }

    fn wrap_nonempty(&self, argv: &mut Vec<String>) {
        if argv.is_empty() {
            return;
        }
        let mut wrapped = self.prefix();
        wrapped.append(argv);
        *argv = wrapped;
    }

    fn prefix(&self) -> Vec<String> {
        vec![
            self.fixture.to_string_lossy().into_owned(),
            "guarded-launch".to_owned(),
            "--profile".to_owned(),
            self.profile.to_string_lossy().into_owned(),
            "--profile-sha256".to_owned(),
            self.evidence.profile_sha256.clone(),
            "--launcher".to_owned(),
            self.launcher.to_string_lossy().into_owned(),
            "--".to_owned(),
        ]
    }

    fn is_wrapped(&self, argv: &[String]) -> bool {
        argv.starts_with(&self.prefix())
    }

    #[cfg(target_os = "macos")]
    fn prove(
        &mut self,
        delivery: &dyn LocalDeliveryOracle,
        interfaces: &BTreeMap<Ipv4Addr, BTreeSet<String>>,
    ) -> Result<()> {
        let fixture = self.fixture.clone();
        let provider = run_probe(self, &fixture, &self.provider_address, false)?;
        if provider.status.code() != Some(4) {
            return Err(AhrbError::Protocol(format!(
                "offline guard did not allow the exact fake-provider listener {}: status={} stdout={} stderr={}",
                self.provider_address,
                provider.status,
                String::from_utf8_lossy(&provider.stdout).trim(),
                String::from_utf8_lossy(&provider.stderr).trim()
            )));
        }
        let alternate_ipv4 = run_probe(
            self,
            &fixture,
            &self.evidence.alternate_ipv4_destination,
            false,
        )?;
        if !alternate_ipv4.status.success() {
            return Err(AhrbError::Protocol(format!(
                "offline guard permitted alternate IPv4 loopback {} on the provider port: status={} stdout={} stderr={}",
                self.evidence.alternate_ipv4_destination,
                alternate_ipv4.status,
                String::from_utf8_lossy(&alternate_ipv4.stdout).trim(),
                String::from_utf8_lossy(&alternate_ipv4.stderr).trim()
            )));
        }
        let alternate_listener = std::net::TcpListener::bind(
            &self.evidence.alternate_loopback_destination,
        )
        .map_err(|error| {
            AhrbError::Protocol(format!(
                "offline guard could not bind alternate loopback control {}: {error}",
                self.evidence.alternate_loopback_destination
            ))
        })?;
        let alternate = run_probe(
            self,
            &fixture,
            &self.evidence.alternate_loopback_destination,
            false,
        )?;
        drop(alternate_listener);
        if !alternate.status.success() {
            return Err(AhrbError::Protocol(format!(
                "offline guard permitted the other loopback address family on the provider port {}: status={} stdout={} stderr={}",
                self.evidence.alternate_loopback_destination,
                alternate.status,
                String::from_utf8_lossy(&alternate.stdout).trim(),
                String::from_utf8_lossy(&alternate.stderr).trim()
            )));
        }
        let control = run_probe(self, &fixture, FORBIDDEN_CONTROL_ADDRESS, true)?;
        if !control.status.success() {
            return Err(AhrbError::Protocol(format!(
                "offline guard did not block the inherited challenged control {} with EPERM/EACCES: status={} stdout={} stderr={}",
                FORBIDDEN_CONTROL_ADDRESS,
                control.status,
                String::from_utf8_lossy(&control.stdout).trim(),
                String::from_utf8_lossy(&control.stderr).trim()
            )));
        }
        self.evidence.provider_probe_allowed = true;
        for destination in self.evidence.udp_probe_destinations.clone() {
            let udp = run_udp_probe(self, &fixture, &destination)?;
            if !udp.status.success() {
                return Err(AhrbError::Protocol(format!(
                    "offline guard permitted UDP to {destination} on the provider port: status={} stdout={} stderr={}",
                    udp.status,
                    String::from_utf8_lossy(&udp.stdout).trim(),
                    String::from_utf8_lossy(&udp.stderr).trim()
                )));
            }
        }
        self.evidence.udp_probes_blocked = true;
        self.evidence.alternate_ipv4_probe_blocked = true;
        self.evidence.alternate_loopback_probe_blocked = true;
        self.evidence.control_probe_blocked = true;
        self.evidence.child_inheritance_proven = true;
        let profile_write = run_profile_write_probe(self, &fixture)?;
        if !profile_write.status.success() {
            return Err(AhrbError::Protocol(format!(
                "offline guard did not deny writes to its profile: status={} stdout={} stderr={}",
                profile_write.status,
                String::from_utf8_lossy(&profile_write.stdout).trim(),
                String::from_utf8_lossy(&profile_write.stderr).trim()
            )));
        }
        verify_profile_hash(&self.profile, &self.evidence.profile_sha256)?;
        self.evidence.profile_write_blocked = true;
        self.evidence.launch_hash_verified = true;
        self.prove_local_delivery(delivery, interfaces)
    }

    /// Connect from inside the profile to every non-loopback owned address at
    /// the provider port and require AHRB's own sentinel to record exactly
    /// that connection (matched by the probe's source address and port).
    #[cfg(target_os = "macos")]
    fn prove_local_delivery(
        &mut self,
        delivery: &dyn LocalDeliveryOracle,
        interfaces: &BTreeMap<Ipv4Addr, BTreeSet<String>>,
    ) -> Result<()> {
        let port = self
            .provider_address
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .ok_or_else(|| {
                AhrbError::Protocol("offline guard provider port disappeared".to_owned())
            })?;
        let fixture = self.fixture.clone();
        let no_interfaces = BTreeSet::new();
        let mut probes = Vec::new();
        for address in self
            .evidence
            .owned_ipv4_addresses
            .iter()
            .filter_map(|address| address.parse::<Ipv4Addr>().ok())
            .filter(|address| !address.is_loopback())
        {
            let destination = SocketAddrV4::new(address, port);
            let output = run_probe(self, &fixture, &destination.to_string(), false)?;
            let connect = parse_delivery_connect(output.status.code(), &output.stdout);
            let recorded = match &connect {
                LocalDeliveryConnect::Connected { local: Some(local) } => {
                    wait_for_sentinel_arrival(delivery, destination, *local)
                }
                _ => false,
            };
            probes.push(classify_local_delivery(
                address,
                interfaces.get(&address).unwrap_or(&no_interfaces),
                port,
                connect,
                recorded,
            ));
        }
        let expected = self
            .evidence
            .owned_ipv4_addresses
            .iter()
            .filter(|address| {
                !address
                    .parse::<Ipv4Addr>()
                    .is_ok_and(|address| address.is_loopback())
            })
            .count();
        self.local_delivery_failure = if probes.len() == expected {
            local_delivery_failure(&probes)
        } else {
            Some(format!(
                "{LOCAL_DELIVERY_FAILURE_PREFIX}: {} non-loopback owned addresses but {} delivery probes",
                expected,
                probes.len()
            ))
        };
        self.evidence.local_delivery_proven = self.local_delivery_failure.is_none();
        self.evidence.local_delivery_probes = probes;
        Ok(())
    }
}

/// Poll the sentinel record until the probe's exact arrival is claimed or the
/// bounded wait ends.
#[cfg(target_os = "macos")]
fn wait_for_sentinel_arrival(
    delivery: &dyn LocalDeliveryOracle,
    destination: SocketAddrV4,
    peer: SocketAddrV4,
) -> bool {
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(LOCAL_DELIVERY_WAIT_MS);
    loop {
        if delivery.claim_setup_arrival(destination, peer) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug, Default)]
struct LocalIpv4MonitorResult {
    samples_completed: u64,
    final_addresses: BTreeSet<Ipv4Addr>,
    first_change: Option<(BTreeSet<Ipv4Addr>, BTreeSet<Ipv4Addr>)>,
    error: Option<String>,
}

#[cfg(target_os = "macos")]
fn compare_local_ipv4_sample(
    initial: &BTreeSet<Ipv4Addr>,
    sampled: &BTreeSet<Ipv4Addr>,
    first_change: &mut Option<(BTreeSet<Ipv4Addr>, BTreeSet<Ipv4Addr>)>,
) {
    if sampled != initial && first_change.is_none() {
        *first_change = Some((initial.clone(), sampled.clone()));
    }
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct LocalIpv4Monitor {
    initial: BTreeSet<Ipv4Addr>,
    stop: Option<std::sync::mpsc::Sender<()>>,
    task: Option<std::thread::JoinHandle<LocalIpv4MonitorResult>>,
}

#[cfg(target_os = "macos")]
impl LocalIpv4Monitor {
    fn start(initial: BTreeSet<Ipv4Addr>) -> Self {
        let thread_initial = initial.clone();
        let (stop, receiver) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            let mut result = LocalIpv4MonitorResult {
                samples_completed: 1,
                final_addresses: thread_initial.clone(),
                ..LocalIpv4MonitorResult::default()
            };
            loop {
                match receiver.recv_timeout(std::time::Duration::from_millis(
                    LOCAL_IPV4_SAMPLE_INTERVAL_MS,
                )) {
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                }
                match local_ipv4_addresses() {
                    Ok(sampled) => {
                        result.samples_completed = result.samples_completed.saturating_add(1);
                        compare_local_ipv4_sample(
                            &thread_initial,
                            &sampled,
                            &mut result.first_change,
                        );
                        result.final_addresses = sampled;
                    }
                    Err(error) => {
                        result.error = Some(format!(
                            "periodic local IPv4 address enumeration failed: {error}"
                        ));
                        break;
                    }
                }
            }
            result
        });
        Self {
            initial,
            stop: Some(stop),
            task: Some(task),
        }
    }

    fn stop_and_join(&mut self) -> LocalIpv4MonitorResult {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task
            .take()
            .and_then(|task| task.join().ok())
            .unwrap_or_else(|| LocalIpv4MonitorResult {
                samples_completed: 1,
                final_addresses: self.initial.clone(),
                error: Some("row-62 local IPv4 monitor thread failed".to_owned()),
                ..LocalIpv4MonitorResult::default()
            })
    }

    fn finish(mut self) -> LocalIpv4MonitorEvidence {
        let mut result = self.stop_and_join();
        match local_ipv4_addresses() {
            Ok(final_addresses) => {
                result.samples_completed = result.samples_completed.saturating_add(1);
                compare_local_ipv4_sample(
                    &self.initial,
                    &final_addresses,
                    &mut result.first_change,
                );
                result.final_addresses = final_addresses;
            }
            Err(error) => {
                result.error = Some(format!(
                    "final local IPv4 address enumeration failed: {error}"
                ));
            }
        }
        local_ipv4_monitor_evidence(result)
    }
}

#[cfg(target_os = "macos")]
impl Drop for LocalIpv4Monitor {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

#[cfg(target_os = "macos")]
fn local_ipv4_monitor_evidence(result: LocalIpv4MonitorResult) -> LocalIpv4MonitorEvidence {
    let change_detected = result.first_change.is_some();
    let (old, new) = result.first_change.unwrap_or_default();
    LocalIpv4MonitorEvidence {
        sample_interval_ms: LOCAL_IPV4_SAMPLE_INTERVAL_MS,
        samples_completed: result.samples_completed,
        final_addresses: ipv4_strings(&result.final_addresses),
        change_detected,
        first_change_old_addresses: ipv4_strings(&old),
        first_change_new_addresses: ipv4_strings(&new),
        error: result.error,
    }
}

#[cfg(target_os = "macos")]
fn ipv4_strings(addresses: &BTreeSet<Ipv4Addr>) -> Vec<String> {
    addresses.iter().map(ToString::to_string).collect()
}

#[cfg(unix)]
pub(crate) fn local_ipv4_addresses() -> Result<BTreeSet<Ipv4Addr>> {
    let mut addresses = local_ipv4_interfaces()?
        .into_keys()
        .collect::<BTreeSet<_>>();
    addresses.insert(Ipv4Addr::LOCALHOST);
    Ok(addresses)
}

/// Local IPv4 addresses from `getifaddrs`, each with the interface names that
/// carry it.
#[cfg(unix)]
pub(crate) fn local_ipv4_interfaces() -> Result<BTreeMap<Ipv4Addr, BTreeSet<String>>> {
    let mut head = std::ptr::null_mut::<libc::ifaddrs>();
    // SAFETY: `head` is a valid output pointer. A successful call returns a
    // linked list which remains valid until the paired `freeifaddrs` below.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    struct IfAddrs(*mut libc::ifaddrs);
    impl Drop for IfAddrs {
        fn drop(&mut self) {
            // SAFETY: this pointer came from a successful `getifaddrs` call
            // and is freed exactly once by this guard.
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
    let _guard = IfAddrs(head);
    let mut addresses = BTreeMap::<Ipv4Addr, BTreeSet<String>>::new();
    let mut current = head;
    while !current.is_null() {
        // SAFETY: `current` walks the live `getifaddrs` list and `ifa_addr`
        // is checked before reading an AF_INET `sockaddr_in`.
        let entry = unsafe { &*current };
        if !entry.ifa_addr.is_null()
            && unsafe { (*entry.ifa_addr).sa_family as i32 } == libc::AF_INET
        {
            // SAFETY: the family check above establishes the concrete layout.
            let address = unsafe { &*(entry.ifa_addr.cast::<libc::sockaddr_in>()) };
            let name = if entry.ifa_name.is_null() {
                String::new()
            } else {
                // SAFETY: a non-null `ifa_name` is a NUL-terminated string
                // owned by the live `getifaddrs` list.
                unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) }
                    .to_string_lossy()
                    .into_owned()
            };
            let names = addresses
                .entry(Ipv4Addr::from(u32::from_be(address.sin_addr.s_addr)))
                .or_default();
            if !name.is_empty() {
                names.insert(name);
            }
        }
        current = entry.ifa_next;
    }
    Ok(addresses)
}

/// Visit every manifest argv that the row-62 driver may launch as a process.
/// Protocol operation names and hook commands executed by an already-confined
/// harness are deliberately excluded. Adding a new driver-launched command
/// requires routing it through this one inventory.
fn visit_row62_launch_commands_mut(
    manifest: &mut Manifest,
    mut visit: impl FnMut(&'static str, &mut Vec<String>),
) {
    visit("transport.command", &mut manifest.transport.command);
    visit("daemon.start", &mut manifest.daemon.start);
    visit("daemon.initialize", &mut manifest.daemon.initialize);
    visit(
        "daemon.readiness.command",
        &mut manifest.daemon.readiness.command,
    );
    if matches!(
        manifest.transport.kind,
        TransportKind::Exec | TransportKind::SocketJsonrpc | TransportKind::Http
    ) {
        visit("daemon.shutdown", &mut manifest.daemon.shutdown);
    }
    if manifest.transport.kind == TransportKind::Exec {
        visit(
            "sessions.continue_turn",
            &mut manifest.sessions.continue_turn,
        );
        visit("sessions.resume", &mut manifest.sessions.resume);
        visit(
            "sessions.resume_control",
            &mut manifest.sessions.resume_control,
        );
        visit(
            "sessions.recover_probe",
            &mut manifest.sessions.recover_probe,
        );
        visit("sessions.close_delete", &mut manifest.sessions.close_delete);
        visit("sessions.wait_ready", &mut manifest.sessions.wait_ready);
        visit("concurrency.release", &mut manifest.concurrency.release);
        visit("agents.cancel", &mut manifest.agents.cancel);
        visit("events.replay_command", &mut manifest.events.replay_command);
        visit(
            "events.replay_state_command",
            &mut manifest.events.replay_state_command,
        );
    }
}

fn validate_row62_launch_commands(
    manifest: &Manifest,
    mut guarded: impl FnMut(&[String]) -> bool,
) -> Result<()> {
    let mut copy = manifest.clone();
    let mut unguarded = Vec::new();
    visit_row62_launch_commands_mut(&mut copy, |name, argv| {
        if !argv.is_empty() && !guarded(argv) {
            unguarded.push(name);
        }
    });
    if unguarded.is_empty() {
        Ok(())
    } else {
        Err(AhrbError::Protocol(format!(
            "row-62 executable harness surfaces are not guarded: {}",
            unguarded.join(", ")
        )))
    }
}

#[cfg(target_os = "macos")]
fn run_probe(
    guard: &OfflineGuard,
    fixture: &Path,
    address: &str,
    through_child: bool,
) -> Result<std::process::Output> {
    let mut argv = vec![fixture.to_string_lossy().into_owned()];
    if through_child {
        argv.push("egress-probe-child".to_owned());
    } else {
        argv.push("egress-probe".to_owned());
    }
    argv.extend([
        "--address".to_owned(),
        address.to_owned(),
        "--timeout-ms".to_owned(),
        "1000".to_owned(),
    ]);
    let wrapped = guard.wrap_argv(&argv)?;
    let (program, arguments) = wrapped
        .split_first()
        .ok_or_else(|| AhrbError::Protocol("offline probe command disappeared".to_owned()))?;
    Ok(Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .output()?)
}

#[cfg(target_os = "macos")]
fn run_udp_probe(
    guard: &OfflineGuard,
    fixture: &Path,
    address: &str,
) -> Result<std::process::Output> {
    let argv = vec![
        fixture.to_string_lossy().into_owned(),
        "egress-udp-probe".to_owned(),
        "--address".to_owned(),
        address.to_owned(),
    ];
    let wrapped = guard.wrap_argv(&argv)?;
    let (program, arguments) = wrapped
        .split_first()
        .ok_or_else(|| AhrbError::Protocol("offline UDP probe command disappeared".to_owned()))?;
    Ok(Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .output()?)
}

#[cfg(all(test, target_os = "macos"))]
mod address_monitor_tests {
    use super::*;

    #[test]
    fn injected_local_ipv4_samples_record_first_change_and_never_clear_it() {
        let initial = BTreeSet::from([Ipv4Addr::LOCALHOST, Ipv4Addr::new(192, 0, 2, 10)]);
        let changed = BTreeSet::from([
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::new(192, 0, 2, 10),
            Ipv4Addr::new(198, 18, 0, 9),
        ]);
        let mut first_change = None;
        compare_local_ipv4_sample(&initial, &initial, &mut first_change);
        assert!(first_change.is_none());
        compare_local_ipv4_sample(&initial, &changed, &mut first_change);
        compare_local_ipv4_sample(&initial, &initial, &mut first_change);
        assert_eq!(first_change, Some((initial, changed)));
    }

    #[test]
    fn injected_local_ipv4_removal_is_a_change_with_old_and_new_sets_in_evidence() {
        let initial = BTreeSet::from([Ipv4Addr::LOCALHOST, Ipv4Addr::new(192, 0, 2, 10)]);
        let removed = BTreeSet::from([Ipv4Addr::LOCALHOST]);
        let mut first_change = None;
        compare_local_ipv4_sample(&initial, &removed, &mut first_change);
        let evidence = local_ipv4_monitor_evidence(LocalIpv4MonitorResult {
            samples_completed: 3,
            final_addresses: initial.clone(),
            first_change,
            error: None,
        });
        assert!(evidence.change_detected);
        assert_eq!(
            evidence.first_change_old_addresses,
            ["127.0.0.1", "192.0.2.10"]
        );
        assert_eq!(evidence.first_change_new_addresses, ["127.0.0.1"]);
        assert_eq!(evidence.final_addresses, ["127.0.0.1", "192.0.2.10"]);
        assert_eq!(evidence.sample_interval_ms, LOCAL_IPV4_SAMPLE_INTERVAL_MS);
        const { assert!(LOCAL_IPV4_SAMPLE_INTERVAL_MS <= 1_000) };

        let stable = local_ipv4_monitor_evidence(LocalIpv4MonitorResult {
            samples_completed: 2,
            final_addresses: initial,
            first_change: None,
            error: None,
        });
        assert!(!stable.change_detected);
        assert!(stable.first_change_old_addresses.is_empty());
        assert!(stable.first_change_new_addresses.is_empty());
    }
}

#[cfg(target_os = "macos")]
fn run_profile_write_probe(guard: &OfflineGuard, fixture: &Path) -> Result<std::process::Output> {
    let argv = vec![
        fixture.to_string_lossy().into_owned(),
        "profile-write-probe".to_owned(),
        "--path".to_owned(),
        guard.profile.to_string_lossy().into_owned(),
    ];
    let wrapped = guard.wrap_argv(&argv)?;
    let (program, arguments) = wrapped
        .split_first()
        .ok_or_else(|| AhrbError::Protocol("profile-write probe command disappeared".to_owned()))?;
    Ok(Command::new(program)
        .args(arguments)
        .stdin(Stdio::null())
        .output()?)
}

#[cfg(target_os = "macos")]
fn fixture_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let parent = current.parent().ok_or_else(|| {
        AhrbError::Protocol("AHRB executable has no parent for row-62 fixture".to_owned())
    })?;
    let mut candidates = vec![parent.join("ahrb-fixture")];
    if parent.file_name().and_then(|name| name.to_str()) == Some("deps")
        && let Some(target) = parent.parent()
    {
        candidates.push(target.join("ahrb-fixture"));
    }
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(std::fs::canonicalize(candidate)?);
        }
    }
    Err(AhrbError::Protocol(
        "row-62 probe fixture is not installed beside AHRB".to_owned(),
    ))
}

#[cfg(target_os = "macos")]
fn hash_file(path: &Path) -> Result<String> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(AhrbError::Protocol(format!(
            "offline guard executable is not a regular file: {}",
            path.display()
        )));
    }
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}

#[cfg(target_os = "macos")]
fn verify_profile_hash(path: &Path, expected: &str) -> Result<()> {
    let actual = hash_file(path)?;
    if actual == expected {
        Ok(())
    } else {
        Err(AhrbError::Protocol(format!(
            "offline guard profile hash mismatch before launch: expected {expected}, found {actual}"
        )))
    }
}

#[cfg(target_os = "macos")]
fn write_profile(path: &Path, rendered: &[u8]) -> Result<String> {
    use std::io::Write as _;
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path).map_err(|error| {
        AhrbError::Protocol(format!(
            "create fresh offline guard profile {}: {error}",
            path.display()
        ))
    })?;
    file.write_all(rendered)?;
    file.sync_all()?;
    Ok(format!("{:x}", Sha256::digest(rendered)))
}

fn parse_loopback_http(base_url: &str) -> Result<(String, u16)> {
    let authority = base_url
        .strip_prefix("http://")
        .and_then(|rest| rest.split('/').next())
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "offline guard requires an injected literal HTTP loopback provider, got {base_url:?}"
            ))
        })?;
    let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
        AhrbError::Protocol(format!(
            "offline guard provider lacks an explicit port: {base_url:?}"
        ))
    })?;
    if host != "127.0.0.1" {
        return Err(AhrbError::Protocol(format!(
            "offline guard provider is not literal loopback: {base_url:?}"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        AhrbError::Protocol(format!(
            "offline guard provider has an invalid port: {base_url:?}"
        ))
    })?;
    Ok(("127.0.0.1".to_owned(), port))
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Sha256;

    #[test]
    fn loopback_provider_parser_is_fail_closed() {
        assert_eq!(
            parse_loopback_http("http://127.0.0.1:43123/v1").unwrap(),
            ("127.0.0.1".to_owned(), 43123)
        );
        for invalid in [
            "https://127.0.0.1:43123",
            "http://192.0.2.1:43123",
            "http://localhost:43123",
            "http://127.0.0.1",
            "http://127.0.0.1:not-a-port",
        ] {
            assert!(parse_loopback_http(invalid).is_err(), "{invalid}");
        }
    }

    /// Injected delivery-probe outcomes: only an arrival recorded by AHRB's
    /// sentinel for the probe's exact source, or a Seatbelt denial, proves
    /// the boundary; everything else names the address and interface.
    #[test]
    fn injected_local_delivery_outcomes_fail_closed_with_address_and_interface() {
        let address = Ipv4Addr::new(198, 18, 0, 1);
        let utun = BTreeSet::from(["utun4".to_owned()]);
        let local = SocketAddrV4::new(address, 50_001);
        let connected = LocalDeliveryConnect::Connected { local: Some(local) };

        let owned = classify_local_delivery(address, &utun, 43123, connected.clone(), true);
        assert_eq!(owned.outcome, LOCAL_DELIVERY_OWNED);
        assert!(owned.sentinel_recorded && owned.proves_boundary());
        assert_eq!(owned.destination, "198.18.0.1:43123");
        assert_eq!(
            owned.probe_local_address.as_deref(),
            Some("198.18.0.1:50001")
        );
        assert_eq!(owned.interfaces, ["utun4"]);
        assert_eq!(local_delivery_failure(std::slice::from_ref(&owned)), None);

        let other = classify_local_delivery(address, &utun, 43123, connected, false);
        assert_eq!(other.outcome, LOCAL_DELIVERY_ANSWERED_BY_OTHER);
        assert!(!other.proves_boundary());
        let reason = local_delivery_failure(&[owned.clone(), other]).expect("tunnel answered");
        assert!(
            reason.starts_with(
                "guard boundary includes a local address AHRB does not own: 198.18.0.1 (utun4) answered-by-other: "
            ),
            "{reason}"
        );
        assert!(reason.contains("from 198.18.0.1:50001"), "{reason}");

        // A sentinel record is never credited without a connected source.
        for (connect, outcome) in [
            (LocalDeliveryConnect::Refused, LOCAL_DELIVERY_REFUSED),
            (LocalDeliveryConnect::TimedOut, LOCAL_DELIVERY_TIMEOUT),
            (
                LocalDeliveryConnect::Connected { local: None },
                LOCAL_DELIVERY_AMBIGUOUS,
            ),
            (
                LocalDeliveryConnect::Failed("unparseable".to_owned()),
                LOCAL_DELIVERY_AMBIGUOUS,
            ),
        ] {
            let probe = classify_local_delivery(address, &utun, 43123, connect, true);
            assert_eq!(probe.outcome, outcome);
            assert!(
                !probe.sentinel_recorded && !probe.proves_boundary(),
                "{outcome}"
            );
            let reason = local_delivery_failure(&[probe]).expect("must fail closed");
            assert!(
                reason.contains(&format!("198.18.0.1 (utun4) {outcome}: ")),
                "{reason}"
            );
        }

        // Seatbelt denying the address leaves it outside the permitted set.
        let blocked =
            classify_local_delivery(address, &utun, 43123, LocalDeliveryConnect::Blocked, false);
        assert_eq!(blocked.outcome, LOCAL_DELIVERY_BLOCKED);
        assert!(blocked.proves_boundary());

        let unnamed = classify_local_delivery(
            address,
            &BTreeSet::new(),
            43123,
            LocalDeliveryConnect::TimedOut,
            false,
        );
        assert!(
            local_delivery_failure(&[unnamed])
                .is_some_and(|reason| reason.contains("198.18.0.1 (unknown interface) timeout"))
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn delivery_probe_output_is_parsed_fail_closed() {
        assert_eq!(
            parse_delivery_connect(
                Some(4),
                br#"{"blocked":false,"connected":true,"address":"198.18.0.1:43123","local":"198.18.0.1:50001"}"#
            ),
            LocalDeliveryConnect::Connected {
                local: Some("198.18.0.1:50001".parse().unwrap())
            }
        );
        assert_eq!(
            parse_delivery_connect(Some(4), br#"{"connected":true,"local":null}"#),
            LocalDeliveryConnect::Connected { local: None }
        );
        assert_eq!(
            parse_delivery_connect(Some(0), br#"{"blocked":true,"errno":1}"#),
            LocalDeliveryConnect::Blocked
        );
        assert_eq!(
            parse_delivery_connect(
                Some(3),
                br#"{"blocked":false,"errno":61,"kind":"ConnectionRefused"}"#
            ),
            LocalDeliveryConnect::Refused
        );
        assert_eq!(
            parse_delivery_connect(
                Some(3),
                br#"{"blocked":false,"errno":null,"kind":"TimedOut"}"#
            ),
            LocalDeliveryConnect::TimedOut
        );
        assert!(matches!(
            parse_delivery_connect(
                Some(3),
                br#"{"blocked":false,"errno":65,"kind":"HostUnreachable"}"#
            ),
            LocalDeliveryConnect::Failed(_)
        ));
        assert!(matches!(
            parse_delivery_connect(Some(0), b"not json"),
            LocalDeliveryConnect::Failed(_)
        ));
        assert!(matches!(
            parse_delivery_connect(None, b""),
            LocalDeliveryConnect::Failed(_)
        ));
    }

    #[test]
    fn profile_hash_binds_exact_rendered_bytes() {
        let first = <Sha256 as sha2::Digest>::digest(b"profile-a");
        let second = <Sha256 as sha2::Digest>::digest(b"profile-b");
        assert_ne!(first, second);
    }

    #[test]
    fn row62_launch_inventory_includes_every_driver_process_surface() {
        let mut manifest = crate::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))
            .expect("load Haider manifest");
        let mut names = Vec::new();
        visit_row62_launch_commands_mut(&mut manifest, |name, _| names.push(name));
        assert_eq!(
            names,
            vec![
                "transport.command",
                "daemon.start",
                "daemon.initialize",
                "daemon.readiness.command",
                "daemon.shutdown",
                "sessions.continue_turn",
                "sessions.resume",
                "sessions.resume_control",
                "sessions.recover_probe",
                "sessions.close_delete",
                "sessions.wait_ready",
                "concurrency.release",
                "agents.cancel",
                "events.replay_command",
                "events.replay_state_command",
            ]
        );
    }

    #[test]
    fn row62_launch_validation_rejects_any_unwrapped_manifest_command() {
        let mut manifest = crate::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))
            .expect("load Haider manifest");
        visit_row62_launch_commands_mut(&mut manifest, |name, argv| {
            if !argv.is_empty() && name != "daemon.readiness.command" {
                argv.insert(0, "guard".to_owned());
            }
        });
        let error = validate_row62_launch_commands(&manifest, |argv| {
            argv.first().is_some_and(|program| program == "guard")
        })
        .expect_err("one unwrapped readiness command must fail closed");
        assert!(error.to_string().contains("daemon.readiness.command"));
        manifest
            .daemon
            .readiness
            .command
            .insert(0, "guard".to_owned());
        validate_row62_launch_commands(&manifest, |argv| {
            argv.first().is_some_and(|program| program == "guard")
        })
        .expect("all inventoried commands are wrapped");
    }
}
