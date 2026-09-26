//! Whole-process-tree ownership discovery and sampling.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Mutex, Once, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// PID plus process start time, safe against PID reuse.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProcIdentity {
    /// Operating-system process ID.
    pub pid: u32,
    /// Platform start-time ticks or microseconds.
    pub start_time: u64,
}

/// Why AHRB owns a process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProcOwnership {
    /// Launcher or verified daemon root.
    DeclaredRoot,
    /// Descendant of an owned process.
    Descendant,
    /// Durable cgroup member on Linux.
    CgroupMember,
    /// Member of the launcher's isolated process group, retained across ordinary
    /// parent exit and reparenting.
    ProcessGroupMember,
    /// Verified reparented process matching adapter evidence.
    Reparented,
    /// Executable and argv identify a disposable profile created by this run.
    ProfilePath,
    /// Holds an adapter-declared lock inside a disposable profile of this run.
    LockHolder,
}

/// One process observed before the teardown signal and its final disposition.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TeardownProcess {
    /// Stable identity used for every signal and liveness check.
    #[serde(flatten)]
    pub identity: ProcIdentity,
    /// Executable basename observed with the identity.
    pub command: String,
    /// Parent observed before teardown; a value of one is valid owned residue.
    pub ppid: u32,
    /// Why AHRB attributed the process to this run.
    pub ownership: ProcOwnership,
}

/// Bounded TERM-to-KILL cleanup evidence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProcessTeardownEvidence {
    /// Live identities observed before the first teardown signal.
    pub observed: Vec<TeardownProcess>,
    /// Observed identities gone after TERM or KILL.
    pub reaped: Vec<ProcIdentity>,
    /// Observed identities still live after the bounded cleanup.
    pub survivors: Vec<ProcIdentity>,
}

/// Result of `fcntl(F_GETLK)` for one adapter-declared profile lock.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case", tag = "outcome")]
pub enum ProfileLockAudit {
    /// The harness never created the declared lock file.
    Absent {
        profile: PathBuf,
        lock_path: PathBuf,
    },
    /// No other process holds a conflicting write lock.
    Unlocked {
        profile: PathBuf,
        lock_path: PathBuf,
    },
    /// A conflicting lock remains. `holder_pid` is the kernel-reported owner;
    /// it is `None` when `F_GETLK` cannot attribute the lock to a process
    /// (macOS reports `l_pid = -1` for `flock(2)`/OFD-style locks). Such a lock
    /// is still held and its holder must be identified by other means.
    Held {
        profile: PathBuf,
        lock_path: PathBuf,
        holder_pid: Option<u32>,
    },
}

impl ProfileLockAudit {
    /// Lock file this audit refers to.
    pub fn lock_path(&self) -> &Path {
        match self {
            Self::Absent { lock_path, .. }
            | Self::Unlocked { lock_path, .. }
            | Self::Held { lock_path, .. } => lock_path,
        }
    }

    /// Disposable profile that declared the lock.
    pub fn profile(&self) -> &Path {
        match self {
            Self::Absent { profile, .. }
            | Self::Unlocked { profile, .. }
            | Self::Held { profile, .. } => profile,
        }
    }
}

/// One process in an owned tree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessInfo {
    /// Stable identity.
    pub identity: ProcIdentity,
    /// Parent PID observed with this sample.
    pub ppid: u32,
    /// Executable basename when available.
    pub command: String,
    /// Ownership evidence.
    pub ownership: ProcOwnership,
}

/// Timestamped resource counters for one owned process identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProcessSample {
    /// Monotonic nanoseconds since sampler start; row-46 process records use
    /// absolute CLOCK_MONOTONIC nanoseconds to match memory-time-samples.jsonl.
    pub elapsed_ns: u64,
    /// Wall-clock timestamp for evidence correlation.
    pub wall_time: SystemTime,
    /// Workflow phase label.
    pub phase: String,
    /// Process membership and ownership evidence.
    #[serde(flatten)]
    pub process: ProcessInfo,
    /// Resident bytes for this process.
    pub rss_bytes: u64,
    /// Proportional-set bytes for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pss_bytes: Option<u64>,
    /// Private bytes for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_bytes: Option<u64>,
    /// Physical footprint for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footprint_bytes: Option<u64>,
    /// Resident-byte cross-check for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss_crosscheck_bytes: Option<u64>,
    /// Cumulative user plus system CPU nanoseconds for this process.
    pub cpu_ns: u64,
    /// Open file descriptors for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_fds: Option<u64>,
    /// Live threads for this process, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_count: Option<u64>,
}

/// One out-of-band read of cumulative disk-write counters for an owned tree.
///
/// `expected_identities` is kept separately from `write_bytes_by_identity` so
/// a process that disappears between discovery and counter collection cannot
/// be mistaken for a process that wrote zero bytes.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProcessDiskObservation {
    /// Identities attributed to the owned tree at the discovery boundary.
    pub expected_identities: BTreeSet<ProcIdentity>,
    /// Successfully read per-process cumulative bytes-written counters.
    pub write_bytes_by_identity: BTreeMap<ProcIdentity, u64>,
    /// Linux cgroup-v2 cumulative `wbytes`, when a dedicated cgroup exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_write_bytes: Option<u64>,
}

/// How one identity's disk counter is accounted at a boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiskIdentityStatus {
    /// The process is live and its cumulative counter was read at this boundary.
    Live,
    /// A discovered live identity had no readable counter at this boundary.
    CounterUnavailable,
    /// The identity disappeared without durable retirement evidence.
    MissingWithoutRetirementEvidence,
    /// Its structured terminal was seen, but no later pre-reap sample was recorded.
    TerminalAwaitingFinalSample,
    /// A final sample was recorded after the structured terminal and before reap.
    FinalSampleBeforeReap,
    /// The identity was explicitly retired after its terminal-before-reap sample.
    RetiredAfterFinalSample,
    /// A post-quiet durable cgroup counter accounts for the retired identity.
    RetiredByDurableCgroup,
}

/// Per-identity evidence emitted by [`TreeDiskTracker`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DiskIdentityEvidence {
    /// Stable process identity.
    pub identity: ProcIdentity,
    /// Last cumulative process counter, absent if it was never readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_bytes: Option<u64>,
    /// Explicit accounting state at this boundary.
    pub status: DiskIdentityStatus,
}

/// Cumulative disk-accounting state for an owned tree.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TreeDiskSnapshot {
    /// Authoritative cumulative bytes when every identity is completely accounted.
    ///
    /// This is deliberately absent, rather than a favorable partial value, when
    /// any identity lacks a current counter or valid retirement evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cumulative_write_bytes: Option<u64>,
    /// Best observed aggregate, retained only for diagnostics when incomplete.
    pub observed_write_bytes: u64,
    /// Whether all live and retired identity counters are complete.
    pub counter_complete: bool,
    /// Identities preventing complete accounting.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incomplete_identities: Vec<ProcIdentity>,
    /// Deterministically ordered per-identity evidence.
    pub identities: Vec<DiskIdentityEvidence>,
    /// Latest cumulative cgroup value, when exposed by Linux.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_write_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiskRetirement {
    TerminalFinalSample,
    DurableCgroup,
}

#[derive(Clone, Copy, Debug, Default)]
struct DiskIdentityState {
    write_bytes: Option<u64>,
    present: bool,
    expected: bool,
    terminal_observed: bool,
    final_sample_observed: bool,
    retirement: Option<DiskRetirement>,
}

/// Monotonic disk accounting across owned-process lifecycles.
///
/// Merely retaining the last poll for a disappeared process never completes
/// retirement. The caller must either record the ordered structured-terminal,
/// final-sample, and retirement operations, or explicitly retire missing
/// identities using a durable cgroup counter captured after membership is
/// quiet.
#[derive(Debug, Default)]
pub struct TreeDiskTracker {
    identities: BTreeMap<ProcIdentity, DiskIdentityState>,
    cgroup_write_bytes: Option<u64>,
}

impl TreeDiskTracker {
    /// Incorporate one live-tree counter observation.
    pub fn observe(&mut self, observation: &ProcessDiskObservation) -> Result<TreeDiskSnapshot> {
        if let (Some(previous), Some(current)) =
            (self.cgroup_write_bytes, observation.cgroup_write_bytes)
            && current < previous
        {
            return Err(AhrbError::Protocol(format!(
                "cgroup cumulative disk writes regressed from {previous} to {current}"
            )));
        }
        if let Some(current) = observation.cgroup_write_bytes {
            self.cgroup_write_bytes = Some(current);
        }

        for state in self.identities.values_mut() {
            state.present = false;
            state.expected = false;
        }
        for identity in &observation.expected_identities {
            let state = self.identities.entry(*identity).or_default();
            state.expected = true;
        }
        for (identity, write_bytes) in &observation.write_bytes_by_identity {
            if !observation.expected_identities.contains(identity) {
                return Err(AhrbError::Protocol(format!(
                    "disk counter observed unexpected process identity ({},{})",
                    identity.pid, identity.start_time
                )));
            }
            let state = self.identities.entry(*identity).or_default();
            if state.retirement.is_some() {
                return Err(AhrbError::Protocol(format!(
                    "retired disk identity ({},{}) was observed again",
                    identity.pid, identity.start_time
                )));
            }
            update_disk_counter(*identity, state, *write_bytes)?;
            state.present = true;
            state.expected = true;
        }
        Ok(self.snapshot())
    }

    /// Record that the harness observed the process's structured terminal.
    pub fn note_structured_terminal(&mut self, identity: ProcIdentity) -> Result<()> {
        let state = self.identities.get_mut(&identity).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "structured terminal references unknown disk identity ({},{})",
                identity.pid, identity.start_time
            ))
        })?;
        if state.retirement.is_some() {
            return Err(AhrbError::Protocol(format!(
                "structured terminal references retired disk identity ({},{})",
                identity.pid, identity.start_time
            )));
        }
        state.terminal_observed = true;
        state.final_sample_observed = false;
        Ok(())
    }

    /// Record the cumulative process counter sampled after its structured
    /// terminal and before the process was reaped.
    pub fn record_final_sample_before_reap(
        &mut self,
        identity: ProcIdentity,
        write_bytes: u64,
    ) -> Result<()> {
        let state = self.identities.get_mut(&identity).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "final disk sample references unknown identity ({},{})",
                identity.pid, identity.start_time
            ))
        })?;
        if !state.terminal_observed {
            return Err(AhrbError::Protocol(format!(
                "final disk sample for ({},{}) preceded its structured terminal",
                identity.pid, identity.start_time
            )));
        }
        if state.retirement.is_some() {
            return Err(AhrbError::Protocol(format!(
                "final disk sample references retired identity ({},{})",
                identity.pid, identity.start_time
            )));
        }
        update_disk_counter(identity, state, write_bytes)?;
        state.present = true;
        state.final_sample_observed = true;
        Ok(())
    }

    /// Retire an identity whose post-terminal final sample was captured before reap.
    pub fn retire_after_final_sample(&mut self, identity: ProcIdentity) -> Result<()> {
        let state = self.identities.get_mut(&identity).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "disk retirement references unknown identity ({},{})",
                identity.pid, identity.start_time
            ))
        })?;
        if !state.terminal_observed || !state.final_sample_observed {
            return Err(AhrbError::Protocol(format!(
                "disk identity ({},{}) lacks a terminal-before-reap final sample",
                identity.pid, identity.start_time
            )));
        }
        state.present = false;
        state.expected = false;
        state.retirement = Some(DiskRetirement::TerminalFinalSample);
        Ok(())
    }

    /// Retire missing identities using a cumulative cgroup `io.stat` value
    /// captured after cgroup membership became quiet.
    pub fn retire_with_cgroup_after_quiet(
        &mut self,
        identities: &BTreeSet<ProcIdentity>,
        cgroup_write_bytes: u64,
    ) -> Result<()> {
        if let Some(previous) = self.cgroup_write_bytes
            && cgroup_write_bytes < previous
        {
            return Err(AhrbError::Protocol(format!(
                "post-quiet cgroup disk writes regressed from {previous} to {cgroup_write_bytes}"
            )));
        }
        for identity in identities {
            let state = self.identities.get_mut(identity).ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "cgroup disk retirement references unknown identity ({},{})",
                    identity.pid, identity.start_time
                ))
            })?;
            if state.present {
                return Err(AhrbError::Protocol(format!(
                    "cgroup disk retirement identity ({},{}) is still present",
                    identity.pid, identity.start_time
                )));
            }
            state.expected = false;
            state.retirement = Some(DiskRetirement::DurableCgroup);
        }
        self.cgroup_write_bytes = Some(cgroup_write_bytes);
        Ok(())
    }

    /// Return current accounting evidence without changing tracker state.
    pub fn snapshot(&self) -> TreeDiskSnapshot {
        let identities = self
            .identities
            .iter()
            .map(|(identity, state)| DiskIdentityEvidence {
                identity: *identity,
                write_bytes: state.write_bytes,
                status: disk_identity_status(state),
            })
            .collect::<Vec<_>>();
        let incomplete_identities = identities
            .iter()
            .filter(|evidence| {
                matches!(
                    evidence.status,
                    DiskIdentityStatus::CounterUnavailable
                        | DiskIdentityStatus::MissingWithoutRetirementEvidence
                        | DiskIdentityStatus::TerminalAwaitingFinalSample
                )
            })
            .map(|evidence| evidence.identity)
            .collect::<Vec<_>>();
        let counter_complete = incomplete_identities.is_empty();
        let per_process_total = self.identities.values().fold(0_u64, |total, state| {
            // The repository forbids unwrap-style operations in production code.
            #[allow(
                clippy::manual_unwrap_or,
                clippy::manual_unwrap_or_default,
                reason = "production process accounting intentionally avoids unwrap-style APIs"
            )]
            let write_bytes = match state.write_bytes {
                Some(value) => value,
                None => 0,
            };
            total.saturating_add(write_bytes)
        });
        let observed_write_bytes = match self.cgroup_write_bytes {
            Some(value) => value,
            None => per_process_total,
        };
        TreeDiskSnapshot {
            cumulative_write_bytes: counter_complete.then_some(observed_write_bytes),
            observed_write_bytes,
            counter_complete,
            incomplete_identities,
            identities,
            cgroup_write_bytes: self.cgroup_write_bytes,
        }
    }
}

fn update_disk_counter(
    identity: ProcIdentity,
    state: &mut DiskIdentityState,
    write_bytes: u64,
) -> Result<()> {
    if let Some(previous) = state.write_bytes
        && write_bytes < previous
    {
        return Err(AhrbError::Protocol(format!(
            "process ({},{}) cumulative disk writes regressed from {previous} to {write_bytes}",
            identity.pid, identity.start_time
        )));
    }
    state.write_bytes = Some(write_bytes);
    Ok(())
}

fn disk_identity_status(state: &DiskIdentityState) -> DiskIdentityStatus {
    match state.retirement {
        Some(DiskRetirement::TerminalFinalSample) => DiskIdentityStatus::RetiredAfterFinalSample,
        Some(DiskRetirement::DurableCgroup) => DiskIdentityStatus::RetiredByDurableCgroup,
        None if state.terminal_observed && state.final_sample_observed => {
            DiskIdentityStatus::FinalSampleBeforeReap
        }
        None if state.terminal_observed => DiskIdentityStatus::TerminalAwaitingFinalSample,
        None if state.expected && state.present && state.write_bytes.is_some() => {
            DiskIdentityStatus::Live
        }
        None if state.expected => DiskIdentityStatus::CounterUnavailable,
        None => DiskIdentityStatus::MissingWithoutRetirementEvidence,
    }
}

/// Complete process membership at one instant.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProcessTree {
    /// Verified roots.
    pub roots: BTreeSet<ProcIdentity>,
    /// Owned processes keyed by stable identity.
    pub members: BTreeMap<ProcIdentity, ProcessInfo>,
}

#[derive(Clone, Debug, Default)]
struct OwnedRegistry {
    groups: BTreeMap<u32, ProcIdentity>,
    observed: BTreeSet<ProcIdentity>,
    identity_groups: BTreeMap<ProcIdentity, u32>,
    trees: BTreeMap<ProcIdentity, BTreeSet<ProcIdentity>>,
}

static OWNED_REGISTRY: OnceLock<Mutex<OwnedRegistry>> = OnceLock::new();
static CLEANUP_INSTALL: Once = Once::new();
static CLEANUP_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static CLEANUP_REQUESTED: AtomicBool = AtomicBool::new(false);
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);
const SIGNAL_TARGET_CAPACITY: usize = 4_096;
static SIGNAL_GROUPS: [AtomicI32; SIGNAL_TARGET_CAPACITY] =
    [const { AtomicI32::new(0) }; SIGNAL_TARGET_CAPACITY];
static SIGNAL_PIDS: [AtomicI32; SIGNAL_TARGET_CAPACITY] =
    [const { AtomicI32::new(0) }; SIGNAL_TARGET_CAPACITY];

fn record_signal_target(targets: &[AtomicI32; SIGNAL_TARGET_CAPACITY], value: u32) {
    let Ok(value) = i32::try_from(value) else {
        return;
    };
    if targets
        .iter()
        .any(|target| target.load(Ordering::Relaxed) == value)
    {
        return;
    }
    if let Some(target) = targets
        .iter()
        .find(|target| target.load(Ordering::Relaxed) == 0)
    {
        target.store(value, Ordering::Release);
    }
}

fn clear_signal_target(targets: &[AtomicI32; SIGNAL_TARGET_CAPACITY], value: u32) {
    let Ok(value) = i32::try_from(value) else {
        return;
    };
    for target in targets {
        if target.load(Ordering::Acquire) == value {
            target.store(0, Ordering::Release);
        }
    }
}

fn owned_registry() -> &'static Mutex<OwnedRegistry> {
    OWNED_REGISTRY.get_or_init(|| Mutex::new(OwnedRegistry::default()))
}

fn registry_lock() -> Result<std::sync::MutexGuard<'static, OwnedRegistry>> {
    owned_registry()
        .lock()
        .map_err(|_| AhrbError::Protocol("owned-process registry lock was poisoned".to_owned()))
}

fn register_process_identity(identity: ProcIdentity, process_group: u32) -> Result<()> {
    let pid = identity.pid;
    if process_group == 0 {
        return Err(AhrbError::Protocol(format!(
            "spawned process PID {pid} has no process group"
        )));
    }
    #[cfg(unix)]
    {
        // SAFETY: `getpgrp` has no preconditions and does not mutate state.
        let parent_group = unsafe { libc::getpgrp() };
        if u32::try_from(parent_group).ok() == Some(process_group) {
            return Err(AhrbError::Protocol(format!(
                "refusing to own process PID {pid} in AHRB's own process group {process_group}"
            )));
        }
    }
    let mut registry = registry_lock()?;
    registry.groups.entry(process_group).or_insert(identity);
    registry.observed.insert(identity);
    registry.identity_groups.insert(identity, process_group);
    registry.trees.entry(identity).or_default().insert(identity);
    record_signal_target(&SIGNAL_GROUPS, process_group);
    record_signal_target(&SIGNAL_PIDS, identity.pid);
    Ok(())
}

/// Register a newly spawned process and the isolated process group it leads or joins.
pub fn register_process(pid: u32) -> Result<()> {
    // A very short-lived child may finish between `spawn()` and this lookup.
    // It no longer has an owned tree to track, so treat that race as a
    // successful registration; externally launched roots use the strict
    // `register_external_process` path below and still report disappearance.
    for _ in 0..5 {
        if let Some((identity, process_group)) = process_identity_and_group(pid)? {
            return register_process_identity(identity, process_group);
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    // A very short-lived child may have been reaped before any lookup could
    // observe it. There is then no safe identity or group to retain.
    Ok(())
}

/// Resolve and register an externally launched root, returning its stable
/// `(pid,start_time)` identity for later PID-reuse-safe lifecycle operations.
pub fn register_external_process(pid: u32) -> Result<ProcIdentity> {
    let Some((identity, process_group)) = process_identity_and_group(pid)? else {
        return Err(AhrbError::Protocol(format!(
            "external process PID {pid} disappeared before ownership registration"
        )));
    };
    register_process_identity(identity, process_group)?;
    Ok(identity)
}

/// Signal the registered group and every sampler-attributed member of one
/// external root. Every direct PID is start-time revalidated before signaling.
#[cfg(unix)]
pub(crate) fn signal_registered_tree(identity: ProcIdentity, signal: i32) -> Result<()> {
    let snapshot = registry_lock()?.clone();
    let process_group = snapshot
        .identity_groups
        .get(&identity)
        .copied()
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "no registered process group for external PID {}",
                identity.pid
            ))
        })?;
    if let Some(leader) = snapshot.groups.get(&process_group)
        && !verified_group_members(&snapshot, process_group, *leader)?.is_empty()
    {
        signal_group(process_group, signal)?;
    }
    if let Some(members) = snapshot.trees.get(&identity) {
        for member in members {
            signal_identity(*member, signal)?;
        }
    }
    Ok(())
}

/// Deliver a benchmark stimulus signal to a live registered owned tree.
///
/// Unlike cleanup signaling, disappearance is an evidence failure: callers use
/// successful return as the external delivery boundary for signal semantics.
#[cfg(unix)]
pub(crate) fn deliver_registered_tree_signal(identity: ProcIdentity, signal: i32) -> Result<()> {
    let snapshot = registry_lock()?.clone();
    let process_group = snapshot
        .identity_groups
        .get(&identity)
        .copied()
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "no registered process group for signal target PID {}",
                identity.pid
            ))
        })?;
    let leader = snapshot
        .groups
        .get(&process_group)
        .copied()
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "registered process group {process_group} has no owned leader"
            ))
        })?;
    let group_members = verified_group_members(&snapshot, process_group, leader)?;
    if group_members.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "owned process group {process_group} disappeared before signal delivery"
        )));
    }
    let group_target = i32::try_from(process_group)
        .map_err(|_| AhrbError::Protocol("owned process group exceeds pid_t range".to_owned()))?;
    // SAFETY: the group leader and membership were revalidated immediately above.
    if unsafe { libc::kill(-group_target, signal) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if let Some(members) = snapshot.trees.get(&identity) {
        for member in members {
            let Some((current, current_group)) = process_identity_and_group(member.pid)? else {
                continue;
            };
            if current != *member || current_group == process_group {
                continue;
            }
            let pid = i32::try_from(member.pid)
                .map_err(|_| AhrbError::Protocol("owned PID exceeds pid_t range".to_owned()))?;
            // SAFETY: the stable PID identity was revalidated immediately above.
            if unsafe { libc::kill(pid, signal) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    Ok(())
}

pub(crate) fn registered_tree_is_live(identity: ProcIdentity) -> Result<bool> {
    let snapshot = registry_lock()?.clone();
    let group_live = snapshot
        .identity_groups
        .get(&identity)
        .and_then(|group| snapshot.groups.get(group).map(|leader| (*group, *leader)))
        .map(|(group, leader)| verified_group_members(&snapshot, group, leader))
        .transpose()?
        .is_some_and(|members| !members.is_empty());
    let member_live = snapshot
        .trees
        .get(&identity)
        .is_some_and(|members| members.iter().copied().any(identity_is_live));
    Ok(group_live || member_live)
}

/// Register a Tokio child immediately after `spawn`.
pub fn register_child(child: &tokio::process::Child) -> Result<()> {
    let pid = child
        .id()
        .ok_or_else(|| AhrbError::Protocol("spawned child has no process ID".to_owned()))?;
    register_process(pid)
}

/// Retire a reaped direct child while preserving any live descendants that
/// still occupy its owned process group.
pub fn retire_process(pid: u32) -> Result<()> {
    let mut registry = registry_lock()?;
    let groups = registry
        .groups
        .iter()
        .filter_map(|(process_group, leader)| {
            (leader.pid == pid).then_some((*process_group, *leader))
        })
        .collect::<Vec<_>>();
    for (process_group, leader) in groups {
        let members = process_group_members(process_group, leader.start_time)?;
        if members.is_empty() {
            registry.groups.remove(&process_group);
            clear_signal_target(&SIGNAL_GROUPS, process_group);
        } else {
            registry.observed.extend(members.iter().copied());
            for member in members {
                record_signal_target(&SIGNAL_PIDS, member.pid);
            }
        }
    }
    registry.observed.retain(|identity| {
        let keep = identity.pid != pid || identity_is_live(*identity);
        if !keep {
            clear_signal_target(&SIGNAL_PIDS, identity.pid);
        }
        keep
    });
    Ok(())
}

/// Run a synchronous helper in an isolated, registered process group while
/// capturing the same stdout/stderr bundle as `Command::output`.
pub fn owned_command_output(command: &mut std::process::Command) -> Result<std::process::Output> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = command.spawn()?;
    let pid = child.id();
    register_process(pid)?;
    let output = child.wait_with_output()?;
    retire_process(pid)?;
    Ok(output)
}

/// Add sampler-confirmed descendants to the final owned-tree sweep.
pub fn track_process_tree(tree: &ProcessTree) -> Result<()> {
    if tree.members.is_empty() {
        return Ok(());
    }
    let mut registry = registry_lock()?;
    registry.observed.extend(tree.members.keys().copied());
    for root in &tree.roots {
        registry
            .trees
            .entry(*root)
            .or_default()
            .extend(tree.members.keys().copied());
    }
    for identity in tree.members.keys() {
        record_signal_target(&SIGNAL_PIDS, identity.pid);
    }
    Ok(())
}

fn argument_names_profile(argument: &str, profile_root: &Path) -> bool {
    let root = profile_root.to_string_lossy();
    if root.is_empty() {
        return false;
    }
    let matches = |candidate: &str| {
        candidate == root
            || candidate
                .strip_prefix(root.as_ref())
                .is_some_and(|suffix| suffix.starts_with('/'))
    };
    matches(argument)
        || argument
            .split_once('=')
            .is_some_and(|(flag, value)| flag.starts_with('-') && matches(value))
}

pub(crate) fn argv_names_profile(arguments: &[String], profile_roots: &[PathBuf]) -> bool {
    arguments.iter().any(|argument| {
        profile_roots
            .iter()
            .any(|root| argument_names_profile(argument, root))
    })
}

/// Spellings under which a process may name `profile_root` in its argv. A
/// harness commonly canonicalizes its store path, and macOS temporary roots
/// are symlinks (`/tmp` -> `/private/tmp`, `/var` -> `/private/var`), so the
/// lexical root AHRB created and the resolved root are both ownership keys.
pub(crate) fn profile_root_aliases(profile_root: &Path) -> Vec<PathBuf> {
    let mut aliases = vec![profile_root.to_path_buf()];
    // Resolve the longest existing ancestor so a profile that was already
    // removed (or not yet created) still maps to its resolved spelling.
    let resolved = profile_root.ancestors().find_map(|ancestor| {
        let resolved = std::fs::canonicalize(ancestor).ok()?;
        let rest = profile_root.strip_prefix(ancestor).ok()?;
        Some(if rest.as_os_str().is_empty() {
            resolved
        } else {
            resolved.join(rest)
        })
    });
    if let Some(resolved) = resolved
        && !aliases.contains(&resolved)
    {
        aliases.push(resolved);
    }
    aliases
}

#[cfg(test)]
mod profile_ownership_tests {
    use super::*;

    #[test]
    fn argv_profile_match_requires_a_path_boundary() {
        let root = &[PathBuf::from("/tmp/ahrb-run/dr57/sigint2-r1")];
        assert!(argv_names_profile(
            &[
                "--store-dir".into(),
                "/tmp/ahrb-run/dr57/sigint2-r1/home".into()
            ],
            root,
        ));
        assert!(argv_names_profile(
            &["--store-dir=/tmp/ahrb-run/dr57/sigint2-r1/home".into()],
            root,
        ));
        assert!(!argv_names_profile(
            &["/tmp/ahrb-run/dr57/sigint2-r10/home".into()],
            root,
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn profile_aliases_include_the_resolved_temporary_root() {
        // Haider passes `/private/tmp/...` to its daemon when AHRB created
        // `/tmp/...`; missing that alias hid the row-57 orphan.
        let root = Path::new("/tmp/ahrb-alias-probe-absent/dr57/sigint2-r1");
        let aliases = profile_root_aliases(root);
        assert_eq!(
            aliases,
            vec![
                root.to_path_buf(),
                PathBuf::from("/private/tmp/ahrb-alias-probe-absent/dr57/sigint2-r1"),
            ]
        );
        assert!(argv_names_profile(
            &[
                "--store-dir".into(),
                "/private/tmp/ahrb-alias-probe-absent/dr57/sigint2-r1/home/.haider/dev-profile"
                    .into()
            ],
            &aliases,
        ));
        assert!(!argv_names_profile(
            &["/private/tmp/ahrb-alias-probe-absent/dr57/sigint2-r10/home".into()],
            &aliases,
        ));
    }
}

/// Find live adapter processes whose executable and immutable argv identify a
/// disposable profile. This ownership source remains valid after reparenting
/// and process-group changes.
pub fn discover_profile_owned_processes(
    profile_root: &Path,
    executable_names: &[String],
) -> Result<Vec<ProcessInfo>> {
    if !profile_root.is_absolute() {
        return Err(AhrbError::Validation(format!(
            "profile-owned process discovery requires an absolute root: {}",
            profile_root.display()
        )));
    }
    if executable_names.is_empty() {
        return Ok(Vec::new());
    }
    let roots = profile_root_aliases(profile_root);
    #[cfg(target_os = "macos")]
    let mut processes = macos::profile_owned_processes(&roots, executable_names)?;
    #[cfg(target_os = "linux")]
    let mut processes = linux::profile_owned_processes(&roots, executable_names)?;
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let mut processes = return Err(AhrbError::Unsupported(
        "profile-owned process discovery is implemented only on macOS and Linux".to_owned(),
    ));
    processes.sort_by_key(|process| process.identity);
    processes.dedup_by_key(|process| process.identity);
    Ok(processes)
}

/// Register profile-attributed identities so process-wide emergency and final
/// cleanup paths retain them even when their original parent has exited.
pub fn track_profile_owned_processes(processes: &[ProcessInfo]) -> Result<()> {
    let mut registry = registry_lock()?;
    for process in processes {
        registry.observed.insert(process.identity);
        record_signal_target(&SIGNAL_PIDS, process.identity.pid);
    }
    Ok(())
}

/// Query a profile lock without acquiring it. `F_GETLK` reports conflicting
/// owners and does not disturb either the lock file or the owning process.
#[cfg(unix)]
pub fn audit_profile_lock(profile: &Path, lock_path: &Path) -> Result<ProfileLockAudit> {
    use std::os::fd::AsRawFd as _;
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProfileLockAudit::Absent {
                profile: profile.to_path_buf(),
                lock_path: lock_path.to_path_buf(),
            });
        }
        Err(error) => return Err(error.into()),
    };
    let mut lock = libc::flock {
        l_start: 0,
        l_len: 0,
        l_pid: 0,
        l_type: libc::F_WRLCK,
        l_whence: libc::SEEK_SET as i16,
    };
    // SAFETY: `file` remains open and `lock` is a valid writable `flock`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut lock) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if lock.l_type == libc::F_UNLCK {
        return Ok(ProfileLockAudit::Unlocked {
            profile: profile.to_path_buf(),
            lock_path: lock_path.to_path_buf(),
        });
    }
    // A positive `l_pid` names a POSIX record-lock owner. Zero or negative
    // means the conflicting lock is not process-associated (`flock(2)` or an
    // OFD lock); the lock is nonetheless held.
    let holder_pid = u32::try_from(lock.l_pid).ok().filter(|pid| *pid != 0);
    Ok(ProfileLockAudit::Held {
        profile: profile.to_path_buf(),
        lock_path: lock_path.to_path_buf(),
        holder_pid,
    })
}

/// Live processes that have `lock_path` open, excluding AHRB itself. Used to
/// identify the holder of a lock whose owner `F_GETLK` cannot report. Each
/// entry is revalidated by `(pid,start_time)`.
pub fn lock_file_openers(lock_path: &Path) -> Result<Vec<ProcessInfo>> {
    let own_pid = std::process::id();
    let mut pids = open_file_pids(lock_path)?;
    pids.retain(|pid| *pid != own_pid);
    let mut openers = Vec::new();
    for pid in pids {
        if let Some(mut info) = live_process_info(pid)? {
            info.ownership = ProcOwnership::LockHolder;
            openers.push(info);
        }
    }
    openers.sort_by_key(|process| process.identity);
    openers.dedup_by_key(|process| process.identity);
    Ok(openers)
}

/// Current information for a live PID, keyed by its stable identity.
pub fn live_process_info(pid: u32) -> Result<Option<ProcessInfo>> {
    let Some((identity, _group)) = process_identity_and_group(pid)? else {
        return Ok(None);
    };
    process_info_for_identity(identity)
}

#[cfg(target_os = "macos")]
fn open_file_pids(lock_path: &Path) -> Result<BTreeSet<u32>> {
    // `lsof -t` prints one PID per line and exits 1 when nothing has the file
    // open; any other failure is surfaced rather than read as "no holder".
    let output = std::process::Command::new("/usr/sbin/lsof")
        .arg("-t")
        .arg("-w")
        .arg("--")
        .arg(lock_path)
        .stdin(std::process::Stdio::null())
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let nothing_open = output.status.code() == Some(1) && stdout.trim().is_empty();
    if !output.status.success() && !nothing_open {
        return Err(AhrbError::Protocol(format!(
            "lsof could not list openers of {}: {}",
            lock_path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .parse::<u32>()
                .map_err(|_| AhrbError::Protocol(format!("lsof printed a non-PID line {line:?}")))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn open_file_pids(lock_path: &Path) -> Result<BTreeSet<u32>> {
    let target = std::fs::canonicalize(lock_path)?;
    let mut pids = BTreeSet::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(fds) = std::fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        for fd in fds.flatten() {
            if std::fs::read_link(fd.path()).is_ok_and(|link| link == target) {
                pids.insert(pid);
                break;
            }
        }
    }
    Ok(pids)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn open_file_pids(_lock_path: &Path) -> Result<BTreeSet<u32>> {
    Err(AhrbError::Unsupported(
        "lock-holder discovery is implemented only on macOS and Linux".to_owned(),
    ))
}

#[cfg(not(unix))]
pub fn audit_profile_lock(_profile: &Path, _lock_path: &Path) -> Result<ProfileLockAudit> {
    Err(AhrbError::Unsupported(
        "profile lock audit requires fcntl(F_GETLK)".to_owned(),
    ))
}

/// Bounded number of lock-driven reap rounds after the ownership passes.
pub const TEARDOWN_LOCK_REAP_ROUNDS: usize = 2;

/// Run-teardown record: owned processes observed before the first signal,
/// what was reaped, survivors, lock-holder attribution and the final lock audit.
#[derive(Clone, Debug, Serialize)]
pub struct RunTeardownEvidence {
    /// `PASS` or `ERROR`.
    pub status: &'static str,
    /// Live owned identities recorded before they were signalled.
    pub observed_owned_processes: Vec<TeardownProcess>,
    /// Observed identities no longer alive after teardown.
    pub reaped_processes: Vec<ProcIdentity>,
    /// Observed identities still alive after every bounded round.
    pub surviving_processes: Vec<ProcIdentity>,
    /// Final lock audit, taken after every bounded reap round.
    pub profile_locks: Vec<ProfileLockAudit>,
    /// How each lock found held during teardown was attributed to a holder.
    pub lock_holders: Vec<LockHolderEvidence>,
    /// Every teardown failure; non-empty means `ERROR`.
    pub errors: Vec<String>,
}

/// Holder attribution for a profile lock that was held during teardown.
#[derive(Clone, Debug, Serialize)]
pub struct LockHolderEvidence {
    /// One-based lock-driven reap round.
    pub round: usize,
    /// Disposable profile that declared the lock.
    pub profile: PathBuf,
    /// Held lock file.
    pub lock_path: PathBuf,
    /// Owner reported by `F_GETLK`; `None` when the kernel cannot attribute it.
    pub kernel_owner_pid: Option<u32>,
    /// `fcntl-owner` or `open-file-scan`.
    pub identified_by: &'static str,
    /// Every live process identified as holding (or having open) the lock.
    pub holders: Vec<TeardownProcess>,
    /// Holders that are neither a declared executable nor profile-owned by argv;
    /// they are recorded but never signalled.
    pub unowned_holders: Vec<ProcIdentity>,
}

fn teardown_process(process: ProcessInfo) -> TeardownProcess {
    TeardownProcess {
        identity: process.identity,
        command: process.command,
        ppid: process.ppid,
        ownership: process.ownership,
    }
}

/// Record `discovered` as observed before any signal, register it, and run one
/// bounded TERM-to-KILL cleanup over every registered owned identity.
fn teardown_reap_pass(
    discovered: &[ProcessInfo],
    observed: &mut BTreeMap<ProcIdentity, TeardownProcess>,
    reaped: &mut BTreeSet<ProcIdentity>,
    errors: &mut Vec<String>,
) {
    for process in discovered {
        observed
            .entry(process.identity)
            .or_insert_with(|| teardown_process(process.clone()));
    }
    if let Err(error) = track_profile_owned_processes(discovered) {
        errors.push(format!(
            "registering profile-owned processes failed: {error}"
        ));
    }
    match cleanup_owned_processes_with_evidence(Duration::from_millis(500)) {
        Ok(cleanup) => {
            for process in cleanup.observed {
                observed.entry(process.identity).or_insert(process);
            }
            reaped.extend(cleanup.reaped);
        }
        Err(error) => errors.push(format!("owned-process cleanup failed: {error}")),
    }
}

fn teardown_discover(
    root: &Path,
    executable_names: &[String],
    errors: &mut Vec<String>,
) -> Vec<ProcessInfo> {
    discover_profile_owned_processes(root, executable_names).unwrap_or_else(|error| {
        errors.push(format!(
            "profile-owned process discovery under {} failed: {error}",
            root.display()
        ));
        Vec::new()
    })
}

fn audit_declared_profile_locks(
    locks: &[(PathBuf, PathBuf)],
) -> (Vec<ProfileLockAudit>, Vec<String>) {
    let mut audits = Vec::new();
    let mut errors = Vec::new();
    for (profile, path) in locks {
        match audit_profile_lock(profile, path) {
            Ok(audit) => audits.push(audit),
            Err(error) => errors.push(format!(
                "profile lock {} could not be audited: {error}",
                path.display()
            )),
        }
    }
    (audits, errors)
}

/// Record, then reap, every AHRB-owned process left under `run_root`, and
/// audit the declared `(profile, lock_path)` pairs. Never returns early: every
/// failure is collected into `errors` so the report always carries the
/// observed/reaped evidence. A lock found held (including one whose owner the
/// kernel cannot report) triggers holder attribution and a bounded reap round;
/// only a lock still held after those rounds, a surviving owned process, or an
/// operation failure makes the teardown `ERROR`.
pub fn teardown_owned_processes(
    run_root: &Path,
    executable_names: &[String],
    locks: &[(PathBuf, PathBuf)],
) -> RunTeardownEvidence {
    let names = executable_names;
    let mut errors = Vec::new();
    let mut observed = BTreeMap::new();
    let mut reaped = BTreeSet::new();

    let profile_owned = teardown_discover(run_root, names, &mut errors);
    teardown_reap_pass(&profile_owned, &mut observed, &mut reaped, &mut errors);
    // Close the narrow race where a launcher creates a detached profile-owned
    // process during the first cleanup pass. The second pass remains bounded.
    let late_profile_owned = teardown_discover(run_root, names, &mut errors);
    if !late_profile_owned.is_empty() {
        teardown_reap_pass(&late_profile_owned, &mut observed, &mut reaped, &mut errors);
    }

    let mut lock_holders = Vec::new();
    let mut round = 0;
    let (lock_audits, lock_audit_errors) = loop {
        let (audits, audit_errors) = audit_declared_profile_locks(locks);
        let held = audits
            .iter()
            .filter_map(|audit| match audit {
                ProfileLockAudit::Held {
                    profile,
                    lock_path,
                    holder_pid,
                } => Some((profile.clone(), lock_path.clone(), *holder_pid)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if held.is_empty() || round == TEARDOWN_LOCK_REAP_ROUNDS {
            break (audits, audit_errors);
        }
        round += 1;
        let mut to_reap = Vec::new();
        for (profile, lock_path, kernel_owner_pid) in held {
            let (identified_by, candidates) = match kernel_owner_pid {
                Some(pid) => (
                    "fcntl-owner",
                    live_process_info(pid).map(|process| process.into_iter().collect::<Vec<_>>()),
                ),
                None => ("open-file-scan", lock_file_openers(&lock_path)),
            };
            let candidates = candidates.unwrap_or_else(|error| {
                errors.push(format!(
                    "holder of profile lock {} could not be identified ({identified_by}): {error}",
                    lock_path.display()
                ));
                Vec::new()
            });
            let owned_by_argv = teardown_discover(&profile, names, &mut errors);
            let owned_identities = owned_by_argv
                .iter()
                .map(|process| process.identity)
                .collect::<BTreeSet<_>>();
            let mut holders = Vec::new();
            let mut unowned_holders = Vec::new();
            for candidate in &candidates {
                if names.contains(&candidate.command)
                    || owned_identities.contains(&candidate.identity)
                {
                    to_reap.push(candidate.clone());
                } else {
                    unowned_holders.push(candidate.identity);
                }
                holders.push(teardown_process(candidate.clone()));
            }
            // A holder that cannot be seen by either method is still bounded
            // by this profile's own processes.
            to_reap.extend(owned_by_argv);
            lock_holders.push(LockHolderEvidence {
                round,
                profile,
                lock_path,
                kernel_owner_pid,
                identified_by,
                holders,
                unowned_holders,
            });
        }
        if to_reap.is_empty() {
            let (audits, audit_errors) = audit_declared_profile_locks(locks);
            break (audits, audit_errors);
        }
        teardown_reap_pass(&to_reap, &mut observed, &mut reaped, &mut errors);
    };
    errors.extend(lock_audit_errors);
    for audit in &lock_audits {
        if let ProfileLockAudit::Held {
            lock_path,
            holder_pid,
            ..
        } = audit
        {
            let holders = lock_holders
                .iter()
                .filter(|evidence| evidence.lock_path == *lock_path)
                .flat_map(|evidence| evidence.holders.iter().map(|holder| holder.identity))
                .collect::<BTreeSet<_>>();
            errors.push(format!(
                "profile lock {} remained held after bounded reaping (kernel owner PID {}, identified holders {:?})",
                lock_path.display(),
                holder_pid.map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
                holders
            ));
        }
    }

    let survivors = observed
        .keys()
        .copied()
        .filter(|identity| identity_is_live(*identity))
        .collect::<Vec<_>>();
    reaped.extend(observed.keys().copied());
    for survivor in &survivors {
        reaped.remove(survivor);
    }
    if !survivors.is_empty() {
        errors.push(format!(
            "owned-process teardown left {} process(es) alive: {:?}",
            survivors.len(),
            survivors
        ));
    }
    RunTeardownEvidence {
        status: if errors.is_empty() { "PASS" } else { "ERROR" },
        observed_owned_processes: observed.into_values().collect(),
        reaped_processes: reaped.into_iter().collect(),
        surviving_processes: survivors,
        profile_locks: lock_audits,
        lock_holders,
        errors,
    }
}

fn process_identity_and_group(pid: u32) -> Result<Option<(ProcIdentity, u32)>> {
    #[cfg(target_os = "macos")]
    {
        return macos::process_identity_and_group(pid);
    }
    #[cfg(target_os = "linux")]
    {
        return linux::process_identity_and_group(pid);
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "owned-process identity is implemented only on macOS and Linux".to_owned(),
    ))
}

fn process_info_for_identity(identity: ProcIdentity) -> Result<Option<ProcessInfo>> {
    #[cfg(target_os = "macos")]
    {
        return macos::process_info_for_identity(identity);
    }
    #[cfg(target_os = "linux")]
    {
        return linux::process_info_for_identity(identity);
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "owned-process information is implemented only on macOS and Linux".to_owned(),
    ))
}

fn process_group_members(process_group: u32, minimum_start: u64) -> Result<Vec<ProcIdentity>> {
    #[cfg(target_os = "macos")]
    {
        return macos::process_group_members(process_group, minimum_start);
    }
    #[cfg(target_os = "linux")]
    {
        return linux::process_group_members(process_group, minimum_start);
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "process-group discovery is implemented only on macOS and Linux".to_owned(),
    ))
}

pub(crate) fn identity_is_live(identity: ProcIdentity) -> bool {
    matches!(
        process_identity_and_group(identity.pid),
        Ok(Some((current, _))) if current == identity
    )
}

#[cfg(unix)]
fn signal_group(process_group: u32, signal: i32) -> Result<()> {
    let process_group = i32::try_from(process_group)
        .map_err(|_| AhrbError::Protocol("owned process group exceeds pid_t range".to_owned()))?;
    // SAFETY: a negative PID targets exactly the previously verified owned group.
    if unsafe { libc::kill(-process_group, signal) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error.into());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn signal_identity(identity: ProcIdentity, signal: i32) -> Result<()> {
    if !identity_is_live(identity) {
        return Ok(());
    }
    let pid = i32::try_from(identity.pid)
        .map_err(|_| AhrbError::Protocol("owned PID exceeds pid_t range".to_owned()))?;
    // SAFETY: the PID's start-time identity was revalidated immediately above.
    if unsafe { libc::kill(pid, signal) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error.into());
        }
    }
    Ok(())
}

fn verified_group_members(
    snapshot: &OwnedRegistry,
    process_group: u32,
    leader: ProcIdentity,
) -> Result<Vec<ProcIdentity>> {
    let leader_valid = matches!(
        process_identity_and_group(leader.pid)?,
        Some((current, current_group)) if current == leader && current_group == process_group
    );
    let observed_member_valid = if leader_valid {
        true
    } else {
        snapshot.observed.iter().any(|identity| {
            matches!(
                process_identity_and_group(identity.pid),
                Ok(Some((current, current_group)))
                    if current == *identity && current_group == process_group
            )
        })
    };
    if !observed_member_valid {
        return Ok(Vec::new());
    }
    process_group_members(process_group, leader.start_time)
}

fn live_owned(snapshot: &OwnedRegistry) -> Result<BTreeSet<ProcIdentity>> {
    let mut live = BTreeSet::new();
    for (process_group, leader) in &snapshot.groups {
        live.extend(verified_group_members(snapshot, *process_group, *leader)?);
    }
    live.extend(
        snapshot
            .observed
            .iter()
            .copied()
            .filter(|identity| identity_is_live(*identity)),
    );
    Ok(live)
}

/// Observe a direct child's exit while retaining its kernel counters for sampling.
/// The caller must still reap the child after taking the final identity receipt.
pub fn direct_child_exited_without_reap(pid: u32) -> Result<bool> {
    #[cfg(unix)]
    {
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: info is a valid output buffer. WNOHANG is nonblocking and
        // WNOWAIT preserves the child's wait status and pre-reap counters.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                return Ok(false);
            }
            return Err(error.into());
        }
        // SAFETY: the buffer was zero-initialized and waitid succeeded.
        let info = unsafe { info.assume_init() };
        // SAFETY: si_pid is valid in the successful child-status result.
        Ok(unsafe { info.si_pid() } == pid as libc::pid_t)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(AhrbError::Unsupported(
            "non-reaping child-exit observation is unavailable".into(),
        ))
    }
}

#[cfg(all(test, unix))]
#[test]
fn final_child_exit_observation_preserves_wait_status() {
    let mut child = std::process::Command::new("/bin/sh")
        .args(["-c", "exit 7"])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !direct_child_exited_without_reap(child.id()).unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(direct_child_exited_without_reap(child.id()).unwrap());
    assert_eq!(child.wait().unwrap().code(), Some(7));
}

#[cfg(unix)]
fn reap_direct_child(pid: u32) {
    let Ok(pid) = i32::try_from(pid) else {
        return;
    };
    let mut status = 0_i32;
    // SAFETY: `status` is a valid output pointer; WNOHANG never blocks. ECHILD
    // simply means the registered process was detached or already reaped.
    let _ = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
}

/// TERM every owned group, wait a bounded grace period, KILL survivors, then
/// sweep sampler-observed identities that may have reparented or changed group.
pub fn cleanup_owned_processes_with_evidence(grace: Duration) -> Result<ProcessTeardownEvidence> {
    let cleanup_lock = CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _exclusive = cleanup_lock
        .lock()
        .map_err(|_| AhrbError::Protocol("owned-process cleanup lock was poisoned".to_owned()))?;
    CLEANUP_REQUESTED.store(true, Ordering::SeqCst);
    let _phase = CleanupPhase;
    let snapshot = registry_lock()?.clone();
    if snapshot.groups.is_empty() && snapshot.observed.is_empty() {
        return Ok(ProcessTeardownEvidence::default());
    }
    #[cfg(unix)]
    {
        let observed_identities = live_owned(&snapshot)?;
        let mut observed = Vec::with_capacity(observed_identities.len());
        for identity in &observed_identities {
            observed.push(
                process_info_for_identity(*identity)?.unwrap_or(ProcessInfo {
                    identity: *identity,
                    ppid: 0,
                    command: String::new(),
                    ownership: ProcOwnership::Reparented,
                }),
            );
        }
        for (process_group, leader) in &snapshot.groups {
            if !verified_group_members(&snapshot, *process_group, *leader)?.is_empty() {
                signal_group(*process_group, libc::SIGTERM)?;
            }
        }
        for identity in &snapshot.observed {
            signal_identity(*identity, libc::SIGTERM)?;
        }
        let deadline = Instant::now() + grace;
        while Instant::now() < deadline && !live_owned(&snapshot)?.is_empty() {
            std::thread::sleep(Duration::from_millis(10));
        }
        for (process_group, leader) in &snapshot.groups {
            if !verified_group_members(&snapshot, *process_group, *leader)?.is_empty() {
                signal_group(*process_group, libc::SIGKILL)?;
            }
        }
        for identity in &snapshot.observed {
            signal_identity(*identity, libc::SIGKILL)?;
        }
        // Direct leaders can become waitable zombies before platform process
        // discovery reports them as live group members. Give SIGKILL a short
        // scheduling window and reap those children explicitly.
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(10));
            for leader in snapshot.groups.values() {
                reap_direct_child(leader.pid);
            }
        }
        let kill_deadline = Instant::now() + Duration::from_millis(250);
        let mut survivors = live_owned(&snapshot)?;
        while !survivors.is_empty() && Instant::now() < kill_deadline {
            for leader in snapshot.groups.values() {
                reap_direct_child(leader.pid);
            }
            std::thread::sleep(Duration::from_millis(10));
            survivors = live_owned(&snapshot)?;
        }
        let mut registry = registry_lock()?;
        if survivors.is_empty() {
            registry.groups.clear();
            registry.observed.clear();
            registry.identity_groups.clear();
            registry.trees.clear();
            for target in &SIGNAL_GROUPS {
                target.store(0, Ordering::Release);
            }
            for target in &SIGNAL_PIDS {
                target.store(0, Ordering::Release);
            }
        } else {
            registry.observed = survivors.clone();
            registry
                .identity_groups
                .retain(|identity, _| survivors.contains(identity));
            registry.trees.retain(|root, members| {
                members.retain(|identity| survivors.contains(identity));
                survivors.contains(root) || !members.is_empty()
            });
            registry.groups.retain(|process_group, leader| {
                process_group_members(*process_group, leader.start_time)
                    .is_ok_and(|members| !members.is_empty())
            });
            for target in &SIGNAL_GROUPS {
                target.store(0, Ordering::Release);
            }
            for target in &SIGNAL_PIDS {
                target.store(0, Ordering::Release);
            }
            for process_group in registry.groups.keys() {
                record_signal_target(&SIGNAL_GROUPS, *process_group);
            }
            for identity in &registry.observed {
                record_signal_target(&SIGNAL_PIDS, identity.pid);
            }
        }
        let survivors = survivors.into_iter().collect::<Vec<_>>();
        let survivor_set = survivors.iter().copied().collect::<BTreeSet<_>>();
        let reaped = observed_identities
            .into_iter()
            .filter(|identity| !survivor_set.contains(identity))
            .collect();
        return Ok(ProcessTeardownEvidence {
            observed: observed
                .into_iter()
                .map(|process| TeardownProcess {
                    identity: process.identity,
                    command: process.command,
                    ppid: process.ppid,
                    ownership: process.ownership,
                })
                .collect(),
            reaped,
            survivors,
        });
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "owned-process cleanup requires Unix".to_owned(),
    ))
}

/// Compatibility wrapper for callers that only need the survivor set.
pub fn cleanup_owned_processes(grace: Duration) -> Result<Vec<ProcIdentity>> {
    Ok(cleanup_owned_processes_with_evidence(grace)?.survivors)
}

struct CleanupPhase;

impl Drop for CleanupPhase {
    fn drop(&mut self) {
        CLEANUP_REQUESTED.store(false, Ordering::SeqCst);
    }
}

extern "C" fn remember_termination_signal(signal: i32) {
    RECEIVED_SIGNAL.store(signal, Ordering::SeqCst);
    if signal == libc::SIGABRT {
        kill_signal_targets();
        // SAFETY: `_exit` is async-signal-safe. Returning from SIGABRT would
        // allow abort(3) to force termination before a watchdog cleanup pass.
        unsafe { libc::_exit(128_i32.saturating_add(signal)) };
    }
}

fn kill_signal_targets() {
    for target in &SIGNAL_GROUPS {
        let process_group = target.load(Ordering::Acquire);
        if process_group > 0 {
            // SAFETY: kill is async-signal-safe and the negative target is a
            // group recorded immediately after an owned spawn.
            let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
        }
    }
    for target in &SIGNAL_PIDS {
        let pid = target.load(Ordering::Acquire);
        if pid > 0 {
            // SAFETY: kill is async-signal-safe and targets a recorded owned PID.
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}

/// Install process-wide SIGINT/SIGTERM/SIGABRT and panic cleanup hooks.
pub fn install_cleanup_handlers() {
    CLEANUP_INSTALL.call_once(|| {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = cleanup_owned_processes(Duration::from_millis(500));
            previous_hook(info);
        }));
        #[cfg(unix)]
        {
            // SAFETY: the handler only performs an atomic store, which is
            // async-signal-safe; cleanup runs on the watchdog thread.
            unsafe {
                libc::signal(
                    libc::SIGINT,
                    remember_termination_signal as *const () as libc::sighandler_t,
                );
                libc::signal(
                    libc::SIGTERM,
                    remember_termination_signal as *const () as libc::sighandler_t,
                );
                libc::signal(
                    libc::SIGABRT,
                    remember_termination_signal as *const () as libc::sighandler_t,
                );
            }
            let _ = std::thread::Builder::new()
                .name("ahrb-process-cleanup".to_owned())
                .spawn(|| {
                    loop {
                        let signal = RECEIVED_SIGNAL.load(Ordering::SeqCst);
                        if signal != 0 {
                            let _ = cleanup_owned_processes(Duration::from_millis(500));
                            // SAFETY: cleanup is complete and signal-driven exit must
                            // not run arbitrary destructors from the watchdog thread.
                            unsafe { libc::_exit(128_i32.saturating_add(signal)) };
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                });
        }
    });
}

/// Whole-tree resource sample.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Sample {
    /// Monotonic nanoseconds since sampler start.
    pub elapsed_ns: u64,
    /// Wall-clock timestamp for evidence correlation.
    pub wall_time: SystemTime,
    /// Workflow phase label.
    pub phase: String,
    /// Aggregate resident bytes.
    pub rss_bytes: u64,
    /// Aggregate proportional-set bytes where available.
    pub pss_bytes: Option<u64>,
    /// Aggregate private bytes where the platform exposes them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_bytes: Option<u64>,
    /// Aggregate physical footprint where available.
    pub footprint_bytes: Option<u64>,
    /// Aggregate resident bytes independently cross-checked with `task_info`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rss_crosscheck_bytes: Option<u64>,
    /// Linux cgroup-v2 `memory.current`, when a dedicated cgroup is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_memory_bytes: Option<u64>,
    /// Linux cgroup-v2 `memory.peak`, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_peak_bytes: Option<u64>,
    /// Cumulative user plus system CPU nanoseconds.
    pub cpu_ns: u64,
    /// Aggregate number of open file descriptors, when exposed by the platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_fds: Option<u64>,
    /// Aggregate number of live threads, when exposed by the platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_count: Option<u64>,
    /// Calling-thread CPU spent collecting this sample.
    #[serde(default)]
    pub collection_ns: u64,
    /// Wall time spent collecting this sample, used to detect overruns.
    #[serde(default)]
    pub collection_wall_ns: u64,
    /// Owned membership.
    pub processes: Vec<ProcessInfo>,
    /// Timestamped, per-process resource observations in deterministic identity order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_samples: Vec<ProcessSample>,
    /// Recoverable CPU-accounting anomalies observed by the out-of-band sampler.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cpu_accounting_warnings: Vec<CpuAccountingWarning>,
}

/// Monotonic cumulative CPU accounting across owned-process lifecycles.
///
/// A process must be absent for at least three consecutive counter refreshes
/// before it is retired. Its last cumulative counter remains accounted forever,
/// which prevents whole-tree CPU from dropping when workers exit or flicker.
#[derive(Debug, Default)]
pub(crate) struct TreeCpuTracker {
    identities: BTreeMap<ProcIdentity, CpuIdentityState>,
}

const CPU_RETIREMENT_MISSES: u32 = 3;

#[derive(Clone, Copy, Debug, Default)]
struct CpuIdentityState {
    accounted_ns: u64,
    consecutive_misses: u32,
    retired: bool,
}

/// A recoverable whole-tree CPU accounting anomaly retained in raw evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CpuAccountingWarning {
    /// Stable warning category.
    pub kind: String,
    /// Process identity that was re-admitted.
    pub identity: ProcIdentity,
    /// Consecutive absent refreshes observed before retirement.
    pub consecutive_misses: u32,
    /// Last cumulative CPU value retained while the identity was absent.
    pub previous_cpu_ns: u64,
    /// Cumulative CPU value observed when the identity reappeared.
    pub observed_cpu_ns: u64,
}

#[derive(Debug)]
pub(crate) struct TreeCpuUpdate {
    pub(crate) cumulative_ns: u64,
    pub(crate) newly_missing: Vec<ProcIdentity>,
    pub(crate) readmitted_after_miss: Vec<(ProcIdentity, u64)>,
    pub(crate) warnings: Vec<CpuAccountingWarning>,
}

impl TreeCpuTracker {
    pub(crate) fn update(
        &mut self,
        current: &BTreeMap<ProcIdentity, u64>,
    ) -> Result<TreeCpuUpdate> {
        let mut warnings = Vec::new();
        let mut readmitted_after_miss = Vec::new();
        for (identity, cpu_ns) in current {
            if let Some(state) = self.identities.get_mut(identity) {
                if *cpu_ns < state.accounted_ns {
                    return Err(AhrbError::Protocol(format!(
                        "process ({},{}) cumulative CPU regressed from {} to {cpu_ns}",
                        identity.pid, identity.start_time, state.accounted_ns
                    )));
                }
                if state.retired {
                    warnings.push(CpuAccountingWarning {
                        kind: "retired-identity-readmitted".to_owned(),
                        identity: *identity,
                        consecutive_misses: state.consecutive_misses,
                        previous_cpu_ns: state.accounted_ns,
                        observed_cpu_ns: *cpu_ns,
                    });
                }
                if state.consecutive_misses > 0 {
                    readmitted_after_miss.push((*identity, state.accounted_ns));
                }
                state.accounted_ns = *cpu_ns;
                state.consecutive_misses = 0;
                state.retired = false;
            } else {
                self.identities.insert(
                    *identity,
                    CpuIdentityState {
                        accounted_ns: *cpu_ns,
                        consecutive_misses: 0,
                        retired: false,
                    },
                );
            }
        }

        let mut newly_missing = Vec::new();
        for (identity, state) in &mut self.identities {
            if current.contains_key(identity) || state.retired {
                continue;
            }
            state.consecutive_misses = state.consecutive_misses.saturating_add(1);
            if state.consecutive_misses == 1 {
                newly_missing.push(*identity);
            }
            if state.consecutive_misses >= CPU_RETIREMENT_MISSES {
                state.retired = true;
            }
        }

        Ok(TreeCpuUpdate {
            cumulative_ns: self.identities.values().fold(0_u64, |total, state| {
                total.saturating_add(state.accounted_ns)
            }),
            newly_missing,
            readmitted_after_miss,
            warnings,
        })
    }
}

/// Supplemental terminal resource usage collected while reaping a direct child.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct TerminalRusage {
    /// Reaped process ID.
    pub pid: u32,
    /// Raw wait status suitable for `WIF*`/`WEXITSTATUS` interpretation.
    pub wait_status: i32,
    /// Peak resident set, normalized to bytes on every platform.
    pub max_rss_bytes: u64,
    /// User CPU consumed by the child.
    pub user_cpu_ns: u64,
    /// System CPU consumed by the child.
    pub system_cpu_ns: u64,
}

/// Platform-specific whole-tree sampler.
pub trait Sampler: Send {
    /// Pre-stimulus facility probe. Platform implementations inspect the
    /// collector's own process and discard that value; it is never harness I/O.
    fn disk_counter_preflight(&mut self) -> Result<()> {
        self.disk_counters(&ProcessTree::default()).map(|_| ())
    }

    /// OS-accounted physical read bytes, separate from allocated footprint.
    fn disk_read_counter_for_identity(&mut self, _identity: ProcIdentity) -> Result<Option<u64>> {
        Ok(None)
    }
    /// Discover ownership from verified roots.
    fn discover(&mut self, roots: &[u32]) -> Result<ProcessTree>;
    /// Discover identities that are live counter candidates. Platforms whose
    /// ordinary discovery retains exited identities for final accounting must
    /// exclude those identities from this view.
    fn discover_live(&mut self, roots: &[u32]) -> Result<ProcessTree> {
        self.discover(roots)
    }
    /// Capture one boundary or cadence sample.
    fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample>;
    /// Capture live process counters, excluding exited identities retained only
    /// for final cumulative accounting. Missing counters on live identities remain
    /// unavailable rather than being replaced with zeros.
    fn sample_live(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample> {
        self.sample(tree, phase)
    }
    /// Capture per-identity cumulative disk-write counters out of band.
    ///
    /// Implementations retain identities whose counters disappear in
    /// `expected_identities`; consumers must feed the result through
    /// [`TreeDiskTracker`] rather than treating a missing counter as zero.
    fn disk_counters(&mut self, _tree: &ProcessTree) -> Result<ProcessDiskObservation> {
        Err(AhrbError::Unsupported(
            "disk counters are not implemented by this sampler".to_owned(),
        ))
    }
    /// Read one identity's cumulative bytes-written counter.
    ///
    /// This narrow operation lets a lifecycle owner take the required final
    /// sample after a structured terminal and immediately before reap, without
    /// relying on a prior cadence poll.
    fn disk_counter_for_identity(&mut self, _identity: ProcIdentity) -> Result<Option<u64>> {
        Err(AhrbError::Unsupported(
            "per-identity disk counters are not implemented by this sampler".to_owned(),
        ))
    }
}

/// Re-resolve a live tree when a process exits between membership discovery and
/// its thread/FD counter reads.
///
/// Every retry is checked against the `(pid,start-time)` identities observed by
/// earlier attempts, so PID reuse is an error rather than replacement evidence.
/// Missing counters are never filled with zero. An empty sample is accepted only
/// when the freshly discovered live view itself produces no live processes.
pub(crate) fn sample_live_with_counter_retries(
    sampler: &mut dyn Sampler,
    roots: &[u32],
    phase: &str,
    max_attempts: u32,
    context: &str,
) -> Result<(ProcessTree, Sample)> {
    if max_attempts == 0 {
        return Err(AhrbError::Validation(format!(
            "{context} live-counter retry count must be positive"
        )));
    }
    let mut identities_by_pid = BTreeMap::<u32, ProcIdentity>::new();
    let mut accounting_warnings = Vec::new();
    for attempt in 1..=max_attempts {
        let tree = sampler.discover_live(roots)?;
        for identity in tree.members.keys() {
            if let Some(previous) = identities_by_pid.get(&identity.pid)
                && previous != identity
            {
                return Err(AhrbError::Protocol(format!(
                    "{context} PID {} changed start-time identity from {} to {} during live-counter retry",
                    identity.pid, previous.start_time, identity.start_time
                )));
            }
            identities_by_pid.entry(identity.pid).or_insert(*identity);
        }
        let mut sample = sampler.sample_live(&tree, phase)?;
        accounting_warnings.append(&mut sample.cpu_accounting_warnings);
        let discovered_identities: BTreeSet<ProcIdentity> = tree.members.keys().copied().collect();
        let process_identities: BTreeSet<ProcIdentity> = sample
            .processes
            .iter()
            .map(|process| process.identity)
            .collect();
        let sample_identities: BTreeSet<ProcIdentity> = sample
            .process_samples
            .iter()
            .map(|process| process.process.identity)
            .collect();
        let counters_complete = sample.open_fds.is_some()
            && sample.thread_count.is_some()
            && process_identities == discovered_identities
            && sample_identities == discovered_identities
            && sample
                .process_samples
                .iter()
                .all(|process| process.thread_count.is_some() && process.open_fds.is_some());
        if counters_complete {
            sample.cpu_accounting_warnings = accounting_warnings;
            return Ok((tree, sample));
        }
        if attempt == max_attempts {
            return Err(AhrbError::Protocol(format!(
                "{context} could not collect complete live thread/FD counters after {max_attempts} identity-safe attempts"
            )));
        }
        // Give a live-to-exit transition time to settle before rediscovery.
        // Back-to-back libproc reads can otherwise observe the same transient
        // omission on every bounded attempt.
        std::thread::sleep(Duration::from_millis(1));
    }
    unreachable!("positive bounded retry loop always returns")
}

/// Reap a direct child with `wait4` and return supplemental terminal usage.
///
/// `no_hang` maps to `WNOHANG`; a return of `Ok(None)` means the child has not
/// exited yet. Sampling the live process tree remains the authoritative memory
/// metric; this helper is only a terminal cross-check.
#[cfg(unix)]
pub fn wait4_rusage(pid: u32, no_hang: bool) -> Result<Option<TerminalRusage>> {
    let pid = i32::try_from(pid).map_err(|_| {
        crate::AhrbError::Validation("process ID is outside the platform pid_t range".to_owned())
    })?;
    let mut status = 0_i32;
    // SAFETY: `rusage` is a plain C output structure and zero is a valid initial
    // representation. `wait4` receives valid pointers for the duration of the call.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let flags = if no_hang { libc::WNOHANG } else { 0 };
    // SAFETY: the pid was range-checked above, and both output pointers refer to
    // live, correctly aligned local variables.
    let reaped = unsafe { libc::wait4(pid, &mut status, flags, &mut usage) };
    if reaped < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if reaped == 0 {
        return Ok(None);
    }

    Ok(Some(TerminalRusage {
        pid: reaped as u32,
        wait_status: status,
        max_rss_bytes: normalize_max_rss(usage.ru_maxrss),
        user_cpu_ns: timeval_ns(usage.ru_utime),
        system_cpu_ns: timeval_ns(usage.ru_stime),
    }))
}

#[cfg(unix)]
fn timeval_ns(value: libc::timeval) -> u64 {
    let seconds = u64::try_from(value.tv_sec).map_or(0, |number| number);
    let micros = u64::try_from(value.tv_usec).map_or(0, |number| number);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(micros.saturating_mul(1_000))
}

#[cfg(target_os = "macos")]
fn normalize_max_rss(value: libc::c_long) -> u64 {
    u64::try_from(value).map_or(0, |number| number)
}

#[cfg(target_os = "linux")]
fn normalize_max_rss(value: libc::c_long) -> u64 {
    u64::try_from(value).map_or(0, |number| number.saturating_mul(1_024))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(pid: u32) -> ProcIdentity {
        ProcIdentity {
            pid,
            start_time: u64::from(pid).saturating_mul(10),
        }
    }

    #[derive(Clone, Copy)]
    enum CounterRaceResolution {
        Empty,
        Exit,
        OmittedThenSameIdentity,
        SameIdentity,
        ReusedPid,
    }

    struct CounterRaceSampler {
        discoveries: u32,
        resolution: CounterRaceResolution,
    }

    impl CounterRaceSampler {
        fn new(resolution: CounterRaceResolution) -> Self {
            Self {
                discoveries: 0,
                resolution,
            }
        }

        fn process_identity(&self) -> Option<ProcIdentity> {
            match (self.discoveries, self.resolution) {
                (_, CounterRaceResolution::Empty) => None,
                (1, _) => Some(ProcIdentity {
                    pid: 42,
                    start_time: 100,
                }),
                (_, CounterRaceResolution::Exit) => None,
                (
                    _,
                    CounterRaceResolution::OmittedThenSameIdentity
                    | CounterRaceResolution::SameIdentity,
                ) => Some(ProcIdentity {
                    pid: 42,
                    start_time: 100,
                }),
                (_, CounterRaceResolution::ReusedPid) => Some(ProcIdentity {
                    pid: 42,
                    start_time: 200,
                }),
            }
        }
    }

    impl Sampler for CounterRaceSampler {
        fn discover(&mut self, _roots: &[u32]) -> Result<ProcessTree> {
            self.discoveries = self.discoveries.saturating_add(1);
            let mut tree = ProcessTree::default();
            if let Some(identity) = self.process_identity() {
                tree.roots.insert(identity);
                tree.members.insert(
                    identity,
                    ProcessInfo {
                        identity,
                        ppid: 0,
                        command: "counter-race".to_owned(),
                        ownership: ProcOwnership::DeclaredRoot,
                    },
                );
            }
            Ok(tree)
        }

        fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample> {
            let complete = self.discoveries > 1 || tree.members.is_empty();
            let wall_time = SystemTime::now();
            let omit_process = self.discoveries == 1
                && matches!(
                    self.resolution,
                    CounterRaceResolution::OmittedThenSameIdentity
                );
            let process_samples = tree
                .members
                .values()
                .filter(|_| !omit_process)
                .cloned()
                .map(|process| ProcessSample {
                    elapsed_ns: u64::from(self.discoveries),
                    wall_time,
                    phase: phase.to_owned(),
                    process,
                    rss_bytes: 1,
                    pss_bytes: None,
                    private_bytes: None,
                    footprint_bytes: None,
                    rss_crosscheck_bytes: None,
                    cpu_ns: 1,
                    open_fds: Some(1),
                    thread_count: complete.then_some(1),
                })
                .collect::<Vec<_>>();
            Ok(Sample {
                elapsed_ns: u64::from(self.discoveries),
                wall_time,
                phase: phase.to_owned(),
                rss_bytes: u64::try_from(process_samples.len()).unwrap(),
                pss_bytes: None,
                private_bytes: None,
                footprint_bytes: None,
                rss_crosscheck_bytes: None,
                cgroup_memory_bytes: None,
                cgroup_peak_bytes: None,
                cpu_ns: u64::try_from(process_samples.len()).unwrap(),
                open_fds: (!omit_process).then(|| u64::try_from(process_samples.len()).unwrap()),
                thread_count: (complete && !omit_process)
                    .then(|| u64::try_from(process_samples.len()).unwrap()),
                collection_ns: 1,
                collection_wall_ns: 1,
                processes: tree
                    .members
                    .values()
                    .filter(|_| !omit_process)
                    .cloned()
                    .collect(),
                process_samples,
                cpu_accounting_warnings: Vec::new(),
            })
        }
    }

    #[test]
    fn live_counter_retry_resolves_the_same_identity_or_its_exit() -> Result<()> {
        for resolution in [
            CounterRaceResolution::Exit,
            CounterRaceResolution::SameIdentity,
        ] {
            let mut sampler = CounterRaceSampler::new(resolution);
            let (tree, sample) = sample_live_with_counter_retries(
                &mut sampler,
                &[42],
                "counter-race",
                3,
                "test counter race",
            )?;
            assert_eq!(sampler.discoveries, 2);
            assert!(sample.thread_count.is_some());
            match resolution {
                CounterRaceResolution::Exit => {
                    assert!(tree.members.is_empty());
                    assert!(sample.processes.is_empty());
                }
                CounterRaceResolution::SameIdentity => {
                    assert_eq!(tree.members.len(), 1);
                    assert_eq!(sample.processes[0].identity.start_time, 100);
                    assert_eq!(sample.thread_count, Some(1));
                }
                CounterRaceResolution::ReusedPid => unreachable!(),
                CounterRaceResolution::Empty => unreachable!(),
                CounterRaceResolution::OmittedThenSameIdentity => unreachable!(),
            }
        }
        Ok(())
    }

    #[test]
    fn live_counter_retry_rejects_an_omitted_attempt_before_same_identity_success() -> Result<()> {
        let mut sampler = CounterRaceSampler::new(CounterRaceResolution::OmittedThenSameIdentity);
        let (tree, sample) = sample_live_with_counter_retries(
            &mut sampler,
            &[42],
            "omitted-counter-attempt",
            3,
            "test omitted counter attempt",
        )?;
        assert_eq!(sampler.discoveries, 2);
        assert_eq!(tree.members.len(), 1);
        assert_eq!(sample.processes.len(), 1);
        assert_eq!(sample.process_samples.len(), 1);
        assert_eq!(sample.processes[0].identity.start_time, 100);
        assert_eq!(sample.open_fds, Some(1));
        assert_eq!(sample.thread_count, Some(1));
        Ok(())
    }

    #[test]
    fn live_counter_retry_accepts_a_verified_empty_tree() -> Result<()> {
        let mut sampler = CounterRaceSampler::new(CounterRaceResolution::Empty);
        let (tree, sample) = sample_live_with_counter_retries(
            &mut sampler,
            &[42],
            "empty-counter-tree",
            3,
            "test empty counter tree",
        )?;
        assert_eq!(sampler.discoveries, 1);
        assert!(tree.members.is_empty());
        assert!(sample.processes.is_empty());
        assert!(sample.process_samples.is_empty());
        assert_eq!(sample.open_fds, Some(0));
        assert_eq!(sample.thread_count, Some(0));
        Ok(())
    }

    #[test]
    fn live_counter_retry_rejects_pid_reuse() {
        let mut sampler = CounterRaceSampler::new(CounterRaceResolution::ReusedPid);
        let error = sample_live_with_counter_retries(
            &mut sampler,
            &[42],
            "counter-race",
            3,
            "test counter race",
        )
        .expect_err("PID reuse must not satisfy a missing same-identity counter");
        assert!(error.to_string().contains("changed start-time identity"));
    }

    #[test]
    fn tree_cpu_remains_monotonic_when_a_member_exits() -> Result<()> {
        let root = identity(10);
        let worker = identity(20);
        let mut tracker = TreeCpuTracker::default();
        assert_eq!(
            tracker
                .update(&BTreeMap::from([(root, 100), (worker, 50)]))?
                .cumulative_ns,
            150
        );
        assert_eq!(
            tracker
                .update(&BTreeMap::from([(root, 120)]))?
                .cumulative_ns,
            170
        );
        assert_eq!(
            tracker
                .update(&BTreeMap::from([(root, 130)]))?
                .cumulative_ns,
            180
        );
        Ok(())
    }

    #[test]
    fn tree_cpu_tolerates_flicker_and_readmits_a_retired_identity() -> Result<()> {
        let root = identity(10);
        let worker = identity(20);
        let mut tracker = TreeCpuTracker::default();
        tracker.update(&BTreeMap::from([(root, 100), (worker, 50)]))?;
        let missed_once = tracker.update(&BTreeMap::from([(root, 120)]))?;
        assert_eq!(missed_once.cumulative_ns, 170);
        assert_eq!(missed_once.newly_missing, vec![worker]);
        let returned = tracker.update(&BTreeMap::from([(root, 130), (worker, 60)]))?;
        assert_eq!(returned.cumulative_ns, 190);
        assert!(returned.warnings.is_empty());
        assert_eq!(returned.readmitted_after_miss, vec![(worker, 50)]);

        for root_cpu in [140, 150, 160] {
            tracker.update(&BTreeMap::from([(root, root_cpu)]))?;
        }
        let readmitted = tracker.update(&BTreeMap::from([(root, 170), (worker, 70)]))?;
        assert_eq!(readmitted.cumulative_ns, 240);
        assert_eq!(readmitted.warnings.len(), 1);
        assert_eq!(readmitted.warnings[0].identity, worker);
        assert_eq!(readmitted.warnings[0].kind, "retired-identity-readmitted");
        Ok(())
    }

    #[test]
    fn tree_cpu_still_rejects_a_same_identity_counter_regression() -> Result<()> {
        let root = identity(10);
        let worker = identity(20);
        let mut tracker = TreeCpuTracker::default();
        tracker.update(&BTreeMap::from([(root, 100), (worker, 50)]))?;
        let regression = tracker
            .update(&BTreeMap::from([(root, 99), (worker, 50)]))
            .expect_err("same-identity CPU regression must be rejected");
        assert!(regression.to_string().contains("CPU regressed"));
        Ok(())
    }

    #[test]
    fn tree_disk_does_not_complete_a_disappeared_identity_from_its_last_poll() -> Result<()> {
        let worker = identity(20);
        let mut tracker = TreeDiskTracker::default();
        let live = ProcessDiskObservation {
            expected_identities: BTreeSet::from([worker]),
            write_bytes_by_identity: BTreeMap::from([(worker, 4_096)]),
            cgroup_write_bytes: None,
        };
        assert!(tracker.observe(&live)?.counter_complete);

        let missing = tracker.observe(&ProcessDiskObservation::default())?;
        assert!(!missing.counter_complete);
        assert_eq!(missing.cumulative_write_bytes, None);
        assert_eq!(missing.observed_write_bytes, 4_096);
        assert_eq!(missing.incomplete_identities, vec![worker]);
        assert_eq!(
            missing.identities[0].status,
            DiskIdentityStatus::MissingWithoutRetirementEvidence
        );
        Ok(())
    }

    #[test]
    fn tree_disk_requires_terminal_before_final_sample_and_explicit_retirement() -> Result<()> {
        let worker = identity(20);
        let mut tracker = TreeDiskTracker::default();
        tracker.observe(&ProcessDiskObservation {
            expected_identities: BTreeSet::from([worker]),
            write_bytes_by_identity: BTreeMap::from([(worker, 100)]),
            cgroup_write_bytes: None,
        })?;
        let out_of_order = tracker
            .record_final_sample_before_reap(worker, 120)
            .expect_err("a final sample before the structured terminal must be rejected");
        assert!(out_of_order.to_string().contains("preceded"));

        tracker.note_structured_terminal(worker)?;
        assert!(!tracker.snapshot().counter_complete);
        tracker.record_final_sample_before_reap(worker, 120)?;
        let final_sample = tracker.snapshot();
        assert!(final_sample.counter_complete);
        assert_eq!(
            final_sample.identities[0].status,
            DiskIdentityStatus::FinalSampleBeforeReap
        );
        tracker.retire_after_final_sample(worker)?;
        let retired = tracker.snapshot();
        assert!(retired.counter_complete);
        assert_eq!(retired.cumulative_write_bytes, Some(120));
        assert_eq!(
            retired.identities[0].status,
            DiskIdentityStatus::RetiredAfterFinalSample
        );
        Ok(())
    }

    #[test]
    fn tree_disk_accepts_post_quiet_cgroup_retirement() -> Result<()> {
        let worker = identity(20);
        let mut tracker = TreeDiskTracker::default();
        tracker.observe(&ProcessDiskObservation {
            expected_identities: BTreeSet::from([worker]),
            write_bytes_by_identity: BTreeMap::from([(worker, 100)]),
            cgroup_write_bytes: Some(500),
        })?;
        tracker.observe(&ProcessDiskObservation {
            expected_identities: BTreeSet::new(),
            write_bytes_by_identity: BTreeMap::new(),
            cgroup_write_bytes: Some(600),
        })?;
        assert!(!tracker.snapshot().counter_complete);
        tracker.retire_with_cgroup_after_quiet(&BTreeSet::from([worker]), 640)?;
        let retired = tracker.snapshot();
        assert!(retired.counter_complete);
        assert_eq!(retired.cumulative_write_bytes, Some(640));
        assert_eq!(
            retired.identities[0].status,
            DiskIdentityStatus::RetiredByDurableCgroup
        );
        Ok(())
    }

    #[test]
    fn tree_disk_keeps_unreadable_expected_counter_incomplete() -> Result<()> {
        let worker = identity(20);
        let mut tracker = TreeDiskTracker::default();
        let snapshot = tracker.observe(&ProcessDiskObservation {
            expected_identities: BTreeSet::from([worker]),
            write_bytes_by_identity: BTreeMap::new(),
            cgroup_write_bytes: None,
        })?;
        assert!(!snapshot.counter_complete);
        assert_eq!(snapshot.cumulative_write_bytes, None);
        assert_eq!(
            snapshot.identities[0].status,
            DiskIdentityStatus::CounterUnavailable
        );
        Ok(())
    }
}
