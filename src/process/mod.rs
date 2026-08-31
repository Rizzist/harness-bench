//! Whole-process-tree ownership discovery and sampling.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
}

/// One process in an owned tree.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    /// Monotonic nanoseconds since sampler start.
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
    detached_matches: Vec<crate::manifest::ProcessMatch>,
}

static OWNED_REGISTRY: OnceLock<Mutex<OwnedRegistry>> = OnceLock::new();
static CLEANUP_INSTALL: Once = Once::new();
static CLEANUP_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static CLEANUP_REQUESTED: AtomicBool = AtomicBool::new(false);
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);
static DETACHED_DISCOVERY_PENDING: AtomicI32 = AtomicI32::new(0);
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

/// Register a newly spawned process and the isolated process group it leads or joins.
pub fn register_process(pid: u32) -> Result<()> {
    let Some((identity, process_group)) = process_identity_and_group(pid)? else {
        return Err(AhrbError::Protocol(format!(
            "spawned process PID {pid} disappeared before ownership registration"
        )));
    };
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
    record_signal_target(&SIGNAL_GROUPS, process_group);
    record_signal_target(&SIGNAL_PIDS, identity.pid);
    Ok(())
}

/// Register exact detached-daemon ownership evidence before its launcher runs.
pub fn register_detached_match(process_match: crate::manifest::ProcessMatch) -> Result<()> {
    if process_match.executable_name.trim().is_empty() {
        return Ok(());
    }
    let cleanup_lock = CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _registration = cleanup_lock
        .lock()
        .map_err(|_| AhrbError::Protocol("owned-process cleanup lock was poisoned".to_owned()))?;
    if CLEANUP_REQUESTED.load(Ordering::SeqCst) {
        return Err(AhrbError::Protocol(
            "refusing to start a detached daemon during owned-process cleanup".to_owned(),
        ));
    }
    let mut registry = registry_lock()?;
    let is_new = !registry.detached_matches.iter().any(|existing| {
        existing.executable_name == process_match.executable_name
            && existing.environment == process_match.environment
    });
    if is_new {
        registry.detached_matches.push(process_match.clone());
    }
    drop(registry);
    DETACHED_DISCOVERY_PENDING.fetch_add(1, Ordering::SeqCst);
    let watcher = std::thread::Builder::new()
        .name("ahrb-detached-owner".to_owned())
        .spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(120);
            let mut termination_deadline = None;
            while Instant::now() < deadline {
                if let Ok([pid]) = matching_processes_fast(
                    &process_match.executable_name,
                    &process_match.environment,
                )
                .as_deref()
                {
                    let _ = register_process(*pid);
                    DETACHED_DISCOVERY_PENDING.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
                if RECEIVED_SIGNAL.load(Ordering::SeqCst) != 0
                    || CLEANUP_REQUESTED.load(Ordering::SeqCst)
                {
                    let signal_deadline = termination_deadline
                        .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
                    if Instant::now() >= *signal_deadline {
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            DETACHED_DISCOVERY_PENDING.fetch_sub(1, Ordering::SeqCst);
        });
    if let Err(error) = watcher {
        DETACHED_DISCOVERY_PENDING.fetch_sub(1, Ordering::SeqCst);
        return Err(error.into());
    }
    Ok(())
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
    for identity in tree.members.keys() {
        record_signal_target(&SIGNAL_PIDS, identity.pid);
    }
    Ok(())
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

fn matching_processes_fast(
    executable_name: &str,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<u32>> {
    #[cfg(target_os = "macos")]
    {
        return macos::matching_processes_fast(executable_name, environment);
    }
    #[cfg(target_os = "linux")]
    {
        return linux::matching_processes_fast(executable_name, environment);
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "detached-process matching is implemented only on macOS and Linux".to_owned(),
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

fn identity_is_live(identity: ProcIdentity) -> bool {
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
pub fn cleanup_owned_processes(grace: Duration) -> Result<Vec<ProcIdentity>> {
    let cleanup_lock = CLEANUP_LOCK.get_or_init(|| Mutex::new(()));
    let _exclusive = cleanup_lock
        .lock()
        .map_err(|_| AhrbError::Protocol("owned-process cleanup lock was poisoned".to_owned()))?;
    CLEANUP_REQUESTED.store(true, Ordering::SeqCst);
    let _phase = CleanupPhase;
    while DETACHED_DISCOVERY_PENDING.load(Ordering::SeqCst) > 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut snapshot = registry_lock()?.clone();
    for process_match in &snapshot.detached_matches {
        for pid in matching_processes(&process_match.executable_name, &process_match.environment)? {
            if let Some((identity, process_group)) = process_identity_and_group(pid)? {
                snapshot.groups.entry(process_group).or_insert(identity);
                snapshot.observed.insert(identity);
                record_signal_target(&SIGNAL_GROUPS, process_group);
                record_signal_target(&SIGNAL_PIDS, pid);
            }
        }
    }
    if snapshot.groups.is_empty()
        && snapshot.observed.is_empty()
        && snapshot.detached_matches.is_empty()
    {
        return Ok(Vec::new());
    }
    #[cfg(unix)]
    {
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
            registry.detached_matches.clear();
            for target in &SIGNAL_GROUPS {
                target.store(0, Ordering::Release);
            }
            for target in &SIGNAL_PIDS {
                target.store(0, Ordering::Release);
            }
        } else {
            registry.observed = survivors.clone();
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
        return Ok(survivors.into_iter().collect());
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "owned-process cleanup requires Unix".to_owned(),
    ))
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
        // A detached launcher can exit between its group registration and the
        // exact daemon match becoming visible. Keep repeating the lock-free
        // sweep while its pre-registered watcher is resolving that handoff.
        // This uses only atomics, kill(2), and spin hints in the signal path.
        let mut rounds = 0_u32;
        while DETACHED_DISCOVERY_PENDING.load(Ordering::SeqCst) > 0 && rounds < 150_000 {
            kill_signal_targets();
            for _ in 0..10_000 {
                std::hint::spin_loop();
            }
            rounds = rounds.saturating_add(1);
        }
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
}

/// Monotonic cumulative CPU accounting across owned-process lifecycles.
///
/// A process that disappears contributes its last observed cumulative counter to
/// `retired_ns`; this prevents whole-tree CPU from dropping when workers exit.
#[derive(Debug, Default)]
pub(crate) struct TreeCpuTracker {
    live: BTreeMap<ProcIdentity, u64>,
    retired: BTreeSet<ProcIdentity>,
    retired_ns: u64,
}

impl TreeCpuTracker {
    pub(crate) fn update(&mut self, current: &BTreeMap<ProcIdentity, u64>) -> Result<u64> {
        for (identity, cpu_ns) in current {
            if self.retired.contains(identity) {
                return Err(AhrbError::Protocol(format!(
                    "retired process identity ({},{}) reappeared in CPU accounting",
                    identity.pid, identity.start_time
                )));
            }
            if let Some(previous) = self.live.get(identity) {
                if cpu_ns < previous {
                    return Err(AhrbError::Protocol(format!(
                        "process ({},{}) cumulative CPU regressed from {previous} to {cpu_ns}",
                        identity.pid, identity.start_time
                    )));
                }
            }
        }

        for (identity, cpu_ns) in &self.live {
            if !current.contains_key(identity) {
                self.retired_ns = self.retired_ns.saturating_add(*cpu_ns);
                self.retired.insert(*identity);
            }
        }
        self.live = current.clone();
        Ok(self
            .live
            .values()
            .fold(self.retired_ns, |total, value| total.saturating_add(*value)))
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
    /// Discover ownership from verified roots.
    fn discover(&mut self, roots: &[u32]) -> Result<ProcessTree>;
    /// Capture one boundary or cadence sample.
    fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample>;
}

/// Locate processes by exact executable basename and isolated-root evidence.
///
/// This is intentionally stricter than a name-only lookup. Linux reads the
/// inherited environment directly. macOS uses that evidence when permitted and
/// otherwise requires an open file beneath one of the same isolated roots.
pub fn matching_processes(
    executable_name: &str,
    environment: &BTreeMap<String, String>,
) -> Result<Vec<u32>> {
    #[cfg(target_os = "macos")]
    {
        return macos::matching_processes(executable_name, environment);
    }
    #[cfg(target_os = "linux")]
    {
        return linux::matching_processes(executable_name, environment);
    }
    #[allow(unreachable_code)]
    Err(AhrbError::Unsupported(
        "detached process matching is implemented only on macOS and Linux".to_owned(),
    ))
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

    #[test]
    fn tree_cpu_remains_monotonic_when_a_member_exits() -> Result<()> {
        let root = identity(10);
        let worker = identity(20);
        let mut tracker = TreeCpuTracker::default();
        assert_eq!(
            tracker.update(&BTreeMap::from([(root, 100), (worker, 50)]))?,
            150
        );
        assert_eq!(tracker.update(&BTreeMap::from([(root, 120)]))?, 170);
        assert_eq!(tracker.update(&BTreeMap::from([(root, 130)]))?, 180);
        Ok(())
    }

    #[test]
    fn tree_cpu_rejects_counter_regression_and_retired_reappearance() -> Result<()> {
        let root = identity(10);
        let worker = identity(20);
        let mut tracker = TreeCpuTracker::default();
        tracker.update(&BTreeMap::from([(root, 100), (worker, 50)]))?;
        let regression = tracker
            .update(&BTreeMap::from([(root, 99), (worker, 50)]))
            .expect_err("same-identity CPU regression must be rejected");
        assert!(regression.to_string().contains("CPU regressed"));

        tracker.update(&BTreeMap::from([(root, 120)]))?;
        let reappearance = tracker
            .update(&BTreeMap::from([(root, 130), (worker, 60)]))
            .expect_err("retired identity must not reappear");
        assert!(reappearance.to_string().contains("reappeared"));
        Ok(())
    }
}
