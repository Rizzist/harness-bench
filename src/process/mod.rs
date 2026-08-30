//! Whole-process-tree ownership discovery and sampling.

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

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
