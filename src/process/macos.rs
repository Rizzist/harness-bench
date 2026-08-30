//! macOS libproc whole-tree sampler.

use crate::process::{
    ProcIdentity, ProcOwnership, ProcessInfo, ProcessSample, ProcessTree, Sample, Sampler,
    TreeCpuTracker,
};
use crate::{AhrbError, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_char, c_int, c_void};
use std::mem::{size_of, zeroed};
use std::time::{Instant, SystemTime};

const PROC_PPID_ONLY: u32 = 6;
const PROC_PGRP_ONLY: u32 = 2;
const PROC_PIDTBSDINFO: c_int = 3;
const PROC_PIDTASKINFO: c_int = 4;
const RUSAGE_INFO_V4: c_int = 4;
const TASK_VM_INFO: c_int = 22;
const KERN_SUCCESS: c_int = 0;
const MAXCOMLEN: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
struct ProcBsdInfo {
    pbi_flags: u32,
    pbi_status: u32,
    pbi_xstatus: u32,
    pbi_pid: u32,
    pbi_ppid: u32,
    pbi_uid: u32,
    pbi_gid: u32,
    pbi_ruid: u32,
    pbi_rgid: u32,
    pbi_svuid: u32,
    pbi_svgid: u32,
    rfu_1: u32,
    pbi_comm: [c_char; MAXCOMLEN],
    pbi_name: [c_char; 2 * MAXCOMLEN],
    pbi_nfiles: u32,
    pbi_pgid: u32,
    pbi_pjobc: u32,
    e_tdev: u32,
    e_tpgid: u32,
    pbi_nice: i32,
    pbi_start_tvsec: u64,
    pbi_start_tvusec: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RusageInfoV4 {
    ri_uuid: [u8; 16],
    ri_user_time: u64,
    ri_system_time: u64,
    ri_pkg_idle_wkups: u64,
    ri_interrupt_wkups: u64,
    ri_pageins: u64,
    ri_wired_size: u64,
    ri_resident_size: u64,
    ri_phys_footprint: u64,
    ri_proc_start_abstime: u64,
    ri_proc_exit_abstime: u64,
    ri_child_user_time: u64,
    ri_child_system_time: u64,
    ri_child_pkg_idle_wkups: u64,
    ri_child_interrupt_wkups: u64,
    ri_child_pageins: u64,
    ri_child_elapsed_abstime: u64,
    ri_diskio_bytesread: u64,
    ri_diskio_byteswritten: u64,
    ri_cpu_time_qos_default: u64,
    ri_cpu_time_qos_maintenance: u64,
    ri_cpu_time_qos_background: u64,
    ri_cpu_time_qos_utility: u64,
    ri_cpu_time_qos_legacy: u64,
    ri_cpu_time_qos_user_initiated: u64,
    ri_cpu_time_qos_user_interactive: u64,
    ri_billed_system_time: u64,
    ri_serviced_system_time: u64,
    ri_logical_writes: u64,
    ri_lifetime_max_phys_footprint: u64,
    ri_instructions: u64,
    ri_cycles: u64,
    ri_billed_energy: u64,
    ri_serviced_energy: u64,
    ri_interval_max_phys_footprint: u64,
    ri_runnable_time: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ProcTaskInfo {
    pti_virtual_size: u64,
    pti_resident_size: u64,
    pti_total_user: u64,
    pti_total_system: u64,
    pti_threads_user: u64,
    pti_threads_system: u64,
    pti_policy: i32,
    pti_faults: i32,
    pti_pageins: i32,
    pti_cow_faults: i32,
    pti_messages_sent: i32,
    pti_messages_received: i32,
    pti_syscalls_mach: i32,
    pti_syscalls_unix: i32,
    pti_csw: i32,
    pti_threadnum: i32,
    pti_numrunning: i32,
    pti_priority: i32,
}

struct MacProcessCounters {
    process: ProcessInfo,
    rss_bytes: u64,
    footprint_bytes: u64,
    rss_crosscheck_bytes: Option<u64>,
    cpu_ns: u64,
    open_fds: u64,
    thread_count: Option<u64>,
}

/// Prefix through revision 1 of Darwin's `task_vm_info` structure.
#[repr(C)]
#[derive(Clone, Copy)]
struct TaskVmInfoRev1 {
    virtual_size: u64,
    region_count: i32,
    page_size: i32,
    resident_size: u64,
    resident_size_peak: u64,
    device: u64,
    device_peak: u64,
    internal: u64,
    internal_peak: u64,
    external: u64,
    external_peak: u64,
    reusable: u64,
    reusable_peak: u64,
    purgeable_volatile_pmap: u64,
    purgeable_volatile_resident: u64,
    purgeable_volatile_virtual: u64,
    compressed: u64,
    compressed_peak: u64,
    compressed_lifetime: u64,
    phys_footprint: u64,
}

#[link(name = "proc")]
unsafe extern "C" {
    fn proc_listpids(kind: u32, type_info: u32, buffer: *mut c_void, buffer_size: c_int) -> c_int;
    fn proc_pidinfo(
        pid: c_int,
        flavor: c_int,
        arg: u64,
        buffer: *mut c_void,
        buffer_size: c_int,
    ) -> c_int;
    fn proc_pid_rusage(pid: c_int, flavor: c_int, buffer: *mut c_void) -> c_int;
}

unsafe extern "C" {
    static mach_task_self_: u32;
    fn task_for_pid(target_task: u32, pid: c_int, task: *mut u32) -> c_int;
    fn task_info(task: u32, flavor: c_int, info: *mut i32, count: *mut u32) -> c_int;
    fn mach_port_deallocate(task: u32, name: u32) -> c_int;
}

/// macOS sampler using start-time identities, libproc rusage, and task-info
/// cross-checks.
#[derive(Debug)]
pub struct MacOsSampler {
    started: Instant,
    known: BTreeSet<ProcIdentity>,
    verified_roots: BTreeMap<u32, ProcIdentity>,
    verified_groups: BTreeMap<ProcIdentity, u32>,
    excluded_roots: BTreeSet<ProcIdentity>,
    discovery_ns: u64,
    cpu: TreeCpuTracker,
    last_self_cpu: BTreeMap<ProcIdentity, u64>,
    parent_by_child: BTreeMap<ProcIdentity, ProcIdentity>,
    child_rollup_cpu: BTreeMap<ProcIdentity, u64>,
    pending_sampled_child_cpu: BTreeMap<ProcIdentity, u64>,
    unsampled_child_cpu_ns: u64,
}

impl Default for MacOsSampler {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            known: BTreeSet::new(),
            verified_roots: BTreeMap::new(),
            verified_groups: BTreeMap::new(),
            excluded_roots: BTreeSet::new(),
            discovery_ns: 0,
            cpu: TreeCpuTracker::default(),
            last_self_cpu: BTreeMap::new(),
            parent_by_child: BTreeMap::new(),
            child_rollup_cpu: BTreeMap::new(),
            pending_sampled_child_cpu: BTreeMap::new(),
            unsampled_child_cpu_ns: 0,
        }
    }
}

impl MacOsSampler {
    fn reconcile_child_cpu(
        &mut self,
        current_self_cpu: &BTreeMap<ProcIdentity, u64>,
        current_parents: &BTreeMap<ProcIdentity, ProcIdentity>,
        current_child_rollup: &BTreeMap<ProcIdentity, u64>,
    ) -> Result<u64> {
        for (identity, cpu_ns) in &self.last_self_cpu {
            if current_self_cpu.contains_key(identity) {
                continue;
            }
            if let Some(parent) = self.parent_by_child.get(identity) {
                self.pending_sampled_child_cpu
                    .entry(*parent)
                    .and_modify(|pending| *pending = pending.saturating_add(*cpu_ns))
                    .or_insert(*cpu_ns);
            }
        }
        for (parent, child_cpu_ns) in current_child_rollup {
            let previous = self.child_rollup_cpu.get(parent).copied().unwrap_or(0);
            if *child_cpu_ns < previous {
                return Err(AhrbError::Protocol(format!(
                    "process ({},{}) cumulative child CPU regressed from {previous} to {child_cpu_ns}",
                    parent.pid, parent.start_time
                )));
            }
            let delta = child_cpu_ns.saturating_sub(previous);
            let pending = self
                .pending_sampled_child_cpu
                .get(parent)
                .copied()
                .unwrap_or(0);
            let already_sampled = delta.min(pending);
            if already_sampled > 0 {
                let remaining = pending.saturating_sub(already_sampled);
                if remaining == 0 {
                    self.pending_sampled_child_cpu.remove(parent);
                } else {
                    self.pending_sampled_child_cpu.insert(*parent, remaining);
                }
            }
            self.unsampled_child_cpu_ns = self
                .unsampled_child_cpu_ns
                .saturating_add(delta.saturating_sub(already_sampled));
        }
        self.last_self_cpu = current_self_cpu.clone();
        self.parent_by_child = current_parents.clone();
        self.child_rollup_cpu = current_child_rollup.clone();
        Ok(self.unsampled_child_cpu_ns)
    }

    /// Exclude a process and its descendants from ownership attribution.
    ///
    /// The process is captured as a `(pid,start-time)` identity immediately, so
    /// a later occupant of the same PID is not accidentally excluded.
    pub fn exclude_process_tree(&mut self, pid: u32) -> Result<ProcIdentity> {
        let info = bsd_info(pid)?.ok_or_else(|| {
            AhrbError::Protocol(format!(
                "cannot exclude PID {pid}: the process does not exist or is not inspectable"
            ))
        })?;
        let identity = identity_of(&info);
        self.excluded_roots.insert(identity);
        Ok(identity)
    }

    /// Exclude an already-verified process identity and its descendants.
    pub fn exclude_verified_process_tree(&mut self, identity: ProcIdentity) {
        self.excluded_roots.insert(identity);
    }

    fn discover_from_table(
        &mut self,
        roots: &[u32],
        by_pid: &BTreeMap<u32, ProcBsdInfo>,
    ) -> Result<ProcessTree> {
        let root_pids: BTreeSet<u32> = roots.iter().copied().collect();
        self.verified_roots.retain(|pid, _| root_pids.contains(pid));
        self.verified_groups
            .retain(|identity, _| root_pids.contains(&identity.pid));
        let mut tree = ProcessTree::default();
        let mut owned_by_pid = BTreeMap::new();

        for pid in &root_pids {
            let Some(info) = by_pid.get(pid) else {
                continue;
            };
            let identity = identity_of(info);
            if let Some(verified) = self.verified_roots.get(pid) {
                if *verified != identity {
                    return Err(AhrbError::Protocol(format!(
                        "ownership root PID {pid} changed start-time identity from {} to {}",
                        verified.start_time, identity.start_time
                    )));
                }
            } else {
                self.verified_roots.insert(*pid, identity);
            }
            if info.pbi_pgid != 0 {
                self.verified_groups.insert(identity, info.pbi_pgid);
            }
            tree.roots.insert(identity);
            owned_by_pid.insert(*pid, identity);
            tree.members
                .insert(identity, process_info(info, ProcOwnership::DeclaredRoot));
        }

        let process_groups: BTreeSet<u32> = self.verified_groups.values().copied().collect();
        for (pid, info) in by_pid {
            if owned_by_pid.contains_key(pid) || !process_groups.contains(&info.pbi_pgid) {
                continue;
            }
            let identity = identity_of(info);
            owned_by_pid.insert(*pid, identity);
            tree.members.insert(
                identity,
                process_info(info, ProcOwnership::ProcessGroupMember),
            );
        }

        // Retain an already-attributed process across reparenting, but only while
        // its `(pid,start-time)` identity still matches.
        for identity in &self.known {
            if owned_by_pid.contains_key(&identity.pid) {
                continue;
            }
            if let Some(info) = by_pid.get(&identity.pid) {
                if identity_of(info) == *identity {
                    owned_by_pid.insert(identity.pid, *identity);
                    tree.members
                        .insert(*identity, process_info(info, ProcOwnership::Reparented));
                }
            }
        }

        add_descendants(by_pid, &mut owned_by_pid, &mut tree.members);

        let mut excluded_by_pid = BTreeMap::new();
        for identity in &self.excluded_roots {
            if let Some(info) = by_pid.get(&identity.pid) {
                if identity_of(info) == *identity {
                    excluded_by_pid.insert(identity.pid, *identity);
                }
            }
        }
        let mut excluded_members = BTreeMap::new();
        add_descendants(by_pid, &mut excluded_by_pid, &mut excluded_members);
        for identity in excluded_by_pid.values() {
            tree.roots.remove(identity);
            tree.members.remove(identity);
        }

        self.known = tree.members.keys().copied().collect();
        Ok(tree)
    }
}

impl Sampler for MacOsSampler {
    fn discover(&mut self, roots: &[u32]) -> Result<ProcessTree> {
        let discovery_started = Instant::now();
        let mut by_pid = BTreeMap::new();
        let mut pending: BTreeSet<u32> = roots.iter().copied().collect();
        let mut process_groups: BTreeSet<u32> = self.verified_groups.values().copied().collect();
        for pid in roots {
            if let Some(info) = bsd_info(*pid)? {
                if info.pbi_pgid != 0 {
                    process_groups.insert(info.pbi_pgid);
                }
                by_pid.insert(*pid, info);
            }
        }
        for process_group in process_groups {
            pending.extend(list_pids_for(PROC_PGRP_ONLY, process_group)?);
        }
        pending.extend(self.known.iter().map(|identity| identity.pid));
        pending.extend(self.excluded_roots.iter().map(|identity| identity.pid));
        while let Some(pid) = pending.pop_first() {
            if by_pid.contains_key(&pid) {
                continue;
            }
            if let Some(info) = bsd_info(pid)? {
                by_pid.insert(pid, info);
                pending.extend(list_pids_for(PROC_PPID_ONLY, pid)?);
            }
        }
        let result = self.discover_from_table(roots, &by_pid);
        self.discovery_ns = duration_ns(discovery_started.elapsed());
        result
    }

    fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample> {
        let collection_started = Instant::now();
        let mut rss_bytes = 0_u64;
        let mut footprint_bytes = 0_u64;
        let mut rss_crosscheck_bytes = 0_u64;
        let mut open_fds = 0_u64;
        let mut thread_count = 0_u64;
        let mut complete_thread_count = true;
        let mut complete_crosscheck = true;
        let mut counters = Vec::new();
        let mut live_cpu = BTreeMap::new();
        let mut child_rollup_cpu = BTreeMap::new();
        let identities_by_pid: BTreeMap<u32, ProcIdentity> = tree
            .members
            .keys()
            .map(|identity| (identity.pid, *identity))
            .collect();
        let current_parents: BTreeMap<ProcIdentity, ProcIdentity> = tree
            .members
            .iter()
            .filter_map(|(identity, process)| {
                identities_by_pid
                    .get(&process.ppid)
                    .map(|parent| (*identity, *parent))
            })
            .collect();

        for (identity, process) in &tree.members {
            let Some(current) = bsd_info(identity.pid)? else {
                continue;
            };
            if identity_of(&current) != *identity {
                continue;
            }

            let Some(usage) = rusage(identity.pid)? else {
                continue;
            };
            rss_bytes = rss_bytes.saturating_add(usage.ri_resident_size);
            footprint_bytes = footprint_bytes.saturating_add(usage.ri_phys_footprint);
            let process_cpu_ns = usage.ri_user_time.saturating_add(usage.ri_system_time);
            live_cpu.insert(*identity, process_cpu_ns);
            child_rollup_cpu.insert(
                *identity,
                usage
                    .ri_child_user_time
                    .saturating_add(usage.ri_child_system_time),
            );
            let process_open_fds = u64::from(current.pbi_nfiles);
            open_fds = open_fds.saturating_add(process_open_fds);
            let (process_threads, proc_crosscheck) = match task_details(identity.pid)? {
                Some(details) => {
                    let threads = u64::try_from(details.pti_threadnum).map_err(|_| {
                        AhrbError::Protocol(format!(
                            "proc_pidinfo returned a negative thread count for PID {}",
                            identity.pid
                        ))
                    })?;
                    thread_count = thread_count.saturating_add(threads);
                    (Some(threads), Some(details.pti_resident_size))
                }
                None => {
                    complete_thread_count = false;
                    (None, None)
                }
            };
            let process_crosscheck = task_vm_resident(identity.pid).or(proc_crosscheck);
            match process_crosscheck {
                Some(value) => {
                    rss_crosscheck_bytes = rss_crosscheck_bytes.saturating_add(value);
                }
                None => complete_crosscheck = false,
            }
            counters.push(MacProcessCounters {
                process: process_info(&current, process.ownership.clone()),
                rss_bytes: usage.ri_resident_size,
                footprint_bytes: usage.ri_phys_footprint,
                rss_crosscheck_bytes: process_crosscheck,
                cpu_ns: process_cpu_ns,
                open_fds: process_open_fds,
                thread_count: process_threads,
            });
        }

        let unsampled_child_cpu_ns =
            self.reconcile_child_cpu(&live_cpu, &current_parents, &child_rollup_cpu)?;
        let cpu_ns = self
            .cpu
            .update(&live_cpu)?
            .saturating_add(unsampled_child_cpu_ns);
        let elapsed_ns = duration_ns(self.started.elapsed());
        let wall_time = SystemTime::now();
        let phase = phase.to_owned();
        let processes = counters
            .iter()
            .map(|counter| counter.process.clone())
            .collect();
        let process_samples = counters
            .into_iter()
            .map(|counter| ProcessSample {
                elapsed_ns,
                wall_time,
                phase: phase.clone(),
                process: counter.process,
                rss_bytes: counter.rss_bytes,
                pss_bytes: None,
                private_bytes: None,
                footprint_bytes: Some(counter.footprint_bytes),
                rss_crosscheck_bytes: counter.rss_crosscheck_bytes,
                cpu_ns: counter.cpu_ns,
                open_fds: Some(counter.open_fds),
                thread_count: counter.thread_count,
            })
            .collect();

        Ok(Sample {
            elapsed_ns,
            wall_time,
            phase,
            rss_bytes,
            pss_bytes: None,
            private_bytes: None,
            footprint_bytes: Some(footprint_bytes),
            rss_crosscheck_bytes: complete_crosscheck.then_some(rss_crosscheck_bytes),
            cgroup_memory_bytes: None,
            cgroup_peak_bytes: None,
            cpu_ns,
            open_fds: Some(open_fds),
            thread_count: complete_thread_count.then_some(thread_count),
            collection_ns: self
                .discovery_ns
                .saturating_add(duration_ns(collection_started.elapsed())),
            collection_wall_ns: self
                .discovery_ns
                .saturating_add(duration_ns(collection_started.elapsed())),
            processes,
            process_samples,
        })
    }
}

fn add_descendants(
    by_pid: &BTreeMap<u32, ProcBsdInfo>,
    owned_by_pid: &mut BTreeMap<u32, ProcIdentity>,
    members: &mut BTreeMap<ProcIdentity, ProcessInfo>,
) {
    // Input and map iteration are ordered, so evidence is deterministic.
    loop {
        let mut added = false;
        for (pid, info) in by_pid {
            if owned_by_pid.contains_key(pid) {
                continue;
            }
            let Some(parent) = owned_by_pid.get(&info.pbi_ppid) else {
                continue;
            };
            let identity = identity_of(info);
            if identity.start_time < parent.start_time {
                continue;
            }
            owned_by_pid.insert(*pid, identity);
            members.insert(identity, process_info(info, ProcOwnership::Descendant));
            added = true;
        }
        if !added {
            break;
        }
    }
}

fn list_pids_for(kind: u32, type_info: u32) -> Result<Vec<u32>> {
    // Direct-child sets are normally tiny. Starting with a useful buffer avoids
    // libproc's separate sizing syscall on every 10 ms membership refresh; a
    // saturated buffer is still retried geometrically below.
    let mut capacity = 64_usize;

    for _ in 0..3 {
        let mut pids = vec![0_u32; capacity];
        let buffer_bytes = capacity
            .checked_mul(size_of::<u32>())
            .and_then(|value| c_int::try_from(value).ok())
            .ok_or_else(|| AhrbError::Protocol("PID list is too large".to_owned()))?;
        // SAFETY: the vector exposes `buffer_bytes` writable bytes and libproc
        // writes a packed array of 32-bit process IDs.
        let used = unsafe {
            proc_listpids(
                kind,
                type_info,
                pids.as_mut_ptr().cast::<c_void>(),
                buffer_bytes,
            )
        };
        if used < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let used = usize::try_from(used).map_err(|_| {
            AhrbError::Protocol("libproc returned an invalid byte count".to_owned())
        })?;
        if used < capacity.saturating_mul(size_of::<u32>()) {
            pids.truncate(used / size_of::<u32>());
            pids.retain(|pid| *pid != 0);
            pids.sort_unstable();
            pids.dedup();
            return Ok(pids);
        }
        capacity = capacity.saturating_mul(2);
    }

    Err(AhrbError::Protocol(
        "process list changed too quickly to capture safely".to_owned(),
    ))
}

fn bsd_info(pid: u32) -> Result<Option<ProcBsdInfo>> {
    let pid = c_int::try_from(pid)
        .map_err(|_| AhrbError::Validation("PID exceeds Darwin pid_t range".to_owned()))?;
    // SAFETY: the all-zero representation is valid for this C output struct.
    let mut info: ProcBsdInfo = unsafe { zeroed() };
    let size = c_int::try_from(size_of::<ProcBsdInfo>())
        .map_err(|_| AhrbError::Protocol("proc_bsdinfo size overflow".to_owned()))?;
    // SAFETY: `info` is a writable buffer of exactly the supplied size.
    let read = unsafe {
        proc_pidinfo(
            pid,
            PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut ProcBsdInfo).cast::<c_void>(),
            size,
        )
    };
    if read == size {
        return Ok(Some(info));
    }
    if read == 0 {
        return Ok(None);
    }
    Err(AhrbError::Protocol(format!(
        "proc_pidinfo returned a short proc_bsdinfo for PID {pid}: {read}/{size} bytes"
    )))
}

fn task_details(pid: u32) -> Result<Option<ProcTaskInfo>> {
    let pid = c_int::try_from(pid)
        .map_err(|_| AhrbError::Validation("PID exceeds Darwin pid_t range".to_owned()))?;
    // SAFETY: the all-zero representation is valid for this C output struct.
    let mut info: ProcTaskInfo = unsafe { zeroed() };
    let size = c_int::try_from(size_of::<ProcTaskInfo>())
        .map_err(|_| AhrbError::Protocol("proc_taskinfo size overflow".to_owned()))?;
    // SAFETY: `info` is a writable buffer of exactly the supplied size.
    let read = unsafe {
        proc_pidinfo(
            pid,
            PROC_PIDTASKINFO,
            0,
            (&mut info as *mut ProcTaskInfo).cast::<c_void>(),
            size,
        )
    };
    if read == size {
        return Ok(Some(info));
    }
    if read == 0 {
        return Ok(None);
    }
    Err(AhrbError::Protocol(format!(
        "proc_pidinfo returned a short proc_taskinfo for PID {pid}: {read}/{size} bytes"
    )))
}

fn rusage(pid: u32) -> Result<Option<RusageInfoV4>> {
    let pid = c_int::try_from(pid)
        .map_err(|_| AhrbError::Validation("PID exceeds Darwin pid_t range".to_owned()))?;
    // SAFETY: the all-zero representation is valid for this C output struct.
    let mut usage: RusageInfoV4 = unsafe { zeroed() };
    // SAFETY: `usage` has the exact layout required by `RUSAGE_INFO_V4`.
    let result = unsafe {
        proc_pid_rusage(
            pid,
            RUSAGE_INFO_V4,
            (&mut usage as *mut RusageInfoV4).cast::<c_void>(),
        )
    };
    if result == 0 {
        return Ok(Some(usage));
    }
    let error = std::io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
        Ok(None)
    } else {
        Err(error.into())
    }
}

fn task_vm_resident(pid: u32) -> Option<u64> {
    let pid = c_int::try_from(pid).ok()?;
    let mut task = 0_u32;
    // SAFETY: reading the exported send right is safe; `task` is a valid output.
    let self_task = unsafe { mach_task_self_ };
    // SAFETY: all scalar values and the output pointer satisfy the Mach API.
    if unsafe { task_for_pid(self_task, pid, &mut task) } != KERN_SUCCESS {
        return None;
    }

    // SAFETY: the all-zero representation is valid for this C output struct.
    let mut info: TaskVmInfoRev1 = unsafe { zeroed() };
    let mut count = u32::try_from(size_of::<TaskVmInfoRev1>() / size_of::<i32>()).ok()?;
    // SAFETY: the buffer contains `count` naturally aligned 32-bit units.
    let result = unsafe {
        task_info(
            task,
            TASK_VM_INFO,
            (&mut info as *mut TaskVmInfoRev1).cast::<i32>(),
            &mut count,
        )
    };
    // SAFETY: `task` is a send right returned by `task_for_pid` above.
    let _deallocate_result = unsafe { mach_port_deallocate(self_task, task) };
    (result == KERN_SUCCESS).then_some(info.resident_size)
}

fn identity_of(info: &ProcBsdInfo) -> ProcIdentity {
    ProcIdentity {
        pid: info.pbi_pid,
        start_time: info
            .pbi_start_tvsec
            .saturating_mul(1_000_000)
            .saturating_add(info.pbi_start_tvusec),
    }
}

fn process_info(info: &ProcBsdInfo, ownership: ProcOwnership) -> ProcessInfo {
    let name = c_chars(&info.pbi_name);
    ProcessInfo {
        identity: identity_of(info),
        ppid: info.pbi_ppid,
        command: if name.is_empty() {
            c_chars(&info.pbi_comm)
        } else {
            name
        },
        ownership,
    }
}

fn c_chars<const N: usize>(value: &[c_char; N]) -> String {
    let bytes: Vec<u8> = value
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| *byte as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).map_or(u64::MAX, |value| value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_process(pid: u32, ppid: u32, start_time: u64, name: &str) -> ProcBsdInfo {
        // SAFETY: the all-zero representation is valid for this C data struct.
        let mut info: ProcBsdInfo = unsafe { zeroed() };
        info.pbi_pid = pid;
        info.pbi_ppid = ppid;
        info.pbi_start_tvsec = start_time;
        info.pbi_nfiles = 3;
        for (slot, byte) in info
            .pbi_name
            .iter_mut()
            .zip(name.as_bytes().iter().copied())
        {
            *slot = byte as c_char;
        }
        info
    }

    fn fixture_table(processes: Vec<ProcBsdInfo>) -> BTreeMap<u32, ProcBsdInfo> {
        processes
            .into_iter()
            .map(|process| (process.pbi_pid, process))
            .collect()
    }

    #[test]
    fn discovers_recursive_roots_deduplicates_and_excludes_subtrees() -> Result<()> {
        let launcher = fixture_process(100, 1, 10, "launcher");
        let daemon = fixture_process(200, 100, 11, "daemon");
        let worker = fixture_process(300, 200, 12, "worker");
        let fake_model = fixture_process(400, 100, 13, "fake-model");
        let fake_child = fixture_process(500, 400, 14, "fake-child");
        let unrelated = fixture_process(600, 1, 15, "unrelated");
        let table = fixture_table(vec![
            launcher, daemon, worker, fake_model, fake_child, unrelated,
        ]);

        let mut sampler = MacOsSampler::default();
        sampler.exclude_verified_process_tree(identity_of(&fake_model));
        let tree = sampler.discover_from_table(&[200, 100, 200], &table)?;
        let pids: Vec<u32> = tree.members.keys().map(|identity| identity.pid).collect();
        assert_eq!(pids, vec![100, 200, 300]);
        assert_eq!(tree.roots.len(), 2);
        assert!(matches!(
            tree.members
                .values()
                .find(|process| process.identity.pid == 300)
                .map(|process| &process.ownership),
            Some(ProcOwnership::Descendant)
        ));
        Ok(())
    }

    #[test]
    fn retains_reparented_identity_but_rejects_root_pid_reuse() -> Result<()> {
        let root = fixture_process(100, 1, 10, "root");
        let child = fixture_process(200, 100, 11, "worker");
        let mut sampler = MacOsSampler::default();
        let first = fixture_table(vec![root, child]);
        let initial = sampler.discover_from_table(&[100], &first)?;
        assert_eq!(initial.members.len(), 2);

        let reparented_child = fixture_process(200, 1, 11, "worker");
        let after_reparent = fixture_table(vec![reparented_child]);
        let retained = sampler.discover_from_table(&[100], &after_reparent)?;
        assert!(matches!(
            retained
                .members
                .values()
                .find(|process| process.identity.pid == 200)
                .map(|process| &process.ownership),
            Some(ProcOwnership::Reparented)
        ));

        let reused_root = fixture_process(100, 1, 99, "other");
        let reused = fixture_table(vec![reused_root, reparented_child]);
        let error = sampler
            .discover_from_table(&[100], &reused)
            .expect_err("reused root PID must not replace a verified identity");
        assert!(error.to_string().contains("changed start-time identity"));
        Ok(())
    }

    #[test]
    fn discovers_and_samples_current_process() -> Result<()> {
        let mut sampler = MacOsSampler::default();
        let tree = sampler.discover(&[std::process::id()])?;
        assert!(
            tree.members
                .keys()
                .any(|identity| identity.pid == std::process::id())
        );
        let sample = sampler.sample(&tree, "self")?;
        assert!(sample.rss_bytes > 0);
        assert!(sample.footprint_bytes.unwrap_or(0) > 0);
        assert!(sample.open_fds.unwrap_or(0) > 0);
        assert!(sample.thread_count.unwrap_or(0) > 0);
        assert!(sample.collection_ns > 0);
        assert_eq!(sample.process_samples.len(), sample.processes.len());
        assert_eq!(
            sample
                .process_samples
                .iter()
                .map(|process| process.rss_bytes)
                .sum::<u64>(),
            sample.rss_bytes
        );
        assert!(sample.process_samples.iter().all(|process| {
            process.elapsed_ns == sample.elapsed_ns
                && process.wall_time == sample.wall_time
                && process.phase == sample.phase
        }));
        let serialized = serde_json::to_value(&sample.process_samples[0])?;
        assert!(serialized.get("identity").is_some());
        assert!(serialized.get("rss_bytes").is_some());
        assert!(serialized.get("process").is_none());
        assert_eq!(sample.phase, "self");
        Ok(())
    }

    #[test]
    fn reconciles_sampled_and_unsampled_short_lived_child_cpu() -> Result<()> {
        let parent = ProcIdentity {
            pid: 100,
            start_time: 1,
        };
        let child = ProcIdentity {
            pid: 200,
            start_time: 2,
        };
        let mut sampler = MacOsSampler::default();
        assert_eq!(
            sampler.reconcile_child_cpu(
                &BTreeMap::from([(parent, 50), (child, 10)]),
                &BTreeMap::from([(child, parent)]),
                &BTreeMap::from([(parent, 0), (child, 0)]),
            )?,
            0
        );
        assert_eq!(
            sampler.reconcile_child_cpu(
                &BTreeMap::from([(parent, 55)]),
                &BTreeMap::new(),
                &BTreeMap::from([(parent, 12)]),
            )?,
            2
        );

        let mut unsampled = MacOsSampler::default();
        assert_eq!(
            unsampled.reconcile_child_cpu(
                &BTreeMap::from([(parent, 50)]),
                &BTreeMap::new(),
                &BTreeMap::from([(parent, 0)]),
            )?,
            0
        );
        assert_eq!(
            unsampled.reconcile_child_cpu(
                &BTreeMap::from([(parent, 55)]),
                &BTreeMap::new(),
                &BTreeMap::from([(parent, 7)]),
            )?,
            7
        );
        Ok(())
    }
}
