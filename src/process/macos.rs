//! macOS libproc whole-tree sampler.

use crate::process::{ProcIdentity, ProcOwnership, ProcessInfo, ProcessTree, Sample, Sampler};
use crate::{AhrbError, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_char, c_int, c_void};
use std::mem::{size_of, zeroed};
use std::time::{Instant, SystemTime};

const PROC_ALL_PIDS: u32 = 1;
const PROC_PIDTBSDINFO: c_int = 3;
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
}

impl Default for MacOsSampler {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            known: BTreeSet::new(),
        }
    }
}

impl Sampler for MacOsSampler {
    fn discover(&mut self, roots: &[u32]) -> Result<ProcessTree> {
        let mut by_pid = BTreeMap::new();
        for pid in list_pids()? {
            if let Some(info) = bsd_info(pid)? {
                by_pid.insert(pid, info);
            }
        }

        let root_pids: BTreeSet<u32> = roots.iter().copied().collect();
        let mut tree = ProcessTree::default();
        let mut owned_by_pid = BTreeMap::new();

        for pid in &root_pids {
            if let Some(info) = by_pid.get(pid) {
                let identity = identity_of(info);
                tree.roots.insert(identity);
                owned_by_pid.insert(*pid, identity);
                tree.members
                    .insert(identity, process_info(info, ProcOwnership::DeclaredRoot));
            }
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

        // Input and map iteration are ordered, so evidence is deterministic.
        loop {
            let mut added = false;
            for (pid, info) in &by_pid {
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
                tree.members
                    .insert(identity, process_info(info, ProcOwnership::Descendant));
                added = true;
            }
            if !added {
                break;
            }
        }

        self.known = tree.members.keys().copied().collect();
        Ok(tree)
    }

    fn sample(&mut self, tree: &ProcessTree, phase: &str) -> Result<Sample> {
        let collection_started = Instant::now();
        let mut rss_bytes = 0_u64;
        let mut footprint_bytes = 0_u64;
        let mut cpu_ns = 0_u64;
        let mut rss_crosscheck_bytes = 0_u64;
        let mut complete_crosscheck = true;
        let mut processes = Vec::new();

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
            cpu_ns = cpu_ns
                .saturating_add(usage.ri_user_time)
                .saturating_add(usage.ri_system_time);
            match task_vm_resident(identity.pid) {
                Some(value) => {
                    rss_crosscheck_bytes = rss_crosscheck_bytes.saturating_add(value);
                }
                None => complete_crosscheck = false,
            }
            processes.push(process.clone());
        }

        Ok(Sample {
            elapsed_ns: duration_ns(self.started.elapsed()),
            wall_time: SystemTime::now(),
            phase: phase.to_owned(),
            rss_bytes,
            pss_bytes: None,
            private_bytes: None,
            footprint_bytes: Some(footprint_bytes),
            rss_crosscheck_bytes: complete_crosscheck.then_some(rss_crosscheck_bytes),
            cgroup_memory_bytes: None,
            cgroup_peak_bytes: None,
            cpu_ns,
            collection_ns: duration_ns(collection_started.elapsed()),
            processes,
        })
    }
}

fn list_pids() -> Result<Vec<u32>> {
    // SAFETY: a null buffer with size zero is the documented sizing call.
    let bytes = unsafe { proc_listpids(PROC_ALL_PIDS, 0, std::ptr::null_mut(), 0) };
    if bytes < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let initial = usize::try_from(bytes)
        .map_err(|_| AhrbError::Protocol("libproc returned a negative PID-list size".to_owned()))?
        / size_of::<u32>();
    let mut capacity = initial.saturating_add(64).max(64);

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
                PROC_ALL_PIDS,
                0,
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
        assert_eq!(sample.phase, "self");
        Ok(())
    }
}
