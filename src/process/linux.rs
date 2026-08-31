//! Linux cgroup-v2 and procfs whole-tree sampler.

use crate::process::{
    ProcIdentity, ProcOwnership, ProcessInfo, ProcessSample, ProcessTree, Sample, Sampler,
    TreeCpuTracker,
};
use crate::{AhrbError, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

/// Linux sampler backed by a dedicated cgroup-v2 when configured, with a
/// start-time-checked procfs ancestry fallback.
#[derive(Debug)]
pub struct LinuxSampler {
    started: Instant,
    proc_root: PathBuf,
    cgroup: Option<PathBuf>,
    known: BTreeSet<ProcIdentity>,
    verified_groups: BTreeMap<ProcIdentity, u32>,
    clock_ticks_per_second: u64,
    cpu: TreeCpuTracker,
    last_cgroup_cpu_ns: Option<u64>,
}

impl Default for LinuxSampler {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            proc_root: PathBuf::from("/proc"),
            cgroup: None,
            known: BTreeSet::new(),
            verified_groups: BTreeMap::new(),
            clock_ticks_per_second: clock_ticks_per_second(),
            cpu: TreeCpuTracker::default(),
            last_cgroup_cpu_ns: None,
        }
    }
}

impl LinuxSampler {
    /// Use durable membership and aggregate counters from an existing cgroup-v2
    /// directory. The lifecycle owner remains responsible for creating and
    /// removing the benchmark cgroup.
    pub fn with_cgroup(path: impl Into<PathBuf>) -> Self {
        Self {
            cgroup: Some(path.into()),
            ..Self::default()
        }
    }

    #[cfg(test)]
    fn with_roots(proc_root: PathBuf, cgroup: Option<PathBuf>) -> Self {
        Self {
            proc_root,
            cgroup,
            ..Self::default()
        }
    }
}

pub(crate) fn matching_processes(
    executable_name: &str,
    expected_environment: &BTreeMap<String, String>,
) -> Result<Vec<u32>> {
    let mut matches = Vec::new();
    let mut entries = fs::read_dir("/proc")?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let directory = entry.path();
        let command = match fs::read_to_string(directory.join("comm")) {
            Ok(command) => command.trim_end().to_owned(),
            Err(error) if transient_process_error(&error) => continue,
            Err(error) => return Err(error.into()),
        };
        if command != executable_name {
            continue;
        }
        let bytes = match fs::read(directory.join("environ")) {
            Ok(bytes) => bytes,
            Err(error) if transient_process_error(&error) => continue,
            Err(error) => return Err(error.into()),
        };
        let environment = parse_environ(&bytes);
        if expected_environment
            .iter()
            .all(|(name, value)| environment.get(name) == Some(value))
        {
            matches.push(pid);
        }
    }
    matches.sort_unstable();
    matches.dedup();
    Ok(matches)
}

fn transient_process_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::InvalidInput
    )
}

fn parse_environ(bytes: &[u8]) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    for item in bytes
        .split(|byte| *byte == 0)
        .filter(|item| !item.is_empty())
    {
        let text = String::from_utf8_lossy(item);
        if let Some((name, value)) = text.split_once('=') {
            environment.insert(name.to_owned(), value.to_owned());
        }
    }
    environment
}

impl Sampler for LinuxSampler {
    fn discover(&mut self, roots: &[u32]) -> Result<ProcessTree> {
        let by_pid = read_process_table(&self.proc_root)?;
        let cgroup_pids = match &self.cgroup {
            Some(path) => read_pid_file(&path.join("cgroup.procs"))?,
            None => BTreeSet::new(),
        };
        let root_pids: BTreeSet<u32> = roots.iter().copied().collect();
        let mut tree = ProcessTree::default();
        let mut owned_by_pid = BTreeMap::new();

        for pid in &cgroup_pids {
            if let Some(process) = by_pid.get(pid) {
                let identity = process.identity();
                owned_by_pid.insert(*pid, identity);
                tree.members
                    .insert(identity, process.to_info(ProcOwnership::CgroupMember));
            }
        }
        for pid in &root_pids {
            if let Some(process) = by_pid.get(pid) {
                let identity = process.identity();
                if process.process_group != 0 {
                    self.verified_groups.insert(identity, process.process_group);
                }
                tree.roots.insert(identity);
                owned_by_pid.insert(*pid, identity);
                tree.members
                    .insert(identity, process.to_info(ProcOwnership::DeclaredRoot));
            }
        }
        self.verified_groups
            .retain(|identity, _| root_pids.contains(&identity.pid));
        let process_groups: BTreeSet<u32> = self.verified_groups.values().copied().collect();
        for (pid, process) in &by_pid {
            if owned_by_pid.contains_key(pid) || !process_groups.contains(&process.process_group) {
                continue;
            }
            let identity = process.identity();
            owned_by_pid.insert(*pid, identity);
            tree.members
                .insert(identity, process.to_info(ProcOwnership::ProcessGroupMember));
        }

        // A cgroup is authoritative across reparenting. Under the reduced-
        // confidence procfs fallback, preserve previously witnessed identities.
        if self.cgroup.is_none() {
            for identity in &self.known {
                if owned_by_pid.contains_key(&identity.pid) {
                    continue;
                }
                if let Some(process) = by_pid.get(&identity.pid) {
                    if process.identity() == *identity {
                        owned_by_pid.insert(identity.pid, *identity);
                        tree.members
                            .insert(*identity, process.to_info(ProcOwnership::Reparented));
                    }
                }
            }
        }

        loop {
            let mut added = false;
            for (pid, process) in &by_pid {
                if owned_by_pid.contains_key(pid) {
                    continue;
                }
                let Some(parent) = owned_by_pid.get(&process.ppid) else {
                    continue;
                };
                let identity = process.identity();
                if identity.start_time < parent.start_time {
                    continue;
                }
                owned_by_pid.insert(*pid, identity);
                tree.members
                    .insert(identity, process.to_info(ProcOwnership::Descendant));
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
        let mut pss_bytes = 0_u64;
        let mut private_bytes = 0_u64;
        let mut open_fds = 0_u64;
        let mut thread_count = 0_u64;
        let mut counters = Vec::new();
        let mut live_cpu = BTreeMap::new();
        let mut complete_pss = true;
        let mut complete_private = true;
        let mut complete_open_fds = true;

        for (identity, process) in &tree.members {
            let stat_path = self.proc_root.join(identity.pid.to_string()).join("stat");
            let Some(stat_text) = read_transient_text(&stat_path)? else {
                continue;
            };
            let current = parse_stat(identity.pid, &stat_text)?;
            if current.identity() != *identity {
                continue;
            }

            let rollup_path = self
                .proc_root
                .join(identity.pid.to_string())
                .join("smaps_rollup");
            let Some(rollup_text) = read_transient_text(&rollup_path)? else {
                continue;
            };
            let memory = parse_smaps_rollup(&rollup_text)?;
            rss_bytes = rss_bytes.saturating_add(memory.rss_bytes);
            match memory.pss_bytes {
                Some(value) => pss_bytes = pss_bytes.saturating_add(value),
                None => complete_pss = false,
            }
            match memory.private_bytes {
                Some(value) => private_bytes = private_bytes.saturating_add(value),
                None => complete_private = false,
            }
            let process_cpu_ns = ticks_to_ns(
                current.user_ticks.saturating_add(current.system_ticks),
                self.clock_ticks_per_second,
            );
            live_cpu.insert(*identity, process_cpu_ns);
            let process_open_fds =
                read_fd_count(&self.proc_root.join(identity.pid.to_string()).join("fd"))?;
            match process_open_fds {
                Some(value) => open_fds = open_fds.saturating_add(value),
                None => complete_open_fds = false,
            }
            thread_count = thread_count.saturating_add(current.thread_count);
            counters.push(LinuxProcessCounters {
                process: current.to_info(process.ownership.clone()),
                memory,
                cpu_ns: process_cpu_ns,
                open_fds: process_open_fds,
                thread_count: current.thread_count,
            });
        }

        let tracked_cpu_ns = self.cpu.update(&live_cpu)?;

        let cgroup_memory_bytes = self
            .cgroup
            .as_ref()
            .map(|path| read_u64_file(&path.join("memory.current")))
            .transpose()?
            .flatten();
        let cgroup_peak_bytes = self
            .cgroup
            .as_ref()
            .map(|path| read_u64_file(&path.join("memory.peak")))
            .transpose()?
            .flatten();
        let cgroup_cpu_ns = match &self.cgroup {
            Some(path) => read_cpu_stat(&path.join("cpu.stat"))?,
            None => None,
        };
        if let (Some(previous), Some(current)) = (self.last_cgroup_cpu_ns, cgroup_cpu_ns) {
            if current < previous {
                return Err(AhrbError::Protocol(format!(
                    "cgroup cumulative CPU regressed from {previous} to {current}"
                )));
            }
        }
        if let Some(current) = cgroup_cpu_ns {
            self.last_cgroup_cpu_ns = Some(current);
        }
        let cpu_ns = cgroup_cpu_ns.unwrap_or(tracked_cpu_ns);
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
                rss_bytes: counter.memory.rss_bytes,
                pss_bytes: counter.memory.pss_bytes,
                private_bytes: counter.memory.private_bytes,
                footprint_bytes: None,
                rss_crosscheck_bytes: None,
                cpu_ns: counter.cpu_ns,
                open_fds: counter.open_fds,
                thread_count: Some(counter.thread_count),
            })
            .collect();

        Ok(Sample {
            elapsed_ns,
            wall_time,
            phase,
            rss_bytes,
            pss_bytes: complete_pss.then_some(pss_bytes),
            private_bytes: complete_private.then_some(private_bytes),
            footprint_bytes: None,
            rss_crosscheck_bytes: None,
            cgroup_memory_bytes,
            cgroup_peak_bytes,
            cpu_ns,
            open_fds: complete_open_fds.then_some(open_fds),
            thread_count: Some(thread_count),
            collection_ns: duration_ns(collection_started.elapsed()),
            collection_wall_ns: duration_ns(collection_started.elapsed()),
            processes,
            process_samples,
        })
    }
}

#[derive(Clone, Debug)]
struct LinuxProcess {
    pid: u32,
    ppid: u32,
    process_group: u32,
    command: String,
    start_ticks: u64,
    user_ticks: u64,
    system_ticks: u64,
    thread_count: u64,
}

struct LinuxProcessCounters {
    process: ProcessInfo,
    memory: SmapsRollup,
    cpu_ns: u64,
    open_fds: Option<u64>,
    thread_count: u64,
}

impl LinuxProcess {
    fn identity(&self) -> ProcIdentity {
        ProcIdentity {
            pid: self.pid,
            start_time: self.start_ticks,
        }
    }

    fn to_info(&self, ownership: ProcOwnership) -> ProcessInfo {
        ProcessInfo {
            identity: self.identity(),
            ppid: self.ppid,
            command: self.command.clone(),
            ownership,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SmapsRollup {
    rss_bytes: u64,
    pss_bytes: Option<u64>,
    private_bytes: Option<u64>,
}

fn read_process_table(proc_root: &Path) -> Result<BTreeMap<u32, LinuxProcess>> {
    let mut table = BTreeMap::new();
    for entry in fs::read_dir(proc_root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let stat_path = entry.path().join("stat");
        let Some(stat) = read_transient_text(&stat_path)? else {
            continue;
        };
        table.insert(pid, parse_stat(pid, &stat)?);
    }
    Ok(table)
}

fn parse_stat(pid: u32, text: &str) -> Result<LinuxProcess> {
    let open = text.find('(').ok_or_else(|| {
        AhrbError::Protocol(format!("/proc/{pid}/stat is missing the command field"))
    })?;
    let close = text.rfind(')').ok_or_else(|| {
        AhrbError::Protocol(format!(
            "/proc/{pid}/stat has an unterminated command field"
        ))
    })?;
    if close <= open {
        return Err(AhrbError::Protocol(format!(
            "/proc/{pid}/stat has an invalid command field"
        )));
    }
    let command = text[open + 1..close].to_owned();
    let fields: Vec<&str> = text[close + 1..].split_whitespace().collect();
    // The first item is field 3 (`state`), so indexes 1, 2, 11, 12, and 19 are
    // PPID, process group, utime, stime, and starttime respectively.
    if fields.len() <= 19 {
        return Err(AhrbError::Protocol(format!(
            "/proc/{pid}/stat ended before starttime"
        )));
    }
    Ok(LinuxProcess {
        pid,
        ppid: parse_stat_number(pid, "ppid", fields[1])?,
        process_group: parse_stat_number(pid, "pgrp", fields[2])?,
        command,
        user_ticks: parse_stat_number(pid, "utime", fields[11])?,
        system_ticks: parse_stat_number(pid, "stime", fields[12])?,
        thread_count: parse_stat_number(pid, "num_threads", fields[17])?,
        start_ticks: parse_stat_number(pid, "starttime", fields[19])?,
    })
}

fn parse_stat_number<T>(pid: u32, field: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value.parse::<T>().map_err(|_| {
        AhrbError::Protocol(format!(
            "/proc/{pid}/stat contains an invalid {field}: {value}"
        ))
    })
}

fn parse_smaps_rollup(text: &str) -> Result<SmapsRollup> {
    let mut rss = None;
    let mut pss = None;
    let mut private_clean = None;
    let mut private_dirty = None;
    let mut private_hugetlb = None;
    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let parsed = match name {
            "Rss" | "Pss" | "Private_Clean" | "Private_Dirty" | "Private_Hugetlb" => {
                Some(parse_kib_value(name, value)?)
            }
            _ => None,
        };
        match (name, parsed) {
            ("Rss", value) => rss = value,
            ("Pss", value) => pss = value,
            ("Private_Clean", value) => private_clean = value,
            ("Private_Dirty", value) => private_dirty = value,
            ("Private_Hugetlb", value) => private_hugetlb = value,
            _ => {}
        }
    }
    let rss_bytes = rss.ok_or_else(|| {
        AhrbError::Protocol("smaps_rollup did not contain an Rss counter".to_owned())
    })?;
    let private_bytes = match (private_clean, private_dirty) {
        (Some(clean), Some(dirty)) => {
            let huge = private_hugetlb.map_or(0, |value| value);
            Some(clean.saturating_add(dirty).saturating_add(huge))
        }
        _ => None,
    };
    Ok(SmapsRollup {
        rss_bytes,
        pss_bytes: pss,
        private_bytes,
    })
}

fn parse_kib_value(name: &str, value: &str) -> Result<u64> {
    let mut fields = value.split_whitespace();
    let number = fields
        .next()
        .ok_or_else(|| AhrbError::Protocol(format!("smaps_rollup {name} is missing a value")))?;
    let kib = number.parse::<u64>().map_err(|_| {
        AhrbError::Protocol(format!(
            "smaps_rollup {name} has an invalid value: {number}"
        ))
    })?;
    if let Some(unit) = fields.next() {
        if unit != "kB" {
            return Err(AhrbError::Protocol(format!(
                "smaps_rollup {name} uses unsupported unit {unit}"
            )));
        }
    }
    Ok(kib.saturating_mul(1_024))
}

fn read_pid_file(path: &Path) -> Result<BTreeSet<u32>> {
    let text = fs::read_to_string(path)?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim().parse::<u32>().map_err(|_| {
                AhrbError::Protocol(format!(
                    "{} contains an invalid PID: {line}",
                    path.display()
                ))
            })
        })
        .collect()
}

fn read_cpu_stat(path: &Path) -> Result<Option<u64>> {
    let Some(text) = read_optional_text(path)? else {
        return Ok(None);
    };
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() != Some("usage_usec") {
            continue;
        }
        let value = fields.next().ok_or_else(|| {
            AhrbError::Protocol(format!("{} has an empty usage_usec", path.display()))
        })?;
        let micros = value.parse::<u64>().map_err(|_| {
            AhrbError::Protocol(format!(
                "{} has an invalid usage_usec: {value}",
                path.display()
            ))
        })?;
        return Ok(Some(micros.saturating_mul(1_000)));
    }
    Ok(None)
}

fn read_u64_file(path: &Path) -> Result<Option<u64>> {
    let Some(text) = read_optional_text(path)? else {
        return Ok(None);
    };
    let value = text.trim();
    if value == "max" {
        return Ok(None);
    }
    value.parse::<u64>().map(Some).map_err(|_| {
        AhrbError::Protocol(format!(
            "{} contains an invalid counter: {value}",
            path.display()
        ))
    })
}

fn read_optional_text(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_transient_text(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::InvalidInput
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn read_fd_count(path: &Path) -> Result<Option<u64>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::InvalidInput
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let mut count = 0_u64;
    for entry in entries {
        match entry {
            Ok(_) => count = count.saturating_add(1),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::InvalidInput
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(count))
}

fn clock_ticks_per_second() -> u64 {
    // SAFETY: `_SC_CLK_TCK` is a read-only process-global system query.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks > 0 { ticks as u64 } else { 100 }
}

fn ticks_to_ns(ticks: u64, ticks_per_second: u64) -> u64 {
    if ticks_per_second == 0 {
        return 0;
    }
    ticks
        .saturating_mul(1_000_000_000)
        .saturating_div(ticks_per_second)
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).map_or(u64::MAX, |value| value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_dir(label: &str) -> Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| AhrbError::Protocol(error.to_string()))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!("ahrb-linux-sampler-{label}-{nanos}"));
        fs::create_dir_all(&path)?;
        Ok(path)
    }

    fn stat(pid: u32, command: &str, ppid: u32, start: u64) -> String {
        stat_with_cpu(pid, command, ppid, start, 11, 13, 1)
    }

    fn stat_with_cpu(
        pid: u32,
        command: &str,
        ppid: u32,
        start: u64,
        user_ticks: u64,
        system_ticks: u64,
        threads: u64,
    ) -> String {
        format!(
            "{pid} ({command}) S {ppid} 0 0 0 0 0 0 0 0 0 {user_ticks} {system_ticks} 0 0 0 0 {threads} 0 {start} 0 0"
        )
    }

    fn write_sample_process(
        proc_root: &Path,
        pid: u32,
        stat_text: &str,
        fd_count: u64,
    ) -> Result<()> {
        let directory = proc_root.join(pid.to_string());
        fs::create_dir_all(directory.join("fd"))?;
        fs::write(directory.join("stat"), stat_text)?;
        fs::write(
            directory.join("smaps_rollup"),
            "Rss: 100 kB\nPss: 50 kB\nPrivate_Clean: 3 kB\nPrivate_Dirty: 4 kB\n",
        )?;
        for descriptor in 0..fd_count {
            fs::write(directory.join("fd").join(descriptor.to_string()), "")?;
        }
        Ok(())
    }

    #[test]
    fn parses_commands_with_parentheses_and_spaces() -> Result<()> {
        let parsed = parse_stat(7, &stat(7, "worker (one)", 3, 99))?;
        assert_eq!(parsed.command, "worker (one)");
        assert_eq!(parsed.ppid, 3);
        assert_eq!(parsed.user_ticks, 11);
        assert_eq!(parsed.system_ticks, 13);
        assert_eq!(parsed.thread_count, 1);
        assert_eq!(parsed.start_ticks, 99);
        Ok(())
    }

    #[test]
    fn parses_rollup_bytes() -> Result<()> {
        let parsed = parse_smaps_rollup(
            "Rss: 100 kB\nPss: 50 kB\nPrivate_Clean: 3 kB\nPrivate_Dirty: 4 kB\n",
        )?;
        assert_eq!(parsed.rss_bytes, 102_400);
        assert_eq!(parsed.pss_bytes, Some(51_200));
        assert_eq!(parsed.private_bytes, Some(7_168));
        Ok(())
    }

    #[test]
    fn cgroup_membership_survives_reparenting() -> Result<()> {
        let root = fixture_dir("membership")?;
        let proc_root = root.join("proc");
        let cgroup = root.join("cgroup");
        fs::create_dir_all(proc_root.join("100"))?;
        fs::create_dir_all(proc_root.join("200"))?;
        fs::create_dir_all(&cgroup)?;
        fs::write(proc_root.join("100/stat"), stat(100, "root", 1, 10))?;
        fs::write(proc_root.join("200/stat"), stat(200, "worker", 1, 20))?;
        fs::write(cgroup.join("cgroup.procs"), "100\n200\n")?;

        let mut sampler = LinuxSampler::with_roots(proc_root, Some(cgroup));
        let tree = sampler.discover(&[100])?;
        assert_eq!(tree.members.len(), 2);
        let worker = tree
            .members
            .values()
            .find(|value| value.identity.pid == 200);
        assert!(matches!(
            worker.map(|value| &value.ownership),
            Some(ProcOwnership::CgroupMember)
        ));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn samples_per_process_counters_and_keeps_cpu_after_worker_exit() -> Result<()> {
        let root = fixture_dir("cpu-exit")?;
        let proc_root = root.join("proc");
        fs::create_dir_all(&proc_root)?;
        write_sample_process(
            &proc_root,
            100,
            &stat_with_cpu(100, "root", 1, 10, 11, 13, 2),
            3,
        )?;
        write_sample_process(
            &proc_root,
            200,
            &stat_with_cpu(200, "worker", 100, 20, 11, 13, 4),
            5,
        )?;

        let mut sampler = LinuxSampler::with_roots(proc_root.clone(), None);
        let first_tree = sampler.discover(&[100])?;
        let first = sampler.sample(&first_tree, "active")?;
        assert_eq!(first.process_samples.len(), 2);
        assert_eq!(first.open_fds, Some(8));
        assert_eq!(first.thread_count, Some(6));
        assert!(first.process_samples.windows(2).all(|pair| {
            pair[0].process.identity < pair[1].process.identity
                && pair[0].elapsed_ns == pair[1].elapsed_ns
                && pair[0].phase == pair[1].phase
        }));

        fs::remove_dir_all(proc_root.join("200"))?;
        write_sample_process(
            &proc_root,
            100,
            &stat_with_cpu(100, "root", 1, 10, 12, 13, 2),
            3,
        )?;
        let second_tree = sampler.discover(&[100])?;
        let second = sampler.sample(&second_tree, "post-exit")?;
        assert!(second.cpu_ns > first.cpu_ns);
        assert_eq!(second.process_samples.len(), 1);
        assert_eq!(second.open_fds, Some(3));
        assert_eq!(second.thread_count, Some(2));
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
