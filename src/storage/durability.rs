//! S2 receipt validation and Darwin interposition. All collector artifacts live
//! outside the measured profile. A load environment is never a coverage receipt.
use super::evidence::*;
use crate::{AhrbError, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Duration,
};

pub const BACKEND: &str = "darwin-arm64-interpose";
pub const VERSION: &str = "darwin-arm64-interpose-v1";

pub fn digest(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(std::fs::read(path)?)))
}

#[derive(Clone, Debug)]
pub struct Collector {
    pub shim: PathBuf,
    pub trace: PathBuf,
}
impl Collector {
    pub fn environment(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "DYLD_INSERT_LIBRARIES".into(),
                self.shim.to_string_lossy().into_owned(),
            ),
            (
                "AHRB_DURABILITY_TRACE".into(),
                self.trace.to_string_lossy().into_owned(),
            ),
        ])
    }
    pub fn new(output: &Path, name: &str) -> Result<Self> {
        let support = output.join("durability-support");
        std::fs::create_dir_all(&support)?;
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        {
            for (name, bytes) in [
                (
                    "libahrb-durability.dylib",
                    include_bytes!(concat!(env!("OUT_DIR"), "/libahrb-durability.dylib"))
                        .as_slice(),
                ),
                (
                    "libahrb-durability-control.dylib",
                    include_bytes!(concat!(
                        env!("OUT_DIR"),
                        "/libahrb-durability-control.dylib"
                    ))
                    .as_slice(),
                ),
            ] {
                let path = support.join(name);
                if !path.exists() {
                    std::fs::write(path, bytes)?;
                }
            }
        }
        let trace = support.join(name);
        std::fs::create_dir(&trace)?;
        Ok(Self {
            shim: support.join("libahrb-durability.dylib"),
            trace,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Raw {
    sequence: u64,
    pid: u32,
    start_time: u64,
    kind: String,
    primitive: Option<Primitive>,
    enter_ns: Option<u64>,
    exit_ns: Option<u64>,
    return_code: Option<i64>,
    errno: Option<i64>,
    self_test: Option<bool>,
    failures: Option<u64>,
    fdatasync_available: Option<bool>,
    drop_count: Option<u64>,
    child_pid: Option<u32>,
    reason: Option<String>,
    version: Option<String>,
}
#[derive(Debug, Default)]
pub struct Capture {
    pub images: Vec<ImageReceipt>,
    pub events: Vec<FsyncEvent>,
    pub gaps: Vec<String>,
    pub spawned: Vec<(u32, u64)>,
}
fn invalid(reason: impl Into<String>) -> AhrbError {
    AhrbError::Protocol(format!("durability trace loss: {}", reason.into()))
}

/// `complete` requires a clean end record (or a witnessed exec into another
/// image). Sequence gaps, partial writes and missing controls are never zero.
pub fn read_trace(trace: &Path, repetition: u32, complete: bool) -> Result<Capture> {
    let mut paths = std::fs::read_dir(trace)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "jsonl"));
    paths.sort();
    let mut result = Capture::default();
    let mut unfinished = Vec::new();
    let mut sequence_offsets = BTreeMap::new();
    let mut data_applicability = BTreeSet::new();
    let mut image_controls = BTreeMap::new();
    for path in paths {
        let bytes = std::fs::read(&path)?;
        if bytes.is_empty() || !bytes.ends_with(b"\n") {
            return Err(invalid("empty or truncated image stream"));
        }
        let records = bytes
            .split(|b| *b == b'\n')
            .filter(|s| !s.is_empty())
            .map(serde_json::from_slice::<Raw>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| invalid(format!("{}: {e}", path.display())))?;
        let first = records
            .first()
            .ok_or_else(|| invalid("empty or truncated image stream"))?;
        if first.kind != "load"
            || first.sequence != 1
            || first.version.as_deref() != Some(VERSION)
            || first.pid == 0
            || first.start_time == 0
        {
            return Err(invalid("invalid load handshake"));
        }
        let offset = *sequence_offsets
            .get(&(first.pid, first.start_time))
            .unwrap_or(&0_u64);
        let mut ended = false;
        let mut exec = false;
        let mut control = false;
        let mut calls = Vec::new();
        for (i, r) in records.iter().enumerate() {
            if r.sequence != i as u64 + 1
                || r.pid != first.pid
                || r.start_time != first.start_time
                || ended
            {
                return Err(invalid(
                    "sequence/identity discontinuity or records after end",
                ));
            }
            match r.kind.as_str() {
                "load" if i == 0 => {}
                "call" => {
                    if r.self_test == Some(control) {
                        return Err(invalid("call/control phase mismatch"));
                    }
                    let event = FsyncEvent {
                        repetition,
                        turn: 0,
                        pid: r.pid,
                        start_time: r.start_time,
                        sequence: offset + r.sequence,
                        primitive: r.primitive.ok_or_else(|| invalid("missing primitive"))?,
                        enter_ns: r.enter_ns.ok_or_else(|| invalid("missing entry"))?,
                        exit_ns: r.exit_ns.ok_or_else(|| invalid("missing exit"))?,
                        return_code: r.return_code.ok_or_else(|| invalid("missing return"))?,
                        errno: r.errno.ok_or_else(|| invalid("missing errno"))?,
                        backend: BACKEND.into(),
                        self_test: r
                            .self_test
                            .ok_or_else(|| invalid("missing self-test flag"))?,
                    };
                    if event.enter_ns == 0
                        || event.exit_ns < event.enter_ns
                        || ![-1, 0].contains(&event.return_code)
                        || (event.return_code == 0 && event.errno != 0)
                    {
                        return Err(invalid("invalid completion"));
                    }
                    if event.self_test {
                        calls.push(event.clone());
                    }
                    result.events.push(event);
                }
                "control" => {
                    if control || r.failures != Some(0) {
                        return Err(invalid("known-call control failed"));
                    }
                    verify_control(
                        &calls,
                        r.fdatasync_available
                            .ok_or_else(|| invalid("missing fdatasync applicability control"))?,
                    )?;
                    data_applicability.insert(r.fdatasync_available);
                    if data_applicability.len() > 1 {
                        return Err(invalid(
                            "primitive applicability differs across owned images",
                        ));
                    }
                    control = true;
                }
                "end" => {
                    if r.drop_count != Some(0) {
                        return Err(invalid("nonzero or missing drop counter"));
                    }
                    ended = true;
                }
                "exec" => exec = true,
                "exec-failed" if exec => exec = false,
                "spawn" => {
                    let pid = r.child_pid.ok_or_else(|| invalid("missing spawned PID"))?;
                    let enter = r.enter_ns.ok_or_else(|| invalid("missing spawn entry"))?;
                    if pid == 0 || enter == 0 || r.exit_ns.is_none_or(|exit| exit < enter) {
                        return Err(invalid("invalid spawn identity or interval"));
                    }
                    result.spawned.push((pid, enter));
                }
                "coverage-gap" => result.gaps.push(
                    r.reason
                        .clone()
                        .ok_or_else(|| invalid("missing gap reason"))?,
                ),
                _ => return Err(invalid("unexpected record")),
            }
        }
        sequence_offsets.insert((first.pid, first.start_time), offset + records.len() as u64);
        if !control {
            return Err(invalid("missing known-call control"));
        }
        // An exec may create several images for one process identity. Only its
        // first control can witness a new spawn; an older/reused PID cannot.
        let first_control = calls.iter().map(|event| event.enter_ns).min().unwrap();
        image_controls
            .entry((first.pid, first.start_time))
            .and_modify(|entry: &mut u64| *entry = (*entry).min(first_control))
            .or_insert(first_control);
        let image_path = PathBuf::from(
            String::from_utf8(std::fs::read(path.with_extension("jsonl.image"))?)
                .map_err(|_| invalid("non-UTF8 executable path"))?,
        );
        let hash = digest(&image_path)?;
        if complete && !ended {
            let control_end = calls.iter().map(|e| e.exit_ns).max().unwrap_or(0);
            unfinished.push((first.pid, first.start_time, exec, control_end));
        }
        result.images.push(ImageReceipt {
            pid: first.pid,
            start_time: first.start_time,
            executable_sha256: hash,
            load_verified: true,
            self_test_verified: true,
            exit_seen: ended,
        });
    }
    for (pid, start, exec, control_end) in unfinished {
        if !exec
            || !result.events.iter().any(|e| {
                e.pid == pid && e.start_time == start && e.self_test && e.enter_ns > control_end
            })
        {
            return Err(invalid(format!("missing end-of-stream for {pid}/{start}")));
        }
    }
    if complete {
        // Match spawns one-to-one to distinct PID/start identities in causal
        // order. Neither a prior image nor one later image reused for multiple
        // spawns can certify a child whose injection was stripped.
        result.spawned.sort_by_key(|(_, enter)| *enter);
        for (pid, enter) in &result.spawned {
            let identity = image_controls
                .iter()
                .filter(|((child, _), control)| child == pid && *control >= enter)
                .min_by_key(|(_, control)| *control)
                .map(|(identity, _)| *identity)
                .ok_or_else(|| {
                    invalid("spawned process lacks a distinct subsequent image control")
                })?;
            image_controls.remove(&identity);
        }
    }
    Ok(result)
}

fn verify_control(events: &[FsyncEvent], data: bool) -> Result<()> {
    let counts = |primitive, success| {
        events
            .iter()
            .filter(|e| e.primitive == primitive && (e.return_code == 0) == success)
            .count()
    };
    if events.len() != if data { 11 } else { 9 }
        || counts(Primitive::Fdatasync, true) != usize::from(data)
        || counts(Primitive::Fdatasync, false) != usize::from(data)
        || counts(Primitive::Fsync, true) != 3
        || counts(Primitive::Fsync, false) != 2
        || counts(Primitive::Fullfsync, true) != 2
        || counts(Primitive::Fullfsync, false) != 2
        || events
            .iter()
            .any(|e| e.return_code == -1 && e.errno != libc::EBADF as i64)
    {
        return Err(invalid(
            "independent control expected fsync=3+2 failed, fullfsync=2+2 failed (including NOCANCEL entry points), fdatasync=1+1 failed iff exported",
        ));
    }
    Ok(())
}

/// Applicability comes from a verified image control, never from nullable
/// aggregate counts. An absent/invalid capture cannot prove OS-inapplicability.
pub fn applicability(capture: &Capture) -> BTreeMap<Primitive, Applicability> {
    if capture.images.is_empty() {
        return BTreeMap::new();
    }
    let data = capture
        .events
        .iter()
        .any(|event| event.self_test && event.primitive == Primitive::Fdatasync);
    BTreeMap::from([
        (Primitive::Fsync, Applicability::Measured),
        (
            Primitive::Fdatasync,
            if data {
                Applicability::Measured
            } else {
                Applicability::NotApplicable
            },
        ),
        (Primitive::Fullfsync, Applicability::Measured),
    ])
}

pub fn summarize(events: &[FsyncEvent], n: u32) -> Result<(DurabilitySummary, u64)> {
    if n == 0 {
        return Err(invalid("zero turn budget"));
    }
    let measured = events
        .iter()
        .filter(|e| !e.self_test && e.turn > 0)
        .collect::<Vec<_>>();
    let data = events
        .iter()
        .any(|e| e.self_test && e.primitive == Primitive::Fdatasync);
    if measured
        .iter()
        .any(|e| e.turn > n || (!data && e.primitive == Primitive::Fdatasync))
    {
        return Err(invalid("inapplicable primitive or out-of-range turn"));
    }
    let count = |p| {
        measured
            .iter()
            .filter(|e| e.primitive == p && e.return_code == 0)
            .count() as f64
            / n as f64
    };
    let fsync = count(Primitive::Fsync);
    let full = count(Primitive::Fullfsync);
    let fdatasync = data.then(|| count(Primitive::Fdatasync));
    // Sum integer completions before division, so an exact band boundary
    // cannot move upward through rounding of separate primitive ratios.
    let total = measured.iter().filter(|e| e.return_code == 0).count() as f64 / n as f64;
    Ok((
        DurabilitySummary {
            fsync_calls_per_turn: Some(fsync),
            fdatasync_calls_per_turn: fdatasync,
            fullfsync_calls_per_turn: Some(full),
            durability_calls_per_turn: Some(total),
            assumed_fsync_cost_ms: Some(4.0),
            estimated_durability_wall_ms_per_turn: Some(total * 4.0),
            durability_class: Some(class(total).into()),
        },
        measured.iter().filter(|e| e.return_code == -1).count() as u64,
    ))
}
pub fn class(calls: f64) -> &'static str {
    if calls == 0.0 {
        "0"
    } else if calls <= 1.0 {
        "1"
    } else if calls <= 10.0 {
        "10"
    } else {
        "10+"
    }
}

pub struct Probe {
    pub instrumentation: Instrumentation,
    pub events: Vec<FsyncEvent>,
}
/// This runs the exact discovered binary with a constructor-only known-call
/// control in an empty HOME. If loading is refused, only its version argv runs.
pub async fn probe(output: &Path, executable: &Path, version_args: &[String]) -> Result<Probe> {
    let mut instrumentation = Instrumentation {
        backend: BACKEND.into(),
        version: VERSION.into(),
        executable_sha256: digest(executable)?,
        ..Instrumentation::default()
    };
    if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        instrumentation.backend = "unavailable".into();
        instrumentation.version = "unavailable".into();
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("strace")
                .arg("-V")
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let diagnostic = match &result {
            Ok(Ok(o)) => {
                serde_json::json!({"exit_code":o.status.code(),"stdout":String::from_utf8_lossy(&o.stdout),"stderr":String::from_utf8_lossy(&o.stderr)})
            }
            Ok(Err(e)) => {
                serde_json::json!({"launch_error":e.to_string(),"kind":format!("{:?}",e.kind())})
            }
            Err(_) => serde_json::json!({"timeout_seconds":15}),
        };
        std::fs::write(
            output.join("durability-linux-preflight.json"),
            serde_json::to_vec_pretty(&diagnostic)?,
        )?;
        match result {
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ) =>
            {
                instrumentation.reason = Some(format!(
                    "os-limited: strace launch unavailable ({e}); see durability-linux-preflight.json"
                ));
                return Ok(Probe {
                    instrumentation,
                    events: Vec::new(),
                });
            }
            _ => {
                return Err(AhrbError::Protocol(format!(
                    "Linux owned-tree tracing backend is not implemented/verified; tracer probe {diagnostic}; see durability-linux-preflight.json"
                )));
            }
        }
    }
    let collector = Collector::new(output, "preflight")?;
    instrumentation.shim_sha256 = Some(digest(&collector.shim)?);
    instrumentation.environment_keys = collector.environment().into_keys().collect();
    instrumentation
        .environment_keys
        .push("AHRB_DURABILITY_CONTROL_ONLY".into());
    // Keep disposable HOME files outside the evidence bundle copied by auto-save.
    // The launch receipt retains this path for ownership and inspection.
    let home = std::env::temp_dir().join(format!(
        "ahrb-durability-home-{}-{}",
        std::process::id(),
        crate::fake_model::monotonic_timestamp_ns()
    ));
    std::fs::create_dir(&home)?;
    let mut command = tokio::process::Command::new(executable);
    command
        .args(version_args)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", &home)
        .envs(collector.environment())
        .env("AHRB_DURABILITY_CONTROL_ONLY", "1")
        .kill_on_drop(true);
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[allow(unused_mut)]
    let mut identity: Option<crate::process::ProcIdentity> = None;
    let mut spawned_pid = None;
    let result = match command.spawn() {
        Ok(child) => {
            spawned_pid = child.id();
            #[cfg(target_os = "macos")]
            if let Some(pid) = spawned_pid {
                identity =
                    crate::process::macos::process_identity_and_group(pid)?.map(|(id, _)| id);
            }
            tokio::time::timeout(Duration::from_secs(15), child.wait_with_output()).await
        }
        Err(e) => Ok(Err(e)),
    };
    let mut diagnostic = match result {
        Ok(Ok(o)) => {
            serde_json::json!({"exit_code":o.status.code(),"stdout":String::from_utf8_lossy(&o.stdout),"stderr":String::from_utf8_lossy(&o.stderr)})
        }
        Ok(Err(e)) => serde_json::json!({"launch_error":e.to_string()}),
        Err(_) => serde_json::json!({"timeout_seconds":15}),
    };
    diagnostic["executable"] = serde_json::to_value(executable)?;
    diagnostic["version_args"] = serde_json::to_value(version_args)?;
    diagnostic["isolated_home"] = serde_json::to_value(&home)?;
    diagnostic["spawned_pid"] = serde_json::to_value(spawned_pid)?;
    diagnostic["owned_launch_identity"] = serde_json::to_value(identity)?;
    std::fs::write(
        collector.trace.join("launch.json"),
        serde_json::to_vec_pretty(&diagnostic)?,
    )?;
    let capture = read_trace(&collector.trace, 0, true)?;
    instrumentation.images = capture.images;
    if instrumentation.images.is_empty() {
        if let Some(id) = identity {
            instrumentation.images.push(ImageReceipt {
                pid: id.pid,
                start_time: id.start_time,
                executable_sha256: instrumentation.executable_sha256.clone(),
                load_verified: false,
                self_test_verified: false,
                exit_seen: diagnostic["exit_code"].is_number(),
            });
        }
        instrumentation.reason = Some(format!(
            "os-limited: no image load/control receipt from injected executable; observed launch {diagnostic}; loader refusal or stripped DYLD state, not zero calls"
        ));
    } else if diagnostic["exit_code"] != 0 {
        instrumentation.reason = Some(format!(
            "os-limited: injected known-call control did not exit successfully: {diagnostic}"
        ));
    } else if !capture.gaps.is_empty() {
        instrumentation.reason = Some(format!("os-limited: {}", capture.gaps.join("; ")));
    }
    Ok(Probe {
        instrumentation,
        events: capture.events,
    })
}

/// Preserve observed drop counters even when strict parsing rejected the stream.
/// A missing end still fails validation; a zero here cannot certify coverage.
pub fn observed_drop_count(trace: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(trace)? {
        let path = entry?.path();
        if path.extension().is_none_or(|e| e != "jsonl") {
            continue;
        }
        for line in std::fs::read(&path)?.split(|b| *b == b'\n') {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) {
                if value["kind"] == "end" {
                    if let Some(drops) = value["drop_count"].as_u64() {
                        total = total
                            .checked_add(drops)
                            .ok_or_else(|| invalid("drop counter overflow"))?;
                    }
                }
            }
        }
    }
    Ok(total)
}
