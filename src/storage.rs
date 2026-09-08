//! Storage v4 declarations, allocation accounting and deterministic row oracles.
//!
//! Physical counters never substitute for allocated blocks, or vice versa.

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub mod accounting;
pub mod auxiliaries;
pub mod evidence;
pub mod fixture;
pub mod retention;

/// Versioned fixture identity, independent of the matrix.
pub const TASK: &str = "ahrb-storage-tiny-turns-v1";
pub const ROWS: [&str; 10] = [
    "write-volume",
    "durability-cost",
    "footprint-curve",
    "compaction-vs-disk",
    "close-retention",
    "delete-uninstall-residue",
    "bounded-auxiliaries",
    "request-body-retention",
    "crash-residue",
    "resume-read-cost",
];
pub const MEASUREMENT_LABEL: &str = "OS-accounted owned-tree physical I/O; per-file allocated footprint, not unique-volume consumption";

/// Optional declarations retain omitted versus explicitly empty arrays.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub areas: Option<BTreeMap<String, Vec<String>>>,
    pub session_delete: Option<Vec<String>>,
    pub uninstall_cleanup: Option<Vec<String>>,
    pub sweep_interval_s: Option<u64>,
    pub session_close: Option<Vec<String>>,
    pub retention_cap_bytes: Option<u64>,
    pub auxiliary_cap_bytes: Option<BTreeMap<String, u64>>,
    pub workspace_path: Option<String>,
}

impl StorageConfig {
    pub fn validate(&self) -> Result<()> {
        let invalid = |text: String| AhrbError::Validation(format!("storage: {text}"));
        if self.sweep_interval_s == Some(0) {
            return Err(invalid("sweep_interval_s must be positive".into()));
        }
        if let Some(path) = &self.workspace_path {
            let suffix = path
                .strip_prefix("{{profile}}/")
                .ok_or_else(|| invalid("workspace_path must be profile-contained".into()))?;
            if suffix.is_empty()
                || suffix
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
                || suffix.contains('\\')
            {
                return Err(invalid("workspace_path must be normalized".into()));
            }
            let rendered = suffix.replace("{{session_id}}", "session");
            if rendered.contains(['{', '}', '$']) {
                return Err(invalid("workspace_path has unresolved variables".into()));
            }
        }
        let areas = self.areas.clone().unwrap_or_default();
        for (name, patterns) in &areas {
            if !valid_family(name) || (name == "other" && !patterns.is_empty()) {
                return Err(invalid(format!(
                    "invalid area {name:?}; other is the computed remainder"
                )));
            }
            for pattern in patterns {
                validate_glob(pattern)?;
            }
        }
        let families = areas.iter().collect::<Vec<_>>();
        for (i, (left, a)) in families.iter().enumerate() {
            for (right, b) in &families[i + 1..] {
                if a.iter().any(|x| b.iter().any(|y| globs_overlap(x, y))) {
                    return Err(invalid(format!("overlapping areas {left} and {right}")));
                }
            }
        }
        for name in self.auxiliary_cap_bytes.iter().flat_map(|caps| caps.keys()) {
            if name != "other" && !areas.contains_key(name) {
                return Err(invalid(format!("cap for undeclared family {name}")));
            }
        }
        for (name, argv, session) in [
            ("session_delete", &self.session_delete, true),
            ("session_close", &self.session_close, true),
            ("uninstall_cleanup", &self.uninstall_cleanup, false),
        ] {
            if let Some(argv) = argv {
                validate_verb(name, argv, session)?;
            }
        }
        if self
            .session_close
            .as_ref()
            .is_some_and(|a| !a.is_empty() && self.session_delete.as_ref() == Some(a))
        {
            return Err(invalid("session_close cannot alias session_delete".into()));
        }
        Ok(())
    }

    pub fn declarations_sha256(&self) -> Result<String> {
        let value = serde_json::json!({"areas":self.areas.clone().unwrap_or_default(), "retention_cap_bytes":self.retention_cap_bytes, "auxiliary_cap_bytes":self.auxiliary_cap_bytes.clone().unwrap_or_default(), "sweep_interval_s":self.sweep_interval_s});
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
    }

    pub fn family(&self, path: &str) -> Result<String> {
        let mut matches = self
            .areas
            .iter()
            .flat_map(|areas| areas.iter())
            .filter(|(_, globs)| globs.iter().any(|glob| glob_matches(glob, path)))
            .map(|(name, _)| name.clone());
        let family = matches.next().unwrap_or_else(|| "other".into());
        if matches.next().is_some() {
            return Err(AhrbError::Validation(format!(
                "storage overlapping family matches at {path}"
            )));
        }
        Ok(family)
    }

    pub fn delete_declared(&self) -> bool {
        self.session_delete.as_ref().is_some_and(|v| !v.is_empty())
            || self
                .uninstall_cleanup
                .as_ref()
                .is_some_and(|v| !v.is_empty())
    }
}

fn valid_family(name: &str) -> bool {
    name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn validate_verb(name: &str, argv: &[String], session: bool) -> Result<()> {
    if argv.is_empty() {
        return Ok(());
    }
    let error = || {
        AhrbError::Validation(format!(
            "storage.{name}: expected scoped direct harness argv with only profile/workspace/session_id/harness variables"
        ))
    };
    let program = std::path::Path::new(&argv[0])
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(error)?;
    if matches!(
        program,
        "sh" | "bash"
            | "zsh"
            | "fish"
            | "dash"
            | "csh"
            | "env"
            | "sudo"
            | "doas"
            | "xargs"
            | "command"
            | "exec"
            | "python"
            | "python3"
            | "perl"
            | "ruby"
            | "node"
            | "osascript"
            | "powershell"
            | "pwsh"
            | "cmd"
            | "busybox"
            | "timeout"
            | "nohup"
            | "setsid"
            | "nice"
            | "ssh"
            | "docker"
            | "uv"
            | "rm"
            | "npm"
            | "npx"
            | "pip"
            | "brew"
    ) {
        return Err(error());
    }
    if session
        && argv
            .iter()
            .map(|s| s.matches("{{session_id}}").count())
            .sum::<usize>()
            != 1
    {
        return Err(error());
    }
    if !argv
        .iter()
        .skip(1)
        .any(|s| s.contains("{{profile}}") || s.contains("{{workspace}}"))
    {
        return Err(error());
    }
    for (index, arg) in argv.iter().enumerate() {
        let lower = arg.to_ascii_lowercase();
        if arg.is_empty()
            || arg.contains(['\0', '\n', '\r', '$', '`', '\\'])
            || [
                "credential",
                "secret",
                "api_key",
                "api-key",
                "access-token",
                "password",
                "private-key",
            ]
            .iter()
            .any(|key| lower.contains(key))
        {
            return Err(error());
        }
        let rendered = arg
            .replace("{{profile}}", "ROOT")
            .replace("{{workspace}}", "ROOT/workspace")
            .replace("{{session_id}}", "session")
            .replace("{{harness}}", "harness");
        if rendered.contains(['{', '}']) || rendered.split('/').any(|p| p == "." || p == "..") {
            return Err(error());
        }
        let value = rendered
            .split_once('=')
            .map_or(rendered.as_str(), |(_, v)| v);
        if index > 0
            && (value.starts_with(['/', '~'])
                || (value.contains('/') && !value.starts_with("ROOT/")))
        {
            return Err(error());
        }
    }
    Ok(())
}

pub fn validate_glob(pattern: &str) -> Result<()> {
    if pattern.is_empty()
        || pattern.contains(['\\', '{', '}'])
        || pattern
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == ".." || (c.contains("**") && c != "**"))
    {
        return Err(AhrbError::Validation(format!(
            "storage invalid root-relative glob {pattern:?}"
        )));
    }
    Ok(())
}

// Intersection of two component wildcard automata. Stars have an epsilon edge
// and a consuming self-loop; '?' consumes one Unicode scalar.
fn components_overlap(a: &str, b: &str) -> bool {
    let a = a.chars().collect::<Vec<_>>();
    let b = b.chars().collect::<Vec<_>>();
    let mut todo = VecDeque::from([(0, 0, false)]);
    let mut seen = BTreeSet::new();
    while let Some((i, j, nonempty)) = todo.pop_front() {
        if !seen.insert((i, j, nonempty)) {
            continue;
        }
        if i == a.len() && j == b.len() && nonempty {
            return true;
        }
        if a.get(i) == Some(&'*') {
            todo.push_back((i + 1, j, nonempty));
        }
        if b.get(j) == Some(&'*') {
            todo.push_back((i, j + 1, nonempty));
        }
        if let (Some(x), Some(y)) = (a.get(i), b.get(j)) {
            if x == y || matches!(x, '*' | '?') || matches!(y, '*' | '?') {
                todo.push_back((i + usize::from(*x != '*'), j + usize::from(*y != '*'), true));
            }
        }
    }
    false
}

pub fn globs_overlap(a: &str, b: &str) -> bool {
    let a = a.split('/').collect::<Vec<_>>();
    let b = b.split('/').collect::<Vec<_>>();
    let mut todo = VecDeque::from([(0, 0)]);
    let mut seen = BTreeSet::new();
    while let Some((i, j)) = todo.pop_front() {
        if !seen.insert((i, j)) {
            continue;
        }
        if i == a.len() && j == b.len() {
            return true;
        }
        if a.get(i) == Some(&"**") {
            todo.push_back((i + 1, j));
        }
        if b.get(j) == Some(&"**") {
            todo.push_back((i, j + 1));
        }
        if let (Some(x), Some(y)) = (a.get(i), b.get(j)) {
            if *x == "**" || *y == "**" || components_overlap(x, y) {
                todo.push_back((i + usize::from(*x != "**"), j + usize::from(*y != "**")));
            }
        }
    }
    false
}

pub fn glob_matches(pattern: &str, path: &str) -> bool {
    let a = pattern.split('/').collect::<Vec<_>>();
    let b = path.split('/').collect::<Vec<_>>();
    fn visit(a: &[&str], b: &[&str]) -> bool {
        match a.first() {
            None => b.is_empty(),
            Some(&"**") => visit(&a[1..], b) || (!b.is_empty() && visit(a, &b[1..])),
            Some(component) => {
                !b.is_empty() && component_matches(component, b[0]) && visit(&a[1..], &b[1..])
            }
        }
    }
    visit(&a, &b)
}

fn component_matches(pattern: &str, text: &str) -> bool {
    let p = pattern.chars().collect::<Vec<_>>();
    let t = text.chars().collect::<Vec<_>>();
    let mut states = BTreeSet::from([0]);
    for ch in t {
        let mut next = BTreeSet::new();
        for mut i in states {
            while p.get(i) == Some(&'*') {
                next.insert(i);
                i += 1;
            }
            if p.get(i).is_some_and(|c| *c == '?' || *c == ch) {
                next.insert(i + 1);
            }
        }
        states = next;
    }
    states.into_iter().any(|mut i| {
        while p.get(i) == Some(&'*') {
            i += 1;
        }
        i == p.len()
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Deadline {
    pub seconds: u64,
    pub source: String,
    pub k: u64,
    pub h: u64,
    pub w: u64,
    pub formula: String,
}

pub fn resolve_deadline(
    profile: crate::cli::Profile,
    flag: Option<u64>,
    environment: Option<&str>,
    sweep: Option<u64>,
) -> Result<Deadline> {
    let (r, k, h, base) = match profile {
        crate::cli::Profile::Quick => (3_u64, 1645, 3600, 10509_u64),
        crate::cli::Profile::Cert => (7, 38233, 14400, 174979),
    };
    let w = sweep.unwrap_or(0);
    let default = r
        .checked_mul(w)
        .and_then(|n| base.checked_add(n))
        .ok_or_else(|| AhrbError::Usage("storage default deadline overflows u64".into()))?;
    let (seconds, source) = if let Some(s) = flag {
        (s, "flag")
    } else if let Some(s) = environment {
        (
            s.parse::<u64>().map_err(|_| {
                AhrbError::Usage(
                    "AHRB_DEADLINE must be a non-negative integer number of seconds".into(),
                )
            })?,
            "environment",
        )
    } else {
        (default, "storage-default")
    };
    if std::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(seconds))
        .is_none()
    {
        return Err(AhrbError::Usage(
            "storage deadline overflows Instant".into(),
        ));
    }
    Ok(Deadline {
        seconds,
        source: source.into(),
        k,
        h,
        w,
        formula: "ceil(2 * 2.1 * K + H + R * W)".into(),
    })
}

pub fn checkpoints(turns: u32) -> Vec<u32> {
    if turns == 1000 {
        vec![0, 1, 10, 50, 100, 500, 1000]
    } else {
        vec![0, 1, 10, 50, 100]
    }
}
pub fn median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let m = values.len() / 2;
    Some(if values.len() % 2 == 0 {
        values[m - 1] / 2.0 + values[m] / 2.0
    } else {
        values[m]
    })
}
pub fn p95(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    values.get((95 * values.len()).div_ceil(100) - 1).copied()
}
pub fn disk_class(bytes: f64) -> &'static str {
    if bytes <= 65536.0 {
        "64"
    } else if bytes <= 262144.0 {
        "256"
    } else if bytes <= 1048576.0 {
        "1024"
    } else if bytes <= 4194304.0 {
        "4096"
    } else {
        "4096+"
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum GrowthClass {
    Bounded,
    Linear,
    Superlinear,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub turn: u32,
    pub allocated_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CurveEvaluation {
    pub checkpoints: Vec<Checkpoint>,
    pub early_bytes_per_turn: Option<f64>,
    pub late_bytes_per_turn: Option<f64>,
    pub late_early_ratio: Option<f64>,
    pub late_range_bytes: Option<u64>,
}

pub fn evaluate_curve(
    points: &[Checkpoint],
    n: u32,
) -> Result<(GrowthClass, f64, CurveEvaluation)> {
    if !matches!(n, 100 | 1000)
        || points.iter().map(|p| p.turn).collect::<Vec<_>>() != checkpoints(n)
    {
        return Err(AhrbError::Protocol(
            "storage missing/duplicate/out-of-order checkpoint".into(),
        ));
    }
    let a = |t| {
        points
            .iter()
            .find(|p| p.turn == t)
            .map(|p| p.allocated_bytes)
            .ok_or_else(|| AhrbError::Protocol("storage missing checkpoint".into()))
    };
    let difference = |after: u64, before: u64| (i128::from(after) - i128::from(before)) as f64;
    let mut slopes = Vec::new();
    for (i, left) in points.iter().enumerate().skip(1) {
        for right in &points[i + 1..] {
            slopes.push(
                difference(right.allocated_bytes, left.allocated_bytes)
                    / (right.turn - left.turn) as f64,
            );
        }
    }
    let slope = median(slopes).ok_or_else(|| AhrbError::Protocol("storage empty slope".into()))?;
    let early = difference(a(n / 2)?, a(n / 10)?) / (n / 2 - n / 10) as f64;
    let late = difference(a(n)?, a(n / 2)?) / (n - n / 2) as f64;
    let range = a(n)?.abs_diff(a(n / 2)?);
    let class = if range <= 65536 && slope <= 65536.0 / n as f64 {
        GrowthClass::Bounded
    } else if slope > 0.0 && late > (1.25 * early.max(0.0)).max(early.max(0.0) + 4096.0) {
        GrowthClass::Superlinear
    } else {
        GrowthClass::Linear
    };
    Ok((
        class,
        slope,
        CurveEvaluation {
            checkpoints: points.to_vec(),
            early_bytes_per_turn: Some(early),
            late_bytes_per_turn: Some(late),
            late_early_ratio: (early > 0.0).then(|| late / early),
            late_range_bytes: Some(range),
        },
    ))
}

pub fn render_markdown(
    summary: &evidence::StorageSummary,
    report: &crate::report::Report,
) -> String {
    use std::fmt::Write as _;
    let rows = &report.results;
    let details = &report.details;
    let show = |v: Option<f64>| {
        v.map(|n| format!("{n:.3}"))
            .unwrap_or_else(|| "unavailable".into())
    };
    let mut out = format!(
        "## Storage v4\n\nProfile: `{}` · OS: `{}` · topology: `{}` · `{}`.\n\n{}\n\n| Row | Outcome | Reason |\n|---|---|---|\n",
        summary.profile,
        summary.os,
        summary.topology,
        summary.comparison_scope,
        summary.measurement_label
    );
    for row in rows {
        let (label, reason) = match &row.outcome {
            crate::evaluate::TestOutcome::Pass => ("PASS", ""),
            crate::evaluate::TestOutcome::Fail(r) => ("FAIL", r.as_str()),
            crate::evaluate::TestOutcome::Error(r) => ("ERROR", r.as_str()),
            crate::evaluate::TestOutcome::Absent(r) => ("ABSENT", r.as_str()),
            crate::evaluate::TestOutcome::Unsupported(r) => ("UNSUPPORTED", r.as_str()),
        };
        let _ = writeln!(
            out,
            "| S{} `{}` | {} | {} |",
            row.row,
            row.id,
            label,
            reason.replace('|', "\\|")
        );
    }
    let _ = writeln!(
        out,
        "\n| Physical write bytes/turn p50 | Physical p95 | Physical max | Allocated growth bytes/turn | Signed net growth bytes/turn | Amplification |\n|---:|---:|---:|---:|---:|---:|\n| {} | {} | {} | {} | {} | {} |\n",
        show(summary.write_bytes_per_turn_p50),
        show(summary.write_bytes_per_turn_p95),
        show(summary.write_bytes_per_turn_max),
        show(summary.logical_growth_bytes_per_turn),
        show(summary.net_growth_bytes_per_turn),
        show(summary.write_amplification_ratio)
    );
    let _ = writeln!(
        out,
        "D{} · G{} (sampled horizon, not an asymptotic proof).\n",
        summary.disk_class.as_deref().unwrap_or("unavailable"),
        summary
            .growth_class
            .map(|c| format!("{c:?}").to_lowercase())
            .unwrap_or_else(|| "unavailable".into())
    );
    out.push_str("| Trial scalar | Median | MAD |\n|---|---:|---:|\n");
    for slug in [ROWS[0], ROWS[2], ROWS[7]] {
        if let Some(trials) = details.get(slug).and_then(|d| d["trials"].as_array()) {
            let keys = trials
                .iter()
                .filter_map(|t| t["summary"].as_object())
                .flat_map(|s| s.keys().cloned())
                .collect::<BTreeSet<_>>();
            for key in keys {
                let values = trials
                    .iter()
                    .map(|t| t["summary"][&key].as_f64())
                    .collect::<Option<Vec<_>>>();
                if let Some(values) = values {
                    let center = median(values.clone());
                    let mad =
                        center.and_then(|c| median(values.iter().map(|v| (v - c).abs()).collect()));
                    let _ = writeln!(out, "| `{key}` | {} | {} |", show(center), show(mad));
                }
            }
        }
    }
    out.push('\n');
    out.push_str("| Turn | Allocated bytes (median) | MAD bytes |\n|---:|---:|---:|\n");
    for p in &summary.footprint_curve {
        let _ = writeln!(
            out,
            "| {} | {} | {} |",
            p.turn, p.allocated_bytes, p.mad_bytes
        );
    }
    out.push_str("\nAllocated families at each repetition's last settled boundary (S1 audit):\n\n| Repetition | Turn | Family | Allocated bytes |\n|---:|---:|---|---:|\n");
    let mut last = BTreeMap::new();
    for sample in &report.storage_samples {
        if sample.row_id == ROWS[0] {
            last.insert(sample.repetition, sample);
        }
    }
    if last.is_empty() {
        out.push_str("| unavailable | unavailable | unavailable | unavailable |\n");
    }
    for sample in last.values() {
        for (family, bytes) in &sample.families {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} |",
                sample.repetition, sample.turn, family, bytes
            );
        }
    }
    out.push_str("\n| Family | Cap bytes | Peak allocated bytes | Final allocated bytes | Slope bytes/turn | Rotation | Class |\n|---|---:|---:|---:|---:|---|---|\n");
    if summary.auxiliaries.is_empty() {
        out.push_str(
            "| unavailable | unavailable | unavailable | unavailable | unavailable | unavailable | unavailable |\n",
        );
    }
    for a in &summary.auxiliaries {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} |",
            a.name,
            a.cap_bytes
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unavailable".into()),
            a.peak_allocated_bytes,
            a.final_allocated_bytes,
            a.slope_bytes_per_turn,
            a.rotation_observed,
            a.class
                .map(|c| format!("{c:?}").to_lowercase())
                .unwrap_or_else(|| "unsupported cap assessment".into())
        );
    }
    let _ = writeln!(
        out,
        "\nRequest retention: **{}** · matched stored bytes: {} · unique content bytes: {} · stored/unique ratio: {}.\n\nPrivacy limitation: these are observational byte-retention classes, not storage architecture or privacy-safety claims. `none` means no matching request bytes found in supported representations; transformed storage may retain prompts. Captured request bodies and matches are private benchmark evidence.\n",
        summary
            .request_retention_class
            .map(|c| format!("{c:?}").to_lowercase())
            .unwrap_or_else(|| "unavailable".into()),
        summary
            .stored_request_bytes
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unavailable".into()),
        summary
            .unique_request_content_bytes
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unavailable".into()),
        show(summary.stored_unique_ratio)
    );
    out.push_str("\n| Residue operation | Allocated bytes | Files |\n|---|---:|---:|\n");
    for (name, bytes, files) in [
        (
            "delete",
            summary.delete_residue_allocated_bytes,
            summary.delete_residue_files,
        ),
        (
            "uninstall",
            summary.uninstall_residue_allocated_bytes,
            summary.uninstall_residue_files,
        ),
    ] {
        let _ = writeln!(
            out,
            "| {name} | {} | {} |",
            bytes
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unavailable".into()),
            files
                .map(|v| v.to_string())
                .unwrap_or_else(|| "unavailable".into())
        );
    }
    out.push_str("\nDurability wall cost is an estimate using 4 ms/call, never measured latency. Request-byte retention requires exact matches and verified representation coverage; disk growth alone cannot establish retention. Context shrinking need not reclaim journals. Sync is an AHRB boundary operation, not evidence of harness fsync or hardware durability.\n\nEvidence: [samples](storage-samples.jsonl), [files](storage-files.jsonl), [processes](processes.jsonl), [turns](turns.jsonl), [requests](model-requests.jsonl), [fsync](fsync-events.jsonl), [body matches](request-body-matches.jsonl). Empty collectors have no coverage claim.\n");
    out
}
