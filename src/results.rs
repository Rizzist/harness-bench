//! Durable benchmark result bundles and compact history reporting.

use crate::cli::RunOptions;
use crate::evaluate::{Badge, TestOutcome, badge_label};
use crate::manifest::Manifest;
use crate::report::{Report, ResourceSummary};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RESULT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Per-run persistence information captured before workload execution.
#[derive(Clone, Debug)]
pub struct RunPersistence {
    /// Primary output requested by the user, or the default durable bundle.
    pub output: PathBuf,
    /// Canonical auto-save directory when saving is enabled.
    pub results_dir: Option<PathBuf>,
    /// UTC run-start timestamp.
    pub timestamp: String,
    /// Harness version captured by the availability probe.
    pub harness_version: String,
    /// One-minute system load average at run start.
    pub load_avg_1m: Option<f64>,
    results_dir_field: Option<String>,
}

/// Four history counts. `ABSENT` outcomes are included in `ERROR` so totals
/// cover every selected row without adding a fifth index category.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OutcomeCounts {
    /// Passing rows.
    #[serde(rename = "PASS")]
    pub pass: u64,
    /// Failing rows.
    #[serde(rename = "FAIL")]
    pub fail: u64,
    /// Honestly unsupported optional rows.
    #[serde(rename = "UNSUPPORTED")]
    pub unsupported: u64,
    /// Infrastructure-error or absent-evidence rows.
    #[serde(rename = "ERROR")]
    pub error: u64,
}

/// Resource fields kept in the compact history index.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct IndexedResourceSummary {
    /// Peak owned-tree effective resident memory.
    pub peak_rss_mib: f64,
    /// Owned-tree CPU per executed turn.
    pub cpu_per_turn_ms: f64,
    /// Mean externally measured turn wall clock.
    pub wall_per_turn_ms: f64,
    /// Membership sampler CPU percentage.
    pub sampler_overhead_pct: f64,
}

/// One line in `results/index.jsonl`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    /// Stable adapter identity.
    pub harness_id: String,
    /// Availability-probe version output.
    pub harness_version: String,
    /// Canonical manifest SHA-256.
    pub manifest_hash: String,
    /// AHRB build revision.
    pub ahrb_revision: String,
    /// OS and architecture.
    pub platform: String,
    /// Quick or cert.
    pub profile: String,
    /// Matrix rows present in the bundle.
    pub rows_run: Vec<u8>,
    /// UTC run-start timestamp.
    pub timestamp: String,
    /// Outcome totals.
    pub counts: OutcomeCounts,
    /// Certified badge, or null.
    pub badge: Option<Badge>,
    /// Stable resource headline fields.
    pub resource_summary: IndexedResourceSummary,
    /// Repository-relative durable bundle path.
    pub results_dir: String,
    /// One-minute load average at run start, or null when unavailable.
    pub load_avg_1m: Option<f64>,
}

/// Resolve default output and capture run-start metadata.
pub fn prepare(options: &RunOptions, manifest: &Manifest) -> Result<RunPersistence> {
    let repository = repository_root();
    let timestamp = utc_timestamp(SystemTime::now())?;
    let short_id = short_run_id(&manifest.identity.id, &timestamp);
    let relative_results = PathBuf::from("results")
        .join(&manifest.identity.id)
        .join(format!("{timestamp}-{short_id}"));
    let no_save = options.no_save || no_save_from_environment()?;
    let results_dir = (!no_save).then(|| repository.join(&relative_results));
    let output = if options.output.as_os_str().is_empty() {
        results_dir
            .clone()
            .unwrap_or_else(|| repository.join("ahrb-output").join(&manifest.identity.id))
    } else {
        options.output.clone()
    };
    let harness_version = options
        .harness_version
        .clone()
        .or_else(|| {
            crate::manifest::doctor(&options.manifest)
                .ok()
                .and_then(|report| report.version)
        })
        .filter(|version| !version.trim().is_empty())
        .or_else(|| {
            (!manifest.identity.revision.trim().is_empty())
                .then(|| manifest.identity.revision.clone())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    Ok(RunPersistence {
        output,
        results_dir,
        timestamp,
        harness_version,
        load_avg_1m: load_average_1m(),
        results_dir_field: (!no_save).then(|| path_string(&relative_results)),
    })
}

fn no_save_from_environment() -> Result<bool> {
    let Some(value) = std::env::var_os("AHRB_NO_SAVE") else {
        return Ok(false);
    };
    match value.to_string_lossy().trim() {
        "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
        "0" | "false" | "FALSE" | "no" | "NO" | "" => Ok(false),
        _ => Err(AhrbError::Usage(
            "AHRB_NO_SAVE must be 1/0, true/false, or yes/no".to_owned(),
        )),
    }
}

/// Write the primary report, mirror it to `results/` when needed, and append
/// exactly one history line for an auto-saved run.
pub fn persist_report(
    persistence: &RunPersistence,
    report: &Report,
    junit: bool,
    include_run_error: bool,
) -> Result<()> {
    crate::report::write_bundle(report, &persistence.output, junit)?;
    let Some(results_dir) = &persistence.results_dir else {
        return Ok(());
    };
    if results_dir != &persistence.output {
        crate::report::write_bundle(report, results_dir, junit)?;
        if include_run_error {
            copy_optional(
                &persistence.output.join("run-error.txt"),
                &results_dir.join("run-error.txt"),
            )?;
        }
    }
    append_index(&index_entry(persistence, report)?)
}

fn copy_optional(source: &Path, destination: &Path) -> Result<()> {
    match std::fs::read(source) {
        Ok(bytes) => {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(destination, bytes)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn index_entry(persistence: &RunPersistence, report: &Report) -> Result<IndexEntry> {
    let results_dir = persistence.results_dir_field.clone().ok_or_else(|| {
        AhrbError::Protocol("saved result has no repository-relative path".to_owned())
    })?;
    let mut counts = OutcomeCounts::default();
    for result in &report.results {
        match result.outcome {
            TestOutcome::Pass => counts.pass = counts.pass.saturating_add(1),
            TestOutcome::Fail(_) => counts.fail = counts.fail.saturating_add(1),
            TestOutcome::Unsupported(_) => {
                counts.unsupported = counts.unsupported.saturating_add(1);
            }
            TestOutcome::Error(_) | TestOutcome::Absent(_) => {
                counts.error = counts.error.saturating_add(1);
            }
        }
    }
    Ok(IndexEntry {
        harness_id: report.fingerprint.harness.clone(),
        harness_version: persistence.harness_version.clone(),
        manifest_hash: report.fingerprint.manifest.clone(),
        ahrb_revision: report.fingerprint.ahrb_revision.clone(),
        platform: report.fingerprint.platform.clone(),
        profile: report.fingerprint.profile.clone(),
        rows_run: report.results.iter().map(|result| result.row).collect(),
        timestamp: persistence.timestamp.clone(),
        counts,
        badge: report.badge.clone(),
        resource_summary: indexed_summary(&report.resource_summary),
        results_dir,
        load_avg_1m: persistence.load_avg_1m,
    })
}

fn indexed_summary(summary: &ResourceSummary) -> IndexedResourceSummary {
    IndexedResourceSummary {
        peak_rss_mib: summary.peak_rss_mib,
        cpu_per_turn_ms: summary.cpu_per_turn_ms,
        wall_per_turn_ms: summary.wall_per_turn_ms,
        sampler_overhead_pct: summary.sampler_overhead_pct,
    }
}

fn append_index(entry: &IndexEntry) -> Result<()> {
    let index = repository_root().join("results/index.jsonl");
    if let Some(parent) = index.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(index)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: flock operates on this live file descriptor and is released
        // explicitly below (and by close on every error path).
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    let mut line = serde_json::to_vec(entry)?;
    line.push(b'\n');
    let write_result = file.write_all(&line).and_then(|()| file.sync_data());
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd as _;
        // SAFETY: unlocking the same live descriptor has no memory-safety preconditions.
        let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    }
    write_result?;
    Ok(())
}

/// Print compact saved-run history. By default only the latest entry for each
/// harness is shown; `all` retains every matching line.
pub fn print_history(harness: Option<&str>, all: bool) -> Result<()> {
    let path = repository_root().join("results/index.jsonl");
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: IndexEntry = serde_json::from_str(line).map_err(|error| {
            AhrbError::Protocol(format!(
                "invalid results/index.jsonl line {}: {error}",
                index + 1
            ))
        })?;
        if harness.is_none_or(|wanted| wanted == entry.harness_id) {
            entries.push(entry);
        }
    }
    if !all {
        let mut latest = BTreeMap::new();
        for entry in entries {
            latest.insert(entry.harness_id.clone(), entry);
        }
        entries = latest.into_values().collect();
    }
    println!(
        "HARNESS\tVERSION\tPROFILE\tROWS\tPASS\tFAIL\tUNSUP\tERROR\tBADGE\tLOAD1\tSTARTED\tRESULTS"
    );
    for entry in entries {
        let badge = entry
            .badge
            .as_ref()
            .map(badge_label)
            .unwrap_or_else(|| "-".to_owned());
        let load = entry
            .load_avg_1m
            .map(|value| format!("{value:.2}"))
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            clean_field(&entry.harness_id),
            clean_field(&entry.harness_version),
            entry.profile,
            compact_rows(&entry.rows_run),
            entry.counts.pass,
            entry.counts.fail,
            entry.counts.unsupported,
            entry.counts.error,
            clean_field(&badge),
            load,
            entry.timestamp,
            entry.results_dir,
        );
    }
    Ok(())
}

fn clean_field(value: &str) -> String {
    value.replace(['\t', '\n', '\r'], " ")
}

fn compact_rows(rows: &[u8]) -> String {
    let mut rows = rows.to_vec();
    rows.sort_unstable();
    rows.dedup();
    let mut parts = Vec::new();
    let mut index = 0;
    while index < rows.len() {
        let start = rows[index];
        let mut end = start;
        while index + 1 < rows.len() && rows[index + 1] == end.saturating_add(1) {
            index += 1;
            end = rows[index];
        }
        if start == end {
            parts.push(start.to_string());
        } else {
            parts.push(format!("{start}-{end}"));
        }
        index += 1;
    }
    parts.join(",")
}

fn repository_root() -> PathBuf {
    if let Some(root) = std::env::var_os("AHRB_RESULTS_ROOT")
        && !root.is_empty()
    {
        return PathBuf::from(root);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn short_run_id(harness: &str, timestamp: &str) -> String {
    let sequence = RESULT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut digest = Sha256::new();
    digest.update(harness.as_bytes());
    digest.update(timestamp.as_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(sequence.to_le_bytes());
    let value = format!("{:x}", digest.finalize());
    value[..8].to_owned()
}

fn utc_timestamp(now: SystemTime) -> Result<String> {
    let seconds = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AhrbError::Protocol("system clock predates the Unix epoch".to_owned()))?
        .as_secs();
    let days = i64::try_from(seconds / 86_400)
        .map_err(|_| AhrbError::Protocol("system timestamp exceeds i64 days".to_owned()))?;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"
    ))
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days.saturating_add(719_468);
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted.saturating_sub(146_096)
    } / 146_097;
    let day_of_era = shifted.saturating_sub(era.saturating_mul(146_097));
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era.saturating_add(era.saturating_mul(400));
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

fn load_average_1m() -> Option<f64> {
    let mut values = [0.0_f64; 3];
    // SAFETY: `values` has room for all three load averages.
    let count = unsafe { libc::getloadavg(values.as_mut_ptr(), 3) };
    (count >= 1).then_some(values[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_epoch_formats_as_utc_iso() -> Result<()> {
        assert_eq!(utc_timestamp(UNIX_EPOCH)?, "1970-01-01T00:00:00Z");
        assert_eq!(
            utc_timestamp(UNIX_EPOCH + std::time::Duration::from_secs(951_827_696))?,
            "2000-02-29T12:34:56Z"
        );
        Ok(())
    }

    #[test]
    fn row_ranges_are_compact() {
        assert_eq!(compact_rows(&[3, 1, 2, 30, 31, 41]), "1-3,30-31,41");
    }
}
