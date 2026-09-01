//! Durable benchmark result bundles and compact history reporting.

use crate::cli::RunOptions;
use crate::evaluate::{Badge, TestOutcome, badge_label};
use crate::manifest::Manifest;
use crate::report::Report;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static RESULT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Per-run persistence information captured before workload execution.
#[derive(Clone, Debug)]
pub struct RunPersistence {
    /// Primary output requested by the user, or the default durable bundle.
    pub output: PathBuf,
    /// Short, output-independent root for isolated harness state.
    pub profile_path: PathBuf,
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
    /// Row-47 disk-write p50 in bytes per turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_p50: Option<f64>,
    /// Row-47 disk-write p95 in bytes per turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_p95: Option<f64>,
    /// Row-47 global maximum bytes written by one turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_bytes_per_turn_max: Option<f64>,
    /// Row-47 median journal growth in bytes per turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_journal_growth_bytes_per_turn: Option<f64>,
    /// Row-47 nullable declared-log growth in bytes per turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_growth_bytes_per_turn: Option<f64>,
    /// Row-47 Theil-Sen disk-write growth slope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_write_growth_slope_bytes_per_turn2: Option<f64>,
    /// Row-47 live/retired accounting completeness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_io_counter_complete: Option<bool>,
    /// Row-47 unbounded-growth decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unbounded_disk_growth: Option<bool>,
    /// Row-48 median owned-tree CPU during model wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_cpu_p50_ms: Option<f64>,
    /// Row-48 median response-header through final-frame wall time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_wall_p50_ms: Option<f64>,
    /// Row-48 maximum CPU/wall ratio against one core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_wait_cpu_one_core_max_ratio: Option<f64>,
    /// Row-60 maximum effective-memory delta above the topology baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub large_tool_output_peak_rss_delta_mib: Option<f64>,
}

/// One line in `results/index.jsonl`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IndexEntry {
    /// Version of the JSONL line contract. Legacy lines have no schema.
    pub schema: u32,
    /// Stable unique occurrence key for this run.
    pub run_key: String,
    /// UTC timestamp captured after report persistence completes.
    pub completed_at: String,
    /// Stable adapter identity.
    pub harness: String,
    /// Availability-probe version output.
    pub harness_version: String,
    /// Repository-relative report path.
    pub report_path: String,
    /// Machine-readable report schema.
    pub report_schema: u32,
    /// Authoritative AHRB specification version.
    pub spec_version: u32,
    /// Quick or cert measurement profile.
    pub profile: String,
    /// Operating system guard used by resource comparisons.
    pub os: String,
    /// Process-topology guard used by resource comparisons.
    pub topology: String,
    /// Compact resource metrics copied from the immutable report.
    #[serde(default)]
    pub resource_summary: IndexedResourceSummary,
    /// Exact row metric names copied from the immutable report for history queries.
    #[serde(default)]
    pub metrics: BTreeMap<String, f64>,
    /// Canonical manifest SHA-256.
    pub manifest_sha256: String,
    /// Canonical workflow-set SHA-256.
    pub workflow_sha256: String,
    /// AHRB build revision.
    pub ahrb_revision: String,
}

/// Historical unversioned line emitted before the diff-capable index contract.
#[derive(Clone, Debug, Default, Deserialize)]
struct LegacyIndexEntry {
    #[serde(default)]
    harness_id: String,
    #[serde(default)]
    harness_version: String,
    #[serde(default)]
    manifest_hash: String,
    #[serde(default)]
    ahrb_revision: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    badge: Option<Badge>,
    #[serde(default)]
    results_dir: String,
}

/// Current index-line schema. Schema 1 is the implicit legacy format.
pub const INDEX_SCHEMA: u32 = 2;

/// Resolve default output and capture run-start metadata.
pub fn prepare(options: &RunOptions, manifest: &Manifest) -> Result<RunPersistence> {
    let repository = absolute_path(&repository_root())?;
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
        absolute_path(&options.output)?
    };
    #[cfg(unix)]
    let temporary_root = PathBuf::from("/tmp");
    #[cfg(not(unix))]
    let temporary_root = std::env::temp_dir();
    let temporary_root = std::fs::canonicalize(&temporary_root).unwrap_or(temporary_root);
    let profile_path = temporary_root.join(format!("ahrb-{short_id}-{:x}", std::process::id()));
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
        profile_path,
        results_dir,
        timestamp,
        harness_version,
        load_avg_1m: load_average_1m(),
        results_dir_field: (!no_save).then(|| path_string(&relative_results)),
    })
}

/// Resolve a path against the CLI's current directory without requiring the
/// destination to exist. This must happen before profile paths are rendered
/// into harness environment/config templates.
fn absolute_path(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(Into::into)
}

/// Source revision embedded by `build.rs`, preferring the build checkout's
/// Git HEAD and falling back there to `AHRB_REVISION` when Git is unavailable.
pub fn ahrb_revision() -> String {
    option_env!("AHRB_BUILD_REVISION")
        .unwrap_or("unknown")
        .to_owned()
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
    let completed_at = utc_timestamp(SystemTime::now())?;
    let results_dir = persistence.results_dir.as_ref().ok_or_else(|| {
        AhrbError::Protocol("saved result has no canonical results directory".to_owned())
    })?;
    let report_body = std::fs::read(results_dir.join("report.json"))?;
    let report_sha256: [u8; 32] = Sha256::digest(report_body).into();
    append_index(persistence, report, completed_at, &report_sha256)
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

fn index_entry(
    persistence: &RunPersistence,
    report: &Report,
    completed_at: String,
    occurrence: u64,
    report_sha256: &[u8; 32],
) -> Result<IndexEntry> {
    let results_dir = persistence.results_dir_field.clone().ok_or_else(|| {
        AhrbError::Protocol("saved result has no repository-relative path".to_owned())
    })?;
    let report_path = format!("{results_dir}/report.json");
    let run_key = stable_run_key(
        occurrence,
        &report.fingerprint.harness,
        &completed_at,
        &report_path,
        report_sha256,
    );
    Ok(IndexEntry {
        schema: INDEX_SCHEMA,
        run_key,
        completed_at,
        harness: report.fingerprint.harness.clone(),
        harness_version: persistence.harness_version.clone(),
        report_path,
        report_schema: report.schema,
        spec_version: report.spec_version,
        profile: report.fingerprint.profile.clone(),
        os: report
            .badge
            .as_ref()
            .map(|badge| badge.os.clone())
            .unwrap_or_else(|| std::env::consts::OS.to_owned()),
        topology: report.resource_summary.topology.clone(),
        resource_summary: IndexedResourceSummary {
            peak_rss_mib: report.resource_summary.peak_rss_mib,
            cpu_per_turn_ms: report.resource_summary.cpu_per_turn_ms,
            wall_per_turn_ms: report.resource_summary.wall_per_turn_ms,
            sampler_overhead_pct: report.resource_summary.sampler_overhead_pct,
            disk_write_bytes_per_turn_p50: report.resource_summary.disk_write_bytes_per_turn_p50,
            disk_write_bytes_per_turn_p95: report.resource_summary.disk_write_bytes_per_turn_p95,
            disk_write_bytes_per_turn_max: report.resource_summary.disk_write_bytes_per_turn_max,
            session_journal_growth_bytes_per_turn: report
                .resource_summary
                .session_journal_growth_bytes_per_turn,
            log_growth_bytes_per_turn: report.resource_summary.log_growth_bytes_per_turn,
            disk_write_growth_slope_bytes_per_turn2: report
                .resource_summary
                .disk_write_growth_slope_bytes_per_turn2,
            disk_io_counter_complete: report.resource_summary.disk_io_counter_complete,
            unbounded_disk_growth: report.resource_summary.unbounded_disk_growth,
            model_wait_cpu_p50_ms: report.resource_summary.model_wait_cpu_p50_ms,
            model_wait_wall_p50_ms: report.resource_summary.model_wait_wall_p50_ms,
            model_wait_cpu_one_core_max_ratio: report
                .resource_summary
                .model_wait_cpu_one_core_max_ratio,
            large_tool_output_peak_rss_delta_mib: report
                .resource_summary
                .large_tool_output_peak_rss_delta_mib,
        },
        metrics: report.metrics.clone(),
        manifest_sha256: report.fingerprint.manifest.clone(),
        workflow_sha256: report.fingerprint.workflows.clone(),
        ahrb_revision: report.fingerprint.ahrb_revision.clone(),
    })
}

fn append_index(
    persistence: &RunPersistence,
    report: &Report,
    completed_at: String,
    report_sha256: &[u8; 32],
) -> Result<()> {
    let index = repository_root().join("results/index.jsonl");
    if let Some(parent) = index.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
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
    file.seek(std::io::SeekFrom::Start(0))?;
    let mut existing = Vec::new();
    file.read_to_end(&mut existing)?;
    if !existing.is_empty() && existing.last() != Some(&b'\n') {
        return Err(AhrbError::Protocol(
            "results/index.jsonl does not end at a complete line".to_owned(),
        ));
    }
    let prior_lines = existing.iter().filter(|byte| **byte == b'\n').count();
    let occurrence = u64::try_from(prior_lines)
        .map_err(|_| AhrbError::Protocol("results index line count exceeds u64".to_owned()))?
        .checked_add(1)
        .ok_or_else(|| AhrbError::Protocol("results index occurrence overflow".to_owned()))?;
    let entry = index_entry(persistence, report, completed_at, occurrence, report_sha256)?;
    let mut line = serde_json::to_vec(&entry)?;
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
    let mut entries = read_index()?
        .into_iter()
        .filter(|entry| harness.is_none_or(|wanted| wanted == entry.harness))
        .collect::<Vec<_>>();
    if !all {
        let mut latest: BTreeMap<String, IndexEntry> = BTreeMap::new();
        for entry in entries {
            let entry_completed_at = utc_timestamp_seconds(&entry.completed_at)?;
            let should_replace = match latest.get(&entry.harness) {
                Some(existing) => {
                    let existing_completed_at = utc_timestamp_seconds(&existing.completed_at)?;
                    (entry_completed_at, &entry.run_key)
                        > (existing_completed_at, &existing.run_key)
                }
                None => true,
            };
            if should_replace {
                latest.insert(entry.harness.clone(), entry);
            }
        }
        entries = latest.into_values().collect();
    }
    println!(
        "HARNESS\tVERSION\tPROFILE\tROWS\tPASS\tFAIL\tUNSUP\tERROR\tBADGE\tCOMPLETED\tRESULTS"
    );
    for entry in entries {
        let report = load_indexed_report(&entry).ok();
        let counts = report
            .as_ref()
            .map(|report| outcome_counts(&report.results))
            .unwrap_or_default();
        let rows = report
            .as_ref()
            .map(|report| {
                report
                    .results
                    .iter()
                    .map(|result| result.row)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let badge = report
            .as_ref()
            .and_then(|report| report.badge.as_ref())
            .map(badge_label)
            .unwrap_or_else(|| "-".to_owned());
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            clean_field(&entry.harness),
            clean_field(&entry.harness_version),
            entry.profile,
            compact_rows(&rows),
            counts.pass,
            counts.fail,
            counts.unsupported,
            counts.error,
            clean_field(&badge),
            entry.completed_at,
            Path::new(&entry.report_path)
                .parent()
                .map(path_string)
                .unwrap_or_else(|| entry.report_path.clone()),
        );
    }
    Ok(())
}

/// Read current and legacy index lines into the current normalized contract.
pub fn read_index() -> Result<Vec<IndexEntry>> {
    let path = repository_root().join("results/index.jsonl");
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error.into()),
    };
    let mut entries = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
            AhrbError::Protocol(format!(
                "invalid results/index.jsonl line {}: {error}",
                line_index + 1
            ))
        })?;
        let entry = if value.get("schema").is_some() {
            let entry: IndexEntry = serde_json::from_value(value).map_err(|error| {
                AhrbError::Protocol(format!(
                    "invalid versioned results/index.jsonl line {}: {error}",
                    line_index + 1
                ))
            })?;
            if entry.schema != INDEX_SCHEMA {
                return Err(AhrbError::Protocol(format!(
                    "unsupported results/index.jsonl schema {} on line {}",
                    entry.schema,
                    line_index + 1
                )));
            }
            entry
        } else {
            let legacy: LegacyIndexEntry = serde_json::from_value(value).map_err(|error| {
                AhrbError::Protocol(format!(
                    "invalid legacy results/index.jsonl line {}: {error}",
                    line_index + 1
                ))
            })?;
            normalize_legacy_index(legacy, line, line_index + 1)
        };
        utc_timestamp_seconds(&entry.completed_at).map_err(|error| {
            AhrbError::Protocol(format!(
                "invalid completed_at on results/index.jsonl line {}: {error}",
                line_index + 1
            ))
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

/// Resolve and deserialize the report referenced by an index occurrence.
pub fn load_indexed_report(entry: &IndexEntry) -> Result<Report> {
    let value = load_indexed_report_value(entry)?;
    serde_json::from_value(value).map_err(|error| {
        AhrbError::Protocol(format!(
            "could not deserialize indexed report {}: {error}",
            entry.report_path
        ))
    })
}

/// Load the original report JSON without erasing old-schema field absence.
pub fn load_indexed_report_value(entry: &IndexEntry) -> Result<serde_json::Value> {
    let path = PathBuf::from(&entry.report_path);
    let path = if path.is_absolute() {
        path
    } else {
        repository_root().join(path)
    };
    let bytes = std::fs::read(&path).map_err(|error| {
        AhrbError::Protocol(format!(
            "could not read indexed report {}: {error}",
            path.display()
        ))
    })?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        AhrbError::Protocol(format!(
            "could not parse indexed report {}: {error}",
            path.display()
        ))
    })?;
    let report_schema = value
        .get("schema")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            AhrbError::Protocol(format!(
                "indexed report {} has no valid schema",
                path.display()
            ))
        })?;
    if report_schema != entry.report_schema {
        return Err(AhrbError::Protocol(format!(
            "indexed report schema mismatch for {}: index {}, report {}",
            entry.run_key, entry.report_schema, report_schema
        )));
    }
    Ok(value)
}

fn normalize_legacy_index(
    legacy: LegacyIndexEntry,
    raw_line: &str,
    line_number: usize,
) -> IndexEntry {
    let report_path = format!("{}/report.json", legacy.results_dir.trim_end_matches('/'));
    let os = legacy
        .badge
        .as_ref()
        .map(|badge| badge.os.clone())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            legacy
                .platform
                .split(['-', ' '])
                .next()
                .unwrap_or("unknown")
                .to_owned()
        });
    let topology = legacy
        .badge
        .as_ref()
        .map(|badge| badge.topology.clone())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut entry = IndexEntry {
        schema: INDEX_SCHEMA,
        run_key: stable_legacy_run_key(raw_line, line_number),
        completed_at: legacy.timestamp,
        harness: legacy.harness_id,
        harness_version: legacy.harness_version,
        report_path,
        report_schema: 2,
        spec_version: 1,
        profile: legacy.profile,
        os,
        topology,
        resource_summary: IndexedResourceSummary::default(),
        metrics: BTreeMap::new(),
        manifest_sha256: legacy.manifest_hash,
        workflow_sha256: "legacy-unavailable".to_owned(),
        ahrb_revision: legacy.ahrb_revision,
    };
    if let Ok(report) = load_report_without_schema_check(&entry) {
        entry.report_schema = report.schema;
        entry.spec_version = report.spec_version;
        entry.harness.clone_from(&report.fingerprint.harness);
        entry
            .harness_version
            .clone_from(&report.fingerprint.harness_version);
        entry.profile.clone_from(&report.fingerprint.profile);
        entry
            .manifest_sha256
            .clone_from(&report.fingerprint.manifest);
        entry
            .workflow_sha256
            .clone_from(&report.fingerprint.workflows);
        entry.metrics.clone_from(&report.metrics);
        entry
            .ahrb_revision
            .clone_from(&report.fingerprint.ahrb_revision);
        entry.topology = if report.resource_summary.topology.is_empty() {
            report
                .badge
                .as_ref()
                .map(|badge| badge.topology.clone())
                .unwrap_or(entry.topology)
        } else {
            report.resource_summary.topology
        };
        entry.os = report.badge.map_or_else(
            || {
                report
                    .fingerprint
                    .platform
                    .split(['-', ' '])
                    .next()
                    .unwrap_or("unknown")
                    .to_owned()
            },
            |badge| badge.os,
        );
    }
    entry
}

fn load_report_without_schema_check(entry: &IndexEntry) -> Result<Report> {
    let path = PathBuf::from(&entry.report_path);
    let path = if path.is_absolute() {
        path
    } else {
        repository_root().join(path)
    };
    let bytes = std::fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn outcome_counts(results: &[crate::evaluate::TestResult]) -> OutcomeCounts {
    let mut counts = OutcomeCounts::default();
    for result in results {
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
    counts
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

fn stable_run_key(
    occurrence: u64,
    harness: &str,
    completed_at: &str,
    report_path: &str,
    report_sha256: &[u8; 32],
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ahrb-index-v2-occurrence\0");
    digest.update(occurrence.to_le_bytes());
    digest.update(b"\0");
    digest.update(harness.as_bytes());
    digest.update(b"\0");
    digest.update(completed_at.as_bytes());
    digest.update(b"\0");
    digest.update(report_path.as_bytes());
    digest.update(b"\0");
    digest.update(report_sha256);
    format!("run-{:x}", digest.finalize())
}

fn stable_legacy_run_key(raw_line: &str, line_number: usize) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ahrb-index-legacy-v1\0");
    digest.update((line_number as u64).to_le_bytes());
    digest.update(b"\0");
    digest.update(raw_line.as_bytes());
    format!("legacy-{:x}", digest.finalize())
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

/// Parse the canonical index timestamp into Unix seconds for chronological
/// comparison. Only `YYYY-MM-DDTHH:MM:SSZ` UTC timestamps are accepted.
pub(crate) fn utc_timestamp_seconds(value: &str) -> Result<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(AhrbError::Protocol(format!(
            "timestamp {value:?} is not canonical YYYY-MM-DDTHH:MM:SSZ UTC"
        )));
    }
    let year = timestamp_digits(bytes, 0, 4)?;
    let month = timestamp_digits(bytes, 5, 7)?;
    let day = timestamp_digits(bytes, 8, 10)?;
    let hour = timestamp_digits(bytes, 11, 13)?;
    let minute = timestamp_digits(bytes, 14, 16)?;
    let second = timestamp_digits(bytes, 17, 19)?;
    if !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(AhrbError::Protocol(format!(
            "timestamp {value:?} contains an invalid UTC calendar value"
        )));
    }
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .ok_or_else(|| AhrbError::Protocol("timestamp exceeds u64 seconds".to_owned()))?;
    u64::try_from(seconds)
        .map_err(|_| AhrbError::Protocol("timestamp predates the Unix epoch".to_owned()))
}

fn timestamp_digits(bytes: &[u8], start: usize, end: usize) -> Result<i64> {
    let digits = bytes.get(start..end).ok_or_else(|| {
        AhrbError::Protocol("timestamp component is outside the input".to_owned())
    })?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return Err(AhrbError::Protocol(
            "timestamp components must contain only ASCII digits".to_owned(),
        ));
    }
    let mut value = 0_i64;
    for digit in digits {
        value = value * 10 + i64::from(*digit - b'0');
    }
    Ok(value)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
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

    fn mock_options(output: PathBuf) -> RunOptions {
        RunOptions {
            manifest: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml"),
            output,
            profile: crate::cli::Profile::Quick,
            tests: vec![1],
            junit: false,
            deadline_secs: Some(120),
            no_save: true,
            harness_version: Some("mock 1".to_owned()),
        }
    }

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
    fn canonical_utc_timestamps_parse_to_numeric_order_keys() -> Result<()> {
        assert_eq!(utc_timestamp_seconds("1970-01-01T00:00:00Z")?, 0);
        assert_eq!(utc_timestamp_seconds("2000-02-29T12:34:56Z")?, 951_827_696);
        assert!(utc_timestamp_seconds("2026-09-01T00:00:00.000Z").is_err());
        assert!(utc_timestamp_seconds("2026-02-29T00:00:00Z").is_err());
        assert!(utc_timestamp_seconds("2026-09-01T00:00:60Z").is_err());
        Ok(())
    }

    #[test]
    fn run_key_is_unique_for_same_second_path_and_report_body() {
        let report_sha256 = [7_u8; 32];
        let first = stable_run_key(
            1,
            "mock",
            "2026-09-01T00:00:00Z",
            "results/mock/report.json",
            &report_sha256,
        );
        let second = stable_run_key(
            2,
            "mock",
            "2026-09-01T00:00:00Z",
            "results/mock/report.json",
            &report_sha256,
        );
        assert_ne!(first, second);
    }

    #[test]
    fn row_ranges_are_compact() {
        assert_eq!(compact_rows(&[3, 1, 2, 30, 31, 41]), "1-3,30-31,41");
    }

    #[test]
    fn persistence_resolves_relative_output_before_profile_rendering() -> Result<()> {
        let manifest = crate::manifest::load(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml"),
        )?;
        let persistence = prepare(&mock_options(PathBuf::from("relative-output")), &manifest)?;
        assert!(persistence.output.is_absolute());
        let mut saved_options = mock_options(PathBuf::new());
        saved_options.no_save = false;
        let saved = prepare(&saved_options, &manifest)?;
        assert!(saved.output.is_absolute());
        assert!(saved.results_dir.is_some_and(|path| path.is_absolute()));
        Ok(())
    }

    #[test]
    fn ordinary_build_embeds_a_real_source_revision() {
        let revision = ahrb_revision();
        assert_ne!(revision, "unknown");
        assert!(!revision.trim().is_empty());
    }

    #[test]
    fn long_output_does_not_lengthen_profile_path() -> Result<()> {
        let manifest = crate::manifest::load(Path::new("adapters/haider-agent/manifest.toml"))?;
        let options = RunOptions {
            manifest: PathBuf::from("adapters/haider-agent/manifest.toml"),
            output: PathBuf::from("/tmp").join("very-long-results-component-".repeat(12)),
            profile: crate::cli::Profile::Quick,
            tests: vec![1],
            junit: false,
            deadline_secs: None,
            no_save: true,
            harness_version: Some("0.0.967".to_owned()),
        };
        let persistence = prepare(&options, &manifest)?;
        assert!(!persistence.profile_path.starts_with(&options.output));
        assert!(persistence.profile_path.as_os_str().len() < 60);
        Ok(())
    }
}
