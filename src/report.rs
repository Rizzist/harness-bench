//! Human-readable, machine-readable, and raw-evidence reports.

use crate::evaluate::{Badge, TestOutcome, TestResult, badge_label};
use crate::process::{ProcessSample, Sample};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// One auditable recursive process-membership refresh boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipSample {
    /// Monotonic nanoseconds since resource collection began.
    pub elapsed_ns: u64,
    /// Resource phase active at this refresh.
    pub phase: String,
    /// Wall time consumed by recursive discovery.
    pub discovery_wall_ns: u64,
    /// Calling-thread CPU consumed by recursive discovery.
    pub discovery_cpu_ns: u64,
    /// Deterministic staggered sampler lane.
    pub lane: u32,
}

/// Reproducibility fingerprint fields.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Fingerprint {
    /// Harness artifact or executable digest.
    pub harness: String,
    /// Harness version.
    pub harness_version: String,
    /// Manifest digest.
    pub manifest: String,
    /// Workflow set digest.
    pub workflows: String,
    /// Fake-model engine version.
    pub fake_model: String,
    /// Event normalizer version.
    pub normalizer: String,
    /// AHRB source revision.
    pub ahrb_revision: String,
    /// OS/kernel/architecture summary.
    pub platform: String,
    /// Host physical memory bytes.
    pub host_memory_bytes: u64,
    /// Quick or certification profile.
    pub profile: String,
}

/// One numeric resource metric with its mandatory topology comparison scope.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TopologyMetric {
    /// Numeric observation in the unit encoded by the metric name.
    pub value: f64,
    /// Architecture topology under which the observation was measured.
    pub topology: String,
    /// Normative comparison guard. Resource classes and marginal beta values
    /// may only be compared when this topology label is identical.
    pub comparison_scope: String,
}

/// Topology-agnostic resource and performance headline values.
///
/// Every value here is derived after workload execution from AHRB's external
/// whole-tree samples, membership-discovery accounting, and the external turn
/// wall clocks that already enforce turn deadlines. Summary construction never
/// executes synchronously in the harness turn path.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ResourceSummary {
    /// Maximum effective owned-tree memory over the sampled run.
    pub peak_rss_mib: f64,
    /// Arithmetic mean effective owned-tree memory over all samples.
    pub mean_rss_mib: f64,
    /// Median effective owned-tree memory over all samples.
    pub median_rss_mib: f64,
    /// Cumulative owned-tree CPU delta over the sampled run.
    pub cpu_total_s: f64,
    /// Cumulative owned-tree CPU divided by executed workflow turns.
    pub cpu_per_turn_ms: f64,
    /// Mean external AHRB turn wall clock.
    pub wall_per_turn_ms: f64,
    /// Resident daemon baseline; absent for zero-process-between-turns topologies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_rss_mib: Option<f64>,
    /// Parallel marginal memory where a complete sweep supports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_beta_mib_per_agent: Option<f64>,
    /// Parallel scaling exponent where a complete sweep supports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_alpha: Option<f64>,
    /// Membership sampler CPU as a percentage of one core.
    pub sampler_overhead_pct: f64,
}

/// Complete benchmark report and embedded evidence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Report {
    /// Report schema version.
    pub schema: u32,
    /// Deterministic run identifier.
    pub run_id: String,
    /// Canonical short root holding isolated harness state for this run.
    #[serde(default)]
    pub profile_path: String,
    /// Reproducibility fingerprint.
    pub fingerprint: Fingerprint,
    /// Matrix results sorted by row.
    pub results: Vec<TestResult>,
    /// Badge when every topology-relative CORE gate passes.
    pub badge: Option<Badge>,
    /// Named non-resource automation diagnostics. Resource observations live
    /// exclusively in `resource_metrics` so topology labels cannot be dropped.
    pub metrics: BTreeMap<String, f64>,
    /// Typed daemon-shutdown outcomes and any owned-tree escalation performed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lifecycle_notes: Vec<String>,
    /// Raw JSON returned by headless resume/recovery controls, annotated with
    /// the local session and action that produced it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_evidence: Vec<Value>,
    /// Resource metrics with topology labels and within-topology comparison scope.
    #[serde(default)]
    pub resource_metrics: BTreeMap<String, TopologyMetric>,
    /// Cross-topology headline summary derived from external observations.
    #[serde(default)]
    pub resource_summary: ResourceSummary,
    /// Raw resource samples.
    pub samples: Vec<Sample>,
    /// Raw process observations.
    pub processes: Vec<ProcessSample>,
    /// Raw recursive process-membership refresh timestamps.
    #[serde(default)]
    pub membership: Vec<MembershipSample>,
    /// Normalized event JSON records.
    pub events: Vec<Value>,
    /// Redacted fake-model request JSON records.
    pub model_requests: Vec<Value>,
}

/// Persist the full report bundle using stable names and ordering.
pub fn write_bundle(report: &Report, directory: &Path, junit: bool) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    write_atomic(
        &directory.join("report.json"),
        &serde_json::to_vec_pretty(report)?,
    )?;
    write_atomic(
        &directory.join("report.md"),
        render_markdown(report).as_bytes(),
    )?;
    write_jsonl(&directory.join("samples.jsonl"), &report.samples)?;
    write_jsonl(&directory.join("processes.jsonl"), &report.processes)?;
    write_jsonl(&directory.join("membership.jsonl"), &report.membership)?;
    write_jsonl(&directory.join("events.jsonl"), &report.events)?;
    write_jsonl(
        &directory.join("model-requests.jsonl"),
        &report.model_requests,
    )?;
    if junit {
        write_atomic(
            &directory.join("junit.xml"),
            render_junit(report).as_bytes(),
        )?;
    }
    Ok(())
}

/// Persist a best-effort diagnostic when a run aborts before its bundle is complete.
pub fn write_failure_diagnostic(
    directory: &Path,
    manifest: &Path,
    error: &AhrbError,
) -> Result<PathBuf> {
    std::fs::create_dir_all(directory)?;
    let path = directory.join("run-error.txt");
    let content = format!(
        "AHRB run aborted\nmanifest={}\nreport=report.json\nerror={error}\n",
        manifest.display()
    );
    write_atomic(&path, content.as_bytes())?;
    Ok(path)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = std::path::PathBuf::from(temporary);
    std::fs::write(&temporary, bytes)?;
    let file = std::fs::OpenOptions::new().write(true).open(&temporary)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn write_jsonl<T: Serialize>(path: &Path, records: &[T]) -> Result<()> {
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record)?;
        bytes.push(b'\n');
    }
    write_atomic(path, &bytes)
}

/// Render the human-readable Markdown report.
pub fn render_markdown(report: &Report) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "# AHRB report `{}`\n", report.run_id);
    if !report.profile_path.is_empty() {
        let _ = writeln!(output, "Profile: `{}`\n", report.profile_path);
    }
    if let Some(badge) = &report.badge {
        let _ = writeln!(output, "**{}**\n", badge_label(badge));
    } else {
        let _ = writeln!(output, "**No badge certified.**\n");
    }
    let _ = writeln!(output, "| Row | Pillar | Test | Outcome |");
    let _ = writeln!(output, "|---:|---|---|---|");
    let mut results: Vec<&TestResult> = report.results.iter().collect();
    results.sort_by_key(|result| result.row);
    for result in results {
        let _ = writeln!(
            output,
            "| {} | {:?} | `{}` | {} |",
            result.row,
            result.pillar,
            result.id,
            outcome_label(&result.outcome)
        );
    }
    let _ = writeln!(output, "\n## Resource summary\n");
    let _ = writeln!(
        output,
        "`{}`",
        render_resource_summary(&report.resource_summary)
    );
    if !report.resource_metrics.is_empty() {
        let topology = report
            .resource_metrics
            .values()
            .next()
            .map(|metric| metric.topology.as_str())
            .unwrap_or("unknown");
        let _ = writeln!(output, "\n## Resource metrics — `{topology}`\n");
        let _ = writeln!(
            output,
            "> R-class and marginal β are comparable only within the same topology.\n"
        );
        for (name, metric) in &report.resource_metrics {
            let _ = writeln!(
                output,
                "- `{name}`: {:.3} (topology: `{}`; scope: `{}`)",
                metric.value, metric.topology, metric.comparison_scope
            );
        }
    }
    let diagnostic_metrics: BTreeMap<_, _> = report
        .metrics
        .iter()
        .filter(|(name, _)| !report.resource_metrics.contains_key(*name))
        .collect();
    if !diagnostic_metrics.is_empty() {
        let _ = writeln!(output, "\n## Automation diagnostics\n");
        for (name, value) in diagnostic_metrics {
            let _ = writeln!(output, "- `{name}`: {value:.3}");
        }
    }
    let _ = writeln!(output, "\n## Fingerprint\n");
    let _ = writeln!(output, "- Harness: `{}`", report.fingerprint.harness);
    let _ = writeln!(output, "- Manifest: `{}`", report.fingerprint.manifest);
    let _ = writeln!(output, "- Workflows: `{}`", report.fingerprint.workflows);
    let _ = writeln!(output, "- Platform: `{}`", report.fingerprint.platform);
    let _ = writeln!(output, "- Profile: `{}`", report.fingerprint.profile);
    output
}

/// Build the concise resource-summary line shared by `ahrb run` and `hbench`.
pub fn render_resource_summary(summary: &ResourceSummary) -> String {
    let mut output = format!(
        "resource_summary peak_rss_mib={:.3} mean_rss_mib={:.3} median_rss_mib={:.3} cpu_total_s={:.3} cpu_per_turn_ms={:.3} wall_per_turn_ms={:.3}",
        summary.peak_rss_mib,
        summary.mean_rss_mib,
        summary.median_rss_mib,
        summary.cpu_total_s,
        summary.cpu_per_turn_ms,
        summary.wall_per_turn_ms,
    );
    if let Some(value) = summary.idle_rss_mib {
        let _ = write!(output, " idle_rss_mib={value:.3}");
    }
    if let Some(value) = summary.parallel_beta_mib_per_agent {
        let _ = write!(output, " parallel_beta_mib_per_agent={value:.3}");
    }
    if let Some(value) = summary.scaling_alpha {
        let _ = write!(output, " scaling_alpha={value:.3}");
    }
    let _ = write!(
        output,
        " sampler_overhead_pct={:.3}",
        summary.sampler_overhead_pct
    );
    output
}

/// Derive summary values exclusively from already-collected external evidence.
///
/// `turn_wall_ns` contains durations from AHRB's pre-existing external deadline
/// clocks; this function performs no sampling and does not interact with a
/// harness. On macOS effective memory is physical footprint, while on Linux it
/// is PSS when available and RSS otherwise.
#[allow(clippy::too_many_arguments)]
pub fn summarize_resources(
    samples: &[Sample],
    membership: &[MembershipSample],
    workflow_turns: u64,
    turn_wall_ns: &[u64],
    idle_rss_mib: Option<f64>,
    parallel_beta_mib_per_agent: Option<f64>,
    scaling_alpha: Option<f64>,
) -> ResourceSummary {
    const MIB: f64 = 1_048_576.0;
    let mut memory = samples
        .iter()
        .map(effective_memory_bytes)
        .collect::<Vec<_>>();
    let peak_bytes = memory.iter().copied().max().unwrap_or(0);
    let mean_bytes = if memory.is_empty() {
        0.0
    } else {
        memory.iter().map(|value| *value as f64).sum::<f64>() / memory.len() as f64
    };
    memory.sort_unstable();
    let median_bytes = match memory.len() {
        0 => 0.0,
        length if length % 2 == 1 => memory[length / 2] as f64,
        length => {
            let upper = memory[length / 2] as f64;
            let lower = memory[length / 2 - 1] as f64;
            (lower + upper) / 2.0
        }
    };
    let cpu_ns = samples
        .first()
        .zip(samples.last())
        .map_or(0, |(first, last)| last.cpu_ns.saturating_sub(first.cpu_ns));
    let cpu_per_turn_ms = if workflow_turns == 0 {
        0.0
    } else {
        cpu_ns as f64 / workflow_turns as f64 / 1_000_000.0
    };
    let wall_per_turn_ms = if turn_wall_ns.is_empty() {
        0.0
    } else {
        turn_wall_ns.iter().map(|value| *value as f64).sum::<f64>()
            / turn_wall_ns.len() as f64
            / 1_000_000.0
    };
    let sampling_wall_span = membership
        .iter()
        .map(|sample| sample.elapsed_ns)
        .min()
        .zip(membership.iter().map(|sample| sample.elapsed_ns).max())
        .map_or(0, |(first, last)| last.saturating_sub(first));
    let discovery_cpu_ns = membership.iter().fold(0_u64, |total, sample| {
        total.saturating_add(sample.discovery_cpu_ns)
    });
    let sampler_overhead_pct = if sampling_wall_span == 0 {
        0.0
    } else {
        100.0 * discovery_cpu_ns as f64 / sampling_wall_span as f64
    };
    ResourceSummary {
        peak_rss_mib: peak_bytes as f64 / MIB,
        mean_rss_mib: mean_bytes / MIB,
        median_rss_mib: median_bytes / MIB,
        cpu_total_s: cpu_ns as f64 / 1_000_000_000.0,
        cpu_per_turn_ms,
        wall_per_turn_ms,
        idle_rss_mib,
        parallel_beta_mib_per_agent,
        scaling_alpha,
        sampler_overhead_pct,
    }
}

fn effective_memory_bytes(sample: &Sample) -> u64 {
    #[cfg(target_os = "macos")]
    {
        sample.footprint_bytes.unwrap_or(sample.rss_bytes)
    }
    #[cfg(target_os = "linux")]
    {
        sample.pss_bytes.unwrap_or(sample.rss_bytes)
    }
}

fn outcome_label(outcome: &TestOutcome) -> &'static str {
    match outcome {
        TestOutcome::Pass => "PASS",
        TestOutcome::Fail(_) => "FAIL",
        TestOutcome::Unsupported(_) => "UNSUPPORTED",
        TestOutcome::Error(_) => "ERROR",
        TestOutcome::Absent(_) => "ABSENT",
    }
}

fn render_junit(report: &Report) -> String {
    let failures = report
        .results
        .iter()
        .filter(|result| {
            !matches!(
                result.outcome,
                TestOutcome::Pass | TestOutcome::Unsupported(_)
            )
        })
        .count();
    let skipped = report
        .results
        .iter()
        .filter(|result| matches!(result.outcome, TestOutcome::Unsupported(_)))
        .count();
    let mut output = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"ahrb\" tests=\"{}\" failures=\"{}\" skipped=\"{}\">\n",
        report.results.len(),
        failures,
        skipped
    );
    let mut results: Vec<&TestResult> = report.results.iter().collect();
    results.sort_by_key(|result| result.row);
    for result in results {
        let _ = writeln!(
            output,
            "  <testcase classname=\"{:?}\" name=\"{}-{}\">",
            result.pillar,
            result.row,
            xml_escape(&result.id)
        );
        match &result.outcome {
            TestOutcome::Pass => {}
            TestOutcome::Unsupported(detail) => {
                let _ = writeln!(output, "    <skipped message=\"{}\" />", xml_escape(detail));
            }
            other => {
                let _ = writeln!(
                    output,
                    "    <failure message=\"{}\" />",
                    xml_escape(outcome_label(other))
                );
            }
        }
        let _ = writeln!(output, "  </testcase>");
    }
    output.push_str("</testsuite>\n");
    output
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
