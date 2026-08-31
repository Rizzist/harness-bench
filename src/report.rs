//! Human-readable, machine-readable, and raw-evidence reports.

use crate::evaluate::{Badge, TestOutcome, TestResult, badge_label};
use crate::process::{ProcIdentity, ProcOwnership, ProcessSample, Sample};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
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

/// One externally observed semantic-turn interval on the shared monotonic clock.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnObservation {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// One-based turn number inside the repetition.
    pub turn_index: u32,
    /// Stable logical actor ID.
    pub actor: String,
    /// Non-secret stable digest of the harness session ID.
    pub session_id_hash: String,
    /// Stable fixture phase.
    pub phase: String,
    /// Child/controller launch boundary when applicable.
    pub launch_ns: Option<u64>,
    /// Turn submission boundary when applicable.
    pub submit_ns: Option<u64>,
    /// First completed provider request-body boundary when applicable.
    pub first_model_request_ns: Option<u64>,
    /// Structured terminal observation boundary when applicable.
    pub terminal_ns: Option<u64>,
    /// One-shot child exit boundary when applicable.
    pub exit_ns: Option<u64>,
    /// Exact topology-specific external turn interval.
    pub turn_wall_ns: Option<u64>,
}

/// One externally sampled process identity and its hygiene counters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneProcess {
    /// Stable PID/start-time identity.
    pub identity: ProcIdentity,
    /// Executable basename captured by the platform sampler.
    pub command: String,
    /// Evidence by which the process belongs to the harness tree.
    pub ownership: ProcOwnership,
    /// Live threads at this checkpoint.
    pub thread_count: Option<u64>,
    /// Live file descriptors at this checkpoint.
    pub open_fds: Option<u64>,
}

/// One externally collected membership/counter sample inside a semantic turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneCadenceSample {
    /// Nanoseconds since the row-44 turn sampler started.
    pub elapsed_ns: u64,
    /// Calling-thread CPU used by discovery plus counter collection.
    pub collection_cpu_ns: u64,
    /// Wall time used by discovery plus counter collection.
    pub collection_wall_ns: u64,
    /// Complete identity/thread/FD observation at this cadence boundary.
    pub processes: Vec<ProcessHygieneProcess>,
}

/// One ordered active-turn process/thread/FD checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneCheckpoint {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// One-based turn number; zero identifies a daemon warm baseline.
    pub turn_index: u32,
    /// Complete externally owned membership at the checkpoint.
    pub processes: Vec<ProcessHygieneProcess>,
    /// Repeated out-of-band samples spanning the active turn. Warm baselines
    /// intentionally leave this empty and use `processes` directly.
    #[serde(default)]
    pub cadence_samples: Vec<ProcessHygieneCadenceSample>,
    /// Submit/release through terminal/exit observation window.
    #[serde(default)]
    pub sampled_wall_ns: u64,
    /// Mandatory process-membership cadence for this platform/profile.
    #[serde(default)]
    pub required_cadence_ns: u64,
}

/// One delayed residue audit after a child exit, daemon close, or shutdown.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneAudit {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// Per-invocation turn index, absent for daemon close/shutdown audits.
    pub turn_index: Option<u32>,
    /// Actual elapsed audit window.
    pub waited_ms: u64,
    /// Complete externally owned membership after the audit window.
    pub processes: Vec<ProcessHygieneProcess>,
}

/// Raw row-44 evidence collected outside the harness turn path.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessHygieneEvidence {
    /// Active-turn checkpoints in repetition/turn order.
    pub checkpoints: Vec<ProcessHygieneCheckpoint>,
    /// One warm controller baseline per daemon repetition.
    pub warm_baselines: Vec<ProcessHygieneCheckpoint>,
    /// Per-invocation audits after every child exit.
    pub per_turn_audits: Vec<ProcessHygieneAudit>,
    /// Daemon audits after official session close.
    pub post_close_audits: Vec<ProcessHygieneAudit>,
    /// Daemon audits after official controller shutdown.
    pub shutdown_audits: Vec<ProcessHygieneAudit>,
    /// Calling-thread CPU consumed by row-44 discovery and sampling.
    pub sampler_collection_cpu_ns: u64,
    /// Wall time consumed by row-44 discovery and sampling.
    pub sampler_collection_wall_ns: u64,
    /// Row-44 sampler CPU spent while turns were active.
    pub active_sampler_collection_cpu_ns: u64,
    /// Total active-turn wall time covered by row-44 cadence sampling.
    pub sampled_turn_wall_ns: u64,
    /// Process-accounting warnings retained from platform samples.
    pub sampler_warnings: Vec<String>,
}

/// Topology-agnostic resource and performance headline values.
///
/// Every value here is derived after workload execution from AHRB's external
/// whole-tree samples, membership-discovery accounting, and the external turn
/// wall clocks that already enforce turn deadlines. Summary construction never
/// executes synchronously in the harness turn path.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ResourceSummary {
    /// Architecture topology under which resource values were measured.
    #[serde(default)]
    pub topology: String,
    /// Normative comparison guard for every resource value.
    #[serde(default)]
    pub comparison_scope: String,
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
    /// Nearest-rank median external turn wall clock.
    #[serde(default)]
    pub wall_per_turn_p50_ms: f64,
    /// Nearest-rank p95 external turn wall clock.
    #[serde(default)]
    pub wall_per_turn_p95_ms: f64,
    /// Maximum external turn wall clock.
    #[serde(default)]
    pub wall_per_turn_max_ms: f64,
    /// Median absolute deviation from the nearest-rank median.
    #[serde(default)]
    pub wall_per_turn_mad_ms: f64,
    /// MAD divided by p50.
    #[serde(default)]
    pub wall_per_turn_jitter_ratio: f64,
    /// Latency class derived from p95: L100 through L1000+.
    #[serde(default)]
    pub latency_class: String,
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
    /// Authoritative benchmark specification version.
    #[serde(default = "default_spec_version")]
    pub spec_version: u32,
    /// Deterministic run identifier.
    pub run_id: String,
    /// Reproducibility fingerprint.
    pub fingerprint: Fingerprint,
    /// Matrix results sorted by row.
    pub results: Vec<TestResult>,
    /// Badge when every topology-relative CORE gate passes.
    pub badge: Option<Badge>,
    /// Named non-resource automation diagnostics. Resource observations live
    /// exclusively in `resource_metrics` so topology labels cannot be dropped.
    pub metrics: BTreeMap<String, f64>,
    /// Row-keyed structured details that cannot live in the numeric metric map.
    #[serde(default)]
    pub details: BTreeMap<String, Value>,
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
    /// External per-turn boundary evidence.
    #[serde(default)]
    pub turns: Vec<TurnObservation>,
}

fn default_spec_version() -> u32 {
    1
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
    write_jsonl(&directory.join("turns.jsonl"), &report.turns)?;
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
    if !report.details.is_empty() {
        let _ = writeln!(output, "\n## Details\n");
        for (name, value) in &report.details {
            let rendered = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
            let _ = writeln!(output, "- `{name}`: `{rendered}`");
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
        "resource_summary peak_rss_mib={:.3} mean_rss_mib={:.3} median_rss_mib={:.3} cpu_total_s={:.3} cpu_per_turn_ms={:.3} wall_per_turn_ms={:.3} wall_per_turn_p50_ms={:.3} wall_per_turn_p95_ms={:.3} wall_per_turn_max_ms={:.3} wall_per_turn_mad_ms={:.3} wall_per_turn_jitter_ratio={:.3} latency_class={}",
        summary.peak_rss_mib,
        summary.mean_rss_mib,
        summary.median_rss_mib,
        summary.cpu_total_s,
        summary.cpu_per_turn_ms,
        summary.wall_per_turn_ms,
        summary.wall_per_turn_p50_ms,
        summary.wall_per_turn_p95_ms,
        summary.wall_per_turn_max_ms,
        summary.wall_per_turn_mad_ms,
        summary.wall_per_turn_jitter_ratio,
        summary.latency_class,
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
        topology: String::new(),
        comparison_scope: String::new(),
        peak_rss_mib: peak_bytes as f64 / MIB,
        mean_rss_mib: mean_bytes / MIB,
        median_rss_mib: median_bytes / MIB,
        cpu_total_s: cpu_ns as f64 / 1_000_000_000.0,
        cpu_per_turn_ms,
        wall_per_turn_ms,
        wall_per_turn_p50_ms: 0.0,
        wall_per_turn_p95_ms: 0.0,
        wall_per_turn_max_ms: 0.0,
        wall_per_turn_mad_ms: 0.0,
        wall_per_turn_jitter_ratio: 0.0,
        latency_class: String::new(),
        idle_rss_mib,
        parallel_beta_mib_per_agent,
        scaling_alpha,
        sampler_overhead_pct,
    }
}

/// Deterministic row-43 distribution and evidence-completeness decision.
#[derive(Clone, Debug, PartialEq)]
pub struct TurnLatencyEvaluation {
    /// Nearest-rank p50 in milliseconds.
    pub wall_per_turn_p50_ms: f64,
    /// Nearest-rank p95 in milliseconds.
    pub wall_per_turn_p95_ms: f64,
    /// Maximum in milliseconds.
    pub wall_per_turn_max_ms: f64,
    /// Median absolute deviation in milliseconds.
    pub wall_per_turn_mad_ms: f64,
    /// MAD divided by p50.
    pub wall_per_turn_jitter_ratio: f64,
    /// Normative latency class.
    pub latency_class: String,
    /// Whether every required external boundary and interval was present.
    pub measurement_complete: bool,
    /// Informational reference-envelope decision.
    pub reference_envelope_pass: bool,
    /// Deterministic measurement diagnostic when incomplete.
    pub measurement_error: Option<String>,
}

/// Evaluate external row-43 turn observations without interacting with the harness.
pub fn evaluate_turn_latency(
    observations: &[TurnObservation],
    expected_turns: u32,
    per_invocation: bool,
    turn_timeout_ms: u64,
) -> TurnLatencyEvaluation {
    let incomplete = |detail: String| TurnLatencyEvaluation {
        wall_per_turn_p50_ms: 0.0,
        wall_per_turn_p95_ms: 0.0,
        wall_per_turn_max_ms: 0.0,
        wall_per_turn_mad_ms: 0.0,
        wall_per_turn_jitter_ratio: 0.0,
        latency_class: "unavailable".to_owned(),
        measurement_complete: false,
        reference_envelope_pass: false,
        measurement_error: Some(detail),
    };
    if observations.len() != expected_turns as usize {
        return incomplete(format!(
            "expected {expected_turns} turn intervals, observed {}",
            observations.len()
        ));
    }
    let mut expected_index = 1_u32;
    let mut walls = Vec::with_capacity(observations.len());
    for observation in observations {
        if observation.turn_index != expected_index {
            return incomplete(format!(
                "turn index sequence expected {expected_index}, observed {}",
                observation.turn_index
            ));
        }
        expected_index = expected_index.saturating_add(1);
        let boundaries = if per_invocation {
            observation.launch_ns.zip(observation.exit_ns)
        } else {
            observation.submit_ns.zip(observation.terminal_ns)
        };
        let Some((start_ns, end_ns)) = boundaries else {
            return incomplete(format!(
                "turn {} lacks required {} boundaries",
                observation.turn_index,
                if per_invocation {
                    "launch/exit"
                } else {
                    "submit/terminal"
                }
            ));
        };
        let Some(expected_wall_ns) = end_ns.checked_sub(start_ns) else {
            return incomplete(format!(
                "turn {} external boundaries are reversed",
                observation.turn_index
            ));
        };
        let Some(wall_ns) = observation.turn_wall_ns else {
            return incomplete(format!(
                "turn {} lacks turn_wall_ns",
                observation.turn_index
            ));
        };
        if wall_ns != expected_wall_ns {
            return incomplete(format!(
                "turn {} wall interval disagrees with external boundaries",
                observation.turn_index
            ));
        }
        walls.push(wall_ns);
    }
    walls.sort_unstable();
    let p50_ns = nearest_rank_u64(&walls, 50);
    if p50_ns == 0 {
        return incomplete("turn latency p50 is zero; jitter is unavailable".to_owned());
    }
    let p95_ns = nearest_rank_u64(&walls, 95);
    let max_ns = walls.last().copied().unwrap_or(0);
    let timeout_ns = turn_timeout_ms.saturating_mul(1_000_000);
    if max_ns >= timeout_ns {
        return incomplete(format!(
            "maximum turn interval {max_ns} ns is not below timeout {timeout_ns} ns"
        ));
    }
    let mut absolute_deviations = walls
        .iter()
        .map(|value| value.abs_diff(p50_ns))
        .collect::<Vec<_>>();
    absolute_deviations.sort_unstable();
    let mad_ns = nearest_rank_u64(&absolute_deviations, 50);
    let p50_ms = p50_ns as f64 / 1_000_000.0;
    let p95_ms = p95_ns as f64 / 1_000_000.0;
    let max_ms = max_ns as f64 / 1_000_000.0;
    let mad_ms = mad_ns as f64 / 1_000_000.0;
    let jitter = mad_ns as f64 / p50_ns as f64;
    let latency_class = if p95_ms <= 100.0 {
        "L100"
    } else if p95_ms <= 250.0 {
        "L250"
    } else if p95_ms <= 500.0 {
        "L500"
    } else if p95_ms <= 1_000.0 {
        "L1000"
    } else {
        "L1000+"
    };
    TurnLatencyEvaluation {
        wall_per_turn_p50_ms: p50_ms,
        wall_per_turn_p95_ms: p95_ms,
        wall_per_turn_max_ms: max_ms,
        wall_per_turn_mad_ms: mad_ms,
        wall_per_turn_jitter_ratio: jitter,
        latency_class: latency_class.to_owned(),
        measurement_complete: true,
        reference_envelope_pass: p95_ms <= 1_000.0 && jitter <= 0.25,
        measurement_error: None,
    }
}

/// Complete row-44 aggregation and CORE oracle decision.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessHygieneEvaluation {
    /// Exact numeric `metrics` entries required by the v2 schema.
    pub metrics: BTreeMap<String, f64>,
    /// Exact structured `details.process-hygiene` value.
    pub details: Value,
    /// Whether every checkpoint, counter, and delayed audit was present.
    pub measurement_complete: bool,
    /// Whether every topology-specific residue and trend oracle passed.
    pub passed: bool,
    /// Deterministic diagnostic for incomplete evidence.
    pub measurement_error: Option<String>,
    /// Deterministic diagnostic for a measured CORE failure.
    pub failure_detail: Option<String>,
    /// Preserved sampler warnings for report evidence.
    pub sampler_warnings: Vec<String>,
    /// Calling-thread CPU consumed by the out-of-band collector.
    pub sampler_collection_cpu_ns: u64,
    /// Wall time consumed by the out-of-band collector.
    pub sampler_collection_wall_ns: u64,
    /// Active-turn sampler CPU divided by the cadence-covered turn wall time.
    pub sampler_overhead_pct: f64,
}

fn empty_process_hygiene_metrics() -> BTreeMap<String, f64> {
    [
        "process_hygiene.observed_processes_spawned_per_turn_p50",
        "process_hygiene.observed_processes_spawned_per_turn_max",
        "process_hygiene.observed_threads_created_per_turn_p50",
        "process_hygiene.observed_threads_created_per_turn_max",
        "process_hygiene.observed_fds_opened_per_turn_p50",
        "process_hygiene.observed_fds_opened_per_turn_max",
        "process_hygiene.peak_live_processes",
        "process_hygiene.peak_threads",
        "process_hygiene.peak_fds",
        "process_hygiene.residue_processes",
        "process_hygiene.residue_threads_delta",
        "process_hygiene.residue_fds_delta",
        "process_hygiene.unique_process_identities",
    ]
    .into_iter()
    .map(|name| (name.to_owned(), 0.0))
    .collect()
}

fn hygiene_process_map(
    processes: &[ProcessHygieneProcess],
) -> Option<BTreeMap<ProcIdentity, &ProcessHygieneProcess>> {
    let values = processes
        .iter()
        .map(|process| (process.identity, process))
        .collect::<BTreeMap<_, _>>();
    (values.len() == processes.len()).then_some(values)
}

fn hygiene_totals(processes: &[ProcessHygieneProcess]) -> Option<(u64, u64, u64)> {
    let mut threads = 0_u64;
    let mut fds = 0_u64;
    for process in processes {
        threads = threads.checked_add(process.thread_count?)?;
        fds = fds.checked_add(process.open_fds?)?;
    }
    Some((processes.len() as u64, threads, fds))
}

fn hygiene_counter_map(
    processes: &[ProcessHygieneProcess],
) -> Option<BTreeMap<ProcIdentity, (u64, u64)>> {
    let values = processes
        .iter()
        .map(|process| Some((process.identity, (process.thread_count?, process.open_fds?))))
        .collect::<Option<BTreeMap<_, _>>>()?;
    (values.len() == processes.len()).then_some(values)
}

fn ownership_label(ownership: &ProcOwnership) -> &'static str {
    match ownership {
        ProcOwnership::DeclaredRoot => "declared-root",
        ProcOwnership::Descendant => "descendant",
        ProcOwnership::CgroupMember => "cgroup-member",
        ProcOwnership::ProcessGroupMember => "process-group-member",
        ProcOwnership::Reparented => "reparented",
    }
}

/// Evaluate row-44 from already captured external checkpoints and residue audits.
pub fn evaluate_process_hygiene(
    evidence: &ProcessHygieneEvidence,
    expected_repetitions: u32,
    turns_per_repetition: u32,
    per_invocation: bool,
) -> ProcessHygieneEvaluation {
    let incomplete = |detail: String| ProcessHygieneEvaluation {
        metrics: empty_process_hygiene_metrics(),
        details: serde_json::json!({"residue_identities": []}),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(detail),
        failure_detail: None,
        sampler_warnings: evidence.sampler_warnings.clone(),
        sampler_collection_cpu_ns: evidence.sampler_collection_cpu_ns,
        sampler_collection_wall_ns: evidence.sampler_collection_wall_ns,
        sampler_overhead_pct: 0.0,
    };
    let expected_checkpoints = expected_repetitions.saturating_mul(turns_per_repetition) as usize;
    if evidence.checkpoints.len() != expected_checkpoints {
        return incomplete(format!(
            "expected {expected_checkpoints} active checkpoints, observed {}",
            evidence.checkpoints.len()
        ));
    }
    for repetition in 1..=expected_repetitions {
        let checkpoints = evidence
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        if checkpoints.len() != turns_per_repetition as usize
            || checkpoints
                .iter()
                .enumerate()
                .any(|(index, checkpoint)| checkpoint.turn_index != index as u32 + 1)
        {
            return incomplete(format!(
                "repetition {repetition} lacks the exact ordered 1..={turns_per_repetition} checkpoint sequence"
            ));
        }
    }
    const REQUIRED_CADENCE_NS: u64 = 10_000_000;
    let mut derived_active_cpu_ns = 0_u64;
    let mut derived_sampled_wall_ns = 0_u64;
    for checkpoint in &evidence.checkpoints {
        if checkpoint.required_cadence_ns != REQUIRED_CADENCE_NS {
            return incomplete(format!(
                "repetition {} turn {} row-44 cadence is {} ns, expected {REQUIRED_CADENCE_NS} ns",
                checkpoint.repetition, checkpoint.turn_index, checkpoint.required_cadence_ns
            ));
        }
        if checkpoint.sampled_wall_ns == 0 || checkpoint.cadence_samples.len() < 2 {
            return incomplete(format!(
                "repetition {} turn {} lacks two boundary samples spanning a nonzero active window",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        let times = checkpoint
            .cadence_samples
            .iter()
            .map(|sample| sample.elapsed_ns)
            .collect::<Vec<_>>();
        if times.windows(2).any(|pair| pair[1] <= pair[0]) {
            return incomplete(format!(
                "repetition {} turn {} cadence timestamps are not strictly increasing",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        if times.first().copied().unwrap_or(REQUIRED_CADENCE_NS) > REQUIRED_CADENCE_NS
            || times
                .last()
                .copied()
                .unwrap_or(0)
                .saturating_add(REQUIRED_CADENCE_NS)
                < checkpoint.sampled_wall_ns
        {
            return incomplete(format!(
                "repetition {} turn {} cadence samples do not cover the active boundaries",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        let (maximum_gap_ns, cadence_gaps, trustworthy) =
            crate::sampler::cadence_quality(&times, REQUIRED_CADENCE_NS);
        if !trustworthy {
            return incomplete(format!(
                "repetition {} turn {} has untrustworthy cadence coverage: {cadence_gaps} gap(s), maximum {maximum_gap_ns} ns",
                checkpoint.repetition, checkpoint.turn_index
            ));
        }
        for sample in &checkpoint.cadence_samples {
            if sample.collection_wall_ns == 0 {
                return incomplete(format!(
                    "repetition {} turn {} sampler collection wall time is absent",
                    checkpoint.repetition, checkpoint.turn_index
                ));
            }
            derived_active_cpu_ns = derived_active_cpu_ns.saturating_add(sample.collection_cpu_ns);
        }
        derived_sampled_wall_ns =
            derived_sampled_wall_ns.saturating_add(checkpoint.sampled_wall_ns);
    }
    if evidence.active_sampler_collection_cpu_ns != derived_active_cpu_ns
        || evidence.sampled_turn_wall_ns != derived_sampled_wall_ns
        || derived_sampled_wall_ns == 0
    {
        return incomplete(format!(
            "row-44 sampler accounting disagrees with cadence samples: cpu {} vs {derived_active_cpu_ns}, wall {} vs {derived_sampled_wall_ns}",
            evidence.active_sampler_collection_cpu_ns, evidence.sampled_turn_wall_ns
        ));
    }
    let sampler_overhead_pct =
        100.0 * derived_active_cpu_ns as f64 / derived_sampled_wall_ns as f64;
    if sampler_overhead_pct > 10.0 {
        return incomplete(format!(
            "sampler overload: {sampler_overhead_pct:.3}% row-44 active-turn sampler CPU"
        ));
    }
    let required_audits = if per_invocation {
        &evidence.per_turn_audits
    } else {
        if evidence.warm_baselines.len() != expected_repetitions as usize
            || evidence.post_close_audits.len() != expected_repetitions as usize
            || evidence.shutdown_audits.len() != expected_repetitions as usize
        {
            return incomplete(
                "daemon hygiene requires one warm baseline, post-close audit, and shutdown audit per repetition"
                    .to_owned(),
            );
        }
        &evidence.shutdown_audits
    };
    if per_invocation && required_audits.len() != expected_checkpoints {
        return incomplete(format!(
            "expected {expected_checkpoints} per-invocation residue audits, observed {}",
            required_audits.len()
        ));
    }
    if per_invocation {
        for repetition in 1..=expected_repetitions {
            let audits = evidence
                .per_turn_audits
                .iter()
                .filter(|audit| audit.repetition == repetition)
                .collect::<Vec<_>>();
            if audits.len() != turns_per_repetition as usize
                || audits
                    .iter()
                    .enumerate()
                    .any(|(index, audit)| audit.turn_index != Some(index as u32 + 1))
            {
                return incomplete(format!(
                    "repetition {repetition} lacks the exact ordered 1..={turns_per_repetition} post-exit audit sequence"
                ));
            }
        }
    } else {
        for repetition in 1..=expected_repetitions {
            if evidence.warm_baselines[repetition as usize - 1].repetition != repetition
                || evidence.post_close_audits[repetition as usize - 1].repetition != repetition
                || evidence.shutdown_audits[repetition as usize - 1].repetition != repetition
            {
                return incomplete(
                    "daemon hygiene audits are not in exact repetition order".to_owned(),
                );
            }
        }
    }
    let delayed_audits = evidence
        .per_turn_audits
        .iter()
        .chain(evidence.post_close_audits.iter())
        .chain(evidence.shutdown_audits.iter());
    if delayed_audits.clone().any(|audit| audit.waited_ms < 2_000) {
        return incomplete("a process-hygiene residue audit was shorter than 2,000 ms".to_owned());
    }
    let all_process_sets = evidence
        .checkpoints
        .iter()
        .flat_map(|checkpoint| {
            checkpoint
                .cadence_samples
                .iter()
                .map(|sample| sample.processes.as_slice())
        })
        .chain(
            evidence
                .warm_baselines
                .iter()
                .map(|checkpoint| checkpoint.processes.as_slice()),
        )
        .chain(
            evidence
                .per_turn_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .chain(
            evidence
                .post_close_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .chain(
            evidence
                .shutdown_audits
                .iter()
                .map(|audit| audit.processes.as_slice()),
        )
        .collect::<Vec<_>>();
    if all_process_sets.iter().any(|processes| {
        hygiene_process_map(processes).is_none() || hygiene_totals(processes).is_none()
    }) {
        return incomplete(
            "process-hygiene evidence has duplicate identities or incomplete thread/FD counters"
                .to_owned(),
        );
    }

    let mut spawned = Vec::with_capacity(expected_checkpoints);
    let mut threads_created = Vec::with_capacity(expected_checkpoints);
    let mut fds_opened = Vec::with_capacity(expected_checkpoints);
    let mut unique_identities = BTreeSet::new();
    let mut peak_processes = 0_u64;
    let mut peak_threads = 0_u64;
    let mut peak_fds = 0_u64;
    let mut failures = Vec::new();

    for repetition in 1..=expected_repetitions {
        let mut previous = if per_invocation {
            BTreeMap::new()
        } else {
            let Some(baseline) = evidence
                .warm_baselines
                .iter()
                .find(|baseline| baseline.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a warm baseline"));
            };
            let Some(processes) = hygiene_counter_map(&baseline.processes) else {
                return incomplete(format!(
                    "repetition {repetition} warm baseline repeats an identity or lacks counters"
                ));
            };
            for identity in processes.keys() {
                unique_identities.insert(*identity);
            }
            processes
        };
        let checkpoints = evidence
            .checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.repetition == repetition)
            .collect::<Vec<_>>();
        let mut ordered_totals = Vec::with_capacity(checkpoints.len());
        for checkpoint in checkpoints {
            let mut turn_threads = 0_u64;
            let mut turn_fds = 0_u64;
            let mut new_processes = 0_u64;
            let mut turn_peak = (0_u64, 0_u64, 0_u64);
            for sample in &checkpoint.cadence_samples {
                let Some(current) = hygiene_counter_map(&sample.processes) else {
                    return incomplete(format!(
                        "repetition {repetition} turn {} cadence sample repeats an identity or lacks counters",
                        checkpoint.turn_index
                    ));
                };
                for (identity, (current_threads, current_fds)) in &current {
                    let first_observation = unique_identities.insert(*identity);
                    if let Some((prior_threads, prior_fds)) = previous.get(identity) {
                        turn_threads = turn_threads
                            .saturating_add(current_threads.saturating_sub(*prior_threads));
                        turn_fds = turn_fds.saturating_add(current_fds.saturating_sub(*prior_fds));
                    } else if first_observation {
                        new_processes = new_processes.saturating_add(1);
                        turn_threads = turn_threads.saturating_add(*current_threads);
                        turn_fds = turn_fds.saturating_add(*current_fds);
                    }
                }
                let Some(totals) = hygiene_totals(&sample.processes) else {
                    return incomplete("active cadence counters are incomplete".to_owned());
                };
                peak_processes = peak_processes.max(totals.0);
                peak_threads = peak_threads.max(totals.1);
                peak_fds = peak_fds.max(totals.2);
                turn_peak.0 = turn_peak.0.max(totals.0);
                turn_peak.1 = turn_peak.1.max(totals.1);
                turn_peak.2 = turn_peak.2.max(totals.2);
                previous = current;
            }
            spawned.push(new_processes);
            threads_created.push(turn_threads);
            fds_opened.push(turn_fds);
            ordered_totals.push(turn_peak);
        }
        let threshold = ordered_totals.len() / 2;
        if threshold > 0 {
            for (label, dimension) in [("process", 0_usize), ("thread", 1), ("FD", 2)] {
                let increases = ordered_totals
                    .windows(2)
                    .filter(|pair| match dimension {
                        0 => pair[1].0 > pair[0].0,
                        1 => pair[1].1 > pair[0].1,
                        _ => pair[1].2 > pair[0].2,
                    })
                    .count();
                if increases >= threshold {
                    failures.push(format!(
                        "repetition {repetition} {label} live count increased in {increases} adjacent pairs (failure threshold {threshold})"
                    ));
                }
            }
        }
    }

    let mut residue_processes = 0_u64;
    let mut residue_threads_delta = 0_u64;
    let mut residue_fds_delta = 0_u64;
    let mut residue_identities = BTreeMap::<ProcIdentity, &ProcessHygieneProcess>::new();
    if per_invocation {
        for audit in &evidence.per_turn_audits {
            let Some((processes, threads, fds)) = hygiene_totals(&audit.processes) else {
                return incomplete("per-invocation residue counters are incomplete".to_owned());
            };
            residue_processes = residue_processes.max(processes);
            residue_threads_delta = residue_threads_delta.max(threads);
            residue_fds_delta = residue_fds_delta.max(fds);
            for process in &audit.processes {
                residue_identities
                    .entry(process.identity)
                    .or_insert(process);
            }
        }
        if residue_processes != 0 {
            failures.push(format!(
                "per-invocation residue remained after a 2,000 ms child-exit audit: {residue_processes} process(es)"
            ));
        }
    } else {
        for repetition in 1..=expected_repetitions {
            let Some(baseline) = evidence
                .warm_baselines
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a warm baseline"));
            };
            let Some(post_close) = evidence
                .post_close_audits
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a post-close audit"));
            };
            let Some(shutdown) = evidence
                .shutdown_audits
                .iter()
                .find(|audit| audit.repetition == repetition)
            else {
                return incomplete(format!("repetition {repetition} lacks a shutdown audit"));
            };
            let Some(baseline_map) = hygiene_process_map(&baseline.processes) else {
                return incomplete("warm baseline repeats an identity".to_owned());
            };
            let new_post_close = post_close
                .processes
                .iter()
                .filter(|process| !baseline_map.contains_key(&process.identity))
                .collect::<Vec<_>>();
            let Some((_, baseline_threads, baseline_fds)) = hygiene_totals(&baseline.processes)
            else {
                return incomplete("warm baseline counters are incomplete".to_owned());
            };
            let Some((_, post_threads, post_fds)) = hygiene_totals(&post_close.processes) else {
                return incomplete("post-close counters are incomplete".to_owned());
            };
            let Some((shutdown_processes, shutdown_threads, shutdown_fds)) =
                hygiene_totals(&shutdown.processes)
            else {
                return incomplete("shutdown counters are incomplete".to_owned());
            };
            let thread_delta = post_threads.saturating_sub(baseline_threads);
            let fd_delta = post_fds.saturating_sub(baseline_fds);
            residue_processes = residue_processes
                .max(new_post_close.len() as u64)
                .max(shutdown_processes);
            residue_threads_delta = residue_threads_delta
                .max(thread_delta)
                .max(shutdown_threads);
            residue_fds_delta = residue_fds_delta.max(fd_delta).max(shutdown_fds);
            for process in new_post_close.into_iter().chain(shutdown.processes.iter()) {
                residue_identities
                    .entry(process.identity)
                    .or_insert(process);
            }
            if post_close
                .processes
                .iter()
                .any(|process| !baseline_map.contains_key(&process.identity))
            {
                failures.push(format!(
                    "repetition {repetition} post-close state contains a new process identity"
                ));
            }
            if post_threads > baseline_threads.saturating_add(2) {
                failures.push(format!(
                    "repetition {repetition} post-close threads {post_threads} exceed warm baseline {baseline_threads} + 2"
                ));
            }
            if post_fds > baseline_fds.saturating_add(4) {
                failures.push(format!(
                    "repetition {repetition} post-close FDs {post_fds} exceed warm baseline {baseline_fds} + 4"
                ));
            }
            if shutdown_processes != 0 {
                failures.push(format!(
                    "repetition {repetition} shutdown left {shutdown_processes} owned process(es) after 2,000 ms"
                ));
            }
        }
    }

    for processes in &all_process_sets {
        for process in *processes {
            unique_identities.insert(process.identity);
        }
    }
    spawned.sort_unstable();
    threads_created.sort_unstable();
    fds_opened.sort_unstable();
    let max_or_zero = |values: &[u64]| values.iter().copied().max().unwrap_or(0) as f64;
    let mut metrics = empty_process_hygiene_metrics();
    metrics.insert(
        "process_hygiene.observed_processes_spawned_per_turn_p50".to_owned(),
        nearest_rank_u64(&spawned, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_processes_spawned_per_turn_max".to_owned(),
        max_or_zero(&spawned),
    );
    metrics.insert(
        "process_hygiene.observed_threads_created_per_turn_p50".to_owned(),
        nearest_rank_u64(&threads_created, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_threads_created_per_turn_max".to_owned(),
        max_or_zero(&threads_created),
    );
    metrics.insert(
        "process_hygiene.observed_fds_opened_per_turn_p50".to_owned(),
        nearest_rank_u64(&fds_opened, 50) as f64,
    );
    metrics.insert(
        "process_hygiene.observed_fds_opened_per_turn_max".to_owned(),
        max_or_zero(&fds_opened),
    );
    metrics.insert(
        "process_hygiene.peak_live_processes".to_owned(),
        peak_processes as f64,
    );
    metrics.insert(
        "process_hygiene.peak_threads".to_owned(),
        peak_threads as f64,
    );
    metrics.insert("process_hygiene.peak_fds".to_owned(), peak_fds as f64);
    metrics.insert(
        "process_hygiene.residue_processes".to_owned(),
        residue_processes as f64,
    );
    metrics.insert(
        "process_hygiene.residue_threads_delta".to_owned(),
        residue_threads_delta as f64,
    );
    metrics.insert(
        "process_hygiene.residue_fds_delta".to_owned(),
        residue_fds_delta as f64,
    );
    metrics.insert(
        "process_hygiene.unique_process_identities".to_owned(),
        unique_identities.len() as f64,
    );
    let residue = residue_identities
        .into_values()
        .map(|process| {
            serde_json::json!({
                "pid": process.identity.pid,
                "start_time": process.identity.start_time,
                "command": process.command,
                "ownership": ownership_label(&process.ownership),
            })
        })
        .collect::<Vec<_>>();
    let failure_detail = (!failures.is_empty()).then(|| failures.join("; "));
    ProcessHygieneEvaluation {
        metrics,
        details: serde_json::json!({"residue_identities": residue}),
        measurement_complete: true,
        passed: failure_detail.is_none(),
        measurement_error: None,
        failure_detail,
        sampler_warnings: evidence.sampler_warnings.clone(),
        sampler_collection_cpu_ns: evidence.sampler_collection_cpu_ns,
        sampler_collection_wall_ns: evidence.sampler_collection_wall_ns,
        sampler_overhead_pct,
    }
}

fn nearest_rank_u64(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.max(1).min(sorted.len()) - 1]
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
