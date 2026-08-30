//! Human-readable, machine-readable, and raw-evidence reports.

use crate::Result;
use crate::evaluate::{Badge, TestOutcome, TestResult, badge_label};
use crate::process::{ProcessSample, Sample};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

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

/// Complete benchmark report and embedded evidence.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Report {
    /// Report schema version.
    pub schema: u32,
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
    /// Resource metrics with topology labels and within-topology comparison scope.
    #[serde(default)]
    pub resource_metrics: BTreeMap<String, TopologyMetric>,
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
