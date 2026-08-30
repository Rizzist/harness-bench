//! Human-readable, machine-readable, and raw-evidence reports.

use crate::Result;
use crate::evaluate::{Badge, TestOutcome, TestResult, badge_label};
use crate::process::{ProcessInfo, Sample};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

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
    /// Badge when every mandatory gate passes.
    pub badge: Option<Badge>,
    /// Named numeric resource metrics.
    pub metrics: BTreeMap<String, f64>,
    /// Raw resource samples.
    pub samples: Vec<Sample>,
    /// Raw process observations.
    pub processes: Vec<ProcessInfo>,
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
    if !report.metrics.is_empty() {
        let _ = writeln!(output, "\n## Resource metrics\n");
        for (name, value) in &report.metrics {
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
        .filter(|result| !matches!(result.outcome, TestOutcome::Pass))
        .count();
    let mut output = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"ahrb\" tests=\"{}\" failures=\"{}\">\n",
        report.results.len(),
        failures
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
        if !matches!(result.outcome, TestOutcome::Pass) {
            let _ = writeln!(
                output,
                "    <failure message=\"{}\" />",
                xml_escape(outcome_label(&result.outcome))
            );
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
