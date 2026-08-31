mod common;

use ahrb::evaluate::TestOutcome;
use ahrb::report::Report;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn run_directory(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-mock-exec-{label}-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn run_certification(manifest: &Path, output: &Path) -> (ExitStatus, Report, String) {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(manifest)
        .arg("--output")
        .arg(output)
        .arg("--profile")
        .arg("quick")
        .arg("--junit")
        .output()
        .expect("execute AHRB against the per-invocation reference harness");
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &result,
        &report_path,
        "AHRB mock-exec subprocess did not produce a report",
    );
    let report: Report = serde_json::from_slice(&report_bytes).expect("parse mock-exec report");
    let junit = std::fs::read_to_string(output.join("junit.xml")).expect("read mock-exec JUnit");
    (result.status, report, junit)
}

fn run_profile(report: &Report) -> PathBuf {
    let profile = PathBuf::from(&report.profile_path);
    assert!(profile.is_dir(), "certification profile must exist");
    profile
}

fn provider_value(provider: &str, key: &str) -> String {
    let prefix = format!("{key} = '");
    provider
        .lines()
        .find_map(|line| line.strip_prefix(&prefix)?.strip_suffix('\''))
        .unwrap_or_else(|| panic!("provider config omitted {key}"))
        .to_owned()
}

fn exec_template_evidence(event: &Value) -> Option<&Value> {
    (event.get("event").and_then(Value::as_str) == Some("turn-accepted"))
        .then(|| event.pointer("/payload/exec_template"))
        .flatten()
}

fn assert_rendered_bindings(evidence: &Value, base_url: &str, credential_fingerprint: &str) {
    assert_eq!(
        evidence.get("base_url").and_then(Value::as_str),
        Some(base_url)
    );
    assert_eq!(
        evidence
            .get("credential_fingerprint")
            .and_then(Value::as_str),
        Some(credential_fingerprint)
    );
    assert_eq!(
        evidence
            .get("base_url_matches_environment")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        evidence
            .get("credential_matches_environment")
            .and_then(Value::as_bool),
        Some(true)
    );
}

fn assert_exec_template_propagation(report: &Report) {
    let profile = run_profile(report);
    let provider = std::fs::read_to_string(profile.join("config/provider.toml"))
        .expect("read generated provider config");
    let base_url = provider_value(&provider, "base_url");
    let credential = provider_value(&provider, "credential");
    let credential_fingerprint = format!("{:x}", Sha256::digest(credential.as_bytes()));

    let rendered = report
        .events
        .iter()
        .filter_map(exec_template_evidence)
        .collect::<Vec<_>>();
    let accepted_turns = report
        .events
        .iter()
        .filter(|event| event.get("event").and_then(Value::as_str) == Some("turn-accepted"))
        .count();
    assert!(
        !rendered.is_empty(),
        "main exec turns recorded no rendered bindings"
    );
    assert_eq!(
        rendered.len(),
        accepted_turns,
        "every accepted main exec turn must record rendered bindings"
    );
    for evidence in &rendered {
        assert_rendered_bindings(evidence, &base_url, &credential_fingerprint);
    }

    let mut turns_by_session = BTreeMap::<&str, usize>::new();
    for event in report
        .events
        .iter()
        .filter(|event| exec_template_evidence(event).is_some())
    {
        let session_id = event
            .get("session_id")
            .and_then(Value::as_str)
            .expect("exec binding event has session ID");
        *turns_by_session.entry(session_id).or_default() += 1;
    }
    assert!(
        turns_by_session.values().any(|turns| *turns >= 2),
        "no session proved both initial-command and resume-command rendering"
    );

    let mut resource_bindings = Vec::new();
    let mut resource_accepted_turns = 0_usize;
    for repetition in std::fs::read_dir(&profile)
        .expect("read run profile")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("pr"))
        })
    {
        let sessions = repetition.join("state/sessions");
        for session in std::fs::read_dir(sessions)
            .expect("read per-invocation resource sessions")
            .filter_map(std::result::Result::ok)
        {
            let journal = std::fs::read_to_string(session.path().join("journal.jsonl"))
                .expect("read per-invocation resource journal");
            for line in journal.lines() {
                let event: Value =
                    serde_json::from_str(line).expect("parse resource journal event");
                if event.get("event").and_then(Value::as_str) == Some("turn-accepted") {
                    resource_accepted_turns = resource_accepted_turns.saturating_add(1);
                }
                if let Some(evidence) = exec_template_evidence(&event) {
                    resource_bindings.push(evidence.clone());
                }
            }
        }
    }
    assert!(
        !resource_bindings.is_empty(),
        "resource collector recorded no rendered bindings"
    );
    assert_eq!(
        resource_bindings.len(),
        resource_accepted_turns,
        "every accepted resource-collector turn must record rendered bindings"
    );
    for evidence in &resource_bindings {
        assert_rendered_bindings(evidence, &base_url, &credential_fingerprint);
    }

    let serialized_report = serde_json::to_string(report).expect("serialize report for redaction");
    assert!(
        !serialized_report.contains(&credential),
        "raw rendered credential leaked into report evidence"
    );
}

#[test]
fn per_invocation_reference_certifies_and_core_underdeclaration_suppresses_badge() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let reference = repository.join("adapters/mock-exec/manifest.toml");
    let output = run_directory("reference");
    let (status, report, junit) = run_certification(&reference, &output);

    assert_eq!(
        status.code(),
        Some(0),
        "reference per-invocation certification must exit zero"
    );
    assert_eq!(report.results.len(), 41);
    let pass_count = report
        .results
        .iter()
        .filter(|result| matches!(result.outcome, TestOutcome::Pass))
        .count();
    let unsupported_rows = report
        .results
        .iter()
        .filter_map(|result| {
            matches!(result.outcome, TestOutcome::Unsupported(_)).then_some(result.row)
        })
        .collect::<Vec<_>>();
    assert_eq!(pass_count, 35);
    assert_eq!(unsupported_rows, vec![4, 18, 31, 32, 33, 39]);
    assert!(
        report.results.iter().all(|result| {
            !matches!(result.outcome, TestOutcome::Fail(_) | TestOutcome::Error(_))
        })
    );
    let badge = report.badge.as_ref().expect("reduced-facet badge");
    assert_eq!(badge.topology, "client-process-fanout");
    assert_eq!(badge.parallel_width, 4);
    assert_eq!(badge.facets, vec!["replay", "crash", "resume"]);
    assert_eq!(badge.comparison_scope, "within-topology-only");
    assert!(report.resource_summary.peak_rss_mib > 0.0);
    assert!(report.resource_summary.mean_rss_mib > 0.0);
    assert!(report.resource_summary.wall_per_turn_ms > 0.0);
    assert!(report.resource_summary.idle_rss_mib.is_none());
    assert!(
        report
            .resource_summary
            .parallel_beta_mib_per_agent
            .is_some()
    );
    assert!(report.resource_summary.scaling_alpha.is_some());
    assert!(report.resource_summary.sampler_overhead_pct >= 0.0);
    let sampled_peak = report
        .samples
        .iter()
        .map(|sample| {
            #[cfg(target_os = "macos")]
            {
                sample.footprint_bytes.unwrap_or(sample.rss_bytes)
            }
            #[cfg(target_os = "linux")]
            {
                sample.pss_bytes.unwrap_or(sample.rss_bytes)
            }
        })
        .max()
        .unwrap_or(0) as f64
        / (1024.0 * 1024.0);
    assert!((report.resource_summary.peak_rss_mib - sampled_peak).abs() <= 1e-9);
    let sampled_cpu_s = report
        .samples
        .first()
        .zip(report.samples.last())
        .map_or(0, |(first, last)| last.cpu_ns.saturating_sub(first.cpu_ns))
        as f64
        / 1_000_000_000.0;
    assert!((report.resource_summary.cpu_total_s - sampled_cpu_s).abs() <= 1e-9);
    assert!(junit.contains("failures=\"0\""));
    assert!(junit.contains("skipped=\"6\""));
    assert_exec_template_propagation(&report);
    std::fs::remove_dir_all(&report.profile_path).expect("remove reference mock-exec profile");
    std::fs::remove_dir_all(&output).expect("remove reference mock-exec output");

    let variant_root = run_directory("without-durable-journal");
    std::fs::create_dir_all(&variant_root).expect("create variant root");
    let source = std::fs::read_to_string(&reference).expect("read reference manifest");
    let changed = source
        .lines()
        .filter(|line| !line.starts_with("durable_journal = "))
        .collect::<Vec<_>>()
        .join("\n");
    assert_ne!(source, changed, "durable-journal declaration was not found");
    let variant = variant_root.join("manifest.toml");
    std::fs::write(&variant, changed).expect("write core-underdeclared manifest");
    let variant_output = variant_root.join("output");
    let (status, report, junit) = run_certification(&variant, &variant_output);

    assert_eq!(
        status.code(),
        Some(0),
        "core UNSUPPORTED is nonfatal when no FAIL/ERROR occurred"
    );
    assert!(report.badge.is_none(), "core UNSUPPORTED must block badge");
    assert!(matches!(
        report
            .results
            .iter()
            .find(|result| result.row == 40)
            .map(|result| &result.outcome),
        Some(TestOutcome::Unsupported(_))
    ));
    assert!(
        report.results.iter().all(|result| {
            !matches!(result.outcome, TestOutcome::Fail(_) | TestOutcome::Error(_))
        })
    );
    assert!(junit.contains("failures=\"0\""));
    assert!(junit.contains("skipped=\"7\""));
    std::fs::remove_dir_all(&report.profile_path).expect("remove variant mock-exec profile");
    std::fs::remove_dir_all(&variant_root).expect("remove variant mock-exec output");
}
