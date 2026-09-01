//! Deterministic comparisons of durable benchmark report occurrences.

use crate::evaluate::{TestOutcome, TestResult};
use crate::results::{IndexEntry, load_indexed_report_value, read_index};
use crate::scenarios::RequirementKind;
use crate::{AhrbError, Result};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Parse selectors, print the canonical diff document, and return its status.
pub fn execute(args: &[String]) -> Result<i32> {
    let entries = read_index()?;
    ensure_unique_run_keys(&entries)?;
    let (left, right) = if args.first().is_some_and(|value| value == "--latest") {
        if args.len() != 2 {
            return Err(AhrbError::Usage(
                "hbench diff --latest requires exactly one harness".to_owned(),
            ));
        }
        resolve_latest_pair(&entries, &args[1])?
    } else {
        if args.len() != 2 {
            return Err(AhrbError::Usage(
                "hbench diff requires exactly two harness selectors".to_owned(),
            ));
        }
        (
            resolve_operand(&entries, &args[0])?,
            resolve_operand(&entries, &args[1])?,
        )
    };
    let left_report = load_supported_report(&left)?;
    let right_report = load_supported_report(&right)?;
    let (document, gating_regression) = build_document(left, right, &left_report, &right_report)?;
    println!("{}", serde_json::to_string_pretty(&document)?);
    Ok(i32::from(gating_regression))
}

fn ensure_unique_run_keys(entries: &[IndexEntry]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if !seen.insert(&entry.run_key) {
            return Err(AhrbError::Protocol(format!(
                "ambiguous duplicate index run key {:?}",
                entry.run_key
            )));
        }
    }
    Ok(())
}

fn resolve_operand(entries: &[IndexEntry], operand: &str) -> Result<IndexEntry> {
    let (harness, selector) = match operand.rsplit_once('@') {
        Some((harness, selector)) if !harness.is_empty() && !selector.is_empty() => {
            (harness, Some(selector))
        }
        Some(_) => {
            return Err(AhrbError::Usage(format!(
                "invalid diff selector {operand:?}"
            )));
        }
        None if !operand.is_empty() => (operand, None),
        None => {
            return Err(AhrbError::Usage(
                "diff harness selector cannot be empty".to_owned(),
            ));
        }
    };
    let harness_entries = entries
        .iter()
        .filter(|entry| entry.harness == harness)
        .collect::<Vec<_>>();
    if harness_entries.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "no indexed runs resolve harness {harness:?}"
        )));
    }
    let Some(selector) = selector else {
        return newest(harness_entries).cloned();
    };
    let by_key = harness_entries
        .iter()
        .copied()
        .filter(|entry| entry.run_key == selector)
        .collect::<Vec<_>>();
    match by_key.as_slice() {
        [entry] => return Ok((*entry).clone()),
        [] => {}
        _ => {
            return Err(AhrbError::Protocol(format!(
                "ambiguous run key {selector:?} for harness {harness:?}"
            )));
        }
    }
    let by_version = harness_entries
        .into_iter()
        .filter(|entry| entry.harness_version == selector)
        .collect::<Vec<_>>();
    if by_version.is_empty() {
        return Err(AhrbError::Protocol(format!(
            "selector {selector:?} resolves neither a run key nor version for harness {harness:?}"
        )));
    }
    newest(by_version).cloned()
}

fn newest(entries: Vec<&IndexEntry>) -> Result<&IndexEntry> {
    let mut latest: Option<(u64, &IndexEntry)> = None;
    for entry in entries {
        let completed_at = crate::results::utc_timestamp_seconds(&entry.completed_at)?;
        let replace = latest.as_ref().is_none_or(|(latest_at, latest_entry)| {
            (completed_at, &entry.run_key) > (*latest_at, &latest_entry.run_key)
        });
        if replace {
            latest = Some((completed_at, entry));
        }
    }
    latest
        .map(|(_, entry)| entry)
        .ok_or_else(|| AhrbError::Protocol("no indexed run matched selector".to_owned()))
}

fn resolve_latest_pair(entries: &[IndexEntry], harness: &str) -> Result<(IndexEntry, IndexEntry)> {
    if harness.is_empty() {
        return Err(AhrbError::Usage(
            "hbench diff --latest harness cannot be empty".to_owned(),
        ));
    }
    let matching = entries
        .iter()
        .filter(|entry| entry.harness == harness)
        .collect::<Vec<_>>();
    let right = newest(matching.clone())?;
    let right_completed_at = crate::results::utc_timestamp_seconds(&right.completed_at)?;
    let mut earlier = Vec::new();
    for entry in matching {
        let entry_completed_at = crate::results::utc_timestamp_seconds(&entry.completed_at)?;
        if entry.run_key != right.run_key
            && entry.os == right.os
            && entry.topology == right.topology
            && entry.profile == right.profile
            && entry.report_schema == right.report_schema
            && (entry_completed_at, &entry.run_key) < (right_completed_at, &right.run_key)
        {
            earlier.push(entry);
        }
    }
    let left = newest(earlier).map_err(|_| {
        AhrbError::Protocol(format!(
            "fewer than two compatible indexed runs for harness {harness:?}"
        ))
    })?;
    Ok((left.clone(), right.clone()))
}

struct LoadedReport {
    parsed: crate::report::Report,
    raw: serde_json::Value,
}

fn load_supported_report(entry: &IndexEntry) -> Result<LoadedReport> {
    if !matches!(entry.report_schema, 2 | 3) {
        return Err(AhrbError::Protocol(format!(
            "unsupported report schema {} for run {}",
            entry.report_schema, entry.run_key
        )));
    }
    let raw = load_indexed_report_value(entry)?;
    let parsed = serde_json::from_value(raw.clone()).map_err(|error| {
        AhrbError::Protocol(format!(
            "could not deserialize indexed report {}: {error}",
            entry.report_path
        ))
    })?;
    Ok(LoadedReport { parsed, raw })
}

#[derive(Debug, Serialize)]
struct DiffDocument {
    schema: u32,
    left: IndexEntry,
    right: IndexEntry,
    comparison_scope: String,
    rows: Vec<RowDiff>,
    resource_summary_deltas: BTreeMap<String, ResourceDelta>,
}

#[derive(Debug, Serialize)]
struct RowDiff {
    row: u8,
    id: String,
    badge_impact: String,
    before: Option<String>,
    after: Option<String>,
    change: String,
}

#[derive(Debug, Serialize)]
struct ResourceDelta {
    before: Option<f64>,
    after: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_pct: Option<Option<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    change: Option<String>,
}

fn build_document(
    left: IndexEntry,
    right: IndexEntry,
    left_report: &LoadedReport,
    right_report: &LoadedReport,
) -> Result<(DiffDocument, bool)> {
    let (rows, gating_regression) =
        compare_rows(&left_report.parsed.results, &right_report.parsed.results)?;
    let comparable = comparable_scope(&left, &right);
    let resource_summary_deltas =
        compare_resources(&left_report.raw, &right_report.raw, comparable);
    Ok((
        DiffDocument {
            schema: 1,
            left,
            right,
            comparison_scope: "within-topology-only".to_owned(),
            rows,
            resource_summary_deltas,
        },
        gating_regression,
    ))
}

fn compare_rows(left: &[TestResult], right: &[TestResult]) -> Result<(Vec<RowDiff>, bool)> {
    let left = results_by_id(left)?;
    let right = results_by_id(right)?;
    let ids = left
        .keys()
        .chain(right.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::with_capacity(ids.len());
    let mut gating_regression = false;
    for id in ids {
        let before = left.get(&id).copied();
        let after = right.get(&id).copied();
        let current = after.or(before).ok_or_else(|| {
            AhrbError::Protocol(format!("row {id:?} disappeared while joining diff"))
        })?;
        let requirement = requirement_for(current);
        let (change, row_regression) = row_change(requirement, before, after);
        gating_regression |= row_regression;
        rows.push(RowDiff {
            row: current.row,
            id,
            badge_impact: requirement_label(requirement).to_owned(),
            before: before.map(|result| outcome_label(&result.outcome).to_owned()),
            after: after.map(|result| outcome_label(&result.outcome).to_owned()),
            change: change.to_owned(),
        });
    }
    rows.sort_by(|left, right| (left.row, &left.id).cmp(&(right.row, &right.id)));
    Ok((rows, gating_regression))
}

fn results_by_id(results: &[TestResult]) -> Result<BTreeMap<String, &TestResult>> {
    let mut by_id = BTreeMap::new();
    for result in results {
        if by_id.insert(result.id.clone(), result).is_some() {
            return Err(AhrbError::Protocol(format!(
                "report contains duplicate stable row ID {:?}",
                result.id
            )));
        }
    }
    Ok(by_id)
}

fn requirement_for(result: &TestResult) -> RequirementKind {
    crate::scenarios::all()
        .iter()
        .find(|definition| definition.id == result.id)
        .map_or_else(
            || match result.metadata.requirement.as_str() {
                "informational" => RequirementKind::Informational,
                "optional-facet" => RequirementKind::OptionalFacet {
                    capability: "legacy-unknown",
                },
                _ => RequirementKind::Core,
            },
            |definition| definition.requirement(),
        )
}

fn requirement_label(requirement: RequirementKind) -> &'static str {
    match requirement {
        RequirementKind::Core => "core",
        RequirementKind::OptionalFacet { .. } => "optional-facet",
        RequirementKind::Informational => "informational",
    }
}

fn row_change(
    requirement: RequirementKind,
    before: Option<&TestResult>,
    after: Option<&TestResult>,
) -> (&'static str, bool) {
    if before.is_none() {
        return ("added", false);
    }
    if after.is_none() {
        let regression = !matches!(requirement, RequirementKind::Informational)
            && before.is_some_and(|result| matches!(result.outcome, TestOutcome::Pass));
        return ("removed", regression);
    }
    let Some(before) = before else {
        return ("added", false);
    };
    let Some(after) = after else {
        return ("removed", false);
    };
    let before_class = outcome_label(&before.outcome);
    let after_class = outcome_label(&after.outcome);
    if matches!(requirement, RequirementKind::OptionalFacet { .. }) {
        let before_declared = optional_capability_declared(before);
        let after_declared = optional_capability_declared(after);
        if before_declared == Some(false) && after_declared == Some(true) {
            return if after_class == "PASS" {
                ("facet-added", false)
            } else {
                ("facet-added-nonpass", false)
            };
        }
        if before_declared == Some(true)
            && after_declared == Some(false)
            && after_class == "UNSUPPORTED"
        {
            return ("facet-removed", before_class == "PASS");
        }
    }
    if before_class == after_class {
        return ("unchanged", false);
    }
    if matches!(requirement, RequirementKind::Informational) {
        return informational_change(before_class, after_class);
    }
    if matches!(requirement, RequirementKind::OptionalFacet { .. }) {
        if before_class == "UNSUPPORTED"
            && optional_capability_declared(before) == Some(false)
            && optional_capability_declared(after) == Some(true)
            && after_class != "PASS"
        {
            return ("facet-added-nonpass", false);
        }
        if before_class == "UNSUPPORTED" && after_class == "PASS" {
            return if optional_capability_declared(before) == Some(false) {
                ("facet-added", false)
            } else {
                ("improvement", false)
            };
        }
        if after_class == "UNSUPPORTED" {
            if optional_capability_declared(after) == Some(false) {
                return ("facet-removed", before_class == "PASS");
            }
            if before_class == "PASS" {
                return ("regression", true);
            }
            return ("changed-nonpass", false);
        }
    }
    if before_class == "PASS" {
        return ("regression", true);
    }
    if after_class == "PASS" {
        return ("improvement", false);
    }
    ("changed-nonpass", false)
}

fn optional_capability_declared(result: &TestResult) -> Option<bool> {
    result.metadata.capability_declared
}

fn comparable_scope(left: &IndexEntry, right: &IndexEntry) -> bool {
    known_scope_value(&left.os)
        && known_scope_value(&left.topology)
        && known_scope_value(&left.profile)
        && known_scope_value(&right.os)
        && known_scope_value(&right.topology)
        && known_scope_value(&right.profile)
        && left.os == right.os
        && left.topology == right.topology
        && left.profile == right.profile
}

fn known_scope_value(value: &str) -> bool {
    !value.is_empty() && value != "unknown"
}

fn informational_change(before: &str, after: &str) -> (&'static str, bool) {
    match (before, after) {
        ("PASS", "FAIL") => ("informational-regression", false),
        ("FAIL", "PASS") => ("informational-improvement", false),
        ("PASS" | "FAIL", "ERROR" | "ABSENT") => ("evidence-regression", false),
        ("ERROR" | "ABSENT", "PASS" | "FAIL") => ("evidence-improvement", false),
        ("PASS" | "FAIL", "UNSUPPORTED") => ("evidence-regression", false),
        ("UNSUPPORTED", "PASS" | "FAIL") => ("evidence-improvement", false),
        ("ERROR", "ABSENT") | ("ABSENT", "ERROR") => ("changed-nonpass", false),
        _ => ("changed-nonpass", false),
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

const RESOURCE_FIELDS: &[&str] = &[
    "cpu_per_turn_ms",
    "cpu_per_turn_p50_ms",
    "cpu_per_turn_p95_ms",
    "cpu_total_s",
    "disk_write_bytes_per_turn_max",
    "disk_write_bytes_per_turn_p50",
    "disk_write_bytes_per_turn_p95",
    "disk_write_growth_slope_bytes_per_turn2",
    "fairness_latency_cv",
    "fairness_latency_max_min_ratio",
    "fairness_latency_spread_ms",
    "fairness_starved_agents",
    "fanout_cliff_n_rss",
    "fanout_cliff_n_wall",
    "fanout_global_rss_alpha",
    "fanout_max_local_rss_alpha",
    "fanout_max_local_wall_alpha",
    "fanout_max_measured_n",
    "idle_rss_mib",
    "latency_last_first_decile_ratio",
    "latency_slope_ms_per_100_turns",
    "large_tool_output_peak_rss_delta_mib",
    "log_growth_bytes_per_turn",
    "mean_rss_mib",
    "median_rss_mib",
    "memory_time_integral_coverage_ratio",
    "memory_time_integral_max_sample_gap_ms",
    "memory_time_integral_mib_s_per_turn",
    "model_wait_cpu_one_core_max_ratio",
    "model_wait_cpu_p50_ms",
    "model_wait_wall_p50_ms",
    "parallel_beta_mib_per_agent",
    "peak_rss_mib",
    "sampler_overhead_pct",
    "scaling_alpha",
    "session_journal_growth_bytes_per_turn",
    "session_residue_final_mib",
    "session_residue_slope_mib_per_session",
    "session_store_byte_slope_per_session",
    "session_store_file_count_slope_per_session",
    "session_store_final_residue_bytes",
    "session_store_final_residue_files",
    "resume_latency_p50_ms",
    "resume_latency_p95_ms",
    "resume_latency_slope_ms_per_turn",
    "time_to_first_model_request_max_ms",
    "time_to_first_model_request_p50_ms",
    "time_to_first_model_request_p95_ms",
    "wall_per_turn_jitter_ratio",
    "wall_per_turn_mad_ms",
    "wall_per_turn_max_ms",
    "wall_per_turn_ms",
    "wall_per_turn_p50_ms",
    "wall_per_turn_p95_ms",
];

fn compare_resources(
    left: &serde_json::Value,
    right: &serde_json::Value,
    comparable: bool,
) -> BTreeMap<String, ResourceDelta> {
    let mut deltas = BTreeMap::new();
    for field in RESOURCE_FIELDS {
        let before = numeric_field(left, field);
        let after = numeric_field(right, field);
        let value = if !comparable {
            ResourceDelta {
                before,
                after,
                delta: None,
                delta_pct: None,
                change: Some("not-comparable".to_owned()),
            }
        } else if let (Some(before_value), Some(after_value)) = (before, after) {
            ResourceDelta {
                before,
                after,
                delta: Some(after_value - before_value),
                delta_pct: Some(if before_value == 0.0 {
                    None
                } else {
                    Some(100.0 * (after_value - before_value) / before_value)
                }),
                change: None,
            }
        } else {
            ResourceDelta {
                before,
                after,
                delta: None,
                delta_pct: None,
                change: Some("unavailable".to_owned()),
            }
        };
        deltas.insert((*field).to_owned(), value);
    }
    deltas
}

fn numeric_field(value: &serde_json::Value, field: &str) -> Option<f64> {
    if let Some(required_id) = resource_field_id(field)
        && !row_measurement_complete(value, required_id)
    {
        return None;
    }
    value
        .get("resource_summary")
        .and_then(serde_json::Value::as_object)
        .and_then(|object| object.get(field))
        .and_then(serde_json::Value::as_f64)
}

fn resource_field_id(field: &str) -> Option<&'static str> {
    match field {
        "wall_per_turn_p50_ms"
        | "wall_per_turn_p95_ms"
        | "wall_per_turn_max_ms"
        | "wall_per_turn_mad_ms"
        | "wall_per_turn_jitter_ratio" => Some("turn-latency-distribution"),
        "time_to_first_model_request_p50_ms"
        | "time_to_first_model_request_p95_ms"
        | "time_to_first_model_request_max_ms" => Some("time-to-first-model-request"),
        "memory_time_integral_mib_s_per_turn"
        | "memory_time_integral_coverage_ratio"
        | "memory_time_integral_max_sample_gap_ms"
        | "cpu_per_turn_p50_ms"
        | "cpu_per_turn_p95_ms" => Some("memory-time-integral"),
        "disk_write_bytes_per_turn_p50"
        | "disk_write_bytes_per_turn_p95"
        | "disk_write_bytes_per_turn_max"
        | "session_journal_growth_bytes_per_turn"
        | "log_growth_bytes_per_turn"
        | "disk_write_growth_slope_bytes_per_turn2" => Some("disk-io-per-turn"),
        "model_wait_cpu_p50_ms"
        | "model_wait_wall_p50_ms"
        | "model_wait_cpu_one_core_max_ratio" => Some("model-wait-cpu"),
        "large_tool_output_peak_rss_delta_mib" => Some("large-tool-output"),
        "latency_slope_ms_per_100_turns" | "latency_last_first_decile_ratio" => {
            Some("latency-vs-turn-index")
        }
        "session_residue_slope_mib_per_session"
        | "session_residue_final_mib"
        | "session_store_byte_slope_per_session"
        | "session_store_file_count_slope_per_session"
        | "session_store_final_residue_bytes"
        | "session_store_final_residue_files" => Some("session-residue-sweep"),
        "resume_latency_p50_ms" | "resume_latency_p95_ms" | "resume_latency_slope_ms_per_turn" => {
            Some("resume-latency-vs-length")
        }
        "fanout_cliff_n_rss"
        | "fanout_cliff_n_wall"
        | "fanout_max_local_rss_alpha"
        | "fanout_max_local_wall_alpha"
        | "fanout_global_rss_alpha"
        | "fanout_max_measured_n" => Some("fanout-cliff"),
        "fairness_latency_cv"
        | "fairness_latency_max_min_ratio"
        | "fairness_latency_spread_ms"
        | "fairness_starved_agents" => Some("fairness-under-fanout"),
        _ => None,
    }
}

fn row_measurement_complete(value: &serde_json::Value, id: &str) -> bool {
    value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|result| {
            result.get("id").and_then(serde_json::Value::as_str) == Some(id)
                && result
                    .get("measurement_complete")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluate::{Pillar, TestResultMetadata};

    fn result(row: u8, id: &str, outcome: TestOutcome) -> TestResult {
        TestResult {
            row,
            id: id.to_owned(),
            pillar: Pillar::Resource,
            metadata: TestResultMetadata::for_row(row, &outcome),
            outcome,
            evidence: Vec::new(),
        }
    }

    fn with_declaration(mut result: TestResult, declared: bool) -> TestResult {
        result.metadata.capability_declared = Some(declared);
        result
    }

    fn index_entry(os: &str, topology: &str, profile: &str) -> IndexEntry {
        IndexEntry {
            schema: crate::results::INDEX_SCHEMA,
            run_key: "run-test".to_owned(),
            completed_at: "2026-09-01T00:00:00Z".to_owned(),
            harness: "mock".to_owned(),
            harness_version: "1".to_owned(),
            report_path: "results/mock/report.json".to_owned(),
            report_schema: 3,
            spec_version: 2,
            profile: profile.to_owned(),
            os: os.to_owned(),
            topology: topology.to_owned(),
            resource_summary: crate::results::IndexedResourceSummary::default(),
            metrics: BTreeMap::new(),
            manifest_sha256: "manifest".to_owned(),
            workflow_sha256: "workflow".to_owned(),
            ahrb_revision: "revision".to_owned(),
        }
    }

    #[test]
    fn informational_labels_are_normative_and_non_gating() -> Result<()> {
        let left = vec![result(42, "model-request-efficiency", TestOutcome::Pass)];
        let right = vec![result(
            42,
            "model-request-efficiency",
            TestOutcome::Fail("envelope".to_owned()),
        )];
        let (rows, regression) = compare_rows(&left, &right)?;
        assert_eq!(rows[0].change, "informational-regression");
        assert!(!regression);
        Ok(())
    }

    #[test]
    fn stable_id_not_conflicting_row_number_controls_requirement() -> Result<()> {
        let left = vec![result(1, "model-request-efficiency", TestOutcome::Pass)];
        let right = vec![result(
            1,
            "model-request-efficiency",
            TestOutcome::Fail("envelope".to_owned()),
        )];
        let (rows, regression) = compare_rows(&left, &right)?;
        assert_eq!(rows[0].badge_impact, "informational");
        assert_eq!(rows[0].change, "informational-regression");
        assert!(!regression);
        Ok(())
    }

    #[test]
    fn informational_unsupported_transitions_are_evidence_changes() -> Result<()> {
        let passed = vec![result(42, "model-request-efficiency", TestOutcome::Pass)];
        let unsupported = vec![result(
            42,
            "model-request-efficiency",
            TestOutcome::Unsupported("not measurable".to_owned()),
        )];
        let (removed, regression) = compare_rows(&passed, &unsupported)?;
        assert_eq!(removed[0].change, "evidence-regression");
        assert!(!regression);
        let (restored, regression) = compare_rows(&unsupported, &passed)?;
        assert_eq!(restored[0].change, "evidence-improvement");
        assert!(!regression);
        Ok(())
    }

    #[test]
    fn core_pass_removal_and_failure_are_gating_regressions() -> Result<()> {
        let left = vec![result(1, "startup", TestOutcome::Pass)];
        let failed = vec![result(1, "startup", TestOutcome::Fail("no".to_owned()))];
        assert!(compare_rows(&left, &failed)?.1);
        assert!(compare_rows(&left, &[])?.1);
        Ok(())
    }

    #[test]
    fn optional_unsupported_distinguishes_removed_from_declared() -> Result<()> {
        let before = vec![result(4, "parallel-tools", TestOutcome::Pass)];
        let mut undeclared = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Unsupported("unavailable".to_owned()),
            ),
            false,
        );
        undeclared.evidence.push(
            "capability: capability parallel_tool_execution is declared but its operation surface is absent"
                .to_owned(),
        );
        let (rows, regression) = compare_rows(&before, &[undeclared])?;
        assert_eq!(rows[0].change, "facet-removed");
        assert!(regression);

        let mut declared = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Unsupported("unavailable".to_owned()),
            ),
            true,
        );
        declared.evidence.push(
            "capability: capability parallel_tool_execution is not declared by this architecture"
                .to_owned(),
        );
        let (rows, regression) = compare_rows(&before, &[declared])?;
        assert_eq!(rows[0].change, "regression");
        assert!(regression);
        Ok(())
    }

    #[test]
    fn newly_declared_optional_nonpass_has_total_neutral_change() -> Result<()> {
        let mut before = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Unsupported("undeclared".to_owned()),
            ),
            false,
        );
        before.evidence.push(
            "capability: capability parallel_tool_execution is not declared by this architecture"
                .to_owned(),
        );
        let mut after = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Fail("declared behavior failed".to_owned()),
            ),
            true,
        );
        after.evidence.push(
            "capability: capability parallel_tool_execution is declared but its operation surface is absent"
                .to_owned(),
        );
        let (rows, regression) = compare_rows(&[before], &[after])?;
        assert_eq!(rows[0].change, "facet-added-nonpass");
        assert!(!regression);
        Ok(())
    }

    #[test]
    fn optional_declaration_changes_are_visible_when_both_states_are_unsupported() -> Result<()> {
        let mut undeclared = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Unsupported("undeclared".to_owned()),
            ),
            false,
        );
        undeclared.evidence.push(
            "capability: capability parallel_tool_execution is not declared by this architecture"
                .to_owned(),
        );
        let mut declared = with_declaration(
            result(
                4,
                "parallel-tools",
                TestOutcome::Unsupported("declared surface absent".to_owned()),
            ),
            true,
        );
        declared.evidence.push(
            "capability: capability parallel_tool_execution is declared but its operation surface is absent"
                .to_owned(),
        );

        let (added, regression) = compare_rows(
            std::slice::from_ref(&undeclared),
            std::slice::from_ref(&declared),
        )?;
        assert_eq!(added[0].change, "facet-added-nonpass");
        assert!(!regression);
        let (removed, regression) = compare_rows(&[declared], &[undeclared])?;
        assert_eq!(removed[0].change, "facet-removed");
        assert!(!regression);
        Ok(())
    }

    #[test]
    fn zero_resource_baseline_has_null_percentage() -> Result<()> {
        let left = crate::report::ResourceSummary {
            wall_per_turn_p95_ms: Some(0.0),
            ..crate::report::ResourceSummary::default()
        };
        let right = crate::report::ResourceSummary {
            wall_per_turn_p95_ms: Some(10.0),
            ..crate::report::ResourceSummary::default()
        };
        let row = serde_json::json!([{
            "id": "turn-latency-distribution",
            "measurement_complete": true
        }]);
        let left = serde_json::json!({"results": row, "resource_summary": left});
        let right = serde_json::json!({
            "results": [{
                "id": "turn-latency-distribution",
                "measurement_complete": true
            }],
            "resource_summary": right
        });
        let deltas = compare_resources(&left, &right, true);
        let delta = &deltas["wall_per_turn_p95_ms"];
        assert_eq!(delta.delta, Some(10.0));
        assert_eq!(delta.delta_pct, Some(None));
        assert!(delta.change.is_none());
        Ok(())
    }

    #[test]
    fn old_missing_resource_field_is_unavailable_not_zero() {
        let left = serde_json::json!({
            "results": [{
                "id": "turn-latency-distribution",
                "measurement_complete": true
            }],
            "resource_summary": {"peak_rss_mib": 2.0}
        });
        let right = serde_json::json!({
            "results": [{
                "id": "turn-latency-distribution",
                "measurement_complete": true
            }],
            "resource_summary": {
                "peak_rss_mib": 3.0,
                "wall_per_turn_p95_ms": 10.0
            }
        });
        let deltas = compare_resources(&left, &right, true);
        let delta = &deltas["wall_per_turn_p95_ms"];
        assert_eq!(delta.before, None);
        assert_eq!(delta.after, Some(10.0));
        assert_eq!(delta.change.as_deref(), Some("unavailable"));
    }

    #[test]
    fn unmeasured_new_row_default_is_unavailable_not_zero() {
        let report = serde_json::json!({
            "results": [{"id": "startup", "measurement_complete": true}],
            "resource_summary": {"wall_per_turn_p95_ms": 0.0}
        });
        assert_eq!(numeric_field(&report, "wall_per_turn_p95_ms"), None);
    }

    #[test]
    fn one_incomplete_resource_row_does_not_mask_other_complete_rows() {
        let report = serde_json::json!({
            "details": {"resource-summary": {"measurement_complete": false}},
            "results": [
                {"id": "turn-latency-distribution", "measurement_complete": true},
                {"id": "memory-time-integral", "measurement_complete": false}
            ],
            "resource_summary": {
                "wall_per_turn_p95_ms": 125.0,
                "memory_time_integral_mib_s_per_turn": 9.0
            }
        });
        assert_eq!(numeric_field(&report, "wall_per_turn_p95_ms"), Some(125.0));
        assert_eq!(
            numeric_field(&report, "memory_time_integral_mib_s_per_turn"),
            None
        );
    }

    #[test]
    fn unknown_scope_dimensions_are_never_resource_comparable() {
        let known = index_entry("macos", "client-process-fanout", "quick");
        assert!(comparable_scope(&known, &known));
        for unknown in [
            index_entry("unknown", "client-process-fanout", "quick"),
            index_entry("macos", "unknown", "quick"),
            index_entry("macos", "client-process-fanout", "unknown"),
        ] {
            assert!(!comparable_scope(&unknown, &unknown));
            assert!(!comparable_scope(&unknown, &known));
        }
    }
}
