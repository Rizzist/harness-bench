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
    entries
        .into_iter()
        .max_by(|left, right| {
            (&left.completed_at, &left.run_key).cmp(&(&right.completed_at, &right.run_key))
        })
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
    let earlier = matching
        .into_iter()
        .filter(|entry| {
            entry.run_key != right.run_key
                && entry.os == right.os
                && entry.topology == right.topology
                && entry.profile == right.profile
                && entry.report_schema == right.report_schema
                && (&entry.completed_at, &entry.run_key) < (&right.completed_at, &right.run_key)
        })
        .collect::<Vec<_>>();
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
    let comparable =
        left.os == right.os && left.topology == right.topology && left.profile == right.profile;
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
        .find(|definition| definition.id == result.id || definition.row == result.row)
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
    if before_class == after_class {
        return ("unchanged", false);
    }
    if matches!(requirement, RequirementKind::Informational) {
        return informational_change(before_class, after_class);
    }
    if matches!(requirement, RequirementKind::OptionalFacet { .. }) {
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
    result.evidence.iter().find_map(|evidence| {
        let detail = evidence.strip_prefix("capability: capability ")?;
        if detail.ends_with(" is not declared by this architecture") {
            Some(false)
        } else if detail.ends_with(" is declared but its operation surface is absent") {
            Some(true)
        } else {
            None
        }
    })
}

fn informational_change(before: &str, after: &str) -> (&'static str, bool) {
    match (before, after) {
        ("PASS", "FAIL") => ("informational-regression", false),
        ("FAIL", "PASS") => ("informational-improvement", false),
        ("PASS" | "FAIL", "ERROR" | "ABSENT") => ("evidence-regression", false),
        ("ERROR" | "ABSENT", "PASS" | "FAIL") => ("evidence-improvement", false),
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
    "idle_rss_mib",
    "mean_rss_mib",
    "median_rss_mib",
    "memory_time_integral_coverage_ratio",
    "memory_time_integral_max_sample_gap_ms",
    "memory_time_integral_mib_s_per_turn",
    "parallel_beta_mib_per_agent",
    "peak_rss_mib",
    "sampler_overhead_pct",
    "scaling_alpha",
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
    let left_available = resource_summary_available(left);
    let right_available = resource_summary_available(right);
    let mut deltas = BTreeMap::new();
    for field in RESOURCE_FIELDS {
        let before = left_available.then(|| numeric_field(left, field)).flatten();
        let after = right_available
            .then(|| numeric_field(right, field))
            .flatten();
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

fn resource_summary_available(value: &serde_json::Value) -> bool {
    value
        .pointer("/details/resource-summary/measurement_complete")
        .and_then(serde_json::Value::as_bool)
        != Some(false)
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
        let mut undeclared = result(
            4,
            "parallel-tools",
            TestOutcome::Unsupported("unavailable".to_owned()),
        );
        undeclared.evidence.push(
            "capability: capability parallel_tool_execution is not declared by this architecture"
                .to_owned(),
        );
        let (rows, regression) = compare_rows(&before, &[undeclared])?;
        assert_eq!(rows[0].change, "facet-removed");
        assert!(regression);

        let mut declared = result(
            4,
            "parallel-tools",
            TestOutcome::Unsupported("unavailable".to_owned()),
        );
        declared.evidence.push(
            "capability: capability parallel_tool_execution is declared but its operation surface is absent"
                .to_owned(),
        );
        let (rows, regression) = compare_rows(&before, &[declared])?;
        assert_eq!(rows[0].change, "regression");
        assert!(regression);
        Ok(())
    }

    #[test]
    fn zero_resource_baseline_has_null_percentage() -> Result<()> {
        let left = crate::report::ResourceSummary::default();
        let right = crate::report::ResourceSummary {
            wall_per_turn_p95_ms: 10.0,
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
}
