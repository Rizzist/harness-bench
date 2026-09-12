//! Deterministic comparisons of durable benchmark report occurrences.

use crate::evaluate::{TestOutcome, TestResult};
use crate::results::{IndexEntry, load_indexed_report_value, read_index};
use crate::scenarios::RequirementKind;
use crate::{AhrbError, Result};
use serde::Serialize;

mod storage;
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
                "hbench diff requires exactly two harness selectors or report paths".to_owned(),
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
    if std::path::Path::new(operand).exists() {
        return storage::path_entry(std::path::Path::new(operand));
    }
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
    let previous = newest(
        matching
            .iter()
            .copied()
            .filter(|e| e.run_key != right.run_key)
            .collect(),
    )?;
    if previous.pillar != right.pillar {
        return Err(AhrbError::Usage(
            "latest occurrences span pillars; use explicit run paths".into(),
        ));
    }
    let mut earlier = Vec::new();
    for entry in matching {
        let entry_completed_at = crate::results::utc_timestamp_seconds(&entry.completed_at)?;
        if entry.run_key != right.run_key
            && entry.pillar == right.pillar
            && (right.pillar != "storage"
                || storage::same_scope(
                    entry.storage_summary.as_ref(),
                    right.storage_summary.as_ref(),
                ))
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
    if !matches!(entry.report_schema, 2..=4) {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    economy_summary_deltas: Option<EconomySummaryDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fidelity_summary_deltas: Option<FidelitySummaryDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_summary_deltas: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct EconomySummaryDiff {
    comparison_scope: String,
    tokenizer_match: bool,
    cache_input_discount_match: bool,
    cache_regime_before: Option<String>,
    cache_regime_after: Option<String>,
    cache_regime_change: String,
    completion_before: Option<String>,
    completion_after: Option<String>,
    completion_change: String,
    values: BTreeMap<String, ResourceDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    context_token_curve: Option<ContextTokenCurveDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    invalidated_prefix_tokens_per_turn: Option<ContextTokenCurveDiff>,
}

#[derive(Debug, Serialize)]
struct ContextTokenCurveDiff {
    before: Option<Vec<u64>>,
    after: Option<Vec<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<Vec<f64>>,
    change: String,
}

#[derive(Debug, Serialize)]
struct FidelitySummaryDiff {
    comparison_scope: String,
    task_profile_match: bool,
    end_reason_before: Option<String>,
    end_reason_after: Option<String>,
    end_reason_change: String,
    workspace_state_before: Option<String>,
    workspace_state_after: Option<String>,
    workspace_state_change: String,
    internal_cap_detected_before: Option<bool>,
    internal_cap_detected_after: Option<bool>,
    internal_cap_detected_change: String,
    values: BTreeMap<String, ResourceDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    survival_curve: Option<FractionCurveDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retained_tool_result_fraction: Option<FractionCurveDiff>,
}

#[derive(Debug, Serialize)]
struct FractionCurveDiff {
    before: Option<Vec<f64>>,
    after: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<Vec<f64>>,
    change: String,
}

#[derive(Debug, Serialize)]
struct RowDiff {
    pillar: crate::evaluate::Pillar,
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
    let explicit_cross_pillar = left_report
        .parsed
        .pillar
        .as_deref()
        .zip(right_report.parsed.pillar.as_deref())
        .is_some_and(|(a, b)| a != b);
    let storage_vs_non_storage = (left_report.parsed.storage_summary.is_some()
        && right_report.parsed.storage_summary.is_none()
        && right_report
            .parsed
            .results
            .iter()
            .any(|result| result.pillar != crate::evaluate::Pillar::Storage))
        || (right_report.parsed.storage_summary.is_some()
            && left_report.parsed.storage_summary.is_none()
            && left_report
                .parsed
                .results
                .iter()
                .any(|result| result.pillar != crate::evaluate::Pillar::Storage));
    if explicit_cross_pillar || storage_vs_non_storage {
        return Err(AhrbError::Usage(format!(
            "cannot diff reports from different pillars: {} vs {}",
            left.pillar, right.pillar
        )));
    }
    let (rows, mut gating_regression) =
        compare_rows(&left_report.parsed.results, &right_report.parsed.results)?;
    if left.pillar != right.pillar && (left.pillar == "storage" || right.pillar == "storage") {
        gating_regression = false;
    }
    let comparable = comparable_scope(&left, &right);
    let resource_summary_deltas =
        compare_resources(&left_report.raw, &right_report.raw, comparable);
    let economy_summary_deltas = compare_economy(
        left_report
            .parsed
            .economy_summary
            .as_ref()
            .filter(|_| left.pillar == "economy"),
        right_report
            .parsed
            .economy_summary
            .as_ref()
            .filter(|_| right.pillar == "economy"),
    );
    let fidelity_summary_deltas = compare_fidelity(
        left_report
            .parsed
            .fidelity_summary
            .as_ref()
            .filter(|_| left.pillar == "fidelity"),
        right_report
            .parsed
            .fidelity_summary
            .as_ref()
            .filter(|_| right.pillar == "fidelity"),
    );
    let storage_summary_deltas = storage::compare(
        left_report
            .parsed
            .storage_summary
            .as_ref()
            .filter(|_| left.pillar == "storage"),
        right_report
            .parsed
            .storage_summary
            .as_ref()
            .filter(|_| right.pillar == "storage"),
    )?
    .or_else(|| {
        (left.pillar == "storage" || right.pillar == "storage")
            .then(|| serde_json::json!({"comparison_scope":"unavailable","values":{}}))
    });
    let comparison_scope = if let Some(storage) = &storage_summary_deltas {
        match storage["comparison_scope"].as_str() {
            Some("comparable") if comparable => "within-topology-only",
            Some("comparable") => "not-comparable",
            Some(status) => status,
            None => "unavailable",
        }
    } else if comparable
        && resource_summary_deltas
            .values()
            .all(|value| value.delta.is_none())
        && economy_summary_deltas.is_none()
        && fidelity_summary_deltas.is_none()
    {
        "unavailable"
    } else if comparable {
        "within-topology-only"
    } else {
        "not-comparable"
    }
    .to_owned();
    Ok((
        DiffDocument {
            schema: 1,
            left,
            right,
            comparison_scope,
            rows,
            resource_summary_deltas,
            economy_summary_deltas,
            fidelity_summary_deltas,
            storage_summary_deltas,
        },
        gating_regression,
    ))
}

fn compare_fidelity(
    left: Option<&crate::fidelity::FidelitySummary>,
    right: Option<&crate::fidelity::FidelitySummary>,
) -> Option<FidelitySummaryDiff> {
    if left.is_none() && right.is_none() {
        return None;
    }
    let task_profile_match = left
        .zip(right)
        .is_some_and(|(left, right)| left.task == right.task && left.profile == right.profile);
    let numeric = |summary: &crate::fidelity::FidelitySummary, field: &str| match field {
        "model_turns" => Some(summary.model_turns as f64),
        "needle_survival_fraction" => Some(summary.needle_survival_fraction),
        "first_loss_turn" => summary.first_loss_turn.map(|turn| turn as f64),
        "end_turn" => Some(summary.end_turn as f64),
        "declared_turn_ceiling" => summary.declared_turn_ceiling.map(|turn| turn as f64),
        "retained_tool_result_fraction_final" => {
            summary.retained_tool_result_fraction.last().copied()
        }
        _ => None,
    };
    let fields = [
        "model_turns",
        "needle_survival_fraction",
        "first_loss_turn",
        "end_turn",
        "declared_turn_ceiling",
        "retained_tool_result_fraction_final",
    ];
    let values = fields
        .into_iter()
        .map(|field| {
            let before = left.and_then(|summary| numeric(summary, field));
            let after = right.and_then(|summary| numeric(summary, field));
            let delta = if task_profile_match {
                numeric_delta(before, after)
            } else {
                ResourceDelta {
                    before,
                    after,
                    delta: None,
                    delta_pct: None,
                    change: Some("not-comparable-task-or-profile".to_owned()),
                }
            };
            (field.to_owned(), delta)
        })
        .collect();
    let end_reason_before =
        left.map(|summary| crate::fidelity::end_reason_name(summary.end_reason).to_owned());
    let end_reason_after =
        right.map(|summary| crate::fidelity::end_reason_name(summary.end_reason).to_owned());
    let workspace_state_before = left
        .map(|summary| crate::fidelity::workspace_state_name(summary.workspace_state).to_owned());
    let workspace_state_after = right
        .map(|summary| crate::fidelity::workspace_state_name(summary.workspace_state).to_owned());
    let internal_cap_detected_before = left.map(|summary| summary.internal_cap_detected);
    let internal_cap_detected_after = right.map(|summary| summary.internal_cap_detected);
    Some(FidelitySummaryDiff {
        comparison_scope: "same-task-and-profile-only".to_owned(),
        task_profile_match,
        end_reason_change: guarded_value_change(
            &end_reason_before,
            &end_reason_after,
            task_profile_match,
        ),
        end_reason_before,
        end_reason_after,
        workspace_state_change: guarded_value_change(
            &workspace_state_before,
            &workspace_state_after,
            task_profile_match,
        ),
        workspace_state_before,
        workspace_state_after,
        internal_cap_detected_change: guarded_value_change(
            &internal_cap_detected_before,
            &internal_cap_detected_after,
            task_profile_match,
        ),
        internal_cap_detected_before,
        internal_cap_detected_after,
        values,
        survival_curve: compare_fraction_curves(
            left.map(|summary| summary.survival_curve.clone()),
            right.map(|summary| summary.survival_curve.clone()),
            task_profile_match,
        ),
        retained_tool_result_fraction: compare_fraction_curves(
            left.map(|summary| summary.retained_tool_result_fraction.clone()),
            right.map(|summary| summary.retained_tool_result_fraction.clone()),
            task_profile_match,
        ),
    })
}

fn guarded_value_change<T: PartialEq>(
    before: &Option<T>,
    after: &Option<T>,
    comparable: bool,
) -> String {
    if comparable || before.is_none() || after.is_none() {
        value_change(before, after)
    } else {
        "not-comparable-task-or-profile".to_owned()
    }
}

fn compare_fraction_curves(
    before: Option<Vec<f64>>,
    after: Option<Vec<f64>>,
    comparable: bool,
) -> Option<FractionCurveDiff> {
    if before.is_none() && after.is_none() {
        return None;
    }
    let (delta, change) = if !comparable {
        (None, "not-comparable-task-or-profile")
    } else if let (Some(before), Some(after)) = (&before, &after) {
        if before.len() == after.len() {
            (
                Some(
                    before
                        .iter()
                        .zip(after)
                        .map(|(before, after)| after - before)
                        .collect(),
                ),
                "comparable",
            )
        } else {
            (None, "different-length")
        }
    } else if before.is_none() {
        (None, "added")
    } else {
        (None, "removed")
    };
    Some(FractionCurveDiff {
        before,
        after,
        delta,
        change: change.to_owned(),
    })
}

fn compare_economy(
    left: Option<&crate::economy::EconomySummary>,
    right: Option<&crate::economy::EconomySummary>,
) -> Option<EconomySummaryDiff> {
    if left.is_none() && right.is_none() {
        return None;
    }
    let tokenizer_match = left.zip(right).is_some_and(|(left, right)| {
        left.reference_tokenizer == right.reference_tokenizer
            && left.reference_tariff_usd_per_million_tokens
                == right.reference_tariff_usd_per_million_tokens
    });
    let cache_input_discount_match = left.zip(right).is_some_and(|(left, right)| {
        left.schema >= 3
            && right.schema >= 3
            && left.cache_input_discount == right.cache_input_discount
    });
    let fields = [
        "model_turns",
        "total_reference_tokens",
        "tool_calls",
        "tool_batching_factor",
        "last_context_size_tokens",
        "reference_cost_usd",
        "effective_reference_tokens",
        "effective_cost_usd",
        "tokens_per_completed_task",
        "cache_eligible_fraction",
        "stable_prefix_preserved_fraction",
        "cache_bust_count",
        "invalidated_prefix_tokens",
        "redundant_tokens",
        "context_token_curve_slope",
        "per_turn_fixed_overhead_tokens",
        "wasted_tool_call_count",
        "cache_control_breakpoints",
        "retry_attempts",
        "retry_reference_tokens",
    ];
    let values = fields
        .into_iter()
        .map(|field| {
            let before = left.and_then(|summary| economy_numeric(summary, field));
            let after = right.and_then(|summary| economy_numeric(summary, field));
            let effective_cost_field =
                matches!(field, "effective_reference_tokens" | "effective_cost_usd");
            let comparable =
                tokenizer_match && (!effective_cost_field || cache_input_discount_match);
            let delta = if comparable {
                numeric_delta(before, after)
            } else {
                ResourceDelta {
                    before,
                    after,
                    delta: None,
                    delta_pct: None,
                    change: Some(
                        if effective_cost_field {
                            "not-comparable-tokenizer-tariff-or-cache-input-discount"
                        } else {
                            "not-comparable-tokenizer-or-tariff"
                        }
                        .to_owned(),
                    ),
                }
            };
            (field.to_owned(), delta)
        })
        .collect();
    let cache_regime_before = left
        .filter(|summary| summary.schema >= 3)
        .map(|summary| summary.cache_regime.clone());
    let cache_regime_after = right
        .filter(|summary| summary.schema >= 3)
        .map(|summary| summary.cache_regime.clone());
    let cache_regime_change = value_change(&cache_regime_before, &cache_regime_after);
    let completion_before =
        left.map(|summary| crate::economy::completion_name(&summary.completion).to_owned());
    let completion_after =
        right.map(|summary| crate::economy::completion_name(&summary.completion).to_owned());
    let completion_change = if completion_before == completion_after {
        "unchanged"
    } else if completion_before.is_none() {
        "added"
    } else if completion_after.is_none() {
        "removed"
    } else {
        "changed"
    };
    let context_token_curve = compare_context_token_curve(left, right, tokenizer_match);
    let invalidated_prefix_tokens_per_turn =
        compare_invalidated_prefix_tokens(left, right, tokenizer_match);
    Some(EconomySummaryDiff {
        comparison_scope: "cross-topology".to_owned(),
        tokenizer_match,
        cache_input_discount_match,
        cache_regime_before,
        cache_regime_after,
        cache_regime_change,
        completion_before,
        completion_after,
        completion_change: completion_change.to_owned(),
        values,
        context_token_curve,
        invalidated_prefix_tokens_per_turn,
    })
}

fn value_change<T: PartialEq>(before: &Option<T>, after: &Option<T>) -> String {
    if before == after {
        "unchanged"
    } else if before.is_none() {
        "added"
    } else if after.is_none() {
        "removed"
    } else {
        "changed"
    }
    .to_owned()
}

fn compare_context_token_curve(
    left: Option<&crate::economy::EconomySummary>,
    right: Option<&crate::economy::EconomySummary>,
    tokenizer_match: bool,
) -> Option<ContextTokenCurveDiff> {
    let before = left
        .filter(|summary| summary.schema >= 2)
        .map(|summary| summary.context_token_curve.clone());
    let after = right
        .filter(|summary| summary.schema >= 2)
        .map(|summary| summary.context_token_curve.clone());
    if before.is_none() && after.is_none() {
        return None;
    }
    let (delta, change) = if !tokenizer_match {
        (None, "not-comparable-tokenizer-or-tariff")
    } else if let (Some(before), Some(after)) = (&before, &after) {
        if before.len() == after.len() {
            (
                Some(
                    before
                        .iter()
                        .zip(after)
                        .map(|(before, after)| *after as f64 - *before as f64)
                        .collect(),
                ),
                "comparable",
            )
        } else {
            (None, "different-length")
        }
    } else if before.is_none() {
        (None, "added")
    } else {
        (None, "removed")
    };
    Some(ContextTokenCurveDiff {
        before,
        after,
        delta,
        change: change.to_owned(),
    })
}

fn compare_invalidated_prefix_tokens(
    left: Option<&crate::economy::EconomySummary>,
    right: Option<&crate::economy::EconomySummary>,
    tokenizer_match: bool,
) -> Option<ContextTokenCurveDiff> {
    let before = left
        .filter(|summary| summary.schema >= 3)
        .map(|summary| summary.invalidated_prefix_tokens_per_turn.clone());
    let after = right
        .filter(|summary| summary.schema >= 3)
        .map(|summary| summary.invalidated_prefix_tokens_per_turn.clone());
    compare_u64_arrays(before, after, tokenizer_match)
}

fn compare_u64_arrays(
    before: Option<Vec<u64>>,
    after: Option<Vec<u64>>,
    tokenizer_match: bool,
) -> Option<ContextTokenCurveDiff> {
    if before.is_none() && after.is_none() {
        return None;
    }
    let (delta, change) = if !tokenizer_match {
        (None, "not-comparable-tokenizer-or-tariff")
    } else if let (Some(before), Some(after)) = (&before, &after) {
        if before.len() == after.len() {
            (
                Some(
                    before
                        .iter()
                        .zip(after)
                        .map(|(before, after)| *after as f64 - *before as f64)
                        .collect(),
                ),
                "comparable",
            )
        } else {
            (None, "different-length")
        }
    } else if before.is_none() {
        (None, "added")
    } else {
        (None, "removed")
    };
    Some(ContextTokenCurveDiff {
        before,
        after,
        delta,
        change: change.to_owned(),
    })
}

fn economy_numeric(summary: &crate::economy::EconomySummary, field: &str) -> Option<f64> {
    let advanced = matches!(
        field,
        "cache_eligible_fraction"
            | "redundant_tokens"
            | "context_token_curve_slope"
            | "per_turn_fixed_overhead_tokens"
            | "wasted_tool_call_count"
            | "cache_control_breakpoints"
            | "retry_attempts"
            | "retry_reference_tokens"
    );
    if advanced && summary.schema < 2 {
        return None;
    }
    let cache_economics = matches!(
        field,
        "effective_reference_tokens"
            | "effective_cost_usd"
            | "stable_prefix_preserved_fraction"
            | "cache_bust_count"
            | "invalidated_prefix_tokens"
    );
    if cache_economics && summary.schema < 3 {
        return None;
    }
    match field {
        "model_turns" => Some(summary.model_turns as f64),
        "total_reference_tokens" => Some(summary.total_reference_tokens as f64),
        "tool_calls" => Some(summary.tool_calls as f64),
        "tool_batching_factor" => Some(summary.tool_batching_factor),
        "last_context_size_tokens" => Some(summary.last_context_size_tokens as f64),
        "reference_cost_usd" => Some(summary.reference_cost_usd),
        "effective_reference_tokens" => Some(summary.effective_reference_tokens),
        "effective_cost_usd" => Some(summary.effective_cost_usd),
        "tokens_per_completed_task" => summary.tokens_per_completed_task.map(|value| value as f64),
        "cache_eligible_fraction" => Some(summary.cache_eligible_fraction),
        "stable_prefix_preserved_fraction" => Some(summary.stable_prefix_preserved_fraction),
        "cache_bust_count" => Some(summary.cache_bust_count as f64),
        "invalidated_prefix_tokens" => Some(summary.invalidated_prefix_tokens as f64),
        "redundant_tokens" => Some(summary.redundant_tokens as f64),
        "context_token_curve_slope" => Some(summary.context_token_curve_slope),
        "per_turn_fixed_overhead_tokens" => Some(summary.per_turn_fixed_overhead_tokens as f64),
        "wasted_tool_call_count" => Some(summary.wasted_tool_call_count as f64),
        "cache_control_breakpoints" => Some(summary.cache_control_breakpoints as f64),
        "retry_attempts" => Some(summary.retry_attempts as f64),
        "retry_reference_tokens" => Some(summary.retry_reference_tokens as f64),
        _ => None,
    }
}

fn numeric_delta(before: Option<f64>, after: Option<f64>) -> ResourceDelta {
    if let (Some(before_value), Some(after_value)) = (before, after) {
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
    }
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
    for key in ids {
        let before = left.get(&key).copied();
        let after = right.get(&key).copied();
        let (pillar, id) = key;
        let current = after.or(before).ok_or_else(|| {
            AhrbError::Protocol(format!("row {id:?} disappeared while joining diff"))
        })?;
        let requirement = requirement_for(current);
        let (change, row_regression) = if current.pillar == crate::evaluate::Pillar::Storage {
            storage::row_change(&id, before, after)
        } else {
            row_change(requirement, before, after)
        };
        gating_regression |= row_regression;
        rows.push(RowDiff {
            pillar,
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

fn results_by_id(
    results: &[TestResult],
) -> Result<BTreeMap<(crate::evaluate::Pillar, String), &TestResult>> {
    let mut by_id = BTreeMap::new();
    for result in results {
        if by_id
            .insert((result.pillar, result.id.clone()), result)
            .is_some()
        {
            return Err(AhrbError::Protocol(format!(
                "report contains duplicate stable row ID {:?}",
                result.id
            )));
        }
    }
    Ok(by_id)
}

fn requirement_for(result: &TestResult) -> RequirementKind {
    if result.pillar == crate::evaluate::Pillar::Storage {
        return if result.id == "footprint-curve"
            || (result.id == "delete-uninstall-residue"
                && result.metadata.requirement != "informational")
        {
            RequirementKind::Core
        } else {
            RequirementKind::Informational
        };
    }
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
    known_scope_value(&left.pillar)
        && left.pillar == right.pillar
        && known_scope_value(&left.os)
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
    !value.is_empty() && value != "unknown" && value != "unavailable"
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
        let value = if comparable || before.is_none() || after.is_none() {
            numeric_delta(before, after)
        } else {
            ResourceDelta {
                before,
                after,
                delta: None,
                delta_pct: None,
                change: Some("not-comparable".to_owned()),
            }
        };
        deltas.insert((*field).to_owned(), value);
    }
    deltas
}

fn numeric_field(value: &serde_json::Value, field: &str) -> Option<f64> {
    // Storage carries the legacy summary's default numbers for schema compatibility,
    // but does not run the general resource collector.
    if value.get("pillar").and_then(serde_json::Value::as_str) == Some("storage")
        || value
            .get("storage_summary")
            .is_some_and(|summary| !summary.is_null())
    {
        return None;
    }
    if let Some(required_id) = resource_field_id(field)
        && !row_measurement_complete(value, required_id)
    {
        return None;
    }
    if resource_field_id(field).is_none()
        && value
            .pointer("/details/resource-summary/measurement_complete")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
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
    use crate::economy::{EconomyCompletion, EconomySummary, ReferenceTokenizerPin};
    use crate::evaluate::{Pillar, TestResultMetadata};
    use crate::fidelity::{FidelityEndReason, FidelitySummary, HarnessExitStatus, WorkspaceState};

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
            pillar: "matrix".into(),
            storage_summary: None,
            badge_label: None,
            outcome_counts: Default::default(),
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

    fn economy(tokens: u64) -> EconomySummary {
        EconomySummary {
            schema: 3,
            task: "economy-test".to_owned(),
            profile: "quick".to_owned(),
            turn_budget: 8,
            reference_tokenizer: ReferenceTokenizerPin {
                encoding: "reference".to_owned(),
                version: "v1".to_owned(),
                vocabulary_sha256: "0".repeat(64),
                vocabulary_entries: 256,
            },
            reference_token_label: "reference tokens".to_owned(),
            model_turns: 8,
            total_reference_tokens: tokens,
            tool_calls: 17,
            tool_results: 17,
            tool_result_requests: 7,
            tool_batching_factor: 17.0 / 7.0,
            last_context_size_tokens: tokens / 2,
            completion: EconomyCompletion::Completed,
            completion_label: "followed scripted terminal".to_owned(),
            effects_verified: Default::default(),
            reference_tariff_usd_per_million_tokens: 10.0,
            reference_cost_usd: tokens as f64 * 10.0 / 1_000_000.0,
            cost_outcome_label: String::new(),
            tokens_per_completed_task: Some(tokens),
            cache_eligible_fraction: 0.5,
            cache_eligible_fraction_label: "cache upper bound".to_owned(),
            cache_control_breakpoints: 0,
            cache_control_breakpoints_per_request: vec![0; 8],
            cache_control_breakpoints_label: "separate declarations".to_owned(),
            cache_eligibility_note: "upper bound".to_owned(),
            redundant_tokens: tokens / 3,
            redundant_tokens_label: "reference tokens".to_owned(),
            context_token_curve: vec![tokens / 2, tokens],
            context_token_curve_slope: tokens as f64 / 2.0,
            context_token_curve_label: "reference tokens/request".to_owned(),
            context_token_curve_last_matches_last_context_size: false,
            per_turn_fixed_overhead_tokens: 10,
            per_turn_fixed_overhead_tokens_label: "reference tokens".to_owned(),
            wasted_tool_call_count: 0,
            wasted_tool_call_count_label: "proven only".to_owned(),
            retry_attempts: 0,
            retry_reference_tokens: 0,
            retry_label: "separate".to_owned(),
            cache_regime: "automatic-prefix".to_owned(),
            cache_regime_label: "zero explicit breakpoints expected".to_owned(),
            cache_input_discount: 0.90,
            cache_input_discount_label: "stated assumption".to_owned(),
            effective_reference_tokens: tokens as f64 * 0.55,
            effective_cost_usd: tokens as f64 * 0.55 * 10.0 / 1_000_000.0,
            effective_cost_label: "upper-bound proxy".to_owned(),
            stable_prefix_preserved_fraction: 1.0,
            cache_bust_count: 0,
            invalidated_prefix_tokens: 0,
            invalidated_prefix_tokens_per_turn: vec![0],
            prefix_stability_label: "reference-token LCP".to_owned(),
        }
    }

    fn fidelity(profile: &str, survival: f64) -> FidelitySummary {
        FidelitySummary {
            schema: 1,
            task: crate::fidelity::FIDELITY_TASK_ID.to_owned(),
            profile: profile.to_owned(),
            turn_budget: 24,
            model_turns: 24,
            measurement_label: "provider bytes only".to_owned(),
            needles: Vec::new(),
            needle_survival_fraction: survival,
            needle_survival_fraction_label: "final bytes".to_owned(),
            survival_curve: vec![1.0, survival],
            survival_curve_label: "ordered bytes".to_owned(),
            first_loss_turn: (survival < 1.0).then_some(2),
            retained_tool_result_fraction: vec![1.0, survival],
            retained_tool_result_fraction_label: "canonical carriers".to_owned(),
            end_reason: FidelityEndReason::ReachedScriptedTerminal,
            end_reason_label: "script only".to_owned(),
            end_turn: 24,
            harness_exit_status: HarnessExitStatus::Running,
            harness_exit_code: None,
            internal_cap_detected: false,
            declared_turn_ceiling: None,
            workspace_state: WorkspaceState::Mutated,
            workspace_state_label: "scripted effects".to_owned(),
            workspace_receipt_before_sha256: "a".repeat(64),
            workspace_receipt_after_sha256: "b".repeat(64),
        }
    }

    #[test]
    fn legacy_pillar_inference_uses_summary_blocks() {
        let mut report = crate::report::Report {
            economy_summary: Some(economy(100)),
            ..Default::default()
        };
        assert_eq!(crate::results::report_pillar(&report), "economy");
        report.economy_summary = None;
        report.fidelity_summary = Some(fidelity("quick", 1.0));
        assert_eq!(crate::results::report_pillar(&report), "fidelity");
    }

    #[test]
    fn storage_latest_refuses_cross_pillar_and_skips_incompatible_scope() -> Result<()> {
        let mut a = index_entry("macos", "per-invocation", "quick");
        a.run_key = "a".into();
        a.harness = "h".into();
        a.completed_at = "2026-01-01T00:00:00Z".into();
        let mut b = a.clone();
        b.run_key = "b".into();
        b.completed_at = "2026-01-02T00:00:00Z".into();
        b.pillar = "storage".into();
        assert!(
            resolve_latest_pair(&[a.clone(), b.clone()], "h")
                .unwrap_err()
                .to_string()
                .contains("span pillars")
        );
        a.pillar = "storage".into();
        let scope = crate::storage::evidence::StorageSummary {
            schema: 1,
            task: crate::storage::TASK.into(),
            profile: "quick".into(),
            os: "macos".into(),
            topology: "per-invocation".into(),
            comparison_scope: "within-topology-only".into(),
            allocation_source: "stat-st_blocks-512".into(),
            counter_source: "macos-ri_diskio_byteswritten".into(),
            declarations_sha256: "a".repeat(64),
            ..Default::default()
        };
        a.storage_summary = Some(scope.clone());
        b.storage_summary = Some(scope);
        let mut incompatible = a.clone();
        incompatible.run_key = "between".into();
        incompatible.completed_at = "2026-01-01T12:00:00Z".into();
        incompatible
            .storage_summary
            .as_mut()
            .unwrap()
            .declarations_sha256 = "b".repeat(64);
        let (left, right) = resolve_latest_pair(&[a, incompatible, b], "h")?;
        assert_eq!(left.run_key, "a");
        assert_eq!(right.run_key, "b");
        Ok(())
    }

    #[test]
    fn fidelity_diff_requires_equal_task_and_profile() {
        let before = fidelity("quick", 1.0);
        let after = fidelity("quick", 0.0);
        let compared = compare_fidelity(Some(&before), Some(&after)).expect("fidelity diff");
        assert!(compared.task_profile_match);
        assert_eq!(
            compared.values["needle_survival_fraction"].delta,
            Some(-1.0)
        );
        assert_eq!(
            compared.survival_curve.expect("survival curve").delta,
            Some(vec![0.0, -1.0])
        );

        let mismatch = fidelity("cert", 0.0);
        let guarded = compare_fidelity(Some(&before), Some(&mismatch))
            .expect("profile-guarded fidelity diff");
        assert!(!guarded.task_profile_match);
        assert_eq!(
            guarded.values["needle_survival_fraction"].change.as_deref(),
            Some("not-comparable-task-or-profile")
        );
        assert!(
            guarded
                .survival_curve
                .expect("guarded survival curve")
                .delta
                .is_none()
        );
    }

    #[test]
    fn economy_diff_is_cross_topology_and_pin_guarded() {
        let before = economy(100);
        let after = economy(125);
        let compared = compare_economy(Some(&before), Some(&after)).expect("economy diff");
        assert_eq!(compared.comparison_scope, "cross-topology");
        assert!(compared.tokenizer_match);
        assert_eq!(compared.values["total_reference_tokens"].delta, Some(25.0));
        assert!(
            compared.values["effective_reference_tokens"]
                .delta
                .is_some_and(|delta| (delta - 13.75).abs() < f64::EPSILON * 64.0)
        );
        assert_eq!(
            compared
                .context_token_curve
                .as_ref()
                .and_then(|curve| curve.delta.as_ref()),
            Some(&vec![12.0, 25.0])
        );
        assert_eq!(
            compared
                .invalidated_prefix_tokens_per_turn
                .as_ref()
                .and_then(|curve| curve.delta.as_ref()),
            Some(&vec![0.0])
        );

        let mut mismatched = after;
        mismatched.reference_tokenizer.version = "v2".to_owned();
        let guarded =
            compare_economy(Some(&before), Some(&mismatched)).expect("guarded economy diff");
        assert!(!guarded.tokenizer_match);
        assert_eq!(
            guarded.values["total_reference_tokens"].change.as_deref(),
            Some("not-comparable-tokenizer-or-tariff")
        );

        let mut mismatched_discount = economy(125);
        mismatched_discount.cache_input_discount = 0.50;
        let discount_guarded = compare_economy(Some(&before), Some(&mismatched_discount))
            .expect("discount-guarded economy diff");
        assert!(!discount_guarded.cache_input_discount_match);
        assert_eq!(
            discount_guarded.values["effective_cost_usd"]
                .change
                .as_deref(),
            Some("not-comparable-tokenizer-tariff-or-cache-input-discount")
        );
        assert_eq!(
            discount_guarded.values["total_reference_tokens"].delta,
            Some(25.0)
        );
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
                "cpu_total_s": 0.0,
                "wall_per_turn_p95_ms": 125.0,
                "memory_time_integral_mib_s_per_turn": 9.0
            }
        });
        assert_eq!(numeric_field(&report, "wall_per_turn_p95_ms"), Some(125.0));
        assert_eq!(numeric_field(&report, "cpu_total_s"), None);
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
