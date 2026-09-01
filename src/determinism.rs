//! Cross-execution canonical-request comparison for matrix rows 63 and 64.

use crate::fake_model::{ModelRequestRecord, canonicalize_json};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Exact AHRB-owned values that may be replaced before cross-run comparison.
///
/// The normalizer never guesses from field names or value shapes. Each value is
/// supplied by the runner that created it, and replacement is additionally
/// restricted by the dialect-specific JSON Pointer/type allowlist below.
#[derive(Clone, Debug, Default)]
pub struct NormalizationContext {
    /// Exact provider credential injected by AHRB.
    pub credential: String,
    /// Exact fresh profile roots created by AHRB.
    pub profile_paths: Vec<String>,
    /// Exact actor workspace roots created by AHRB.
    pub workspace_paths: Vec<String>,
    /// Exact temporary paths created by AHRB for this execution.
    pub temporary_paths: Vec<String>,
    /// Exact Unix-socket paths created by AHRB for this execution.
    pub socket_paths: Vec<String>,
    /// Exact route/run markers created by AHRB for this workflow.
    pub run_markers: Vec<String>,
    /// Exact row-execution occurrence ID created by AHRB.
    pub execution_id: String,
}

/// One fresh isolated execution used by rows 63 and 64.
#[derive(Clone, Debug)]
pub struct DeterminismRun {
    /// One-based execution occurrence.
    pub run: u32,
    /// Physical request attempts observed in this execution.
    pub records: Vec<ModelRequestRecord>,
    /// Whether the external request collector completed and its ledger was
    /// snapshotted after the execution terminalized.
    pub request_collector_complete: bool,
    /// Exact AHRB-owned values for this execution.
    pub normalization: NormalizationContext,
}

/// Row-63 score, exact counters, and structured diagnostics.
#[derive(Clone, Debug)]
pub struct NondeterministicFieldEvaluation {
    /// Score `1 - varying/comparable`.
    pub score: f64,
    /// Leaf comparisons plus one occurrence for each wholly missing request.
    pub comparable_leaf_occurrences: u64,
    /// Differing leaves plus one occurrence for each wholly missing request.
    pub varying_leaf_occurrences: u64,
    /// Number of distinct varying JSON Pointers.
    pub varying_pointer_count: u64,
    /// Number of distinct varying critical JSON Pointers.
    pub varying_critical_field_count: u64,
    /// Exact `details.nondeterministic-field-report` value.
    pub details: Value,
    /// Whether the requested run set and denominator were complete.
    pub measurement_complete: bool,
    /// Informational reference-envelope decision.
    pub reference_envelope_pass: bool,
    /// Deterministic diagnostic for incomplete evidence.
    pub measurement_error: Option<String>,
}

/// Row-64 normalized semantic-stream decision.
#[derive(Clone, Debug)]
pub struct CrossRunReproducibilityEvaluation {
    /// Whether every normalized semantic request stream is identical.
    pub identical: bool,
    /// Number of independently collected run streams.
    pub request_stream_count: u64,
    /// Physical attempt count in the baseline stream.
    pub attempt_count: u64,
    /// Exact `details.cross-run-reproducibility` value.
    pub details: Value,
    /// Whether the expected run set and nonempty baseline were available.
    pub measurement_complete: bool,
    /// Deterministic diagnostic for incomplete evidence.
    pub measurement_error: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticRequestKey {
    scenario: String,
    actor: String,
    semantic_ordinal: u64,
    checkpoint: String,
    attempt: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticAttemptKey {
    scenario: String,
    actor: String,
    semantic_ordinal: u64,
    checkpoint: String,
}

#[derive(Clone, Debug)]
struct ComparableRequest {
    dialect: String,
    canonical: Value,
    semantic_attempts_total: u64,
}

#[derive(Clone, Debug, Default)]
struct VaryingField {
    occurrences: u64,
    comparison_runs: BTreeSet<u32>,
    before_types: BTreeSet<String>,
    after_types: BTreeSet<String>,
    dialects: BTreeSet<String>,
}

#[derive(Clone, Copy)]
enum OwnedValueKind {
    Credential,
    ProfilePath,
    WorkspacePath,
    TemporaryPath,
    SocketPath,
    RunMarker,
    ExecutionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PointerPatternSegment {
    Wildcard,
    Literal(String),
}

const CHAT_CONTENT_POINTERS: &[&str] = &[
    "/messages/*/content",
    "/messages/*/content/*/text",
    "/messages/*/tool_calls/*/function/arguments",
];
const RESPONSES_CONTENT_POINTERS: &[&str] = &[
    "/instructions",
    "/input",
    "/input/*/content",
    "/input/*/content/*/text",
    "/input/*/arguments",
];
const ANTHROPIC_CONTENT_POINTERS: &[&str] = &[
    "/system",
    "/system/*/text",
    "/messages/*/content",
    "/messages/*/content/*/text",
    "/messages/*/content/*/input",
];

fn metadata_pointer(kind: OwnedValueKind) -> &'static str {
    match kind {
        OwnedValueKind::Credential => "/metadata/ahrb_credential",
        OwnedValueKind::ProfilePath => "/metadata/ahrb_profile_path",
        OwnedValueKind::WorkspacePath => "/metadata/ahrb_workspace_path",
        OwnedValueKind::TemporaryPath => "/metadata/ahrb_tmp_path",
        OwnedValueKind::SocketPath => "/metadata/ahrb_socket_path",
        OwnedValueKind::RunMarker => "/metadata/ahrb_run_marker",
        OwnedValueKind::ExecutionId => "/metadata/ahrb_execution_id",
    }
}

fn decode_pointer_segment(segment: &str, pattern: bool) -> Result<String, String> {
    let mut decoded = String::new();
    let mut characters = segment.chars();
    while let Some(character) = characters.next() {
        if character == '~' {
            match characters.next() {
                Some('0') => decoded.push('~'),
                Some('1') => decoded.push('/'),
                Some('2') if pattern => decoded.push('*'),
                Some(escape) => {
                    return Err(format!("invalid pointer escape ~{escape}"));
                }
                None => return Err("trailing ~ in pointer segment".to_owned()),
            }
        } else if pattern && character == '*' {
            return Err("literal * in a pointer pattern must be encoded as ~2".to_owned());
        } else {
            decoded.push(character);
        }
    }
    Ok(decoded)
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    let Some(segments) = pointer.strip_prefix('/') else {
        return Err("JSON pointer must be empty or begin with /".to_owned());
    };
    segments
        .split('/')
        .map(|segment| decode_pointer_segment(segment, false))
        .collect()
}

fn parse_pointer_pattern(pattern: &str) -> Result<Vec<PointerPatternSegment>, String> {
    if pattern.is_empty() {
        return Ok(Vec::new());
    }
    let Some(segments) = pattern.strip_prefix('/') else {
        return Err("pointer pattern must be empty or begin with /".to_owned());
    };
    segments
        .split('/')
        .map(|segment| {
            if segment == "*" {
                Ok(PointerPatternSegment::Wildcard)
            } else {
                decode_pointer_segment(segment, true).map(PointerPatternSegment::Literal)
            }
        })
        .collect()
}

fn pointer_pattern_matches(pointer: &str, pattern: &str) -> bool {
    let (Ok(pointer), Ok(pattern)) = (parse_json_pointer(pointer), parse_pointer_pattern(pattern))
    else {
        return false;
    };
    pointer.len() >= pattern.len()
        && pointer
            .iter()
            .zip(pattern)
            .all(|(actual, expected)| match expected {
                PointerPatternSegment::Wildcard => true,
                PointerPatternSegment::Literal(expected) => actual == &expected,
            })
}

fn content_pointer_allowed(dialect: &str, pointer: &str) -> bool {
    let patterns = match dialect {
        "openai-chat-completions" => CHAT_CONTENT_POINTERS,
        "openai-responses" => RESPONSES_CONTENT_POINTERS,
        "anthropic-messages" => ANTHROPIC_CONTENT_POINTERS,
        _ => &[],
    };
    patterns
        .iter()
        .any(|pattern| pointer_pattern_matches(pointer, pattern))
}

fn normalization_allowed(dialect: &str, pointer: &str, kind: OwnedValueKind) -> bool {
    if pointer == metadata_pointer(kind) {
        return true;
    }
    match kind {
        OwnedValueKind::ProfilePath
        | OwnedValueKind::WorkspacePath
        | OwnedValueKind::TemporaryPath
        | OwnedValueKind::SocketPath
        | OwnedValueKind::RunMarker => content_pointer_allowed(dialect, pointer),
        OwnedValueKind::Credential | OwnedValueKind::ExecutionId => false,
    }
}

fn replace_owned_string(
    value: &mut String,
    pointer: &str,
    dialect: &str,
    kind: OwnedValueKind,
    sources: &[String],
    sentinel: &str,
) {
    if !normalization_allowed(dialect, pointer, kind) {
        return;
    }
    let mut sources = sources
        .iter()
        .filter(|source| !source.is_empty())
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    sources.dedup();
    for source in sources {
        if value.contains(source.as_str()) {
            *value = value.replace(source.as_str(), sentinel);
        }
    }
}

fn normalize_at_pointer(
    value: &Value,
    pointer: &str,
    dialect: &str,
    context: &NormalizationContext,
) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    normalize_at_pointer(item, &format!("{pointer}/{index}"), dialect, context)
                })
                .collect(),
        ),
        Value::Object(object) => {
            let mut normalized = Map::new();
            for (key, item) in object {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                normalized.insert(
                    key.clone(),
                    normalize_at_pointer(item, &format!("{pointer}/{escaped}"), dialect, context),
                );
            }
            Value::Object(normalized)
        }
        Value::String(original) => {
            let mut normalized = original.clone();
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::WorkspacePath,
                &context.workspace_paths,
                "<AHRB_WORKSPACE_PATH>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::SocketPath,
                &context.socket_paths,
                "<AHRB_SOCKET_PATH>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::TemporaryPath,
                &context.temporary_paths,
                "<AHRB_TMP_PATH>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::ProfilePath,
                &context.profile_paths,
                "<AHRB_PROFILE_PATH>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::RunMarker,
                &context.run_markers,
                "<AHRB_RUN_MARKER>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::Credential,
                std::slice::from_ref(&context.credential),
                "<AHRB_CREDENTIAL>",
            );
            replace_owned_string(
                &mut normalized,
                pointer,
                dialect,
                OwnedValueKind::ExecutionId,
                std::slice::from_ref(&context.execution_id),
                "<AHRB_EXECUTION_ID>",
            );
            Value::String(normalized)
        }
        _ => value.clone(),
    }
}

/// Normalize one canonical request with the exact pointer/type allowlist.
pub fn normalize_canonical_request(
    dialect: &str,
    canonical: &Value,
    context: &NormalizationContext,
) -> Value {
    canonicalize_json(&normalize_at_pointer(canonical, "", dialect, context))
}

fn semantic_key(record: &ModelRequestRecord) -> SemanticRequestKey {
    SemanticRequestKey {
        scenario: record.request.scenario.clone(),
        actor: record.request.actor.clone(),
        checkpoint: record.request.checkpoint.clone(),
        semantic_ordinal: record.semantic_ordinal,
        attempt: record.attempt,
    }
}

fn attempt_key(key: &SemanticRequestKey) -> SemanticAttemptKey {
    SemanticAttemptKey {
        scenario: key.scenario.clone(),
        actor: key.actor.clone(),
        checkpoint: key.checkpoint.clone(),
        semantic_ordinal: key.semantic_ordinal,
    }
}

fn semantic_key_json(key: &SemanticRequestKey) -> Value {
    json!({
        "scenario": key.scenario,
        "actor": key.actor,
        "checkpoint": key.checkpoint,
        "semantic_ordinal": key.semantic_ordinal,
        "attempt": key.attempt,
    })
}

fn attempt_key_json(key: &SemanticAttemptKey) -> Value {
    json!({
        "scenario": key.scenario,
        "actor": key.actor,
        "checkpoint": key.checkpoint,
        "semantic_ordinal": key.semantic_ordinal,
    })
}

fn comparable_requests(
    run: &DeterminismRun,
) -> Result<BTreeMap<SemanticRequestKey, ComparableRequest>, String> {
    if !run.request_collector_complete {
        return Err(format!(
            "run {} request collector evidence is incomplete",
            run.run
        ));
    }
    let mut requests = BTreeMap::new();
    for record in &run.records {
        let key = semantic_key(record);
        let request = ComparableRequest {
            dialect: record.request.dialect.clone(),
            canonical: normalize_canonical_request(
                &record.request.dialect,
                &record.request.canonical,
                &run.normalization,
            ),
            semantic_attempts_total: record.semantic_attempts_total,
        };
        if requests.insert(key, request).is_some() {
            return Err(format!(
                "run {} contains a duplicate semantic request key",
                run.run
            ));
        }
    }
    Ok(requests)
}

fn json_type(value: Option<&Value>) -> &'static str {
    match value {
        None => "missing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(Value::Array(_)) => "array",
        Some(Value::Object(_)) => "object",
    }
}

#[derive(Clone, Debug)]
struct ComparisonOccurrence {
    pointer: String,
    before: Option<Value>,
    after: Option<Value>,
}

fn pure_array_reorder(before: &[Value], after: &[Value]) -> Result<bool, String> {
    if before == after || before.len() != after.len() {
        return Ok(false);
    }
    let mut before_elements = before
        .iter()
        .map(serde_json::to_vec)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("serialize baseline array element: {error}"))?;
    let mut after_elements = after
        .iter()
        .map(serde_json::to_vec)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("serialize comparison array element: {error}"))?;
    before_elements.sort();
    after_elements.sort();
    Ok(before_elements == after_elements)
}

fn comparison_occurrences(
    before: &Value,
    after: &Value,
) -> Result<Vec<ComparisonOccurrence>, String> {
    fn push_present_subtree(
        value: &Value,
        pointer: &str,
        before: bool,
        occurrences: &mut Vec<ComparisonOccurrence>,
    ) {
        match value {
            Value::Array(items) if !items.is_empty() => {
                for (index, item) in items.iter().enumerate() {
                    push_present_subtree(item, &format!("{pointer}/{index}"), before, occurrences);
                }
            }
            Value::Object(object) if !object.is_empty() => {
                for (key, item) in object {
                    let escaped = key.replace('~', "~0").replace('/', "~1");
                    push_present_subtree(
                        item,
                        &format!("{pointer}/{escaped}"),
                        before,
                        occurrences,
                    );
                }
            }
            _ if before => occurrences.push(ComparisonOccurrence {
                pointer: pointer.to_owned(),
                before: Some(value.clone()),
                after: None,
            }),
            _ => occurrences.push(ComparisonOccurrence {
                pointer: pointer.to_owned(),
                before: None,
                after: Some(value.clone()),
            }),
        }
    }

    fn visit(
        before: Option<&Value>,
        after: Option<&Value>,
        pointer: &str,
        occurrences: &mut Vec<ComparisonOccurrence>,
    ) -> Result<(), String> {
        match (before, after) {
            (Some(Value::Array(before)), Some(Value::Array(after))) => {
                if pure_array_reorder(before, after)? {
                    occurrences.push(ComparisonOccurrence {
                        pointer: pointer.to_owned(),
                        before: Some(Value::Array(before.clone())),
                        after: Some(Value::Array(after.clone())),
                    });
                } else if before.is_empty() && after.is_empty() {
                    occurrences.push(ComparisonOccurrence {
                        pointer: pointer.to_owned(),
                        before: Some(Value::Array(Vec::new())),
                        after: Some(Value::Array(Vec::new())),
                    });
                } else {
                    for index in 0..before.len().max(after.len()) {
                        visit(
                            before.get(index),
                            after.get(index),
                            &format!("{pointer}/{index}"),
                            occurrences,
                        )?;
                    }
                }
            }
            (Some(Value::Object(before)), Some(Value::Object(after))) => {
                if before.is_empty() && after.is_empty() {
                    occurrences.push(ComparisonOccurrence {
                        pointer: pointer.to_owned(),
                        before: Some(Value::Object(Map::new())),
                        after: Some(Value::Object(Map::new())),
                    });
                } else {
                    let keys = before
                        .keys()
                        .chain(after.keys())
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    for key in keys {
                        let escaped = key.replace('~', "~0").replace('/', "~1");
                        visit(
                            before.get(&key),
                            after.get(&key),
                            &format!("{pointer}/{escaped}"),
                            occurrences,
                        )?;
                    }
                }
            }
            (Some(before), Some(after)) => occurrences.push(ComparisonOccurrence {
                pointer: pointer.to_owned(),
                before: Some(before.clone()),
                after: Some(after.clone()),
            }),
            (Some(before), None) => push_present_subtree(before, pointer, true, occurrences),
            (None, Some(after)) => push_present_subtree(after, pointer, false, occurrences),
            (None, None) => {}
        }
        Ok(())
    }

    let mut occurrences = Vec::new();
    visit(Some(before), Some(after), "", &mut occurrences)?;
    Ok(occurrences)
}

const CHAT_CRITICAL_ID_POINTERS: &[&str] = &[
    "/messages/*/tool_calls/*/id",
    "/messages/*/tool_call_id",
    "/tool_calls/*/id",
];
const RESPONSES_CRITICAL_ID_POINTERS: &[&str] = &[
    "/input/*/call_id",
    "/input/*/id",
    "/output/*/call_id",
    "/output/*/id",
];
const ANTHROPIC_CRITICAL_ID_POINTERS: &[&str] = &[
    "/messages/*/content/*/id",
    "/messages/*/content/*/tool_use_id",
];

fn is_critical_pointer(dialects: &BTreeSet<String>, pointer: &str) -> bool {
    if pointer.is_empty() {
        // The synthetic root pointer denotes a wholly missing semantic
        // request, which necessarily removes its critical routing/content.
        return true;
    }
    let general = ["/model", "/messages", "/input", "/tools", "/tool_choice"];
    if general
        .iter()
        .any(|prefix| pointer == *prefix || pointer.starts_with(&format!("{prefix}/")))
    {
        return true;
    }
    dialects.iter().any(|dialect| {
        let patterns = match dialect.as_str() {
            "openai-chat-completions" => CHAT_CRITICAL_ID_POINTERS,
            "openai-responses" => RESPONSES_CRITICAL_ID_POINTERS,
            "anthropic-messages" => ANTHROPIC_CRITICAL_ID_POINTERS,
            _ => &[],
        };
        patterns
            .iter()
            .any(|pattern| pointer_pattern_matches(pointer, pattern))
    })
}

fn normalized_stream_sha256(
    requests: &BTreeMap<SemanticRequestKey, ComparableRequest>,
) -> Result<String, String> {
    let mut hasher = Sha256::new();
    for request in requests.values() {
        let bytes = serde_json::to_vec(&request.canonical)
            .map_err(|error| format!("serialize normalized request stream: {error}"))?;
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}

fn attempt_multiplicity(
    requests: &BTreeMap<SemanticRequestKey, ComparableRequest>,
) -> Result<BTreeMap<SemanticAttemptKey, u64>, String> {
    let mut attempts = BTreeMap::<SemanticAttemptKey, (BTreeSet<u64>, BTreeSet<u64>)>::new();
    for (key, request) in requests {
        let (observed, declared_totals) = attempts.entry(attempt_key(key)).or_default();
        observed.insert(key.attempt);
        declared_totals.insert(request.semantic_attempts_total);
    }
    let mut multiplicity = BTreeMap::new();
    for (key, (observed, declared_totals)) in attempts {
        let Some(&declared_total) = declared_totals.iter().next() else {
            return Err("semantic request attempt total is absent".to_owned());
        };
        if declared_totals.len() != 1 || declared_total == 0 {
            return Err(format!(
                "semantic request {}:{}:{}:{} has inconsistent declared attempt totals {declared_totals:?}",
                key.scenario, key.actor, key.checkpoint, key.semantic_ordinal,
            ));
        }
        let expected = (1..=declared_total).collect::<BTreeSet<_>>();
        if observed != expected {
            return Err(format!(
                "semantic request {}:{}:{}:{} has physical attempts {observed:?}, expected 1..={declared_total}",
                key.scenario, key.actor, key.checkpoint, key.semantic_ordinal,
            ));
        }
        multiplicity.insert(key, declared_total);
    }
    Ok(multiplicity)
}

fn collector_complete_by_run(runs: &[DeterminismRun]) -> BTreeMap<String, bool> {
    runs.iter()
        .map(|run| (run.run.to_string(), run.request_collector_complete))
        .collect()
}

fn cross_run_incomplete(
    detail: String,
    runs: &[DeterminismRun],
) -> CrossRunReproducibilityEvaluation {
    CrossRunReproducibilityEvaluation {
        identical: false,
        request_stream_count: 0,
        attempt_count: 0,
        details: json!({
            "stream_sha256_by_run": {},
            "first_difference": null,
            "collector_complete_by_run": collector_complete_by_run(runs),
        }),
        measurement_complete: false,
        measurement_error: Some(detail),
    }
}

fn first_stream_difference(
    baseline: &BTreeMap<SemanticRequestKey, ComparableRequest>,
    comparison: &BTreeMap<SemanticRequestKey, ComparableRequest>,
    baseline_attempts: &BTreeMap<SemanticAttemptKey, u64>,
    comparison_attempts: &BTreeMap<SemanticAttemptKey, u64>,
    comparison_run: u32,
) -> Option<Value> {
    let attempt_keys = baseline_attempts
        .keys()
        .chain(comparison_attempts.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in attempt_keys {
        let before = baseline_attempts.get(&key).copied();
        let after = comparison_attempts.get(&key).copied();
        match (before, after) {
            (Some(_), None) => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "missing-request",
                    "semantic_key": attempt_key_json(&key),
                    "baseline": "present",
                    "comparison": "missing",
                }));
            }
            (None, Some(_)) => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "added-request",
                    "semantic_key": attempt_key_json(&key),
                    "baseline": "missing",
                    "comparison": "present",
                }));
            }
            (Some(before), Some(after)) if before != after => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "attempt-multiplicity",
                    "semantic_key": attempt_key_json(&key),
                    "baseline_attempts": before,
                    "comparison_attempts": after,
                }));
            }
            _ => {}
        }
    }
    let keys = baseline
        .keys()
        .chain(comparison.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        let before = baseline.get(&key);
        let after = comparison.get(&key);
        match (before, after) {
            (Some(before), Some(after)) if before.canonical == after.canonical => {}
            (Some(_), Some(_)) => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "canonical-body",
                    "semantic_key": semantic_key_json(&key),
                }));
            }
            (Some(_), None) => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "missing-request",
                    "semantic_key": semantic_key_json(&key),
                    "baseline": "present",
                    "comparison": "missing",
                }));
            }
            (None, Some(_)) => {
                return Some(json!({
                    "run": comparison_run,
                    "kind": "added-request",
                    "semantic_key": semantic_key_json(&key),
                    "baseline": "missing",
                    "comparison": "present",
                }));
            }
            (None, None) => {}
        }
    }
    None
}

/// Evaluate row 64 from the same isolated runs and exact normalization used by row 63.
///
/// `BTreeMap<SemanticRequestKey, _>` supplies the complete concurrency-safe
/// semantic order `(scenario, actor, semantic_ordinal, checkpoint, attempt)`;
/// provider arrival timestamps never participate.
pub fn evaluate_cross_run_reproducibility(
    runs: &[DeterminismRun],
    expected_runs: u32,
) -> CrossRunReproducibilityEvaluation {
    if runs.len() != expected_runs as usize
        || runs
            .iter()
            .enumerate()
            .any(|(index, run)| run.run != index as u32 + 1)
    {
        return cross_run_incomplete(
            format!(
                "expected ordered runs 1..={expected_runs}, observed {} run(s)",
                runs.len()
            ),
            runs,
        );
    }
    let mut normalized_runs = Vec::with_capacity(runs.len());
    let mut multiplicities = Vec::with_capacity(runs.len());
    let mut hashes = BTreeMap::new();
    for run in runs {
        let requests = match comparable_requests(run) {
            Ok(requests) => requests,
            Err(detail) => return cross_run_incomplete(detail, runs),
        };
        let attempts = match attempt_multiplicity(&requests) {
            Ok(attempts) => attempts,
            Err(detail) => return cross_run_incomplete(detail, runs),
        };
        let hash = match normalized_stream_sha256(&requests) {
            Ok(hash) => hash,
            Err(detail) => return cross_run_incomplete(detail, runs),
        };
        hashes.insert(run.run.to_string(), hash);
        normalized_runs.push(requests);
        multiplicities.push(attempts);
    }
    let Some(baseline) = normalized_runs.first() else {
        return cross_run_incomplete("cross-run-reproducibility has no baseline".to_owned(), runs);
    };
    let Some(baseline_attempts) = multiplicities.first() else {
        return cross_run_incomplete(
            "cross-run-reproducibility has no baseline attempt map".to_owned(),
            runs,
        );
    };
    if normalized_runs.iter().all(BTreeMap::is_empty) {
        return cross_run_incomplete(
            "all complete runs have empty normalized request streams".to_owned(),
            runs,
        );
    }
    let mut first_difference = None;
    for (index, comparison) in normalized_runs.iter().enumerate().skip(1) {
        let comparison_run = index as u32 + 1;
        if let Some(difference) = first_stream_difference(
            baseline,
            comparison,
            baseline_attempts,
            &multiplicities[index],
            comparison_run,
        ) {
            first_difference = Some(difference);
            break;
        }
    }
    let baseline_hash = hashes.get("1");
    let hashes_identical = baseline_hash.is_some()
        && hashes
            .values()
            .all(|comparison_hash| Some(comparison_hash) == baseline_hash);
    let identical = first_difference.is_none()
        && normalized_runs
            .iter()
            .all(|requests| requests.len() == baseline.len())
        && multiplicities
            .iter()
            .all(|attempts| attempts == baseline_attempts)
        && hashes_identical;
    CrossRunReproducibilityEvaluation {
        identical,
        request_stream_count: runs.len() as u64,
        attempt_count: baseline.len() as u64,
        details: json!({
            "stream_sha256_by_run": hashes,
            "first_difference": first_difference,
            "collector_complete_by_run": collector_complete_by_run(runs),
        }),
        measurement_complete: true,
        measurement_error: None,
    }
}

fn incomplete(detail: String, runs: &[DeterminismRun]) -> NondeterministicFieldEvaluation {
    NondeterministicFieldEvaluation {
        score: 0.0,
        comparable_leaf_occurrences: 0,
        varying_leaf_occurrences: 0,
        varying_pointer_count: 0,
        varying_critical_field_count: 0,
        details: json!({
            "varying_fields": [],
            "run_hashes": [],
            "collector_complete_by_run": collector_complete_by_run(runs),
        }),
        measurement_complete: false,
        reference_envelope_pass: false,
        measurement_error: Some(detail),
    }
}

/// Evaluate row 63 from fresh isolated canonical-request executions.
pub fn evaluate_nondeterministic_fields(
    runs: &[DeterminismRun],
    expected_runs: u32,
) -> NondeterministicFieldEvaluation {
    if runs.len() != expected_runs as usize
        || runs
            .iter()
            .enumerate()
            .any(|(index, run)| run.run != index as u32 + 1)
    {
        return incomplete(
            format!(
                "expected ordered runs 1..={expected_runs}, observed {} run(s)",
                runs.len()
            ),
            runs,
        );
    }
    let mut normalized_runs = Vec::with_capacity(runs.len());
    for run in runs {
        match comparable_requests(run) {
            Ok(requests) => normalized_runs.push(requests),
            Err(detail) => return incomplete(detail, runs),
        }
    }
    let mut run_hashes = Vec::with_capacity(normalized_runs.len());
    for requests in &normalized_runs {
        match normalized_stream_sha256(requests) {
            Ok(hash) => run_hashes.push(hash),
            Err(detail) => return incomplete(detail, runs),
        }
    }
    let Some(baseline) = normalized_runs.first() else {
        return incomplete(
            "nondeterministic-field-report has no baseline run".to_owned(),
            runs,
        );
    };
    let mut comparable = 0_u64;
    let mut varying = 0_u64;
    let mut fields = BTreeMap::<String, VaryingField>::new();
    let mut all_comparisons_pass = true;
    for (comparison_index, comparison) in normalized_runs.iter().enumerate().skip(1) {
        let comparison_run = comparison_index as u32 + 1;
        let mut comparison_comparable = 0_u64;
        let mut comparison_varying = 0_u64;
        let mut comparison_has_critical_variation = false;
        let keys = baseline
            .keys()
            .chain(comparison.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in keys {
            let before_request = baseline.get(&key);
            let after_request = comparison.get(&key);
            let mut dialects = BTreeSet::new();
            if let Some(request) = before_request {
                dialects.insert(request.dialect.clone());
            }
            if let Some(request) = after_request {
                dialects.insert(request.dialect.clone());
            }
            let (Some(before_request), Some(after_request)) = (before_request, after_request)
            else {
                // A wholly missing semantic request contributes exactly one
                // denominator occurrence, rather than zero or every present leaf.
                comparable = comparable.saturating_add(1);
                varying = varying.saturating_add(1);
                comparison_comparable = comparison_comparable.saturating_add(1);
                comparison_varying = comparison_varying.saturating_add(1);
                comparison_has_critical_variation = true;
                let field = fields.entry(String::new()).or_default();
                field.occurrences = field.occurrences.saturating_add(1);
                field.comparison_runs.insert(comparison_run);
                field.before_types.insert(
                    if before_request.is_some() {
                        "object"
                    } else {
                        "missing"
                    }
                    .to_owned(),
                );
                field.after_types.insert(
                    if after_request.is_some() {
                        "object"
                    } else {
                        "missing"
                    }
                    .to_owned(),
                );
                field.dialects.extend(dialects);
                continue;
            };
            let occurrences =
                match comparison_occurrences(&before_request.canonical, &after_request.canonical) {
                    Ok(occurrences) => occurrences,
                    Err(detail) => return incomplete(detail, runs),
                };
            for occurrence in occurrences {
                comparable = comparable.saturating_add(1);
                comparison_comparable = comparison_comparable.saturating_add(1);
                let before_value = occurrence.before.as_ref();
                let after_value = occurrence.after.as_ref();
                if before_value == after_value {
                    continue;
                }
                varying = varying.saturating_add(1);
                comparison_varying = comparison_varying.saturating_add(1);
                let critical_pointer = is_critical_pointer(&dialects, &occurrence.pointer);
                let field = fields.entry(occurrence.pointer).or_default();
                field.occurrences = field.occurrences.saturating_add(1);
                field.comparison_runs.insert(comparison_run);
                field
                    .before_types
                    .insert(json_type(before_value).to_owned());
                field.after_types.insert(json_type(after_value).to_owned());
                field.dialects.extend(dialects.iter().cloned());
                comparison_has_critical_variation |= critical_pointer;
            }
        }
        if comparison_comparable == 0 {
            return incomplete(
                format!(
                    "nondeterministic-field-report comparison run {comparison_run} has a zero comparable leaf denominator"
                ),
                runs,
            );
        }
        let comparison_score = 1.0 - comparison_varying as f64 / comparison_comparable as f64;
        all_comparisons_pass &= comparison_score >= 0.99 && !comparison_has_critical_variation;
    }
    if comparable == 0 {
        return incomplete(
            "nondeterministic-field-report comparable leaf denominator is zero".to_owned(),
            runs,
        );
    }
    let critical = fields
        .iter()
        .filter(|(pointer, field)| is_critical_pointer(&field.dialects, pointer))
        .count() as u64;
    let varying_fields = fields
        .iter()
        .map(|(pointer, field)| {
            json!({
                "pointer": pointer,
                "occurrences": field.occurrences,
                "comparison_runs": field.comparison_runs,
                "before_types": field.before_types,
                "after_types": field.after_types,
                "dialects": field.dialects,
            })
        })
        .collect::<Vec<_>>();
    let score = 1.0 - varying as f64 / comparable as f64;
    NondeterministicFieldEvaluation {
        score,
        comparable_leaf_occurrences: comparable,
        varying_leaf_occurrences: varying,
        varying_pointer_count: fields.len() as u64,
        varying_critical_field_count: critical,
        details: json!({
            "varying_fields": varying_fields,
            "run_hashes": run_hashes,
            "collector_complete_by_run": collector_complete_by_run(runs),
        }),
        measurement_complete: true,
        reference_envelope_pass: all_comparisons_pass,
        measurement_error: None,
    }
}

#[cfg(test)]
mod pointer_pattern_tests {
    use super::*;

    #[test]
    fn wildcard_is_one_segment_and_container_matching_is_prefix_based() {
        assert!(pointer_pattern_matches(
            "/messages/0/content/nested/value",
            "/messages/*/content"
        ));
        assert!(!pointer_pattern_matches(
            "/messages/0/wrapper/content",
            "/messages/*/content"
        ));
        assert!(!pointer_pattern_matches(
            "/messages/content",
            "/messages/*/content"
        ));
    }

    #[test]
    fn pattern_parser_rejects_bad_escapes_and_supports_literal_star() {
        assert!(parse_pointer_pattern("messages/*").is_err());
        assert!(parse_pointer_pattern("/messages/~3").is_err());
        assert!(pointer_pattern_matches("/metadata/*", "/metadata/~2"));
    }
}
