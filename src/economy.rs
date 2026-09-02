//! Pure fake-provider-side analysis for the AHRB harness-economy pillar.

use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{ModelRequestRecord, retry_attempt_count};
use crate::manifest::{TopologyFamily, topology_family};
use crate::workflow::Workflow;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Stable economy-task and analysis schema.
pub const ECONOMY_SCHEMA_VERSION: u32 = 2;
/// Stable identifier for the single standardized MVP task.
pub const ECONOMY_TASK_ID: &str = "ahrb-harness-economy-mvp-v1";
/// Human-readable reference-token label. This is intentionally not a bill.
pub const REFERENCE_TOKEN_LABEL: &str = "reference tokens (o200k_base-style)";
/// Pinned tokenizer implementation/vocabulary version.
pub const REFERENCE_TOKENIZER_VERSION: &str = "ahrb-o200k-base-style-bpe-v1";
/// Fixed neutral tariff applied to request/reference tokens only.
pub const REFERENCE_TARIFF_USD_PER_MILLION_TOKENS: f64 = 10.0;
/// Honest public label for the cache-prefix proxy.
pub const CACHE_ELIGIBLE_LABEL: &str = "cache-eligible fraction (prefix upper bound)";
/// Honest public label for block-level repeated context.
pub const REDUNDANT_TOKENS_LABEL: &str = "redundant reference tokens";
/// Honest public label for the ordered context measurement.
pub const CONTEXT_TOKEN_CURVE_LABEL: &str =
    "context token curve (reference tokens per primary request)";
/// Honest public label for invariant instruction and tool-schema overhead.
pub const FIXED_OVERHEAD_LABEL: &str = "fixed overhead / turn (reference tokens)";
/// Honest public label for the conservative scripted-effect oracle.
pub const WASTED_TOOL_CALL_LABEL: &str = "wasted tool calls (deterministic task-proven only)";
/// The in-repository BPE merge vocabulary. Single-byte tokens are normative and implicit.
const REFERENCE_VOCABULARY: &[u8] = include_bytes!("../assets/ahrb_o200k_base_style_v1.tiktoken");

/// Script-terminal classification. It describes orchestration behavior, not real task success.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EconomyCompletion {
    /// The harness consumed the scripted terminal within the primary-request budget.
    Completed,
    /// The harness emitted a failure/cancellation terminal before following the script.
    Aborted,
    /// No harness terminal and no positive loop evidence were observed.
    Stalled,
    /// A successful provider response was requested repeatedly at the same semantic route.
    Looped,
    /// The scripted terminal was not followed within the profile's primary-request budget.
    OverBudget,
}

/// Tokenizer identity echoed into every economy report so stored runs remain comparable.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReferenceTokenizerPin {
    /// Honest family label for this provider-neutral reference BPE.
    pub encoding: String,
    /// AHRB's immutable tokenizer contract version.
    pub version: String,
    /// SHA-256 of the exact in-repository merge vocabulary bytes.
    pub vocabulary_sha256: String,
    /// Number of implicit byte tokens plus pinned merge tokens.
    pub vocabulary_entries: u64,
}

/// The schema-2 economy result plus the metadata needed to audit each value.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EconomySummary {
    /// Economy summary schema.
    pub schema: u32,
    /// Single standardized task identifier.
    pub task: String,
    /// Quick or cert profile.
    pub profile: String,
    /// Maximum allowed primary model requests.
    pub turn_budget: u64,
    /// Pinned tokenizer identity.
    pub reference_tokenizer: ReferenceTokenizerPin,
    /// Honest label for `total_reference_tokens` and context sizes.
    pub reference_token_label: String,
    /// Count of primary model requests using the row-42 role classification.
    pub model_turns: u64,
    /// Reference BPE tokens summed over every captured request body.
    pub total_reference_tokens: u64,
    /// Tool calls actually emitted by accepted scripted responses.
    pub tool_calls: u64,
    /// Unique tool results observed in the captured request stream.
    pub tool_results: u64,
    /// Requests that first carried at least one tool result.
    pub tool_result_requests: u64,
    /// Mean new tool results per result-bearing following request.
    pub tool_batching_factor: f64,
    /// Reference-token size of the largest primary request (the carried-context peak).
    pub last_context_size_tokens: u64,
    /// Script-terminal classification.
    pub completion: EconomyCompletion,
    /// Honest interpretation of `completion`.
    pub completion_label: String,
    /// Fixed tariff used by `reference_cost_usd`.
    pub reference_tariff_usd_per_million_tokens: f64,
    /// `total_reference_tokens * reference_tariff / 1_000_000`.
    pub reference_cost_usd: f64,
    /// Total reference tokens when completed; null when no task completed.
    pub tokens_per_completed_task: Option<u64>,
    /// Consecutive-primary-request message-prefix reuse upper bound.
    #[serde(default)]
    pub cache_eligible_fraction: f64,
    /// Honest label: this is an eligibility upper bound, not a realized hit rate.
    #[serde(default)]
    pub cache_eligible_fraction_label: String,
    /// Harness-declared `cache_control` occurrences across primary requests.
    #[serde(default)]
    pub cache_control_breakpoints: u64,
    /// Ordered `cache_control` occurrence counts, one per primary request.
    #[serde(default)]
    pub cache_control_breakpoints_per_request: Vec<u64>,
    /// Honest interpretation of the separately reported declarations.
    #[serde(default)]
    pub cache_control_breakpoints_label: String,
    /// Topology-aware caveat for the cache-prefix proxy.
    #[serde(default)]
    pub cache_eligibility_note: String,
    /// Re-sent identical message/segment blocks, measured in reference tokens.
    #[serde(default)]
    pub redundant_tokens: u64,
    /// Honest reference-token label for `redundant_tokens`.
    #[serde(default)]
    pub redundant_tokens_label: String,
    /// Ordered reference-token count for every primary physical request.
    #[serde(default)]
    pub context_token_curve: Vec<u64>,
    /// Theil-Sen slope of `context_token_curve`, in reference tokens/request.
    #[serde(default)]
    pub context_token_curve_slope: f64,
    /// Honest reference-token label for the curve and its slope.
    #[serde(default)]
    pub context_token_curve_label: String,
    /// Cross-check between the curve's final point and the MVP peak field.
    #[serde(default)]
    pub context_token_curve_last_matches_last_context_size: bool,
    /// Invariant system/developer and tool-schema preamble in reference tokens.
    #[serde(default)]
    pub per_turn_fixed_overhead_tokens: u64,
    /// Honest reference-token label for fixed overhead.
    #[serde(default)]
    pub per_turn_fixed_overhead_tokens_label: String,
    /// Calls proven to have neither a workspace mutation nor a dependent result.
    #[serde(default)]
    pub wasted_tool_call_count: u64,
    /// Conservative interpretation of `wasted_tool_call_count`.
    #[serde(default)]
    pub wasted_tool_call_count_label: String,
    /// Physical retry attempts, attributed with row 42's attempt tracking.
    #[serde(default)]
    pub retry_attempts: u64,
    /// Reference request tokens sent by physical attempts after attempt one.
    #[serde(default)]
    pub retry_reference_tokens: u64,
    /// Honest interpretation of the retry attribution.
    #[serde(default)]
    pub retry_label: String,
}

impl EconomySummary {
    /// Whether this run followed the scripted terminal within budget.
    pub fn completed(&self) -> bool {
        self.completion == EconomyCompletion::Completed
    }
}

/// Analyze one economy workflow exclusively from fake-provider requests and normalized events.
pub fn analyze(
    workflow: &Workflow,
    records: &[ModelRequestRecord],
    events: &[NormalizedEvent],
    profile: &str,
    turn_budget: u64,
    collector_timed_out: bool,
    topology: &str,
) -> Result<EconomySummary> {
    let tokenizer = ReferenceTokenizer::load()?;
    let mut ordered = records.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        (left.received_ns, left.semantic_ordinal, left.attempt).cmp(&(
            right.received_ns,
            right.semantic_ordinal,
            right.attempt,
        ))
    });

    let mut total_reference_tokens = 0_u64;
    let mut last_context_size_tokens = 0_u64;
    let mut model_turns = 0_u64;
    let mut seen_tool_results = BTreeSet::new();
    let mut tool_result_requests = 0_u64;
    let mut primary_records = Vec::new();
    let mut context_token_curve = Vec::new();
    let mut retry_reference_tokens = 0_u64;
    for record in &ordered {
        let body = serde_json::to_vec(&record.request.canonical)?;
        let tokens = tokenizer.count(&body)?;
        total_reference_tokens = total_reference_tokens.checked_add(tokens).ok_or_else(|| {
            AhrbError::Protocol("economy reference-token total overflow".to_owned())
        })?;
        if record.attempt > 1 {
            retry_reference_tokens =
                retry_reference_tokens.checked_add(tokens).ok_or_else(|| {
                    AhrbError::Protocol("economy retry reference-token total overflow".to_owned())
                })?;
        }
        if record.role == "primary" {
            model_turns = model_turns.saturating_add(1);
            last_context_size_tokens = last_context_size_tokens.max(tokens);
            primary_records.push(*record);
            context_token_curve.push(tokens);
            let result_ids = tool_result_ids(&record.request.canonical);
            let new_results = result_ids
                .into_iter()
                .filter(|id| seen_tool_results.insert(id.clone()))
                .count() as u64;
            if new_results > 0 {
                tool_result_requests = tool_result_requests.saturating_add(1);
            }
        }
    }

    let tool_results = seen_tool_results.len() as u64;
    let tool_batching_factor = if tool_result_requests == 0 {
        0.0
    } else {
        tool_results as f64 / tool_result_requests as f64
    };
    let tool_calls = emitted_tool_calls(workflow, records);
    let scripted_terminal = records.iter().any(|record| {
        record.role == "primary"
            && record.accepted
            && record.request.scenario == workflow.scenario
            && record.request.checkpoint == "terminal"
    });
    let terminal_success = events
        .iter()
        .any(|event| event.event == EventVocab::TerminalSuccess);
    let terminal_abort = events.iter().any(|event| {
        matches!(
            event.event,
            EventVocab::TerminalFailure
                | EventVocab::TerminalCancelled
                | EventVocab::TerminalTimeout
        )
    });
    let successful_route_repeated = records.iter().any(|record| {
        record.role == "primary"
            && record.semantic_attempts_total > 1
            && record
                .response_status
                .is_some_and(|status| (200..300).contains(&status))
    });
    let completion =
        if model_turns > turn_budget || (!scripted_terminal && model_turns >= turn_budget) {
            EconomyCompletion::OverBudget
        } else if scripted_terminal && terminal_success {
            EconomyCompletion::Completed
        } else if successful_route_repeated {
            EconomyCompletion::Looped
        } else if collector_timed_out {
            EconomyCompletion::Stalled
        } else if terminal_abort {
            EconomyCompletion::Aborted
        } else {
            EconomyCompletion::Stalled
        };
    let tokens_per_completed_task =
        (completion == EconomyCompletion::Completed).then_some(total_reference_tokens);
    let reference_cost_usd =
        total_reference_tokens as f64 * REFERENCE_TARIFF_USD_PER_MILLION_TOKENS / 1_000_000.0;
    let cache_eligible_fraction = cache_eligible_fraction(&primary_records, &tokenizer)?;
    let cache_control_breakpoints_per_request = primary_records
        .iter()
        .map(|record| count_named_fields(&record.request.canonical, "cache_control"))
        .collect::<Vec<_>>();
    let cache_control_breakpoints = cache_control_breakpoints_per_request
        .iter()
        .try_fold(0_u64, |total, count| total.checked_add(*count))
        .ok_or_else(|| AhrbError::Protocol("cache_control count overflow".to_owned()))?;
    let redundant_tokens = redundant_tokens(&primary_records, &tokenizer)?;
    let context_token_curve_slope = theil_sen_by_index(&context_token_curve);
    let context_token_curve_last_matches_last_context_size =
        context_token_curve.last().copied().unwrap_or(0) == last_context_size_tokens;
    let per_turn_fixed_overhead_tokens =
        per_turn_fixed_overhead_tokens(&primary_records, &tokenizer)?;
    let wasted_tool_call_count = wasted_tool_call_count(workflow, &primary_records, events);
    let retry_attempts = retry_attempt_count(records);

    Ok(EconomySummary {
        schema: ECONOMY_SCHEMA_VERSION,
        task: ECONOMY_TASK_ID.to_owned(),
        profile: profile.to_owned(),
        turn_budget,
        reference_tokenizer: tokenizer.pin,
        reference_token_label: REFERENCE_TOKEN_LABEL.to_owned(),
        model_turns,
        total_reference_tokens,
        tool_calls,
        tool_results,
        tool_result_requests,
        tool_batching_factor,
        last_context_size_tokens,
        completion,
        completion_label: "followed scripted terminal".to_owned(),
        reference_tariff_usd_per_million_tokens: REFERENCE_TARIFF_USD_PER_MILLION_TOKENS,
        reference_cost_usd,
        tokens_per_completed_task,
        cache_eligible_fraction,
        cache_eligible_fraction_label: CACHE_ELIGIBLE_LABEL.to_owned(),
        cache_control_breakpoints,
        cache_control_breakpoints_per_request,
        cache_control_breakpoints_label:
            "harness-declared cache_control occurrences (separate from cache eligibility)"
                .to_owned(),
        cache_eligibility_note: cache_eligibility_note(topology),
        redundant_tokens,
        redundant_tokens_label: REDUNDANT_TOKENS_LABEL.to_owned(),
        context_token_curve,
        context_token_curve_slope,
        context_token_curve_label: CONTEXT_TOKEN_CURVE_LABEL.to_owned(),
        context_token_curve_last_matches_last_context_size,
        per_turn_fixed_overhead_tokens,
        per_turn_fixed_overhead_tokens_label: FIXED_OVERHEAD_LABEL.to_owned(),
        wasted_tool_call_count,
        wasted_tool_call_count_label: WASTED_TOOL_CALL_LABEL.to_owned(),
        retry_attempts,
        retry_reference_tokens,
        retry_label:
            "physical retries reported separately; retry request tokens remain in MVP totals"
                .to_owned(),
    })
}

/// Render the deterministic one-line block printed by the economy subcommand.
pub fn render_summary(summary: &EconomySummary) -> String {
    let context_token_curve = render_u64_array(&summary.context_token_curve);
    let cache_control_breakpoints =
        render_u64_array(&summary.cache_control_breakpoints_per_request);
    format!(
        "economy_summary schema={} task={} model_turns={} total_reference_tokens={} tool_calls={} tool_batching_factor={:.6} last_context_size_tokens={} completion={} completion_label=\"{}\" reference_cost_usd={:.8} tokens_per_completed_task={} reference_tariff_usd_per_million_tokens={:.2} reference_tokenizer={} reference_vocabulary_sha256={} cache_eligible_fraction={:.6} cache_eligible_label=\"{}\" cache_control_breakpoints={} cache_control_breakpoints_per_request={} redundant_tokens={} redundant_tokens_label=\"{}\" context_token_curve={} context_token_curve_slope={:.6} context_token_curve_label=\"{}\" per_turn_fixed_overhead_tokens={} per_turn_fixed_overhead_label=\"{}\" wasted_tool_call_count={} wasted_tool_call_label=\"{}\" retry_attempts={} retry_reference_tokens={}",
        summary.schema,
        summary.task,
        summary.model_turns,
        summary.total_reference_tokens,
        summary.tool_calls,
        summary.tool_batching_factor,
        summary.last_context_size_tokens,
        completion_name(&summary.completion),
        summary.completion_label,
        summary.reference_cost_usd,
        summary
            .tokens_per_completed_task
            .map_or_else(|| "unavailable".to_owned(), |value| value.to_string()),
        summary.reference_tariff_usd_per_million_tokens,
        summary.reference_tokenizer.version,
        summary.reference_tokenizer.vocabulary_sha256,
        summary.cache_eligible_fraction,
        summary.cache_eligible_fraction_label,
        summary.cache_control_breakpoints,
        cache_control_breakpoints,
        summary.redundant_tokens,
        summary.redundant_tokens_label,
        context_token_curve,
        summary.context_token_curve_slope,
        summary.context_token_curve_label,
        summary.per_turn_fixed_overhead_tokens,
        summary.per_turn_fixed_overhead_tokens_label,
        summary.wasted_tool_call_count,
        summary.wasted_tool_call_count_label,
        summary.retry_attempts,
        summary.retry_reference_tokens,
    )
}

fn render_u64_array(values: &[u64]) -> String {
    let values = values.iter().map(u64::to_string).collect::<Vec<_>>();
    format!("[{}]", values.join(","))
}

/// Stable lowercase spelling shared by stdout and Markdown.
pub fn completion_name(completion: &EconomyCompletion) -> &'static str {
    match completion {
        EconomyCompletion::Completed => "completed",
        EconomyCompletion::Aborted => "aborted",
        EconomyCompletion::Stalled => "stalled",
        EconomyCompletion::Looped => "looped",
        EconomyCompletion::OverBudget => "over-budget",
    }
}

fn emitted_tool_calls(workflow: &Workflow, records: &[ModelRequestRecord]) -> u64 {
    let scripted = workflow
        .responses
        .iter()
        .map(|response| {
            (
                (
                    response.scenario.as_str(),
                    response.actor.as_str(),
                    response.checkpoint.as_str(),
                ),
                response
                    .response
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .map_or(0_u64, |calls| calls.len() as u64),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    records
        .iter()
        .filter(|record| record.role == "primary" && record.accepted)
        .filter_map(|record| {
            let semantic = (
                record.request.scenario.as_str(),
                record.request.actor.as_str(),
                record.request.checkpoint.as_str(),
                record.semantic_ordinal,
            );
            seen.insert(semantic).then(|| {
                scripted
                    .get(&(
                        record.request.scenario.as_str(),
                        record.request.actor.as_str(),
                        record.request.checkpoint.as_str(),
                    ))
                    .copied()
                    .unwrap_or(0)
            })
        })
        .sum()
}

fn cache_eligibility_note(topology: &str) -> String {
    match topology_family(topology) {
        Some(TopologyFamily::PerInvocation) => format!(
            "{topology} is per-invocation and has no cross-turn server cache; this value describes repeated prefixes serialized within requests only, not realized cache reuse"
        ),
        Some(TopologyFamily::SharedController) => format!(
            "{topology} can preserve cross-turn process state, but this value remains a serialized-prefix upper bound, not an observed server-cache hit rate"
        ),
        None => format!(
            "{topology} has unknown cache persistence; this value is a serialized-prefix upper bound, not an observed server-cache hit rate"
        ),
    }
}

fn primary_message_array(record: &ModelRequestRecord) -> Value {
    let canonical = &record.request.canonical;
    let value = if record.request.dialect == "openai-responses" {
        canonical.get("input")
    } else {
        canonical.get("messages")
    };
    match value {
        Some(Value::Array(items)) => Value::Array(items.clone()),
        Some(Value::Null) | None => Value::Array(Vec::new()),
        Some(value) => Value::Array(vec![value.clone()]),
    }
}

fn cache_eligible_fraction(
    records: &[&ModelRequestRecord],
    tokenizer: &ReferenceTokenizer,
) -> Result<f64> {
    let mut previous = None::<Vec<Vec<u8>>>;
    let mut eligible = 0_u64;
    let mut denominator = 0_u64;
    for record in records {
        let serialized = serde_json::to_vec(&primary_message_array(record))?;
        let current = tokenizer.encode(&serialized)?;
        denominator = denominator
            .checked_add(u64::try_from(current.len()).map_err(|_| {
                AhrbError::Protocol("cache token count does not fit u64".to_owned())
            })?)
            .ok_or_else(|| AhrbError::Protocol("cache denominator overflow".to_owned()))?;
        if let Some(prior) = previous.as_ref() {
            let prefix = prior
                .iter()
                .zip(&current)
                .take_while(|(left, right)| left == right)
                .count();
            eligible = eligible
                .checked_add(u64::try_from(prefix).map_err(|_| {
                    AhrbError::Protocol("cache prefix count does not fit u64".to_owned())
                })?)
                .ok_or_else(|| AhrbError::Protocol("cache numerator overflow".to_owned()))?;
        }
        previous = Some(current);
    }
    Ok(if denominator == 0 {
        0.0
    } else {
        eligible as f64 / denominator as f64
    })
}

fn count_named_fields(value: &Value, field: &str) -> u64 {
    fn visit(value: &Value, field: &str, schema_name_map: bool) -> u64 {
        match value {
            Value::Array(items) => items.iter().fold(0_u64, |total, item| {
                total.saturating_add(visit(item, field, false))
            }),
            Value::Object(object) => object.iter().fold(0_u64, |total, (key, item)| {
                total
                    .saturating_add(u64::from(!schema_name_map && key == field))
                    .saturating_add(visit(
                        item,
                        field,
                        matches!(key.as_str(), "properties" | "$defs"),
                    ))
            }),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => 0,
        }
    }
    visit(value, field, false)
}

fn message_blocks(record: &ModelRequestRecord) -> Vec<Value> {
    let Value::Array(items) = primary_message_array(record) else {
        return Vec::new();
    };
    let mut blocks = Vec::new();
    for item in items {
        if let Some(segments) = item.get("content").and_then(Value::as_array) {
            blocks.extend(segments.iter().cloned());
        } else {
            blocks.push(item);
        }
    }
    blocks
}

fn redundant_tokens(
    records: &[&ModelRequestRecord],
    tokenizer: &ReferenceTokenizer,
) -> Result<u64> {
    let mut seen = BTreeSet::<Vec<u8>>::new();
    let mut total = 0_u64;
    for record in records {
        let blocks = message_blocks(record)
            .into_iter()
            .map(|block| serde_json::to_vec(&block))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for block in &blocks {
            if seen.contains(block) {
                total = total.checked_add(tokenizer.count(block)?).ok_or_else(|| {
                    AhrbError::Protocol("redundant reference-token total overflow".to_owned())
                })?;
            }
        }
        seen.extend(blocks);
    }
    Ok(total)
}

fn leading_instruction_messages(record: &ModelRequestRecord) -> Vec<Value> {
    let canonical = &record.request.canonical;
    match record.request.dialect.as_str() {
        "openai-responses" => canonical
            .get("instructions")
            .filter(|value| !value.is_null())
            .cloned()
            .into_iter()
            .collect(),
        "anthropic-messages" => match canonical.get("system") {
            Some(Value::Array(items)) => items.clone(),
            Some(value) if !value.is_null() => vec![value.clone()],
            Some(_) | None => Vec::new(),
        },
        _ => canonical
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| {
                messages
                    .iter()
                    .take_while(|message| {
                        matches!(
                            message.get("role").and_then(Value::as_str),
                            Some("system" | "developer")
                        )
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn common_value_prefix(sequences: &[Vec<Value>]) -> Vec<Value> {
    let Some(first) = sequences.first() else {
        return Vec::new();
    };
    let length = (0..first.len())
        .take_while(|index| {
            sequences
                .iter()
                .skip(1)
                .all(|sequence| sequence.get(*index) == first.get(*index))
        })
        .count();
    first[..length].to_vec()
}

fn per_turn_fixed_overhead_tokens(
    records: &[&ModelRequestRecord],
    tokenizer: &ReferenceTokenizer,
) -> Result<u64> {
    let Some(first) = records.first() else {
        return Ok(0);
    };
    if records
        .iter()
        .any(|record| record.request.dialect != first.request.dialect)
    {
        return Ok(0);
    }
    let instruction_sequences = records
        .iter()
        .map(|record| leading_instruction_messages(record))
        .collect::<Vec<_>>();
    let tool_sequences = records
        .iter()
        .map(|record| {
            record
                .request
                .canonical
                .get("tools")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let instructions = common_value_prefix(&instruction_sequences);
    let tools = common_value_prefix(&tool_sequences);
    if instructions.is_empty() && tools.is_empty() {
        return Ok(0);
    }
    let instruction_value = match first.request.dialect.as_str() {
        "openai-responses" => invariant_instruction_value(records, "instructions", instructions),
        "anthropic-messages" => invariant_instruction_value(records, "system", instructions),
        _ => Value::Array(instructions),
    };
    let envelope = match first.request.dialect.as_str() {
        "openai-responses" => json_object("instructions", instruction_value, tools),
        "anthropic-messages" => json_object("system", instruction_value, tools),
        _ => json_object("messages", instruction_value, tools),
    };
    tokenizer.count(&serde_json::to_vec(&envelope)?)
}

fn invariant_instruction_value(
    records: &[&ModelRequestRecord],
    field: &str,
    common: Vec<Value>,
) -> Value {
    let first = records
        .first()
        .and_then(|record| record.request.canonical.get(field));
    match first {
        Some(Value::Array(_)) => Value::Array(common),
        Some(value) if !value.is_null() && common.len() == 1 => value.clone(),
        Some(_) | None => Value::Null,
    }
}

fn json_object(instruction_key: &str, instructions: Value, tools: Vec<Value>) -> Value {
    let mut object = serde_json::Map::new();
    object.insert(instruction_key.to_owned(), instructions);
    object.insert("tools".to_owned(), Value::Array(tools));
    Value::Object(object)
}

fn event_call_id(event: &NormalizedEvent) -> Option<&str> {
    event
        .payload
        .get("call_id")
        .or_else(|| event.payload.pointer("/payload/call_id"))
        .and_then(Value::as_str)
}

fn successful_tool_result_ids(events: &[NormalizedEvent]) -> BTreeSet<&str> {
    events
        .iter()
        .filter(|event| event.event == EventVocab::ToolResult)
        .filter(|event| {
            event
                .payload
                .pointer("/result/ok")
                .or_else(|| event.payload.pointer("/payload/result/ok"))
                .and_then(Value::as_bool)
                == Some(true)
        })
        .filter_map(event_call_id)
        .collect()
}

fn scripted_call_semantic(call: &Value) -> Option<&str> {
    call.pointer("/_ahrb_native/semantic")
        .and_then(Value::as_str)
        .or_else(|| {
            call.get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.strip_suffix("_fixture"))
        })
}

fn wasted_tool_call_count(
    workflow: &Workflow,
    records: &[&ModelRequestRecord],
    events: &[NormalizedEvent],
) -> u64 {
    let responses = workflow
        .responses
        .iter()
        .map(|response| {
            (
                (
                    response.scenario.as_str(),
                    response.actor.as_str(),
                    response.checkpoint.as_str(),
                ),
                response
                    .response
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let successful_results = successful_tool_result_ids(events);
    let mut seen_responses = BTreeSet::new();
    let mut wasted = 0_u64;
    for record in records.iter().copied().filter(|record| record.accepted) {
        let semantic_response = (
            record.request.scenario.as_str(),
            record.request.actor.as_str(),
            record.request.checkpoint.as_str(),
            record.semantic_ordinal,
        );
        if !seen_responses.insert(semantic_response) {
            continue;
        }
        let response_key = (
            record.request.scenario.as_str(),
            record.request.actor.as_str(),
            record.request.checkpoint.as_str(),
        );
        let Some(calls) = responses.get(&response_key) else {
            continue;
        };
        for call in calls {
            let Some(call_id) = call.get("id").and_then(Value::as_str) else {
                continue;
            };
            if !successful_results.contains(call_id) {
                continue;
            }
            let mutated_workspace = scripted_call_semantic(call) == Some("write");
            let produced_dependent_effect = records.iter().any(|following| {
                following.semantic_ordinal > record.semantic_ordinal
                    && tool_result_ids(&following.request.canonical).contains(call_id)
            });
            if !mutated_workspace && !produced_dependent_effect {
                wasted = wasted.saturating_add(1);
            }
        }
    }
    wasted
}

fn theil_sen_by_index(values: &[u64]) -> f64 {
    let mut slopes = Vec::new();
    for left in 0..values.len() {
        for right in left.saturating_add(1)..values.len() {
            let distance = right.saturating_sub(left) as f64;
            slopes.push((values[right] as f64 - values[left] as f64) / distance);
        }
    }
    slopes.sort_by(f64::total_cmp);
    match slopes.len() {
        0 => 0.0,
        length if length % 2 == 1 => slopes[length / 2],
        length => (slopes[length / 2 - 1] + slopes[length / 2]) / 2.0,
    }
}

fn tool_result_ids(value: &Value) -> BTreeSet<String> {
    fn visit(value: &Value, output: &mut BTreeSet<String>) {
        match value {
            Value::Array(items) => {
                for item in items {
                    visit(item, output);
                }
            }
            Value::Object(object) => {
                let role_is_tool = object.get("role").and_then(Value::as_str) == Some("tool");
                let type_is_result = matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("tool_result" | "function_call_output")
                );
                if role_is_tool || type_is_result {
                    let id = ["tool_call_id", "tool_use_id", "call_id", "id"]
                        .into_iter()
                        .find_map(|field| object.get(field).and_then(Value::as_str));
                    if let Some(id) = id {
                        output.insert(id.to_owned());
                    }
                }
                for child in object.values() {
                    visit(child, output);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    let mut output = BTreeSet::new();
    visit(value, &mut output);
    output
}

struct ReferenceTokenizer {
    ranks: BTreeMap<Vec<u8>, u32>,
    pattern: regex::Regex,
    pin: ReferenceTokenizerPin,
}

impl ReferenceTokenizer {
    fn load() -> Result<Self> {
        let mut ranks = BTreeMap::new();
        // Every byte is an implicit base token. Merge ranks begin above this range.
        for byte in 0_u8..=u8::MAX {
            ranks.insert(vec![byte], u32::from(byte));
        }
        let text = std::str::from_utf8(REFERENCE_VOCABULARY).map_err(|error| {
            AhrbError::Protocol(format!(
                "reference tokenizer vocabulary is not UTF-8: {error}"
            ))
        })?;
        for (line_index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (encoded, rank) = line.split_once(' ').ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "reference tokenizer vocabulary line {} is malformed",
                    line_index.saturating_add(1)
                ))
            })?;
            let token = decode_base64(encoded).map_err(|error| {
                AhrbError::Protocol(format!(
                    "reference tokenizer vocabulary line {} has invalid base64: {error}",
                    line_index.saturating_add(1)
                ))
            })?;
            let rank = rank.parse::<u32>().map_err(|_| {
                AhrbError::Protocol(format!(
                    "reference tokenizer vocabulary line {} has an invalid rank",
                    line_index.saturating_add(1)
                ))
            })?;
            if token.len() < 2 {
                return Err(AhrbError::Protocol(format!(
                    "reference tokenizer merge line {} must contain at least two bytes",
                    line_index.saturating_add(1)
                )));
            }
            if ranks.insert(token, rank).is_some() {
                return Err(AhrbError::Protocol(format!(
                    "reference tokenizer vocabulary line {} duplicates a token",
                    line_index.saturating_add(1)
                )));
            }
        }
        let pattern = regex::Regex::new(concat!(
            r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
            "|",
            r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
            "|",
            r"\p{N}{1,3}",
            "|",
            r" ?[^\s\p{L}\p{N}]+[\r\n/]*",
            "|",
            r"\s*[\r\n]+",
            "|",
            r"\s+"
        ))
        .map_err(|error| AhrbError::Protocol(format!("compile reference tokenizer: {error}")))?;
        let vocabulary_sha256 = format!("{:x}", Sha256::digest(REFERENCE_VOCABULARY));
        Ok(Self {
            pin: ReferenceTokenizerPin {
                encoding: "o200k_base-style".to_owned(),
                version: REFERENCE_TOKENIZER_VERSION.to_owned(),
                vocabulary_sha256,
                vocabulary_entries: ranks.len() as u64,
            },
            ranks,
            pattern,
        })
    }

    fn count(&self, bytes: &[u8]) -> Result<u64> {
        u64::try_from(self.encode(bytes)?.len())
            .map_err(|_| AhrbError::Protocol("reference token count does not fit u64".to_owned()))
    }

    fn encode(&self, bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
        let text = std::str::from_utf8(bytes).map_err(|error| {
            AhrbError::Protocol(format!("canonical request body is not UTF-8: {error}"))
        })?;
        let mut tokens = Vec::new();
        let mut consumed = 0_usize;
        for found in self.pattern.find_iter(text) {
            if found.start() != consumed {
                return Err(AhrbError::Protocol(format!(
                    "reference tokenizer left bytes {}..{} unmatched",
                    consumed,
                    found.start()
                )));
            }
            tokens.extend(self.encode_piece(found.as_str().as_bytes())?);
            consumed = found.end();
        }
        if consumed != text.len() {
            return Err(AhrbError::Protocol(format!(
                "reference tokenizer left bytes {consumed}..{} unmatched",
                text.len()
            )));
        }
        Ok(tokens)
    }

    fn encode_piece(&self, piece: &[u8]) -> Result<Vec<Vec<u8>>> {
        if piece.is_empty() {
            return Ok(Vec::new());
        }
        if self.ranks.contains_key(piece) {
            return Ok(vec![piece.to_vec()]);
        }
        let mut boundaries = (0..=piece.len()).collect::<Vec<_>>();
        while boundaries.len() > 2 {
            let candidate = boundaries
                .windows(3)
                .enumerate()
                .filter_map(|(index, points)| {
                    self.ranks
                        .get(&piece[points[0]..points[2]])
                        .copied()
                        .map(|rank| (rank, index))
                })
                .min();
            let Some((_, index)) = candidate else {
                break;
            };
            boundaries.remove(index.saturating_add(1));
        }
        Ok(boundaries
            .windows(2)
            .map(|points| piece[points[0]..points[1]].to_vec())
            .collect())
    }
}

fn decode_base64(encoded: &str) -> std::result::Result<Vec<u8>, &'static str> {
    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    if encoded.is_empty() || encoded.len() % 4 != 0 {
        return Err("length is not a positive multiple of four");
    }
    let bytes = encoded.as_bytes();
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks_exact(4).enumerate() {
        let final_chunk = chunk_index == bytes.len() / 4 - 1;
        let padding = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        if padding > 2 || (!final_chunk && padding != 0) {
            return Err("padding is invalid");
        }
        let a = value(chunk[0]).ok_or("alphabet is invalid")?;
        let b = value(chunk[1]).ok_or("alphabet is invalid")?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            value(chunk[2]).ok_or("alphabet is invalid")?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            value(chunk[3]).ok_or("alphabet is invalid")?
        };
        output.push((a << 2) | (b >> 4));
        if padding < 2 {
            output.push((b << 4) | (c >> 2));
        }
        if padding == 0 {
            output.push((c << 6) | d);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_model::{ModelRequest, ModelRequestRecord};
    use crate::workflow::{Actor, ScriptedResponse, WORKFLOW_SCHEMA_VERSION};
    use serde_json::json;

    fn workflow() -> Workflow {
        Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario: "economy-test".to_owned(),
            actors: BTreeMap::from([(
                "agent".to_owned(),
                Actor {
                    id: "agent".to_owned(),
                    parent: None,
                    prompt: "test".to_owned(),
                    workspace: ".".to_owned(),
                },
            )]),
            barriers: BTreeMap::new(),
            responses: vec![ScriptedResponse {
                scenario: "economy-test".to_owned(),
                actor: "agent".to_owned(),
                checkpoint: "terminal".to_owned(),
                request_hash: String::new(),
                response: json!({"tool_calls":[{"id":"call-1"}]}),
                fault: None,
                barrier: None,
            }],
        }
    }

    fn record(canonical: Value) -> ModelRequestRecord {
        ModelRequestRecord {
            request: ModelRequest {
                dialect: "openai-chat-completions".to_owned(),
                endpoint: "/v1/chat/completions".to_owned(),
                model: "mock".to_owned(),
                scenario: "economy-test".to_owned(),
                actor: "agent".to_owned(),
                checkpoint: "terminal".to_owned(),
                canonical,
                credential_fingerprint: "redacted".to_owned(),
                stream: false,
            },
            canonical_hash: "hash".to_owned(),
            attempts: 1,
            accepted: true,
            semantic_ordinal: 1,
            attempt: 1,
            received_ns: 1,
            body_bytes: 1,
            input_tokens: None,
            role: "primary".to_owned(),
            side_channel_kind: None,
            response_status: Some(200),
            response_headers_ns: Some(2),
            response_first_frame_yield_ns: Some(3),
            response_last_frame_yield_ns: Some(4),
            semantic_attempts_total: 1,
        }
    }

    #[test]
    fn batching_counts_only_first_occurrence_of_accumulated_results() {
        let records = vec![
            record(
                json!({"messages":[{"role":"tool","tool_call_id":"a"},{"role":"tool","tool_call_id":"b"}]}),
            ),
            ModelRequestRecord {
                request: ModelRequest {
                    canonical: json!({"messages":[{"role":"tool","tool_call_id":"a"},{"role":"tool","tool_call_id":"b"},{"role":"tool","tool_call_id":"c"}]}),
                    checkpoint: "later".to_owned(),
                    ..record(json!({})).request
                },
                semantic_ordinal: 2,
                received_ns: 2,
                ..record(json!({}))
            },
        ];
        let summary = analyze(
            &workflow(),
            &records,
            &[],
            "quick",
            8,
            false,
            "shared-daemon-sessions",
        )
        .expect("analyze");
        assert_eq!(summary.tool_results, 3);
        assert_eq!(summary.tool_result_requests, 2);
        assert_eq!(summary.tool_batching_factor, 1.5);
    }

    #[test]
    fn tokenizer_pin_and_counts_are_stable() {
        let tokenizer = ReferenceTokenizer::load().expect("load tokenizer");
        assert_eq!(tokenizer.pin.version, REFERENCE_TOKENIZER_VERSION);
        assert_eq!(
            tokenizer
                .count(br#"{\"model\":\"reference\"}"#)
                .expect("count"),
            21
        );
        assert_eq!(tokenizer.pin.vocabulary_entries, 272);
    }
}
