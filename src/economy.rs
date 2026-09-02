//! Pure fake-provider-side analysis for the AHRB harness-economy pillar.

use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::ModelRequestRecord;
use crate::workflow::Workflow;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Stable economy-task and analysis schema.
pub const ECONOMY_SCHEMA_VERSION: u32 = 1;
/// Stable identifier for the single standardized MVP task.
pub const ECONOMY_TASK_ID: &str = "ahrb-harness-economy-mvp-v1";
/// Human-readable reference-token label. This is intentionally not a bill.
pub const REFERENCE_TOKEN_LABEL: &str = "reference tokens (o200k_base-style)";
/// Pinned tokenizer implementation/vocabulary version.
pub const REFERENCE_TOKENIZER_VERSION: &str = "ahrb-o200k-base-style-bpe-v1";
/// Fixed neutral tariff applied to request/reference tokens only.
pub const REFERENCE_TARIFF_USD_PER_MILLION_TOKENS: f64 = 10.0;
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

/// The six-column economy result plus the metadata needed to audit each value.
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
    for record in &ordered {
        let body = serde_json::to_vec(&record.request.canonical)?;
        let tokens = tokenizer.count(&body)?;
        total_reference_tokens = total_reference_tokens.checked_add(tokens).ok_or_else(|| {
            AhrbError::Protocol("economy reference-token total overflow".to_owned())
        })?;
        if record.role == "primary" {
            model_turns = model_turns.saturating_add(1);
            last_context_size_tokens = last_context_size_tokens.max(tokens);
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
    })
}

/// Render the deterministic one-line block printed by the economy subcommand.
pub fn render_summary(summary: &EconomySummary) -> String {
    format!(
        "economy_summary task={} model_turns={} total_reference_tokens={} tool_calls={} tool_batching_factor={:.6} last_context_size_tokens={} completion={} completion_label=\"{}\" reference_cost_usd={:.8} tokens_per_completed_task={} reference_tariff_usd_per_million_tokens={:.2} reference_tokenizer={} reference_vocabulary_sha256={}",
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
    )
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
        let text = std::str::from_utf8(bytes).map_err(|error| {
            AhrbError::Protocol(format!("canonical request body is not UTF-8: {error}"))
        })?;
        let mut tokens = 0_u64;
        let mut consumed = 0_usize;
        for found in self.pattern.find_iter(text) {
            if found.start() != consumed {
                return Err(AhrbError::Protocol(format!(
                    "reference tokenizer left bytes {}..{} unmatched",
                    consumed,
                    found.start()
                )));
            }
            tokens = tokens
                .checked_add(self.count_piece(found.as_str().as_bytes())?)
                .ok_or_else(|| AhrbError::Protocol("reference token count overflow".to_owned()))?;
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

    fn count_piece(&self, piece: &[u8]) -> Result<u64> {
        if piece.is_empty() {
            return Ok(0);
        }
        if self.ranks.contains_key(piece) {
            return Ok(1);
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
        u64::try_from(boundaries.len().saturating_sub(1))
            .map_err(|_| AhrbError::Protocol("reference token count does not fit u64".to_owned()))
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
        let summary = analyze(&workflow(), &records, &[], "quick", 8, false).expect("analyze");
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
