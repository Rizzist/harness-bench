//! Wave-4 automation-interface evidence evaluators.

use crate::events::{EventVocab, NormalizedEvent};
use crate::manifest::{EventMetadata, UsageScope};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Common result of one Wave-4 evidence evaluator.
#[derive(Clone, Debug, Default)]
pub struct Wave4Evaluation {
    /// Exact report metric names and values.
    pub metrics: BTreeMap<String, f64>,
    /// Typed report detail block.
    pub details: Value,
    /// Whether all required evidence carriers were readable.
    pub measurement_complete: bool,
    /// Infrastructure/evidence error, when incomplete.
    pub measurement_error: Option<String>,
    /// Normative row oracle.
    pub passed: bool,
}

fn event_name(event: &EventVocab) -> Option<String> {
    serde_json::to_value(event)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
}

fn integer_at(value: &Value, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(Value::as_u64)
}

fn usage_fields(value: &Value, metadata: &EventMetadata) -> Option<[u64; 5]> {
    Some([
        integer_at(value, &metadata.input_tokens_pointer)?,
        integer_at(value, &metadata.output_tokens_pointer)?,
        integer_at(value, &metadata.total_tokens_pointer)?,
        integer_at(value, &metadata.cost_microusd_pointer)?,
        integer_at(value, &metadata.turns_pointer)?,
    ])
}

fn raw_event(event: &NormalizedEvent) -> Value {
    event
        .payload
        .get("_ahrb_source_raw")
        .cloned()
        .unwrap_or_else(|| serde_json::to_value(event).unwrap_or(Value::Null))
}

fn usage_actor(repetition: u32, turn: u32) -> String {
    format!("r67p{repetition}t{turn}")
}

fn actor_matches(event: &NormalizedEvent, actor: &str) -> bool {
    event.actor == actor || event.actor.rsplit(':').next() == Some(actor)
}

/// Evaluate row 67 from the unmodified normalized-event envelopes retained by
/// the reference driver. JSON pointers are resolved against their serialized
/// raw envelopes, not against an AHRB-synthesized usage object.
pub fn evaluate_usage_reporting(
    events: &[NormalizedEvent],
    metadata: Option<&EventMetadata>,
    repetitions: u32,
    turns_per_repetition: u32,
) -> Wave4Evaluation {
    let Some(metadata) = metadata else {
        return Wave4Evaluation {
            measurement_error: Some("events.metadata is absent".to_owned()),
            ..Wave4Evaluation::default()
        };
    };
    let Some(scope) = metadata.usage_scope else {
        return Wave4Evaluation {
            measurement_error: Some("events.metadata.usage_scope is absent".to_owned()),
            ..Wave4Evaluation::default()
        };
    };
    let expected = if turns_per_repetition == 3 {
        [440_u64, 90, 530, 1_150, 3]
    } else if turns_per_repetition == 20 {
        [2_140_u64, 430, 2_570, 5_570, 20]
    } else {
        return Wave4Evaluation {
            measurement_error: Some(format!(
                "unsupported row-67 turn count {turns_per_repetition}"
            )),
            ..Wave4Evaluation::default()
        };
    };
    let mut repetition_details = Vec::new();
    let mut complete = true;
    let mut exact_repetitions = true;
    let mut one_carrier_per_turn = true;
    let mut tool_turn_two_response_sum = true;
    let mut crosscheck_errors = 0_u64;
    let mut totals_by_repetition = Vec::new();
    for repetition in 1..=repetitions {
        let mut turn_values = Vec::new();
        let mut running = [0_u64; 5];
        let mut prior = [0_u64; 5];
        let mut response_values = Vec::new();
        for turn in 1..=turns_per_repetition {
            let actor = usage_actor(repetition, turn);
            let carriers = events
                .iter()
                .filter(|event| {
                    actor_matches(event, &actor)
                        && event_name(&event.event).as_deref()
                            == Some(metadata.usage_event.as_str())
                })
                .collect::<Vec<_>>();
            if carriers.len() != 1 {
                complete = false;
                one_carrier_per_turn = false;
                continue;
            }
            let raw = raw_event(carriers[0]);
            let Some(values) = usage_fields(&raw, metadata) else {
                complete = false;
                continue;
            };
            if values[2] != values[0].saturating_add(values[1]) {
                crosscheck_errors = crosscheck_errors.saturating_add(1);
            }
            let per_turn = match scope {
                UsageScope::Turn => values,
                UsageScope::CumulativeRun => {
                    if values
                        .iter()
                        .zip(prior)
                        .any(|(current, previous)| *current < previous)
                    {
                        crosscheck_errors = crosscheck_errors.saturating_add(1);
                    }
                    let delta =
                        std::array::from_fn(|index| values[index].saturating_sub(prior[index]));
                    prior = values;
                    delta
                }
            };
            if scope == UsageScope::Turn {
                for index in 0..5 {
                    running[index] = running[index].saturating_add(values[index]);
                }
            } else {
                running = values;
            }
            let tool_turn = turn == turns_per_repetition;
            if tool_turn && per_turn != [240, 50, 290, 630, 1] {
                tool_turn_two_response_sum = false;
                crosscheck_errors = crosscheck_errors.saturating_add(1);
            }
            if !tool_turn && per_turn != [100, 20, 120, 260, 1] {
                crosscheck_errors = crosscheck_errors.saturating_add(1);
            }
            turn_values.push(json!({
                "turn": turn,
                "response": null,
                "input_tokens": per_turn[0],
                "output_tokens": per_turn[1],
                "total_tokens": per_turn[2],
                "cost_microusd": per_turn[3],
                "turns": per_turn[4],
            }));
            for (response_index, response) in events
                .iter()
                .filter(|event| {
                    actor_matches(event, &actor) && event.event == EventVocab::ModelResponse
                })
                .enumerate()
            {
                let Some(usage) = response.payload.get("usage") else {
                    complete = false;
                    continue;
                };
                let Some(input_tokens) = usage.get("input_tokens").and_then(Value::as_u64) else {
                    complete = false;
                    continue;
                };
                let Some(output_tokens) = usage.get("output_tokens").and_then(Value::as_u64) else {
                    complete = false;
                    continue;
                };
                let total_tokens = input_tokens.saturating_add(output_tokens);
                response_values.push(json!({
                    "turn": turn,
                    "response": u32::try_from(response_index + 1).unwrap_or(u32::MAX),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "cost_microusd": input_tokens.saturating_mul(2).saturating_add(output_tokens.saturating_mul(3)),
                    "turns": 0,
                }));
            }
        }
        if running != expected {
            exact_repetitions = false;
        }
        totals_by_repetition.push(running);
        repetition_details.push(json!({
            "repetition": repetition,
            "input_tokens": running[0],
            "output_tokens": running[1],
            "total_tokens": running[2],
            "cost_microusd": running[3],
            "turns": running[4],
            "per_turn": turn_values,
            "per_response": response_values,
        }));
    }
    let repetitions_identical = totals_by_repetition
        .first()
        .is_some_and(|first| totals_by_repetition.iter().all(|values| values == first));
    let correct_fields = if complete {
        (0..5)
            .filter(|index| {
                totals_by_repetition
                    .iter()
                    .all(|values| values[*index] == expected[*index])
            })
            .count() as u64
    } else {
        0
    };
    let score = correct_fields as f64 / 5.0;
    let headline = totals_by_repetition.first().copied().unwrap_or([0; 5]);
    let passed = complete
        && exact_repetitions
        && one_carrier_per_turn
        && tool_turn_two_response_sum
        && repetitions_identical
        && crosscheck_errors == 0
        && score == 1.0;
    Wave4Evaluation {
        metrics: BTreeMap::from([
            (
                "usage_reporting.input_tokens".to_owned(),
                headline[0] as f64,
            ),
            (
                "usage_reporting.output_tokens".to_owned(),
                headline[1] as f64,
            ),
            (
                "usage_reporting.total_tokens".to_owned(),
                headline[2] as f64,
            ),
            (
                "usage_reporting.cost_microusd".to_owned(),
                headline[3] as f64,
            ),
            ("usage_reporting.turns".to_owned(), headline[4] as f64),
            (
                "usage_reporting.crosscheck_errors".to_owned(),
                crosscheck_errors as f64,
            ),
            ("usage_reporting.score".to_owned(), score),
        ]),
        details: json!({
            "source_pointers": {
                "input_tokens": metadata.input_tokens_pointer,
                "output_tokens": metadata.output_tokens_pointer,
                "total_tokens": metadata.total_tokens_pointer,
                "cost_microusd": metadata.cost_microusd_pointer,
                "turns": metadata.turns_pointer,
            },
            "usage_event": metadata.usage_event,
            "usage_scope": scope,
            "repetitions": repetition_details,
            "one_carrier_per_turn": one_carrier_per_turn,
            "tool_turn_two_response_sum": tool_turn_two_response_sum,
            "repetitions_identical": repetitions_identical,
        }),
        measurement_complete: complete,
        measurement_error: (!complete).then(|| {
            "row-67 requires exactly one readable usage carrier after every turn".to_owned()
        }),
        passed,
    }
}

fn row69_actor(repetition: u32, success: bool) -> String {
    format!("r69p{repetition}{}", if success { "s" } else { "f" })
}

/// Evaluate row 69 from the declared machine event stream.
pub fn evaluate_event_stream_completeness(
    events: &[NormalizedEvent],
    metadata: Option<&EventMetadata>,
    repetitions: u32,
) -> Wave4Evaluation {
    let mut components = [true; 6];
    let mut failures = Vec::new();
    for repetition in 1..=repetitions {
        let success_actor = row69_actor(repetition, true);
        let failure_actor = row69_actor(repetition, false);
        let success = events
            .iter()
            .filter(|event| actor_matches(event, &success_actor))
            .collect::<Vec<_>>();
        let failure = events
            .iter()
            .filter(|event| actor_matches(event, &failure_actor))
            .collect::<Vec<_>>();
        let call = success
            .iter()
            .position(|event| event.event == EventVocab::ToolCall);
        let result = success
            .iter()
            .position(|event| event.event == EventVocab::ToolResult);
        let call_id = call
            .and_then(|index| success.get(index))
            .and_then(|event| event.payload.get("call_id"))
            .and_then(Value::as_str);
        let correlated = result
            .and_then(|index| success.get(index))
            .and_then(|event| event.payload.get("call_id"))
            .and_then(Value::as_str);
        let tool_id_ok = call_id.is_some_and(|id| !id.is_empty());
        let correlation_ok = call
            .zip(result)
            .is_some_and(|(call_index, result_index)| call_index < result_index)
            && call_id == correlated
            && success
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count()
                == 1;
        let terminal_ok = success
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count()
            == 1
            && failure
                .iter()
                .filter(|event| event.event == EventVocab::TerminalFailure)
                .count()
                == 1;
        components[0] &= tool_id_ok;
        components[1] &= correlation_ok;
        components[4] &= terminal_ok;

        let is_metadata_event = |event: &&NormalizedEvent| {
            matches!(
                event.event,
                EventVocab::ToolCall
                    | EventVocab::ToolResult
                    | EventVocab::TerminalSuccess
                    | EventVocab::TerminalFailure
            )
        };
        let success_metadata_events = success
            .iter()
            .copied()
            .filter(is_metadata_event)
            .collect::<Vec<_>>();
        let failure_metadata_events = failure
            .iter()
            .copied()
            .filter(is_metadata_event)
            .collect::<Vec<_>>();
        let metadata_events = success_metadata_events
            .iter()
            .chain(failure_metadata_events.iter())
            .copied()
            .collect::<Vec<_>>();
        let timestamp_ok = metadata.is_some_and(|metadata| {
            !metadata.timestamp_pointer.is_empty()
                && [&success_metadata_events, &failure_metadata_events]
                    .into_iter()
                    .all(|stream| {
                        !stream.is_empty()
                            && stream
                                .iter()
                                .map(|event| {
                                    serde_json::to_value(event).ok().and_then(|raw| {
                                        integer_at(&raw, &metadata.timestamp_pointer)
                                    })
                                })
                                .collect::<Option<Vec<_>>>()
                                .is_some_and(|values| {
                                    values.windows(2).all(|pair| pair[0] <= pair[1])
                                })
                    })
        });
        components[2] &= timestamp_ok;
        let usage_ok = metadata.is_some_and(|metadata| {
            success
                .iter()
                .filter(|event| {
                    event_name(&event.event).as_deref() == Some(metadata.usage_event.as_str())
                })
                .filter_map(|event| serde_json::to_value(event).ok())
                .filter_map(|raw| usage_fields(&raw, metadata))
                .any(|usage| usage == [240, 50, 290, 630, 1])
        });
        components[3] &= usage_ok;
        let schema_ok = metadata.is_some_and(|metadata| {
            let parsed_schema_version =
                serde_json::from_str::<Value>(&metadata.schema_version_value).ok();
            !metadata.schema_version_pointer.is_empty()
                && metadata_events.iter().all(|event| {
                    serde_json::to_value(event).ok().is_some_and(|raw| {
                        raw.pointer(&metadata.schema_version_pointer)
                            .is_some_and(|value| {
                                value.as_str() == Some(metadata.schema_version_value.as_str())
                                    || parsed_schema_version.as_ref() == Some(value)
                            })
                    })
                })
        });
        components[5] &= schema_ok;
        let receipt_bound = |pointer: &str, earliest: bool| {
            let values = metadata_events
                .iter()
                .filter_map(|event| event.payload.pointer(pointer).and_then(Value::as_u64));
            if earliest {
                values.min().unwrap_or(0)
            } else {
                values.max().unwrap_or(0)
            }
        };
        let receipt_start_ns = receipt_bound("/_ahrb_receipt/receipt_start_ns", true);
        let receipt_end_ns = receipt_bound("/_ahrb_receipt/receipt_end_ns", false);
        let receipt_wall_start_ns = receipt_bound("/_ahrb_receipt/receipt_wall_start_ns", true);
        let receipt_wall_end_ns = receipt_bound("/_ahrb_receipt/receipt_wall_end_ns", false);
        for (index, (name, ok)) in [
            ("tool_call_id", tool_id_ok),
            ("correlated_result", correlation_ok),
            ("timestamps", timestamp_ok),
            ("usage", usage_ok),
            ("terminal_typing", terminal_ok),
            ("schema_version", schema_ok),
        ]
        .into_iter()
        .enumerate()
        {
            if !ok {
                failures.push(json!({
                    "repetition": repetition,
                    "component": name,
                    "detail": format!("{name} component did not satisfy its declared event-stream contract"),
                    "receipt_start_ns": receipt_start_ns,
                    "receipt_end_ns": receipt_end_ns,
                    "receipt_wall_start_ns": receipt_wall_start_ns,
                    "receipt_wall_end_ns": receipt_wall_end_ns,
                }));
                components[index] = false;
            }
        }
    }
    let names = [
        "tool_call_id",
        "correlated_result",
        "timestamps",
        "usage",
        "terminal_typing",
        "schema_version",
    ];
    let passed_components = components.iter().filter(|value| **value).count();
    let score = passed_components as f64 / 6.0;
    let passed = passed_components >= 4 && components[0] && components[1] && components[4];
    let mut metrics = BTreeMap::new();
    for (name, passed) in names.into_iter().zip(components) {
        metrics.insert(
            format!("event_stream_completeness.{name}"),
            if passed { 1.0 } else { 0.0 },
        );
    }
    metrics.insert("event_stream_completeness.score".to_owned(), score);
    Wave4Evaluation {
        metrics,
        details: json!({
            "missing_components": names
                .into_iter()
                .zip(components)
                .filter_map(|(name, passed)| (!passed).then_some(name))
                .collect::<Vec<_>>(),
            "component_failures": failures,
        }),
        measurement_complete: true,
        measurement_error: None,
        passed,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn row69_exact_integer_threshold_accepts_four_with_hard_trio() {
        let components = [true, true, false, false, true, true];
        let passed_components = components.iter().filter(|value| **value).count();
        assert_eq!(passed_components, 4);
        assert!(passed_components >= 4 && components[0] && components[1] && components[4]);
    }
}
