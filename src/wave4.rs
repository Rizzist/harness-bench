//! Wave-4 automation-interface evidence evaluators.

use crate::events::{EventVocab, NormalizedEvent};
use crate::manifest::{
    CompactionCapture, EventMetadata, NarrativeAggregation, NarrativeCapture, UsageScope,
};
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

fn narrative_values(
    stream: &[&NormalizedEvent],
    expected_event: &str,
    value_pointer: &str,
    aggregation: NarrativeAggregation,
    item_pointer: Option<&str>,
) -> Option<Vec<String>> {
    match aggregation {
        NarrativeAggregation::CompleteEvent => stream
            .iter()
            .filter(|event| event_name(&event.event).as_deref() == Some(expected_event))
            .filter_map(|event| {
                let raw = raw_event(event);
                raw.pointer(value_pointer)
                    .map(|value| value.as_str().map(str::to_owned))
            })
            .collect(),
        NarrativeAggregation::ItemDeltas => {
            let item_pointer = item_pointer?;
            let mut items = Vec::<(String, String)>::new();
            let mut item_indexes = BTreeMap::<String, usize>::new();
            for event in stream
                .iter()
                .filter(|event| event_name(&event.event).as_deref() == Some(expected_event))
            {
                let raw = raw_event(event);
                let Some(value) = raw.pointer(value_pointer) else {
                    continue;
                };
                let value = value.as_str()?;
                let item = raw.pointer(item_pointer).and_then(Value::as_str)?;
                if item.is_empty() {
                    return None;
                }
                let index = match item_indexes.get(item).copied() {
                    Some(index) => index,
                    None => {
                        let index = items.len();
                        items.push((item.to_owned(), String::new()));
                        item_indexes.insert(item.to_owned(), index);
                        index
                    }
                };
                items[index].1.push_str(value);
            }
            Some(items.into_iter().map(|(_, value)| value).collect())
        }
    }
}

fn narrative_turns_correlate(
    stream: &[&NormalizedEvent],
    capture: &NarrativeCapture,
    actor: &str,
) -> bool {
    stream
        .iter()
        .filter(|event| {
            let name = event_name(&event.event);
            let raw = raw_event(event);
            (name.as_deref() == Some(capture.assistant_text_event.as_str())
                && raw
                    .pointer(&capture.assistant_text_pointer)
                    .is_some_and(Value::is_string))
                || (name.as_deref() == Some(capture.reasoning_event.as_str())
                    && raw
                        .pointer(&capture.reasoning_pointer)
                        .is_some_and(Value::is_string))
        })
        .all(|event| {
            raw_event(event)
                .pointer(&capture.turn_pointer)
                .and_then(Value::as_str)
                .is_some_and(|turn| turn == actor || turn.rsplit(':').next() == Some(actor))
        })
}

/// Evaluate row 69 from the declared machine event stream.
pub fn evaluate_event_stream_completeness(
    events: &[NormalizedEvent],
    metadata: Option<&EventMetadata>,
    narrative: Option<&NarrativeCapture>,
    repetitions: u32,
) -> Wave4Evaluation {
    let mut components = [true; 7];
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
        let narrative_ok = narrative.is_some_and(|capture| {
            narrative_values(
                &success,
                &capture.assistant_text_event,
                &capture.assistant_text_pointer,
                capture.assistant_text_aggregation,
                capture.assistant_text_item_pointer.as_deref(),
            ) == Some(vec![
                "I will write the event-stream fixture.".to_owned(),
                "SUCCESS".to_owned(),
            ]) && narrative_values(
                &failure,
                &capture.assistant_text_event,
                &capture.assistant_text_pointer,
                capture.assistant_text_aggregation,
                capture.assistant_text_item_pointer.as_deref(),
            ) == Some(vec![
                r#"{"status":"FAILURE","category":"scripted"}"#.to_owned(),
            ]) && narrative_values(
                &success,
                &capture.reasoning_event,
                &capture.reasoning_pointer,
                capture.reasoning_aggregation,
                capture.reasoning_item_pointer.as_deref(),
            ) == Some(vec![
                "prepare the requested tool call".to_owned(),
                "verify the tool result and conclude".to_owned(),
            ]) && narrative_values(
                &failure,
                &capture.reasoning_event,
                &capture.reasoning_pointer,
                capture.reasoning_aggregation,
                capture.reasoning_item_pointer.as_deref(),
            ) == Some(vec!["report the scripted failure".to_owned()])
                && narrative_turns_correlate(&success, capture, &success_actor)
                && narrative_turns_correlate(&failure, capture, &failure_actor)
        });
        components[6] &= narrative_ok;
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
            ("narrative_reconstructability", narrative_ok),
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
        "narrative_reconstructability",
    ];
    let passed_components = components.iter().filter(|value| **value).count();
    let score = passed_components as f64 / 7.0;
    let passed =
        passed_components >= 5 && components[0] && components[1] && components[4] && components[6];
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

/// External evidence that one completed long-horizon run did or did not compact.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionTransparencyTrial {
    pub repetition: u32,
    pub compaction_observed: bool,
    pub omitted_markers: Vec<String>,
}

fn compaction_scope_matches(
    raw: &Value,
    capture: &CompactionCapture,
    omitted_markers: &[String],
) -> bool {
    if omitted_markers.is_empty() {
        return false;
    }
    let count_matches = capture.dropped_count_pointer.as_deref().map(|pointer| {
        u64::try_from(omitted_markers.len())
            .ok()
            .is_some_and(|expected| raw.pointer(pointer).and_then(Value::as_u64) == Some(expected))
    });
    let span_matches = capture
        .dropped_span_start_pointer
        .as_deref()
        .zip(capture.dropped_span_end_pointer.as_deref())
        .map(|(start_pointer, end_pointer)| {
            raw.pointer(start_pointer).and_then(Value::as_str)
                == omitted_markers.first().map(String::as_str)
                && raw.pointer(end_pointer).and_then(Value::as_str)
                    == omitted_markers.last().map(String::as_str)
        });
    let declared = count_matches.is_some() || span_matches.is_some();
    declared
        && count_matches
            .into_iter()
            .chain(span_matches)
            .all(|value| value)
}

/// Evaluate row 73 from externally proven compaction trials and the adapter's
/// declared durable signal locations.
pub fn evaluate_compaction_transparency(
    events: &[NormalizedEvent],
    capture: Option<&CompactionCapture>,
    trials: &[CompactionTransparencyTrial],
    expected_repetitions: u32,
) -> Wave4Evaluation {
    let incomplete = |message: String| Wave4Evaluation {
        details: json!({
            "measurement_complete": false,
            "measurement_error": message,
            "compaction_observed": false,
            "observations": [],
        }),
        measurement_error: Some(message),
        ..Wave4Evaluation::default()
    };
    if u32::try_from(trials.len()).ok() != Some(expected_repetitions) {
        return incomplete("row-73 compaction trial set is incomplete".to_owned());
    }
    let mut repetitions = std::collections::BTreeSet::new();
    let observed_repetitions = trials
        .iter()
        .filter(|trial| repetitions.insert(trial.repetition) && trial.compaction_observed)
        .count();
    if repetitions.len() != trials.len()
        || trials
            .iter()
            .any(|trial| trial.repetition == 0 || trial.repetition > expected_repetitions)
    {
        return incomplete("row-73 has invalid or duplicate repetition evidence".to_owned());
    }
    if observed_repetitions != 0 && observed_repetitions != trials.len() {
        return incomplete(
            "row-73 observed compaction in only part of the required trial set".to_owned(),
        );
    }
    let compactions_observed = observed_repetitions as u64;
    let matching_events = capture.map_or_else(Vec::new, |capture| {
        events
            .iter()
            .filter(|event| event_name(&event.event).as_deref() == Some(capture.event.as_str()))
            .collect::<Vec<_>>()
    });
    let announcements = matching_events.len() as u64;
    let mut correlated_announcements = 0_u64;
    let mut scoped_announcements = 0_u64;
    let mut observations = Vec::with_capacity(trials.len());
    for trial in trials {
        let expected_turn = format!("row-51-recover-r{}", trial.repetition);
        let correlated = capture.map_or_else(Vec::new, |capture| {
            matching_events
                .iter()
                .copied()
                .filter(|event| {
                    raw_event(event)
                        .pointer(&capture.turn_pointer)
                        .and_then(Value::as_str)
                        == Some(expected_turn.as_str())
                })
                .collect::<Vec<_>>()
        });
        let correlated_once = correlated.len() == 1;
        correlated_announcements =
            correlated_announcements.saturating_add(u64::from(correlated_once));
        let scoped = correlated_once
            && capture.is_some_and(|capture| {
                correlated.first().is_some_and(|event| {
                    let raw = raw_event(event);
                    compaction_scope_matches(&raw, capture, &trial.omitted_markers)
                })
            });
        scoped_announcements = scoped_announcements.saturating_add(u64::from(scoped));
        observations.push(json!({
            "repetition": trial.repetition,
            "announcement_id": correlated.first().map(|event| event.id.clone()),
            "correlated": correlated_once,
            "scoped": scoped,
        }));
    }
    let expected = u64::from(expected_repetitions);
    let score = if compactions_observed == expected
        && announcements == expected
        && correlated_announcements == expected
        && scoped_announcements == expected
    {
        1.0
    } else if compactions_observed == expected
        && announcements == expected
        && correlated_announcements == expected
    {
        0.5
    } else {
        0.0
    };
    Wave4Evaluation {
        metrics: BTreeMap::from([
            (
                "compaction_transparency.compactions_observed".to_owned(),
                compactions_observed as f64,
            ),
            (
                "compaction_transparency.announcements".to_owned(),
                announcements as f64,
            ),
            (
                "compaction_transparency.scoped_announcements".to_owned(),
                scoped_announcements as f64,
            ),
            (
                "compaction_transparency.correlated_announcements".to_owned(),
                correlated_announcements as f64,
            ),
            ("compaction_transparency.score".to_owned(), score),
        ]),
        details: json!({
            "measurement_complete": true,
            "compaction_observed": compactions_observed > 0,
            "observations": observations,
        }),
        measurement_complete: true,
        measurement_error: None,
        passed: score == 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row69_event(
        id: &str,
        cursor: u64,
        actor: &str,
        event: EventVocab,
        payload: Value,
    ) -> NormalizedEvent {
        let mut payload = payload;
        if let Some(object) = payload.as_object_mut() {
            object.insert("timestamp_ns".to_owned(), json!(cursor));
            object.insert("schema_version".to_owned(), json!(1));
        }
        NormalizedEvent {
            id: id.to_owned(),
            cursor,
            session_id: actor.to_owned(),
            actor: actor.to_owned(),
            event,
            payload,
        }
    }

    fn row69_complete_events() -> Vec<NormalizedEvent> {
        vec![
            row69_event(
                "s1",
                1,
                "r69p1s",
                EventVocab::ModelResponse,
                json!({"assistant_text":"I will write the event-stream fixture.","reasoning":"prepare the requested tool call"}),
            ),
            row69_event(
                "s2",
                2,
                "r69p1s",
                EventVocab::ToolCall,
                json!({"call_id":"call-1"}),
            ),
            row69_event(
                "s3",
                3,
                "r69p1s",
                EventVocab::ToolResult,
                json!({"call_id":"call-1"}),
            ),
            row69_event(
                "s4",
                4,
                "r69p1s",
                EventVocab::ModelResponse,
                json!({"assistant_text":"SUCCESS","reasoning":"verify the tool result and conclude"}),
            ),
            row69_event(
                "s5",
                5,
                "r69p1s",
                EventVocab::TerminalSuccess,
                json!({"usage":{"input_tokens":240,"output_tokens":50,"total_tokens":290,"cost_microusd":630,"turns":1}}),
            ),
            row69_event(
                "f1",
                1,
                "r69p1f",
                EventVocab::ModelResponse,
                json!({"assistant_text":"{\"status\":\"FAILURE\",\"category\":\"scripted\"}","reasoning":"report the scripted failure"}),
            ),
            row69_event("f2", 2, "r69p1f", EventVocab::TerminalFailure, json!({})),
        ]
    }

    fn row69_metadata() -> EventMetadata {
        EventMetadata {
            timestamp_pointer: "/payload/timestamp_ns".to_owned(),
            timestamp_format: Some(crate::manifest::TimestampFormat::MonotonicNs),
            schema_version_pointer: "/payload/schema_version".to_owned(),
            schema_version_value: "1".to_owned(),
            usage_event: "terminal-success".to_owned(),
            usage_scope: Some(UsageScope::Turn),
            input_tokens_pointer: "/payload/usage/input_tokens".to_owned(),
            output_tokens_pointer: "/payload/usage/output_tokens".to_owned(),
            total_tokens_pointer: "/payload/usage/total_tokens".to_owned(),
            cost_microusd_pointer: "/payload/usage/cost_microusd".to_owned(),
            turns_pointer: "/payload/usage/turns".to_owned(),
        }
    }

    fn row69_narrative() -> NarrativeCapture {
        NarrativeCapture {
            assistant_text_event: "model-response".to_owned(),
            assistant_text_pointer: "/payload/assistant_text".to_owned(),
            assistant_text_aggregation: NarrativeAggregation::CompleteEvent,
            assistant_text_item_pointer: None,
            reasoning_event: "model-response".to_owned(),
            reasoning_pointer: "/payload/reasoning".to_owned(),
            reasoning_aggregation: NarrativeAggregation::CompleteEvent,
            reasoning_item_pointer: None,
            turn_pointer: "/actor".to_owned(),
        }
    }

    fn compaction_trial(repetition: u32) -> CompactionTransparencyTrial {
        CompactionTransparencyTrial {
            repetition,
            compaction_observed: true,
            omitted_markers: vec!["old-1".to_owned(), "old-2".to_owned()],
        }
    }

    #[test]
    fn row69_exact_integer_threshold_accepts_five_with_hard_quartet() {
        let components = [true, true, false, false, true, true, true];
        let passed_components = components.iter().filter(|value| **value).count();
        assert_eq!(passed_components, 5);
        assert!(
            passed_components >= 5
                && components[0]
                && components[1]
                && components[4]
                && components[6]
        );
    }

    #[test]
    fn row69_narrative_mock_discriminates_full_journal_from_tool_metadata_only() {
        let complete = evaluate_event_stream_completeness(
            &row69_complete_events(),
            Some(&row69_metadata()),
            Some(&row69_narrative()),
            1,
        );
        assert_eq!(
            complete.metrics["event_stream_completeness.narrative_reconstructability"],
            1.0
        );
        assert_eq!(complete.metrics["event_stream_completeness.score"], 1.0);
        assert!(complete.passed);

        let mut metadata_only = row69_complete_events();
        for event in &mut metadata_only {
            if event.event == EventVocab::ModelResponse
                && let Some(payload) = event.payload.as_object_mut()
            {
                payload.remove("assistant_text");
                payload.remove("reasoning");
            }
        }
        let metadata_only = evaluate_event_stream_completeness(
            &metadata_only,
            Some(&row69_metadata()),
            Some(&row69_narrative()),
            1,
        );
        for component in [
            "tool_call_id",
            "correlated_result",
            "timestamps",
            "usage",
            "terminal_typing",
            "schema_version",
        ] {
            assert_eq!(
                complete.metrics[&format!("event_stream_completeness.{component}")],
                metadata_only.metrics[&format!("event_stream_completeness.{component}")]
            );
        }
        assert_eq!(
            metadata_only.metrics["event_stream_completeness.narrative_reconstructability"],
            0.0
        );
        assert_eq!(
            metadata_only.metrics["event_stream_completeness.score"],
            6.0 / 7.0
        );
        assert!(!metadata_only.passed);
    }

    #[test]
    fn row69_reconstructs_adapter_declared_ordered_item_deltas() {
        let mut events = Vec::new();
        for event in row69_complete_events() {
            if event.event != EventVocab::ModelResponse {
                events.push(event);
                continue;
            }
            let assistant = event.payload["assistant_text"].as_str().unwrap_or_default();
            let reasoning = event.payload["reasoning"].as_str().unwrap_or_default();
            let assistant_split = assistant.len() / 2;
            let reasoning_split = reasoning.len() / 2;
            for (index, (assistant_delta, reasoning_delta)) in [
                (&assistant[..assistant_split], &reasoning[..reasoning_split]),
                (&assistant[assistant_split..], &reasoning[reasoning_split..]),
            ]
            .into_iter()
            .enumerate()
            {
                let mut delta = event.clone();
                delta.id = format!("{}-delta-{index}", event.id);
                delta.payload["assistant_text"] = json!(assistant_delta);
                delta.payload["reasoning"] = json!(reasoning_delta);
                delta.payload["item_id"] = json!(event.id);
                events.push(delta);
            }
        }
        let mut capture = row69_narrative();
        capture.assistant_text_aggregation = NarrativeAggregation::ItemDeltas;
        capture.assistant_text_item_pointer = Some("/payload/item_id".to_owned());
        capture.reasoning_aggregation = NarrativeAggregation::ItemDeltas;
        capture.reasoning_item_pointer = Some("/payload/item_id".to_owned());
        let evaluation =
            evaluate_event_stream_completeness(&events, Some(&row69_metadata()), Some(&capture), 1);
        assert_eq!(
            evaluation.metrics["event_stream_completeness.narrative_reconstructability"],
            1.0
        );
        assert!(evaluation.passed);
    }

    #[test]
    fn row73_mock_discriminates_scoped_announcement_from_silent_compaction() {
        let capture = CompactionCapture {
            event: "context-compacted".to_owned(),
            turn_pointer: "/payload/turn_key".to_owned(),
            dropped_count_pointer: Some("/payload/dropped_count".to_owned()),
            dropped_span_start_pointer: Some("/payload/dropped_span/first".to_owned()),
            dropped_span_end_pointer: Some("/payload/dropped_span/last".to_owned()),
        };
        let event = row69_event(
            "compact-1",
            1,
            "r51-context",
            EventVocab::ContextCompacted,
            json!({
                "turn_key":"row-51-recover-r1",
                "dropped_count":2,
                "dropped_span":{"first":"old-1","last":"old-2"}
            }),
        );
        let scoped = evaluate_compaction_transparency(
            std::slice::from_ref(&event),
            Some(&capture),
            &[compaction_trial(1)],
            1,
        );
        assert_eq!(scoped.metrics["compaction_transparency.score"], 1.0);
        assert!(scoped.passed);

        let silent =
            evaluate_compaction_transparency(&[], Some(&capture), &[compaction_trial(1)], 1);
        assert_eq!(silent.metrics["compaction_transparency.score"], 0.0);
        assert!(!silent.passed);

        let announced_only_capture = CompactionCapture {
            dropped_count_pointer: None,
            dropped_span_start_pointer: None,
            dropped_span_end_pointer: None,
            ..capture.clone()
        };
        let announced_only = evaluate_compaction_transparency(
            &[event],
            Some(&announced_only_capture),
            &[compaction_trial(1)],
            1,
        );
        assert_eq!(announced_only.metrics["compaction_transparency.score"], 0.5);
        assert!(!announced_only.passed);

        let false_scope = row69_event(
            "compact-false-scope",
            1,
            "r51-context",
            EventVocab::ContextCompacted,
            json!({
                "turn_key":"row-51-recover-r1",
                "dropped_count":99,
                "dropped_span":{"first":null,"last":null}
            }),
        );
        let false_scope = evaluate_compaction_transparency(
            &[false_scope],
            Some(&capture),
            &[compaction_trial(1)],
            1,
        );
        assert_eq!(false_scope.metrics["compaction_transparency.score"], 0.5);
        assert!(!false_scope.passed);
    }

    #[test]
    fn row73_complete_run_without_compaction_is_nonpassing_and_inapplicable() {
        let mut trial = compaction_trial(1);
        trial.compaction_observed = false;
        trial.omitted_markers.clear();
        let evaluation = evaluate_compaction_transparency(&[], None, &[trial], 1);
        assert!(evaluation.measurement_complete);
        assert_eq!(
            evaluation.metrics["compaction_transparency.compactions_observed"],
            0.0
        );
        assert_eq!(evaluation.details["compaction_observed"], false);
        assert!(!evaluation.passed);
    }
}
