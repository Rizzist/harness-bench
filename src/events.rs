//! Stable event vocabulary and table-driven normalization.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

use crate::manifest::EventMapping;
use crate::{AhrbError, Result};

/// AHRB's normalized event vocabulary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventVocab {
    /// Harness accepted a semantic turn.
    TurnAccepted,
    /// A model request began.
    ModelRequest,
    /// A model response completed.
    ModelResponse,
    /// A tool call was fully assembled.
    ToolCall,
    /// A tool result was committed.
    ToolResult,
    /// A child actor was durably created.
    AgentSpawned,
    /// A named workflow barrier was reached.
    BarrierReached,
    /// A safe-boundary input was accepted.
    InputAccepted,
    /// A lifecycle hook completed.
    HookCompleted,
    /// Terminal structured success.
    TerminalSuccess,
    /// Terminal structured failure.
    TerminalFailure,
    /// Terminal cancellation.
    TerminalCancelled,
}

/// One normalized, cursor-addressable event.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NormalizedEvent {
    /// Stable identity used for deduplication.
    pub id: String,
    /// Monotonic durable cursor.
    pub cursor: u64,
    /// Session identifier.
    pub session_id: String,
    /// Actor route marker.
    pub actor: String,
    /// Event vocabulary entry.
    pub event: EventVocab,
    /// Redacted semantic payload.
    pub payload: Value,
}

/// Stateful table-driven normalizer with stable-ID deduplication.
#[derive(Clone, Debug, Default)]
pub struct EventNormalizer {
    seen: BTreeSet<String>,
}

impl EventNormalizer {
    /// Normalize one source event; duplicate stable IDs yield `None`.
    pub fn normalize(
        &mut self,
        raw: &Value,
        mapping: &EventMapping,
    ) -> Result<Option<NormalizedEvent>> {
        let event_type = raw
            .get("type")
            .and_then(Value::as_str)
            .or_else(|| raw.get("event").and_then(Value::as_str))
            .ok_or_else(|| AhrbError::Protocol("source event has no type".to_owned()))?;
        let rule = mapping
            .rules
            .iter()
            .find(|rule| rule.matches == event_type)
            .ok_or_else(|| {
                AhrbError::Protocol(format!("no normalization rule for {event_type:?}"))
            })?;
        let id = pointer_string(raw, &mapping.id_pointer, "stable event ID")?;
        if !self.seen.insert(id.clone()) {
            return Ok(None);
        }
        let cursor_value = raw.pointer(&mapping.cursor_pointer).ok_or_else(|| {
            AhrbError::Protocol(format!(
                "event missing cursor at {:?}",
                mapping.cursor_pointer
            ))
        })?;
        let cursor = cursor_value
            .as_u64()
            .ok_or_else(|| AhrbError::Protocol("event cursor is not u64".to_owned()))?;
        let payload = if rule.payload_pointer.is_empty() {
            raw.clone()
        } else {
            raw.pointer(&rule.payload_pointer).cloned().ok_or_else(|| {
                AhrbError::Protocol(format!(
                    "event missing payload at {:?}",
                    rule.payload_pointer
                ))
            })?
        };
        Ok(Some(NormalizedEvent {
            id,
            cursor,
            session_id: optional_pointer_string(raw, "/session_id"),
            actor: optional_pointer_string(raw, "/actor"),
            event: parse_vocab(&rule.event)?,
            payload,
        }))
    }
}

fn pointer_string(raw: &Value, pointer: &str, label: &str) -> Result<String> {
    raw.pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| AhrbError::Protocol(format!("event missing {label} at {pointer:?}")))
}

fn optional_pointer_string(raw: &Value, pointer: &str) -> String {
    raw.pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_default()
}

fn parse_vocab(name: &str) -> Result<EventVocab> {
    match name {
        "turn-accepted" => Ok(EventVocab::TurnAccepted),
        "model-request" => Ok(EventVocab::ModelRequest),
        "model-response" => Ok(EventVocab::ModelResponse),
        "tool-call" => Ok(EventVocab::ToolCall),
        "tool-result" => Ok(EventVocab::ToolResult),
        "agent-spawned" => Ok(EventVocab::AgentSpawned),
        "barrier-reached" => Ok(EventVocab::BarrierReached),
        "input-accepted" => Ok(EventVocab::InputAccepted),
        "hook-completed" => Ok(EventVocab::HookCompleted),
        "terminal-success" => Ok(EventVocab::TerminalSuccess),
        "terminal-failure" => Ok(EventVocab::TerminalFailure),
        "terminal-cancelled" => Ok(EventVocab::TerminalCancelled),
        other => Err(AhrbError::Validation(format!(
            "unknown normalized event vocabulary {other:?}"
        ))),
    }
}
