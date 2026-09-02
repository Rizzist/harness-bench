//! Deterministic, marker-routed workflow definitions and state coordination.

use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Current workflow schema version understood by AHRB.
pub const WORKFLOW_SCHEMA_VERSION: u32 = 1;

/// Prefix used for opaque scenario route markers embedded in prompts.
pub const MARKER_PREFIX: &str = "[[AHRB:";

/// A versioned declarative benchmark workflow.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Workflow {
    /// Workflow schema version.
    pub version: u32,
    /// Stable scenario identifier.
    pub scenario: String,
    /// Logical actors keyed by stable ID.
    pub actors: BTreeMap<String, Actor>,
    /// Named state barriers.
    #[serde(default)]
    pub barriers: BTreeMap<String, Barrier>,
    /// Ordered marker-routed response scripts.
    pub responses: Vec<ScriptedResponse>,
}

impl Workflow {
    /// Validate all cross-references and deterministic transition invariants.
    pub fn validate(&self) -> Result<()> {
        if self.version != WORKFLOW_SCHEMA_VERSION {
            return Err(validation(format!(
                "unsupported workflow schema {}; expected {WORKFLOW_SCHEMA_VERSION}",
                self.version
            )));
        }
        validate_component("scenario", &self.scenario)?;
        if self.actors.is_empty() {
            return Err(validation("workflow must declare at least one actor"));
        }

        for (actor_id, actor) in &self.actors {
            validate_component("actor", actor_id)?;
            if actor.id != *actor_id {
                return Err(validation(format!(
                    "actor map key {actor_id:?} does not match actor.id {:?}",
                    actor.id
                )));
            }
            if actor.prompt.trim().is_empty() {
                return Err(validation(format!(
                    "actor {actor_id:?} has an empty prompt"
                )));
            }
            if let Some(parent) = &actor.parent {
                if parent == actor_id {
                    return Err(validation(format!("actor {actor_id:?} is its own parent")));
                }
                if !self.actors.contains_key(parent) {
                    return Err(validation(format!(
                        "actor {actor_id:?} references missing parent {parent:?}"
                    )));
                }
            }
        }

        for (name, barrier) in &self.barriers {
            validate_component("barrier", name)?;
            if barrier.name != *name {
                return Err(validation(format!(
                    "barrier map key {name:?} does not match barrier.name {:?}",
                    barrier.name
                )));
            }
            validate_component("checkpoint", &barrier.checkpoint)?;
            if barrier.actors.is_empty() {
                return Err(validation(format!("barrier {name:?} has no actors")));
            }
            let mut unique = BTreeSet::new();
            for actor in &barrier.actors {
                if !self.actors.contains_key(actor) {
                    return Err(validation(format!(
                        "barrier {name:?} references missing actor {actor:?}"
                    )));
                }
                if !unique.insert(actor) {
                    return Err(validation(format!(
                        "barrier {name:?} repeats actor {actor:?}"
                    )));
                }
            }
        }

        let mut checkpoints = BTreeSet::new();
        for response in &self.responses {
            if let Some(fault) = &response.fault {
                fault.validate()?;
            }
            if response.scenario != self.scenario {
                return Err(validation(format!(
                    "response scenario {:?} does not match workflow scenario {:?}",
                    response.scenario, self.scenario
                )));
            }
            if !self.actors.contains_key(&response.actor) {
                return Err(validation(format!(
                    "response references missing actor {:?}",
                    response.actor
                )));
            }
            validate_component("checkpoint", &response.checkpoint)?;
            if !checkpoints.insert((response.actor.as_str(), response.checkpoint.as_str())) {
                return Err(validation(format!(
                    "duplicate response for actor {:?} checkpoint {:?}",
                    response.actor, response.checkpoint
                )));
            }
            if !response.request_hash.is_empty()
                && (response.request_hash.len() != 64
                    || !response
                        .request_hash
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()))
            {
                return Err(validation(format!(
                    "request hash at {}/{} must be a 64-character hexadecimal SHA-256",
                    response.actor, response.checkpoint
                )));
            }
            if let Some(barrier_name) = &response.barrier {
                let Some(barrier) = self.barriers.get(barrier_name) else {
                    return Err(validation(format!(
                        "response {}/{} references missing barrier {barrier_name:?}",
                        response.actor, response.checkpoint
                    )));
                };
                if barrier.checkpoint != response.checkpoint
                    || !barrier.actors.contains(&response.actor)
                {
                    return Err(validation(format!(
                        "response {}/{} does not match barrier {barrier_name:?} state",
                        response.actor, response.checkpoint
                    )));
                }
            }
        }

        for (name, barrier) in &self.barriers {
            for actor in &barrier.actors {
                let has_transition = self.responses.iter().any(|response| {
                    response.actor == *actor
                        && response.checkpoint == barrier.checkpoint
                        && response.barrier.as_deref() == Some(name.as_str())
                });
                if !has_transition {
                    return Err(validation(format!(
                        "barrier {name:?} actor {actor:?} has no matching scripted transition"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Return the canonical marker for one declared actor and checkpoint.
    pub fn marker(&self, actor: &str, checkpoint: &str) -> Result<RouteMarker> {
        if !self.actors.contains_key(actor) {
            return Err(validation(format!("unknown actor {actor:?}")));
        }
        RouteMarker::new(&self.scenario, actor, checkpoint)
    }
}

/// A logical concurrent workflow actor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Actor {
    /// Stable actor identifier.
    pub id: String,
    /// Parent actor for native delegation.
    #[serde(default)]
    pub parent: Option<String>,
    /// Initial prompt containing the opaque route marker.
    pub prompt: String,
    /// Per-actor isolated workspace.
    pub workspace: String,
}

/// A named state condition released by the runner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Barrier {
    /// Stable barrier name.
    pub name: String,
    /// Actors that must reach the barrier.
    pub actors: Vec<String>,
    /// Checkpoint each actor must reach.
    pub checkpoint: String,
}

/// One legal fake-model transition and exact response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScriptedResponse {
    /// Scenario route marker.
    pub scenario: String,
    /// Actor route marker.
    pub actor: String,
    /// Stable checkpoint name.
    pub checkpoint: String,
    /// Canonical request hash, or empty to bind the first canonical request.
    #[serde(default)]
    pub request_hash: String,
    /// Response body semantic content.
    pub response: Value,
    /// Optional deterministic transport fault.
    #[serde(default)]
    pub fault: Option<Fault>,
    /// Optional barrier reached before emitting this response.
    #[serde(default)]
    pub barrier: Option<String>,
}

/// A deterministic fake-upstream fault.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Fault {
    /// Return an HTTP status with an optional body.
    HttpStatus {
        /// Status code.
        status: u16,
        /// Exact response body.
        body: String,
    },
    /// Return the same retryable HTTP status on every physical attempt.
    ///
    /// This is distinct from the v1 one-shot status fixture so row 58 can
    /// prove exhaustion of a documented retry budget without changing row 11.
    SustainedHttpStatus {
        /// Retryable status code (429 or 500).
        status: u16,
        /// Exact response body.
        body: String,
    },
    /// Return one dialect-native context-length error, then accept one compacted retry.
    ContextLength {
        /// Advertised fake-provider context window.
        window_tokens: u64,
    },
    /// Delay an otherwise normal complete response by a fixed interval.
    Delay {
        /// Delay after request acceptance and any barrier release.
        delay_ms: u64,
    },
    /// Close after a deterministic byte prefix.
    MidStreamDisconnect {
        /// Number of body bytes emitted before close.
        after_bytes: usize,
    },
    /// Accept the request and emit no bytes until externally stopped.
    Stall,
    /// Emit a fixed number of one-byte frames at an anchored cadence.
    Trickle {
        /// Milliseconds between scheduled one-byte frames.
        cadence_ms: u64,
        /// Number of one-byte frames to emit.
        count: u32,
    },
    /// Fragment a streaming response at exact byte offsets.
    Fragment {
        /// Strictly increasing byte offsets.
        boundaries: Vec<usize>,
    },
    /// Repeat the same logical frame a fixed number of times.
    RepeatFrame {
        /// Total copies of the frame.
        copies: usize,
    },
}

impl Fault {
    /// Validate parameters which must be safe to apply to a byte stream.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::HttpStatus { status, .. } if !(100..=599).contains(status) => {
                Err(validation(format!("invalid HTTP fault status {status}")))
            }
            Self::SustainedHttpStatus { status, .. } if !matches!(status, 429 | 500) => Err(
                validation(format!("invalid sustained HTTP fault status {status}")),
            ),
            Self::Delay { delay_ms: 0 } => Err(validation(
                "delay fault delay-ms must be positive".to_owned(),
            )),
            Self::ContextLength { window_tokens } if *window_tokens == 0 => {
                Err(validation("context-length window-tokens must be nonzero"))
            }
            Self::Fragment { boundaries } => {
                let mut previous = 0;
                for boundary in boundaries {
                    if *boundary == 0 || *boundary <= previous {
                        return Err(validation(
                            "fragment boundaries must be nonzero and strictly increasing",
                        ));
                    }
                    previous = *boundary;
                }
                Ok(())
            }
            Self::RepeatFrame { copies } if *copies == 0 => {
                Err(validation("repeat-frame copies must be nonzero"))
            }
            Self::Trickle { cadence_ms, count } if *cadence_ms == 0 || *count == 0 => Err(
                validation("trickle cadence-ms and count must both be nonzero"),
            ),
            _ => Ok(()),
        }
    }
}

/// A named workflow test with assertions and pillar ownership.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Scenario {
    /// Matrix row number.
    pub row: u8,
    /// Stable slug.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Pillar label.
    pub pillar: String,
    /// Declarative workflow.
    pub workflow: Workflow,
    /// Assertion names evaluated from normalized evidence.
    pub assertions: Vec<String>,
}

/// Opaque marker used to route requests without relying on arrival order.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RouteMarker {
    /// Scenario identifier.
    pub scenario: String,
    /// Stable actor identifier.
    pub actor: String,
    /// Expected conversation checkpoint.
    pub checkpoint: String,
}

impl RouteMarker {
    /// Construct and validate a marker.
    pub fn new(scenario: &str, actor: &str, checkpoint: &str) -> Result<Self> {
        validate_component("scenario", scenario)?;
        validate_component("actor", actor)?;
        validate_component("checkpoint", checkpoint)?;
        Ok(Self {
            scenario: scenario.to_owned(),
            actor: actor.to_owned(),
            checkpoint: checkpoint.to_owned(),
        })
    }

    /// Encode a marker for inclusion in prompts and child-spawn arguments.
    pub fn encode(&self) -> String {
        format!(
            "{MARKER_PREFIX}scenario={};actor={};checkpoint={}]]",
            self.scenario, self.actor, self.checkpoint
        )
    }

    /// Extract the last valid AHRB marker from arbitrary text.
    pub fn extract(text: &str) -> Result<Option<Self>> {
        let mut remaining = text;
        let mut last = None;
        while let Some(start) = remaining.find(MARKER_PREFIX) {
            let payload_start = start + MARKER_PREFIX.len();
            let Some(end_offset) = remaining[payload_start..].find("]]") else {
                return Err(AhrbError::Protocol(
                    "unterminated AHRB route marker".to_owned(),
                ));
            };
            let end = payload_start + end_offset;
            last = Some(Self::parse_payload(&remaining[payload_start..end])?);
            remaining = &remaining[end + 2..];
        }
        Ok(last)
    }

    fn parse_payload(payload: &str) -> Result<Self> {
        let mut parts = BTreeMap::new();
        for item in payload.split(';') {
            let Some((key, value)) = item.split_once('=') else {
                return Err(AhrbError::Protocol(format!(
                    "invalid AHRB marker component {item:?}"
                )));
            };
            if parts.insert(key, value).is_some() {
                return Err(AhrbError::Protocol(format!(
                    "duplicate AHRB marker field {key:?}"
                )));
            }
        }
        let scenario = parts
            .remove("scenario")
            .ok_or_else(|| AhrbError::Protocol("AHRB marker lacks scenario".to_owned()))?;
        let actor = parts
            .remove("actor")
            .ok_or_else(|| AhrbError::Protocol("AHRB marker lacks actor".to_owned()))?;
        let checkpoint = parts
            .remove("checkpoint")
            .ok_or_else(|| AhrbError::Protocol("AHRB marker lacks checkpoint".to_owned()))?;
        if !parts.is_empty() {
            return Err(AhrbError::Protocol(
                "AHRB marker contains unknown fields".to_owned(),
            ));
        }
        Self::new(scenario, actor, checkpoint)
            .map_err(|error| AhrbError::Protocol(error.to_string()))
    }
}

/// Outcome of accepting a transition into the deterministic state machine.
#[derive(Clone, Debug)]
pub struct AcceptedTransition {
    /// Exact scripted response selected by marker and state.
    pub response: ScriptedResponse,
    /// Whether this was an identical retry of an already accepted request.
    pub retry: bool,
}

/// State-only classification used before accepting a model request.
///
/// This lets the fake provider distinguish a scripted primary request (including an
/// already accepted checkpoint retry) from an auxiliary request without suppressing
/// protocol or canonical-hash errors raised while accepting a primary request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionRecognition {
    /// The marker names the actor's exact next scripted checkpoint.
    Current,
    /// The marker names a checkpoint already accepted for this actor.
    Retry,
    /// The marker does not name the current or an accepted scripted checkpoint.
    NonPrimary,
}

#[derive(Debug, Default)]
struct MachineState {
    next_by_actor: BTreeMap<String, usize>,
    seen: BTreeMap<(String, String), String>,
}

/// Per-actor transition validator. Actors advance independently of request arrival order.
#[derive(Debug)]
pub struct WorkflowMachine {
    scenario: String,
    scripts_by_actor: BTreeMap<String, Vec<ScriptedResponse>>,
    state: Mutex<MachineState>,
}

impl WorkflowMachine {
    /// Build a state machine from a validated workflow.
    pub fn new(workflow: &Workflow) -> Result<Self> {
        workflow.validate()?;
        let mut scripts_by_actor: BTreeMap<String, Vec<ScriptedResponse>> = workflow
            .actors
            .keys()
            .map(|actor| (actor.clone(), Vec::new()))
            .collect();
        for response in &workflow.responses {
            let Some(scripts) = scripts_by_actor.get_mut(&response.actor) else {
                return Err(validation(format!(
                    "script references unknown actor {:?}",
                    response.actor
                )));
            };
            scripts.push(response.clone());
        }
        Ok(Self {
            scenario: workflow.scenario.clone(),
            scripts_by_actor,
            state: Mutex::new(MachineState::default()),
        })
    }

    /// Recognize whether a marker is current, a retry, or non-primary without mutating state.
    pub async fn recognize(&self, marker: &RouteMarker) -> TransitionRecognition {
        if marker.scenario != self.scenario {
            return TransitionRecognition::NonPrimary;
        }
        let Some(scripts) = self.scripts_by_actor.get(&marker.actor) else {
            return TransitionRecognition::NonPrimary;
        };
        let state = self.state.lock().await;
        if state
            .seen
            .contains_key(&(marker.actor.clone(), marker.checkpoint.clone()))
        {
            return TransitionRecognition::Retry;
        }
        let next_index = state.next_by_actor.get(&marker.actor).copied().unwrap_or(0);
        if scripts
            .get(next_index)
            .is_some_and(|response| response.checkpoint == marker.checkpoint)
        {
            TransitionRecognition::Current
        } else {
            TransitionRecognition::NonPrimary
        }
    }

    /// Return the immutable scripted response for a recognized checkpoint.
    ///
    /// The fake provider uses this only for the one context-length retry whose
    /// canonical request must change after deterministic compaction.
    pub fn response_for_checkpoint(&self, marker: &RouteMarker) -> Option<ScriptedResponse> {
        if marker.scenario != self.scenario {
            return None;
        }
        self.scripts_by_actor
            .get(&marker.actor)?
            .iter()
            .find(|response| response.checkpoint == marker.checkpoint)
            .cloned()
    }

    /// Validate and accept one request transition.
    ///
    /// Identical retries of any accepted checkpoint are returned without advancing.
    pub async fn accept(
        &self,
        marker: &RouteMarker,
        canonical_request_hash: &str,
    ) -> Result<AcceptedTransition> {
        if marker.scenario != self.scenario {
            return Err(AhrbError::Protocol(format!(
                "unexpected scenario {:?}; expected {:?}",
                marker.scenario, self.scenario
            )));
        }
        let Some(scripts) = self.scripts_by_actor.get(&marker.actor) else {
            return Err(AhrbError::Protocol(format!(
                "unexpected actor {:?}",
                marker.actor
            )));
        };
        let key = (marker.actor.clone(), marker.checkpoint.clone());
        let mut state = self.state.lock().await;
        if let Some(seen_hash) = state.seen.get(&key) {
            if seen_hash != canonical_request_hash {
                return Err(AhrbError::Protocol(format!(
                    "checkpoint {}/{} was retried with different canonical request",
                    marker.actor, marker.checkpoint
                )));
            }
            let Some(response) = scripts
                .iter()
                .find(|response| response.checkpoint == marker.checkpoint)
            else {
                return Err(AhrbError::Protocol(format!(
                    "accepted checkpoint {}/{} has no response",
                    marker.actor, marker.checkpoint
                )));
            };
            return Ok(AcceptedTransition {
                response: response.clone(),
                retry: true,
            });
        }

        let next_index = match state.next_by_actor.get(&marker.actor) {
            Some(index) => *index,
            None => 0,
        };
        let Some(response) = scripts.get(next_index) else {
            return Err(AhrbError::Protocol(format!(
                "actor {:?} has no remaining transitions (received {:?})",
                marker.actor, marker.checkpoint
            )));
        };
        if response.checkpoint != marker.checkpoint {
            return Err(AhrbError::Protocol(format!(
                "illegal transition for actor {:?}: received checkpoint {:?}, expected {:?}",
                marker.actor, marker.checkpoint, response.checkpoint
            )));
        }
        if !response.request_hash.is_empty()
            && !response
                .request_hash
                .eq_ignore_ascii_case(canonical_request_hash)
        {
            return Err(AhrbError::Protocol(format!(
                "canonical request hash mismatch for {}/{}: got {canonical_request_hash}",
                marker.actor, marker.checkpoint
            )));
        }

        state.seen.insert(key, canonical_request_hash.to_owned());
        state
            .next_by_actor
            .insert(marker.actor.clone(), next_index + 1);
        Ok(AcceptedTransition {
            response: response.clone(),
            retry: false,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct BarrierState {
    reached: BTreeMap<String, BTreeSet<String>>,
    released: BTreeSet<String>,
}

/// State-based named barrier coordination shared by runner and fake server.
#[derive(Clone, Debug)]
pub struct BarrierCoordinator {
    definitions: Arc<BTreeMap<String, Barrier>>,
    state: Arc<Mutex<BarrierState>>,
    changed: Arc<Notify>,
}

impl BarrierCoordinator {
    /// Create barriers from a validated workflow.
    pub fn new(workflow: &Workflow) -> Result<Self> {
        workflow.validate()?;
        Ok(Self {
            definitions: Arc::new(workflow.barriers.clone()),
            state: Arc::new(Mutex::new(BarrierState::default())),
            changed: Arc::new(Notify::new()),
        })
    }

    /// Register an actor at a validated state. Repeated arrivals are idempotent.
    pub async fn arrive(&self, barrier_name: &str, marker: &RouteMarker) -> Result<()> {
        let Some(definition) = self.definitions.get(barrier_name) else {
            return Err(AhrbError::Protocol(format!(
                "unknown barrier {barrier_name:?}"
            )));
        };
        if marker.checkpoint != definition.checkpoint || !definition.actors.contains(&marker.actor)
        {
            return Err(AhrbError::Protocol(format!(
                "actor {:?} at checkpoint {:?} cannot enter barrier {barrier_name:?}",
                marker.actor, marker.checkpoint
            )));
        }
        let changed = {
            let mut state = self.state.lock().await;
            state
                .reached
                .entry(barrier_name.to_owned())
                .or_default()
                .insert(marker.actor.clone())
        };
        if changed {
            self.changed.notify_waiters();
        }
        Ok(())
    }

    /// Return actors registered at a barrier in stable order.
    pub async fn reached(&self, barrier_name: &str) -> Result<Vec<String>> {
        if !self.definitions.contains_key(barrier_name) {
            return Err(AhrbError::Protocol(format!(
                "unknown barrier {barrier_name:?}"
            )));
        }
        let state = self.state.lock().await;
        Ok(match state.reached.get(barrier_name) {
            Some(actors) => actors.iter().cloned().collect(),
            None => Vec::new(),
        })
    }

    /// Return whether every named actor has reached the barrier state.
    pub async fn is_ready(&self, barrier_name: &str) -> Result<bool> {
        let Some(definition) = self.definitions.get(barrier_name) else {
            return Err(AhrbError::Protocol(format!(
                "unknown barrier {barrier_name:?}"
            )));
        };
        let state = self.state.lock().await;
        let reached = state.reached.get(barrier_name);
        Ok(definition
            .actors
            .iter()
            .all(|actor| reached.is_some_and(|actors| actors.contains(actor))))
    }

    /// Wait until all declared actors have reached the barrier.
    pub async fn wait_until_ready(&self, barrier_name: &str) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            if self.is_ready(barrier_name).await? {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Release a ready barrier. Repeated releases are idempotent.
    pub async fn release(&self, barrier_name: &str) -> Result<()> {
        if !self.is_ready(barrier_name).await? {
            return Err(AhrbError::Protocol(format!(
                "barrier {barrier_name:?} cannot be released before all actors arrive"
            )));
        }
        let changed = {
            let mut state = self.state.lock().await;
            state.released.insert(barrier_name.to_owned())
        };
        if changed {
            self.changed.notify_waiters();
        }
        Ok(())
    }

    /// Wait until the runner releases a barrier.
    pub async fn wait_for_release(&self, barrier_name: &str) -> Result<()> {
        if !self.definitions.contains_key(barrier_name) {
            return Err(AhrbError::Protocol(format!(
                "unknown barrier {barrier_name:?}"
            )));
        }
        loop {
            let notified = self.changed.notified();
            if self.state.lock().await.released.contains(barrier_name) {
                return Ok(());
            }
            notified.await;
        }
    }
}

fn validate_component(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(validation(format!(
            "{kind} {value:?} must contain only ASCII letters, digits, '-', '_' or '.'"
        )));
    }
    Ok(())
}

fn validation(message: impl Into<String>) -> AhrbError {
    AhrbError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflow() -> Workflow {
        let mut actors = BTreeMap::new();
        actors.insert(
            "a".to_owned(),
            Actor {
                id: "a".to_owned(),
                parent: None,
                prompt: "hello".to_owned(),
                workspace: "a".to_owned(),
            },
        );
        Workflow {
            version: 1,
            scenario: "scenario".to_owned(),
            actors,
            barriers: BTreeMap::new(),
            responses: vec![ScriptedResponse {
                scenario: "scenario".to_owned(),
                actor: "a".to_owned(),
                checkpoint: "one".to_owned(),
                request_hash: String::new(),
                response: Value::String("done".to_owned()),
                fault: None,
                barrier: None,
            }],
        }
    }

    #[test]
    fn marker_round_trip_uses_last_marker() -> Result<()> {
        let marker = RouteMarker::new("scenario", "a", "one")?;
        let text = format!(
            "old [[AHRB:scenario=x;actor=y;checkpoint=z]] {}",
            marker.encode()
        );
        assert_eq!(RouteMarker::extract(&text)?, Some(marker));
        Ok(())
    }

    #[test]
    fn trickle_fault_round_trips_and_rejects_zero_parameters() -> Result<()> {
        let fault = Fault::Trickle {
            cadence_ms: 1_000,
            count: 20,
        };
        let encoded = serde_json::to_value(&fault)?;
        assert_eq!(
            encoded,
            serde_json::json!({
                "kind": "trickle",
                "cadence_ms": 1_000,
                "count": 20
            })
        );
        assert_eq!(serde_json::from_value::<Fault>(encoded)?, fault);
        fault.validate()?;
        assert!(
            Fault::Trickle {
                cadence_ms: 0,
                count: 1
            }
            .validate()
            .is_err()
        );
        assert!(
            Fault::Trickle {
                cadence_ms: 1,
                count: 0
            }
            .validate()
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn sustained_http_status_fault_accepts_only_retryable_fixture_statuses() -> Result<()> {
        for status in [429, 500] {
            let fault = Fault::SustainedHttpStatus {
                status,
                body: "retry later".to_owned(),
            };
            let encoded = serde_json::to_value(&fault)?;
            assert_eq!(
                encoded.get("kind").and_then(Value::as_str),
                Some("sustained-http-status")
            );
            assert_eq!(serde_json::from_value::<Fault>(encoded)?, fault);
            fault.validate()?;
        }
        assert!(
            Fault::SustainedHttpStatus {
                status: 503,
                body: "unavailable".to_owned(),
            }
            .validate()
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn identical_retry_does_not_advance() -> Result<()> {
        let machine = WorkflowMachine::new(&workflow())?;
        let marker = RouteMarker::new("scenario", "a", "one")?;
        assert_eq!(
            machine.recognize(&marker).await,
            TransitionRecognition::Current
        );
        assert!(!machine.accept(&marker, "hash").await?.retry);
        assert_eq!(
            machine.recognize(&marker).await,
            TransitionRecognition::Retry
        );
        assert!(machine.accept(&marker, "hash").await?.retry);
        assert!(machine.accept(&marker, "different").await.is_err());
        assert_eq!(
            machine
                .recognize(&RouteMarker::new("scenario", "a", "aux")?)
                .await,
            TransitionRecognition::NonPrimary
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_first_checkpoint_keeps_later_checkpoint_non_primary() -> Result<()> {
        let mut scripted = workflow();
        scripted.responses.push(ScriptedResponse {
            scenario: "scenario".to_owned(),
            actor: "a".to_owned(),
            checkpoint: "two".to_owned(),
            request_hash: String::new(),
            response: Value::String("done again".to_owned()),
            fault: None,
            barrier: None,
        });
        let machine = WorkflowMachine::new(&scripted)?;
        let first = RouteMarker::new("scenario", "a", "one")?;
        let second = RouteMarker::new("scenario", "a", "two")?;

        assert_eq!(
            machine.recognize(&second).await,
            TransitionRecognition::NonPrimary
        );
        assert!(!machine.accept(&first, "first-hash").await?.retry);
        assert_eq!(
            machine.recognize(&second).await,
            TransitionRecognition::Current
        );
        Ok(())
    }

    #[tokio::test]
    async fn barrier_requires_all_declared_actor_states() -> Result<()> {
        let mut workflow = workflow();
        workflow.actors.insert(
            "b".to_owned(),
            Actor {
                id: "b".to_owned(),
                parent: None,
                prompt: "hello b".to_owned(),
                workspace: "b".to_owned(),
            },
        );
        workflow.responses[0].barrier = Some("held".to_owned());
        workflow.responses.push(ScriptedResponse {
            scenario: "scenario".to_owned(),
            actor: "b".to_owned(),
            checkpoint: "one".to_owned(),
            request_hash: String::new(),
            response: Value::String("done".to_owned()),
            fault: None,
            barrier: Some("held".to_owned()),
        });
        workflow.barriers.insert(
            "held".to_owned(),
            Barrier {
                name: "held".to_owned(),
                actors: vec!["a".to_owned(), "b".to_owned()],
                checkpoint: "one".to_owned(),
            },
        );
        let barriers = BarrierCoordinator::new(&workflow)?;
        barriers
            .arrive("held", &RouteMarker::new("scenario", "b", "one")?)
            .await?;
        assert!(!barriers.is_ready("held").await?);
        assert!(barriers.release("held").await.is_err());
        barriers
            .arrive("held", &RouteMarker::new("scenario", "a", "one")?)
            .await?;
        assert_eq!(barriers.reached("held").await?, vec!["a", "b"]);
        barriers.release("held").await?;
        barriers.wait_for_release("held").await?;
        Ok(())
    }
}
