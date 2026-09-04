//! Pure fake-provider-side analysis for the AHRB long-horizon context-fidelity pillar.

use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::ModelRequestRecord;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Stable long-horizon analysis schema.
pub const FIDELITY_SCHEMA_VERSION: u32 = 1;
/// Stable identifier for the standardized long-horizon task.
pub const FIDELITY_TASK_ID: &str = "ahrb-harness-fidelity-longhorizon-v1";
/// Deterministic edit whose receipt is planted as a structural needle.
pub const APPLIED_EDIT_PAYLOAD: &str =
    "AHRB fidelity applied edit payload v1\nfield=preserve-structural-context\n";
/// Honest boundary for all request-content measurements in this pillar.
pub const MEASUREMENT_LABEL: &str = "fake-provider observation of byte presence in serialized harness requests; NOT model understanding, competence, or task success";
/// Interpretation of the headline fraction.
pub const NEEDLE_SURVIVAL_LABEL: &str = "task structural needles byte-present in the final measured primary request / complete task-planted set; undelivered needles count as absent";
/// Interpretation of the ordered needle curve.
pub const SURVIVAL_CURVE_LABEL: &str = "ordered fraction of the complete task-planted structural-needle set byte-present per primary request; undelivered needles count as absent";
/// Interpretation of the general tool-result retention curve.
pub const TOOL_RESULT_RETENTION_LABEL: &str = "byte-identical canonical tool-result carriers present per primary request / unique carriers delivered through that request";
/// Interpretation of the terminal classification.
pub const END_REASON_LABEL: &str = "script-orchestration end state from fake-provider requests, normalized terminals, task deadline, and adapter-declared cap evidence; NOT real-task success";
/// Interpretation of workspace evidence.
pub const WORKSPACE_STATE_LABEL: &str = "pre-cleanup full-tree receipt of the isolated actor workspace; records scripted effects left behind, NOT model choice or task success";

const SIGNATURE_NEEDLE: &str =
    "fn apply_delta(input: &[u8], ordinal: u32) -> Result<EditDigest, FidelityError>";
const ABSOLUTE_PATH_NEEDLE: &str =
    "AHRB-FIDELITY-NEEDLE-ABSOLUTE-PATH /ahrb/fidelity/fixtures/ledger-v1.toml";
const EDIT_DIGEST_NEEDLE: &str = "AHRB-FIDELITY-NEEDLE-DIGEST sha256:30533c9848ceb07a1a1d608e138ed5c3546da76efdf45ec4682d730f8bd124b6";
const ORDINAL_NEEDLE: &str = "AHRB-FIDELITY-NEEDLE-ORDINAL checkpoint-0004";

/// One task-planted structural token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NeedleDefinition {
    /// Stable semantic name.
    pub id: &'static str,
    /// Exact byte sequence searched for in canonical request serialization.
    pub token: &'static str,
}

/// Return the immutable planted set used by task construction and analysis.
pub fn needle_definitions() -> [NeedleDefinition; 4] {
    [
        NeedleDefinition {
            id: "exact-function-signature",
            token: SIGNATURE_NEEDLE,
        },
        NeedleDefinition {
            id: "absolute-fixture-path",
            token: ABSOLUTE_PATH_NEEDLE,
        },
        NeedleDefinition {
            id: "applied-edit-digest",
            token: EDIT_DIGEST_NEEDLE,
        },
        NeedleDefinition {
            id: "ordinal-marker",
            token: ORDINAL_NEEDLE,
        },
    ]
}

/// Per-needle survival evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NeedleSurvival {
    /// Stable semantic name.
    pub id: String,
    /// Exact greppable token searched for as bytes.
    pub token: String,
    /// First primary request with the token in a tool-result carrier; null if never delivered.
    pub planted_turn: Option<u64>,
    /// First later primary request that omitted the token.
    pub first_disappeared_turn: Option<u64>,
    /// Whether the token appeared again after its first observed disappearance.
    pub ever_reappeared: bool,
}

/// Observable reason the scripted fidelity run ended.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FidelityEndReason {
    /// The harness requested the scripted terminal and emitted a success terminal.
    ReachedScriptedTerminal,
    /// Adapter-declared evidence identifies the harness's own request/loop ceiling.
    HarnessInternalCeiling,
    /// AHRB's fidelity-task deadline expired before a terminal was observed.
    AhrbDeadline,
    /// A terminal failure, non-cap process exit, or collection failure ended observation.
    Crashed,
}

/// Process status visible before AHRB performs cleanup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessExitStatus {
    /// This topology exposes no applicable process exit at the observation boundary.
    NotApplicable,
    /// The harness/controller was still running at the observation boundary.
    Running,
    /// The harness exited with a numeric code.
    ExitCode,
    /// The harness exited due to a signal or without a numeric code.
    SignalOrUnknown,
}

/// Whether the full isolated actor-workspace tree changed during the run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkspaceState {
    /// The post-run actor-workspace tree differs from its pre-run state.
    Mutated,
    /// The post-run actor-workspace tree equals its pre-run state.
    Untouched,
}

/// External evidence collected by the runner and consumed by pure analysis.
#[derive(Clone, Debug)]
pub struct FidelityRunEvidence {
    /// Whether the fidelity-task deadline expired.
    pub deadline_reached: bool,
    /// Whether observation ended on a non-timeout collection/protocol error.
    pub collection_failed: bool,
    /// Whether the request count stopped at a declared ceiling until AHRB's deadline.
    pub request_stream_stalled: bool,
    /// Process status before cleanup.
    pub harness_exit_status: HarnessExitStatus,
    /// Numeric process exit when one was available.
    pub harness_exit_code: Option<i32>,
    /// Adapter-declared harness request/loop ceiling.
    pub declared_turn_ceiling: Option<u64>,
    /// Typed exit codes the adapter declares to mean its own request/loop ceiling.
    pub internal_cap_exit_codes: Vec<i32>,
    /// Full isolated actor-workspace state after the run.
    pub workspace_state: WorkspaceState,
    /// Stable digest of the pre-run actor-workspace tree receipt.
    pub workspace_receipt_before_sha256: String,
    /// Stable digest of the post-run actor-workspace tree receipt.
    pub workspace_receipt_after_sha256: String,
}

/// Versioned context-fidelity result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FidelitySummary {
    /// Fidelity summary schema.
    pub schema: u32,
    /// Standardized task identifier.
    pub task: String,
    /// Quick or cert profile.
    pub profile: String,
    /// Planned primary-request envelope for this profile.
    pub turn_budget: u64,
    /// Count of captured primary physical requests.
    pub model_turns: u64,
    /// Honest boundary for all request-content measurements.
    pub measurement_label: String,
    /// Per-needle first-loss and reappearance evidence.
    pub needles: Vec<NeedleSurvival>,
    /// Final-request needle survival fraction.
    pub needle_survival_fraction: f64,
    /// Honest interpretation of `needle_survival_fraction`.
    pub needle_survival_fraction_label: String,
    /// Ordered per-primary-request needle survival fractions.
    pub survival_curve: Vec<f64>,
    /// Honest interpretation of `survival_curve`.
    pub survival_curve_label: String,
    /// Earliest primary request on which any planted needle disappeared.
    pub first_loss_turn: Option<u64>,
    /// Ordered per-primary-request canonical tool-result retention fractions.
    pub retained_tool_result_fraction: Vec<f64>,
    /// Honest interpretation of `retained_tool_result_fraction`.
    pub retained_tool_result_fraction_label: String,
    /// Observable run-end classification.
    pub end_reason: FidelityEndReason,
    /// Honest interpretation of `end_reason`.
    pub end_reason_label: String,
    /// Last captured primary physical request ordinal, or zero with no request.
    pub end_turn: u64,
    /// Harness/controller process status observed before cleanup.
    pub harness_exit_status: HarnessExitStatus,
    /// Numeric exit code when exposed by the topology.
    pub harness_exit_code: Option<i32>,
    /// Whether typed adapter evidence identified the harness's own ceiling.
    pub internal_cap_detected: bool,
    /// Adapter-declared request/loop ceiling, when present.
    pub declared_turn_ceiling: Option<u64>,
    /// Whether the isolated actor workspace changed during the run.
    pub workspace_state: WorkspaceState,
    /// Honest interpretation of `workspace_state` and its receipt digests.
    pub workspace_state_label: String,
    /// Stable digest of the pre-run actor-workspace tree receipt.
    pub workspace_receipt_before_sha256: String,
    /// Stable digest of the post-run actor-workspace tree receipt.
    pub workspace_receipt_after_sha256: String,
}

/// Analyze canonical fake-provider requests plus normalized harness evidence.
pub fn analyze(
    records: &[ModelRequestRecord],
    events: &[NormalizedEvent],
    profile: &str,
    turn_budget: u64,
    evidence: FidelityRunEvidence,
) -> Result<FidelitySummary> {
    let mut primary = records
        .iter()
        .filter(|record| record.role == "primary")
        .collect::<Vec<_>>();
    primary.sort_by(|left, right| {
        (left.received_ns, left.semantic_ordinal, left.attempt).cmp(&(
            right.received_ns,
            right.semantic_ordinal,
            right.attempt,
        ))
    });

    let request_bytes = primary
        .iter()
        .map(|record| serde_json::to_vec(&record.request.canonical).map_err(AhrbError::from))
        .collect::<Result<Vec<_>>>()?;
    let needles = analyze_needles(&primary, &request_bytes)?;
    let survival_curve = needle_survival_curve(&request_bytes, &needles);
    let needle_survival_fraction = survival_curve.last().copied().unwrap_or(0.0);
    let first_loss_turn = needles
        .iter()
        .filter_map(|needle| needle.first_disappeared_turn)
        .min();
    let retained_tool_result_fraction = tool_result_retention_curve(&primary)?;
    let model_turns = u64::try_from(primary.len()).map_err(|_| {
        AhrbError::Protocol("fidelity primary-request count does not fit u64".to_owned())
    })?;
    let scripted_terminal = primary.iter().any(|record| {
        record.accepted
            && record.request.scenario == FIDELITY_TASK_ID
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
    let nonzero_exit = evidence.harness_exit_code.is_some_and(|code| code != 0);
    let cap_exit = evidence
        .harness_exit_code
        .is_some_and(|code| evidence.internal_cap_exit_codes.contains(&code));
    let reached_declared_cap = evidence
        .declared_turn_ceiling
        .is_some_and(|ceiling| model_turns >= ceiling);
    let internal_cap_detected = !scripted_terminal
        && (cap_exit
            || (reached_declared_cap
                && (terminal_abort
                    || nonzero_exit
                    || evidence.collection_failed
                    || evidence.request_stream_stalled)));
    let end_reason = if scripted_terminal && terminal_success {
        FidelityEndReason::ReachedScriptedTerminal
    } else if internal_cap_detected {
        FidelityEndReason::HarnessInternalCeiling
    } else if terminal_abort || nonzero_exit || evidence.collection_failed {
        FidelityEndReason::Crashed
    } else if evidence.deadline_reached {
        FidelityEndReason::AhrbDeadline
    } else {
        FidelityEndReason::Crashed
    };

    Ok(FidelitySummary {
        schema: FIDELITY_SCHEMA_VERSION,
        task: FIDELITY_TASK_ID.to_owned(),
        profile: profile.to_owned(),
        turn_budget,
        model_turns,
        measurement_label: MEASUREMENT_LABEL.to_owned(),
        needles,
        needle_survival_fraction,
        needle_survival_fraction_label: NEEDLE_SURVIVAL_LABEL.to_owned(),
        survival_curve,
        survival_curve_label: SURVIVAL_CURVE_LABEL.to_owned(),
        first_loss_turn,
        retained_tool_result_fraction,
        retained_tool_result_fraction_label: TOOL_RESULT_RETENTION_LABEL.to_owned(),
        end_reason,
        end_reason_label: END_REASON_LABEL.to_owned(),
        end_turn: model_turns,
        harness_exit_status: evidence.harness_exit_status,
        harness_exit_code: evidence.harness_exit_code,
        internal_cap_detected,
        declared_turn_ceiling: evidence.declared_turn_ceiling,
        workspace_state: evidence.workspace_state,
        workspace_state_label: WORKSPACE_STATE_LABEL.to_owned(),
        workspace_receipt_before_sha256: evidence.workspace_receipt_before_sha256,
        workspace_receipt_after_sha256: evidence.workspace_receipt_after_sha256,
    })
}

fn analyze_needles(
    records: &[&ModelRequestRecord],
    requests: &[Vec<u8>],
) -> Result<Vec<NeedleSurvival>> {
    let carriers = records
        .iter()
        .map(|record| tool_result_carriers(&record.request.canonical))
        .collect::<Result<Vec<_>>>()?;
    needle_definitions()
        .into_iter()
        .map(|definition| {
            let planted_index = carriers.iter().position(|current| {
                current
                    .values()
                    .any(|carrier| contains_bytes(carrier, definition.token.as_bytes()))
            });
            let presence = requests
                .iter()
                .map(|request| contains_bytes(request, definition.token.as_bytes()))
                .collect::<Vec<_>>();
            let first_loss_index = planted_index.and_then(|planted| {
                presence
                    .iter()
                    .enumerate()
                    .skip(planted.saturating_add(1))
                    .find_map(|(index, present)| (!present).then_some(index))
            });
            let ever_reappeared = first_loss_index.is_some_and(|loss| {
                presence
                    .iter()
                    .skip(loss.saturating_add(1))
                    .any(|present| *present)
            });
            Ok(NeedleSurvival {
                id: definition.id.to_owned(),
                token: definition.token.to_owned(),
                planted_turn: planted_index.and_then(one_based_turn),
                first_disappeared_turn: first_loss_index.and_then(one_based_turn),
                ever_reappeared,
            })
        })
        .collect()
}

fn needle_survival_curve(requests: &[Vec<u8>], needles: &[NeedleSurvival]) -> Vec<f64> {
    requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let Some(turn) = one_based_turn(index) else {
                return 0.0;
            };
            let present = needles
                .iter()
                .filter(|needle| {
                    needle.planted_turn.is_some_and(|planted| planted <= turn)
                        && contains_bytes(request, needle.token.as_bytes())
                })
                .count();
            if needles.is_empty() {
                0.0
            } else {
                present as f64 / needles.len() as f64
            }
        })
        .collect()
}

fn tool_result_retention_curve(records: &[&ModelRequestRecord]) -> Result<Vec<f64>> {
    let mut delivered = BTreeMap::<String, Vec<u8>>::new();
    let mut curve = Vec::with_capacity(records.len());
    for record in records {
        let current = tool_result_carriers(&record.request.canonical)?;
        for (id, bytes) in &current {
            delivered.entry(id.clone()).or_insert_with(|| bytes.clone());
        }
        let retained = delivered
            .iter()
            .filter(|(id, bytes)| current.get(*id) == Some(*bytes))
            .count();
        let fraction = if delivered.is_empty() {
            1.0
        } else {
            retained as f64 / delivered.len() as f64
        };
        curve.push(fraction);
    }
    Ok(curve)
}

fn tool_result_carriers(value: &Value) -> Result<BTreeMap<String, Vec<u8>>> {
    fn visit(value: &Value, output: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
        match value {
            Value::Array(items) => {
                for item in items {
                    visit(item, output)?;
                }
            }
            Value::Object(object) => {
                let role_is_tool = object.get("role").and_then(Value::as_str) == Some("tool");
                let type_is_result = matches!(
                    object.get("type").and_then(Value::as_str),
                    Some("tool_result" | "function_call_output")
                );
                if role_is_tool || type_is_result {
                    if let Some(id) = ["tool_call_id", "tool_use_id", "call_id", "id"]
                        .into_iter()
                        .find_map(|field| object.get(field).and_then(Value::as_str))
                    {
                        output.insert(id.to_owned(), serde_json::to_vec(value)?);
                    }
                    return Ok(());
                }
                for child in object.values() {
                    visit(child, output)?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }

    let mut output = BTreeMap::new();
    visit(value, &mut output)?;
    Ok(output)
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|candidate| candidate == needle)
}

fn one_based_turn(index: usize) -> Option<u64> {
    u64::try_from(index).ok()?.checked_add(1)
}

/// Stable spelling used by summaries, Markdown, and diffs.
pub fn end_reason_name(reason: FidelityEndReason) -> &'static str {
    match reason {
        FidelityEndReason::ReachedScriptedTerminal => "reached-scripted-terminal",
        FidelityEndReason::HarnessInternalCeiling => "harness-internal-ceiling",
        FidelityEndReason::AhrbDeadline => "ahrb-deadline",
        FidelityEndReason::Crashed => "crashed",
    }
}

/// Stable spelling for pre-cleanup process status.
pub fn harness_exit_status_name(status: HarnessExitStatus) -> &'static str {
    match status {
        HarnessExitStatus::NotApplicable => "not-applicable",
        HarnessExitStatus::Running => "running",
        HarnessExitStatus::ExitCode => "exit-code",
        HarnessExitStatus::SignalOrUnknown => "signal-or-unknown",
    }
}

/// Stable spelling for the actor-workspace receipt state.
pub fn workspace_state_name(state: WorkspaceState) -> &'static str {
    match state {
        WorkspaceState::Mutated => "mutated",
        WorkspaceState::Untouched => "untouched",
    }
}

/// Render the deterministic one-line fidelity block.
pub fn render_summary(summary: &FidelitySummary) -> String {
    format!(
        "fidelity_summary schema={} task={} profile={} model_turns={} turn_budget={} needle_survival_fraction={:.6} first_loss_turn={} retained_tool_result_fraction_last={:.6} end_reason={} end_turn={} harness_exit_status={} harness_exit_code={} internal_cap_detected={} declared_turn_ceiling={} workspace_state={} measurement_label=\"{}\"",
        summary.schema,
        summary.task,
        summary.profile,
        summary.model_turns,
        summary.turn_budget,
        summary.needle_survival_fraction,
        summary
            .first_loss_turn
            .map_or_else(|| "none".to_owned(), |turn| turn.to_string()),
        summary
            .retained_tool_result_fraction
            .last()
            .copied()
            .unwrap_or(0.0),
        end_reason_name(summary.end_reason),
        summary.end_turn,
        harness_exit_status_name(summary.harness_exit_status),
        summary
            .harness_exit_code
            .map_or_else(|| "none".to_owned(), |code| code.to_string()),
        summary.internal_cap_detected,
        summary
            .declared_turn_ceiling
            .map_or_else(|| "none".to_owned(), |turn| turn.to_string()),
        workspace_state_name(summary.workspace_state),
        summary.measurement_label,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_model::{ModelRequest, ModelRequestRecord};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    #[test]
    fn applied_edit_digest_needle_matches_the_scripted_payload() {
        let digest = format!("{:x}", Sha256::digest(APPLIED_EDIT_PAYLOAD.as_bytes()));
        assert!(EDIT_DIGEST_NEEDLE.ends_with(&digest));
    }

    fn record(turn: u64, messages: Value, checkpoint: &str) -> ModelRequestRecord {
        let canonical = json!({"messages":messages});
        ModelRequestRecord {
            request: ModelRequest {
                dialect: "openai-chat-completions".to_owned(),
                endpoint: "/v1/chat/completions".to_owned(),
                model: "fake".to_owned(),
                scenario: FIDELITY_TASK_ID.to_owned(),
                actor: "fidelity".to_owned(),
                checkpoint: checkpoint.to_owned(),
                canonical,
                credential_fingerprint: "redacted".to_owned(),
                stream: false,
            },
            canonical_hash: format!("hash-{turn}"),
            attempts: 1,
            accepted: true,
            semantic_ordinal: turn,
            attempt: 1,
            received_ns: turn,
            body_bytes: 1,
            input_tokens: None,
            role: "primary".to_owned(),
            side_channel_kind: None,
            response_status: Some(200),
            response_headers_ns: None,
            response_first_frame_yield_ns: None,
            response_last_frame_yield_ns: None,
            semantic_attempts_total: 1,
        }
    }

    fn evidence() -> FidelityRunEvidence {
        FidelityRunEvidence {
            deadline_reached: false,
            collection_failed: false,
            request_stream_stalled: false,
            harness_exit_status: HarnessExitStatus::Running,
            harness_exit_code: None,
            declared_turn_ceiling: None,
            internal_cap_exit_codes: Vec::new(),
            workspace_state: WorkspaceState::Mutated,
            workspace_receipt_before_sha256: "before".to_owned(),
            workspace_receipt_after_sha256: "after".to_owned(),
        }
    }

    #[test]
    fn needle_curve_records_a_cliff_and_reappearance() -> Result<()> {
        let combined = needle_definitions()
            .into_iter()
            .map(|needle| needle.token)
            .collect::<Vec<_>>()
            .join(" | ");
        let records = [
            record(1, json!([{"role":"user","content":"start"}]), "bootstrap"),
            record(
                2,
                json!([{"role":"tool","tool_call_id":"a","content":combined}]),
                "cycle",
            ),
            record(3, json!([{"role":"user","content":"compacted"}]), "cycle-2"),
            record(
                4,
                json!([{"role":"user","content":SIGNATURE_NEEDLE}]),
                "cycle-3",
            ),
        ];
        let summary = analyze(&records, &[], "quick", 24, evidence())?;
        assert_eq!(summary.survival_curve, vec![0.0, 1.0, 0.0, 0.25]);
        assert_eq!(summary.first_loss_turn, Some(3));
        assert!(summary.needles[0].ever_reappeared);
        assert!(
            summary.needles[1..]
                .iter()
                .all(|needle| !needle.ever_reappeared)
        );
        Ok(())
    }

    #[test]
    fn a_scripted_call_argument_does_not_plant_a_tool_result_needle() -> Result<()> {
        let records = vec![record(
            1,
            json!([{"role":"assistant","tool_calls":[{"arguments":SIGNATURE_NEEDLE}]}]),
            "bootstrap",
        )];
        let summary = analyze(&records, &[], "quick", 24, evidence())?;
        assert_eq!(summary.needles[0].planted_turn, None);
        assert_eq!(summary.needle_survival_fraction, 0.0);
        assert_eq!(summary.survival_curve, vec![0.0]);
        Ok(())
    }

    #[test]
    fn tool_result_retention_is_byte_exact() -> Result<()> {
        let records = [
            record(
                1,
                json!([{"role":"tool","tool_call_id":"a","content":"one"}]),
                "one",
            ),
            record(
                2,
                json!([
                    {"role":"tool","tool_call_id":"a","content":"one"},
                    {"role":"tool","tool_call_id":"b","content":"two"}
                ]),
                "two",
            ),
            record(
                3,
                json!([{"role":"tool","tool_call_id":"b","content":"changed"}]),
                "three",
            ),
        ];
        let records = records.iter().collect::<Vec<_>>();
        assert_eq!(tool_result_retention_curve(&records)?, vec![1.0, 1.0, 0.0]);
        Ok(())
    }

    #[test]
    fn typed_exit_code_makes_an_undeclared_ceiling_detectable() -> Result<()> {
        let records = (1..=12)
            .map(|turn| record(turn, json!([]), "cycle"))
            .collect::<Vec<_>>();
        let mut run_evidence = evidence();
        run_evidence.harness_exit_status = HarnessExitStatus::ExitCode;
        run_evidence.harness_exit_code = Some(23);
        run_evidence.internal_cap_exit_codes = vec![23];
        let summary = analyze(&records, &[], "quick", 24, run_evidence)?;
        assert!(summary.internal_cap_detected);
        assert_eq!(summary.declared_turn_ceiling, None);
        assert_eq!(
            summary.end_reason,
            FidelityEndReason::HarnessInternalCeiling
        );
        Ok(())
    }

    #[test]
    fn deadline_and_crash_remain_distinct_without_cap_evidence() -> Result<()> {
        let records = vec![record(1, json!([]), "bootstrap")];
        let mut deadline = evidence();
        deadline.deadline_reached = true;
        assert_eq!(
            analyze(&records, &[], "quick", 24, deadline)?.end_reason,
            FidelityEndReason::AhrbDeadline
        );
        assert_eq!(
            analyze(&records, &[], "quick", 24, evidence())?.end_reason,
            FidelityEndReason::Crashed
        );
        let mut exited_at_deadline = evidence();
        exited_at_deadline.deadline_reached = true;
        exited_at_deadline.harness_exit_status = HarnessExitStatus::ExitCode;
        exited_at_deadline.harness_exit_code = Some(7);
        assert_eq!(
            analyze(&records, &[], "quick", 24, exited_at_deadline)?.end_reason,
            FidelityEndReason::Crashed
        );
        let cap_records = (1..=12)
            .map(|turn| record(turn, json!([]), "cycle"))
            .collect::<Vec<_>>();
        let mut stalled_at_cap = evidence();
        stalled_at_cap.deadline_reached = true;
        stalled_at_cap.request_stream_stalled = true;
        stalled_at_cap.declared_turn_ceiling = Some(12);
        let summary = analyze(&cap_records, &[], "quick", 24, stalled_at_cap)?;
        assert!(summary.internal_cap_detected);
        assert_eq!(
            summary.end_reason,
            FidelityEndReason::HarnessInternalCeiling
        );
        Ok(())
    }
}
