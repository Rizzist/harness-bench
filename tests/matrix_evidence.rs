use ahrb::evaluate::{Pillar, TestOutcome, TestResult, certify};
use ahrb::matrix_evidence::{
    CapabilityStatus, ObservationSet, RowEvidence, capability_for_row, evaluate_row,
    suite_exit_code,
};
use std::path::Path;

fn manifest() -> ahrb::manifest::Manifest {
    ahrb::manifest::load(Path::new("adapters/mock/manifest.toml")).expect("mock manifest")
}

fn empty_row(row: u8) -> RowEvidence {
    let values = ObservationSet::new();
    match row {
        1 => RowEvidence::Routing(values),
        2 => RowEvidence::SingleToolCall(values),
        3 => RowEvidence::SequentialToolCalls(values),
        4 => RowEvidence::ParallelToolCalls(values),
        5 => RowEvidence::FragmentedToolArgs(values),
        6 => RowEvidence::FailedToolResult(values),
        7 => RowEvidence::MalformedToolCall(values),
        8 => RowEvidence::ToolIdDedup(values),
        9 => RowEvidence::TerminalSuccess(values),
        10 => RowEvidence::TerminalFailure(values),
        11 => RowEvidence::UpstreamRetry(values),
        12 => RowEvidence::IdleDeadline(values),
        13 => RowEvidence::WorkspaceEffects(values),
        14 => RowEvidence::StateNetworkConfinement(values),
        15 => RowEvidence::HeadlessWorkflow(values),
        16 => RowEvidence::TranscriptDeterminism(values),
        17 => RowEvidence::ActorIsolation(values),
        18 => RowEvidence::NativeDelegation(values),
        19 => RowEvidence::ExitCodes(values),
        20 => RowEvidence::IdleRss(values),
        21 => RowEvidence::IdleCpu(values),
        22 => RowEvidence::IdleDrift(values),
        23 => RowEvidence::ReturnToIdle(values),
        24 => RowEvidence::ColdStart(values),
        25 => RowEvidence::SingleAgentResource(values),
        26 => RowEvidence::ParallelMemory(values),
        27 => RowEvidence::ScalingCurve(values),
        28 => RowEvidence::PostCloseReclaim(values),
        29 => RowEvidence::LongHorizon(values),
        30 => RowEvidence::SessionReplay(values),
        31 => RowEvidence::Steer(values),
        32 => RowEvidence::Subturn(values),
        33 => RowEvidence::QueuedTurn(values),
        34 => RowEvidence::Noninteractive(values),
        35 => RowEvidence::CrashRecovery(values),
        36 => RowEvidence::CancelCleanup(values),
        37 => RowEvidence::ResumeIdempotency(values),
        38 => RowEvidence::ResourceBounds(values),
        39 => RowEvidence::Hooks(values),
        40 => RowEvidence::DurableJournal(values),
        41 => RowEvidence::ProfileNetworkIsolation(values),
        _ => panic!("test requested invalid row"),
    }
}

#[test]
fn all_41_nominal_payloads_reject_missing_evidence() {
    let manifest = manifest();
    for row in 1_u8..=41 {
        let evidence = empty_row(row);
        assert_eq!(evidence.row(), row);
        let result = evaluate_row(&manifest, row, Some(&evidence));
        assert!(
            !matches!(result.outcome, TestOutcome::Pass),
            "row {row} passed empty evidence"
        );
    }
}

#[test]
fn absent_payload_is_an_error_for_a_supported_row() {
    let result = evaluate_row(&manifest(), 2, None);
    assert!(matches!(result.outcome, TestOutcome::Error(_)));
}

#[test]
fn terminal_only_evidence_cannot_pass_nonterminal_methods() {
    let manifest = manifest();
    let terminal_only = || ObservationSet::new().with_bool("structural_terminal", true);
    let evidence = [
        RowEvidence::FragmentedToolArgs(terminal_only()),
        RowEvidence::ToolIdDedup(terminal_only()),
        RowEvidence::TranscriptDeterminism(terminal_only()),
        RowEvidence::ParallelMemory(terminal_only()),
        RowEvidence::CrashRecovery(terminal_only()),
        RowEvidence::DurableJournal(terminal_only()),
        RowEvidence::ProfileNetworkIsolation(terminal_only()),
    ];
    for evidence in evidence {
        let row = evidence.row();
        let result = evaluate_row(&manifest, row, Some(&evidence));
        assert!(
            matches!(result.outcome, TestOutcome::Fail(_)),
            "row {row} accepted terminal-only evidence: {:?}",
            result.outcome
        );
    }
}

#[test]
fn exact_structured_success_can_pass() {
    let evidence = RowEvidence::TerminalSuccess(
        ObservationSet::new()
            .with_u64("success_terminals", 1)
            .with_u64("failure_terminals", 0)
            .with_bool("machine_parseable", true)
            .with_bool("exit_matches_contract", true)
            .with_bool("later_contradiction", false),
    );
    let result = evaluate_row(&manifest(), 9, Some(&evidence));
    assert!(matches!(result.outcome, TestOutcome::Pass));
}

#[test]
fn capability_resolution_distinguishes_unsupported_and_absent() {
    let original = manifest();
    assert_eq!(
        capability_for_row(&original, 18),
        CapabilityStatus::Supported
    );
    assert_eq!(
        capability_for_row(&original, 18).as_classify_value(),
        Some(true)
    );

    let mut undeclared = original.clone();
    undeclared.capabilities.optional.remove("native_delegation");
    assert!(matches!(
        capability_for_row(&undeclared, 18),
        CapabilityStatus::Unsupported(_)
    ));
    assert_eq!(
        capability_for_row(&undeclared, 18).as_classify_value(),
        Some(false)
    );

    let mut missing_surface = original;
    missing_surface.agents.spawn.clear();
    assert!(matches!(
        capability_for_row(&missing_surface, 18),
        CapabilityStatus::Absent(_)
    ));
    assert_eq!(
        capability_for_row(&missing_surface, 18).as_classify_value(),
        None
    );
}

#[test]
fn unsupported_mandatory_row_blocks_exit_and_badge() {
    let result = TestResult {
        row: 1,
        id: "routing".to_owned(),
        pillar: Pillar::ToolCallCorrectness,
        outcome: TestOutcome::Unsupported("not supported".to_owned()),
        evidence: vec!["capability: explicitly unsupported".to_owned()],
    };
    assert_eq!(suite_exit_code(std::slice::from_ref(&result)), 1);
    assert!(certify(&[result], "macos", "shared-daemon-sessions", 8, 1.0).is_none());
}
