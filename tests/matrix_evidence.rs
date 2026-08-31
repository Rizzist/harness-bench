use ahrb::evaluate::{TestOutcome, TestResult, certify};
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
fn capability_resolution_treats_missing_operations_as_unsupported() {
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
        CapabilityStatus::Unsupported(_)
    ));
    assert_eq!(
        capability_for_row(&missing_surface, 18).as_classify_value(),
        Some(false)
    );
}

#[test]
fn unsupported_operation_is_nonfatal_and_omitted_from_badge_facets() {
    let mut reduced_manifest = manifest();
    reduced_manifest
        .capabilities
        .optional
        .remove("native_delegation");
    reduced_manifest.agents.spawn.clear();
    reduced_manifest.agents.status.clear();
    reduced_manifest.agents.collect.clear();
    let results = ahrb::scenarios::all()
        .iter()
        .map(|definition| TestResult {
            row: definition.row,
            id: definition.id.to_owned(),
            pillar: definition.pillar,
            outcome: if definition.row == 18 {
                TestOutcome::Unsupported("native delegation operations are absent".to_owned())
            } else {
                TestOutcome::Pass
            },
            evidence: vec!["capability: explicit".to_owned()],
            metadata: ahrb::evaluate::TestResultMetadata::for_row(
                definition.row,
                &TestOutcome::Pass,
            ),
        })
        .collect::<Vec<_>>();
    let badge = certify(&results, &reduced_manifest, "macos", 8, 1.0, "L100")
        .expect("unsupported facet does not suppress badge");
    assert_eq!(
        suite_exit_code(&results, Some(&badge), &reduced_manifest),
        0
    );
    assert!(
        !badge
            .facets
            .iter()
            .any(|facet| facet == "native-delegation")
    );
    assert!(badge.facets.iter().any(|facet| facet == "queue"));

    let core_unsupported = results
        .iter()
        .cloned()
        .map(|mut result| {
            if result.row == 40 {
                result.outcome = TestOutcome::Unsupported("durable journal is absent".to_owned());
            }
            result
        })
        .collect::<Vec<_>>();
    assert_eq!(
        suite_exit_code(&core_unsupported, None, &reduced_manifest),
        0
    );
    assert!(
        certify(
            &core_unsupported,
            &reduced_manifest,
            "macos",
            8,
            1.0,
            "L100"
        )
        .is_none()
    );
}

#[test]
fn every_empty_special_operation_surface_is_unsupported() {
    type ManifestMutator = fn(&mut ahrb::manifest::Manifest);
    let original = manifest();
    let cases: [(u8, ManifestMutator); 6] = [
        (18, |item| item.agents.spawn.clear()),
        (30, |item| item.sessions.attach.clear()),
        (31, |item| item.next_input.steer.clear()),
        (33, |item| item.next_input.queue.clear()),
        (36, |item| item.agents.cancel.clear()),
        (37, |item| item.sessions.resume.clear()),
    ];
    for (row, clear) in cases {
        let mut changed = original.clone();
        clear(&mut changed);
        assert!(
            matches!(
                capability_for_row(&changed, row),
                CapabilityStatus::Unsupported(_)
            ),
            "row {row} did not capability-gate its empty operation"
        );
    }
}
