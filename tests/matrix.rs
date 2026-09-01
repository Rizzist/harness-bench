use ahrb::evaluate::{TestOutcome, classify};
use ahrb::scenarios::{PRIORITIZED_ROWS, RequirementKind, all};

#[test]
fn matrix_has_implemented_rows_in_exact_order_with_unique_ids() {
    let tests = all();
    assert_eq!(tests.len(), 57);
    let rows: Vec<u8> = tests.iter().map(|test| test.row).collect();
    let mut expected = (1_u8..=48).collect::<Vec<_>>();
    expected.extend(56_u8..=64);
    assert_eq!(rows, expected);
    let mut ids: Vec<&str> = tests.iter().map(|test| test.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 57);
}

#[test]
fn row_42_is_informational_resource_evidence() {
    let definition = all().iter().find(|test| test.row == 42).expect("row 42");
    assert_eq!(definition.id, "model-request-efficiency");
    assert_eq!(definition.requirement(), RequirementKind::Informational);
}

#[test]
fn row_43_is_informational_resource_evidence() {
    let definition = all().iter().find(|test| test.row == 43).expect("row 43");
    assert_eq!(definition.id, "turn-latency-distribution");
    assert_eq!(definition.requirement(), RequirementKind::Informational);
}

#[test]
fn row_44_is_core_resource_evidence() {
    let definition = all().iter().find(|test| test.row == 44).expect("row 44");
    assert_eq!(definition.id, "process-hygiene");
    assert_eq!(definition.requirement(), RequirementKind::Core);
}

#[test]
fn row_45_is_informational_resource_evidence() {
    let definition = all().iter().find(|test| test.row == 45).expect("row 45");
    assert_eq!(definition.id, "time-to-first-model-request");
    assert_eq!(definition.requirement(), RequirementKind::Informational);
}

#[test]
fn row_46_is_informational_resource_evidence() {
    let definition = all().iter().find(|test| test.row == 46).expect("row 46");
    assert_eq!(definition.id, "memory-time-integral");
    assert_eq!(definition.requirement(), RequirementKind::Informational);
}

#[test]
fn row_63_is_informational_functionality_evidence() {
    let definition = all().iter().find(|test| test.row == 63).expect("row 63");
    assert_eq!(definition.id, "nondeterministic-field-report");
    assert_eq!(definition.requirement(), RequirementKind::Informational);
}

#[test]
fn row_64_is_core_functionality_evidence() {
    let definition = all().iter().find(|test| test.row == 64).expect("row 64");
    assert_eq!(definition.id, "cross-run-reproducibility");
    assert_eq!(definition.requirement(), RequirementKind::Core);
}

#[test]
fn prioritized_rows_are_present_and_core() {
    for row in PRIORITIZED_ROWS {
        let definition = all().iter().find(|test| test.row == *row);
        assert!(definition.is_some(), "missing priority row {row}");
        if let Some(definition) = definition {
            assert_eq!(definition.requirement(), RequirementKind::Core);
        }
    }
}

#[test]
fn metadata_alone_never_passes_a_row() {
    for definition in all() {
        let result = classify(
            definition.row,
            definition.id,
            definition.pillar,
            Some(true),
            &[],
            None,
        );
        assert!(matches!(result.outcome, TestOutcome::Error(_)));
    }
}
