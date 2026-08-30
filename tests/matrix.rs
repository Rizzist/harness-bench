use ahrb::evaluate::{TestOutcome, classify};
use ahrb::scenarios::{PRIORITIZED_ROWS, RequirementKind, all};

#[test]
fn matrix_has_exactly_41_ordered_unique_rows() {
    let tests = all();
    assert_eq!(tests.len(), 41);
    let rows: Vec<u8> = tests.iter().map(|test| test.row).collect();
    assert_eq!(rows, (1_u8..=41).collect::<Vec<_>>());
    let mut ids: Vec<&str> = tests.iter().map(|test| test.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 41);
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
