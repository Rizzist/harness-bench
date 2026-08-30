use ahrb::evaluate::{Assertion, Pillar, TestOutcome, badge_label, certify, classify};
use ahrb::report::{Report, render_markdown};

#[test]
fn markdown_sorts_rows_and_names_outcomes() {
    let mut report = Report {
        schema: 1,
        run_id: "deterministic".to_owned(),
        ..Report::default()
    };
    report.results.push(classify(
        2,
        "single-tool-call",
        Pillar::ToolCallCorrectness,
        Some(true),
        &[Assertion {
            name: "effect".to_owned(),
            passed: true,
            detail: "once".to_owned(),
        }],
        None,
    ));
    let markdown = render_markdown(&report);
    assert!(markdown.contains("| 2 | ToolCallCorrectness | `single-tool-call` | PASS |"));
}

#[test]
fn unsupported_is_never_silently_passed() {
    let result = classify(
        18,
        "native-delegation",
        Pillar::Functionality,
        Some(false),
        &[],
        None,
    );
    assert!(matches!(result.outcome, TestOutcome::Unsupported(_)));
}

#[test]
fn resource_class_is_derived_from_marginal_memory() {
    let results: Vec<_> = ahrb::scenarios::all()
        .iter()
        .map(|definition| {
            classify(
                definition.row,
                definition.id,
                definition.pillar,
                Some(true),
                &[Assertion {
                    name: "criterion".to_owned(),
                    passed: true,
                    detail: "met".to_owned(),
                }],
                None,
            )
        })
        .collect();
    let badge = certify(
        &results,
        "macos",
        "shared-daemon-sessions",
        8,
        64.0 * 1024.0 * 1024.0,
    );
    assert!(badge.is_some());
    if let Some(badge) = badge {
        assert!(badge_label(&badge).contains("R96"));
    }
}
