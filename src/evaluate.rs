//! Four-pillar evaluation and badge certification types.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Badge-gating benchmark pillar.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Pillar {
    /// Tool-call correctness.
    ToolCallCorrectness,
    /// Functional orchestration.
    Functionality,
    /// Simulated-workflow resources.
    Resource,
    /// Automation readiness.
    AutomationReadiness,
}

/// The five possible matrix-row classifications.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "class", content = "detail", rename_all = "UPPERCASE")]
pub enum TestOutcome {
    /// Every pass criterion was met.
    Pass,
    /// Harness behavior violated a pass criterion.
    Fail(String),
    /// Harness honestly lacks an optional facet.
    Unsupported(String),
    /// Benchmark infrastructure could not measure the row.
    Error(String),
    /// The declared surface or evidence was absent.
    Absent(String),
}

/// One evaluated matrix row.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestResult {
    /// Matrix row number.
    pub row: u8,
    /// Stable scenario ID.
    pub id: String,
    /// Owning pillar.
    pub pillar: Pillar,
    /// Classification.
    pub outcome: TestOutcome,
    /// Deterministically ordered evidence references.
    pub evidence: Vec<String>,
}

/// A certified automation-readiness badge.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Badge {
    /// Operating-system label.
    pub os: String,
    /// Process topology label.
    pub topology: String,
    /// Certified parallel width.
    pub parallel_width: usize,
    /// Resource class such as R32 or R256+.
    pub resource_class: String,
    /// Certified readiness facets.
    pub facets: Vec<String>,
}

/// A deterministic assertion emitted by a scenario runner.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Assertion {
    /// Stable assertion name.
    pub name: String,
    /// Whether the criterion held.
    pub passed: bool,
    /// Human-readable evidence or diagnostic.
    pub detail: String,
}

/// Classify one row from capability state and evaluated assertions.
pub fn classify(
    row: u8,
    id: &str,
    pillar: Pillar,
    capability: Option<bool>,
    assertions: &[Assertion],
    infrastructure_error: Option<String>,
) -> TestResult {
    let mut evidence: Vec<String> = assertions
        .iter()
        .map(|assertion| format!("{}: {}", assertion.name, assertion.detail))
        .collect();
    evidence.sort();
    let outcome = if let Some(error) = infrastructure_error {
        TestOutcome::Error(error)
    } else if capability.is_none() {
        TestOutcome::Absent(format!("capability declaration missing for {id}"))
    } else if capability == Some(false) {
        TestOutcome::Unsupported(format!("harness declares {id} unsupported"))
    } else if let Some(failure) = assertions.iter().find(|assertion| !assertion.passed) {
        TestOutcome::Fail(format!("{}: {}", failure.name, failure.detail))
    } else if assertions.is_empty() {
        TestOutcome::Error(format!("scenario {id} produced no assertions"))
    } else {
        TestOutcome::Pass
    };
    TestResult {
        row,
        id: id.to_owned(),
        pillar,
        outcome,
        evidence,
    }
}

/// Return whether the complete mandatory matrix can earn a badge.
pub fn mandatory_passes(results: &[TestResult]) -> bool {
    crate::scenarios::all()
        .iter()
        .filter(|definition| definition.mandatory)
        .all(|definition| {
            results.iter().any(|result| {
                result.row == definition.row && matches!(result.outcome, TestOutcome::Pass)
            })
        })
}

/// Construct a badge only when every mandatory row passed.
pub fn certify(
    results: &[TestResult],
    os: &str,
    topology: &str,
    parallel_width: usize,
    marginal_bytes: f64,
) -> Option<Badge> {
    // Quick certification uses the required N=1,2,4 sweep; the full certification
    // profile reports N=8. The width remains explicit in every badge label.
    if parallel_width < 4 || !mandatory_passes(results) {
        return None;
    }
    let mib = marginal_bytes / (1024.0 * 1024.0);
    let resource_class = if mib <= 32.0 {
        "R32"
    } else if mib <= 96.0 {
        "R96"
    } else if mib <= 256.0 {
        "R256"
    } else {
        "R256+"
    };
    let optional_rows: BTreeMap<u8, &str> =
        BTreeMap::from([(18, "native-delegation"), (32, "subturn"), (39, "hooks")]);
    let mut facets = vec![
        "replay".to_owned(),
        "crash".to_owned(),
        "steer".to_owned(),
        "queue".to_owned(),
    ];
    for (row, facet) in optional_rows {
        if results
            .iter()
            .any(|result| result.row == row && matches!(result.outcome, TestOutcome::Pass))
        {
            facets.push(facet.to_owned());
        }
    }
    Some(Badge {
        os: os.to_owned(),
        topology: topology.to_owned(),
        parallel_width,
        resource_class: resource_class.to_owned(),
        facets,
    })
}

/// Render the normative badge label.
pub fn badge_label(badge: &Badge) -> String {
    format!(
        "Automation Ready v1 · {} · {} · N{} · {} · {}",
        badge.os,
        badge.topology,
        badge.parallel_width,
        badge.resource_class,
        badge.facets.join("+")
    )
}
