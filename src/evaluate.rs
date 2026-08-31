//! Four-pillar evaluation and badge certification types.

use serde::{Deserialize, Serialize};

use crate::manifest::{Manifest, TopologyFamily, topology_family};
use crate::scenarios::{BadgeFacetScope, RequirementKind};

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
    /// Additive v2 result metadata serialized as exact top-level fields.
    #[serde(flatten)]
    pub metadata: TestResultMetadata,
}

/// Additive v2 metadata shared by every matrix result.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestResultMetadata {
    /// `core`, `optional-facet`, or `informational`.
    #[serde(default = "default_core_requirement")]
    pub requirement: String,
    /// Optional capability key associated with the row.
    #[serde(default)]
    pub capability: Option<String>,
    /// Whether the row produced all required observations.
    #[serde(default)]
    pub measurement_complete: bool,
    /// Optional normalized grade in [0,1].
    #[serde(default)]
    pub score: Option<f64>,
    /// Reference-envelope result for informational rows only.
    #[serde(default)]
    pub reference_envelope_pass: Option<bool>,
}

fn default_core_requirement() -> String {
    "core".to_owned()
}

impl Default for TestResultMetadata {
    fn default() -> Self {
        Self {
            requirement: default_core_requirement(),
            capability: None,
            measurement_complete: false,
            score: None,
            reference_envelope_pass: None,
        }
    }
}

impl TestResultMetadata {
    /// Derive exact row metadata from authoritative scenario declarations.
    pub fn for_row(row: u8, outcome: &TestOutcome) -> Self {
        let requirement = crate::scenarios::all()
            .iter()
            .find(|definition| definition.row == row)
            .map(|definition| definition.requirement());
        let (requirement, capability) = match requirement {
            Some(RequirementKind::Core) | None => ("core", None),
            Some(RequirementKind::OptionalFacet { capability }) => {
                ("optional-facet", Some(capability.to_owned()))
            }
            Some(RequirementKind::Informational) => ("informational", None),
        };
        let measurement_complete = !matches!(outcome, TestOutcome::Error(_));
        let informational = matches!(
            crate::scenarios::all()
                .iter()
                .find(|definition| definition.row == row)
                .map(|definition| definition.requirement()),
            Some(RequirementKind::Informational)
        );
        let reference_envelope_pass =
            (informational && measurement_complete).then_some(matches!(outcome, TestOutcome::Pass));
        Self {
            requirement: requirement.to_owned(),
            capability,
            measurement_complete,
            score: None,
            reference_envelope_pass,
        }
    }
}

/// A certified automation-readiness badge.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Badge {
    /// Authoritative benchmark specification version for this badge.
    #[serde(default = "default_badge_spec_version")]
    pub spec_version: u32,
    /// Operating-system label.
    pub os: String,
    /// Process topology label.
    pub topology: String,
    /// Certified parallel width.
    pub parallel_width: usize,
    /// Resource class such as R32 or R256+.
    pub resource_class: String,
    /// Turn-latency class such as L100 or L1000+.
    #[serde(default)]
    pub latency_class: String,
    /// Certified readiness facets.
    pub facets: Vec<String>,
    /// Explicit guard against cross-topology resource ranking.
    #[serde(default)]
    pub comparison_scope: String,
}

fn default_badge_spec_version() -> u32 {
    1
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
    let metadata = TestResultMetadata::for_row(row, &outcome);
    TestResult {
        row,
        id: id.to_owned(),
        pillar,
        outcome,
        evidence,
        metadata,
    }
}

fn certification_topology(manifest: &Manifest) -> Option<TopologyFamily> {
    let family = topology_family(&manifest.concurrency.topology)?;
    let lifecycle_matches = matches!(
        (manifest.daemon.persistent, family),
        (false, TopologyFamily::PerInvocation) | (true, TopologyFamily::SharedController)
    );
    lifecycle_matches.then_some(family)
}

fn result_for_row(results: &[TestResult], row: u8) -> Option<&TestResult> {
    results.iter().find(|result| result.row == row)
}

fn optional_unsupported_is_honest(manifest: &Manifest, result: &TestResult) -> bool {
    let Some(definition) = crate::scenarios::all()
        .iter()
        .find(|definition| definition.row == result.row)
    else {
        return false;
    };
    let RequirementKind::OptionalFacet { capability } = definition.requirement() else {
        return false;
    };
    let declared = manifest.capabilities.required.contains_key(capability)
        || manifest.capabilities.optional.contains_key(capability);
    !declared
        && matches!(result.outcome, TestOutcome::Unsupported(_))
        && matches!(
            crate::matrix_evidence::capability_for_row(manifest, result.row),
            crate::matrix_evidence::CapabilityStatus::Unsupported(_)
        )
}

/// Return whether every topology-independent CORE row passed.
pub fn mandatory_passes(results: &[TestResult], manifest: &Manifest) -> bool {
    certification_topology(manifest).is_some()
        && crate::scenarios::all()
            .iter()
            .filter(|definition| matches!(definition.requirement(), RequirementKind::Core))
            .all(|definition| {
                result_for_row(results, definition.row)
                    .is_some_and(|result| matches!(result.outcome, TestOutcome::Pass))
            })
}

fn badge_compatible_results(results: &[TestResult], manifest: &Manifest) -> bool {
    results.iter().all(|result| {
        if matches!(result.outcome, TestOutcome::Pass) {
            let requirement = crate::scenarios::all()
                .iter()
                .find(|definition| definition.row == result.row)
                .map(|definition| definition.requirement());
            return matches!(
                requirement,
                Some(RequirementKind::Core | RequirementKind::Informational)
            ) || (matches!(requirement, Some(RequirementKind::OptionalFacet { .. }))
                && matches!(
                    crate::matrix_evidence::capability_for_row(manifest, result.row),
                    crate::matrix_evidence::CapabilityStatus::Supported
                ));
        }
        let informational = crate::scenarios::all()
            .iter()
            .find(|definition| definition.row == result.row)
            .is_some_and(|definition| {
                matches!(definition.requirement(), RequirementKind::Informational)
            });
        (informational && matches!(result.outcome, TestOutcome::Fail(_)))
            || optional_unsupported_is_honest(manifest, result)
    })
}

/// Construct a topology-relative badge only when every CORE row passed and
/// every other observed row either passed or was an honestly absent facet.
pub fn certify(
    results: &[TestResult],
    manifest: &Manifest,
    os: &str,
    parallel_width: usize,
    marginal_bytes: f64,
    latency_class: &str,
) -> Option<Badge> {
    // Quick certification uses the required N=1,2,4 sweep; the full certification
    // profile reports N=8. The width remains explicit in every badge label.
    if parallel_width < 4
        || !matches!(latency_class, "L100" | "L250" | "L500" | "L1000" | "L1000+")
        || !mandatory_passes(results, manifest)
        || !badge_compatible_results(results, manifest)
    {
        return None;
    }
    // This class is meaningful only inside the topology printed on the badge.
    // AHRB deliberately has no topology-erasing leaderboard/ranking key.
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
    let mut badge_facets = crate::scenarios::BADGE_FACETS.to_vec();
    badge_facets.sort_by_key(|facet| facet.order);
    let topology_family = certification_topology(manifest)?;
    let facets = badge_facets
        .into_iter()
        .filter(|facet| {
            let topology_applies = matches!(facet.scope, BadgeFacetScope::All)
                || matches!(
                    (facet.scope, topology_family),
                    (
                        BadgeFacetScope::PerInvocation,
                        TopologyFamily::PerInvocation
                    )
                );
            topology_applies
                && result_for_row(results, facet.row)
                    .is_some_and(|result| matches!(result.outcome, TestOutcome::Pass))
                && crate::scenarios::all()
                    .iter()
                    .find(|definition| definition.row == facet.row)
                    .is_some_and(|definition| {
                        matches!(definition.requirement(), RequirementKind::Core)
                            || matches!(
                                crate::matrix_evidence::capability_for_row(manifest, facet.row),
                                crate::matrix_evidence::CapabilityStatus::Supported
                            )
                    })
        })
        .map(|facet| facet.label.to_owned())
        .collect();
    Some(Badge {
        spec_version: 2,
        os: os.to_owned(),
        topology: manifest.concurrency.topology.clone(),
        parallel_width,
        resource_class: resource_class.to_owned(),
        latency_class: latency_class.to_owned(),
        facets,
        comparison_scope: "within-topology-only".to_owned(),
    })
}

/// Return the process status for a completed report. Badge eligibility and
/// process health are separate: only an observed FAIL or ERROR is nonzero.
pub fn suite_exit_code(
    results: &[TestResult],
    _badge: Option<&Badge>,
    _manifest: &Manifest,
) -> i32 {
    let has_failure_or_error = results.iter().any(|result| match result.outcome {
        TestOutcome::Error(_) => true,
        TestOutcome::Fail(_) => crate::scenarios::all()
            .iter()
            .find(|definition| definition.row == result.row)
            .is_none_or(|definition| {
                !matches!(definition.requirement(), RequirementKind::Informational)
            }),
        _ => false,
    });
    if has_failure_or_error { 1 } else { 0 }
}

/// Render the normative badge label.
pub fn badge_label(badge: &Badge) -> String {
    if badge.spec_version == 1 {
        return format!(
            "Automation Ready v1 · {} · {} · N{} · {} · {}",
            badge.os,
            badge.topology,
            badge.parallel_width,
            badge.resource_class,
            badge.facets.join("+")
        );
    }
    let mut label = format!(
        "Automation Ready v2 · {} · {} · N{} · {} · {}",
        badge.os, badge.topology, badge.parallel_width, badge.resource_class, badge.latency_class,
    );
    if !badge.facets.is_empty() {
        label.push_str(" · ");
        label.push_str(&badge.facets.join("+"));
    }
    label
}
