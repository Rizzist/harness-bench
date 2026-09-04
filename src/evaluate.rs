//! Four-pillar evaluation and badge certification types.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Whether the optional capability was declared by the manifest. This is
    /// null for rows without an optional capability.
    #[serde(default)]
    pub capability_declared: Option<bool>,
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
            capability_declared: None,
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
        let measurement_complete =
            !matches!(outcome, TestOutcome::Error(_) | TestOutcome::Absent(_));
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
            capability_declared: None,
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
    /// Quick or certification profile for every badge component.
    #[serde(default)]
    pub profile: String,
    /// Certified parallel width.
    pub parallel_width: usize,
    /// Resource class such as R32 or R256+.
    pub resource_class: String,
    /// Turn-latency class such as L100 or L1000+.
    #[serde(default)]
    pub latency_class: String,
    /// CPU-per-turn class such as C10 or C250+.
    #[serde(default)]
    pub cpu_class: String,
    /// Equal-weight rows 65 through 72 composite, rounded half up.
    #[serde(default)]
    pub automation_score: u8,
    /// Certified readiness facets.
    pub facets: Vec<String>,
    /// Explicit guard against cross-topology resource ranking.
    #[serde(default)]
    pub comparison_scope: String,
}

/// Topology-scoped G2 score plus staged-rollout provenance.
#[derive(Clone, Debug, PartialEq)]
pub struct AutomationScoreEvaluation {
    /// Composite A score, or unavailable when component evidence is invalid.
    pub score: Option<u8>,
    /// True until all eight component rows are present in the matrix result.
    pub provisional: bool,
    /// Exact normalized component values keyed by row.
    pub components: BTreeMap<u8, f64>,
}

/// Compute the deterministic equal-weight G2 score.
///
/// A verified UNSUPPORTED component maps to zero. Missing, ABSENT, ERROR,
/// incomplete, invalid, or missing required component scores make the composite
/// unavailable. Complete graded PASS/FAIL rows use their explicit score; boolean
/// rows 71 and 72 map PASS/FAIL to one/zero.
pub fn automation_score(results: &[TestResult]) -> AutomationScoreEvaluation {
    let mut components = BTreeMap::new();
    let mut provisional = false;
    for row in 65..=72 {
        let Some(result) = result_for_row(results, row) else {
            provisional = true;
            return AutomationScoreEvaluation {
                score: None,
                provisional,
                components,
            };
        };
        if matches!(
            result.outcome,
            TestOutcome::Error(_) | TestOutcome::Absent(_)
        ) || !result.metadata.measurement_complete
        {
            return AutomationScoreEvaluation {
                score: None,
                provisional,
                components,
            };
        }
        let value = match result.outcome {
            TestOutcome::Unsupported(_) if matches!(row, 65 | 66 | 67 | 68 | 70) => 0.0,
            TestOutcome::Unsupported(_) => {
                return AutomationScoreEvaluation {
                    score: None,
                    provisional,
                    components,
                };
            }
            TestOutcome::Pass if matches!(row, 71 | 72) => 1.0,
            TestOutcome::Fail(_) if matches!(row, 71 | 72) => 0.0,
            TestOutcome::Pass | TestOutcome::Fail(_) => {
                let Some(score) = result.metadata.score else {
                    return AutomationScoreEvaluation {
                        score: None,
                        provisional,
                        components,
                    };
                };
                score
            }
            TestOutcome::Error(_) | TestOutcome::Absent(_) => {
                return AutomationScoreEvaluation {
                    score: None,
                    provisional,
                    components,
                };
            }
        };
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return AutomationScoreEvaluation {
                score: None,
                provisional,
                components,
            };
        }
        components.insert(row, value);
    }
    let total = components.values().copied().sum::<f64>();
    let rounded = (total * 100.0 / 8.0 + 0.5).floor();
    AutomationScoreEvaluation {
        score: Some(rounded.clamp(0.0, 100.0) as u8),
        provisional,
        components,
    }
}

/// Row-69's reference envelope expressed with exact integer component counts.
pub fn event_stream_reference_envelope(components: [bool; 7]) -> bool {
    let passed = components.iter().filter(|value| **value).count();
    passed >= 5 && components[0] && components[1] && components[4] && components[6]
}

/// Row 70's exact count-sensitive PASS boundary.
///
/// The three effect counts are sums over independent trials, so each must
/// equal the repetition count rather than merely being nonzero.
#[allow(clippy::too_many_arguments)]
pub fn headless_permission_model_passes(
    score: f64,
    repetitions: u64,
    tty_prompts: u64,
    allowed_effects: u64,
    denied_filesystem_effects: u64,
    denied_network_effects: u64,
    scope_violations: u64,
) -> bool {
    score.is_finite()
        && (0.50..=1.0).contains(&score)
        && repetitions > 0
        && allowed_effects == repetitions
        && denied_filesystem_effects == repetitions
        && denied_network_effects == repetitions
        && tty_prompts == 0
        && scope_violations == 0
}

/// Row 72's exact non-vacuous, repetition-sensitive PASS boundary.
pub fn tool_result_role_fidelity_passes(
    repetitions: u64,
    checks: u64,
    violations: u64,
    plain_user_text_violations: u64,
    missing_results: u64,
    duplicate_results: u64,
) -> bool {
    repetitions > 0
        && checks == repetitions.saturating_mul(2)
        && violations == 0
        && plain_user_text_violations == 0
        && missing_results == 0
        && duplicate_results == 0
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

fn optional_rows_satisfied(results: &[TestResult], manifest: &Manifest) -> bool {
    crate::scenarios::all()
        .iter()
        .filter(|definition| {
            matches!(
                definition.requirement(),
                RequirementKind::OptionalFacet { .. }
            )
        })
        .all(|definition| {
            let Some(result) = result_for_row(results, definition.row) else {
                return false;
            };
            match crate::matrix_evidence::capability_for_row(manifest, definition.row) {
                crate::matrix_evidence::CapabilityStatus::Supported => {
                    matches!(result.outcome, TestOutcome::Pass)
                }
                crate::matrix_evidence::CapabilityStatus::Unsupported(_) => {
                    optional_unsupported_is_honest(manifest, result)
                }
                crate::matrix_evidence::CapabilityStatus::Absent(_) => false,
            }
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
            || (result.row == 65
                && result.metadata.measurement_complete
                && matches!(result.outcome, TestOutcome::Unsupported(_)))
            || optional_unsupported_is_honest(manifest, result)
    })
}

/// Construct a topology-relative badge only when every CORE row passed and
/// every implemented optional row passed or was honestly undeclared.
#[allow(clippy::too_many_arguments)]
pub fn certify(
    results: &[TestResult],
    manifest: &Manifest,
    os: &str,
    profile: &str,
    parallel_width: usize,
    marginal_bytes: f64,
    latency_class: &str,
    cpu_class: &str,
) -> Option<Badge> {
    let automation = automation_score(results);
    if manifest.identity.schema != 2
        || parallel_width < 8
        || !matches!(latency_class, "L100" | "L250" | "L500" | "L1000" | "L1000+")
        || !matches!(cpu_class, "C10" | "C50" | "C250" | "C250+")
        || automation.score.is_none()
        || !mandatory_passes(results, manifest)
        || !optional_rows_satisfied(results, manifest)
        || !badge_compatible_results(results, manifest)
    {
        return None;
    }
    // This class is meaningful only inside the topology printed on the badge.
    // AHRB deliberately has no topology-erasing leaderboard/ranking key.
    let resource_class = resource_class(marginal_bytes);
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
            let shared_facet_rows_pass = facet.label != "native-delegation"
                || result_for_row(results, 56)
                    .is_some_and(|result| matches!(result.outcome, TestOutcome::Pass));
            topology_applies
                && shared_facet_rows_pass
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
        profile: profile.to_owned(),
        parallel_width,
        resource_class: resource_class.to_owned(),
        latency_class: latency_class.to_owned(),
        cpu_class: cpu_class.to_owned(),
        automation_score: automation.score?,
        facets,
        comparison_scope: "within-topology-only".to_owned(),
    })
}

fn resource_class(marginal_peak_bytes: f64) -> &'static str {
    let peak_mib = marginal_peak_bytes / (1024.0 * 1024.0);
    if peak_mib <= 32.0 {
        "R32"
    } else if peak_mib <= 96.0 {
        "R96"
    } else if peak_mib <= 256.0 {
        "R256"
    } else {
        "R256+"
    }
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
        "Automation Ready v2 · {} · {} · {} · N{} · {} · {} · {} · A{}",
        badge.os,
        badge.topology,
        badge.profile,
        badge.parallel_width,
        badge.resource_class,
        badge.latency_class,
        badge.cpu_class,
        badge.automation_score,
    );
    if !badge.facets.is_empty() {
        label.push_str(" · ");
        label.push_str(&badge.facets.join("+"));
    }
    label
}

#[cfg(test)]
mod automation_tests {
    use super::*;

    fn component(row: u8, outcome: TestOutcome, score: Option<f64>) -> TestResult {
        let mut metadata = TestResultMetadata::for_row(row, &outcome);
        metadata.score = score;
        TestResult {
            row,
            id: format!("component-{row}"),
            pillar: Pillar::AutomationReadiness,
            outcome,
            evidence: Vec::new(),
            metadata,
        }
    }

    #[test]
    fn golden_components_round_half_up() {
        let results = (65..=72)
            .map(|row| component(row, TestOutcome::Pass, Some(0.5)))
            .collect::<Vec<_>>();
        let evaluation = automation_score(&results);
        assert_eq!(evaluation.score, Some(63));
        assert!(!evaluation.provisional);
        assert_eq!(evaluation.components.len(), 8);
    }

    #[test]
    fn absent_missing_or_missing_required_scores_make_composite_unavailable() {
        let results = vec![
            component(65, TestOutcome::Fail("no subscore".to_owned()), None),
            component(66, TestOutcome::Fail("partial".to_owned()), Some(0.5)),
            component(67, TestOutcome::Absent("missing".to_owned()), Some(1.0)),
            component(
                68,
                TestOutcome::Unsupported("undeclared".to_owned()),
                Some(1.0),
            ),
        ];
        let evaluation = automation_score(&results);
        assert_eq!(evaluation.score, None);
        assert!(!evaluation.provisional);
        assert!(evaluation.components.is_empty());
    }

    #[test]
    fn component_error_or_invalid_score_makes_composite_unavailable() {
        let error = component(65, TestOutcome::Error("collector".to_owned()), None);
        assert_eq!(automation_score(&[error]).score, None);
        let invalid = component(65, TestOutcome::Pass, Some(1.01));
        assert_eq!(automation_score(&[invalid]).score, None);
    }

    #[test]
    fn event_stream_envelope_uses_integer_count_and_hard_trio() {
        assert!(event_stream_reference_envelope([
            true, true, false, false, true, true, true
        ]));
        assert!(!event_stream_reference_envelope([
            true, true, true, true, true, true, false
        ]));
        assert!(!event_stream_reference_envelope([
            true, false, true, true, true, true, true
        ]));
    }

    #[test]
    fn permission_boundary_requires_exact_trial_counts() {
        assert!(headless_permission_model_passes(0.50, 5, 0, 5, 5, 5, 0));
        assert!(!headless_permission_model_passes(0.50, 5, 0, 4, 5, 5, 0));
        assert!(!headless_permission_model_passes(0.50, 5, 1, 5, 5, 5, 0));
        assert!(!headless_permission_model_passes(0.25, 5, 0, 5, 5, 5, 0));
    }

    #[test]
    fn tool_role_boundary_requires_two_checks_per_repetition() {
        assert!(tool_result_role_fidelity_passes(5, 10, 0, 0, 0, 0));
        assert!(!tool_result_role_fidelity_passes(5, 9, 0, 0, 0, 0));
        assert!(!tool_result_role_fidelity_passes(0, 0, 0, 0, 0, 0));
        assert!(!tool_result_role_fidelity_passes(5, 10, 1, 0, 0, 0));
    }

    #[test]
    fn badge_rejects_omitted_implemented_optional_rows() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml");
        let manifest = crate::manifest::load(&path).expect("mock manifest should load");
        assert!(!optional_rows_satisfied(&[], &manifest));
    }

    #[test]
    fn resource_classes_use_inclusive_normative_mib_boundaries() {
        let mib = 1024.0 * 1024.0;
        assert_eq!(resource_class(32.0 * mib), "R32");
        assert_eq!(resource_class(32.0 * mib + 1.0), "R96");
        assert_eq!(resource_class(96.0 * mib), "R96");
        assert_eq!(resource_class(96.0 * mib + 1.0), "R256");
        assert_eq!(resource_class(256.0 * mib), "R256");
        assert_eq!(resource_class(256.0 * mib + 1.0), "R256+");
    }
}
