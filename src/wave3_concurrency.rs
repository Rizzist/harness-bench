//! Wave-3 concurrency evidence evaluators.
//!
//! This module contains only deterministic calculations over observations made
//! outside the harness.  In particular, the fanout evaluator aggregates fresh-
//! profile repetitions before looking for a cliff, and the fairness evaluator
//! turns a missing-by-deadline terminal into a censored measurement rather than
//! treating a measured timeout as missing evidence.

use crate::cli::Profile;
use crate::report::ResourceSummary;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Required row-54 widths for the quick profile.
pub const QUICK_FANOUT_WIDTHS: [u32; 4] = [1, 2, 4, 8];
/// Number of repetitions at each row-54 width in the quick profile.
pub const QUICK_FANOUT_REPETITIONS: u32 = 3;
/// Number of repetitions at each row-54 width in the certification profile.
pub const CERT_FANOUT_REPETITIONS: u32 = 7;
/// Per-trial outer-deadline allowance beyond the declared turn timeout.
pub const FANOUT_TRIAL_ALLOWANCE_MS: u64 = 3_000;
/// Row-level scheduling allowance beyond the sum of per-trial deadlines.
pub const FANOUT_ROW_ALLOWANCE_MS: u64 = 60_000;

const FOUR_GIB: u64 = 4 * 1_024 * 1_024 * 1_024;

/// Exact row-54 execution plan for a profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FanoutPlan {
    pub widths: Vec<u32>,
    pub repetitions: u32,
}

impl FanoutPlan {
    /// Return the normative quick or certification sweep shape.
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Quick => Self {
                widths: QUICK_FANOUT_WIDTHS.to_vec(),
                repetitions: QUICK_FANOUT_REPETITIONS,
            },
            Profile::Cert => Self {
                widths: (1..=32).collect(),
                repetitions: CERT_FANOUT_REPETITIONS,
            },
        }
    }

    /// Number of fresh-profile width trials required by this plan.
    pub fn trial_count(&self) -> u64 {
        u64::try_from(self.widths.len())
            .map_or(u64::MAX, |count| count)
            .saturating_mul(u64::from(self.repetitions))
    }

    /// Outer deadline for each individual width trial.
    pub fn trial_deadline_ms(turn_timeout_ms: u64) -> u64 {
        turn_timeout_ms.saturating_add(FANOUT_TRIAL_ALLOWANCE_MS)
    }

    /// Row-level deadline for scheduling all required width trials.
    pub fn row_deadline_ms(&self, turn_timeout_ms: u64) -> u64 {
        self.trial_count()
            .saturating_mul(Self::trial_deadline_ms(turn_timeout_ms))
            .saturating_add(FANOUT_ROW_ALLOWANCE_MS)
    }
}

/// One externally measured row-54 trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FanoutTrialEvidence {
    pub repetition: u32,
    pub n: u32,
    pub steady_bytes: u64,
    pub peak_bytes: u64,
    pub active_memory_delta_bytes: u64,
    pub wall_p95_ms: f64,
    pub sampler_cadence_ns: u64,
    pub sampler_sample_count: u32,
    pub sampler_collection_cpu_ns: u64,
    pub sampler_collection_wall_ns: u64,
    pub sampler_observation_wall_ns: u64,
    pub sampler_max_gap_ns: u64,
    /// False is a measured terminalization failure, not missing evidence.
    #[serde(skip)]
    pub terminalized: bool,
}

/// One row-54 point after repetitions at the same width are aggregated.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FanoutPointDetail {
    pub n: u32,
    pub y_rss_bytes: f64,
    pub y_wall_ms: f64,
    pub local_rss_alpha: Option<f64>,
    pub local_wall_alpha: Option<f64>,
    pub rss_increment_bytes: Option<f64>,
    pub wall_increment_ms: Option<f64>,
}

/// Exact typed `details.fanout-cliff` block.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct FanoutCliffDetails {
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
    pub fanout_cliff_n_rss: Option<u32>,
    pub fanout_cliff_n_wall: Option<u32>,
    pub fanout_max_local_rss_alpha: Option<f64>,
    pub fanout_max_local_wall_alpha: Option<f64>,
    pub fanout_global_rss_alpha: Option<f64>,
    pub fanout_max_measured_n: Option<u32>,
    pub trials: Vec<FanoutTrialEvidence>,
    pub points: Vec<FanoutPointDetail>,
}

/// Row-54 headline, numeric mirrors, detail block, and CORE decision.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FanoutCliffEvaluation {
    /// Exactly the three f64 fields allowed in `resource_metrics`.
    pub resource_values: BTreeMap<String, f64>,
    /// Nullable cliff width; retained outside numeric metric maps.
    pub fanout_cliff_n_rss: Option<u32>,
    /// Nullable cliff width; retained outside numeric metric maps.
    pub fanout_cliff_n_wall: Option<u32>,
    pub fanout_max_local_rss_alpha: Option<f64>,
    pub fanout_max_local_wall_alpha: Option<f64>,
    pub fanout_global_rss_alpha: Option<f64>,
    /// Integer maximum width; retained outside numeric metric maps.
    pub fanout_max_measured_n: Option<u32>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

impl FanoutCliffEvaluation {
    fn incomplete(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            details: json!({
                "measurement_complete": false,
                "passed": false,
                "measurement_error": message,
                "fanout_cliff_n_rss": null,
                "fanout_cliff_n_wall": null,
                "fanout_max_local_rss_alpha": null,
                "fanout_max_local_wall_alpha": null,
                "fanout_global_rss_alpha": null,
                "fanout_max_measured_n": null,
                "trials": [],
                "points": [],
            }),
            measurement_error: Some(message),
            ..Self::default()
        }
    }
}

/// Aggregate row-54 trials by width, detect RSS/wall cliffs, and apply its CORE oracle.
pub fn evaluate_fanout_cliff(
    trials: &[FanoutTrialEvidence],
    plan: &FanoutPlan,
) -> FanoutCliffEvaluation {
    if plan.repetitions == 0 || plan.widths.len() < 2 {
        return FanoutCliffEvaluation::incomplete(
            "fanout plan needs at least two widths and one repetition",
        );
    }
    if plan.widths.contains(&0)
        || plan
            .widths
            .windows(2)
            .any(|pair| pair.first() >= pair.get(1))
    {
        return FanoutCliffEvaluation::incomplete(
            "fanout plan widths must be nonzero, unique, and strictly increasing",
        );
    }
    if u64::try_from(trials.len()).ok() != Some(plan.trial_count()) {
        return FanoutCliffEvaluation::incomplete(format!(
            "fanout trial set has {} trials; expected {}",
            trials.len(),
            plan.trial_count()
        ));
    }

    let expected_widths = plan.widths.iter().copied().collect::<BTreeSet<_>>();
    let mut trial_keys = BTreeSet::new();
    for trial in trials {
        if trial.repetition == 0
            || trial.repetition > plan.repetitions
            || !expected_widths.contains(&trial.n)
            || !trial_keys.insert((trial.repetition, trial.n))
        {
            return FanoutCliffEvaluation::incomplete(
                "fanout trial keys do not match the required repetition/width grid",
            );
        }
        if !trial.wall_p95_ms.is_finite()
            || trial.peak_bytes < trial.steady_bytes
            || trial.sampler_cadence_ns == 0
            || trial.sampler_sample_count < 2
            || trial.sampler_observation_wall_ns == 0
            || trial.sampler_collection_cpu_ns.saturating_mul(10)
                > trial.sampler_observation_wall_ns
            || trial.sampler_max_gap_ns > trial.sampler_cadence_ns.saturating_mul(2)
            || trial.sampler_collection_wall_ns > trial.sampler_observation_wall_ns
        {
            return FanoutCliffEvaluation::incomplete(format!(
                "fanout trial repetition {} N={} has invalid measurements or untrustworthy sampler evidence",
                trial.repetition, trial.n
            ));
        }
    }

    let mut points = Vec::with_capacity(plan.widths.len());
    for width in &plan.widths {
        let mut rss = trials
            .iter()
            .filter(|trial| trial.n == *width)
            .map(|trial| trial.active_memory_delta_bytes as f64)
            .collect::<Vec<_>>();
        let mut wall = trials
            .iter()
            .filter(|trial| trial.n == *width)
            .map(|trial| trial.wall_p95_ms)
            .collect::<Vec<_>>();
        rss.sort_by(f64::total_cmp);
        wall.sort_by(f64::total_cmp);
        let Some(y_rss_bytes) = median_sorted(&rss) else {
            return FanoutCliffEvaluation::incomplete(format!(
                "fanout width N={width} has no RSS observations"
            ));
        };
        let Some(y_wall_ms) = median_sorted(&wall) else {
            return FanoutCliffEvaluation::incomplete(format!(
                "fanout width N={width} has no wall observations"
            ));
        };
        if y_rss_bytes <= 0.0 || y_wall_ms <= 0.0 {
            return FanoutCliffEvaluation::incomplete(format!(
                "fanout width N={width} has a nonpositive median-aggregated RSS or wall point"
            ));
        }
        points.push(FanoutPointDetail {
            n: *width,
            y_rss_bytes,
            y_wall_ms,
            local_rss_alpha: None,
            local_wall_alpha: None,
            rss_increment_bytes: None,
            wall_increment_ms: None,
        });
    }
    for index in 1..points.len() {
        let previous = points[index.saturating_sub(1)].clone();
        let point = &mut points[index];
        let log_width_ratio = (f64::from(point.n) / f64::from(previous.n)).ln();
        point.local_rss_alpha =
            Some((point.y_rss_bytes / previous.y_rss_bytes).ln() / log_width_ratio);
        point.local_wall_alpha =
            Some((point.y_wall_ms / previous.y_wall_ms).ln() / log_width_ratio);
        point.rss_increment_bytes = Some(point.y_rss_bytes - previous.y_rss_bytes);
        point.wall_increment_ms = Some(point.y_wall_ms - previous.y_wall_ms);
    }

    let fanout_cliff_n_rss = first_cliff_n(
        &points,
        |point| point.local_rss_alpha,
        |point| point.rss_increment_bytes,
    );
    let fanout_cliff_n_wall = first_cliff_n(
        &points,
        |point| point.local_wall_alpha,
        |point| point.wall_increment_ms,
    );
    let fanout_max_local_rss_alpha =
        maximum_finite(points.iter().filter_map(|point| point.local_rss_alpha));
    let fanout_max_local_wall_alpha =
        maximum_finite(points.iter().filter_map(|point| point.local_wall_alpha));
    let fanout_global_rss_alpha = ols_log_log_alpha(
        &points
            .iter()
            .map(|point| (f64::from(point.n), point.y_rss_bytes))
            .collect::<Vec<_>>(),
    );
    let (Some(max_rss_alpha), Some(max_wall_alpha), Some(global_rss_alpha)) = (
        fanout_max_local_rss_alpha,
        fanout_max_local_wall_alpha,
        fanout_global_rss_alpha,
    ) else {
        return FanoutCliffEvaluation::incomplete(
            "fanout alpha calculation did not produce finite values",
        );
    };
    let n8_peak = trials
        .iter()
        .filter(|trial| trial.n == 8)
        .map(|trial| trial.peak_bytes)
        .max();
    let n8_within_limit =
        !expected_widths.contains(&8) || n8_peak.is_some_and(|peak| peak <= FOUR_GIB);
    let all_terminalized = trials.iter().all(|trial| trial.terminalized);
    let passed = fanout_cliff_n_rss.is_none()
        && fanout_cliff_n_wall.is_none()
        && global_rss_alpha <= 1.20
        && all_terminalized
        && n8_within_limit;
    let fanout_max_measured_n = plan.widths.last().copied();
    let resource_values = BTreeMap::from([
        ("fanout_max_local_rss_alpha".to_owned(), max_rss_alpha),
        ("fanout_max_local_wall_alpha".to_owned(), max_wall_alpha),
        ("fanout_global_rss_alpha".to_owned(), global_rss_alpha),
    ]);
    let details = json!({
        "measurement_complete": true,
        "passed": passed,
        "measurement_error": null,
        "fanout_cliff_n_rss": fanout_cliff_n_rss,
        "fanout_cliff_n_wall": fanout_cliff_n_wall,
        "fanout_max_local_rss_alpha": max_rss_alpha,
        "fanout_max_local_wall_alpha": max_wall_alpha,
        "fanout_global_rss_alpha": global_rss_alpha,
        "fanout_max_measured_n": fanout_max_measured_n,
        "trials": trials,
        "points": points,
    });
    FanoutCliffEvaluation {
        resource_values,
        fanout_cliff_n_rss,
        fanout_cliff_n_wall,
        fanout_max_local_rss_alpha,
        fanout_max_local_wall_alpha,
        fanout_global_rss_alpha,
        fanout_max_measured_n,
        details,
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

fn first_cliff_n<A, I>(points: &[FanoutPointDetail], alpha: A, increment: I) -> Option<u32>
where
    A: Fn(&FanoutPointDetail) -> Option<f64>,
    I: Fn(&FanoutPointDetail) -> Option<f64>,
{
    // `index` is the upper point of an interval.  Index 1 is deliberately
    // skipped: the first interval has neither a previous elasticity nor a
    // prior-increment break test.
    for index in 2..points.len() {
        let current_alpha = alpha(&points[index])?;
        let previous_alpha = alpha(&points[index.saturating_sub(1)])?;
        let elasticity_jump = current_alpha > 1.50 && current_alpha - previous_alpha > 0.35;
        let increment_break = if index >= 3 {
            let mut earlier_positive = points[1..index]
                .iter()
                .filter_map(&increment)
                .filter(|value| *value > 0.0)
                .collect::<Vec<_>>();
            earlier_positive.sort_by(f64::total_cmp);
            let previous_median = median_sorted(&earlier_positive);
            increment(&points[index])
                .is_some_and(|current| previous_median.is_some_and(|prior| current > 2.0 * prior))
        } else {
            false
        };
        if elasticity_jump || increment_break {
            return Some(points[index].n);
        }
    }
    None
}

fn ols_log_log_alpha(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2
        || points
            .iter()
            .any(|(x, y)| !x.is_finite() || !y.is_finite() || *x <= 0.0 || *y <= 0.0)
    {
        return None;
    }
    let count = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x.ln()).sum::<f64>() / count;
    let mean_y = points.iter().map(|(_, y)| y.ln()).sum::<f64>() / count;
    let numerator = points
        .iter()
        .map(|(x, y)| (x.ln() - mean_x) * (y.ln() - mean_y))
        .sum::<f64>();
    let denominator = points
        .iter()
        .map(|(x, _)| (x.ln() - mean_x).powi(2))
        .sum::<f64>();
    if denominator <= 0.0 {
        return None;
    }
    let alpha = numerator / denominator;
    alpha.is_finite().then_some(alpha)
}

fn maximum_finite(values: impl Iterator<Item = f64>) -> Option<f64> {
    values
        .filter(|value| value.is_finite())
        .max_by(f64::total_cmp)
}

/// One scheduled actor's external row-55 timestamps.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FairnessActorEvidence {
    pub repetition: u32,
    pub n: u32,
    pub actor: String,
    /// Missing is an infrastructure error, never a censored latency.
    pub barrier_release_ns: Option<u64>,
    /// Missing, or later than the deadline, is censored at the deadline.
    pub terminal_ns: Option<u64>,
}

/// Normalized actor latency retained in `details.fairness-under-fanout`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FairnessActorLatencyDetail {
    pub repetition: u32,
    pub n: u32,
    pub actor: String,
    pub latency_ms: f64,
    pub censored: bool,
}

/// Per-(repetition,N) fairness statistics.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FairnessGroupDetail {
    pub repetition: u32,
    pub n: u32,
    pub median_ms: f64,
    pub latency_cv: f64,
    pub max_min_ratio: Option<f64>,
    pub spread_ms: f64,
    pub starved_agents: u32,
}

/// Exact typed `details.fairness-under-fanout` block.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct FairnessDetails {
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
    pub clock_resolution_ns: Option<u64>,
    pub fairness_latency_cv: Option<f64>,
    pub fairness_latency_max_min_ratio: Option<f64>,
    pub fairness_latency_spread_ms: Option<f64>,
    pub fairness_starved_agents: Option<u32>,
    pub actor_latencies: Vec<FairnessActorLatencyDetail>,
    pub groups: Vec<FairnessGroupDetail>,
}

/// Row-55 headline, numeric mirrors, detail block, and CORE decision.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FairnessEvaluation {
    /// Exactly CV and spread, the two f64 fields allowed in `resource_metrics`.
    pub resource_values: BTreeMap<String, f64>,
    pub fairness_latency_cv: Option<f64>,
    /// Nullable ratio; retained outside numeric metric maps.
    pub fairness_latency_max_min_ratio: Option<f64>,
    pub fairness_latency_spread_ms: Option<f64>,
    /// Integer counter; retained outside numeric metric maps.
    pub fairness_starved_agents: Option<u32>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

impl FairnessEvaluation {
    fn incomplete(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            details: json!({
                "measurement_complete": false,
                "passed": false,
                "measurement_error": message,
                "clock_resolution_ns": null,
                "fairness_latency_cv": null,
                "fairness_latency_max_min_ratio": null,
                "fairness_latency_spread_ms": null,
                "fairness_starved_agents": null,
                "actor_latencies": [],
                "groups": [],
            }),
            measurement_error: Some(message),
            ..Self::default()
        }
    }
}

/// Evaluate row 55 from the actor boundaries inherited from every row-54 trial.
pub fn evaluate_fairness_under_fanout(
    actors: &[FairnessActorEvidence],
    plan: &FanoutPlan,
    turn_deadline_ms: u64,
    clock_resolution_ns: Option<u64>,
) -> FairnessEvaluation {
    let Some(clock_resolution_ns) = clock_resolution_ns.filter(|resolution| *resolution > 0) else {
        return FairnessEvaluation::incomplete(
            "CLOCK_MONOTONIC resolution is missing or nonpositive",
        );
    };
    if turn_deadline_ms == 0 {
        return FairnessEvaluation::incomplete("fairness turn deadline must be positive");
    }
    let fairness_widths = plan
        .widths
        .iter()
        .copied()
        .filter(|width| *width >= 2)
        .collect::<Vec<_>>();
    if plan.repetitions == 0 || fairness_widths.is_empty() {
        return FairnessEvaluation::incomplete(
            "fairness plan needs an N>=2 width and one repetition",
        );
    }
    let expected_actors = fairness_widths
        .iter()
        .fold(0_u64, |total, width| {
            total.saturating_add(u64::from(*width))
        })
        .saturating_mul(u64::from(plan.repetitions));
    if u64::try_from(actors.len()).ok() != Some(expected_actors) {
        return FairnessEvaluation::incomplete(format!(
            "fairness evidence has {} actors; expected {expected_actors}",
            actors.len()
        ));
    }

    let expected_widths = fairness_widths.iter().copied().collect::<BTreeSet<_>>();
    let mut actor_keys = BTreeSet::new();
    for actor in actors {
        if actor.repetition == 0
            || actor.repetition > plan.repetitions
            || !expected_widths.contains(&actor.n)
            || actor.actor.is_empty()
            || !actor_keys.insert((actor.repetition, actor.n, actor.actor.as_str()))
        {
            return FairnessEvaluation::incomplete(
                "fairness actor keys do not match the required repetition/width grid",
            );
        }
        let Some(release_ns) = actor.barrier_release_ns else {
            return FairnessEvaluation::incomplete(format!(
                "fairness actor {:?} omitted the barrier-release timestamp",
                actor.actor
            ));
        };
        if actor
            .terminal_ns
            .is_some_and(|terminal| terminal < release_ns)
        {
            return FairnessEvaluation::incomplete(format!(
                "fairness actor {:?} terminal precedes barrier release",
                actor.actor
            ));
        }
    }

    let deadline_ns = turn_deadline_ms.saturating_mul(1_000_000);
    let mut actor_latencies = Vec::with_capacity(actors.len());
    let mut groups = Vec::new();
    for repetition in 1..=plan.repetitions {
        for n in &fairness_widths {
            let mut group_evidence = actors
                .iter()
                .filter(|actor| actor.repetition == repetition && actor.n == *n)
                .collect::<Vec<_>>();
            group_evidence.sort_by(|left, right| left.actor.cmp(&right.actor));
            if group_evidence.len() != usize::try_from(*n).map_or(usize::MAX, |count| count) {
                return FairnessEvaluation::incomplete(format!(
                    "fairness group repetition {repetition} N={n} is incomplete"
                ));
            }
            let Some(release_ns) = group_evidence
                .first()
                .and_then(|actor| actor.barrier_release_ns)
            else {
                return FairnessEvaluation::incomplete(format!(
                    "fairness group repetition {repetition} N={n} omitted release time"
                ));
            };
            if group_evidence
                .iter()
                .any(|actor| actor.barrier_release_ns != Some(release_ns))
            {
                return FairnessEvaluation::incomplete(format!(
                    "fairness group repetition {repetition} N={n} has inconsistent release times"
                ));
            }

            let mut group_latencies = Vec::with_capacity(group_evidence.len());
            let mut group_censored = Vec::with_capacity(group_evidence.len());
            for actor in group_evidence {
                let observed_ns = actor
                    .terminal_ns
                    .map(|terminal| terminal.saturating_sub(release_ns));
                let censored = observed_ns.is_none_or(|latency| latency > deadline_ns);
                let latency_ns = observed_ns
                    .filter(|latency| *latency <= deadline_ns)
                    .map_or(deadline_ns, |latency| latency);
                let latency_ms = latency_ns as f64 / 1_000_000.0;
                actor_latencies.push(FairnessActorLatencyDetail {
                    repetition,
                    n: *n,
                    actor: actor.actor.clone(),
                    latency_ms,
                    censored,
                });
                group_latencies.push(latency_ms);
                group_censored.push(censored);
            }
            groups.push(evaluate_fairness_group(
                repetition,
                *n,
                &group_latencies,
                &group_censored,
                clock_resolution_ns as f64 / 1_000_000.0,
            ));
        }
    }

    let fairness_latency_cv = maximum_finite(groups.iter().map(|group| group.latency_cv));
    let fairness_latency_max_min_ratio =
        maximum_finite(groups.iter().filter_map(|group| group.max_min_ratio));
    let fairness_latency_spread_ms = maximum_finite(groups.iter().map(|group| group.spread_ms));
    let fairness_starved_agents = Some(groups.iter().fold(0_u32, |total, group| {
        total.saturating_add(group.starved_agents)
    }));
    let (Some(worst_cv), Some(worst_spread), Some(starved_agents)) = (
        fairness_latency_cv,
        fairness_latency_spread_ms,
        fairness_starved_agents,
    ) else {
        return FairnessEvaluation::incomplete("fairness statistics did not produce finite values");
    };
    let passed = fairness_groups_pass(&groups);
    let resource_values = BTreeMap::from([
        ("fairness_latency_cv".to_owned(), worst_cv),
        ("fairness_latency_spread_ms".to_owned(), worst_spread),
    ]);
    let details = json!({
        "measurement_complete": true,
        "passed": passed,
        "measurement_error": null,
        "clock_resolution_ns": clock_resolution_ns,
        "fairness_latency_cv": worst_cv,
        "fairness_latency_max_min_ratio": fairness_latency_max_min_ratio,
        "fairness_latency_spread_ms": worst_spread,
        "fairness_starved_agents": starved_agents,
        "actor_latencies": actor_latencies,
        "groups": groups,
    });
    FairnessEvaluation {
        resource_values,
        fairness_latency_cv,
        fairness_latency_max_min_ratio,
        fairness_latency_spread_ms,
        fairness_starved_agents,
        details,
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// Read and validate the host's system-wide monotonic-clock resolution.
///
/// A failed call, malformed `timespec`, overflow, or zero resolution is an
/// infrastructure error under row 55 rather than a harness behavior failure.
pub fn monotonic_clock_resolution_ns() -> Result<u64> {
    let mut resolution = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `resolution` is a valid writable timespec and CLOCK_MONOTONIC is
    // a process-independent clock identifier on supported benchmark hosts.
    if unsafe { libc::clock_getres(libc::CLOCK_MONOTONIC, &mut resolution) } != 0 {
        return Err(AhrbError::Io(std::io::Error::last_os_error()));
    }
    let seconds = u64::try_from(resolution.tv_sec).map_err(|_| {
        AhrbError::Validation("CLOCK_MONOTONIC returned a negative resolution".to_owned())
    })?;
    let nanoseconds = u64::try_from(resolution.tv_nsec).map_err(|_| {
        AhrbError::Validation("CLOCK_MONOTONIC returned a negative resolution".to_owned())
    })?;
    if nanoseconds >= 1_000_000_000 {
        return Err(AhrbError::Validation(
            "CLOCK_MONOTONIC returned an invalid nanosecond resolution".to_owned(),
        ));
    }
    let total = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanoseconds))
        .ok_or_else(|| {
            AhrbError::Validation("CLOCK_MONOTONIC resolution overflowed u64".to_owned())
        })?;
    if total == 0 {
        return Err(AhrbError::Validation(
            "CLOCK_MONOTONIC returned a nonpositive resolution".to_owned(),
        ));
    }
    Ok(total)
}

fn evaluate_fairness_group(
    repetition: u32,
    n: u32,
    latencies: &[f64],
    censored: &[bool],
    clock_resolution_ms: f64,
) -> FairnessGroupDetail {
    let mut sorted = latencies.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median_ms = median_sorted(&sorted).map_or(0.0, |value| value);
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let variance = sorted
        .iter()
        .map(|latency| (*latency - mean).powi(2))
        .sum::<f64>()
        / sorted.len() as f64;
    // Equal zero-duration observations have no dispersion.  The ratio remains
    // null because their minimum is below the measured clock resolution.
    let latency_cv = if mean == 0.0 {
        0.0
    } else {
        variance.sqrt() / mean
    };
    let minimum = sorted.first().copied().map_or(0.0, |value| value);
    let maximum = sorted.last().copied().map_or(0.0, |value| value);
    let max_min_ratio = (minimum >= clock_resolution_ms).then_some(maximum / minimum);
    let spread_ms = maximum - minimum;
    let starvation_boundary = (3.0 * median_ms).max(median_ms + 1_000.0);
    let starved_agents = latencies
        .iter()
        .zip(censored)
        .filter(|(latency, censored)| **censored || **latency > starvation_boundary)
        .count() as u32;
    FairnessGroupDetail {
        repetition,
        n,
        median_ms,
        latency_cv,
        max_min_ratio,
        spread_ms,
        starved_agents,
    }
}

fn fairness_groups_pass(groups: &[FairnessGroupDetail]) -> bool {
    groups.iter().fold(0_u32, |total, group| {
        total.saturating_add(group.starved_agents)
    }) == 0
        && groups.iter().all(|group| group.latency_cv <= 0.35)
        && groups.iter().all(|group| group.spread_ms <= 500.0)
        && groups
            .iter()
            .filter_map(|group| group.max_min_ratio)
            .all(|ratio| ratio <= 3.0)
}

fn median_sorted(values: &[f64]) -> Option<f64> {
    let middle = values.len() / 2;
    if values.is_empty() {
        None
    } else if values.len() % 2 == 1 {
        values.get(middle).copied()
    } else {
        let lower = values.get(middle.saturating_sub(1)).copied()?;
        let upper = values.get(middle).copied()?;
        Some(lower + (upper - lower) / 2.0)
    }
}

/// Copy the row-54 values into their typed report-summary fields.
pub fn apply_fanout_cliff_summary(
    summary: &mut ResourceSummary,
    evaluation: &FanoutCliffEvaluation,
) {
    summary.fanout_cliff_n_rss = evaluation.fanout_cliff_n_rss.into();
    summary.fanout_cliff_n_wall = evaluation.fanout_cliff_n_wall.into();
    summary.fanout_max_local_rss_alpha = evaluation.fanout_max_local_rss_alpha;
    summary.fanout_max_local_wall_alpha = evaluation.fanout_max_local_wall_alpha;
    summary.fanout_global_rss_alpha = evaluation.fanout_global_rss_alpha;
    summary.fanout_max_measured_n = evaluation.fanout_max_measured_n;
}

/// Copy the row-55 values into their typed report-summary fields.
pub fn apply_fairness_summary(summary: &mut ResourceSummary, evaluation: &FairnessEvaluation) {
    summary.fairness_latency_cv = evaluation.fairness_latency_cv;
    summary.fairness_latency_max_min_ratio = evaluation.fairness_latency_max_min_ratio.into();
    summary.fairness_latency_spread_ms = evaluation.fairness_latency_spread_ms;
    summary.fairness_starved_agents = evaluation.fairness_starved_agents;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_plan() -> FanoutPlan {
        FanoutPlan {
            widths: QUICK_FANOUT_WIDTHS.to_vec(),
            repetitions: 3,
        }
    }

    fn fanout_trials(rss: [u64; 4], wall: [f64; 4]) -> Vec<FanoutTrialEvidence> {
        let mut trials = Vec::new();
        for repetition in 1..=3 {
            for (index, n) in QUICK_FANOUT_WIDTHS.iter().enumerate() {
                trials.push(FanoutTrialEvidence {
                    repetition,
                    n: *n,
                    steady_bytes: rss[index],
                    peak_bytes: rss[index].saturating_add(1_000),
                    active_memory_delta_bytes: rss[index],
                    wall_p95_ms: wall[index],
                    sampler_cadence_ns: 20_000_000,
                    sampler_sample_count: 10,
                    sampler_collection_cpu_ns: 1_000,
                    sampler_collection_wall_ns: 2_000,
                    sampler_observation_wall_ns: 200_000_000,
                    sampler_max_gap_ns: 20_000_000,
                    terminalized: true,
                });
            }
        }
        trials
    }

    fn fairness_actors(
        plan: &FanoutPlan,
        latency_ms: impl Fn(u32, u32, u32) -> Option<u64>,
    ) -> Vec<FairnessActorEvidence> {
        let mut actors = Vec::new();
        for repetition in 1..=plan.repetitions {
            for n in plan.widths.iter().copied().filter(|width| *width >= 2) {
                for actor in 1..=n {
                    actors.push(FairnessActorEvidence {
                        repetition,
                        n,
                        actor: format!("actor-{actor:02}"),
                        barrier_release_ns: Some(1_000_000),
                        terminal_ns: latency_ms(repetition, n, actor)
                            .map(|latency| 1_000_000 + latency.saturating_mul(1_000_000)),
                    });
                }
            }
        }
        actors
    }

    #[test]
    fn profile_plans_have_exact_widths_trials_and_deadlines() {
        let quick = FanoutPlan::for_profile(Profile::Quick);
        assert_eq!(quick.widths, vec![1, 2, 4, 8]);
        assert_eq!(quick.repetitions, 3);
        assert_eq!(quick.trial_count(), 12);

        let cert = FanoutPlan::for_profile(Profile::Cert);
        assert_eq!(cert.widths, (1..=32).collect::<Vec<_>>());
        assert_eq!(cert.repetitions, 7);
        assert_eq!(cert.trial_count(), 224);
        assert_eq!(
            cert.row_deadline_ms(10_000),
            224 * (10_000 + 3_000) + 60_000
        );
    }

    #[test]
    fn fanout_aggregates_repetitions_before_testing_and_obeys_mirrors() {
        let mut trials = fanout_trials([100, 200, 300, 400], [10.0, 20.0, 30.0, 40.0]);
        // A single wild trial would create a trial-level cliff, but the median
        // point at N=4 remains 300 for RSS and 30 ms for wall.
        if let Some(trial) = trials
            .iter_mut()
            .find(|trial| trial.repetition == 1 && trial.n == 4)
        {
            trial.active_memory_delta_bytes = 40_000;
            trial.steady_bytes = 40_000;
            trial.peak_bytes = 41_000;
            trial.wall_p95_ms = 4_000.0;
        }
        let evaluation = evaluate_fanout_cliff(&trials, &small_plan());
        assert!(evaluation.measurement_complete);
        assert!(evaluation.passed);
        assert_eq!(evaluation.fanout_cliff_n_rss, None);
        assert_eq!(evaluation.fanout_cliff_n_wall, None);
        assert_eq!(evaluation.resource_values.len(), 3);
        assert!(
            !evaluation
                .resource_values
                .contains_key("fanout_cliff_n_rss")
        );
        assert!(
            !evaluation
                .resource_values
                .contains_key("fanout_cliff_n_wall")
        );
        assert!(
            !evaluation
                .resource_values
                .contains_key("fanout_max_measured_n")
        );
        assert!(evaluation.details["fanout_cliff_n_rss"].is_null());
        assert!(evaluation.details["fanout_cliff_n_wall"].is_null());
        assert_eq!(evaluation.details["points"][2]["y_rss_bytes"], 300.0);
    }

    #[test]
    fn first_interval_cannot_declare_a_cliff() {
        let evaluation = evaluate_fanout_cliff(
            &fanout_trials([10, 1_000, 1_000, 1_000], [1.0, 100.0, 100.0, 100.0]),
            &small_plan(),
        );
        assert!(evaluation.measurement_complete);
        assert_eq!(evaluation.fanout_cliff_n_rss, None);
        assert_eq!(evaluation.fanout_cliff_n_wall, None);
    }

    #[test]
    fn elasticity_and_prior_increment_cliff_methods_are_both_active() {
        let points = vec![
            point(1, None, None),
            point(2, Some(1.15), Some(10.0)),
            point(4, Some(1.50), Some(20.0)),
            point(8, Some(1.500_001), Some(40.000_001)),
        ];
        // At N=4, both strict boundaries are equal and therefore do not trip.
        // At N=8 the elasticity jump is too small, but the prior-increment
        // method trips strictly above 2x median(10,20)=30.
        assert_eq!(
            first_cliff_n(
                &points,
                |point| point.local_rss_alpha,
                |point| { point.rss_increment_bytes }
            ),
            Some(8)
        );

        let exactly_twice = vec![
            point(1, None, None),
            point(2, Some(1.15), Some(10.0)),
            point(4, Some(1.50), Some(20.0)),
            point(8, Some(1.50), Some(30.0)),
        ];
        assert_eq!(
            first_cliff_n(
                &exactly_twice,
                |point| point.local_rss_alpha,
                |point| { point.rss_increment_bytes }
            ),
            None
        );

        let elasticity = vec![
            point(1, None, None),
            point(2, Some(1.15), Some(10.0)),
            point(4, Some(1.500_001), Some(1.0)),
            point(8, Some(1.0), Some(1.0)),
        ];
        assert_eq!(
            first_cliff_n(
                &elasticity,
                |point| point.local_rss_alpha,
                |point| { point.rss_increment_bytes }
            ),
            Some(4)
        );
    }

    #[test]
    fn nonpositive_earlier_increments_do_not_create_a_synthetic_baseline() {
        let points = vec![
            point(1, None, None),
            point(2, Some(0.0), Some(-10.0)),
            point(4, Some(0.0), Some(0.0)),
            point(8, Some(1.4), Some(100.0)),
        ];
        assert_eq!(
            first_cliff_n(
                &points,
                |point| point.local_rss_alpha,
                |point| { point.rss_increment_bytes }
            ),
            None
        );
    }

    #[test]
    fn fanout_core_boundaries_fail_only_when_strictly_exceeded() {
        let at_boundary = evaluate_fanout_cliff(
            &fanout_trials(
                [1_000_000, 2_297_397, 5_278_032, 12_125_733],
                [10.0, 20.0, 40.0, 80.0],
            ),
            &small_plan(),
        );
        assert!(at_boundary.measurement_complete);
        // Rounded integer samples approximate y=N^1.2 and remain on the
        // inclusive global-alpha boundary within sampling precision.
        assert!(
            at_boundary
                .fanout_global_rss_alpha
                .is_some_and(|alpha| alpha <= 1.200_001)
        );

        let above = evaluate_fanout_cliff(
            &fanout_trials(
                [1_000_000, 2_378_414, 5_656_854, 13_454_343],
                [10.0, 20.0, 40.0, 80.0],
            ),
            &small_plan(),
        );
        assert!(
            above
                .fanout_global_rss_alpha
                .is_some_and(|alpha| alpha > 1.20)
        );
        assert!(!above.passed);

        let mut peak = fanout_trials([100, 200, 300, 400], [10.0, 20.0, 30.0, 40.0]);
        for trial in peak.iter_mut().filter(|trial| trial.n == 8) {
            trial.peak_bytes = FOUR_GIB;
        }
        assert!(evaluate_fanout_cliff(&peak, &small_plan()).passed);
        peak.iter_mut()
            .filter(|trial| trial.n == 8)
            .for_each(|trial| trial.peak_bytes = FOUR_GIB + 1);
        assert!(!evaluate_fanout_cliff(&peak, &small_plan()).passed);
    }

    #[test]
    fn fanout_rejects_missing_trials_and_nonpositive_aggregates() {
        let mut missing = fanout_trials([100, 200, 400, 800], [10.0, 20.0, 40.0, 80.0]);
        missing.pop();
        assert!(!evaluate_fanout_cliff(&missing, &small_plan()).measurement_complete);

        let zero = fanout_trials([100, 0, 400, 800], [10.0, 20.0, 40.0, 80.0]);
        assert!(!evaluate_fanout_cliff(&zero, &small_plan()).measurement_complete);
    }

    #[test]
    fn fanout_rejects_sampler_overload_and_untrustworthy_cadence() {
        let mut overloaded = fanout_trials([100, 200, 400, 800], [10.0, 20.0, 40.0, 80.0]);
        overloaded[0].sampler_collection_cpu_ns =
            overloaded[0].sampler_observation_wall_ns / 10 + 1;
        assert!(!evaluate_fanout_cliff(&overloaded, &small_plan()).measurement_complete);

        let mut gapped = fanout_trials([100, 200, 400, 800], [10.0, 20.0, 40.0, 80.0]);
        gapped[0].sampler_max_gap_ns = gapped[0].sampler_cadence_ns * 2 + 1;
        assert!(!evaluate_fanout_cliff(&gapped, &small_plan()).measurement_complete);

        let mut undersampled = fanout_trials([100, 200, 400, 800], [10.0, 20.0, 40.0, 80.0]);
        undersampled[0].sampler_sample_count = 1;
        assert!(!evaluate_fanout_cliff(&undersampled, &small_plan()).measurement_complete);
    }

    #[test]
    fn fairness_censors_missing_terminal_at_exact_deadline_and_counts_starvation() {
        let plan = small_plan();
        let actors = fairness_actors(&plan, |repetition, n, actor| {
            (!(repetition == 1 && n == 8 && actor == 8)).then_some(100)
        });
        let evaluation = evaluate_fairness_under_fanout(&actors, &plan, 2_000, Some(1));
        assert!(evaluation.measurement_complete);
        assert!(!evaluation.passed);
        assert_eq!(evaluation.fairness_starved_agents, Some(1));
        let censored = evaluation.details["actor_latencies"]
            .as_array()
            .and_then(|entries| entries.iter().find(|entry| entry["censored"] == true));
        assert!(censored.is_some());
        assert_eq!(
            censored.map(|entry| &entry["latency_ms"]),
            Some(&json!(2_000.0))
        );
    }

    #[test]
    fn fairness_null_ratio_and_numeric_mirrors_follow_schema() {
        let plan = FanoutPlan {
            widths: vec![1, 2],
            repetitions: 1,
        };
        let actors = fairness_actors(&plan, |_, _, actor| Some(actor.saturating_sub(1) as u64));
        let evaluation = evaluate_fairness_under_fanout(&actors, &plan, 2_000, Some(2_000_000));
        assert!(evaluation.measurement_complete);
        assert_eq!(evaluation.fairness_latency_max_min_ratio, None);
        assert_eq!(evaluation.resource_values.len(), 2);
        assert!(
            evaluation
                .resource_values
                .contains_key("fairness_latency_cv")
        );
        assert!(
            evaluation
                .resource_values
                .contains_key("fairness_latency_spread_ms")
        );
        assert!(
            !evaluation
                .resource_values
                .contains_key("fairness_latency_max_min_ratio")
        );
        assert!(
            !evaluation
                .resource_values
                .contains_key("fairness_starved_agents")
        );
    }

    #[test]
    fn fairness_ratio_is_present_at_clock_resolution_boundary() {
        let plan = FanoutPlan {
            widths: vec![1, 2],
            repetitions: 1,
        };
        let actors = fairness_actors(&plan, |_, _, actor| Some(u64::from(actor)));
        let evaluation = evaluate_fairness_under_fanout(&actors, &plan, 2_000, Some(1_000_000));
        assert_eq!(evaluation.fairness_latency_max_min_ratio, Some(2.0));
    }

    #[test]
    fn fairness_starvation_threshold_is_strict() {
        let at = evaluate_fairness_group(1, 4, &[500.0, 500.0, 500.0, 1_500.0], &[false; 4], 0.001);
        assert_eq!(at.median_ms, 500.0);
        assert_eq!(at.starved_agents, 0);
        let above = evaluate_fairness_group(
            1,
            4,
            &[500.0, 500.0, 500.0, 1_500.000_001],
            &[false; 4],
            0.001,
        );
        assert_eq!(above.starved_agents, 1);
    }

    #[test]
    fn fairness_oracle_boundaries_are_inclusive() {
        let groups = [
            FairnessGroupDetail {
                repetition: 1,
                n: 2,
                median_ms: 1.0,
                latency_cv: 0.35,
                max_min_ratio: Some(3.0),
                spread_ms: 500.0,
                starved_agents: 0,
            },
            FairnessGroupDetail {
                latency_cv: 0.350_001,
                ..FairnessGroupDetail {
                    repetition: 1,
                    n: 2,
                    median_ms: 1.0,
                    latency_cv: 0.35,
                    max_min_ratio: Some(3.0),
                    spread_ms: 500.0,
                    starved_agents: 0,
                }
            },
        ];
        assert!(fairness_groups_pass(&groups[..1]));
        assert!(!fairness_groups_pass(&groups[1..]));
    }

    #[test]
    fn fairness_requires_positive_clock_and_release_boundary() {
        let plan = small_plan();
        let actors = fairness_actors(&plan, |_, _, _| Some(100));
        assert!(!evaluate_fairness_under_fanout(&actors, &plan, 2_000, None).measurement_complete);
        assert!(
            !evaluate_fairness_under_fanout(&actors, &plan, 2_000, Some(0)).measurement_complete
        );

        let mut missing_release = actors;
        missing_release[0].barrier_release_ns = None;
        assert!(
            !evaluate_fairness_under_fanout(&missing_release, &plan, 2_000, Some(1))
                .measurement_complete
        );
    }

    #[test]
    fn summary_helpers_preserve_nullable_and_integer_fields() {
        let fanout = evaluate_fanout_cliff(
            &fanout_trials([100, 200, 300, 400], [10.0, 20.0, 30.0, 40.0]),
            &small_plan(),
        );
        let plan = FanoutPlan {
            widths: vec![1, 2],
            repetitions: 1,
        };
        let fairness = evaluate_fairness_under_fanout(
            &fairness_actors(&plan, |_, _, actor| Some(u64::from(actor))),
            &plan,
            2_000,
            Some(1_000_000),
        );
        let mut summary = ResourceSummary::default();
        apply_fanout_cliff_summary(&mut summary, &fanout);
        apply_fairness_summary(&mut summary, &fairness);
        assert_eq!(summary.fanout_cliff_n_rss, None);
        assert_eq!(summary.fanout_max_measured_n, Some(8));
        assert_eq!(summary.fairness_latency_max_min_ratio, Some(2.0));
        assert_eq!(summary.fairness_starved_agents, Some(0));
    }

    #[test]
    fn detail_blocks_validate_and_keep_trial_shape_exact() {
        let fanout = evaluate_fanout_cliff(
            &fanout_trials([100, 200, 300, 400], [10.0, 20.0, 30.0, 40.0]),
            &small_plan(),
        );
        assert!(fanout.details["trials"][0].get("terminalized").is_none());
        let fanout_block = json!({"fanout-cliff": fanout.details});
        assert!(serde_json::from_value::<crate::report::ReportDetails>(fanout_block).is_ok());

        let plan = FanoutPlan {
            widths: vec![1, 2],
            repetitions: 1,
        };
        let fairness = evaluate_fairness_under_fanout(
            &fairness_actors(&plan, |_, _, actor| Some(u64::from(actor))),
            &plan,
            2_000,
            Some(1_000_000),
        );
        let fairness_block = json!({"fairness-under-fanout": fairness.details});
        assert!(serde_json::from_value::<crate::report::ReportDetails>(fairness_block).is_ok());

        let missing_required = json!({
            "fanout-cliff": {
                "measurement_complete": true,
                "passed": true,
                "measurement_error": null,
                "fanout_cliff_n_rss": null,
                "fanout_cliff_n_wall": null,
                "fanout_max_local_rss_alpha": 1.0,
                "fanout_max_local_wall_alpha": 1.0,
                "fanout_global_rss_alpha": 1.0,
                "trials": [],
            }
        });
        assert!(serde_json::from_value::<crate::report::ReportDetails>(missing_required).is_err());
    }

    #[test]
    fn monotonic_clock_resolution_is_strictly_positive() -> Result<()> {
        assert!(monotonic_clock_resolution_ns()? > 0);
        Ok(())
    }

    fn point(
        n: u32,
        local_rss_alpha: Option<f64>,
        rss_increment_bytes: Option<f64>,
    ) -> FanoutPointDetail {
        FanoutPointDetail {
            n,
            y_rss_bytes: 1.0,
            y_wall_ms: 1.0,
            local_rss_alpha,
            local_wall_alpha: local_rss_alpha,
            rss_increment_bytes,
            wall_increment_ms: rss_increment_bytes,
        }
    }
}
