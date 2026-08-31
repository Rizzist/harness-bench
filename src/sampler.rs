//! Phase-aware resource series, stability checks, and sweep metrics.

use crate::process::Sample;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MIB: f64 = 1_048_576.0;

pub(crate) fn cadence_quality(times: &[u64], cadence_ns: u64) -> (u64, usize, bool) {
    let gaps = times
        .windows(2)
        .map(|pair| pair[1].saturating_sub(pair[0]))
        .collect::<Vec<_>>();
    let maximum_gap_ns = gaps.iter().copied().max().unwrap_or(0);
    let jitter_gaps = gaps.iter().filter(|gap| **gap > cadence_ns).count();
    let allowed_jitter = gaps.len().div_ceil(100).max(1);
    let bounded = maximum_gap_ns <= cadence_ns.saturating_mul(2) && jitter_gaps <= allowed_jitter;
    (maximum_gap_ns, jitter_gaps, bounded)
}

/// Selects the memory counter used for an analysis.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryMetric {
    /// Sum of per-process resident sets.
    Rss,
    /// Sum of per-process proportional sets (Linux).
    Pss,
    /// Sum of physical footprints (macOS).
    Footprint,
    /// Dedicated cgroup-v2 `memory.current`.
    Cgroup,
    /// Preferred comparison metric: PSS, then footprint, then RSS.
    Effective,
}

impl MemoryMetric {
    fn value(self, sample: &Sample) -> Option<u64> {
        match self {
            Self::Rss => Some(sample.rss_bytes),
            Self::Pss => sample.pss_bytes,
            Self::Footprint => sample.footprint_bytes,
            Self::Cgroup => sample.cgroup_memory_bytes,
            Self::Effective => Some(match (sample.pss_bytes, sample.footprint_bytes) {
                (Some(value), _) => value,
                (None, Some(value)) => value,
                (None, None) => sample.rss_bytes,
            }),
        }
    }
}

/// A phase-labeled resource time series.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SampleSeries {
    /// Samples in monotonic time order.
    pub samples: Vec<Sample>,
}

impl SampleSeries {
    /// Append a sample while enforcing monotonic evidence order.
    pub fn push(&mut self, sample: Sample) -> Result<()> {
        if let Some(previous) = self.samples.last() {
            if sample.elapsed_ns < previous.elapsed_ns {
                return Err(AhrbError::Validation(format!(
                    "sample time moved backwards: {} < {}",
                    sample.elapsed_ns, previous.elapsed_ns
                )));
            }
        }
        self.samples.push(sample);
        Ok(())
    }

    /// Analyze every sample carrying `phase` as one plateau candidate.
    pub fn plateau(
        &self,
        phase: &str,
        metric: MemoryMetric,
        minimum_processes: usize,
    ) -> Result<Plateau> {
        self.plateau_in_window(phase, metric, minimum_processes, None)
    }

    /// Analyze the trailing duration of a phase. This is used for the last two
    /// seconds of a three-second barrier hold.
    pub fn trailing_plateau(
        &self,
        phase: &str,
        metric: MemoryMetric,
        minimum_processes: usize,
        trailing_ns: u64,
    ) -> Result<Plateau> {
        if trailing_ns == 0 {
            return Err(AhrbError::Validation(
                "plateau trailing window must be nonzero".to_owned(),
            ));
        }
        let mut phase_times = self
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns);
        let first = phase_times
            .next()
            .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no samples")))?;
        let mut end = first;
        for elapsed_ns in phase_times {
            end = end.max(elapsed_ns);
        }
        if end.saturating_sub(first) < trailing_ns {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} spans {} ns, shorter than the requested {trailing_ns} ns trailing window",
                end.saturating_sub(first)
            )));
        }
        self.plateau_in_window(
            phase,
            metric,
            minimum_processes,
            Some(end.saturating_sub(trailing_ns)),
        )
    }

    /// Compute a least-squares memory drift slope for one phase.
    pub fn drift_bytes_per_minute(&self, phase: &str, metric: MemoryMetric) -> Result<f64> {
        let points: Vec<(f64, f64)> = self
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| {
                metric
                    .value(sample)
                    .map(|bytes| (sample.elapsed_ns as f64 / 60_000_000_000.0, bytes as f64))
                    .ok_or_else(|| missing_metric(metric, phase))
            })
            .collect::<Result<Vec<_>>>()?;
        linear_slope(&points).ok_or_else(|| {
            AhrbError::Validation(format!(
                "phase {phase:?} needs at least two distinct sample times for drift"
            ))
        })
    }

    /// Calculate the whole-tree CPU delta and fraction of one core for a phase.
    pub fn phase_cpu(&self, phase: &str) -> Result<PhaseCpu> {
        let mut samples = self.samples.iter().filter(|sample| sample.phase == phase);
        let first = samples
            .next()
            .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no samples")))?;
        let mut last = first;
        let mut sample_count = 1_usize;
        for sample in samples {
            last = sample;
            sample_count = sample_count.saturating_add(1);
        }
        if sample_count < 2 || last.elapsed_ns <= first.elapsed_ns {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} needs at least two distinct sample times for CPU"
            )));
        }
        if last.cpu_ns < first.cpu_ns {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} cumulative CPU moved backwards: {} < {}",
                last.cpu_ns, first.cpu_ns
            )));
        }
        let elapsed_ns = last.elapsed_ns - first.elapsed_ns;
        let cpu_ns = last.cpu_ns - first.cpu_ns;
        let one_core_fraction = cpu_ns as f64 / elapsed_ns as f64;
        Ok(PhaseCpu {
            elapsed_ns,
            cpu_ns,
            one_core_fraction,
        })
    }

    /// Verify that one phase was sampled continuously for its required duration.
    ///
    /// The first and last samples are the phase-boundary observations. Because a
    /// periodic sampler can finish up to one cadence before the controller changes
    /// phase, one cadence is allowed at the trailing boundary. Any within-phase gap
    /// larger than the requested cadence is a missed collection.
    pub fn phase_coverage(
        &self,
        phase: &str,
        cadence_ns: u64,
        required_duration_ns: u64,
    ) -> Result<PhaseCoverage> {
        if cadence_ns == 0 {
            return Err(AhrbError::Validation(
                "sampling cadence must be nonzero".to_owned(),
            ));
        }
        if required_duration_ns == 0 {
            return Err(AhrbError::Validation(
                "required phase duration must be nonzero".to_owned(),
            ));
        }
        let times: Vec<u64> = self
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .collect();
        if times.len() < 2 {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} needs at least two boundary samples"
            )));
        }
        if times.windows(2).any(|pair| pair[1] <= pair[0]) {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} sample times are not strictly increasing"
            )));
        }
        let start_ns = times.first().copied().unwrap_or(0);
        let end_ns = times.last().copied().unwrap_or(start_ns);
        let observed_duration_ns = end_ns.saturating_sub(start_ns);
        let (maximum_gap_ns, cadence_gaps, cadence_trustworthy) =
            cadence_quality(&times, cadence_ns);
        let duration_covered =
            observed_duration_ns.saturating_add(cadence_ns) >= required_duration_ns;
        Ok(PhaseCoverage {
            sample_count: times.len(),
            start_ns,
            end_ns,
            observed_duration_ns,
            required_duration_ns,
            maximum_gap_ns,
            cadence_gaps,
            duration_covered,
            trustworthy: duration_covered && cadence_trustworthy,
        })
    }

    /// Detect sampler-thread CPU cost and cadence overruns.
    pub fn sampling_health(&self, cadence_ns: u64) -> Result<SamplingHealth> {
        if cadence_ns == 0 {
            return Err(AhrbError::Validation(
                "sampling cadence must be nonzero".to_owned(),
            ));
        }
        let collection_ns = self.samples.iter().fold(0_u64, |total, sample| {
            total.saturating_add(sample.collection_ns)
        });
        let mut observation_ns = 0_u64;
        let mut segment_times = Vec::new();
        let mut cadence_gaps = 0_usize;
        let mut cadence_untrustworthy = false;
        let mut previous: Option<&Sample> = None;
        for sample in &self.samples {
            match previous {
                Some(prior) if prior.phase == sample.phase => {
                    let gap = sample.elapsed_ns.saturating_sub(prior.elapsed_ns);
                    observation_ns = observation_ns.saturating_add(gap);
                    segment_times.push(sample.elapsed_ns);
                }
                _ => {
                    if !segment_times.is_empty() {
                        let (_, gaps, trustworthy) = cadence_quality(&segment_times, cadence_ns);
                        cadence_gaps = cadence_gaps.saturating_add(gaps);
                        cadence_untrustworthy |= !trustworthy;
                    }
                    segment_times.clear();
                    segment_times.push(sample.elapsed_ns);
                    observation_ns = observation_ns.saturating_add(cadence_ns);
                }
            }
            previous = Some(sample);
        }
        if !segment_times.is_empty() {
            let (_, gaps, trustworthy) = cadence_quality(&segment_times, cadence_ns);
            cadence_gaps = cadence_gaps.saturating_add(gaps);
            cadence_untrustworthy |= !trustworthy;
        }
        let cadence_overruns = self
            .samples
            .iter()
            .filter(|sample| sample.collection_wall_ns > cadence_ns)
            .count();
        // Periodic collectors store direct thread CPU time in `collection_ns`.
        let collection_cpu_fraction = if observation_ns == 0 {
            0.0
        } else {
            collection_ns as f64 / observation_ns as f64
        };
        Ok(SamplingHealth {
            collection_ns,
            observation_ns,
            collection_cpu_fraction,
            cadence_overruns,
            cadence_gaps,
            overloaded: collection_cpu_fraction > 0.10
                || cadence_overruns > 0
                || cadence_untrustworthy,
        })
    }

    fn plateau_in_window(
        &self,
        phase: &str,
        metric: MemoryMetric,
        minimum_processes: usize,
        start_ns: Option<u64>,
    ) -> Result<Plateau> {
        let selected: Vec<&Sample> = self
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .filter(|sample| start_ns.is_none_or(|start| sample.elapsed_ns >= start))
            .collect();
        if selected.is_empty() {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} has no samples in the requested plateau window"
            )));
        }
        let mut values = Vec::with_capacity(selected.len());
        for sample in &selected {
            values.push(
                metric
                    .value(sample)
                    .ok_or_else(|| missing_metric(metric, phase))?,
            );
        }
        values.sort_unstable();
        let median_bytes = median_u64(&values).ok_or_else(|| {
            AhrbError::Validation("cannot calculate a plateau from no values".to_owned())
        })?;
        let p05_bytes = percentile_nearest_rank(&values, 5).ok_or_else(|| {
            AhrbError::Validation("cannot calculate the fifth percentile".to_owned())
        })?;
        let p95_bytes = percentile_nearest_rank(&values, 95).ok_or_else(|| {
            AhrbError::Validation("cannot calculate the ninety-fifth percentile".to_owned())
        })?;
        let spread = p95_bytes.saturating_sub(p05_bytes);
        let relative_spread = if median_bytes == 0 {
            if spread == 0 { 0.0 } else { f64::MAX }
        } else {
            spread as f64 / median_bytes as f64
        };
        let minimum_observed_processes = selected
            .iter()
            .map(|sample| {
                sample
                    .processes
                    .iter()
                    .map(|process| process.identity)
                    .collect::<BTreeSet<_>>()
                    .len()
            })
            .min()
            .map_or(0, |value| value);
        let all_expected_present = minimum_observed_processes >= minimum_processes;
        let distinct_sample_times = selected
            .iter()
            .map(|sample| sample.elapsed_ns)
            .collect::<BTreeSet<_>>()
            .len();
        let has_multiple_sample_times = distinct_sample_times >= 2;
        Ok(Plateau {
            metric,
            sample_count: values.len(),
            distinct_sample_times,
            minimum_observed_processes,
            median_bytes,
            p05_bytes,
            p95_bytes,
            relative_spread,
            all_expected_present,
            has_multiple_sample_times,
            trustworthy: relative_spread <= 0.05
                && all_expected_present
                && has_multiple_sample_times,
        })
    }
}

/// Stability statistics for a candidate plateau window.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Plateau {
    /// Counter analyzed.
    pub metric: MemoryMetric,
    /// Samples in the candidate window.
    pub sample_count: usize,
    /// Number of distinct monotonic timestamps represented by the window.
    pub distinct_sample_times: usize,
    /// Smallest whole-tree membership observed in the window.
    pub minimum_observed_processes: usize,
    /// Median bytes.
    pub median_bytes: u64,
    /// Fifth percentile bytes.
    pub p05_bytes: u64,
    /// Ninety-fifth percentile bytes.
    pub p95_bytes: u64,
    /// Relative P95-P5 spread.
    pub relative_spread: f64,
    /// Whether every sample met the caller's membership requirement.
    pub all_expected_present: bool,
    /// Whether the window contains at least two distinct sample times.
    pub has_multiple_sample_times: bool,
    /// Whether the window has temporal coverage, spread is at most five percent,
    /// and all expected processes were present.
    pub trustworthy: bool,
}

/// CPU consumed during one phase.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct PhaseCpu {
    /// Monotonic duration covered by the first and last samples.
    pub elapsed_ns: u64,
    /// Delta of cumulative whole-tree CPU.
    pub cpu_ns: u64,
    /// CPU delta divided by wall duration (1.0 is one fully busy core).
    pub one_core_fraction: f64,
}

/// Boundary, duration, and cadence evidence for one phase window.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct PhaseCoverage {
    /// Samples carrying this phase, including the boundary observations.
    pub sample_count: usize,
    /// First phase sample in monotonic nanoseconds.
    pub start_ns: u64,
    /// Last phase sample in monotonic nanoseconds.
    pub end_ns: u64,
    /// Difference between the first and last boundary samples.
    pub observed_duration_ns: u64,
    /// Minimum duration required by the selected profile.
    pub required_duration_ns: u64,
    /// Largest interval between consecutive phase samples.
    pub maximum_gap_ns: u64,
    /// Intervals larger than the requested cadence.
    pub cadence_gaps: usize,
    /// Whether boundary coverage reaches the required duration within one cadence.
    pub duration_covered: bool,
    /// Whether duration and cadence coverage are both trustworthy.
    pub trustworthy: bool,
}

/// Evidence that the sampler itself did not distort the benchmark.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SamplingHealth {
    /// Sum of measured collection durations.
    pub collection_ns: u64,
    /// Time span represented by the series.
    pub observation_ns: u64,
    /// Aggregate sampler thread-CPU fraction of one core.
    pub collection_cpu_fraction: f64,
    /// Samples whose wall collection duration exceeded the requested cadence.
    pub cadence_overruns: usize,
    /// Within-phase intervals larger than the requested cadence.
    pub cadence_gaps: usize,
    /// Whether the run must become `ERROR: sampler overload`.
    pub overloaded: bool,
}

/// One N-point in a parallel-agent resource sweep.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SweepPoint {
    /// Simultaneous agents at the shared state barrier.
    pub agents: u32,
    /// Warm idle baseline B.
    pub baseline_bytes: u64,
    /// Barrier steady median S_n.
    pub steady_bytes: u64,
    /// Workload peak P_n.
    pub workload_peak_bytes: u64,
    /// Cold peak C_n.
    pub cold_peak_bytes: u64,
    /// Post-turn retention I_n.
    pub post_turn_bytes: u64,
    /// Post-close plateau R_n.
    pub post_close_bytes: u64,
}

/// Derived measurements for one sweep point.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct PointMetrics {
    /// `(S_n-B)/N`.
    pub average_added_bytes_per_agent: f64,
    /// Marginal bytes per additional agent from the preceding N point.
    pub adjacent_marginal_bytes_per_agent: Option<f64>,
    /// `P_n/S_n` when S_n is nonzero.
    pub peak_amplification: Option<f64>,
    /// `I_n-B`, saturated at zero.
    pub post_turn_retained_bytes: u64,
    /// `max(0,R_n-B)`.
    pub post_close_residual_bytes: u64,
    /// Residual divided by N.
    pub residual_bytes_per_agent: f64,
    /// `(S_n-R_n)/(S_n-B)`, clamped to zero through one.
    pub reclaim_ratio: f64,
}

/// Active delta below which a sweep width carries no fittable scaling signal.
///
/// A log-log exponent over deltas of a few hundred KiB is dominated by the
/// baseline's own jitter (observed: a ~10 MiB daemon whose per-width baseline
/// moved by more than its N=1 delta), so widths under this floor are reported
/// as flat rather than fitted.
pub const SCALING_NOISE_FLOOR_BYTES: u64 = 4 * 1_048_576;

/// Parallel-agent scaling and reclaim metrics for a complete sweep.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SweepMetrics {
    /// Input points sorted by N and their derived metrics.
    pub points: Vec<(SweepPoint, PointMetrics)>,
    /// Theil-Sen slope of steady bytes versus N.
    pub headline_beta_bytes_per_agent: Option<f64>,
    /// The same headline slope in MiB per agent.
    pub headline_beta_mib_per_agent: Option<f64>,
    /// Log-log scaling exponent of active delta versus N; `None` when the
    /// curve is flat within the noise floor.
    pub scaling_exponent_alpha: Option<f64>,
    /// True when every width's active delta stays under
    /// [`SCALING_NOISE_FLOOR_BYTES`]: no exponent is fitted.
    #[serde(default)]
    pub scaling_curve_within_noise_floor: bool,
    /// Largest cold whole-tree peak in the sweep.
    pub maximum_cold_peak_bytes: u64,
    /// Largest workload whole-tree peak in the sweep.
    pub maximum_workload_peak_bytes: u64,
}

impl SweepMetrics {
    /// Derive deterministic, robust headline and per-point metrics.
    pub fn calculate(points: &[SweepPoint]) -> Result<Self> {
        if points.is_empty() {
            return Err(AhrbError::Validation(
                "resource sweep needs at least one point".to_owned(),
            ));
        }
        let mut sorted = points.to_vec();
        sorted.sort_by_key(|point| point.agents);
        let mut previous: Option<SweepPoint> = None;
        let mut derived = Vec::with_capacity(sorted.len());
        let mut maximum_cold_peak_bytes = 0_u64;
        let mut maximum_workload_peak_bytes = 0_u64;
        for point in &sorted {
            if point.agents == 0 {
                return Err(AhrbError::Validation(
                    "resource sweep agent count must be nonzero".to_owned(),
                ));
            }
            if previous.is_some_and(|prior| prior.agents == point.agents) {
                return Err(AhrbError::Validation(format!(
                    "resource sweep contains duplicate N={}",
                    point.agents
                )));
            }
            if point.steady_bytes < point.baseline_bytes {
                return Err(AhrbError::Validation(format!(
                    "resource sweep N={} steady memory {} is below baseline {}",
                    point.agents, point.steady_bytes, point.baseline_bytes
                )));
            }
            if point.workload_peak_bytes < point.steady_bytes {
                return Err(AhrbError::Validation(format!(
                    "resource sweep N={} workload peak {} is below steady memory {}",
                    point.agents, point.workload_peak_bytes, point.steady_bytes
                )));
            }
            let active_delta = point.steady_bytes.saturating_sub(point.baseline_bytes);
            let average_added_bytes_per_agent = active_delta as f64 / point.agents as f64;
            let adjacent_marginal_bytes_per_agent = previous.map(|prior| {
                let delta_agents = point.agents.saturating_sub(prior.agents);
                let delta_bytes = point.steady_bytes as f64 - prior.steady_bytes as f64;
                delta_bytes / delta_agents as f64
            });
            let peak_amplification = (point.steady_bytes != 0)
                .then_some(point.workload_peak_bytes as f64 / point.steady_bytes as f64);
            let post_turn_retained_bytes =
                point.post_turn_bytes.saturating_sub(point.baseline_bytes);
            let post_close_residual_bytes =
                point.post_close_bytes.saturating_sub(point.baseline_bytes);
            let residual_bytes_per_agent = post_close_residual_bytes as f64 / point.agents as f64;
            let reclaim_ratio = reclaim_ratio(*point);
            derived.push((
                *point,
                PointMetrics {
                    average_added_bytes_per_agent,
                    adjacent_marginal_bytes_per_agent,
                    peak_amplification,
                    post_turn_retained_bytes,
                    post_close_residual_bytes,
                    residual_bytes_per_agent,
                    reclaim_ratio,
                },
            ));
            maximum_cold_peak_bytes = maximum_cold_peak_bytes.max(point.cold_peak_bytes);
            maximum_workload_peak_bytes =
                maximum_workload_peak_bytes.max(point.workload_peak_bytes);
            previous = Some(*point);
        }

        let headline_beta_bytes_per_agent = theil_sen(&sorted);
        let headline_beta_mib_per_agent = headline_beta_bytes_per_agent.map(|value| value / MIB);
        let scaling_curve_within_noise_floor = sorted.iter().all(|point| {
            point.steady_bytes.saturating_sub(point.baseline_bytes) < SCALING_NOISE_FLOOR_BYTES
        });
        let scaling_exponent_alpha = if scaling_curve_within_noise_floor {
            None
        } else {
            scaling_exponent(&sorted)
        };
        Ok(Self {
            points: derived,
            headline_beta_bytes_per_agent,
            headline_beta_mib_per_agent,
            scaling_exponent_alpha,
            scaling_curve_within_noise_floor,
            maximum_cold_peak_bytes,
            maximum_workload_peak_bytes,
        })
    }
}

fn missing_metric(metric: MemoryMetric, phase: &str) -> AhrbError {
    AhrbError::Unsupported(format!(
        "memory metric {metric:?} is unavailable in phase {phase:?}"
    ))
}

fn median_u64(sorted: &[u64]) -> Option<u64> {
    let length = sorted.len();
    if length == 0 {
        return None;
    }
    let middle = length / 2;
    if length % 2 == 1 {
        sorted.get(middle).copied()
    } else {
        let lower = sorted.get(middle.saturating_sub(1)).copied()?;
        let upper = sorted.get(middle).copied()?;
        Some(lower.saturating_add(upper.saturating_sub(lower) / 2))
    }
}

fn percentile_nearest_rank(sorted: &[u64], percent: usize) -> Option<u64> {
    if sorted.is_empty() || percent == 0 || percent > 100 {
        return None;
    }
    let rank = percent
        .saturating_mul(sorted.len())
        .saturating_add(99)
        .saturating_div(100);
    sorted.get(rank.saturating_sub(1)).copied()
}

fn linear_slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let count = points.len() as f64;
    let mean_x = points.iter().map(|point| point.0).sum::<f64>() / count;
    let mean_y = points.iter().map(|point| point.1).sum::<f64>() / count;
    let numerator = points
        .iter()
        .map(|point| (point.0 - mean_x) * (point.1 - mean_y))
        .sum::<f64>();
    let denominator = points
        .iter()
        .map(|point| (point.0 - mean_x).powi(2))
        .sum::<f64>();
    (denominator > 0.0).then_some(numerator / denominator)
}

fn theil_sen(points: &[SweepPoint]) -> Option<f64> {
    let mut slopes = Vec::new();
    for (index, left) in points.iter().enumerate() {
        for right in points.iter().skip(index.saturating_add(1)) {
            let delta_agents = right.agents.saturating_sub(left.agents);
            if delta_agents == 0 {
                continue;
            }
            slopes
                .push((right.steady_bytes as f64 - left.steady_bytes as f64) / delta_agents as f64);
        }
    }
    slopes.sort_by(f64::total_cmp);
    median_f64(&slopes)
}

fn median_f64(sorted: &[f64]) -> Option<f64> {
    let length = sorted.len();
    if length == 0 {
        return None;
    }
    let middle = length / 2;
    if length % 2 == 1 {
        sorted.get(middle).copied()
    } else {
        let lower = sorted.get(middle.saturating_sub(1)).copied()?;
        let upper = sorted.get(middle).copied()?;
        Some(lower + (upper - lower) / 2.0)
    }
}

fn scaling_exponent(points: &[SweepPoint]) -> Option<f64> {
    if points.iter().any(|point| {
        point.agents == 0 || point.steady_bytes.saturating_sub(point.baseline_bytes) == 0
    }) {
        return None;
    }
    let log_points: Vec<(f64, f64)> = points
        .iter()
        .map(|point| {
            let delta = point.steady_bytes.saturating_sub(point.baseline_bytes);
            ((point.agents as f64).ln(), (delta as f64).ln())
        })
        .collect();
    linear_slope(&log_points)
}

fn reclaim_ratio(point: SweepPoint) -> f64 {
    let active = point.steady_bytes.saturating_sub(point.baseline_bytes);
    if active == 0 {
        return 0.0;
    }
    let reclaimed = point.steady_bytes as f64 - point.post_close_bytes as f64;
    (reclaimed / active as f64).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::{ProcIdentity, ProcOwnership, ProcessInfo};
    use std::time::SystemTime;

    fn sample(elapsed_ns: u64, rss_bytes: u64, process_count: usize) -> Sample {
        let processes = (0..process_count)
            .map(|index| ProcessInfo {
                identity: ProcIdentity {
                    pid: u32::try_from(index + 1).unwrap_or(u32::MAX),
                    start_time: 1,
                },
                ppid: 0,
                command: "fixture".to_owned(),
                ownership: ProcOwnership::Descendant,
            })
            .collect();
        Sample {
            elapsed_ns,
            wall_time: SystemTime::now(),
            phase: "steady".to_owned(),
            rss_bytes,
            pss_bytes: None,
            private_bytes: None,
            footprint_bytes: None,
            rss_crosscheck_bytes: None,
            cgroup_memory_bytes: None,
            cgroup_peak_bytes: None,
            cpu_ns: elapsed_ns / 100,
            open_fds: None,
            thread_count: None,
            collection_ns: 1,
            collection_wall_ns: 1,
            processes,
            process_samples: Vec::new(),
        }
    }

    #[test]
    fn plateau_requires_stability_and_membership() -> Result<()> {
        let mut series = SampleSeries::default();
        series.push(sample(0, 100, 8))?;
        series.push(sample(1, 101, 8))?;
        series.push(sample(2, 103, 8))?;
        let plateau = series.plateau("steady", MemoryMetric::Rss, 8)?;
        assert_eq!(plateau.median_bytes, 101);
        assert!(plateau.trustworthy);

        let missing = series.plateau("steady", MemoryMetric::Rss, 9)?;
        assert!(!missing.trustworthy);
        Ok(())
    }

    #[test]
    fn plateau_requires_temporal_coverage_and_unique_membership() -> Result<()> {
        let mut series = SampleSeries::default();
        let mut only = sample(0, 100, 2);
        only.processes[1].identity = only.processes[0].identity;
        series.push(only)?;

        let plateau = series.plateau("steady", MemoryMetric::Rss, 2)?;
        assert_eq!(plateau.sample_count, 1);
        assert_eq!(plateau.distinct_sample_times, 1);
        assert_eq!(plateau.minimum_observed_processes, 1);
        assert!(!plateau.has_multiple_sample_times);
        assert!(!plateau.all_expected_present);
        assert!(!plateau.trustworthy);
        Ok(())
    }

    #[test]
    fn phase_cpu_rejects_missing_interval_and_counter_regression() -> Result<()> {
        let mut singleton = SampleSeries::default();
        singleton.push(sample(0, 100, 1))?;
        assert!(singleton.phase_cpu("steady").is_err());

        let mut regressed = SampleSeries::default();
        let mut first = sample(0, 100, 1);
        first.cpu_ns = 20;
        let mut second = sample(100, 100, 1);
        second.cpu_ns = 10;
        regressed.push(first)?;
        regressed.push(second)?;
        assert!(regressed.phase_cpu("steady").is_err());
        Ok(())
    }

    #[test]
    fn trailing_plateau_requires_the_requested_phase_duration() -> Result<()> {
        let mut series = SampleSeries::default();
        series.push(sample(0, 100, 1))?;
        series.push(sample(10, 100, 1))?;
        assert!(
            series
                .trailing_plateau("steady", MemoryMetric::Rss, 1, 11)
                .is_err()
        );
        assert!(
            series
                .trailing_plateau("steady", MemoryMetric::Rss, 1, 10)?
                .trustworthy
        );
        Ok(())
    }

    #[test]
    fn sweep_metrics_match_linear_fixture() -> Result<()> {
        let mib = 1_048_576_u64;
        let points = [1_u32, 2, 4, 8].map(|agents| {
            let baseline = 10 * mib;
            let steady = baseline + u64::from(agents) * 20 * mib;
            SweepPoint {
                agents,
                baseline_bytes: baseline,
                steady_bytes: steady,
                workload_peak_bytes: steady + mib,
                cold_peak_bytes: steady + 2 * mib,
                post_turn_bytes: steady,
                post_close_bytes: baseline,
            }
        });
        let metrics = SweepMetrics::calculate(&points)?;
        assert_eq!(metrics.headline_beta_mib_per_agent, Some(20.0));
        assert!(
            metrics
                .scaling_exponent_alpha
                .is_some_and(|alpha| (alpha - 1.0).abs() < 1e-10)
        );
        assert!(
            metrics
                .points
                .iter()
                .all(|(_, point)| point.reclaim_ratio == 1.0)
        );
        assert_eq!(metrics.maximum_workload_peak_bytes, 171 * 1_048_576_u64);
        Ok(())
    }

    #[test]
    fn sweep_rejects_impossible_steady_and_peak_relationships() {
        let below_baseline = SweepPoint {
            agents: 1,
            baseline_bytes: 100,
            steady_bytes: 99,
            workload_peak_bytes: 100,
            cold_peak_bytes: 100,
            post_turn_bytes: 100,
            post_close_bytes: 100,
        };
        assert!(SweepMetrics::calculate(&[below_baseline]).is_err());

        let peak_below_steady = SweepPoint {
            agents: 1,
            baseline_bytes: 100,
            steady_bytes: 110,
            workload_peak_bytes: 109,
            cold_peak_bytes: 100,
            post_turn_bytes: 110,
            post_close_bytes: 100,
        };
        assert!(SweepMetrics::calculate(&[peak_below_steady]).is_err());
    }

    #[test]
    fn overload_is_an_error_signal() -> Result<()> {
        let mut series = SampleSeries::default();
        let mut first = sample(0, 100, 1);
        first.collection_ns = 20;
        let mut second = sample(100, 100, 1);
        second.collection_ns = 20;
        series.push(first)?;
        series.push(second)?;
        let health = series.sampling_health(100)?;
        assert!(health.overloaded);
        Ok(())
    }

    #[test]
    fn sampling_health_detects_a_missed_cadence_gap() -> Result<()> {
        let mut series = SampleSeries::default();
        series.push(sample(0, 100, 1))?;
        series.push(sample(201, 100, 1))?;
        let health = series.sampling_health(100)?;
        assert_eq!(health.cadence_gaps, 1);
        assert!(health.overloaded);
        Ok(())
    }

    #[test]
    fn sampling_health_allows_one_bounded_scheduler_jitter_gap() -> Result<()> {
        let mut series = SampleSeries::default();
        let mut elapsed = 0_u64;
        for index in 0..101 {
            series.push(sample(elapsed, 100, 1))?;
            elapsed = elapsed.saturating_add(if index == 50 { 150 } else { 100 });
        }
        let health = series.sampling_health(100)?;
        assert_eq!(health.cadence_gaps, 1);
        assert!(!health.overloaded);
        Ok(())
    }

    #[test]
    fn sampling_health_rejects_more_than_one_percent_jitter_gaps() -> Result<()> {
        let mut series = SampleSeries::default();
        let mut elapsed = 0_u64;
        for index in 0..201 {
            series.push(sample(elapsed, 100, 1))?;
            elapsed = elapsed.saturating_add(if matches!(index, 25 | 75 | 125) {
                150
            } else {
                100
            });
        }
        let health = series.sampling_health(100)?;
        assert_eq!(health.cadence_gaps, 3);
        assert!(health.overloaded);
        Ok(())
    }

    #[test]
    fn phase_coverage_rejects_truncation_and_gaps() -> Result<()> {
        let mut truncated = SampleSeries::default();
        truncated.push(sample(0, 100, 1))?;
        truncated.push(sample(100, 100, 1))?;
        let coverage = truncated.phase_coverage("steady", 100, 300)?;
        assert!(!coverage.duration_covered);
        assert!(!coverage.trustworthy);

        let mut gapped = SampleSeries::default();
        gapped.push(sample(0, 100, 1))?;
        gapped.push(sample(201, 100, 1))?;
        gapped.push(sample(300, 100, 1))?;
        let coverage = gapped.phase_coverage("steady", 100, 300)?;
        assert_eq!(coverage.cadence_gaps, 1);
        assert!(!coverage.trustworthy);
        Ok(())
    }
}
