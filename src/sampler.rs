//! Phase-aware resource series, stability checks, and sweep metrics.

use crate::process::Sample;
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};

const MIB: f64 = 1_048_576.0;

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
        let end = self
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .max()
            .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no samples")))?;
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
        for sample in samples {
            last = sample;
        }
        let elapsed_ns = last.elapsed_ns.saturating_sub(first.elapsed_ns);
        let cpu_ns = last.cpu_ns.saturating_sub(first.cpu_ns);
        let one_core_fraction = if elapsed_ns == 0 {
            0.0
        } else {
            cpu_ns as f64 / elapsed_ns as f64
        };
        Ok(PhaseCpu {
            elapsed_ns,
            cpu_ns,
            one_core_fraction,
        })
    }

    /// Detect collection overhead and cadence overruns. AHRB classifies any
    /// overhead above ten percent of one core as sampler overload.
    pub fn sampling_health(&self, cadence_ns: u64) -> Result<SamplingHealth> {
        if cadence_ns == 0 {
            return Err(AhrbError::Validation(
                "sampling cadence must be nonzero".to_owned(),
            ));
        }
        let collection_ns = self.samples.iter().fold(0_u64, |total, sample| {
            total.saturating_add(sample.collection_ns)
        });
        let observation_ns = match (self.samples.first(), self.samples.last()) {
            (Some(first), Some(last)) => last
                .elapsed_ns
                .saturating_sub(first.elapsed_ns)
                .saturating_add(cadence_ns),
            _ => 0,
        };
        let cadence_overruns = self
            .samples
            .iter()
            .filter(|sample| sample.collection_ns > cadence_ns)
            .count();
        let overhead_one_core = if observation_ns == 0 {
            0.0
        } else {
            collection_ns as f64 / observation_ns as f64
        };
        Ok(SamplingHealth {
            collection_ns,
            observation_ns,
            overhead_one_core,
            cadence_overruns,
            overloaded: overhead_one_core > 0.10 || cadence_overruns > 0,
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
            .map(|sample| sample.processes.len())
            .min()
            .map_or(0, |value| value);
        let all_expected_present = minimum_observed_processes >= minimum_processes;
        Ok(Plateau {
            metric,
            sample_count: values.len(),
            minimum_observed_processes,
            median_bytes,
            p05_bytes,
            p95_bytes,
            relative_spread,
            all_expected_present,
            trustworthy: relative_spread <= 0.05 && all_expected_present,
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
    /// Whether spread is at most five percent and all expected processes were present.
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

/// Evidence that the sampler itself did not distort the benchmark.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SamplingHealth {
    /// Sum of measured collection durations.
    pub collection_ns: u64,
    /// Time span represented by the series.
    pub observation_ns: u64,
    /// Collection CPU-time approximation as a fraction of one core.
    pub overhead_one_core: f64,
    /// Samples whose collection duration exceeded the requested cadence.
    pub cadence_overruns: usize,
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

/// Parallel-agent scaling and reclaim metrics for a complete sweep.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SweepMetrics {
    /// Input points sorted by N and their derived metrics.
    pub points: Vec<(SweepPoint, PointMetrics)>,
    /// Theil-Sen slope of steady bytes versus N.
    pub headline_beta_bytes_per_agent: Option<f64>,
    /// The same headline slope in MiB per agent.
    pub headline_beta_mib_per_agent: Option<f64>,
    /// Log-log scaling exponent of active delta versus N.
    pub scaling_exponent_alpha: Option<f64>,
    /// Largest cold whole-tree peak in the sweep.
    pub maximum_cold_peak_bytes: u64,
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
            previous = Some(*point);
        }

        let headline_beta_bytes_per_agent = theil_sen(&sorted);
        let headline_beta_mib_per_agent = headline_beta_bytes_per_agent.map(|value| value / MIB);
        let scaling_exponent_alpha = scaling_exponent(&sorted);
        Ok(Self {
            points: derived,
            headline_beta_bytes_per_agent,
            headline_beta_mib_per_agent,
            scaling_exponent_alpha,
            maximum_cold_peak_bytes,
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
    let log_points: Vec<(f64, f64)> = points
        .iter()
        .filter_map(|point| {
            let delta = point.steady_bytes.saturating_sub(point.baseline_bytes);
            (point.agents > 0 && delta > 0)
                .then_some(((point.agents as f64).ln(), (delta as f64).ln()))
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
            collection_ns: 1,
            processes,
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
        Ok(())
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
}
