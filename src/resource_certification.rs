//! Normative resource-evidence analysis for matrix rows 20 through 29.
//!
//! This module deliberately separates collection from certification.  A runner records
//! phase-labelled [`Sample`](crate::process::Sample) values and the small pieces of
//! evidence which an operating-system sampler cannot observe (for example open file
//! descriptors and thread counts).  The analyzer then applies the v1 envelope without
//! inventing evidence: a missing observation becomes `ERROR`, never `PASS`.

use crate::evaluate::{Assertion, Pillar, TestResult, classify};
use crate::sampler::{
    MemoryMetric, Plateau, SampleSeries, SamplingHealth, SweepMetrics, SweepPoint,
};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MIB: u64 = 1_048_576;
const GIB: u64 = 1_073_741_824;

/// Measurement schedule for a quick diagnostic or a certification run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceTimingPlan {
    /// Stable profile label.
    pub profile: ResourceProfile,
    /// Complete repetitions of every measurement.
    pub repetitions: u32,
    /// Unmeasured warm-up turns per fresh profile.
    pub warmup_turns: u32,
    /// Required parallel widths, in their deterministic rotated order before seeding.
    pub sweep_widths: Vec<u32>,
    /// Environment load observation before a repetition.
    pub load_guard_ms: u64,
    /// Maximum time spent waiting for a stable environment.
    pub load_guard_timeout_ms: u64,
    /// Warm idle baseline duration.
    pub idle_baseline_ms: u64,
    /// Quiet window used for idle CPU and polling detection.
    pub idle_cpu_ms: u64,
    /// Untouched window used for memory drift.
    pub idle_drift_ms: u64,
    /// State-barrier hold duration.
    pub barrier_hold_ms: u64,
    /// Initial barrier time discarded before steady analysis.
    pub barrier_discard_ms: u64,
    /// Trailing barrier duration analyzed as the steady window.
    pub barrier_steady_ms: u64,
    /// Maximum wait for return-to-idle and post-close reclaim.
    pub reclaim_deadline_ms: u64,
    /// Whole-tree membership cadence.
    pub membership_cadence_ms: u64,
    /// macOS rusage cadence.
    pub macos_rusage_cadence_ms: u64,
    /// Linux cgroup counter cadence.
    pub linux_cgroup_cadence_ms: u64,
    /// Linux smaps-rollup cadence.
    pub linux_smaps_cadence_ms: u64,
    /// Turns required by the long-horizon workload.
    pub long_horizon_turns: u32,
    /// Interval between long-horizon resource checkpoints.
    pub long_horizon_sample_turns: u32,
}

impl ResourceTimingPlan {
    /// Return the deterministic plan for a profile.
    pub fn for_profile(profile: ResourceProfile) -> Self {
        let (repetitions, idle_drift_ms, long_horizon_turns) = match profile {
            ResourceProfile::Quick => (3, 30_000, 1_000),
            ResourceProfile::Cert => (7, 120_000, 1_000),
        };
        Self {
            profile,
            repetitions,
            warmup_turns: 1,
            sweep_widths: vec![1, 2, 4, 8],
            load_guard_ms: 5_000,
            load_guard_timeout_ms: 60_000,
            idle_baseline_ms: 3_000,
            idle_cpu_ms: 10_000,
            idle_drift_ms,
            barrier_hold_ms: 3_000,
            barrier_discard_ms: 1_000,
            barrier_steady_ms: 2_000,
            reclaim_deadline_ms: 10_000,
            membership_cadence_ms: 10,
            macos_rusage_cadence_ms: 20,
            linux_cgroup_cadence_ms: 10,
            linux_smaps_cadence_ms: 50,
            long_horizon_turns,
            long_horizon_sample_turns: 100,
        }
    }
}

/// Resource measurement profile.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceProfile {
    /// Three repetitions and shortened leak horizons.
    Quick,
    /// Seven repetitions and the complete v1 certification horizon.
    Cert,
}

impl From<crate::cli::Profile> for ResourceProfile {
    fn from(value: crate::cli::Profile) -> Self {
        match value {
            crate::cli::Profile::Quick => Self::Quick,
            crate::cli::Profile::Cert => Self::Cert,
        }
    }
}

/// v1 small-host thresholds.  Values are explicit so future profiles can be compared.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceEnvelope {
    /// Largest trustworthy plateau spread, `(P95-P5)/median`.
    pub maximum_plateau_spread: f64,
    /// Maximum quiet CPU as a fraction of one core.
    pub maximum_idle_cpu_fraction: f64,
    /// Maximum barrier CPU as a fraction of one core.
    pub maximum_barrier_cpu_fraction: f64,
    /// Maximum CPU per scripted turn.
    pub maximum_cpu_ns_per_turn: u64,
    /// Maximum idle memory slope.
    pub maximum_idle_drift_bytes_per_minute: f64,
    /// Maximum net idle growth.
    pub maximum_idle_net_growth_bytes: u64,
    /// Cold and workload whole-tree peak ceiling.
    pub maximum_peak_bytes: u64,
    /// Theil-Sen marginal memory ceiling.
    pub maximum_beta_bytes_per_agent: f64,
    /// Log-log scaling exponent ceiling.
    pub maximum_scaling_exponent: f64,
    /// Minimum post-close reclaim ratio.
    pub minimum_reclaim_ratio: f64,
    /// Absolute residual allowance.
    pub residual_floor_bytes: u64,
    /// Residual allowance as a fraction of active memory.
    pub residual_active_fraction: f64,
    /// Maximum long-horizon memory growth per turn.
    pub maximum_long_horizon_bytes_per_turn: f64,
    /// Long-horizon final residual allowance as a fraction of baseline.
    pub long_horizon_baseline_fraction: f64,
}

impl Default for ResourceEnvelope {
    fn default() -> Self {
        Self {
            maximum_plateau_spread: 0.05,
            maximum_idle_cpu_fraction: 0.01,
            maximum_barrier_cpu_fraction: 0.05,
            maximum_cpu_ns_per_turn: 250_000_000,
            maximum_idle_drift_bytes_per_minute: MIB as f64,
            maximum_idle_net_growth_bytes: 8 * MIB,
            maximum_peak_bytes: 4 * GIB,
            maximum_beta_bytes_per_agent: 256.0 * MIB as f64,
            maximum_scaling_exponent: 1.20,
            minimum_reclaim_ratio: 0.80,
            residual_floor_bytes: 64 * MIB,
            residual_active_fraction: 0.20,
            maximum_long_horizon_bytes_per_turn: 64.0 * 1024.0,
            long_horizon_baseline_fraction: 0.05,
        }
    }
}

/// Stable phase names used to locate samples for rows 20 through 25.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourcePhases {
    /// Quiescent warm baseline B.
    pub warm_idle: String,
    /// Ten-second quiet CPU window.
    pub idle_cpu: String,
    /// Untouched idle drift window.
    pub idle_drift: String,
}

impl Default for ResourcePhases {
    fn default() -> Self {
        Self {
            warm_idle: "warm-idle".to_owned(),
            idle_cpu: "idle-cpu".to_owned(),
            idle_drift: "idle-drift".to_owned(),
        }
    }
}

/// The process model observed while the harness is not executing a turn.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdleProcessModel {
    /// A daemon/controller remains resident between turns.
    PersistentTree,
    /// A one-shot harness has no owned process between turns.
    ZeroProcessBetweenTurns,
}

/// Idle state evidence which cannot be derived from memory counters alone.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IdleObservation {
    /// Model declared by the manifest.
    pub declared_model: IdleProcessModel,
    /// Whether a periodic store/session-file polling signature was observed.
    pub busy_polling_detected: Option<bool>,
    /// Initial owned worker count in the drift window.
    pub initial_workers: usize,
    /// Final owned worker count in the drift window.
    pub final_workers: usize,
    /// Initial whole-tree thread count.
    pub initial_threads: Option<u64>,
    /// Final whole-tree thread count.
    pub final_threads: Option<u64>,
}

/// Phase references and membership requirements for one fresh-profile N point.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SweepObservation {
    /// Simultaneous agents at the shared state barrier.
    pub agents: u32,
    /// Fresh-profile warm baseline phase.
    pub baseline_phase: String,
    /// Complete workload phase, used for peak P_n.
    pub workload_phase: String,
    /// Cold launch phase, used for peak C_n.
    pub cold_phase: String,
    /// Three-second barrier hold phase, whose final two seconds define S_n.
    pub steady_phase: String,
    /// Post-turn retention phase I_n.
    pub post_turn_phase: String,
    /// Post-close plateau phase R_n.
    pub post_close_phase: String,
    /// Minimum whole-tree membership required throughout the steady window.
    pub minimum_steady_processes: usize,
    /// Minimum membership required in the fresh warm baseline.
    pub minimum_baseline_processes: usize,
    /// Minimum membership required after turn completion and before close.
    pub minimum_post_turn_processes: usize,
    /// Minimum membership required after official close/delete.
    pub minimum_post_close_processes: usize,
}

/// Ordinary workflow return-to-idle evidence for row 23.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReturnToIdleObservation {
    /// Baseline phase before the workflow.
    pub baseline_phase: String,
    /// Active workflow phase.
    pub active_phase: String,
    /// Stable post-close phase.
    pub returned_phase: String,
    /// Milliseconds until the stable returned phase began.
    pub settled_after_ms: u64,
    /// Owned processes remaining after official session close/delete.
    pub remaining_workers: usize,
    /// Minimum membership required in the ordinary baseline.
    pub minimum_baseline_processes: usize,
    /// Minimum membership required after the workflow returns to idle.
    pub minimum_returned_processes: usize,
}

/// Cold start and steady-readiness evidence for row 24.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ColdStartObservation {
    /// Launch-to-ready phase, including the cold peak.
    pub cold_phase: String,
    /// Ready idle phase.
    pub ready_idle_phase: String,
    /// Observed launch-to-readiness duration.
    pub readiness_ms: u64,
    /// Adapter-declared startup bound.
    pub startup_bound_ms: u64,
    /// Minimum expected process membership at ready idle.
    pub minimum_idle_processes: usize,
}

/// CPU evidence for one scripted single-agent workload.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SingleAgentObservation {
    /// Phase spanning the complete scripted turns.
    pub turn_phase: String,
    /// Number of scripted turns represented by the CPU delta.
    pub scripted_turns: u32,
    /// State-barrier idle phase.
    pub barrier_phase: String,
}

/// Cleanup evidence after every session has been officially closed and deleted.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CleanupObservation {
    /// Time until reclaim was sampled.
    pub reclaim_after_ms: u64,
    /// Owned session workers remaining after cleanup grace.
    pub remaining_workers: usize,
}

/// One resource checkpoint in the long-horizon workload.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct LongHorizonPoint {
    /// Number of committed turns at this checkpoint.
    pub turn: u32,
    /// Preferred whole-tree memory counter.
    pub memory_bytes: u64,
    /// Whole-tree open file-descriptor count.
    pub open_fds: u64,
    /// Whole-tree thread count.
    pub threads: u64,
}

/// Long-horizon memory and descriptor/thread evidence for row 29.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LongHorizonObservation {
    /// Ordered checkpoints, conventionally every 100 turns.
    pub points: Vec<LongHorizonPoint>,
    /// Warm baseline before the first turn.
    pub baseline_bytes: u64,
    /// Stable memory after the final close/delete.
    pub final_post_close_bytes: u64,
}

/// Complete collection input for resource rows.  Optional fields are intentional:
/// absence is reported as infrastructure `ERROR`, not inferred success.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ResourceEvidence {
    /// Complete fresh-profile repetitions represented by the evidence.
    pub completed_repetitions: u32,
    /// Phase-aware whole-tree samples.
    pub series: SampleSeries,
    /// Standard idle phase names.
    #[serde(default)]
    pub phases: ResourcePhases,
    /// Preferred comparison counter.
    pub memory_metric: Option<MemoryMetric>,
    /// Cadence against which sampler overhead is checked.
    pub sampler_cadence_ns: Option<u64>,
    /// Idle topology, polling, worker, and thread observations.
    pub idle: Option<IdleObservation>,
    /// N=1,2,4,8 fresh-profile observations.
    #[serde(default)]
    pub sweep: Vec<SweepObservation>,
    /// Ordinary workflow return-to-idle observation.
    pub ordinary_return: Option<ReturnToIdleObservation>,
    /// Cold start observation.
    pub cold_start: Option<ColdStartObservation>,
    /// Single-agent CPU observation.
    pub single_agent: Option<SingleAgentObservation>,
    /// Post-close worker cleanup observation.
    pub cleanup: Option<CleanupObservation>,
    /// Long-horizon memory/FD/thread observation.
    pub long_horizon: Option<LongHorizonObservation>,
}

/// Derived certification evidence and row outcomes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceCertification {
    /// Timing plan whose completeness was enforced.
    pub timing: ResourceTimingPlan,
    /// Matrix results in row order, exactly 20 through 29.
    pub rows: Vec<TestResult>,
    /// Plateau analyses keyed by stable evidence name.
    pub plateaus: BTreeMap<String, Plateau>,
    /// Sampler-health analysis when cadence evidence was supplied.
    pub sampling_health: Option<SamplingHealth>,
    /// Complete N-sweep metrics when every point was valid.
    pub sweep_metrics: Option<SweepMetrics>,
    /// Deterministically ordered numeric metrics for reports.
    pub metrics: BTreeMap<String, f64>,
}

/// Analyze rows 20 through 29 against the normative v1 envelope.
pub fn evaluate_resources(
    profile: ResourceProfile,
    evidence: &ResourceEvidence,
    envelope: &ResourceEnvelope,
) -> ResourceCertification {
    let timing = ResourceTimingPlan::for_profile(profile);
    let mut analysis = Analysis::new(timing.clone(), evidence, envelope);
    analysis.evaluate();
    analysis.finish()
}

struct Analysis<'a> {
    timing: ResourceTimingPlan,
    evidence: &'a ResourceEvidence,
    envelope: &'a ResourceEnvelope,
    rows: BTreeMap<u8, TestResult>,
    plateaus: BTreeMap<String, Plateau>,
    sampling_health: Option<SamplingHealth>,
    sweep_metrics: Option<SweepMetrics>,
    sweep_points: BTreeMap<u32, SweepPoint>,
    metrics: BTreeMap<String, f64>,
    sampler_error: Option<String>,
    sweep_error: Option<String>,
}

impl<'a> Analysis<'a> {
    fn new(
        timing: ResourceTimingPlan,
        evidence: &'a ResourceEvidence,
        envelope: &'a ResourceEnvelope,
    ) -> Self {
        Self {
            timing,
            evidence,
            envelope,
            rows: BTreeMap::new(),
            plateaus: BTreeMap::new(),
            sampling_health: None,
            sweep_metrics: None,
            sweep_points: BTreeMap::new(),
            metrics: BTreeMap::new(),
            sampler_error: None,
            sweep_error: None,
        }
    }

    fn evaluate(&mut self) {
        self.evaluate_sampler_health();
        self.evaluate_idle_rows();
        self.derive_sweep();
        self.evaluate_return_and_cold();
        self.evaluate_single_agent();
        self.evaluate_parallel_rows();
        self.evaluate_long_horizon();
        for row in 20..=29 {
            if !self.rows.contains_key(&row) {
                self.insert_error(row, "resource row was not evaluated".to_owned());
            }
        }
    }

    fn evaluate_sampler_health(&mut self) {
        if self.evidence.completed_repetitions < self.timing.repetitions {
            self.sampler_error = Some(format!(
                "only {} of {} required fresh-profile repetitions were collected",
                self.evidence.completed_repetitions, self.timing.repetitions
            ));
        }
        let Some(cadence) = self.evidence.sampler_cadence_ns else {
            if self.sampler_error.is_none() {
                self.sampler_error = Some("sampler cadence evidence is absent".to_owned());
            }
            return;
        };
        match self.evidence.series.sampling_health(cadence) {
            Ok(health) => {
                self.metrics.insert(
                    "sampler_overhead_one_core".to_owned(),
                    health.overhead_one_core,
                );
                if health.overloaded && self.sampler_error.is_none() {
                    self.sampler_error = Some(format!(
                        "sampler overload: {:.3}% of one core, {} cadence overruns",
                        health.overhead_one_core * 100.0,
                        health.cadence_overruns
                    ));
                }
                self.sampling_health = Some(health);
            }
            Err(error) => {
                if self.sampler_error.is_none() {
                    self.sampler_error = Some(error.to_string());
                }
            }
        }
    }

    fn evaluate_idle_rows(&mut self) {
        let Some(metric) = self.evidence.memory_metric else {
            for row in 20..=22 {
                self.insert_error(row, "preferred memory metric is absent".to_owned());
            }
            return;
        };
        let Some(idle) = &self.evidence.idle else {
            for row in 20..=22 {
                self.insert_error(row, "idle observation is absent".to_owned());
            }
            return;
        };
        let minimum = match idle.declared_model {
            IdleProcessModel::PersistentTree => 1,
            IdleProcessModel::ZeroProcessBetweenTurns => 0,
        };
        let baseline = self.plateau(
            "idle-baseline",
            &self.evidence.phases.warm_idle,
            metric,
            minimum,
            None,
        );
        match baseline {
            Ok(plateau) => {
                let samples: Vec<_> = self
                    .evidence
                    .series
                    .samples
                    .iter()
                    .filter(|sample| sample.phase == self.evidence.phases.warm_idle)
                    .collect();
                let topology_matches = match idle.declared_model {
                    IdleProcessModel::PersistentTree => {
                        !samples.is_empty()
                            && samples.iter().all(|sample| !sample.processes.is_empty())
                    }
                    IdleProcessModel::ZeroProcessBetweenTurns => {
                        !samples.is_empty()
                            && samples.iter().all(|sample| sample.processes.is_empty())
                    }
                };
                self.metrics
                    .insert("idle_median_bytes".to_owned(), plateau.median_bytes as f64);
                self.insert_checks(
                    20,
                    vec![
                        check(
                            "idle-topology",
                            topology_matches,
                            format!("declared and observed {:?}", idle.declared_model),
                        ),
                        check(
                            "idle-plateau",
                            plateau.trustworthy
                                && plateau.relative_spread <= self.envelope.maximum_plateau_spread,
                            format!(
                                "median={} spread={:.3}% processes>={}",
                                plateau.median_bytes,
                                plateau.relative_spread * 100.0,
                                plateau.minimum_observed_processes
                            ),
                        ),
                    ],
                );
            }
            Err(error) => self.insert_error(20, error.to_string()),
        }

        match (
            self.evidence
                .series
                .phase_cpu(&self.evidence.phases.idle_cpu),
            idle.busy_polling_detected,
        ) {
            (Ok(cpu), Some(busy_polling_detected)) => {
                self.metrics
                    .insert("idle_cpu_one_core".to_owned(), cpu.one_core_fraction);
                self.insert_checks(
                    21,
                    vec![
                        check(
                            "idle-cpu",
                            cpu.one_core_fraction <= self.envelope.maximum_idle_cpu_fraction,
                            format!("{:.3}% of one core", cpu.one_core_fraction * 100.0),
                        ),
                        check(
                            "busy-polling",
                            !busy_polling_detected,
                            format!("detected={busy_polling_detected}"),
                        ),
                    ],
                );
            }
            (Err(error), _) => self.insert_error(21, error.to_string()),
            (_, None) => self.insert_error(21, "busy-polling evidence is absent".to_owned()),
        }

        let drift = self
            .evidence
            .series
            .drift_bytes_per_minute(&self.evidence.phases.idle_drift, metric);
        let net = phase_net_growth(
            &self.evidence.series,
            &self.evidence.phases.idle_drift,
            metric,
        );
        match (drift, net, idle.initial_threads, idle.final_threads) {
            (Ok(slope), Ok(net_growth), Some(initial_threads), Some(final_threads)) => {
                self.metrics
                    .insert("idle_drift_bytes_per_minute".to_owned(), slope);
                self.metrics
                    .insert("idle_net_growth_bytes".to_owned(), net_growth as f64);
                self.insert_checks(
                    22,
                    vec![
                        check(
                            "idle-drift",
                            slope <= self.envelope.maximum_idle_drift_bytes_per_minute,
                            format!("{slope:.3} bytes/minute"),
                        ),
                        check(
                            "idle-net-growth",
                            net_growth <= self.envelope.maximum_idle_net_growth_bytes,
                            format!("{net_growth} bytes"),
                        ),
                        check(
                            "worker-growth",
                            idle.final_workers <= idle.initial_workers,
                            format!("{} -> {}", idle.initial_workers, idle.final_workers),
                        ),
                        check(
                            "thread-growth",
                            final_threads <= initial_threads,
                            format!("{initial_threads} -> {final_threads}"),
                        ),
                    ],
                );
            }
            (Err(error), _, _, _) | (_, Err(error), _, _) => {
                self.insert_error(22, error.to_string())
            }
            (_, _, _, _) => self.insert_error(22, "thread-count evidence is absent".to_owned()),
        }
    }

    fn derive_sweep(&mut self) {
        let Some(metric) = self.evidence.memory_metric else {
            return;
        };
        for observation in &self.evidence.sweep {
            match derive_sweep_point(
                &self.evidence.series,
                observation,
                metric,
                self.timing.barrier_steady_ms.saturating_mul(1_000_000),
                self.envelope.maximum_plateau_spread,
            ) {
                Ok((point, named_plateaus)) => {
                    if self.sweep_points.insert(point.agents, point).is_some() {
                        self.sweep_error = Some(format!(
                            "duplicate resource observation for N={}",
                            point.agents
                        ));
                    }
                    for (name, plateau) in named_plateaus {
                        self.plateaus.insert(name, plateau);
                    }
                }
                Err(error) => {
                    self.sweep_error = Some(format!(
                        "could not derive N={}: {error}",
                        observation.agents
                    ));
                }
            }
        }
        let points: Vec<SweepPoint> = self.sweep_points.values().copied().collect();
        if !points.is_empty() {
            match SweepMetrics::calculate(&points) {
                Ok(metrics) => {
                    if let Some(beta) = metrics.headline_beta_bytes_per_agent {
                        self.metrics
                            .insert("parallel_beta_bytes_per_agent".to_owned(), beta);
                    }
                    if let Some(alpha) = metrics.scaling_exponent_alpha {
                        self.metrics
                            .insert("parallel_scaling_exponent".to_owned(), alpha);
                    }
                    self.metrics.insert(
                        "maximum_cold_peak_bytes".to_owned(),
                        metrics.maximum_cold_peak_bytes as f64,
                    );
                    self.sweep_metrics = Some(metrics);
                }
                Err(error) => self.sweep_error = Some(error.to_string()),
            }
        }
    }

    fn evaluate_return_and_cold(&mut self) {
        let Some(metric) = self.evidence.memory_metric else {
            self.insert_error(23, "preferred memory metric is absent".to_owned());
            self.insert_error(24, "preferred memory metric is absent".to_owned());
            return;
        };
        let n8_return = self.sweep_points.get(&8).copied();
        if n8_return.is_none() {
            self.insert_error(23, "N=8 return-to-idle observation is absent".to_owned());
        }
        match &self.evidence.ordinary_return {
            Some(observation) => {
                match (
                    derive_recovery(&self.evidence.series, observation, metric),
                    n8_return,
                ) {
                    (Ok(recovery), Some(n8)) => {
                        let n8_active = n8.steady_bytes.saturating_sub(n8.baseline_bytes);
                        let n8_residual = n8.post_close_bytes.saturating_sub(n8.baseline_bytes);
                        self.insert_checks(
                            23,
                            vec![
                                check(
                                    "ordinary-residual",
                                    recovery.residual_bytes
                                        <= recovery.residual_limit_bytes(self.envelope),
                                    format!(
                                        "residual={} limit={}",
                                        recovery.residual_bytes,
                                        recovery.residual_limit_bytes(self.envelope)
                                    ),
                                ),
                                check(
                                    "return-deadline",
                                    observation.settled_after_ms <= self.timing.reclaim_deadline_ms,
                                    format!("{} ms", observation.settled_after_ms),
                                ),
                                check(
                                    "ordinary-cleanup",
                                    observation.remaining_workers == 0,
                                    format!("{} workers", observation.remaining_workers),
                                ),
                                check(
                                    "n8-residual",
                                    n8_residual <= residual_limit(n8_active, self.envelope),
                                    format!(
                                        "residual={} limit={}",
                                        n8_residual,
                                        residual_limit(n8_active, self.envelope)
                                    ),
                                ),
                            ],
                        );
                    }
                    (Err(error), _) => self.insert_error(23, error.to_string()),
                    (_, None) => {}
                }
            }
            None => self.insert_error(
                23,
                "ordinary return-to-idle observation is absent".to_owned(),
            ),
        }

        match &self.evidence.cold_start {
            Some(observation) => {
                let peak = phase_peak(&self.evidence.series, &observation.cold_phase, metric);
                let plateau = self.plateau(
                    "cold-ready-idle",
                    &observation.ready_idle_phase,
                    metric,
                    observation.minimum_idle_processes,
                    None,
                );
                match (peak, plateau) {
                    (Ok(peak), Ok(plateau)) => self.insert_checks(
                        24,
                        vec![
                            check(
                                "startup-bound",
                                observation.readiness_ms <= observation.startup_bound_ms,
                                format!(
                                    "{} ms <= {} ms",
                                    observation.readiness_ms, observation.startup_bound_ms
                                ),
                            ),
                            check(
                                "cold-peak",
                                peak <= self.envelope.maximum_peak_bytes,
                                format!("{peak} bytes"),
                            ),
                            check(
                                "ready-plateau",
                                plateau.trustworthy
                                    && plateau.relative_spread
                                        <= self.envelope.maximum_plateau_spread,
                                format!("spread={:.3}%", plateau.relative_spread * 100.0),
                            ),
                        ],
                    ),
                    (Err(error), _) | (_, Err(error)) => self.insert_error(24, error.to_string()),
                }
            }
            None => self.insert_error(24, "cold-start observation is absent".to_owned()),
        }
    }

    fn evaluate_single_agent(&mut self) {
        let Some(observation) = &self.evidence.single_agent else {
            self.insert_error(25, "single-agent CPU observation is absent".to_owned());
            return;
        };
        if observation.scripted_turns == 0 {
            self.insert_error(25, "single-agent scripted turn count is zero".to_owned());
            return;
        }
        let cpu = self.evidence.series.phase_cpu(&observation.turn_phase);
        let barrier_cpu = self.evidence.series.phase_cpu(&observation.barrier_phase);
        let n1 = self.sweep_points.get(&1).copied();
        match (cpu, barrier_cpu, n1) {
            (Ok(cpu), Ok(barrier_cpu), Some(point)) => {
                let cpu_per_turn = cpu.cpu_ns / u64::from(observation.scripted_turns);
                self.metrics.insert(
                    "single_agent_cpu_ns_per_turn".to_owned(),
                    cpu_per_turn as f64,
                );
                self.metrics.insert(
                    "single_agent_added_bytes".to_owned(),
                    point.steady_bytes.saturating_sub(point.baseline_bytes) as f64,
                );
                self.insert_checks(
                    25,
                    vec![
                        check(
                            "cpu-per-turn",
                            cpu_per_turn <= self.envelope.maximum_cpu_ns_per_turn,
                            format!("{cpu_per_turn} ns/turn"),
                        ),
                        check(
                            "barrier-idle-cpu",
                            barrier_cpu.one_core_fraction
                                < self.envelope.maximum_barrier_cpu_fraction,
                            format!("{:.3}%", barrier_cpu.one_core_fraction * 100.0),
                        ),
                        check(
                            "single-agent-footprint",
                            point.steady_bytes >= point.baseline_bytes,
                            format!(
                                "B={} S1={} P1={} C1={}",
                                point.baseline_bytes,
                                point.steady_bytes,
                                point.workload_peak_bytes,
                                point.cold_peak_bytes
                            ),
                        ),
                    ],
                );
            }
            (Err(error), _, _) | (_, Err(error), _) => self.insert_error(25, error.to_string()),
            (_, _, None) => self.insert_error(25, "N=1 sweep point is absent".to_owned()),
        }
    }

    fn evaluate_parallel_rows(&mut self) {
        if let Some(error) = self.sweep_error.clone() {
            for row in 26..=28 {
                self.insert_error(row, error.clone());
            }
            return;
        }
        let required: BTreeSet<u32> = self.timing.sweep_widths.iter().copied().collect();
        let observed: BTreeSet<u32> = self.sweep_points.keys().copied().collect();
        let Some(metrics) = self.sweep_metrics.clone() else {
            for row in 26..=28 {
                self.insert_error(row, "complete sweep metrics are absent".to_owned());
            }
            return;
        };
        let beta = metrics.headline_beta_bytes_per_agent;
        let n8 = metrics
            .points
            .iter()
            .find(|(point, _)| point.agents == 8)
            .copied();
        let maximum_workload_peak = metrics
            .points
            .iter()
            .map(|(point, _)| point.workload_peak_bytes)
            .max()
            .unwrap_or(0);
        let sweep_complete = required == observed;
        self.insert_checks(
            26,
            vec![
                check(
                    "required-widths",
                    sweep_complete,
                    format!("required={required:?} observed={observed:?}"),
                ),
                check(
                    "n8-completes",
                    n8.is_some(),
                    format!("N8 present={}", n8.is_some()),
                ),
                check(
                    "total-peak",
                    metrics.maximum_cold_peak_bytes <= self.envelope.maximum_peak_bytes
                        && maximum_workload_peak <= self.envelope.maximum_peak_bytes,
                    format!(
                        "cold={} workload={maximum_workload_peak}",
                        metrics.maximum_cold_peak_bytes
                    ),
                ),
                check(
                    "headline-beta",
                    beta.is_some_and(|value| value <= self.envelope.maximum_beta_bytes_per_agent),
                    format!("{:?} bytes/agent", beta),
                ),
            ],
        );

        let alpha = metrics.scaling_exponent_alpha;
        let marginal_curve_ok = adjacent_marginals_stable(&metrics);
        self.insert_checks(
            27,
            vec![
                check(
                    "scaling-alpha",
                    alpha.is_some_and(|value| value <= self.envelope.maximum_scaling_exponent),
                    format!("alpha={alpha:?}"),
                ),
                check(
                    "adjacent-marginals",
                    marginal_curve_ok,
                    "no adjacent marginal exceeds twice the preceding median".to_owned(),
                ),
            ],
        );

        let cleanup = self.evidence.cleanup.as_ref();
        match n8 {
            Some((point, derived)) => {
                let active = point.steady_bytes.saturating_sub(point.baseline_bytes);
                let residual_limit = residual_limit(active, self.envelope);
                self.insert_checks(
                    28,
                    vec![
                        check(
                            "reclaim-ratio",
                            derived.reclaim_ratio >= self.envelope.minimum_reclaim_ratio,
                            format!("{:.3}", derived.reclaim_ratio),
                        ),
                        check(
                            "post-close-residual",
                            derived.post_close_residual_bytes <= residual_limit,
                            format!("{} <= {residual_limit}", derived.post_close_residual_bytes),
                        ),
                        check(
                            "cleanup-deadline",
                            cleanup.is_some_and(|value| {
                                value.reclaim_after_ms <= self.timing.reclaim_deadline_ms
                            }),
                            format!(
                                "{} ms",
                                cleanup.map_or(u64::MAX, |value| value.reclaim_after_ms)
                            ),
                        ),
                        check(
                            "no-owned-worker",
                            cleanup.is_some_and(|value| value.remaining_workers == 0),
                            format!(
                                "{} workers",
                                cleanup.map_or(usize::MAX, |value| value.remaining_workers)
                            ),
                        ),
                    ],
                );
            }
            None => self.insert_error(28, "N=8 sweep point is absent".to_owned()),
        }
    }

    fn evaluate_long_horizon(&mut self) {
        let Some(observation) = &self.evidence.long_horizon else {
            self.insert_error(29, "long-horizon observation is absent".to_owned());
            return;
        };
        match long_horizon_metrics(observation) {
            Ok(metrics) => {
                let expected_turns = self.timing.long_horizon_turns;
                let residual = observation
                    .final_post_close_bytes
                    .saturating_sub(observation.baseline_bytes);
                let residual_limit = self.envelope.residual_floor_bytes.max(
                    (observation.baseline_bytes as f64
                        * self.envelope.long_horizon_baseline_fraction) as u64,
                );
                self.metrics.insert(
                    "long_horizon_bytes_per_turn".to_owned(),
                    metrics.bytes_per_turn,
                );
                self.metrics.insert(
                    "long_horizon_final_residual_bytes".to_owned(),
                    residual as f64,
                );
                self.insert_checks(
                    29,
                    vec![
                        check(
                            "long-horizon-turns",
                            metrics.final_turn >= expected_turns,
                            format!("{} >= {expected_turns}", metrics.final_turn),
                        ),
                        check(
                            "checkpoint-cadence",
                            metrics.maximum_turn_gap <= self.timing.long_horizon_sample_turns,
                            format!(
                                "maximum gap {} turns <= {}",
                                metrics.maximum_turn_gap, self.timing.long_horizon_sample_turns
                            ),
                        ),
                        check(
                            "memory-per-turn",
                            metrics.bytes_per_turn
                                <= self.envelope.maximum_long_horizon_bytes_per_turn,
                            format!("{:.3} bytes/turn", metrics.bytes_per_turn),
                        ),
                        check(
                            "final-residual",
                            residual <= residual_limit,
                            format!("{residual} <= {residual_limit}"),
                        ),
                        check(
                            "fd-leak",
                            !metrics.monotonic_fd_growth,
                            format!("monotonic={}", metrics.monotonic_fd_growth),
                        ),
                        check(
                            "thread-leak",
                            !metrics.monotonic_thread_growth,
                            format!("monotonic={}", metrics.monotonic_thread_growth),
                        ),
                    ],
                );
            }
            Err(error) => self.insert_error(29, error.to_string()),
        }
    }

    fn plateau(
        &mut self,
        name: &str,
        phase: &str,
        metric: MemoryMetric,
        minimum_processes: usize,
        trailing_ns: Option<u64>,
    ) -> Result<Plateau> {
        let plateau = match trailing_ns {
            Some(window) => {
                self.evidence
                    .series
                    .trailing_plateau(phase, metric, minimum_processes, window)?
            }
            None => self
                .evidence
                .series
                .plateau(phase, metric, minimum_processes)?,
        };
        self.plateaus.insert(name.to_owned(), plateau.clone());
        Ok(plateau)
    }

    fn insert_checks(&mut self, row: u8, assertions: Vec<Assertion>) {
        if let Some(error) = &self.sampler_error {
            self.insert_error(row, error.clone());
            return;
        }
        let id = resource_row_id(row);
        self.rows.insert(
            row,
            classify(row, id, Pillar::Resource, Some(true), &assertions, None),
        );
    }

    fn insert_error(&mut self, row: u8, message: String) {
        let id = resource_row_id(row);
        self.rows.insert(
            row,
            classify(row, id, Pillar::Resource, Some(true), &[], Some(message)),
        );
    }

    fn finish(self) -> ResourceCertification {
        ResourceCertification {
            timing: self.timing,
            rows: self.rows.into_values().collect(),
            plateaus: self.plateaus,
            sampling_health: self.sampling_health,
            sweep_metrics: self.sweep_metrics,
            metrics: self.metrics,
        }
    }
}

fn check(name: &str, passed: bool, detail: String) -> Assertion {
    Assertion {
        name: name.to_owned(),
        passed,
        detail,
    }
}

fn resource_row_id(row: u8) -> &'static str {
    crate::scenarios::all()
        .iter()
        .find(|definition| definition.row == row)
        .map_or("unknown-resource-row", |definition| definition.id)
}

fn metric_value(metric: MemoryMetric, sample: &crate::process::Sample) -> Option<u64> {
    match metric {
        MemoryMetric::Rss => Some(sample.rss_bytes),
        MemoryMetric::Pss => sample.pss_bytes,
        MemoryMetric::Footprint => sample.footprint_bytes,
        MemoryMetric::Cgroup => sample.cgroup_memory_bytes,
        MemoryMetric::Effective => Some(
            sample
                .pss_bytes
                .or(sample.footprint_bytes)
                .unwrap_or(sample.rss_bytes),
        ),
    }
}

fn phase_values<'a>(
    series: &'a SampleSeries,
    phase: &'a str,
    metric: MemoryMetric,
) -> Result<Vec<(u64, u64)>> {
    let values: Vec<(u64, u64)> = series
        .samples
        .iter()
        .filter(|sample| sample.phase == phase)
        .map(|sample| {
            metric_value(metric, sample)
                .map(|value| (sample.elapsed_ns, value))
                .ok_or_else(|| {
                    AhrbError::Unsupported(format!(
                        "memory metric {metric:?} is unavailable in phase {phase:?}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(AhrbError::Validation(format!(
            "phase {phase:?} has no samples"
        )));
    }
    Ok(values)
}

fn phase_peak(series: &SampleSeries, phase: &str, metric: MemoryMetric) -> Result<u64> {
    phase_values(series, phase, metric)?
        .into_iter()
        .map(|(_, value)| value)
        .max()
        .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no peak")))
}

fn phase_net_growth(series: &SampleSeries, phase: &str, metric: MemoryMetric) -> Result<u64> {
    let values = phase_values(series, phase, metric)?;
    let first = values
        .first()
        .map(|(_, value)| *value)
        .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no first sample")))?;
    let last = values
        .last()
        .map(|(_, value)| *value)
        .ok_or_else(|| AhrbError::Validation(format!("phase {phase:?} has no last sample")))?;
    Ok(last.saturating_sub(first))
}

fn derive_sweep_point(
    series: &SampleSeries,
    observation: &SweepObservation,
    metric: MemoryMetric,
    trailing_ns: u64,
    maximum_spread: f64,
) -> Result<(SweepPoint, Vec<(String, Plateau)>)> {
    if observation.agents == 0 {
        return Err(AhrbError::Validation(
            "sweep observation has N=0".to_owned(),
        ));
    }
    if observation.minimum_steady_processes < observation.agents as usize {
        return Err(AhrbError::Validation(format!(
            "N={} steady membership requirement {} is smaller than N",
            observation.agents, observation.minimum_steady_processes
        )));
    }
    let baseline = series.plateau(
        &observation.baseline_phase,
        metric,
        observation.minimum_baseline_processes,
    )?;
    let steady = series.trailing_plateau(
        &observation.steady_phase,
        metric,
        observation.minimum_steady_processes,
        trailing_ns,
    )?;
    let post_turn = series.plateau(
        &observation.post_turn_phase,
        metric,
        observation.minimum_post_turn_processes,
    )?;
    let post_close = series.plateau(
        &observation.post_close_phase,
        metric,
        observation.minimum_post_close_processes,
    )?;
    for (name, plateau) in [
        ("baseline", &baseline),
        ("steady", &steady),
        ("post-turn", &post_turn),
        ("post-close", &post_close),
    ] {
        if !plateau.trustworthy || plateau.relative_spread > maximum_spread {
            return Err(AhrbError::Validation(format!(
                "N={} {name} plateau is untrustworthy (spread {:.3}%, min processes {})",
                observation.agents,
                plateau.relative_spread * 100.0,
                plateau.minimum_observed_processes
            )));
        }
    }
    let point = SweepPoint {
        agents: observation.agents,
        baseline_bytes: baseline.median_bytes,
        steady_bytes: steady.median_bytes,
        workload_peak_bytes: phase_peak(series, &observation.workload_phase, metric)?,
        cold_peak_bytes: phase_peak(series, &observation.cold_phase, metric)?,
        post_turn_bytes: post_turn.median_bytes,
        post_close_bytes: post_close.median_bytes,
    };
    Ok((
        point,
        vec![
            (format!("n{}-baseline", observation.agents), baseline),
            (format!("n{}-steady", observation.agents), steady),
            (format!("n{}-post-turn", observation.agents), post_turn),
            (format!("n{}-post-close", observation.agents), post_close),
        ],
    ))
}

struct RecoveryMetrics {
    baseline_bytes: u64,
    active_bytes: u64,
    residual_bytes: u64,
}

impl RecoveryMetrics {
    fn residual_limit_bytes(&self, envelope: &ResourceEnvelope) -> u64 {
        let active_delta = self.active_bytes.saturating_sub(self.baseline_bytes);
        residual_limit(active_delta, envelope)
    }
}

fn derive_recovery(
    series: &SampleSeries,
    observation: &ReturnToIdleObservation,
    metric: MemoryMetric,
) -> Result<RecoveryMetrics> {
    let baseline = series.plateau(
        &observation.baseline_phase,
        metric,
        observation.minimum_baseline_processes,
    )?;
    let returned = series.plateau(
        &observation.returned_phase,
        metric,
        observation.minimum_returned_processes,
    )?;
    if !baseline.trustworthy || !returned.trustworthy {
        return Err(AhrbError::Validation(
            "ordinary return-to-idle plateaus are untrustworthy".to_owned(),
        ));
    }
    let active_bytes = phase_peak(series, &observation.active_phase, metric)?;
    Ok(RecoveryMetrics {
        baseline_bytes: baseline.median_bytes,
        active_bytes,
        residual_bytes: returned.median_bytes.saturating_sub(baseline.median_bytes),
    })
}

fn residual_limit(active_delta: u64, envelope: &ResourceEnvelope) -> u64 {
    envelope
        .residual_floor_bytes
        .max((active_delta as f64 * envelope.residual_active_fraction) as u64)
}

fn adjacent_marginals_stable(metrics: &SweepMetrics) -> bool {
    let marginals: Vec<f64> = metrics
        .points
        .iter()
        .filter_map(|(_, point)| point.adjacent_marginal_bytes_per_agent)
        .collect();
    for index in 1..marginals.len() {
        let mut preceding = marginals[..index].to_vec();
        preceding.sort_by(f64::total_cmp);
        let median = if preceding.len() % 2 == 1 {
            preceding[preceding.len() / 2]
        } else {
            let upper = preceding.len() / 2;
            preceding[upper - 1] + (preceding[upper] - preceding[upper - 1]) / 2.0
        };
        if median > 0.0 && marginals[index] > median * 2.0 {
            return false;
        }
    }
    true
}

struct LongHorizonMetrics {
    final_turn: u32,
    maximum_turn_gap: u32,
    bytes_per_turn: f64,
    monotonic_fd_growth: bool,
    monotonic_thread_growth: bool,
}

fn long_horizon_metrics(observation: &LongHorizonObservation) -> Result<LongHorizonMetrics> {
    if observation.points.len() < 2 {
        return Err(AhrbError::Validation(
            "long-horizon observation needs at least two checkpoints".to_owned(),
        ));
    }
    for pair in observation.points.windows(2) {
        if pair[0].turn >= pair[1].turn {
            return Err(AhrbError::Validation(
                "long-horizon turns are not strictly increasing".to_owned(),
            ));
        }
    }
    let slope = linear_slope(
        &observation
            .points
            .iter()
            .map(|point| (point.turn as f64, point.memory_bytes as f64))
            .collect::<Vec<_>>(),
    )?;
    let monotonic_fd_growth = monotonic_growth(
        &observation
            .points
            .iter()
            .map(|point| point.open_fds)
            .collect::<Vec<_>>(),
    );
    let monotonic_thread_growth = monotonic_growth(
        &observation
            .points
            .iter()
            .map(|point| point.threads)
            .collect::<Vec<_>>(),
    );
    let final_turn = observation.points.last().map_or(0, |point| point.turn);
    let maximum_turn_gap = observation
        .points
        .windows(2)
        .map(|pair| pair[1].turn.saturating_sub(pair[0].turn))
        .max()
        .unwrap_or(0);
    Ok(LongHorizonMetrics {
        final_turn,
        maximum_turn_gap,
        bytes_per_turn: slope,
        monotonic_fd_growth,
        monotonic_thread_growth,
    })
}

fn linear_slope(points: &[(f64, f64)]) -> Result<f64> {
    if points.len() < 2 {
        return Err(AhrbError::Validation(
            "linear slope needs at least two points".to_owned(),
        ));
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
    if denominator <= 0.0 {
        return Err(AhrbError::Validation(
            "linear slope has no distinct turn values".to_owned(),
        ));
    }
    Ok(numerator / denominator)
}

fn monotonic_growth(values: &[u64]) -> bool {
    values.len() >= 2
        && values.windows(2).all(|pair| pair[0] <= pair[1])
        && values.first() < values.last()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluate::TestOutcome;
    use crate::process::{ProcIdentity, ProcOwnership, ProcessInfo, Sample};
    use std::time::SystemTime;

    fn sample(
        elapsed_ns: u64,
        phase: &str,
        bytes: u64,
        cpu_ns: u64,
        process_count: usize,
    ) -> Sample {
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
            phase: phase.to_owned(),
            rss_bytes: bytes,
            pss_bytes: Some(bytes),
            private_bytes: Some(bytes),
            footprint_bytes: None,
            rss_crosscheck_bytes: None,
            cgroup_memory_bytes: None,
            cgroup_peak_bytes: None,
            cpu_ns,
            collection_ns: 1_000,
            processes,
        }
    }

    fn add_phase(
        series: &mut SampleSeries,
        clock: &mut u64,
        phase: &str,
        bytes: u64,
        cpu_fraction_percent: u64,
        process_count: usize,
    ) -> Result<()> {
        let initial_cpu = *clock / 100 * cpu_fraction_percent;
        for offset in 0..=2_u64 {
            let elapsed = clock.saturating_add(offset * 1_000_000_000);
            let cpu = initial_cpu.saturating_add(offset * 10_000_000 * cpu_fraction_percent);
            series.push(sample(elapsed, phase, bytes, cpu, process_count))?;
        }
        *clock = clock.saturating_add(3_000_000_000);
        Ok(())
    }

    fn passing_evidence(profile: ResourceProfile) -> Result<ResourceEvidence> {
        let mut series = SampleSeries::default();
        let mut clock = 0_u64;
        add_phase(&mut series, &mut clock, "warm-idle", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "idle-cpu", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "idle-drift", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "ordinary-base", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "ordinary-active", 120 * MIB, 1, 2)?;
        add_phase(&mut series, &mut clock, "ordinary-return", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "cold-start", 130 * MIB, 2, 1)?;
        add_phase(&mut series, &mut clock, "ready-idle", 100 * MIB, 0, 1)?;
        add_phase(&mut series, &mut clock, "single-turn", 120 * MIB, 1, 2)?;
        add_phase(&mut series, &mut clock, "single-barrier", 120 * MIB, 1, 2)?;

        let mut sweep = Vec::new();
        for agents in [1_u32, 2, 4, 8] {
            let baseline = format!("n{agents}-base");
            let workload = format!("n{agents}-workload");
            let cold = format!("n{agents}-cold");
            let steady = format!("n{agents}-steady");
            let post_turn = format!("n{agents}-post-turn");
            let post_close = format!("n{agents}-post-close");
            let active = (100 + u64::from(agents) * 8) * MIB;
            add_phase(&mut series, &mut clock, &baseline, 100 * MIB, 0, 1)?;
            add_phase(
                &mut series,
                &mut clock,
                &workload,
                active + MIB,
                2,
                agents as usize + 1,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &cold,
                active + 2 * MIB,
                2,
                agents as usize + 1,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &steady,
                active,
                1,
                agents as usize + 1,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &post_turn,
                active,
                1,
                agents as usize + 1,
            )?;
            add_phase(&mut series, &mut clock, &post_close, 100 * MIB, 0, 1)?;
            sweep.push(SweepObservation {
                agents,
                baseline_phase: baseline,
                workload_phase: workload,
                cold_phase: cold,
                steady_phase: steady,
                post_turn_phase: post_turn,
                post_close_phase: post_close,
                minimum_steady_processes: agents as usize + 1,
                minimum_baseline_processes: 1,
                minimum_post_turn_processes: agents as usize + 1,
                minimum_post_close_processes: 1,
            });
        }
        let turns = ResourceTimingPlan::for_profile(profile).long_horizon_turns;
        Ok(ResourceEvidence {
            completed_repetitions: ResourceTimingPlan::for_profile(profile).repetitions,
            series,
            phases: ResourcePhases::default(),
            memory_metric: Some(MemoryMetric::Effective),
            sampler_cadence_ns: Some(50_000_000),
            idle: Some(IdleObservation {
                declared_model: IdleProcessModel::PersistentTree,
                busy_polling_detected: Some(false),
                initial_workers: 1,
                final_workers: 1,
                initial_threads: Some(2),
                final_threads: Some(2),
            }),
            sweep,
            ordinary_return: Some(ReturnToIdleObservation {
                baseline_phase: "ordinary-base".to_owned(),
                active_phase: "ordinary-active".to_owned(),
                returned_phase: "ordinary-return".to_owned(),
                settled_after_ms: 500,
                remaining_workers: 0,
                minimum_baseline_processes: 1,
                minimum_returned_processes: 1,
            }),
            cold_start: Some(ColdStartObservation {
                cold_phase: "cold-start".to_owned(),
                ready_idle_phase: "ready-idle".to_owned(),
                readiness_ms: 50,
                startup_bound_ms: 10_000,
                minimum_idle_processes: 1,
            }),
            single_agent: Some(SingleAgentObservation {
                turn_phase: "single-turn".to_owned(),
                scripted_turns: 1,
                barrier_phase: "single-barrier".to_owned(),
            }),
            cleanup: Some(CleanupObservation {
                reclaim_after_ms: 500,
                remaining_workers: 0,
            }),
            long_horizon: Some(LongHorizonObservation {
                points: (0..=turns)
                    .step_by(100)
                    .map(|turn| LongHorizonPoint {
                        turn,
                        memory_bytes: 100 * MIB,
                        open_fds: 10,
                        threads: 2,
                    })
                    .collect(),
                baseline_bytes: 100 * MIB,
                final_post_close_bytes: 100 * MIB,
            }),
        })
    }

    #[test]
    fn timing_plans_encode_normative_cert_and_quick_windows() {
        let quick = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let cert = ResourceTimingPlan::for_profile(ResourceProfile::Cert);
        assert_eq!(quick.repetitions, 3);
        assert_eq!(cert.repetitions, 7);
        assert_eq!(quick.sweep_widths, vec![1, 2, 4, 8]);
        assert_eq!(cert.barrier_hold_ms, 3_000);
        assert_eq!(cert.barrier_discard_ms, 1_000);
        assert_eq!(cert.barrier_steady_ms, 2_000);
        assert_eq!(quick.idle_drift_ms, 30_000);
        assert_eq!(cert.idle_drift_ms, 120_000);
        assert_eq!(cert.long_horizon_turns, 1_000);
    }

    #[test]
    fn complete_stable_evidence_passes_all_resource_rows() -> Result<()> {
        let evidence = passing_evidence(ResourceProfile::Cert)?;
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &evidence,
            &ResourceEnvelope::default(),
        );
        assert_eq!(certification.rows.len(), 10);
        for row in &certification.rows {
            assert!(
                matches!(row.outcome, TestOutcome::Pass),
                "row {} did not pass: {:?}",
                row.row,
                row.outcome
            );
        }
        assert_eq!(
            certification
                .sweep_metrics
                .as_ref()
                .and_then(|metrics| metrics.headline_beta_mib_per_agent),
            Some(8.0)
        );
        Ok(())
    }

    #[test]
    fn absent_evidence_never_passes() {
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &ResourceEvidence::default(),
            &ResourceEnvelope::default(),
        );
        assert_eq!(certification.rows.len(), 10);
        assert!(certification.rows.iter().all(|row| {
            matches!(
                row.outcome,
                TestOutcome::Error(_) | TestOutcome::Fail(_) | TestOutcome::Absent(_)
            )
        }));
    }

    #[test]
    fn unstable_plateau_and_missing_n4_cannot_certify() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Cert)?;
        evidence.sweep.retain(|point| point.agents != 4);
        if let Some(sample) = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == "warm-idle")
        {
            sample.pss_bytes = Some(200 * MIB);
        }
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row20 = certification.rows.iter().find(|row| row.row == 20);
        let row26 = certification.rows.iter().find(|row| row.row == 26);
        assert!(row20.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));
        assert!(row26.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));
        Ok(())
    }

    #[test]
    fn sampler_overload_turns_every_resource_row_into_error() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        evidence.sampler_cadence_ns = Some(1);
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        assert!(certification.rows.iter().all(|row| {
            matches!(&row.outcome, TestOutcome::Error(message) if message.contains("sampler overload"))
        }));
        Ok(())
    }

    #[test]
    fn monotonic_fd_or_thread_growth_fails_long_horizon() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Cert)?;
        if let Some(long) = evidence.long_horizon.as_mut() {
            for (index, point) in long.points.iter_mut().enumerate() {
                let growth = u64::try_from(index).unwrap_or(u64::MAX);
                point.open_fds = 10_u64.saturating_add(growth);
                point.threads = 2_u64.saturating_add(growth);
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row29 = certification.rows.iter().find(|row| row.row == 29);
        assert!(row29.is_some_and(|row| matches!(row.outcome, TestOutcome::Fail(_))));
        Ok(())
    }
}
