//! Normative resource-evidence analysis for matrix rows 20 through 29.
//!
//! This module deliberately separates collection from certification.  A runner records
//! phase-labelled [`Sample`](crate::process::Sample) values and the small pieces of
//! evidence which an operating-system sampler cannot observe (for example open file
//! descriptors and thread counts).  The analyzer then applies the v1 envelope without
//! inventing evidence: a missing observation becomes `ERROR`, never `PASS`.

use crate::evaluate::{Assertion, Pillar, TestResult, classify};
use crate::process::ProcIdentity;
use crate::sampler::{
    MemoryMetric, PhaseCoverage, Plateau, SampleSeries, SamplingHealth, SweepMetrics, SweepPoint,
    cadence_quality,
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
        match profile {
            ResourceProfile::Quick => Self {
                profile,
                repetitions: 3,
                warmup_turns: 1,
                sweep_widths: vec![1, 2, 4],
                load_guard_ms: 1_000,
                load_guard_timeout_ms: 10_000,
                idle_baseline_ms: 500,
                idle_cpu_ms: 1_000,
                idle_drift_ms: 4_000,
                barrier_hold_ms: 500,
                barrier_discard_ms: 100,
                barrier_steady_ms: 400,
                reclaim_deadline_ms: 10_000,
                membership_cadence_ms: 10,
                macos_rusage_cadence_ms: 20,
                linux_cgroup_cadence_ms: 10,
                linux_smaps_cadence_ms: 50,
                long_horizon_turns: 100,
                long_horizon_sample_turns: 10,
            },
            ResourceProfile::Cert => Self {
                profile,
                repetitions: 7,
                warmup_turns: 1,
                sweep_widths: vec![1, 2, 4, 8],
                load_guard_ms: 5_000,
                load_guard_timeout_ms: 60_000,
                idle_baseline_ms: 3_000,
                idle_cpu_ms: 10_000,
                idle_drift_ms: 120_000,
                barrier_hold_ms: 3_000,
                barrier_discard_ms: 1_000,
                barrier_steady_ms: 2_000,
                reclaim_deadline_ms: 10_000,
                membership_cadence_ms: 10,
                macos_rusage_cadence_ms: 20,
                linux_cgroup_cadence_ms: 10,
                linux_smaps_cadence_ms: 50,
                long_horizon_turns: 1_000,
                long_horizon_sample_turns: 100,
            },
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
    /// Explicit fresh-profile idle phases, one entry per required repetition.
    #[serde(default)]
    pub repetitions: Vec<IdlePhaseRepetition>,
    /// Split membership/counter cadence and raw membership-refresh evidence.
    pub cadence: Option<ResourceCadenceEvidence>,
}

impl Default for ResourcePhases {
    fn default() -> Self {
        Self {
            warm_idle: "warm-idle".to_owned(),
            idle_cpu: "idle-cpu".to_owned(),
            idle_drift: "idle-drift".to_owned(),
            repetitions: Vec::new(),
            cadence: None,
        }
    }
}

/// Identity proving a measurement came from the selected profile and a fresh isolation root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepetitionIdentity {
    /// Zero-based repetition index.
    pub repetition: u32,
    /// Profile used for this measurement.
    pub profile: ResourceProfile,
    /// Opaque fresh HOME/profile/isolation token, unique across repetitions.
    pub isolation_token: String,
}

/// Unmeasured warm-up evidence required before a repetition's baseline.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WarmupObservation {
    /// Fresh-profile identity shared by the measured repetition.
    pub identity: RepetitionIdentity,
    /// Number of scripted warm-up turns completed.
    pub completed_turns: u32,
    /// Whether every warm-up turn reached a structural terminal.
    pub terminalized: bool,
    /// Whether every warm-up session was officially closed/deleted.
    pub closed: bool,
}

/// Platform counter whose cadence is represented by the resource samples.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourceCounterKind {
    /// macOS `proc_pid_rusage` resource counters.
    MacOsRusage,
    /// Linux cgroup-v2 memory/cpu counters.
    LinuxCgroup,
    /// Linux per-process smaps-rollup counters.
    LinuxSmapsRollup,
}

/// Timing for one recursive process-membership refresh.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipRefreshEvidence {
    /// Monotonic observation-start time since resource collection began.
    pub elapsed_ns: u64,
    /// Wall time consumed by recursive membership discovery.
    pub discovery_wall_ns: u64,
    /// Calling-thread CPU consumed by recursive membership discovery.
    pub discovery_cpu_ns: u64,
    /// Deterministic staggered sampler lane.
    pub lane: u32,
}

/// Separate cadence evidence for process membership and resource counters.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceCadenceEvidence {
    /// Requested recursive ownership-membership refresh cadence.
    pub membership_cadence_ns: u64,
    /// Requested platform resource-counter cadence.
    pub counter_cadence_ns: u64,
    /// Counter implementation used by the samples.
    pub counter_kind: ResourceCounterKind,
    /// Raw membership refresh timestamps grouped by phase.
    pub membership_samples_by_phase: BTreeMap<String, Vec<u64>>,
    /// Auditable membership discovery timing grouped by phase.
    #[serde(default)]
    pub membership_refreshes_by_phase: BTreeMap<String, Vec<MembershipRefreshEvidence>>,
}

/// Phase labels proving that idle measurements were repeated on a fresh profile.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IdlePhaseRepetition {
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
    /// Warm idle baseline phase in this repetition.
    pub warm_idle: String,
    /// Quiet CPU phase in this repetition.
    pub idle_cpu: String,
    /// Untouched drift phase in this repetition.
    pub idle_drift: String,
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

/// Detect an ungated, approximately half-second idle polling loop from sampled
/// whole-tree CPU counters.
///
/// A poll is represented by a short CPU burst followed by quiescence. Adjacent
/// active samples are coalesced into one burst, then at least three consecutive
/// burst intervals must fall within 500 ms +/- 150 ms. Continuous CPU activity is
/// handled by the independent idle-CPU ceiling rather than mislabeled as polling.
pub fn detect_busy_polling(
    series: &SampleSeries,
    repetitions: &[IdlePhaseRepetition],
    counter_cadence_ns: u64,
) -> Result<bool> {
    if counter_cadence_ns == 0 {
        return Err(AhrbError::Validation(
            "busy-poll detector cadence must be nonzero".to_owned(),
        ));
    }
    let phases: BTreeSet<&str> = repetitions
        .iter()
        .flat_map(|repetition| {
            [
                repetition.warm_idle.as_str(),
                repetition.idle_cpu.as_str(),
                repetition.idle_drift.as_str(),
            ]
        })
        .collect();
    if phases.is_empty() {
        return Err(AhrbError::Validation(
            "busy-poll detector has no idle phases".to_owned(),
        ));
    }
    let coalesce_ns = counter_cadence_ns.saturating_mul(2);
    const NOMINAL_POLL_NS: u64 = 500_000_000;
    const POLL_TOLERANCE_NS: u64 = 150_000_000;
    for phase in phases {
        let samples: Vec<_> = series
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .collect();
        if samples.len() < 2 {
            return Err(AhrbError::Validation(format!(
                "busy-poll detector phase {phase:?} needs at least two samples"
            )));
        }
        let mut burst_times = Vec::new();
        let mut last_active = None;
        for pair in samples.windows(2) {
            if pair[1].cpu_ns < pair[0].cpu_ns {
                return Err(AhrbError::Validation(format!(
                    "busy-poll detector phase {phase:?} CPU counter regressed"
                )));
            }
            if pair[1].cpu_ns == pair[0].cpu_ns {
                continue;
            }
            let active_at = pair[1].elapsed_ns;
            if last_active.is_none_or(|previous| active_at.saturating_sub(previous) > coalesce_ns) {
                burst_times.push(active_at);
            }
            last_active = Some(active_at);
        }
        let gaps: Vec<_> = burst_times
            .windows(2)
            .map(|pair| pair[1].saturating_sub(pair[0]))
            .collect();
        let matching = gaps
            .iter()
            .filter(|gap| gap.abs_diff(NOMINAL_POLL_NS) <= POLL_TOLERANCE_NS)
            .count();
        if matching >= 3 && matching.saturating_mul(4) >= gaps.len().saturating_mul(3) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Phase references and membership requirements for one fresh-profile N point.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SweepObservation {
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
    /// Simultaneous agents at the shared state barrier.
    pub agents: u32,
    /// Logical actor IDs required to reach the named barrier.
    pub expected_barrier_actors: BTreeSet<String>,
    /// Logical actor IDs actually observed at the same validated transition.
    pub observed_barrier_actors: BTreeSet<String>,
    /// Stable fake-model barrier/checkpoint name shared by those actors.
    pub barrier_checkpoint: String,
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
    /// Time from official close/delete until the stable post-close window.
    pub post_close_settled_after_ms: u64,
    /// Seed used to rotate width order deterministically.
    pub width_rotation_seed: u64,
    /// Zero-based position of this width in the seeded repetition order.
    pub width_order_index: u32,
}

/// Ordinary workflow return-to-idle evidence for row 23.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReturnToIdleObservation {
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
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
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
    /// Launch-to-ready phase, including the cold peak.
    pub cold_phase: String,
    /// Ready idle phase.
    pub ready_idle_phase: String,
    /// Delay from launch initiation until the first sampled cold boundary.
    pub sampling_started_after_launch_ms: u64,
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
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
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
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
    /// Time until reclaim was sampled.
    pub reclaim_after_ms: u64,
    /// Owned session workers remaining after cleanup grace.
    pub remaining_workers: usize,
    /// Exact actor-to-session set created for the maximum-width group.
    pub expected_actor_sessions: BTreeMap<String, String>,
    /// Exact actor-to-session set successfully closed through the official surface.
    pub closed_actor_sessions: BTreeMap<String, String>,
    /// Owned process identities at the pre-workload baseline.
    pub baseline_processes: BTreeSet<ProcIdentity>,
    /// Owned process identities after close/delete and reclaim settling.
    pub post_close_processes: BTreeSet<ProcIdentity>,
    /// Whole-tree thread count at the pre-workload baseline.
    pub baseline_threads: Option<u64>,
    /// Whole-tree thread count after close/delete and reclaim settling.
    pub post_close_threads: Option<u64>,
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

/// One durable fixture result attributed to a specific long-horizon turn.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LongHorizonToolResult {
    /// Stable normalized event identity.
    pub event_id: String,
    /// Durable journal cursor, used to distinguish duplicate result events.
    pub cursor: u64,
    /// Tool-call identity emitted by the scripted model response.
    pub call_id: String,
    /// Canonical fixture tool name.
    pub name: String,
}

/// Long-horizon memory and descriptor/thread evidence for row 29.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LongHorizonObservation {
    /// Fresh-profile identity.
    pub identity: RepetitionIdentity,
    /// Stable warm baseline phase before the long session is created.
    pub baseline_phase: String,
    /// Stable post-close phase after the long session is officially deleted.
    pub final_post_close_phase: String,
    /// Ordered checkpoints, conventionally every 100 turns.
    pub points: Vec<LongHorizonPoint>,
    /// Number of turns that reached a durable terminal event.
    pub completed_turns: u32,
    /// Every turn mapped to its distinct durable tool-result events.
    pub tool_results_by_turn: BTreeMap<u32, Vec<LongHorizonToolResult>>,
    /// Session created for the long-horizon workload.
    pub expected_session_id: String,
    /// Session successfully closed through the official surface.
    pub closed_session_id: Option<String>,
    /// Warm baseline before the first turn.
    pub baseline_bytes: u64,
    /// Stable memory after the final close/delete.
    pub final_post_close_bytes: u64,
    /// Whole-tree FD count before creating the long session.
    pub baseline_open_fds: u64,
    /// Whole-tree FD count after final close/delete.
    pub final_post_close_open_fds: u64,
    /// Whole-tree thread count before creating the long session.
    pub baseline_threads: u64,
    /// Whole-tree thread count after final close/delete.
    pub final_post_close_threads: u64,
    /// Owned process identities before creating the long session.
    pub baseline_processes: BTreeSet<ProcIdentity>,
    /// Owned process identities after final close/delete.
    pub final_post_close_processes: BTreeSet<ProcIdentity>,
}

/// Complete collection input for resource rows.  Optional fields are intentional:
/// absence is reported as infrastructure `ERROR`, not inferred success.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ResourceEvidence {
    /// Legacy diagnostic count reported by collectors. Certification derives
    /// completeness from explicit repetition IDs and never trusts this scalar.
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
    /// One completed, closed, unmeasured warm-up per fresh repetition.
    pub warmup: Option<Vec<WarmupObservation>>,
    /// N=1,2,4,8 fresh-profile observations.
    #[serde(default)]
    pub sweep: Vec<SweepObservation>,
    /// Ordinary workflow return-to-idle observation.
    pub ordinary_return: Option<Vec<ReturnToIdleObservation>>,
    /// Cold start observation.
    pub cold_start: Option<Vec<ColdStartObservation>>,
    /// Single-agent CPU observation.
    pub single_agent: Option<Vec<SingleAgentObservation>>,
    /// Post-close worker cleanup observation.
    pub cleanup: Option<Vec<CleanupObservation>>,
    /// Long-horizon memory/FD/thread observation.
    pub long_horizon: Option<Vec<LongHorizonObservation>>,
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

/// One completed fresh-process resource trial. Memory is the sampled whole-tree
/// peak while all `agents` CLI processes are live; process exit is the reclaim
/// boundary and is intentionally not compared with a daemon idle baseline.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PerInvocationObservation {
    /// Fresh-profile repetition number.
    pub repetition: u32,
    /// Concurrent harness process count.
    pub agents: u32,
    /// Maximum effective whole-tree bytes during the turn.
    pub peak_bytes: u64,
    /// Launch-to-first-observation cold peak.
    pub cold_peak_bytes: u64,
    /// Whole-tree cumulative CPU for the scripted turn group.
    pub cpu_ns: u64,
    /// Processes that reached terminal exit.
    pub completed_processes: u32,
    /// Owned process-group members still alive after launcher terminal exit.
    #[serde(default)]
    pub residual_processes: u32,
}

/// Evaluate transient, one-process-per-turn resource evidence without folding a
/// nonexistent idle baseline into per-turn cost.
pub fn evaluate_per_invocation_resources(
    profile: ResourceProfile,
    observations: &[PerInvocationObservation],
    envelope: &ResourceEnvelope,
) -> ResourceCertification {
    let timing = ResourceTimingPlan::for_profile(profile);
    let mut rows = Vec::new();
    let mut metrics = BTreeMap::new();
    let expected_trials = timing
        .repetitions
        .saturating_mul(u32::try_from(timing.sweep_widths.len()).unwrap_or(u32::MAX));
    let trial_identities: BTreeSet<(u32, u32)> = observations
        .iter()
        .map(|observation| (observation.repetition, observation.agents))
        .collect();
    let lifecycle_error = if observations.len()
        != usize::try_from(expected_trials).unwrap_or(usize::MAX)
        || trial_identities.len() != observations.len()
    {
        Some(format!(
            "per-invocation lifecycle has {} unique trials and {} observations, expected {expected_trials}",
            trial_identities.len(),
            observations.len()
        ))
    } else {
        None
    };
    let maximum_residual_processes = observations
        .iter()
        .map(|observation| observation.residual_processes)
        .max()
        .unwrap_or(0);
    let every_tree_exited = observations.iter().all(|observation| {
        observation.completed_processes == observation.agents && observation.residual_processes == 0
    });
    metrics.insert(
        "maximum_residual_processes".to_owned(),
        f64::from(maximum_residual_processes),
    );
    let lifecycle_row = |row: u8, name: &str, detail: String| {
        classify(
            row,
            resource_row_id(row),
            Pillar::Resource,
            Some(true),
            &[Assertion {
                name: name.to_owned(),
                passed: every_tree_exited,
                detail,
            }],
            lifecycle_error.clone(),
        )
    };
    metrics.insert("idle_median_bytes".to_owned(), 0.0);
    metrics.insert("idle_cpu_one_core".to_owned(), 0.0);
    rows.push(lifecycle_row(
        20,
        "zero-process-between-turns",
        format!(
            "client process fan-out retained at most {maximum_residual_processes} owned processes between turns; certified idle footprint is 0 bytes only when every tree exits"
        ),
    ));
    rows.push(lifecycle_row(
        21,
        "zero-process-idle-cpu",
        format!(
            "{maximum_residual_processes} owned processes remained capable of busy-polling after launcher exit"
        ),
    ));
    rows.push(lifecycle_row(
        22,
        "zero-process-idle-drift",
        format!(
            "memory-at-rest drift is structurally zero only when all process groups exit; maximum residual={maximum_residual_processes}"
        ),
    ));
    rows.push(lifecycle_row(
        23,
        "process-exit-return",
        format!(
            "every completed invocation must return to zero owned processes; maximum residual={maximum_residual_processes}"
        ),
    ));

    let mut points = Vec::new();
    let mut incomplete = None;
    for agents in &timing.sweep_widths {
        let trials: Vec<_> = observations
            .iter()
            .filter(|observation| observation.agents == *agents)
            .collect();
        if trials.len() != timing.repetitions as usize {
            incomplete = Some(format!(
                "N={agents} has {} repetitions, expected {}",
                trials.len(),
                timing.repetitions
            ));
            break;
        }
        let repetitions: BTreeSet<u32> = trials.iter().map(|trial| trial.repetition).collect();
        let expected: BTreeSet<u32> = (0..timing.repetitions).collect();
        if repetitions != expected {
            incomplete = Some(format!(
                "N={agents} repetition identities were {repetitions:?}, expected {expected:?}"
            ));
            break;
        }
        if trials
            .iter()
            .any(|trial| trial.completed_processes != *agents)
        {
            incomplete = Some(format!("N={agents} did not terminalize every CLI process"));
            break;
        }
        let peak = median_u64_values(
            &trials
                .iter()
                .map(|trial| trial.peak_bytes)
                .collect::<Vec<_>>(),
        )
        .unwrap_or(0);
        let cold = median_u64_values(
            &trials
                .iter()
                .map(|trial| trial.cold_peak_bytes)
                .collect::<Vec<_>>(),
        )
        .unwrap_or(0);
        points.push(SweepPoint {
            agents: *agents,
            baseline_bytes: 0,
            steady_bytes: peak,
            workload_peak_bytes: peak,
            cold_peak_bytes: cold,
            post_turn_bytes: 0,
            post_close_bytes: 0,
        });
        metrics.insert(
            format!("parallel_n{agents}_process_peak_bytes"),
            peak as f64,
        );
    }
    let (sweep_metrics, sweep_error) = if incomplete.is_none() {
        match SweepMetrics::calculate(&points) {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(format!("per-process sweep is invalid: {error}"))),
        }
    } else {
        (None, None)
    };
    let maximum_cold = points
        .iter()
        .map(|point| point.cold_peak_bytes)
        .max()
        .unwrap_or(0);
    metrics.insert("maximum_cold_peak_bytes".to_owned(), maximum_cold as f64);
    rows.push(classify(
        24,
        resource_row_id(24),
        Pillar::Resource,
        Some(true),
        &[check(
            "transient-cold-peak",
            incomplete.is_none() && maximum_cold <= envelope.maximum_peak_bytes,
            format!("maximum transient cold peak {maximum_cold} bytes"),
        )],
        incomplete.clone(),
    ));

    let n1 = observations
        .iter()
        .filter(|observation| observation.agents == 1)
        .collect::<Vec<_>>();
    let n1_peak =
        median_u64_values(&n1.iter().map(|item| item.peak_bytes).collect::<Vec<_>>()).unwrap_or(0);
    let n1_cpu = median_u64_values(&n1.iter().map(|item| item.cpu_ns).collect::<Vec<_>>())
        .unwrap_or(u64::MAX);
    metrics.insert("single_process_peak_bytes".to_owned(), n1_peak as f64);
    metrics.insert("single_process_cpu_ns".to_owned(), n1_cpu as f64);
    rows.push(classify(
        25,
        resource_row_id(25),
        Pillar::Resource,
        Some(true),
        &[
            check(
                "single-process-measured",
                n1_peak > 0,
                format!("peak={n1_peak} bytes"),
            ),
            check(
                "single-process-cpu",
                n1_cpu <= envelope.maximum_cpu_ns_per_turn,
                format!("{n1_cpu} ns/scripted turn"),
            ),
        ],
        incomplete.clone(),
    ));

    if let Some(metrics_value) = &sweep_metrics {
        if let Some(beta) = metrics_value.headline_beta_bytes_per_agent {
            metrics.insert("parallel_beta_bytes_per_agent".to_owned(), beta);
            metrics.insert("parallel_beta_mib_per_agent".to_owned(), beta / MIB as f64);
        }
        if let Some(alpha) = metrics_value.scaling_exponent_alpha {
            metrics.insert("parallel_scaling_exponent".to_owned(), alpha);
        }
    }
    let beta = sweep_metrics
        .as_ref()
        .and_then(|value| value.headline_beta_bytes_per_agent)
        .unwrap_or(f64::INFINITY);
    let alpha = sweep_metrics
        .as_ref()
        .and_then(|value| value.scaling_exponent_alpha)
        .unwrap_or(f64::INFINITY);
    let maximum_width = timing.sweep_widths.last().copied().unwrap_or(0);
    rows.push(classify(
        26,
        resource_row_id(26),
        Pillar::Resource,
        Some(true),
        &[
            check(
                "parallel-process-width",
                points
                    .last()
                    .is_some_and(|point| point.agents == maximum_width),
                format!("measured through N={maximum_width}"),
            ),
            check(
                "per-process-beta",
                beta <= envelope.maximum_beta_bytes_per_agent,
                format!("beta={beta:.3} bytes/process"),
            ),
        ],
        incomplete.clone().or_else(|| sweep_error.clone()),
    ));
    rows.push(classify(
        27,
        resource_row_id(27),
        Pillar::Resource,
        Some(true),
        &[check(
            "process-scaling-alpha",
            alpha <= envelope.maximum_scaling_exponent,
            format!("alpha={alpha:.6}"),
        )],
        incomplete.or(sweep_error),
    ));
    rows.push(lifecycle_row(
        28,
        "process-exit-reclaim",
        format!(
            "automatic PASS only after whole-tree exit is observed; maximum residual={maximum_residual_processes}"
        ),
    ));
    rows.push(lifecycle_row(
        29,
        "process-exit-long-horizon",
        format!(
            "automatic PASS only after every fresh process group exits; maximum residual={maximum_residual_processes}"
        ),
    ));
    rows.sort_by_key(|row| row.row);
    ResourceCertification {
        timing,
        rows,
        plateaus: BTreeMap::new(),
        sampling_health: None,
        sweep_metrics,
        metrics,
    }
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
    sweep_repetition_points: BTreeMap<(u32, u32), SweepPoint>,
    sweep_reclaim_after_ms: BTreeMap<u32, u64>,
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
            sweep_repetition_points: BTreeMap::new(),
            sweep_reclaim_after_ms: BTreeMap::new(),
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
        if let Some(error) = self.sampler_error.clone() {
            for row in 20..=29 {
                self.insert_error(row, error.clone());
            }
        }
        for row in 20..=29 {
            if !self.rows.contains_key(&row) {
                self.insert_error(row, "resource row was not evaluated".to_owned());
            }
        }
    }

    fn evaluate_sampler_health(&mut self) {
        let Some(cadence) = self.evidence.phases.cadence.as_ref() else {
            self.sampler_error = Some("split sampler cadence evidence is absent".to_owned());
            return;
        };
        let expected_membership_ns = self.timing.membership_cadence_ms.saturating_mul(1_000_000);
        let (expected_kind, expected_counter_ns) = self.expected_counter_cadence();
        if cadence.membership_cadence_ns != expected_membership_ns
            || cadence.counter_kind != expected_kind
            || cadence.counter_cadence_ns != expected_counter_ns
        {
            self.sampler_error = Some(format!(
                "sampler cadence mismatch: membership={} ns expected={} ns, counter={:?}/{} ns expected={:?}/{} ns",
                cadence.membership_cadence_ns,
                expected_membership_ns,
                cadence.counter_kind,
                cadence.counter_cadence_ns,
                expected_kind,
                expected_counter_ns
            ));
        }
        if self
            .evidence
            .series
            .samples
            .iter()
            .any(|sample| sample.collection_ns == 0 || sample.collection_wall_ns == 0)
            && self.sampler_error.is_none()
        {
            self.sampler_error = Some(
                "sampler collection-duration evidence is absent from one or more samples"
                    .to_owned(),
            );
        }
        if let Err(error) = validate_membership_cadence(cadence) {
            if self.sampler_error.is_none() {
                self.sampler_error = Some(error.to_string());
            }
        }
        match self
            .evidence
            .series
            .sampling_health(cadence.counter_cadence_ns)
        {
            Ok(health) => {
                self.metrics.insert(
                    "sampler_collection_cpu_fraction".to_owned(),
                    health.collection_cpu_fraction,
                );
                if health.overloaded && self.sampler_error.is_none() {
                    self.sampler_error = Some(format!(
                        "sampler overload: {:.3}% aggregate sampler thread CPU, {} collection wall overruns, {} bounded-jitter cadence gaps",
                        health.collection_cpu_fraction * 100.0,
                        health.cadence_overruns,
                        health.cadence_gaps
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

    fn expected_counter_cadence(&self) -> (ResourceCounterKind, u64) {
        #[cfg(target_os = "macos")]
        {
            return (
                ResourceCounterKind::MacOsRusage,
                self.timing
                    .macos_rusage_cadence_ms
                    .saturating_mul(1_000_000),
            );
        }
        #[cfg(target_os = "linux")]
        {
            let (kind, cadence_ms) = match self.evidence.memory_metric {
                Some(MemoryMetric::Cgroup) => (
                    ResourceCounterKind::LinuxCgroup,
                    self.timing.linux_cgroup_cadence_ms,
                ),
                _ => (
                    ResourceCounterKind::LinuxSmapsRollup,
                    self.timing.linux_smaps_cadence_ms,
                ),
            };
            return (kind, cadence_ms.saturating_mul(1_000_000));
        }
        #[allow(unreachable_code)]
        (
            ResourceCounterKind::LinuxSmapsRollup,
            self.timing.linux_smaps_cadence_ms.saturating_mul(1_000_000),
        )
    }

    fn validate_repetition_component(
        &self,
        component: &str,
        identities: &[&RepetitionIdentity],
    ) -> Result<()> {
        let (_, expected_tokens) = complete_idle_repetitions(
            &self.evidence.phases.repetitions,
            self.timing.repetitions,
            self.timing.profile,
        )?;
        validate_component_identities(
            component,
            identities,
            self.timing.profile,
            self.timing.repetitions,
            Some(&expected_tokens),
        )?;
        Ok(())
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
        let (repetitions, _) = match complete_idle_repetitions(
            &self.evidence.phases.repetitions,
            self.timing.repetitions,
            self.timing.profile,
        ) {
            Ok(repetitions) => repetitions,
            Err(error) => {
                for row in 20..=22 {
                    self.insert_error(row, error.to_string());
                }
                return;
            }
        };
        let minimum = match idle.declared_model {
            IdleProcessModel::PersistentTree => 1,
            IdleProcessModel::ZeroProcessBetweenTurns => 0,
        };
        let mut baseline_medians = Vec::new();
        let mut baseline_checks = Vec::new();
        let mut baseline_error = None;
        for repetition in &repetitions {
            let result = self
                .require_phase_coverage(&repetition.warm_idle, self.timing.idle_baseline_ms)
                .and_then(|()| {
                    self.plateau(
                        &format!("rep{}-idle-baseline", repetition.identity.repetition),
                        &repetition.warm_idle,
                        metric,
                        minimum,
                        Some(self.timing.idle_baseline_ms.saturating_mul(1_000_000)),
                    )
                });
            match result {
                Ok(plateau) => {
                    let samples: Vec<_> = self
                        .evidence
                        .series
                        .samples
                        .iter()
                        .filter(|sample| sample.phase == repetition.warm_idle)
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
                    baseline_medians.push(plateau.median_bytes);
                    baseline_checks.push(check(
                        &format!("rep{}-idle-topology", repetition.identity.repetition),
                        topology_matches,
                        format!("declared and observed {:?}", idle.declared_model),
                    ));
                    baseline_checks.push(check(
                        &format!("rep{}-idle-plateau", repetition.identity.repetition),
                        plateau.trustworthy
                            && plateau.relative_spread <= self.envelope.maximum_plateau_spread,
                        format!(
                            "median={} spread={:.3}% processes>={}",
                            plateau.median_bytes,
                            plateau.relative_spread * 100.0,
                            plateau.minimum_observed_processes
                        ),
                    ));
                }
                Err(error) => {
                    baseline_error = Some(error.to_string());
                    break;
                }
            }
        }
        if let Some(error) = baseline_error {
            self.insert_error(20, error);
        } else if let Some(median) = median_u64_values(&baseline_medians) {
            self.metrics
                .insert("idle_median_bytes".to_owned(), median as f64);
            record_distribution(
                &mut self.metrics,
                "idle_baseline_bytes",
                &baseline_medians
                    .iter()
                    .map(|value| *value as f64)
                    .collect::<Vec<_>>(),
            );
            self.insert_checks(20, baseline_checks);
        } else {
            self.insert_error(20, "idle baseline repetitions are absent".to_owned());
        }

        let mut cpu_values = Vec::new();
        let mut cpu_checks = Vec::new();
        let mut cpu_error = None;
        for repetition in &repetitions {
            let result = self
                .require_phase_coverage(&repetition.idle_cpu, self.timing.idle_cpu_ms)
                .and_then(|()| self.evidence.series.phase_cpu(&repetition.idle_cpu));
            match result {
                Ok(cpu) => {
                    cpu_values.push(cpu.one_core_fraction);
                    cpu_checks.push(check(
                        &format!("rep{}-idle-cpu", repetition.identity.repetition),
                        cpu.one_core_fraction <= self.envelope.maximum_idle_cpu_fraction,
                        format!("{:.3}% of one core", cpu.one_core_fraction * 100.0),
                    ));
                }
                Err(error) => {
                    cpu_error = Some(error.to_string());
                    break;
                }
            }
        }
        match (cpu_error, idle.busy_polling_detected) {
            (Some(error), _) => self.insert_error(21, error),
            (_, None) => self.insert_error(21, "busy-polling evidence is absent".to_owned()),
            (None, Some(busy_polling_detected)) => {
                cpu_checks.push(check(
                    "busy-polling",
                    !busy_polling_detected,
                    format!("detected={busy_polling_detected}"),
                ));
                if let Some(median) = median_f64_values(&cpu_values) {
                    self.metrics.insert("idle_cpu_one_core".to_owned(), median);
                }
                record_distribution(&mut self.metrics, "idle_cpu_one_core", &cpu_values);
                self.insert_checks(21, cpu_checks);
            }
        }

        let mut slopes = Vec::new();
        let mut net_growths = Vec::new();
        let mut drift_checks = Vec::new();
        let mut drift_error = None;
        for repetition in &repetitions {
            let result = self
                .require_phase_coverage(&repetition.idle_drift, self.timing.idle_drift_ms)
                .and_then(|()| {
                    let samples: Vec<_> = self
                        .evidence
                        .series
                        .samples
                        .iter()
                        .filter(|sample| sample.phase == repetition.idle_drift)
                        .collect();
                    let first = samples.first().copied().ok_or_else(|| {
                        AhrbError::Validation(format!(
                            "idle drift phase {:?} has no first sample",
                            repetition.idle_drift
                        ))
                    })?;
                    let last = samples.last().copied().ok_or_else(|| {
                        AhrbError::Validation(format!(
                            "idle drift phase {:?} has no last sample",
                            repetition.idle_drift
                        ))
                    })?;
                    let persistent_root = usize::from(matches!(
                        idle.declared_model,
                        IdleProcessModel::PersistentTree
                    ));
                    let initial_threads = first.thread_count.ok_or_else(|| {
                        AhrbError::Validation(format!(
                            "idle drift phase {:?} has no initial thread count",
                            repetition.idle_drift
                        ))
                    })?;
                    let final_threads = last.thread_count.ok_or_else(|| {
                        AhrbError::Validation(format!(
                            "idle drift phase {:?} has no final thread count",
                            repetition.idle_drift
                        ))
                    })?;
                    Ok((
                        self.evidence
                            .series
                            .drift_bytes_per_minute(&repetition.idle_drift, metric)?,
                        phase_net_growth(&self.evidence.series, &repetition.idle_drift, metric)?,
                        first.processes.len().saturating_sub(persistent_root),
                        last.processes.len().saturating_sub(persistent_root),
                        initial_threads,
                        final_threads,
                    ))
                });
            match result {
                Ok((
                    slope,
                    net_growth,
                    initial_workers,
                    final_workers,
                    initial_threads,
                    final_threads,
                )) => {
                    slopes.push(slope);
                    net_growths.push(net_growth);
                    drift_checks.push(check(
                        &format!("rep{}-idle-drift", repetition.identity.repetition),
                        slope <= self.envelope.maximum_idle_drift_bytes_per_minute,
                        format!("{slope:.3} bytes/minute"),
                    ));
                    drift_checks.push(check(
                        &format!("rep{}-idle-net-growth", repetition.identity.repetition),
                        net_growth <= self.envelope.maximum_idle_net_growth_bytes,
                        format!("{net_growth} bytes"),
                    ));
                    drift_checks.push(check(
                        &format!("rep{}-worker-growth", repetition.identity.repetition),
                        final_workers <= initial_workers,
                        format!("{initial_workers} -> {final_workers}"),
                    ));
                    drift_checks.push(check(
                        &format!("rep{}-thread-growth", repetition.identity.repetition),
                        final_threads <= initial_threads,
                        format!("{initial_threads} -> {final_threads}"),
                    ));
                }
                Err(error) => {
                    drift_error = Some(error.to_string());
                    break;
                }
            }
        }
        match drift_error {
            None => {
                let slope = median_f64_values(&slopes).unwrap_or(f64::INFINITY);
                let net_growth = median_u64_values(&net_growths).unwrap_or(u64::MAX);
                self.metrics
                    .insert("idle_drift_bytes_per_minute".to_owned(), slope);
                self.metrics
                    .insert("idle_net_growth_bytes".to_owned(), net_growth as f64);
                record_distribution(&mut self.metrics, "idle_drift_bytes_per_minute", &slopes);
                record_distribution(
                    &mut self.metrics,
                    "idle_net_growth_bytes",
                    &net_growths
                        .iter()
                        .map(|value| *value as f64)
                        .collect::<Vec<_>>(),
                );
                self.insert_checks(22, drift_checks);
            }
            Some(error) => self.insert_error(22, error),
        }
    }

    fn derive_sweep(&mut self) {
        let Some(metric) = self.evidence.memory_metric else {
            return;
        };
        let Some(cadence) = self.evidence.phases.cadence.as_ref() else {
            return;
        };
        let expected_tokens = match complete_idle_repetitions(
            &self.evidence.phases.repetitions,
            self.timing.repetitions,
            self.timing.profile,
        ) {
            Ok((_, tokens)) => tokens,
            Err(error) => {
                self.sweep_error = Some(error.to_string());
                return;
            }
        };
        if let Err(error) = validate_warmups(
            self.evidence.warmup.as_deref(),
            &self.timing,
            &expected_tokens,
        ) {
            self.sweep_error = Some(error.to_string());
            return;
        }
        if let Err(error) =
            validate_sweep_structure(&self.evidence.sweep, &self.timing, &expected_tokens)
        {
            self.sweep_error = Some(error.to_string());
            return;
        }
        let expected_repetitions: BTreeSet<u32> = (0..self.timing.repetitions).collect();
        let mut grouped: BTreeMap<u32, BTreeMap<u32, SweepPoint>> = BTreeMap::new();
        for observation in &self.evidence.sweep {
            match derive_sweep_point(
                &self.evidence.series,
                observation,
                metric,
                &self.timing,
                cadence,
                self.envelope.maximum_plateau_spread,
            ) {
                Ok((point, named_plateaus)) => {
                    if !expected_repetitions.contains(&observation.identity.repetition) {
                        self.sweep_error.get_or_insert_with(|| {
                            format!(
                                "resource observation N={} has out-of-range repetition {}",
                                point.agents, observation.identity.repetition
                            )
                        });
                        continue;
                    }
                    if grouped
                        .entry(point.agents)
                        .or_default()
                        .insert(observation.identity.repetition, point)
                        .is_some()
                    {
                        self.sweep_error.get_or_insert_with(|| {
                            format!(
                                "duplicate resource observation for N={} repetition {}",
                                point.agents, observation.identity.repetition
                            )
                        });
                    }
                    self.sweep_repetition_points
                        .insert((point.agents, observation.identity.repetition), point);
                    self.sweep_reclaim_after_ms
                        .entry(point.agents)
                        .and_modify(|value| {
                            *value = (*value).max(observation.post_close_settled_after_ms);
                        })
                        .or_insert(observation.post_close_settled_after_ms);
                    for (name, plateau) in named_plateaus {
                        self.plateaus.insert(name, plateau);
                    }
                }
                Err(error) => {
                    self.sweep_error.get_or_insert_with(|| {
                        format!(
                            "could not derive N={} repetition {}: {error}",
                            observation.agents, observation.identity.repetition
                        )
                    });
                }
            }
        }
        let required_widths: BTreeSet<u32> = self.timing.sweep_widths.iter().copied().collect();
        let observed_widths: BTreeSet<u32> = grouped.keys().copied().collect();
        if observed_widths != required_widths {
            self.sweep_error.get_or_insert_with(|| {
                format!(
                    "resource sweep widths incomplete: required={required_widths:?} observed={observed_widths:?}"
                )
            });
        }
        for (agents, repetitions) in grouped {
            let observed: BTreeSet<u32> = repetitions.keys().copied().collect();
            if observed != expected_repetitions {
                self.sweep_error.get_or_insert_with(|| {
                    format!(
                        "N={agents} fresh-profile repetitions incomplete: required={expected_repetitions:?} observed={observed:?}"
                    )
                });
                continue;
            }
            match aggregate_sweep_points(repetitions.values().copied()) {
                Ok(point) => {
                    self.sweep_points.insert(agents, point);
                }
                Err(error) => {
                    self.sweep_error.get_or_insert_with(|| error.to_string());
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
                    if let Some(beta) = metrics.headline_beta_mib_per_agent {
                        self.metrics
                            .insert("parallel_beta_mib_per_agent".to_owned(), beta);
                    }
                    if let Some(alpha) = metrics.scaling_exponent_alpha {
                        self.metrics
                            .insert("parallel_scaling_exponent".to_owned(), alpha);
                    }
                    self.metrics.insert(
                        "maximum_cold_peak_bytes".to_owned(),
                        metrics.maximum_cold_peak_bytes as f64,
                    );
                    self.metrics.insert(
                        "maximum_workload_peak_bytes".to_owned(),
                        metrics.maximum_workload_peak_bytes as f64,
                    );
                    for (point, derived) in &metrics.points {
                        self.record_sweep_point_metrics(*point, *derived);
                    }
                    if let Err(error) = self.record_sweep_repetition_distributions() {
                        self.sweep_error = Some(error.to_string());
                        return;
                    }
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
        let target_agents = self.timing.sweep_widths.last().copied().unwrap_or(0);
        let target_return = self.sweep_points.get(&target_agents).copied();
        if target_return.is_none() {
            self.insert_error(
                23,
                format!("N={target_agents} return-to-idle observation is absent"),
            );
        }
        let ordinary_result = (|| -> Result<(Vec<Assertion>, Vec<f64>, Vec<f64>)> {
            let observations = self.evidence.ordinary_return.as_ref().ok_or_else(|| {
                AhrbError::Validation("ordinary return-to-idle observations are absent".to_owned())
            })?;
            let identities: Vec<&RepetitionIdentity> =
                observations.iter().map(|item| &item.identity).collect();
            self.validate_repetition_component("ordinary-return", &identities)?;
            let mut assertions = Vec::new();
            let mut residuals = Vec::new();
            let mut settled = Vec::new();
            for observation in observations {
                self.require_phase_coverage(
                    &observation.baseline_phase,
                    self.timing.idle_baseline_ms,
                )?;
                self.require_phase_coverage(
                    &observation.returned_phase,
                    self.timing.barrier_steady_ms,
                )?;
                let recovery = derive_recovery(&self.evidence.series, observation, metric)?;
                residuals.push(recovery.residual_bytes as f64);
                settled.push(observation.settled_after_ms as f64);
                assertions.extend([
                    check(
                        &format!("rep{}-ordinary-residual", observation.identity.repetition),
                        recovery.residual_bytes <= recovery.residual_limit_bytes(self.envelope),
                        format!("{} bytes", recovery.residual_bytes),
                    ),
                    check(
                        &format!("rep{}-return-deadline", observation.identity.repetition),
                        observation.settled_after_ms <= self.timing.reclaim_deadline_ms,
                        format!("{} ms", observation.settled_after_ms),
                    ),
                    check(
                        &format!("rep{}-ordinary-cleanup", observation.identity.repetition),
                        observation.remaining_workers == 0,
                        format!("{} workers", observation.remaining_workers),
                    ),
                ]);
            }
            Ok((assertions, residuals, settled))
        })();
        match (ordinary_result, target_return) {
            (Ok((mut assertions, residuals, settled)), Some(target)) => {
                let target_active = target.steady_bytes.saturating_sub(target.baseline_bytes);
                let target_residual = target
                    .post_close_bytes
                    .saturating_sub(target.baseline_bytes);
                let target_reclaim_after_ms =
                    self.sweep_reclaim_after_ms.get(&target_agents).copied();
                assertions.extend([
                    check(
                        &format!("n{target_agents}-residual"),
                        target_residual <= residual_limit(target_active, self.envelope),
                        format!("{target_residual} bytes"),
                    ),
                    check(
                        &format!("n{target_agents}-return-deadline"),
                        target_reclaim_after_ms
                            .is_some_and(|value| value <= self.timing.reclaim_deadline_ms),
                        format!("{} ms", target_reclaim_after_ms.unwrap_or(u64::MAX)),
                    ),
                ]);
                record_distribution(&mut self.metrics, "ordinary_residual_bytes", &residuals);
                record_distribution(&mut self.metrics, "ordinary_settled_ms", &settled);
                self.insert_checks(23, assertions);
            }
            (Err(error), _) => self.insert_error(23, error.to_string()),
            (_, None) => {}
        }

        let cold_result = (|| -> Result<(Vec<Assertion>, Vec<f64>, Vec<f64>)> {
            let observations = self.evidence.cold_start.as_ref().ok_or_else(|| {
                AhrbError::Validation("cold-start observations are absent".to_owned())
            })?;
            let identities: Vec<&RepetitionIdentity> =
                observations.iter().map(|item| &item.identity).collect();
            self.validate_repetition_component("cold-start", &identities)?;
            let mut assertions = Vec::new();
            let mut peaks = Vec::new();
            let mut readiness = Vec::new();
            for observation in observations {
                let cadence = self.evidence.phases.cadence.as_ref().ok_or_else(|| {
                    AhrbError::Validation("cold-start cadence evidence is absent".to_owned())
                })?;
                let counter_ms = cadence.counter_cadence_ns / 1_000_000;
                let sampled_cold_ms = observation
                    .readiness_ms
                    .saturating_sub(observation.sampling_started_after_launch_ms)
                    .max(1);
                let cold_coverage = self.evidence.series.phase_coverage(
                    &observation.cold_phase,
                    cadence.counter_cadence_ns,
                    sampled_cold_ms.saturating_mul(1_000_000),
                )?;
                let cold_membership = membership_phase_coverage(
                    cadence,
                    &observation.cold_phase,
                    sampled_cold_ms.saturating_mul(1_000_000),
                )?;
                if !cold_coverage.trustworthy || !cold_membership.trustworthy {
                    return Err(AhrbError::Validation(format!(
                        "repetition {} cold-start samples do not cover launch-to-readiness",
                        observation.identity.repetition
                    )));
                }
                self.require_phase_coverage(
                    &observation.ready_idle_phase,
                    self.timing.idle_baseline_ms,
                )?;
                let peak = phase_peak(&self.evidence.series, &observation.cold_phase, metric)?;
                let plateau = self.plateau(
                    &format!("rep{}-cold-ready-idle", observation.identity.repetition),
                    &observation.ready_idle_phase,
                    metric,
                    observation.minimum_idle_processes,
                    Some(self.timing.idle_baseline_ms.saturating_mul(1_000_000)),
                )?;
                peaks.push(peak as f64);
                readiness.push(observation.readiness_ms as f64);
                assertions.extend([
                    check(
                        &format!("rep{}-startup-bound", observation.identity.repetition),
                        observation.readiness_ms <= observation.startup_bound_ms,
                        format!(
                            "{} <= {} ms",
                            observation.readiness_ms, observation.startup_bound_ms
                        ),
                    ),
                    check(
                        &format!("rep{}-cold-peak", observation.identity.repetition),
                        peak <= self.envelope.maximum_peak_bytes,
                        format!("{peak} bytes"),
                    ),
                    check(
                        &format!("rep{}-cold-sampling-start", observation.identity.repetition),
                        observation.sampling_started_after_launch_ms <= counter_ms,
                        format!(
                            "{} <= {counter_ms} ms after launch",
                            observation.sampling_started_after_launch_ms
                        ),
                    ),
                    check(
                        &format!("rep{}-ready-plateau", observation.identity.repetition),
                        plateau.trustworthy
                            && plateau.relative_spread <= self.envelope.maximum_plateau_spread,
                        format!("spread={:.3}%", plateau.relative_spread * 100.0),
                    ),
                ]);
            }
            Ok((assertions, peaks, readiness))
        })();
        match cold_result {
            Ok((assertions, peaks, readiness)) => {
                record_distribution(&mut self.metrics, "cold_peak_bytes", &peaks);
                record_distribution(&mut self.metrics, "cold_readiness_ms", &readiness);
                self.insert_checks(24, assertions);
            }
            Err(error) => self.insert_error(24, error.to_string()),
        }
    }

    fn evaluate_single_agent(&mut self) {
        let n1 = self.sweep_points.get(&1).copied();
        let result = (|| -> Result<(Vec<Assertion>, Vec<f64>, Vec<f64>)> {
            let observations = self.evidence.single_agent.as_ref().ok_or_else(|| {
                AhrbError::Validation("single-agent observations are absent".to_owned())
            })?;
            let identities: Vec<&RepetitionIdentity> =
                observations.iter().map(|item| &item.identity).collect();
            self.validate_repetition_component("single-agent", &identities)?;
            let mut assertions = Vec::new();
            let mut cpu_per_turns = Vec::new();
            let mut barrier_fractions = Vec::new();
            for observation in observations {
                if observation.scripted_turns == 0 {
                    return Err(AhrbError::Validation(format!(
                        "single-agent repetition {} scripted turn count is zero",
                        observation.identity.repetition
                    )));
                }
                self.require_phase_coverage(
                    &observation.barrier_phase,
                    self.timing.barrier_hold_ms,
                )?;
                let turn_samples: Vec<_> = self
                    .evidence
                    .series
                    .samples
                    .iter()
                    .filter(|sample| sample.phase == observation.turn_phase)
                    .collect();
                let barrier_samples: Vec<_> = self
                    .evidence
                    .series
                    .samples
                    .iter()
                    .filter(|sample| sample.phase == observation.barrier_phase)
                    .collect();
                let turn_first = turn_samples.first().copied().ok_or_else(|| {
                    AhrbError::Validation(format!(
                        "single-agent repetition {} has no turn-start boundary",
                        observation.identity.repetition
                    ))
                })?;
                let turn_last = turn_samples.last().copied().ok_or_else(|| {
                    AhrbError::Validation(format!(
                        "single-agent repetition {} has no turn-end boundary",
                        observation.identity.repetition
                    ))
                })?;
                let barrier_first = barrier_samples.first().copied().ok_or_else(|| {
                    AhrbError::Validation(format!(
                        "single-agent repetition {} has no barrier-start sample",
                        observation.identity.repetition
                    ))
                })?;
                let barrier_last = barrier_samples.last().copied().ok_or_else(|| {
                    AhrbError::Validation(format!(
                        "single-agent repetition {} has no barrier-end sample",
                        observation.identity.repetition
                    ))
                })?;
                let turn_first_identities: BTreeSet<_> = turn_first
                    .processes
                    .iter()
                    .map(|process| process.identity)
                    .collect();
                let turn_last_identities: BTreeSet<_> = turn_last
                    .processes
                    .iter()
                    .map(|process| process.identity)
                    .collect();
                let cpu = self.evidence.series.phase_cpu(&observation.turn_phase)?;
                let barrier_cpu = self.evidence.series.phase_cpu(&observation.barrier_phase)?;
                let cpu_per_turn = cpu.cpu_ns as f64 / f64::from(observation.scripted_turns);
                cpu_per_turns.push(cpu_per_turn);
                barrier_fractions.push(barrier_cpu.one_core_fraction);
                assertions.extend([
                    check(
                        &format!(
                            "rep{}-complete-turn-boundaries",
                            observation.identity.repetition
                        ),
                        turn_first.elapsed_ns <= barrier_first.elapsed_ns
                            && turn_last.elapsed_ns >= barrier_last.elapsed_ns
                            && turn_first_identities == turn_last_identities
                            && !turn_first_identities.is_empty(),
                        format!(
                            "turn={}..{} barrier={}..{} processes={}->{}",
                            turn_first.elapsed_ns,
                            turn_last.elapsed_ns,
                            barrier_first.elapsed_ns,
                            barrier_last.elapsed_ns,
                            turn_first.processes.len(),
                            turn_last.processes.len()
                        ),
                    ),
                    check(
                        &format!("rep{}-cpu-per-turn", observation.identity.repetition),
                        cpu_per_turn <= self.envelope.maximum_cpu_ns_per_turn as f64,
                        format!("{cpu_per_turn:.3} ns/turn"),
                    ),
                    check(
                        &format!("rep{}-barrier-idle-cpu", observation.identity.repetition),
                        barrier_cpu.one_core_fraction < self.envelope.maximum_barrier_cpu_fraction,
                        format!("{:.3}%", barrier_cpu.one_core_fraction * 100.0),
                    ),
                ]);
            }
            Ok((assertions, cpu_per_turns, barrier_fractions))
        })();
        match (result, n1) {
            (Ok((mut assertions, cpu_per_turns, barrier_fractions)), Some(point)) => {
                assertions.extend([
                    check(
                        "single-agent-footprint",
                        point.steady_bytes >= point.baseline_bytes,
                        format!("B={} S1={}", point.baseline_bytes, point.steady_bytes),
                    ),
                    check(
                        "single-agent-cold-peak",
                        point.cold_peak_bytes <= self.envelope.maximum_peak_bytes,
                        format!("{} bytes", point.cold_peak_bytes),
                    ),
                    check(
                        "single-agent-workload-peak",
                        point.workload_peak_bytes <= self.envelope.maximum_peak_bytes,
                        format!("{} bytes", point.workload_peak_bytes),
                    ),
                ]);
                record_distribution(
                    &mut self.metrics,
                    "single_agent_cpu_ns_per_turn",
                    &cpu_per_turns,
                );
                record_distribution(
                    &mut self.metrics,
                    "single_agent_barrier_idle_cpu_one_core",
                    &barrier_fractions,
                );
                self.metrics.insert(
                    "single_agent_cpu_ns_per_turn".to_owned(),
                    median_f64_values(&cpu_per_turns).unwrap_or(f64::INFINITY),
                );
                self.metrics.insert(
                    "single_agent_barrier_idle_cpu_one_core".to_owned(),
                    median_f64_values(&barrier_fractions).unwrap_or(f64::INFINITY),
                );
                self.insert_checks(25, assertions);
            }
            (Err(error), _) => self.insert_error(25, error.to_string()),
            (_, None) => self.insert_error(25, "N=1 sweep point is absent".to_owned()),
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
        let target_agents = self.timing.sweep_widths.last().copied().unwrap_or(0);
        let target = metrics
            .points
            .iter()
            .find(|(point, _)| point.agents == target_agents)
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
                    &format!("n{target_agents}-completes"),
                    target.is_some(),
                    format!("N{target_agents} present={}", target.is_some()),
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

        let cleanup_result = (|| -> Result<(Vec<Assertion>, Vec<f64>)> {
            let observations = self.evidence.cleanup.as_ref().ok_or_else(|| {
                AhrbError::Validation("cleanup observations are absent".to_owned())
            })?;
            let identities: Vec<&RepetitionIdentity> =
                observations.iter().map(|item| &item.identity).collect();
            self.validate_repetition_component("cleanup", &identities)?;
            let mut assertions = Vec::new();
            let mut deadlines = Vec::new();
            for observation in observations {
                deadlines.push(observation.reclaim_after_ms as f64);
                let expected_sessions: BTreeSet<&str> = observation
                    .expected_actor_sessions
                    .values()
                    .map(String::as_str)
                    .collect();
                let expected_width = usize::try_from(target_agents).unwrap_or(usize::MAX);
                assertions.extend([
                    check(
                        &format!("rep{}-cleanup-deadline", observation.identity.repetition),
                        observation.reclaim_after_ms <= self.timing.reclaim_deadline_ms,
                        format!("{} ms", observation.reclaim_after_ms),
                    ),
                    check(
                        &format!("rep{}-no-owned-worker", observation.identity.repetition),
                        observation.remaining_workers == 0,
                        format!("{} workers", observation.remaining_workers),
                    ),
                    check(
                        &format!(
                            "rep{}-expected-cleanup-set",
                            observation.identity.repetition
                        ),
                        observation.expected_actor_sessions.len() == expected_width
                            && expected_sessions.len() == expected_width,
                        format!(
                            "{} actors, {} unique sessions, expected {expected_width}",
                            observation.expected_actor_sessions.len(),
                            expected_sessions.len()
                        ),
                    ),
                    check(
                        &format!("rep{}-official-close-set", observation.identity.repetition),
                        observation.closed_actor_sessions == observation.expected_actor_sessions,
                        format!(
                            "expected={:?} closed={:?}",
                            observation.expected_actor_sessions, observation.closed_actor_sessions
                        ),
                    ),
                    check(
                        &format!(
                            "rep{}-process-identity-reclaim",
                            observation.identity.repetition
                        ),
                        !observation.baseline_processes.is_empty()
                            && observation.post_close_processes == observation.baseline_processes,
                        format!(
                            "baseline={:?} post-close={:?}",
                            observation.baseline_processes, observation.post_close_processes
                        ),
                    ),
                    check(
                        &format!("rep{}-thread-reclaim", observation.identity.repetition),
                        observation.baseline_threads.is_some()
                            && observation.post_close_threads == observation.baseline_threads,
                        format!(
                            "baseline={:?} post-close={:?}",
                            observation.baseline_threads, observation.post_close_threads
                        ),
                    ),
                ]);
            }
            Ok((assertions, deadlines))
        })();
        match target {
            Some((point, derived)) => {
                let active = point.steady_bytes.saturating_sub(point.baseline_bytes);
                let residual_limit = residual_limit(active, self.envelope);
                let repetition_reclaim_ok = self
                    .sweep_repetition_points
                    .iter()
                    .filter(|((agents, _), _)| *agents == target_agents)
                    .all(|(_, point)| {
                        point_reclaim_ratio(*point) >= self.envelope.minimum_reclaim_ratio
                    });
                let measured_reclaim_after_ms =
                    self.sweep_reclaim_after_ms.get(&target_agents).copied();
                let (mut cleanup_assertions, cleanup_deadlines) = match cleanup_result {
                    Ok(value) => value,
                    Err(error) => {
                        self.insert_error(28, error.to_string());
                        return;
                    }
                };
                record_distribution(
                    &mut self.metrics,
                    "cleanup_reclaim_after_ms",
                    &cleanup_deadlines,
                );
                cleanup_assertions.extend([
                    check(
                        "post-close-residual",
                        derived.post_close_residual_bytes <= residual_limit,
                        format!("{} <= {residual_limit}", derived.post_close_residual_bytes),
                    ),
                    check(
                        "reclaim-ratio",
                        derived.reclaim_ratio >= self.envelope.minimum_reclaim_ratio
                            && repetition_reclaim_ok,
                        format!("{:.3}", derived.reclaim_ratio),
                    ),
                    check(
                        "measured-reclaim-deadline",
                        measured_reclaim_after_ms
                            .is_some_and(|value| value <= self.timing.reclaim_deadline_ms),
                        format!("{} ms", measured_reclaim_after_ms.unwrap_or(u64::MAX)),
                    ),
                ]);
                self.insert_checks(28, cleanup_assertions);
            }
            None => self.insert_error(28, format!("N={target_agents} sweep point is absent")),
        }
    }

    fn evaluate_long_horizon(&mut self) {
        let result = (|| -> Result<(Vec<Assertion>, Vec<f64>, Vec<f64>)> {
            let metric = self.evidence.memory_metric.ok_or_else(|| {
                AhrbError::Validation("long-horizon memory metric is absent".to_owned())
            })?;
            let observations = self.evidence.long_horizon.as_ref().ok_or_else(|| {
                AhrbError::Validation("long-horizon observations are absent".to_owned())
            })?;
            let identities: Vec<&RepetitionIdentity> =
                observations.iter().map(|item| &item.identity).collect();
            self.validate_repetition_component("long-horizon", &identities)?;
            let mut assertions = Vec::new();
            let mut slopes = Vec::new();
            let mut residuals = Vec::new();
            for observation in observations {
                self.require_phase_coverage(
                    &observation.baseline_phase,
                    self.timing.idle_baseline_ms,
                )?;
                self.require_phase_coverage(
                    &observation.final_post_close_phase,
                    self.timing
                        .barrier_discard_ms
                        .saturating_add(self.timing.barrier_steady_ms),
                )?;
                let final_plateau = self.evidence.series.trailing_plateau(
                    &observation.final_post_close_phase,
                    metric,
                    1,
                    self.timing.barrier_steady_ms.saturating_mul(1_000_000),
                )?;
                let metrics = long_horizon_metrics(observation)?;
                let residual = observation
                    .final_post_close_bytes
                    .saturating_sub(observation.baseline_bytes);
                let residual_limit = self.envelope.residual_floor_bytes.max(
                    (observation.baseline_bytes as f64
                        * self.envelope.long_horizon_baseline_fraction) as u64,
                );
                slopes.push(metrics.bytes_per_turn);
                residuals.push(residual as f64);
                let prefix = format!("rep{}", observation.identity.repetition);
                let expected_checkpoint_count = self
                    .timing
                    .long_horizon_turns
                    .checked_div(self.timing.long_horizon_sample_turns)
                    .unwrap_or(0)
                    .saturating_add(1) as usize;
                let maximum_checkpoint_fds = observation
                    .points
                    .iter()
                    .map(|point| point.open_fds)
                    .max()
                    .unwrap_or(0);
                let maximum_checkpoint_threads = observation
                    .points
                    .iter()
                    .map(|point| point.threads)
                    .max()
                    .unwrap_or(0);
                let expected_turn_records: BTreeSet<u32> =
                    (1..=self.timing.long_horizon_turns).collect();
                let observed_turn_records: BTreeSet<u32> =
                    observation.tool_results_by_turn.keys().copied().collect();
                let mut fixture_errors = Vec::new();
                let mut result_cursors = BTreeSet::new();
                let mut result_event_ids = BTreeSet::new();
                let mut result_count = 0_usize;
                for turn in 1..=self.timing.long_horizon_turns {
                    let results = observation
                        .tool_results_by_turn
                        .get(&turn)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    result_count = result_count.saturating_add(results.len());
                    for result in results {
                        result_cursors.insert(result.cursor);
                        result_event_ids.insert(result.event_id.as_str());
                    }
                    if turn % 10 == 0 {
                        let expected_call_id =
                            format!("resource-long-r{}-t{turn}", observation.identity.repetition);
                        if results.len() != 1
                            || results.first().is_none_or(|result| {
                                result.call_id != expected_call_id || result.name != "write_fixture"
                            })
                        {
                            fixture_errors.push(format!(
                                "turn {turn}: expected one write_fixture/{expected_call_id}, observed {results:?}"
                            ));
                        }
                    } else if !results.is_empty() {
                        fixture_errors.push(format!(
                            "turn {turn}: expected no fixture result, observed {results:?}"
                        ));
                    }
                }
                let distinct_results =
                    result_cursors.len() == result_count && result_event_ids.len() == result_count;
                assertions.extend([
                    check(
                        &format!("{prefix}-long-horizon-turns"),
                        observation.completed_turns == self.timing.long_horizon_turns
                            && metrics.final_turn == self.timing.long_horizon_turns,
                        format!(
                            "completed={} final-checkpoint={} expected={}",
                            observation.completed_turns,
                            metrics.final_turn,
                            self.timing.long_horizon_turns
                        ),
                    ),
                    check(
                        &format!("{prefix}-checkpoint-count"),
                        observation.points.len() == expected_checkpoint_count
                            && observation.points.first().map(|point| point.turn) == Some(0),
                        format!(
                            "{} checkpoints, expected {expected_checkpoint_count}, first={:?}",
                            observation.points.len(),
                            observation.points.first().map(|point| point.turn)
                        ),
                    ),
                    check(
                        &format!("{prefix}-checkpoint-cadence"),
                        metrics.maximum_turn_gap <= self.timing.long_horizon_sample_turns,
                        format!("maximum gap {} turns", metrics.maximum_turn_gap),
                    ),
                    check(
                        &format!("{prefix}-tool-result-turn-coverage"),
                        observed_turn_records == expected_turn_records && distinct_results,
                        format!(
                            "turn-records={} expected={} distinct-results={distinct_results}",
                            observed_turn_records.len(),
                            expected_turn_records.len()
                        ),
                    ),
                    check(
                        &format!("{prefix}-fixture-tool-cadence"),
                        fixture_errors.is_empty(),
                        if fixture_errors.is_empty() {
                            format!("{result_count} exact fixture results")
                        } else {
                            fixture_errors.join("; ")
                        },
                    ),
                    check(
                        &format!("{prefix}-official-session-close"),
                        observation.closed_session_id.as_deref()
                            == Some(observation.expected_session_id.as_str()),
                        format!(
                            "expected={:?} closed={:?}",
                            observation.expected_session_id, observation.closed_session_id
                        ),
                    ),
                    check(
                        &format!("{prefix}-memory-per-turn"),
                        metrics.bytes_per_turn <= self.envelope.maximum_long_horizon_bytes_per_turn,
                        format!("{:.3} bytes/turn", metrics.bytes_per_turn),
                    ),
                    check(
                        &format!("{prefix}-final-residual"),
                        residual <= residual_limit,
                        format!("{residual} <= {residual_limit}"),
                    ),
                    check(
                        &format!("{prefix}-final-plateau"),
                        final_plateau.trustworthy
                            && final_plateau.median_bytes == observation.final_post_close_bytes,
                        format!(
                            "trustworthy={} median={} recorded={}",
                            final_plateau.trustworthy,
                            final_plateau.median_bytes,
                            observation.final_post_close_bytes
                        ),
                    ),
                    check(
                        &format!("{prefix}-process-identity-reclaim"),
                        !observation.baseline_processes.is_empty()
                            && observation.final_post_close_processes
                                == observation.baseline_processes,
                        format!(
                            "baseline={:?} final={:?}",
                            observation.baseline_processes, observation.final_post_close_processes
                        ),
                    ),
                    check(
                        &format!("{prefix}-final-fd-reclaim"),
                        observation.final_post_close_open_fds <= maximum_checkpoint_fds,
                        format!(
                            "baseline={} final={} workload-max={maximum_checkpoint_fds}",
                            observation.baseline_open_fds, observation.final_post_close_open_fds,
                        ),
                    ),
                    check(
                        &format!("{prefix}-final-thread-reclaim"),
                        observation.final_post_close_threads <= maximum_checkpoint_threads,
                        format!(
                            "baseline={} final={} workload-max={maximum_checkpoint_threads}",
                            observation.baseline_threads, observation.final_post_close_threads,
                        ),
                    ),
                    check(
                        &format!("{prefix}-fd-leak"),
                        !metrics.monotonic_fd_growth,
                        format!("monotonic={}", metrics.monotonic_fd_growth),
                    ),
                    check(
                        &format!("{prefix}-thread-leak"),
                        !metrics.monotonic_thread_growth,
                        format!("monotonic={}", metrics.monotonic_thread_growth),
                    ),
                ]);
            }
            Ok((assertions, slopes, residuals))
        })();
        match result {
            Ok((assertions, slopes, residuals)) => {
                record_distribution(&mut self.metrics, "long_horizon_bytes_per_turn", &slopes);
                record_distribution(
                    &mut self.metrics,
                    "long_horizon_final_residual_bytes",
                    &residuals,
                );
                self.metrics.insert(
                    "long_horizon_bytes_per_turn".to_owned(),
                    median_f64_values(&slopes).unwrap_or(f64::INFINITY),
                );
                self.insert_checks(29, assertions);
            }
            Err(error) => self.insert_error(29, error.to_string()),
        }
    }

    fn require_phase_coverage(&self, phase: &str, required_ms: u64) -> Result<()> {
        let cadence = self.evidence.phases.cadence.as_ref().ok_or_else(|| {
            AhrbError::Validation("split sampler cadence evidence is absent".to_owned())
        })?;
        let coverage = self.evidence.series.phase_coverage(
            phase,
            cadence.counter_cadence_ns,
            required_ms.saturating_mul(1_000_000),
        )?;
        if !coverage.trustworthy {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} coverage is untrustworthy: observed={} ns required={} ns maximum-gap={} ns cadence-gaps={}",
                coverage.observed_duration_ns,
                coverage.required_duration_ns,
                coverage.maximum_gap_ns,
                coverage.cadence_gaps
            )));
        }
        let membership =
            membership_phase_coverage(cadence, phase, required_ms.saturating_mul(1_000_000))?;
        if !membership.trustworthy {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} membership coverage is untrustworthy: observed={} ns required={} ns maximum-gap={} ns cadence-gaps={}",
                membership.observed_duration_ns,
                membership.required_duration_ns,
                membership.maximum_gap_ns,
                membership.cadence_gaps
            )));
        }
        Ok(())
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

    fn record_sweep_point_metrics(
        &mut self,
        point: SweepPoint,
        derived: crate::sampler::PointMetrics,
    ) {
        let prefix = format!("parallel_n{}", point.agents);
        for (suffix, value) in [
            ("baseline_bytes", point.baseline_bytes as f64),
            ("steady_bytes", point.steady_bytes as f64),
            ("workload_peak_bytes", point.workload_peak_bytes as f64),
            ("cold_peak_bytes", point.cold_peak_bytes as f64),
            ("post_turn_bytes", point.post_turn_bytes as f64),
            ("post_close_bytes", point.post_close_bytes as f64),
            (
                "average_added_bytes_per_agent",
                derived.average_added_bytes_per_agent,
            ),
            (
                "post_turn_retained_bytes",
                derived.post_turn_retained_bytes as f64,
            ),
            (
                "post_close_residual_bytes",
                derived.post_close_residual_bytes as f64,
            ),
            ("residual_bytes_per_agent", derived.residual_bytes_per_agent),
            ("reclaim_ratio", derived.reclaim_ratio),
        ] {
            self.metrics.insert(format!("{prefix}_{suffix}"), value);
        }
        if let Some(value) = derived.adjacent_marginal_bytes_per_agent {
            self.metrics
                .insert(format!("{prefix}_adjacent_marginal_bytes_per_agent"), value);
        }
        if let Some(value) = derived.peak_amplification {
            self.metrics
                .insert(format!("{prefix}_peak_amplification"), value);
        }
    }

    fn record_sweep_repetition_distributions(&mut self) -> Result<()> {
        let mut repetitions = Vec::new();
        for repetition in 0..self.timing.repetitions {
            let points: Vec<SweepPoint> = self
                .timing
                .sweep_widths
                .iter()
                .map(|agents| {
                    self.sweep_repetition_points
                        .get(&(*agents, repetition))
                        .copied()
                        .ok_or_else(|| {
                            AhrbError::Validation(format!(
                                "missing N={agents} repetition {repetition} distribution point"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            repetitions.push(SweepMetrics::calculate(&points)?);
        }

        let beta: Vec<f64> = repetitions
            .iter()
            .filter_map(|metrics| metrics.headline_beta_bytes_per_agent)
            .collect();
        let beta_mib: Vec<f64> = repetitions
            .iter()
            .filter_map(|metrics| metrics.headline_beta_mib_per_agent)
            .collect();
        let alpha: Vec<f64> = repetitions
            .iter()
            .filter_map(|metrics| metrics.scaling_exponent_alpha)
            .collect();
        if alpha.len() != repetitions.len() {
            return Err(AhrbError::Validation(
                "scaling alpha requires a strictly positive active delta at every width and repetition"
                    .to_owned(),
            ));
        }
        let cold_peaks: Vec<f64> = repetitions
            .iter()
            .map(|metrics| metrics.maximum_cold_peak_bytes as f64)
            .collect();
        let workload_peaks: Vec<f64> = repetitions
            .iter()
            .map(|metrics| metrics.maximum_workload_peak_bytes as f64)
            .collect();
        record_distribution(&mut self.metrics, "parallel_beta_bytes_per_agent", &beta);
        record_distribution(&mut self.metrics, "parallel_beta_mib_per_agent", &beta_mib);
        record_distribution(&mut self.metrics, "parallel_scaling_exponent", &alpha);
        record_distribution(&mut self.metrics, "maximum_cold_peak_bytes", &cold_peaks);
        record_distribution(
            &mut self.metrics,
            "maximum_workload_peak_bytes",
            &workload_peaks,
        );

        for &agents in &self.timing.sweep_widths {
            let mut values: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
            for metrics in &repetitions {
                let (point, derived) = metrics
                    .points
                    .iter()
                    .find(|(point, _)| point.agents == agents)
                    .ok_or_else(|| {
                        AhrbError::Validation(format!("N={agents} distribution is absent"))
                    })?;
                for (name, value) in [
                    ("baseline_bytes", point.baseline_bytes as f64),
                    ("steady_bytes", point.steady_bytes as f64),
                    ("workload_peak_bytes", point.workload_peak_bytes as f64),
                    ("cold_peak_bytes", point.cold_peak_bytes as f64),
                    ("post_turn_bytes", point.post_turn_bytes as f64),
                    ("post_close_bytes", point.post_close_bytes as f64),
                    (
                        "average_added_bytes_per_agent",
                        derived.average_added_bytes_per_agent,
                    ),
                    (
                        "peak_amplification",
                        derived.peak_amplification.unwrap_or(0.0),
                    ),
                    (
                        "post_turn_retained_bytes",
                        derived.post_turn_retained_bytes as f64,
                    ),
                    (
                        "post_close_residual_bytes",
                        derived.post_close_residual_bytes as f64,
                    ),
                    ("residual_bytes_per_agent", derived.residual_bytes_per_agent),
                    ("reclaim_ratio", derived.reclaim_ratio),
                ] {
                    values.entry(name).or_default().push(value);
                }
                if let Some(value) = derived.adjacent_marginal_bytes_per_agent {
                    values
                        .entry("adjacent_marginal_bytes_per_agent")
                        .or_default()
                        .push(value);
                }
            }
            for (name, distribution) in values {
                record_distribution(
                    &mut self.metrics,
                    &format!("parallel_n{agents}_{name}"),
                    &distribution,
                );
            }
        }
        Ok(())
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

fn membership_phase_coverage(
    cadence: &ResourceCadenceEvidence,
    phase: &str,
    required_duration_ns: u64,
) -> Result<PhaseCoverage> {
    if cadence.membership_cadence_ns == 0 {
        return Err(AhrbError::Validation(
            "membership cadence must be nonzero".to_owned(),
        ));
    }
    let times = cadence
        .membership_samples_by_phase
        .get(phase)
        .ok_or_else(|| {
            AhrbError::Validation(format!(
                "phase {phase:?} has no membership-refresh evidence"
            ))
        })?;
    if times.len() < 2 || times.windows(2).any(|pair| pair[1] <= pair[0]) {
        return Err(AhrbError::Validation(format!(
            "phase {phase:?} needs strictly increasing membership boundary samples"
        )));
    }
    let start_ns = times.first().copied().unwrap_or(0);
    let end_ns = times.last().copied().unwrap_or(start_ns);
    let observed_duration_ns = end_ns.saturating_sub(start_ns);
    let (maximum_gap_ns, cadence_gaps, cadence_trustworthy) =
        cadence_quality(times, cadence.membership_cadence_ns);
    let duration_covered =
        observed_duration_ns.saturating_add(cadence.membership_cadence_ns) >= required_duration_ns;
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

fn validate_membership_cadence(cadence: &ResourceCadenceEvidence) -> Result<()> {
    if cadence.membership_samples_by_phase.is_empty() {
        return Err(AhrbError::Validation(
            "membership-refresh evidence is absent".to_owned(),
        ));
    }
    if cadence.membership_refreshes_by_phase.is_empty() {
        return Err(AhrbError::Validation(
            "membership discovery-duration evidence is absent".to_owned(),
        ));
    }
    for (phase, times) in &cadence.membership_samples_by_phase {
        let refreshes = cadence
            .membership_refreshes_by_phase
            .get(phase)
            .ok_or_else(|| {
                AhrbError::Validation(format!(
                    "phase {phase:?} has no membership discovery-duration evidence"
                ))
            })?;
        let mut refresh_times = refreshes
            .iter()
            .map(|refresh| refresh.elapsed_ns)
            .collect::<Vec<_>>();
        refresh_times.sort_unstable();
        refresh_times.dedup();
        if refresh_times != *times {
            return Err(AhrbError::Validation(format!(
                "phase {phase:?} membership timestamps disagree with discovery evidence"
            )));
        }
        if let Some(overrun) = refreshes
            .iter()
            .find(|refresh| refresh.discovery_wall_ns > cadence.membership_cadence_ns)
        {
            return Err(AhrbError::Validation(format!(
                "sampler overload: membership phase {phase:?} lane {} discovery consumed {} wall ns at a {} ns cadence",
                overrun.lane, overrun.discovery_wall_ns, cadence.membership_cadence_ns
            )));
        }
        let required = times
            .last()
            .copied()
            .unwrap_or(0)
            .saturating_sub(times.first().copied().unwrap_or(0));
        let coverage = membership_phase_coverage(cadence, phase, required.max(1))?;
        if !coverage.trustworthy {
            return Err(AhrbError::Validation(format!(
                "sampler overload: membership phase {phase:?} has {} bounded-jitter gaps and maximum gap {} ns",
                coverage.cadence_gaps, coverage.maximum_gap_ns
            )));
        }
    }
    if cadence.membership_refreshes_by_phase.len() != cadence.membership_samples_by_phase.len() {
        return Err(AhrbError::Validation(
            "membership discovery evidence contains unknown phases".to_owned(),
        ));
    }
    Ok(())
}

fn complete_idle_repetitions(
    repetitions: &[IdlePhaseRepetition],
    required_count: u32,
    profile: ResourceProfile,
) -> Result<(Vec<IdlePhaseRepetition>, BTreeMap<u32, String>)> {
    let expected: BTreeSet<u32> = (0..required_count).collect();
    let mut by_repetition = BTreeMap::new();
    for repetition in repetitions {
        if by_repetition
            .insert(repetition.identity.repetition, repetition.clone())
            .is_some()
        {
            return Err(AhrbError::Validation(format!(
                "duplicate idle evidence for repetition {}",
                repetition.identity.repetition
            )));
        }
    }
    let observed: BTreeSet<u32> = by_repetition.keys().copied().collect();
    if observed != expected {
        return Err(AhrbError::Validation(format!(
            "idle fresh-profile repetitions incomplete: required={expected:?} observed={observed:?}"
        )));
    }
    let identities: Vec<&RepetitionIdentity> = by_repetition
        .values()
        .map(|repetition| &repetition.identity)
        .collect();
    let tokens = validate_component_identities("idle", &identities, profile, required_count, None)?;
    Ok((by_repetition.into_values().collect(), tokens))
}

fn validate_component_identities(
    component: &str,
    identities: &[&RepetitionIdentity],
    profile: ResourceProfile,
    required_count: u32,
    expected_tokens: Option<&BTreeMap<u32, String>>,
) -> Result<BTreeMap<u32, String>> {
    let expected_repetitions: BTreeSet<u32> = (0..required_count).collect();
    let mut tokens = BTreeMap::new();
    let mut unique_tokens = BTreeSet::new();
    for identity in identities {
        if identity.profile != profile {
            return Err(AhrbError::Validation(format!(
                "{component} repetition {} used profile {:?}, expected {profile:?}",
                identity.repetition, identity.profile
            )));
        }
        if identity.isolation_token.is_empty() {
            return Err(AhrbError::Validation(format!(
                "{component} repetition {} has an empty isolation token",
                identity.repetition
            )));
        }
        if tokens
            .insert(identity.repetition, identity.isolation_token.clone())
            .is_some()
        {
            return Err(AhrbError::Validation(format!(
                "{component} has duplicate repetition {}",
                identity.repetition
            )));
        }
        if !unique_tokens.insert(identity.isolation_token.clone()) {
            return Err(AhrbError::Validation(format!(
                "{component} reused isolation token {:?}",
                identity.isolation_token
            )));
        }
    }
    let observed: BTreeSet<u32> = tokens.keys().copied().collect();
    if observed != expected_repetitions {
        return Err(AhrbError::Validation(format!(
            "{component} repetitions incomplete: required={expected_repetitions:?} observed={observed:?}"
        )));
    }
    if let Some(expected) = expected_tokens {
        if &tokens != expected {
            return Err(AhrbError::Validation(format!(
                "{component} isolation tokens do not match the fresh-profile idle repetitions"
            )));
        }
    }
    Ok(tokens)
}

fn validate_warmups(
    observations: Option<&[WarmupObservation]>,
    timing: &ResourceTimingPlan,
    expected_tokens: &BTreeMap<u32, String>,
) -> Result<()> {
    let observations = observations
        .ok_or_else(|| AhrbError::Validation("unmeasured warm-up evidence is absent".to_owned()))?;
    let identities: Vec<&RepetitionIdentity> = observations
        .iter()
        .map(|observation| &observation.identity)
        .collect();
    validate_component_identities(
        "warm-up",
        &identities,
        timing.profile,
        timing.repetitions,
        Some(expected_tokens),
    )?;
    for observation in observations {
        if observation.completed_turns != timing.warmup_turns
            || !observation.terminalized
            || !observation.closed
        {
            return Err(AhrbError::Validation(format!(
                "warm-up repetition {} is incomplete: turns={} required={} terminalized={} closed={}",
                observation.identity.repetition,
                observation.completed_turns,
                timing.warmup_turns,
                observation.terminalized,
                observation.closed
            )));
        }
    }
    Ok(())
}

fn validate_sweep_structure(
    observations: &[SweepObservation],
    timing: &ResourceTimingPlan,
    expected_tokens: &BTreeMap<u32, String>,
) -> Result<()> {
    let mut identities = BTreeMap::<u32, RepetitionIdentity>::new();
    let mut seeds = BTreeSet::new();
    let mut orders = BTreeMap::<u32, BTreeMap<u32, u32>>::new();
    for observation in observations {
        let repetition = observation.identity.repetition;
        if let Some(existing) = identities.get(&repetition) {
            if existing != &observation.identity {
                return Err(AhrbError::Validation(format!(
                    "sweep repetition {repetition} has inconsistent profile isolation identity"
                )));
            }
        } else {
            identities.insert(repetition, observation.identity.clone());
        }
        seeds.insert(observation.width_rotation_seed);
        if orders
            .entry(repetition)
            .or_default()
            .insert(observation.width_order_index, observation.agents)
            .is_some()
        {
            return Err(AhrbError::Validation(format!(
                "sweep repetition {repetition} has duplicate width order index {}",
                observation.width_order_index
            )));
        }
    }
    if seeds.len() != 1 {
        return Err(AhrbError::Validation(format!(
            "sweep width rotation seed is not stable: {seeds:?}"
        )));
    }
    let identity_refs: Vec<&RepetitionIdentity> = identities.values().collect();
    validate_component_identities(
        "sweep",
        &identity_refs,
        timing.profile,
        timing.repetitions,
        Some(expected_tokens),
    )?;
    let seed = seeds.first().copied().unwrap_or(0);
    for repetition in 0..timing.repetitions {
        let mut expected = timing.sweep_widths.clone();
        if !expected.is_empty() {
            let offset = usize::try_from(seed)
                .unwrap_or(usize::MAX)
                .wrapping_add(repetition as usize)
                % expected.len();
            expected.rotate_left(offset);
        }
        let observed_order = orders.get(&repetition).ok_or_else(|| {
            AhrbError::Validation(format!(
                "sweep repetition {repetition} has no width-order evidence"
            ))
        })?;
        let expected_indexes: BTreeSet<u32> =
            (0..u32::try_from(expected.len()).unwrap_or(u32::MAX)).collect();
        let observed_indexes: BTreeSet<u32> = observed_order.keys().copied().collect();
        let observed: Vec<u32> = observed_order.values().copied().collect();
        if observed_indexes != expected_indexes || observed != expected {
            return Err(AhrbError::Validation(format!(
                "sweep repetition {repetition} width rotation mismatch: expected={expected:?} observed={observed:?} indexes={observed_indexes:?}"
            )));
        }
    }
    Ok(())
}

fn median_u64_values(values: &[u64]) -> Option<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        length if length % 2 == 1 => sorted.get(middle).copied(),
        _ => {
            let lower = sorted.get(middle.saturating_sub(1)).copied()?;
            let upper = sorted.get(middle).copied()?;
            Some(lower.saturating_add(upper.saturating_sub(lower) / 2))
        }
    }
}

fn median_f64_values(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        length if length % 2 == 1 => sorted.get(middle).copied(),
        _ => {
            let lower = sorted.get(middle.saturating_sub(1)).copied()?;
            let upper = sorted.get(middle).copied()?;
            Some(lower + (upper - lower) / 2.0)
        }
    }
}

fn record_distribution(metrics: &mut BTreeMap<String, f64>, prefix: &str, values: &[f64]) {
    let Some(median) = median_f64_values(values) else {
        return;
    };
    let deviations: Vec<f64> = values.iter().map(|value| (value - median).abs()).collect();
    let mad = median_f64_values(&deviations).unwrap_or(0.0);
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = 95_usize
        .saturating_mul(sorted.len())
        .saturating_add(99)
        .saturating_div(100)
        .saturating_sub(1);
    let p95 = sorted.get(rank).copied().unwrap_or(median);
    metrics.insert(format!("{prefix}_median"), median);
    metrics.insert(format!("{prefix}_mad"), mad);
    metrics.insert(format!("{prefix}_p95"), p95);
}

fn aggregate_sweep_points(points: impl Iterator<Item = SweepPoint>) -> Result<SweepPoint> {
    let points: Vec<SweepPoint> = points.collect();
    let agents = points
        .first()
        .map(|point| point.agents)
        .ok_or_else(|| AhrbError::Validation("cannot aggregate an empty sweep".to_owned()))?;
    if points.iter().any(|point| point.agents != agents) {
        return Err(AhrbError::Validation(
            "cannot aggregate sweep points with different widths".to_owned(),
        ));
    }
    let median = |select: fn(&SweepPoint) -> u64| {
        median_u64_values(&points.iter().map(select).collect::<Vec<_>>())
            .ok_or_else(|| AhrbError::Validation("sweep median is absent".to_owned()))
    };
    Ok(SweepPoint {
        agents,
        baseline_bytes: median(|point| point.baseline_bytes)?,
        steady_bytes: median(|point| point.steady_bytes)?,
        workload_peak_bytes: points
            .iter()
            .map(|point| point.workload_peak_bytes)
            .max()
            .unwrap_or(0),
        cold_peak_bytes: points
            .iter()
            .map(|point| point.cold_peak_bytes)
            .max()
            .unwrap_or(0),
        post_turn_bytes: median(|point| point.post_turn_bytes)?,
        post_close_bytes: median(|point| point.post_close_bytes)?,
    })
}

fn point_reclaim_ratio(point: SweepPoint) -> f64 {
    let active = point.steady_bytes.saturating_sub(point.baseline_bytes);
    if active == 0 {
        return 0.0;
    }
    ((point.steady_bytes as f64 - point.post_close_bytes as f64) / active as f64).clamp(0.0, 1.0)
}

fn derive_sweep_point(
    series: &SampleSeries,
    observation: &SweepObservation,
    metric: MemoryMetric,
    timing: &ResourceTimingPlan,
    cadence: &ResourceCadenceEvidence,
    maximum_spread: f64,
) -> Result<(SweepPoint, Vec<(String, Plateau)>)> {
    if observation.agents == 0 {
        return Err(AhrbError::Validation(
            "sweep observation has N=0".to_owned(),
        ));
    }
    if observation.barrier_checkpoint.is_empty() {
        return Err(AhrbError::Validation(format!(
            "N={} repetition {} has no named barrier checkpoint",
            observation.agents, observation.identity.repetition
        )));
    }
    if observation.expected_barrier_actors.len() != observation.agents as usize {
        return Err(AhrbError::Validation(format!(
            "N={} repetition {} expected actor set has {} unique actors",
            observation.agents,
            observation.identity.repetition,
            observation.expected_barrier_actors.len()
        )));
    }
    if observation.observed_barrier_actors != observation.expected_barrier_actors {
        return Err(AhrbError::Validation(format!(
            "N={} repetition {} barrier {:?} actor evidence is incomplete: expected={:?} observed={:?}",
            observation.agents,
            observation.identity.repetition,
            observation.barrier_checkpoint,
            observation.expected_barrier_actors,
            observation.observed_barrier_actors
        )));
    }
    for (phase, duration_ms) in [
        (&observation.baseline_phase, timing.idle_baseline_ms),
        (&observation.steady_phase, timing.barrier_hold_ms),
        (&observation.post_turn_phase, timing.barrier_steady_ms),
        (&observation.post_close_phase, timing.barrier_steady_ms),
    ] {
        let coverage = series.phase_coverage(
            phase,
            cadence.counter_cadence_ns,
            duration_ms.saturating_mul(1_000_000),
        )?;
        if !coverage.trustworthy {
            return Err(AhrbError::Validation(format!(
                "N={} repetition {} phase {phase:?} is truncated or gapped: observed={} ns required={} ns maximum-gap={} ns",
                observation.agents,
                observation.identity.repetition,
                coverage.observed_duration_ns,
                coverage.required_duration_ns,
                coverage.maximum_gap_ns
            )));
        }
        let membership =
            membership_phase_coverage(cadence, phase, duration_ms.saturating_mul(1_000_000))?;
        if !membership.trustworthy {
            return Err(AhrbError::Validation(format!(
                "N={} repetition {} membership phase {phase:?} is truncated or gapped",
                observation.agents, observation.identity.repetition
            )));
        }
    }
    for phase in [&observation.workload_phase, &observation.cold_phase] {
        let coverage = series.phase_coverage(
            phase,
            cadence.counter_cadence_ns,
            cadence.counter_cadence_ns,
        )?;
        if !coverage.trustworthy {
            return Err(AhrbError::Validation(format!(
                "N={} repetition {} boundary phase {phase:?} is gapped",
                observation.agents, observation.identity.repetition
            )));
        }
        let membership = membership_phase_coverage(cadence, phase, cadence.membership_cadence_ns)?;
        if !membership.trustworthy {
            return Err(AhrbError::Validation(format!(
                "N={} repetition {} boundary membership phase {phase:?} is gapped",
                observation.agents, observation.identity.repetition
            )));
        }
    }
    if timing
        .barrier_discard_ms
        .saturating_add(timing.barrier_steady_ms)
        > timing.barrier_hold_ms
    {
        return Err(AhrbError::Validation(
            "barrier discard plus steady window exceeds the hold duration".to_owned(),
        ));
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
        timing.barrier_steady_ms.saturating_mul(1_000_000),
    )?;
    let post_turn = series.trailing_plateau(
        &observation.post_turn_phase,
        metric,
        observation.minimum_post_turn_processes,
        timing.barrier_steady_ms.saturating_mul(1_000_000),
    )?;
    let post_close = series.trailing_plateau(
        &observation.post_close_phase,
        metric,
        observation.minimum_post_close_processes,
        timing.barrier_steady_ms.saturating_mul(1_000_000),
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
    let workload_peak = phase_peak(series, &observation.workload_phase, metric)?;
    let point = SweepPoint {
        agents: observation.agents,
        baseline_bytes: baseline.median_bytes,
        steady_bytes: steady.median_bytes,
        // The steady barrier is part of the active workload lifecycle. Sampling jitter
        // can observe a small allocation after the last workload-boundary sample, so
        // the lifecycle peak must never exclude the later steady plateau.
        workload_peak_bytes: workload_peak.max(steady.median_bytes),
        cold_peak_bytes: phase_peak(series, &observation.cold_phase, metric)?,
        post_turn_bytes: post_turn.median_bytes,
        post_close_bytes: post_close.median_bytes,
    };
    Ok((
        point,
        vec![
            (
                format!(
                    "rep{}-n{}-baseline",
                    observation.identity.repetition, observation.agents
                ),
                baseline,
            ),
            (
                format!(
                    "rep{}-n{}-steady",
                    observation.identity.repetition, observation.agents
                ),
                steady,
            ),
            (
                format!(
                    "rep{}-n{}-post-turn",
                    observation.identity.repetition, observation.agents
                ),
                post_turn,
            ),
            (
                format!(
                    "rep{}-n{}-post-close",
                    observation.identity.repetition, observation.agents
                ),
                post_close,
            ),
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
        if marginals[index] > median * 2.0 {
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
    if values.len() < 2 || values.windows(2).any(|pair| pair[0] > pair[1]) {
        return false;
    }
    let intervals = values.len() - 1;
    let increases = values.windows(2).filter(|pair| pair[0] < pair[1]).count();
    // A lazy runtime helper or bounded pool can add one resource and then plateau.
    // Treat growth as a leak only when accumulation is sustained through at least
    // half of the sampled intervals; regular staircase and per-turn leaks still fail.
    increases.saturating_mul(2) >= intervals
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
            open_fds: None,
            thread_count: Some(2),
            collection_ns: 1_000,
            collection_wall_ns: 1_000,
            processes,
            process_samples: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_phase(
        series: &mut SampleSeries,
        clock: &mut u64,
        phase: &str,
        bytes: u64,
        cpu_fraction_percent: u64,
        process_count: usize,
        duration_ms: u64,
        cadence_ns: u64,
    ) -> Result<()> {
        let duration_ns = duration_ms.saturating_mul(1_000_000);
        let steps = duration_ns.saturating_add(cadence_ns.saturating_sub(1)) / cadence_ns;
        let initial_cpu = *clock / 10;
        for offset in 0..=steps {
            let phase_elapsed = offset.saturating_mul(cadence_ns).min(duration_ns);
            let elapsed = clock.saturating_add(phase_elapsed);
            let cpu = initial_cpu
                .saturating_add(phase_elapsed.saturating_mul(cpu_fraction_percent) / 100);
            series.push(sample(elapsed, phase, bytes, cpu, process_count))?;
        }
        *clock = clock.saturating_add(duration_ns).saturating_add(cadence_ns);
        Ok(())
    }

    fn repetition_identity(profile: ResourceProfile, repetition: u32) -> RepetitionIdentity {
        RepetitionIdentity {
            repetition,
            profile,
            isolation_token: format!("fresh-{profile:?}-{repetition}"),
        }
    }

    fn membership_samples(series: &SampleSeries, cadence_ns: u64) -> BTreeMap<String, Vec<u64>> {
        let mut bounds = BTreeMap::<String, (u64, u64)>::new();
        for sample in &series.samples {
            bounds
                .entry(sample.phase.clone())
                .and_modify(|(_, end)| *end = sample.elapsed_ns)
                .or_insert((sample.elapsed_ns, sample.elapsed_ns));
        }
        bounds
            .into_iter()
            .map(|(phase, (start, end))| {
                let mut times = Vec::new();
                let mut current = start;
                while current < end {
                    times.push(current);
                    current = current.saturating_add(cadence_ns).min(end);
                }
                times.push(end);
                (phase, times)
            })
            .collect()
    }

    fn passing_evidence(profile: ResourceProfile) -> Result<ResourceEvidence> {
        let timing = ResourceTimingPlan::for_profile(profile);
        let cadence_ns = 20_000_000;
        let mut series = SampleSeries::default();
        let mut clock = 0_u64;
        let mut idle_repetitions = Vec::new();
        for repetition in 0..timing.repetitions {
            let warm_idle = format!("rep{repetition}-warm-idle");
            let idle_cpu = format!("rep{repetition}-idle-cpu");
            let idle_drift = format!("rep{repetition}-idle-drift");
            add_phase(
                &mut series,
                &mut clock,
                &warm_idle,
                100 * MIB,
                0,
                1,
                timing.idle_baseline_ms,
                cadence_ns,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &idle_cpu,
                100 * MIB,
                0,
                1,
                timing.idle_cpu_ms,
                cadence_ns,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &idle_drift,
                100 * MIB,
                0,
                1,
                timing.idle_drift_ms,
                cadence_ns,
            )?;
            idle_repetitions.push(IdlePhaseRepetition {
                identity: repetition_identity(profile, repetition),
                warm_idle,
                idle_cpu,
                idle_drift,
            });
        }
        let mut ordinary_returns = Vec::new();
        let mut cold_starts = Vec::new();
        let mut single_agents = Vec::new();
        let mut cleanups = Vec::new();
        let mut long_horizons = Vec::new();
        for repetition in 0..timing.repetitions {
            let identity = repetition_identity(profile, repetition);
            let ordinary_base = format!("rep{repetition}-ordinary-base");
            let ordinary_active = format!("rep{repetition}-ordinary-active");
            let ordinary_return = format!("rep{repetition}-ordinary-return");
            let cold_start = format!("rep{repetition}-cold-start");
            let ready_idle = format!("rep{repetition}-ready-idle");
            let single_turn = format!("rep{repetition}-single-turn");
            let single_barrier = format!("rep{repetition}-single-barrier");
            for (phase, bytes, cpu, duration) in [
                (&ordinary_base, 100 * MIB, 0, timing.idle_baseline_ms),
                (&ordinary_active, 120 * MIB, 1, 20),
                (&ordinary_return, 100 * MIB, 0, timing.barrier_steady_ms),
                (&cold_start, 130 * MIB, 2, 60),
                (&ready_idle, 100 * MIB, 0, timing.idle_baseline_ms),
                (&single_turn, 120 * MIB, 1, 20),
                (&single_barrier, 120 * MIB, 1, timing.barrier_hold_ms),
            ] {
                add_phase(
                    &mut series,
                    &mut clock,
                    phase,
                    bytes,
                    cpu,
                    1,
                    duration,
                    cadence_ns,
                )?;
            }
            let turn_cpu_start = series
                .samples
                .iter()
                .find(|sample| sample.phase == single_turn)
                .map_or(0, |sample| sample.cpu_ns);
            series.push(sample(
                clock,
                &single_turn,
                120 * MIB,
                turn_cpu_start.saturating_add(1_000_000),
                1,
            ))?;
            clock = clock.saturating_add(cadence_ns);
            ordinary_returns.push(ReturnToIdleObservation {
                identity: identity.clone(),
                baseline_phase: ordinary_base,
                active_phase: ordinary_active,
                returned_phase: ordinary_return,
                settled_after_ms: 500,
                remaining_workers: 0,
                minimum_baseline_processes: 1,
                minimum_returned_processes: 1,
            });
            cold_starts.push(ColdStartObservation {
                identity: identity.clone(),
                cold_phase: cold_start,
                ready_idle_phase: ready_idle,
                sampling_started_after_launch_ms: 5,
                readiness_ms: 50,
                startup_bound_ms: 10_000,
                minimum_idle_processes: 1,
            });
            single_agents.push(SingleAgentObservation {
                identity: identity.clone(),
                turn_phase: single_turn,
                scripted_turns: 1,
                barrier_phase: single_barrier,
            });
            cleanups.push(CleanupObservation {
                identity: identity.clone(),
                reclaim_after_ms: 500,
                remaining_workers: 0,
                expected_actor_sessions: timing
                    .sweep_widths
                    .last()
                    .copied()
                    .into_iter()
                    .flat_map(|agents| 0..agents)
                    .map(|actor| (format!("actor-{actor}"), format!("session-{actor}")))
                    .collect(),
                closed_actor_sessions: timing
                    .sweep_widths
                    .last()
                    .copied()
                    .into_iter()
                    .flat_map(|agents| 0..agents)
                    .map(|actor| (format!("actor-{actor}"), format!("session-{actor}")))
                    .collect(),
                baseline_processes: BTreeSet::from([ProcIdentity {
                    pid: 1,
                    start_time: 1,
                }]),
                post_close_processes: BTreeSet::from([ProcIdentity {
                    pid: 1,
                    start_time: 1,
                }]),
                baseline_threads: Some(2),
                post_close_threads: Some(2),
            });
            let long_baseline = format!("rep{repetition}-long-baseline");
            let long_final = format!("rep{repetition}-long-final");
            add_phase(
                &mut series,
                &mut clock,
                &long_baseline,
                100 * MIB,
                0,
                1,
                timing.idle_baseline_ms,
                cadence_ns,
            )?;
            add_phase(
                &mut series,
                &mut clock,
                &long_final,
                100 * MIB,
                0,
                1,
                timing
                    .barrier_discard_ms
                    .saturating_add(timing.barrier_steady_ms),
                cadence_ns,
            )?;
            let tool_results_by_turn: BTreeMap<u32, Vec<LongHorizonToolResult>> = (1..=timing
                .long_horizon_turns)
                .map(|turn| {
                    let results = if turn % 10 == 0 {
                        vec![LongHorizonToolResult {
                            event_id: format!("fixture-r{repetition}-t{turn}"),
                            cursor: u64::from(turn),
                            call_id: format!("resource-long-r{repetition}-t{turn}"),
                            name: "write_fixture".to_owned(),
                        }]
                    } else {
                        Vec::new()
                    };
                    (turn, results)
                })
                .collect();
            long_horizons.push(LongHorizonObservation {
                identity,
                baseline_phase: long_baseline,
                final_post_close_phase: long_final,
                points: (0..=timing.long_horizon_turns)
                    .step_by(timing.long_horizon_sample_turns as usize)
                    .map(|turn| LongHorizonPoint {
                        turn,
                        memory_bytes: 100 * MIB,
                        open_fds: 10,
                        threads: 2,
                    })
                    .collect(),
                completed_turns: timing.long_horizon_turns,
                tool_results_by_turn,
                expected_session_id: format!("long-session-{repetition}"),
                closed_session_id: Some(format!("long-session-{repetition}")),
                baseline_bytes: 100 * MIB,
                final_post_close_bytes: 100 * MIB,
                baseline_open_fds: 10,
                final_post_close_open_fds: 10,
                baseline_threads: 2,
                final_post_close_threads: 2,
                baseline_processes: BTreeSet::from([ProcIdentity {
                    pid: 1,
                    start_time: 1,
                }]),
                final_post_close_processes: BTreeSet::from([ProcIdentity {
                    pid: 1,
                    start_time: 1,
                }]),
            });
        }

        let mut sweep = Vec::new();
        let width_rotation_seed = 17_u64;
        for repetition in 0..timing.repetitions {
            let mut rotated_widths = timing.sweep_widths.clone();
            let rotation = (usize::try_from(width_rotation_seed).unwrap_or(usize::MAX)
                + repetition as usize)
                % rotated_widths.len();
            rotated_widths.rotate_left(rotation);
            for (width_order_index, agents) in rotated_widths.into_iter().enumerate() {
                let baseline = format!("rep{repetition}-n{agents}-base");
                let workload = format!("rep{repetition}-n{agents}-workload");
                let cold = format!("rep{repetition}-n{agents}-cold");
                let steady = format!("rep{repetition}-n{agents}-steady");
                let post_turn = format!("rep{repetition}-n{agents}-post-turn");
                let post_close = format!("rep{repetition}-n{agents}-post-close");
                let active = (100 + u64::from(agents) * 8) * MIB;
                add_phase(
                    &mut series,
                    &mut clock,
                    &baseline,
                    100 * MIB,
                    0,
                    1,
                    timing.idle_baseline_ms,
                    cadence_ns,
                )?;
                add_phase(
                    &mut series,
                    &mut clock,
                    &workload,
                    active + MIB,
                    2,
                    1,
                    20,
                    cadence_ns,
                )?;
                add_phase(
                    &mut series,
                    &mut clock,
                    &cold,
                    active + 2 * MIB,
                    2,
                    1,
                    20,
                    cadence_ns,
                )?;
                add_phase(
                    &mut series,
                    &mut clock,
                    &steady,
                    active,
                    1,
                    1,
                    timing.barrier_hold_ms,
                    cadence_ns,
                )?;
                add_phase(
                    &mut series,
                    &mut clock,
                    &post_turn,
                    active,
                    1,
                    1,
                    timing.barrier_steady_ms,
                    cadence_ns,
                )?;
                add_phase(
                    &mut series,
                    &mut clock,
                    &post_close,
                    100 * MIB,
                    0,
                    1,
                    timing.barrier_steady_ms,
                    cadence_ns,
                )?;
                let actors: BTreeSet<String> =
                    (0..agents).map(|actor| format!("actor-{actor}")).collect();
                sweep.push(SweepObservation {
                    identity: repetition_identity(profile, repetition),
                    agents,
                    expected_barrier_actors: actors.clone(),
                    observed_barrier_actors: actors,
                    barrier_checkpoint: "tool-complete-hold".to_owned(),
                    baseline_phase: baseline,
                    workload_phase: workload,
                    cold_phase: cold,
                    steady_phase: steady,
                    post_turn_phase: post_turn,
                    post_close_phase: post_close,
                    minimum_steady_processes: 1,
                    minimum_baseline_processes: 1,
                    minimum_post_turn_processes: 1,
                    minimum_post_close_processes: 1,
                    post_close_settled_after_ms: 500,
                    width_rotation_seed,
                    width_order_index: u32::try_from(width_order_index).unwrap_or(u32::MAX),
                });
            }
        }
        let membership_cadence_ns = 10_000_000;
        let membership_samples_by_phase = membership_samples(&series, membership_cadence_ns);
        let membership_refreshes_by_phase = membership_samples_by_phase
            .iter()
            .map(|(phase, times)| {
                (
                    phase.clone(),
                    times
                        .iter()
                        .map(|elapsed_ns| MembershipRefreshEvidence {
                            elapsed_ns: *elapsed_ns,
                            discovery_wall_ns: 1_000,
                            discovery_cpu_ns: 1_000,
                            lane: 0,
                        })
                        .collect(),
                )
            })
            .collect();
        let cadence = ResourceCadenceEvidence {
            membership_cadence_ns,
            counter_cadence_ns: cadence_ns,
            counter_kind: ResourceCounterKind::MacOsRusage,
            membership_samples_by_phase,
            membership_refreshes_by_phase,
        };
        Ok(ResourceEvidence {
            completed_repetitions: timing.repetitions,
            series,
            phases: ResourcePhases {
                repetitions: idle_repetitions,
                cadence: Some(cadence),
                ..ResourcePhases::default()
            },
            memory_metric: Some(MemoryMetric::Effective),
            sampler_cadence_ns: Some(cadence_ns),
            idle: Some(IdleObservation {
                declared_model: IdleProcessModel::PersistentTree,
                busy_polling_detected: Some(false),
                initial_workers: 1,
                final_workers: 1,
                initial_threads: Some(2),
                final_threads: Some(2),
            }),
            warmup: Some(
                (0..timing.repetitions)
                    .map(|repetition| WarmupObservation {
                        identity: repetition_identity(profile, repetition),
                        completed_turns: timing.warmup_turns,
                        terminalized: true,
                        closed: true,
                    })
                    .collect(),
            ),
            sweep,
            ordinary_return: Some(ordinary_returns),
            cold_start: Some(cold_starts),
            single_agent: Some(single_agents),
            cleanup: Some(cleanups),
            long_horizon: Some(long_horizons),
        })
    }

    fn assert_row_28_fails(evidence: &ResourceEvidence, expected: &str) -> Result<()> {
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|row| row.row == 28)
            .ok_or_else(|| AhrbError::Validation("row 28 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains(expected)),
            "unexpected row-28 outcome: {:?}",
            row.outcome
        );
        Ok(())
    }

    fn assert_row_29_fails(evidence: &ResourceEvidence, expected: &str) -> Result<()> {
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|row| row.row == 29)
            .ok_or_else(|| AhrbError::Validation("row 29 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains(expected)),
            "unexpected row-29 outcome: {:?}",
            row.outcome
        );
        Ok(())
    }

    #[test]
    fn timing_plans_encode_normative_cert_and_quick_windows() {
        let quick = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let cert = ResourceTimingPlan::for_profile(ResourceProfile::Cert);
        assert_eq!(quick.repetitions, 3);
        assert_eq!(cert.repetitions, 7);
        assert_eq!(quick.sweep_widths, vec![1, 2, 4]);
        assert_eq!(cert.barrier_hold_ms, 3_000);
        assert_eq!(cert.barrier_discard_ms, 1_000);
        assert_eq!(cert.barrier_steady_ms, 2_000);
        assert_eq!(quick.idle_cpu_ms, 1_000);
        assert_eq!(quick.idle_drift_ms, 4_000);
        assert_eq!(quick.barrier_hold_ms, 500);
        assert_eq!(cert.idle_drift_ms, 120_000);
        assert_eq!(cert.long_horizon_turns, 1_000);
    }

    #[test]
    fn missing_official_session_close_cannot_pass_row_28() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let cleanup = evidence
            .cleanup
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| AhrbError::Validation("cleanup observation is absent".to_owned()))?;
        let actor = cleanup
            .closed_actor_sessions
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| AhrbError::Validation("closed session set is empty".to_owned()))?;
        cleanup.closed_actor_sessions.remove(&actor);
        assert_row_28_fails(&evidence, "official-close-set")
    }

    #[test]
    fn equal_count_process_replacement_cannot_pass_row_28() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let cleanup = evidence
            .cleanup
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| AhrbError::Validation("cleanup observation is absent".to_owned()))?;
        cleanup.post_close_processes = BTreeSet::from([ProcIdentity {
            pid: 9_999,
            start_time: 2,
        }]);
        assert_row_28_fails(&evidence, "process-identity-reclaim")
    }

    #[test]
    fn leaked_thread_cannot_pass_row_28() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let cleanup = evidence
            .cleanup
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| AhrbError::Validation("cleanup observation is absent".to_owned()))?;
        cleanup.post_close_threads = cleanup.baseline_threads.map(|threads| threads + 1);
        assert_row_28_fails(&evidence, "thread-reclaim")
    }

    #[test]
    fn reclaim_below_eighty_percent_cannot_pass_row_28() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let target_agents = ResourceTimingPlan::for_profile(ResourceProfile::Quick)
            .sweep_widths
            .last()
            .copied()
            .unwrap_or(0);
        let phase = evidence
            .sweep
            .iter()
            .find(|observation| {
                observation.identity.repetition == 0 && observation.agents == target_agents
            })
            .map(|observation| observation.post_close_phase.clone())
            .ok_or_else(|| AhrbError::Validation("Nmax post-close phase is absent".to_owned()))?;
        for sample in evidence
            .series
            .samples
            .iter_mut()
            .filter(|sample| sample.phase == phase)
        {
            let retained = 120 * MIB;
            sample.rss_bytes = retained;
            sample.pss_bytes = Some(retained);
            sample.private_bytes = Some(retained);
        }
        assert_row_28_fails(&evidence, "reclaim-ratio")
    }

    #[test]
    fn excessive_post_close_residual_cannot_pass_row_28() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let target_agents = ResourceTimingPlan::for_profile(ResourceProfile::Quick)
            .sweep_widths
            .last()
            .copied()
            .unwrap_or(0);
        let target_phases: Vec<(String, String)> = evidence
            .sweep
            .iter()
            .filter(|observation| observation.agents == target_agents)
            .map(|observation| {
                (
                    observation.post_turn_phase.clone(),
                    observation.post_close_phase.clone(),
                )
            })
            .collect();
        if target_phases.is_empty() {
            return Err(AhrbError::Validation(
                "Nmax post-close phases are absent".to_owned(),
            ));
        }
        for (post_turn, post_close) in target_phases {
            for sample in &mut evidence.series.samples {
                let bytes = if sample.phase == post_turn {
                    Some(500 * MIB)
                } else if sample.phase == post_close {
                    Some(165 * MIB)
                } else {
                    None
                };
                if let Some(bytes) = bytes {
                    sample.rss_bytes = bytes;
                    sample.pss_bytes = Some(bytes);
                    sample.private_bytes = Some(bytes);
                }
            }
        }
        assert_row_28_fails(&evidence, "post-close-residual")
    }

    #[test]
    fn missing_fixture_tool_turn_cannot_pass_row_29() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let observation = evidence
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation
            .tool_results_by_turn
            .get_mut(&10)
            .ok_or_else(|| AhrbError::Validation("turn 10 tool record is absent".to_owned()))?
            .clear();
        assert_row_29_fails(&evidence, "fixture-tool-cadence")
    }

    #[test]
    fn duplicate_or_wrong_fixture_result_cannot_pass_row_29() -> Result<()> {
        let mut duplicate = passing_evidence(ResourceProfile::Quick)?;
        let observation = duplicate
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        let results = observation
            .tool_results_by_turn
            .get_mut(&10)
            .ok_or_else(|| AhrbError::Validation("turn 10 tool record is absent".to_owned()))?;
        results.push(LongHorizonToolResult {
            event_id: "duplicate-result".to_owned(),
            cursor: 9_999,
            call_id: "resource-long-r0-t10".to_owned(),
            name: "write_fixture".to_owned(),
        });
        assert_row_29_fails(&duplicate, "fixture-tool-cadence")?;

        let mut wrong = passing_evidence(ResourceProfile::Quick)?;
        let observation = wrong
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        let result = observation
            .tool_results_by_turn
            .get_mut(&10)
            .and_then(|results| results.first_mut())
            .ok_or_else(|| AhrbError::Validation("turn 10 fixture result is absent".to_owned()))?;
        result.call_id = "matching-but-wrong".to_owned();
        result.name = "read_fixture".to_owned();
        assert_row_29_fails(&wrong, "fixture-tool-cadence")
    }

    #[test]
    fn missing_per_turn_tool_record_cannot_pass_row_29() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let observation = evidence
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.tool_results_by_turn.remove(&11);
        assert_row_29_fails(&evidence, "tool-result-turn-coverage")
    }

    #[test]
    fn missing_session_close_or_initial_checkpoint_cannot_pass_row_29() -> Result<()> {
        let mut missing_close = passing_evidence(ResourceProfile::Quick)?;
        let observation = missing_close
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.closed_session_id = None;
        assert_row_29_fails(&missing_close, "official-session-close")?;

        let mut missing_initial = passing_evidence(ResourceProfile::Quick)?;
        let observation = missing_initial
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        if !observation.points.is_empty() {
            observation.points.remove(0);
        }
        assert_row_29_fails(&missing_initial, "checkpoint-count")
    }

    #[test]
    fn final_identity_fd_or_thread_leak_cannot_pass_row_29() -> Result<()> {
        let mut replaced = passing_evidence(ResourceProfile::Quick)?;
        let observation = replaced
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.final_post_close_processes = BTreeSet::from([ProcIdentity {
            pid: 9_999,
            start_time: 2,
        }]);
        assert_row_29_fails(&replaced, "process-identity-reclaim")?;

        let mut fd_leak = passing_evidence(ResourceProfile::Quick)?;
        let observation = fd_leak
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.final_post_close_open_fds = observation.baseline_open_fds.saturating_add(1);
        assert_row_29_fails(&fd_leak, "final-fd-reclaim")?;

        let mut thread_leak = passing_evidence(ResourceProfile::Quick)?;
        let observation = thread_leak
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.final_post_close_threads = observation.baseline_threads.saturating_add(1);
        assert_row_29_fails(&thread_leak, "final-thread-reclaim")
    }

    #[test]
    fn excessive_memory_slope_or_final_residual_cannot_pass_row_29() -> Result<()> {
        let mut slope = passing_evidence(ResourceProfile::Quick)?;
        let observation = slope
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        for point in &mut observation.points {
            point.memory_bytes = point
                .memory_bytes
                .saturating_add(u64::from(point.turn).saturating_mul(65_537));
        }
        assert_row_29_fails(&slope, "memory-per-turn")?;

        let mut residual = passing_evidence(ResourceProfile::Quick)?;
        let observation = residual
            .long_horizon
            .as_mut()
            .and_then(|observations| observations.first_mut())
            .ok_or_else(|| {
                AhrbError::Validation("long-horizon observation is absent".to_owned())
            })?;
        observation.final_post_close_bytes = observation.baseline_bytes.saturating_add(65 * MIB);
        let final_phase = observation.final_post_close_phase.clone();
        for sample in residual
            .series
            .samples
            .iter_mut()
            .filter(|sample| sample.phase == final_phase)
        {
            sample.rss_bytes = observation.final_post_close_bytes;
            sample.pss_bytes = Some(observation.final_post_close_bytes);
            sample.private_bytes = Some(observation.final_post_close_bytes);
        }
        assert_row_29_fails(&residual, "final-residual")
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
        assert_eq!(
            certification.metrics["parallel_n4_baseline_bytes"],
            (100 * MIB) as f64
        );
        assert_eq!(
            certification.metrics["parallel_n4_steady_bytes"],
            (132 * MIB) as f64
        );
        assert_eq!(
            certification.metrics["parallel_n4_adjacent_marginal_bytes_per_agent"],
            (8 * MIB) as f64
        );
        assert_eq!(certification.metrics["parallel_n8_reclaim_ratio"], 1.0);
        assert_eq!(certification.metrics["parallel_beta_mib_per_agent"], 8.0);
        assert_eq!(
            certification.metrics["maximum_workload_peak_bytes"],
            (165 * MIB) as f64
        );
        assert_eq!(
            certification.metrics["single_agent_barrier_idle_cpu_one_core"],
            0.01
        );
        for key in [
            "idle_baseline_bytes_median",
            "idle_baseline_bytes_mad",
            "idle_baseline_bytes_p95",
            "idle_cpu_one_core_p95",
            "idle_drift_bytes_per_minute_mad",
            "idle_net_growth_bytes_p95",
            "parallel_beta_bytes_per_agent_median",
            "parallel_beta_bytes_per_agent_mad",
            "parallel_beta_bytes_per_agent_p95",
            "parallel_scaling_exponent_p95",
            "maximum_workload_peak_bytes_p95",
            "parallel_n4_baseline_bytes_mad",
            "parallel_n4_steady_bytes_p95",
            "parallel_n4_adjacent_marginal_bytes_per_agent_p95",
            "parallel_n8_reclaim_ratio_median",
            "parallel_n8_post_close_residual_bytes_p95",
            "ordinary_residual_bytes_median",
            "ordinary_residual_bytes_mad",
            "ordinary_residual_bytes_p95",
            "cold_readiness_ms_median",
            "single_agent_cpu_ns_per_turn_p95",
            "cleanup_reclaim_after_ms_median",
            "long_horizon_bytes_per_turn_mad",
        ] {
            assert!(certification.metrics.contains_key(key), "missing {key}");
        }
        Ok(())
    }

    #[test]
    fn shared_daemon_proves_n8_with_logical_barrier_actors() -> Result<()> {
        let evidence = passing_evidence(ResourceProfile::Cert)?;
        assert!(evidence.sweep.iter().all(|point| {
            point.minimum_steady_processes == 1
                && point.expected_barrier_actors == point.observed_barrier_actors
        }));
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row26 = certification.rows.iter().find(|row| row.row == 26);
        assert!(row26.is_some_and(|row| matches!(row.outcome, TestOutcome::Pass)));
        Ok(())
    }

    #[test]
    fn incomplete_barrier_actor_or_repetition_evidence_cannot_certify() -> Result<()> {
        let mut missing_actor = passing_evidence(ResourceProfile::Cert)?;
        if let Some(observation) = missing_actor
            .sweep
            .iter_mut()
            .find(|point| point.agents == 8 && point.identity.repetition == 0)
        {
            observation.observed_barrier_actors.pop_first();
        }
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &missing_actor,
            &ResourceEnvelope::default(),
        );
        let row26 = certification.rows.iter().find(|row| row.row == 26);
        assert!(row26.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));

        let mut missing_repetition = passing_evidence(ResourceProfile::Cert)?;
        missing_repetition
            .sweep
            .retain(|point| !(point.agents == 8 && point.identity.repetition == 6));
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &missing_repetition,
            &ResourceEnvelope::default(),
        );
        let row26 = certification.rows.iter().find(|row| row.row == 26);
        assert!(row26.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));
        Ok(())
    }

    #[test]
    fn omitted_or_unclosed_warmup_cannot_certify_row_26() -> Result<()> {
        let mut omitted = passing_evidence(ResourceProfile::Quick)?;
        omitted.warmup = None;
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &omitted,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|row| row.row == 26)
            .ok_or_else(|| AhrbError::Validation("row 26 is absent".to_owned()))?;
        assert!(matches!(&row.outcome, TestOutcome::Error(message) if message.contains("warm-up")));

        let mut unclosed = passing_evidence(ResourceProfile::Quick)?;
        if let Some(observation) = unclosed
            .warmup
            .as_mut()
            .and_then(|observations| observations.get_mut(1))
        {
            observation.closed = false;
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &unclosed,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|row| row.row == 26)
            .ok_or_else(|| AhrbError::Validation("row 26 is absent".to_owned()))?;
        assert!(matches!(&row.outcome, TestOutcome::Error(message) if message.contains("warm-up")));
        Ok(())
    }

    #[test]
    fn per_component_identity_and_seeded_rotation_are_mandatory() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        if let Some(observations) = evidence.ordinary_return.as_mut() {
            observations.pop();
        }
        if let Some(observations) = evidence.cold_start.as_mut() {
            observations[0].identity.profile = ResourceProfile::Cert;
        }
        if let Some(observations) = evidence.single_agent.as_mut() {
            let reused = observations[0].identity.isolation_token.clone();
            observations[1].identity.isolation_token = reused;
        }
        if let Some(observations) = evidence.cleanup.as_mut() {
            observations[1].identity.repetition = 0;
        }
        if let Some(observations) = evidence.long_horizon.as_mut() {
            observations.pop();
        }
        if let Some(observation) = evidence.sweep.first_mut() {
            observation.width_order_index = u32::MAX;
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        for row in [23_u8, 24, 25, 26, 28, 29] {
            assert!(
                certification
                    .rows
                    .iter()
                    .find(|result| result.row == row)
                    .is_some_and(|result| !matches!(result.outcome, TestOutcome::Pass))
            );
        }
        Ok(())
    }

    #[test]
    fn membership_and_counter_cadence_are_independent_gates() -> Result<()> {
        let mut membership_gap = passing_evidence(ResourceProfile::Quick)?;
        if let Some(cadence) = membership_gap.phases.cadence.as_mut() {
            let retained = if let Some(times) = cadence
                .membership_samples_by_phase
                .get_mut("rep0-warm-idle")
            {
                if times.len() > 4 {
                    times.remove(1);
                    times.remove(1);
                }
                Some(times.iter().copied().collect::<BTreeSet<_>>())
            } else {
                None
            };
            if let (Some(retained), Some(refreshes)) = (
                retained,
                cadence
                    .membership_refreshes_by_phase
                    .get_mut("rep0-warm-idle"),
            ) {
                refreshes.retain(|refresh| retained.contains(&refresh.elapsed_ns));
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &membership_gap,
            &ResourceEnvelope::default(),
        );
        assert!(certification.rows.iter().all(|row| {
            matches!(&row.outcome, TestOutcome::Error(message) if message.contains("membership"))
        }));

        let mut wrong_counter = passing_evidence(ResourceProfile::Quick)?;
        if let Some(cadence) = wrong_counter.phases.cadence.as_mut() {
            cadence.counter_cadence_ns = 10_000_000;
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &wrong_counter,
            &ResourceEnvelope::default(),
        );
        assert!(certification.rows.iter().all(|row| {
            matches!(&row.outcome, TestOutcome::Error(message) if message.contains("cadence mismatch"))
        }));
        Ok(())
    }

    #[test]
    fn bounded_membership_scheduler_jitter_is_accepted() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        if let Some(cadence) = evidence.phases.cadence.as_mut() {
            let phase = "rep0-warm-idle";
            let times = cadence
                .membership_samples_by_phase
                .get_mut(phase)
                .ok_or_else(|| AhrbError::Validation("fixture phase absent".to_owned()))?;
            if times.len() < 6 {
                return Err(AhrbError::Validation(
                    "fixture phase has insufficient samples".to_owned(),
                ));
            }
            times[5] = times[5].saturating_add(5_000_000);
            let refreshes = cadence
                .membership_refreshes_by_phase
                .get_mut(phase)
                .ok_or_else(|| AhrbError::Validation("fixture refreshes absent".to_owned()))?;
            refreshes[5].elapsed_ns = times[5];
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        assert!(
            certification
                .rows
                .iter()
                .all(|row| matches!(row.outcome, TestOutcome::Pass))
        );
        Ok(())
    }

    #[test]
    fn membership_discovery_wall_overrun_errors_every_resource_row() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        if let Some(cadence) = evidence.phases.cadence.as_mut() {
            let refresh = cadence
                .membership_refreshes_by_phase
                .get_mut("rep0-warm-idle")
                .and_then(|refreshes| refreshes.first_mut())
                .ok_or_else(|| AhrbError::Validation("fixture refresh absent".to_owned()))?;
            refresh.discovery_wall_ns = cadence.membership_cadence_ns.saturating_add(1);
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        assert!(certification.rows.iter().all(|row| {
            matches!(&row.outcome, TestOutcome::Error(message) if message.contains("discovery consumed"))
        }));
        Ok(())
    }

    #[test]
    fn periodic_idle_store_poll_signature_forces_row_21_failure() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let repetition = evidence
            .phases
            .repetitions
            .first()
            .ok_or_else(|| AhrbError::Validation("idle repetition is absent".to_owned()))?;
        let phase = repetition.idle_drift.clone();
        let first = evidence
            .series
            .samples
            .iter()
            .find(|sample| sample.phase == phase)
            .map(|sample| (sample.elapsed_ns, sample.cpu_ns))
            .ok_or_else(|| AhrbError::Validation("idle drift phase is absent".to_owned()))?;
        // Model a cheap session-file stat every 500 ms. Its average CPU remains
        // far below the 1% ceiling, so row 21 must fail because of periodicity.
        for sample in evidence
            .series
            .samples
            .iter_mut()
            .filter(|sample| sample.phase == phase)
        {
            let polls = sample.elapsed_ns.saturating_sub(first.0) / 500_000_000;
            sample.cpu_ns = first.1.saturating_add(polls.saturating_mul(100_000));
        }
        let detected =
            detect_busy_polling(&evidence.series, &evidence.phases.repetitions, 20_000_000)?;
        assert!(detected);
        if let Some(idle) = evidence.idle.as_mut() {
            idle.busy_polling_detected = Some(detected);
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 21)
            .ok_or_else(|| AhrbError::Validation("row 21 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("busy-polling"))
        );
        Ok(())
    }

    #[test]
    fn intermediate_idle_repetition_worker_growth_forces_row_22_failure() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let phase = evidence
            .phases
            .repetitions
            .get(1)
            .map(|repetition| repetition.idle_drift.clone())
            .ok_or_else(|| AhrbError::Validation("second idle repetition is absent".to_owned()))?;
        let final_elapsed = evidence
            .series
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .max()
            .ok_or_else(|| AhrbError::Validation("idle drift samples are absent".to_owned()))?;
        let final_sample = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == phase && sample.elapsed_ns == final_elapsed)
            .ok_or_else(|| AhrbError::Validation("final idle sample is absent".to_owned()))?;
        final_sample.processes.push(ProcessInfo {
            identity: ProcIdentity {
                pid: 9_999,
                start_time: 1,
            },
            ppid: 1,
            command: "leaked-worker".to_owned(),
            ownership: ProcOwnership::Descendant,
        });
        final_sample.thread_count = Some(3);

        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 22)
            .ok_or_else(|| AhrbError::Validation("row 22 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("rep1-worker-growth"))
        );
        assert!(
            row.evidence
                .iter()
                .any(|item| item.starts_with("rep1-thread-growth: 2 -> 3"))
        );
        Ok(())
    }

    #[test]
    fn pre_readiness_cold_peak_forces_row_24_failure() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let phase = evidence
            .cold_start
            .as_ref()
            .and_then(|observations| observations.first())
            .map(|observation| observation.cold_phase.clone())
            .ok_or_else(|| AhrbError::Validation("cold-start observation is absent".to_owned()))?;
        let first_elapsed = evidence
            .series
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .min()
            .ok_or_else(|| AhrbError::Validation("cold-start sample is absent".to_owned()))?;
        let sample = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == phase && sample.elapsed_ns == first_elapsed)
            .ok_or_else(|| AhrbError::Validation("first cold-start sample is absent".to_owned()))?;
        let excessive_peak = 5 * GIB;
        sample.rss_bytes = excessive_peak;
        sample.pss_bytes = Some(excessive_peak);
        sample.private_bytes = Some(excessive_peak);

        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 24)
            .ok_or_else(|| AhrbError::Validation("row 24 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("cold-peak"))
        );
        Ok(())
    }

    #[test]
    fn single_agent_cold_peak_over_envelope_forces_row_25_failure() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let phase = evidence
            .sweep
            .iter()
            .find(|observation| observation.agents == 1)
            .map(|observation| observation.cold_phase.clone())
            .ok_or_else(|| AhrbError::Validation("N=1 cold phase is absent".to_owned()))?;
        let sample = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == phase)
            .ok_or_else(|| AhrbError::Validation("N=1 cold sample is absent".to_owned()))?;
        let excessive_peak = 5 * GIB;
        sample.rss_bytes = excessive_peak;
        sample.pss_bytes = Some(excessive_peak);
        sample.private_bytes = Some(excessive_peak);

        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 25)
            .ok_or_else(|| AhrbError::Validation("row 25 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("single-agent-cold-peak"))
        );
        Ok(())
    }

    #[test]
    fn post_barrier_terminal_cpu_is_included_in_row_25() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let phase = evidence
            .single_agent
            .as_ref()
            .and_then(|observations| observations.first())
            .map(|observation| observation.turn_phase.clone())
            .ok_or_else(|| AhrbError::Validation("single-agent turn phase is absent".to_owned()))?;
        let first_cpu = evidence
            .series
            .samples
            .iter()
            .find(|sample| sample.phase == phase)
            .map(|sample| sample.cpu_ns)
            .ok_or_else(|| AhrbError::Validation("turn-start sample is absent".to_owned()))?;
        let final_elapsed = evidence
            .series
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .max()
            .ok_or_else(|| AhrbError::Validation("turn-end sample is absent".to_owned()))?;
        let final_sample = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == phase && sample.elapsed_ns == final_elapsed)
            .ok_or_else(|| AhrbError::Validation("turn-end sample is absent".to_owned()))?;
        final_sample.cpu_ns = first_cpu.saturating_add(300_000_000);

        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 25)
            .ok_or_else(|| AhrbError::Validation("row 25 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("cpu-per-turn"))
        );
        Ok(())
    }

    #[test]
    fn equal_count_process_replacement_invalidates_row_25_cpu_delta() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let phase = evidence
            .single_agent
            .as_ref()
            .and_then(|observations| observations.first())
            .map(|observation| observation.turn_phase.clone())
            .ok_or_else(|| AhrbError::Validation("single-agent turn phase is absent".to_owned()))?;
        let final_elapsed = evidence
            .series
            .samples
            .iter()
            .filter(|sample| sample.phase == phase)
            .map(|sample| sample.elapsed_ns)
            .max()
            .ok_or_else(|| AhrbError::Validation("turn-end sample is absent".to_owned()))?;
        let final_sample = evidence
            .series
            .samples
            .iter_mut()
            .find(|sample| sample.phase == phase && sample.elapsed_ns == final_elapsed)
            .ok_or_else(|| AhrbError::Validation("turn-end sample is absent".to_owned()))?;
        let process = final_sample.processes.first_mut().ok_or_else(|| {
            AhrbError::Validation("turn-end process membership is absent".to_owned())
        })?;
        process.identity = ProcIdentity {
            pid: 9_999,
            start_time: 2,
        };

        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|result| result.row == 25)
            .ok_or_else(|| AhrbError::Validation("row 25 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Fail(message) if message.contains("complete-turn-boundaries"))
        );
        Ok(())
    }

    #[test]
    fn truncated_profile_window_and_late_n8_reclaim_do_not_pass() -> Result<()> {
        let mut truncated = passing_evidence(ResourceProfile::Cert)?;
        let phase = "rep0-warm-idle";
        let start = truncated
            .series
            .samples
            .iter()
            .find(|sample| sample.phase == phase)
            .map_or(0, |sample| sample.elapsed_ns);
        truncated.series.samples.retain(|sample| {
            sample.phase != phase || sample.elapsed_ns <= start.saturating_add(1_000_000_000)
        });
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &truncated,
            &ResourceEnvelope::default(),
        );
        let row20 = certification.rows.iter().find(|row| row.row == 20);
        assert!(row20.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));

        let mut late_reclaim = passing_evidence(ResourceProfile::Cert)?;
        for observation in &mut late_reclaim.sweep {
            if observation.agents == 8 && observation.identity.repetition == 6 {
                observation.post_close_settled_after_ms = 10_001;
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Cert,
            &late_reclaim,
            &ResourceEnvelope::default(),
        );
        let row23 = certification.rows.iter().find(|row| row.row == 23);
        let row28 = certification.rows.iter().find(|row| row.row == 28);
        assert!(row23.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));
        assert!(row28.is_some_and(|row| !matches!(row.outcome, TestOutcome::Pass)));
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
        for sample in evidence
            .series
            .samples
            .iter_mut()
            .filter(|sample| sample.phase == "rep0-warm-idle")
            .take(10)
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
        for sample in &mut evidence.series.samples {
            sample.collection_ns = 40_000_000;
        }
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
    fn workload_peak_includes_allocations_observed_at_the_steady_barrier() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        for observation in &evidence.sweep {
            let steady_bytes = evidence
                .series
                .samples
                .iter()
                .find(|sample| sample.phase == observation.steady_phase)
                .and_then(|sample| sample.pss_bytes)
                .ok_or_else(|| {
                    AhrbError::Validation(format!(
                        "steady sample {:?} is absent",
                        observation.steady_phase
                    ))
                })?;
            for sample in evidence
                .series
                .samples
                .iter_mut()
                .filter(|sample| sample.phase == observation.workload_phase)
            {
                let boundary_bytes = steady_bytes.saturating_sub(1);
                sample.rss_bytes = boundary_bytes;
                sample.pss_bytes = Some(boundary_bytes);
                sample.private_bytes = Some(boundary_bytes);
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        for row in [26_u8, 27, 28] {
            assert!(
                certification
                    .rows
                    .iter()
                    .find(|result| result.row == row)
                    .is_some_and(|result| matches!(result.outcome, TestOutcome::Pass)),
                "resource row {row} must accept a later steady-barrier peak"
            );
        }
        Ok(())
    }

    #[test]
    fn zero_n1_active_delta_cannot_disappear_from_row_27_fit() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        let steady_and_baseline: Vec<_> = evidence
            .sweep
            .iter()
            .filter(|observation| observation.agents == 1)
            .map(|observation| {
                let baseline = evidence
                    .series
                    .samples
                    .iter()
                    .find(|sample| sample.phase == observation.baseline_phase)
                    .and_then(|sample| sample.pss_bytes)
                    .unwrap_or(0);
                (observation.steady_phase.clone(), baseline)
            })
            .collect();
        for (phase, baseline) in steady_and_baseline {
            for sample in evidence
                .series
                .samples
                .iter_mut()
                .filter(|sample| sample.phase == phase)
            {
                sample.rss_bytes = baseline;
                sample.pss_bytes = Some(baseline);
                sample.private_bytes = Some(baseline);
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row = certification
            .rows
            .iter()
            .find(|row| row.row == 27)
            .ok_or_else(|| AhrbError::Validation("row 27 is absent".to_owned()))?;
        assert!(
            matches!(&row.outcome, TestOutcome::Error(message) if message.contains("positive active delta"))
        );
        Ok(())
    }

    #[test]
    fn nonpositive_preceding_marginal_does_not_hide_a_scaling_jump() -> Result<()> {
        let points = [
            SweepPoint {
                agents: 1,
                baseline_bytes: 0,
                steady_bytes: 100,
                workload_peak_bytes: 100,
                cold_peak_bytes: 100,
                post_turn_bytes: 100,
                post_close_bytes: 0,
            },
            SweepPoint {
                agents: 2,
                baseline_bytes: 0,
                steady_bytes: 90,
                workload_peak_bytes: 90,
                cold_peak_bytes: 90,
                post_turn_bytes: 90,
                post_close_bytes: 0,
            },
            SweepPoint {
                agents: 4,
                baseline_bytes: 0,
                steady_bytes: 110,
                workload_peak_bytes: 110,
                cold_peak_bytes: 110,
                post_turn_bytes: 110,
                post_close_bytes: 0,
            },
        ];
        let metrics = SweepMetrics::calculate(&points)?;
        assert!(!adjacent_marginals_stable(&metrics));
        Ok(())
    }

    #[test]
    fn monotonic_fd_or_thread_growth_fails_long_horizon() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Cert)?;
        if let Some(observations) = evidence.long_horizon.as_mut() {
            for long in observations {
                for (index, point) in long.points.iter_mut().enumerate() {
                    let growth = u64::try_from(index).unwrap_or(u64::MAX);
                    point.open_fds = 10_u64.saturating_add(growth);
                    point.threads = 2_u64.saturating_add(growth);
                }
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

    #[test]
    fn bounded_fd_and_thread_ramp_passes_long_horizon() -> Result<()> {
        let mut evidence = passing_evidence(ResourceProfile::Quick)?;
        if let Some(observations) = evidence.long_horizon.as_mut() {
            for long in observations {
                let ramp_start = long.points.len().saturating_sub(2);
                for point in long.points.iter_mut().skip(ramp_start) {
                    point.open_fds = point.open_fds.saturating_add(1);
                    point.threads = point.threads.saturating_add(1);
                }
            }
        }
        let certification = evaluate_resources(
            ResourceProfile::Quick,
            &evidence,
            &ResourceEnvelope::default(),
        );
        let row29 = certification.rows.iter().find(|row| row.row == 29);
        assert!(row29.is_some_and(|row| matches!(row.outcome, TestOutcome::Pass)));
        Ok(())
    }

    #[test]
    fn monotonic_growth_requires_sustained_accumulation() {
        assert!(!monotonic_growth(&[6, 6, 6, 6, 7, 7]));
        assert!(monotonic_growth(&[6, 7, 8, 9, 10, 11]));
        assert!(monotonic_growth(&[6, 7, 7, 8, 8, 9]));
        assert!(!monotonic_growth(&[6, 7, 8, 7, 9, 10]));
    }

    #[test]
    fn per_invocation_resources_use_zero_idle_and_process_marginals() {
        let timing = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let observations = timing
            .sweep_widths
            .iter()
            .flat_map(|agents| {
                (0..timing.repetitions).map(move |repetition| PerInvocationObservation {
                    repetition,
                    agents: *agents,
                    peak_bytes: u64::from(*agents) * 20 * MIB,
                    cold_peak_bytes: u64::from(*agents) * 21 * MIB,
                    cpu_ns: u64::from(*agents) * 10_000_000,
                    completed_processes: *agents,
                    residual_processes: 0,
                })
            })
            .collect::<Vec<_>>();
        let certification = evaluate_per_invocation_resources(
            ResourceProfile::Quick,
            &observations,
            &ResourceEnvelope::default(),
        );

        assert_eq!(certification.rows.len(), 10);
        assert!(
            certification
                .rows
                .iter()
                .all(|row| matches!(row.outcome, TestOutcome::Pass))
        );
        assert_eq!(certification.metrics.get("idle_median_bytes"), Some(&0.0));
        assert_eq!(
            certification.metrics.get("parallel_beta_mib_per_agent"),
            Some(&20.0)
        );
        for row in [20_u8, 21, 22, 23, 28, 29] {
            let evidence = certification
                .rows
                .iter()
                .find(|result| result.row == row)
                .map(|result| result.evidence.join(" "))
                .unwrap_or_default();
            assert!(evidence.contains("process") || evidence.contains("resident"));
        }
    }

    #[test]
    fn per_invocation_resources_reject_duplicate_repetition_evidence() {
        let timing = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let observations = timing
            .sweep_widths
            .iter()
            .flat_map(|agents| {
                (0..timing.repetitions).map(move |_| PerInvocationObservation {
                    repetition: 0,
                    agents: *agents,
                    peak_bytes: u64::from(*agents) * 20 * MIB,
                    cold_peak_bytes: u64::from(*agents) * 20 * MIB,
                    cpu_ns: 10_000_000,
                    completed_processes: *agents,
                    residual_processes: 0,
                })
            })
            .collect::<Vec<_>>();
        let certification = evaluate_per_invocation_resources(
            ResourceProfile::Quick,
            &observations,
            &ResourceEnvelope::default(),
        );

        for row in [24_u8, 25, 26, 27] {
            assert!(certification.rows.iter().any(|result| {
                result.row == row
                    && matches!(&result.outcome, TestOutcome::Error(detail) if detail.contains("repetition identities"))
            }));
        }
    }

    #[test]
    fn per_invocation_residual_child_fails_zero_idle_and_automatic_trials() {
        let timing = ResourceTimingPlan::for_profile(ResourceProfile::Quick);
        let mut observations = timing
            .sweep_widths
            .iter()
            .flat_map(|agents| {
                (0..timing.repetitions).map(move |repetition| PerInvocationObservation {
                    repetition,
                    agents: *agents,
                    peak_bytes: u64::from(*agents) * 20 * MIB,
                    cold_peak_bytes: u64::from(*agents) * 20 * MIB,
                    cpu_ns: 10_000_000,
                    completed_processes: *agents,
                    residual_processes: 0,
                })
            })
            .collect::<Vec<_>>();
        if let Some(first) = observations.first_mut() {
            first.residual_processes = 1;
        }
        let certification = evaluate_per_invocation_resources(
            ResourceProfile::Quick,
            &observations,
            &ResourceEnvelope::default(),
        );

        for row in [20_u8, 21, 22, 23, 28, 29] {
            assert!(certification.rows.iter().any(|result| {
                result.row == row && matches!(result.outcome, TestOutcome::Fail(_))
            }));
        }
    }
}
