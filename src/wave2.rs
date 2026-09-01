//! Wave-2 fault-fixture evidence and row-specific oracle evaluation.
//!
//! The types in this module contain only AHRB-owned/provider/process observations.
//! They do not add callbacks or instrumentation to a harness turn path.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::fake_model::ModelFrameObservation;

/// How the row-47 log-path declaration was supplied and verified.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogEvidenceKind {
    /// The schema-2 `resources.log_paths` key was omitted.
    Omitted,
    /// A nonempty set of declared paths was sampled at every turn boundary.
    DeclaredPaths,
    /// The declaration was explicitly empty and an isolated-root scan found no log artifact.
    VerifiedNoLog,
}

/// One complete external turn observation for row 47.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DiskTurnEvidence {
    /// Fresh-profile repetition number, one-based.
    pub repetition: u32,
    /// Semantic turn index, one-based.
    pub turn: u32,
    /// Owned-tree cumulative bytes-written delta.
    pub disk_write_bytes: u64,
    /// Growth across the event path and declared extra journal paths.
    pub journal_growth_bytes: u64,
    /// Growth across declared log paths; absent only for verified-no-log.
    pub log_growth_bytes: Option<u64>,
    /// All live and retired identities have final counter evidence.
    pub counter_complete: bool,
    /// Every sampled file has a stable device/file identity at both boundaries.
    pub file_identity_complete: bool,
    /// Exited identities were finalized terminal-before-reap or through durable cgroup data.
    pub retired_accounting_complete: bool,
}

/// Exact row-47 aggregation.
#[derive(Clone, Debug, PartialEq)]
pub struct DiskIoEvaluation {
    /// Exact numeric resource-summary values; nullable log growth is separate.
    pub resource_values: BTreeMap<String, f64>,
    /// Nullable log-growth headline.
    pub log_growth_bytes_per_turn: Option<f64>,
    /// Counter-completeness summary.
    pub disk_io_counter_complete: bool,
    /// Whether any repetition exhibited unbounded growth.
    pub unbounded_disk_growth: bool,
    /// Exact typed detail block.
    pub details: Value,
    /// False when the observation cannot be trusted.
    pub measurement_complete: bool,
    /// Informational reference-envelope result when complete.
    pub reference_envelope_pass: bool,
    /// Stable infrastructure diagnostic when incomplete.
    pub measurement_error: Option<String>,
}

impl DiskIoEvaluation {
    fn incomplete(error: impl Into<String>, log_evidence: LogEvidenceKind) -> Self {
        let error = error.into();
        Self {
            resource_values: BTreeMap::new(),
            log_growth_bytes_per_turn: None,
            disk_io_counter_complete: false,
            unbounded_disk_growth: false,
            details: json!({
                "measurement_complete": false,
                "measurement_error": error,
                "log_evidence": log_evidence,
            }),
            measurement_complete: false,
            reference_envelope_pass: false,
            measurement_error: Some(error),
        }
    }
}

#[derive(Clone, Debug)]
struct DiskRepetition {
    repetition: u32,
    p50: f64,
    p95: f64,
    max: f64,
    journal_median: f64,
    log_median: Option<f64>,
    slope: f64,
    unbounded: bool,
    passed: bool,
}

/// Evaluate row 47 with the exact per-repetition-first aggregation contract.
pub fn evaluate_disk_io_per_turn(
    evidence: &[DiskTurnEvidence],
    expected_repetitions: u32,
    expected_turns: u32,
    log_evidence: LogEvidenceKind,
) -> DiskIoEvaluation {
    if log_evidence == LogEvidenceKind::Omitted {
        return DiskIoEvaluation::incomplete(
            "resources.log_paths was omitted; omitted logs are not verified-no-log evidence",
            log_evidence,
        );
    }
    let expected_count = u64::from(expected_repetitions) * u64::from(expected_turns);
    if u64::try_from(evidence.len()).ok() != Some(expected_count) {
        return DiskIoEvaluation::incomplete(
            format!(
                "row-47 evidence has {} turns; expected {expected_count}",
                evidence.len()
            ),
            log_evidence,
        );
    }
    let mut unique = BTreeSet::new();
    for turn in evidence {
        if turn.repetition == 0
            || turn.repetition > expected_repetitions
            || turn.turn == 0
            || turn.turn > expected_turns
            || !unique.insert((turn.repetition, turn.turn))
        {
            return DiskIoEvaluation::incomplete(
                "row-47 evidence has an invalid or duplicate repetition/turn key",
                log_evidence,
            );
        }
        if !turn.counter_complete || !turn.retired_accounting_complete {
            return DiskIoEvaluation::incomplete(
                "owned-tree disk accounting lacks terminal-before-reap or durable cgroup retirement evidence",
                log_evidence,
            );
        }
        if !turn.file_identity_complete {
            return DiskIoEvaluation::incomplete(
                "journal/log snapshot lacks a complete device and file identity",
                log_evidence,
            );
        }
        match log_evidence {
            LogEvidenceKind::DeclaredPaths if turn.log_growth_bytes.is_none() => {
                return DiskIoEvaluation::incomplete(
                    "a declared log path lacks a boundary growth observation",
                    log_evidence,
                );
            }
            LogEvidenceKind::VerifiedNoLog if turn.log_growth_bytes.is_some() => {
                return DiskIoEvaluation::incomplete(
                    "verified-no-log evidence contradicts a numeric log-growth observation",
                    log_evidence,
                );
            }
            _ => {}
        }
    }

    let mut repetitions = Vec::new();
    for repetition in 1..=expected_repetitions {
        let mut turns = evidence
            .iter()
            .filter(|turn| turn.repetition == repetition)
            .collect::<Vec<_>>();
        turns.sort_by_key(|turn| turn.turn);
        if u32::try_from(turns.len()).ok() != Some(expected_turns) {
            return DiskIoEvaluation::incomplete(
                format!("row-47 repetition {repetition} is incomplete"),
                log_evidence,
            );
        }
        let disk = turns
            .iter()
            .map(|turn| turn.disk_write_bytes as f64)
            .collect::<Vec<_>>();
        let journal = turns
            .iter()
            .map(|turn| turn.journal_growth_bytes as f64)
            .collect::<Vec<_>>();
        let log = turns
            .iter()
            .filter_map(|turn| turn.log_growth_bytes.map(|value| value as f64))
            .collect::<Vec<_>>();
        let p50 = nearest_rank(&disk, 50);
        let p95 = nearest_rank(&disk, 95);
        let maximum = disk.iter().copied().fold(0.0_f64, f64::max);
        let journal_median = median(&journal);
        let log_median = (log_evidence == LogEvidenceKind::DeclaredPaths).then(|| median(&log));
        let slope = theil_sen_indexed(&disk);
        let midpoint = disk.len() / 2;
        let first = median(&disk[..midpoint]);
        let second = median(&disk[midpoint..]);
        let unbounded = slope > 4_096.0 || second > (1.25 * first).max(first + 65_536.0);
        let passed = p95 <= 67_108_864.0
            && journal_median <= 1_048_576.0
            && log_median.is_none_or(|value| value <= 1_048_576.0)
            && slope <= 4_096.0
            && !unbounded;
        repetitions.push(DiskRepetition {
            repetition,
            p50,
            p95,
            max: maximum,
            journal_median,
            log_median,
            slope,
            unbounded,
            passed,
        });
    }
    let p50 = median(
        &repetitions
            .iter()
            .map(|value| value.p50)
            .collect::<Vec<_>>(),
    );
    let p95 = median(
        &repetitions
            .iter()
            .map(|value| value.p95)
            .collect::<Vec<_>>(),
    );
    let maximum = repetitions
        .iter()
        .map(|value| value.max)
        .fold(0.0_f64, f64::max);
    let journal = median(
        &repetitions
            .iter()
            .map(|value| value.journal_median)
            .collect::<Vec<_>>(),
    );
    let log = (log_evidence == LogEvidenceKind::DeclaredPaths).then(|| {
        median(
            &repetitions
                .iter()
                .filter_map(|value| value.log_median)
                .collect::<Vec<_>>(),
        )
    });
    let slope = median(
        &repetitions
            .iter()
            .map(|value| value.slope)
            .collect::<Vec<_>>(),
    );
    let unbounded = repetitions.iter().any(|value| value.unbounded);
    let reference_envelope_pass = repetitions.iter().all(|value| value.passed);
    let resource_values = BTreeMap::from([
        ("disk_write_bytes_per_turn_p50".to_owned(), p50),
        ("disk_write_bytes_per_turn_p95".to_owned(), p95),
        ("disk_write_bytes_per_turn_max".to_owned(), maximum),
        ("session_journal_growth_bytes_per_turn".to_owned(), journal),
        ("disk_write_growth_slope_bytes_per_turn2".to_owned(), slope),
    ]);
    let details_repetitions = repetitions
        .iter()
        .map(|value| {
            json!({
                "repetition": value.repetition,
                "disk_write_bytes_per_turn_p50": value.p50,
                "disk_write_bytes_per_turn_p95": value.p95,
                "disk_write_bytes_per_turn_max": value.max,
                "session_journal_growth_bytes_per_turn": value.journal_median,
                "log_growth_bytes_per_turn": value.log_median,
                "disk_write_growth_slope_bytes_per_turn2": value.slope,
                "unbounded_disk_growth": value.unbounded,
                "passed": value.passed,
            })
        })
        .collect::<Vec<_>>();
    DiskIoEvaluation {
        resource_values,
        log_growth_bytes_per_turn: log,
        disk_io_counter_complete: true,
        unbounded_disk_growth: unbounded,
        details: json!({
            "measurement_complete": true,
            "log_evidence": log_evidence,
            "repetitions": details_repetitions,
        }),
        measurement_complete: true,
        reference_envelope_pass,
        measurement_error: None,
    }
}

/// One complete externally sampled row-48 trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelWaitCpuSample {
    pub sample_started_ns: u64,
    pub sample_finished_ns: u64,
    pub cpu_ns: u64,
}

/// One complete externally sampled row-48 trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelWaitCpuTrialEvidence {
    pub repetition: u32,
    pub response_headers_ns: u64,
    pub terminal_ns: Option<u64>,
    pub cpu_ns: u64,
    pub cpu_samples: Vec<ModelWaitCpuSample>,
    pub frames: Vec<ModelFrameObservation>,
    pub terminal_count: u32,
    pub success_terminals: u32,
    pub outer_kill_used: bool,
    pub outer_kill_ns: Option<u64>,
    pub outer_deadline_ms: u64,
}

/// Exact row-48 aggregation and precedence decision.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelWaitCpuEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub cpu_p50_ms: f64,
    pub wall_p50_ms: f64,
    pub one_core_max_ratio: f64,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// One independent row-59 trickle/stall pair.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SlowStreamStallTrialEvidence {
    pub repetition: u32,
    pub slow_response_headers_ns: u64,
    pub slow_terminal_ns: Option<u64>,
    pub slow_frames: Vec<ModelFrameObservation>,
    pub slow_terminal_count: u32,
    pub slow_success_terminals: u32,
    pub slow_idle_timeout_fired: bool,
    pub slow_outer_kill_used: bool,
    pub slow_outer_kill_ns: Option<u64>,
    pub slow_outer_deadline_ms: u64,
    pub stall_response_headers_ns: u64,
    pub stall_terminal_ns: Option<u64>,
    pub stall_bytes_yielded: u64,
    pub stall_terminal_count: u32,
    pub stall_failure_terminals: u32,
    pub stall_outer_kill_used: bool,
    pub stall_outer_kill_ns: Option<u64>,
    pub stall_outer_deadline_ms: u64,
}

/// Exact row-59 aggregation and precedence decision.
#[derive(Clone, Debug, PartialEq)]
pub struct SlowStreamStallEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

struct ValidatedFrames {
    bytes: u64,
    last_yield_ns: u64,
    wall_ns: u64,
    max_inter_frame_ms: f64,
}

fn cpu_sample_point(sample: &ModelWaitCpuSample) -> std::result::Result<u64, String> {
    let width = sample
        .sample_finished_ns
        .checked_sub(sample.sample_started_ns)
        .ok_or_else(|| "model-wait CPU sample boundaries regressed".to_owned())?;
    Ok(sample.sample_started_ns.saturating_add(width / 2))
}

fn interpolate_cpu_at(
    samples: &[ModelWaitCpuSample],
    boundary_ns: u64,
) -> std::result::Result<f64, String> {
    let mut points = samples
        .iter()
        .map(|sample| cpu_sample_point(sample).map(|point| (point, sample.cpu_ns)))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    points.sort_by_key(|point| point.0);
    if points.windows(2).any(|pair| pair[0].1 > pair[1].1) {
        return Err("model-wait cumulative CPU samples regressed".to_owned());
    }
    let left = points
        .iter()
        .rev()
        .find(|point| point.0 <= boundary_ns)
        .copied()
        .ok_or_else(|| format!("no CPU sample precedes boundary {boundary_ns}"))?;
    let right = points
        .iter()
        .find(|point| point.0 >= boundary_ns)
        .copied()
        .ok_or_else(|| format!("no CPU sample follows boundary {boundary_ns}"))?;
    if left.0 == right.0 {
        return Ok(left.1 as f64);
    }
    let position = boundary_ns.saturating_sub(left.0) as f64;
    let width = right.0.saturating_sub(left.0) as f64;
    Ok(left.1 as f64 + (right.1.saturating_sub(left.1) as f64 * position / width))
}

/// Interpolate cumulative owned-tree CPU at the exact provider boundaries.
pub fn interpolated_model_wait_cpu_ns(
    samples: &[ModelWaitCpuSample],
    response_headers_ns: u64,
    final_frame_yield_ns: u64,
) -> std::result::Result<u64, String> {
    let start = interpolate_cpu_at(samples, response_headers_ns)?;
    let end = interpolate_cpu_at(samples, final_frame_yield_ns)?;
    if end < start {
        return Err("interpolated model-wait CPU regressed".to_owned());
    }
    Ok((end - start).round() as u64)
}

fn validate_trickle_frames(
    frames: &[ModelFrameObservation],
    response_headers_ns: u64,
    expected_count: u32,
) -> std::result::Result<ValidatedFrames, String> {
    if response_headers_ns == 0 {
        return Err("provider response-header boundary is missing".to_owned());
    }
    if frames.len() != expected_count as usize {
        return Err(format!(
            "observed {} paced frames; expected {expected_count}",
            frames.len()
        ));
    }
    let mut frames = frames.to_vec();
    frames.sort_by_key(|frame| frame.ordinal);
    let mut previous_yield_ns = response_headers_ns;
    let mut max_gap_ns = 0_u64;
    let mut bytes = 0_u64;
    for (index, frame) in frames.iter().enumerate() {
        let ordinal = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| "paced-frame ordinal overflow".to_owned())?;
        if frame.ordinal != ordinal {
            return Err("paced-frame ordinals are missing or duplicated".to_owned());
        }
        if frame.bytes != 1 {
            return Err(format!(
                "paced frame {} carried {} bytes instead of one",
                frame.ordinal, frame.bytes
            ));
        }
        let scheduled_ns = response_headers_ns
            .checked_add(u64::from(ordinal).saturating_mul(1_000_000_000))
            .ok_or_else(|| "paced-frame scheduled boundary overflow".to_owned())?;
        if frame.scheduled_ns != scheduled_ns || frame.frame_yielded_ns < frame.scheduled_ns {
            return Err(format!(
                "paced frame {} has an invalid scheduled/yield boundary",
                frame.ordinal
            ));
        }
        let gap_ns = frame
            .frame_yielded_ns
            .checked_sub(previous_yield_ns)
            .ok_or_else(|| "paced-frame yield boundaries regressed".to_owned())?;
        max_gap_ns = max_gap_ns.max(gap_ns);
        previous_yield_ns = frame.frame_yielded_ns;
        bytes = bytes.saturating_add(frame.bytes);
    }
    if max_gap_ns > 1_250_000_000 {
        return Err(format!(
            "AHRB provider pacing gap {:.3} ms exceeded 1250 ms",
            max_gap_ns as f64 / 1_000_000.0
        ));
    }
    let last_yield_ns = frames
        .last()
        .map(|frame| frame.frame_yielded_ns)
        .ok_or_else(|| "paced-frame final yield boundary is missing".to_owned())?;
    Ok(ValidatedFrames {
        bytes,
        last_yield_ns,
        wall_ns: last_yield_ns.saturating_sub(response_headers_ns),
        max_inter_frame_ms: max_gap_ns as f64 / 1_000_000.0,
    })
}

/// Evaluate row 48. Frame/pacing defects are infrastructure errors; complete
/// CPU or terminal violations are measured harness failures.
pub fn evaluate_model_wait_cpu(
    trials: &[ModelWaitCpuTrialEvidence],
    expected_repetitions: u32,
    expected_count: u32,
    idle_timeout_ms: u64,
) -> ModelWaitCpuEvaluation {
    let incomplete = |message: String| ModelWaitCpuEvaluation {
        metrics: BTreeMap::new(),
        details: json!({"measurement_complete":false,"measurement_error":message}),
        cpu_p50_ms: 0.0,
        wall_p50_ms: 0.0,
        one_core_max_ratio: 0.0,
        measurement_complete: false,
        passed: false,
        measurement_error: Some(message),
    };
    if trials.len() != expected_repetitions as usize {
        return incomplete(format!(
            "row-48 has {} trials; expected {expected_repetitions}",
            trials.len()
        ));
    }
    let expected_deadline_ms = u64::from(expected_count)
        .saturating_mul(1_000)
        .saturating_add(idle_timeout_ms)
        .saturating_add(3_000);
    let mut repetitions = BTreeSet::new();
    let mut cpu_ms = Vec::new();
    let mut wall_ms = Vec::new();
    let mut ratios = Vec::new();
    let mut bytes = 0_u64;
    let mut max_gap_ms = 0.0_f64;
    let mut all_passed = true;
    let mut details = Vec::new();
    for trial in trials {
        if trial.repetition == 0
            || trial.repetition > expected_repetitions
            || !repetitions.insert(trial.repetition)
        {
            return incomplete("row-48 repetition keys are invalid or duplicated".to_owned());
        }
        if trial.outer_deadline_ms != expected_deadline_ms {
            return incomplete(format!(
                "row-48 repetition {} used invalid outer deadline {} ms; expected {expected_deadline_ms} ms",
                trial.repetition, trial.outer_deadline_ms
            ));
        }
        let frames =
            match validate_trickle_frames(&trial.frames, trial.response_headers_ns, expected_count)
            {
                Ok(frames) => frames,
                Err(error) => {
                    return incomplete(format!("row-48 repetition {}: {error}", trial.repetition));
                }
            };
        let terminal_ns = match (trial.terminal_ns, trial.outer_kill_ns) {
            (Some(terminal), _) => terminal,
            (None, Some(outer_kill)) if trial.outer_kill_used => outer_kill,
            _ => {
                return incomplete(format!(
                    "row-48 repetition {} lacks a terminal or outer-kill boundary",
                    trial.repetition
                ));
            }
        };
        let derived_cpu_ns = match interpolated_model_wait_cpu_ns(
            &trial.cpu_samples,
            trial.response_headers_ns,
            frames.last_yield_ns,
        ) {
            Ok(value) => value,
            Err(error) => {
                return incomplete(format!("row-48 repetition {}: {error}", trial.repetition));
            }
        };
        if derived_cpu_ns != trial.cpu_ns {
            return incomplete(format!(
                "row-48 repetition {} CPU interpolation disagrees: declared {}, derived {derived_cpu_ns}",
                trial.repetition, trial.cpu_ns
            ));
        }
        let outer_deadline_ns = trial
            .response_headers_ns
            .saturating_add(expected_deadline_ms.saturating_mul(1_000_000));
        let terminal_ok = trial.terminal_count == 1
            && trial.success_terminals == 1
            && terminal_ns >= frames.last_yield_ns
            && terminal_ns < outer_deadline_ns
            && !trial.outer_kill_used
            && trial.outer_kill_ns.is_none();
        let ratio = if frames.wall_ns == 0 {
            f64::INFINITY
        } else {
            trial.cpu_ns as f64 / frames.wall_ns as f64
        };
        let trial_passed = terminal_ok && ratio <= 0.05;
        all_passed &= trial_passed;
        bytes = bytes.saturating_add(frames.bytes);
        max_gap_ms = max_gap_ms.max(frames.max_inter_frame_ms);
        cpu_ms.push(trial.cpu_ns as f64 / 1_000_000.0);
        wall_ms.push(frames.wall_ns as f64 / 1_000_000.0);
        ratios.push(ratio);
        details.push(json!({
            "repetition":trial.repetition,
            "response_headers_ns":trial.response_headers_ns,
            "final_frame_yield_ns":frames.last_yield_ns,
            "terminal_ns":terminal_ns,
            "cpu_ns":trial.cpu_ns,
            "cpu_samples":trial.cpu_samples,
            "wall_ns":frames.wall_ns,
            "one_core_ratio":ratio,
            "bytes_yielded":frames.bytes,
            "max_inter_frame_ms":frames.max_inter_frame_ms,
            "terminal_count":trial.terminal_count,
            "success_terminals":trial.success_terminals,
            "outer_kill_used":trial.outer_kill_used,
            "outer_kill_ns":trial.outer_kill_ns,
            "passed":trial_passed,
        }));
    }
    let cpu_p50_ms = median(&cpu_ms);
    let wall_p50_ms = median(&wall_ms);
    let one_core_max_ratio = ratios.into_iter().fold(0.0_f64, f64::max);
    ModelWaitCpuEvaluation {
        metrics: BTreeMap::from([
            ("model_wait_cpu.bytes_yielded".to_owned(), bytes as f64),
            ("model_wait_cpu.max_inter_frame_ms".to_owned(), max_gap_ms),
        ]),
        details: json!({
            "measurement_complete":true,
            "repetitions":details,
            "daemon_baseline_subtracted":false,
        }),
        cpu_p50_ms,
        wall_p50_ms,
        one_core_max_ratio,
        measurement_complete: true,
        passed: all_passed,
        measurement_error: None,
    }
}

/// Evaluate row 59 with infrastructure precedence for fixture timing and
/// measured FAIL semantics for trustworthy harness timeout behavior.
pub fn evaluate_slow_stream_vs_stall(
    trials: &[SlowStreamStallTrialEvidence],
    expected_repetitions: u32,
    expected_count: u32,
    idle_timeout_ms: u64,
) -> SlowStreamStallEvaluation {
    let incomplete = |message: String| SlowStreamStallEvaluation {
        metrics: BTreeMap::new(),
        details: json!({"measurement_complete":false,"measurement_error":message}),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(message),
    };
    if idle_timeout_ms < 1_250 {
        return incomplete(format!(
            "row-59 idle_timeout_ms={idle_timeout_ms} is below 1250"
        ));
    }
    if trials.len() != expected_repetitions as usize {
        return incomplete(format!(
            "row-59 has {} pairs; expected {expected_repetitions}",
            trials.len()
        ));
    }
    let expected_slow_deadline_ms = u64::from(expected_count)
        .saturating_mul(1_000)
        .saturating_add(idle_timeout_ms)
        .saturating_add(3_000);
    let expected_stall_deadline_ms = idle_timeout_ms.saturating_add(3_000);
    let mut repetitions = BTreeSet::new();
    let mut slow_bytes = 0_u64;
    let mut slow_max_gap_ms = 0.0_f64;
    let mut slow_success = 0_u64;
    let mut slow_idle = 0_u64;
    let mut stall_bytes = 0_u64;
    let mut stall_max_timeout_ms = 0.0_f64;
    let mut stall_failure = 0_u64;
    let mut stall_outer_kill = 0_u64;
    let mut all_passed = true;
    let mut details = Vec::new();
    for trial in trials {
        if trial.repetition == 0
            || trial.repetition > expected_repetitions
            || !repetitions.insert(trial.repetition)
        {
            return incomplete("row-59 repetition keys are invalid or duplicated".to_owned());
        }
        if trial.slow_outer_deadline_ms != expected_slow_deadline_ms
            || trial.stall_outer_deadline_ms != expected_stall_deadline_ms
        {
            return incomplete(format!(
                "row-59 repetition {} used invalid row-local deadlines",
                trial.repetition
            ));
        }
        let slow = match validate_trickle_frames(
            &trial.slow_frames,
            trial.slow_response_headers_ns,
            expected_count,
        ) {
            Ok(frames) => frames,
            Err(error) => {
                return incomplete(format!(
                    "row-59 repetition {} slow case: {error}",
                    trial.repetition
                ));
            }
        };
        let slow_terminal_ns = match (trial.slow_terminal_ns, trial.slow_outer_kill_ns) {
            (Some(terminal), _) => terminal,
            (None, Some(outer_kill)) if trial.slow_outer_kill_used => outer_kill,
            _ => {
                return incomplete(format!(
                    "row-59 repetition {} slow case lacks terminal or outer-kill boundary",
                    trial.repetition
                ));
            }
        };
        if trial.stall_response_headers_ns == 0 {
            return incomplete(format!(
                "row-59 repetition {} stall case lacks response-header boundary",
                trial.repetition
            ));
        }
        let stall_terminal_ns = match (trial.stall_terminal_ns, trial.stall_outer_kill_ns) {
            (Some(terminal), _) => terminal,
            (None, Some(outer_kill)) if trial.stall_outer_kill_used => outer_kill,
            _ => {
                return incomplete(format!(
                    "row-59 repetition {} stall case lacks timeout or outer-kill boundary",
                    trial.repetition
                ));
            }
        };
        let stall_elapsed_ns = match stall_terminal_ns.checked_sub(trial.stall_response_headers_ns)
        {
            Some(value) => value,
            None => {
                return incomplete(format!(
                    "row-59 repetition {} stall terminal precedes headers",
                    trial.repetition
                ));
            }
        };
        let stall_elapsed_ms = stall_elapsed_ns as f64 / 1_000_000.0;
        let slow_outer_ns = trial
            .slow_response_headers_ns
            .saturating_add(expected_slow_deadline_ms.saturating_mul(1_000_000));
        let slow_passed = trial.slow_terminal_count == 1
            && trial.slow_success_terminals == 1
            && !trial.slow_idle_timeout_fired
            && !trial.slow_outer_kill_used
            && slow_terminal_ns >= slow.last_yield_ns
            && slow_terminal_ns < slow_outer_ns;
        let stall_passed = trial.stall_bytes_yielded == 0
            && trial.stall_terminal_count == 1
            && trial.stall_failure_terminals == 1
            && !trial.stall_outer_kill_used
            && stall_elapsed_ms >= idle_timeout_ms as f64
            && stall_elapsed_ms <= idle_timeout_ms.saturating_add(1_000) as f64
            && stall_terminal_ns
                < trial
                    .stall_response_headers_ns
                    .saturating_add(expected_stall_deadline_ms.saturating_mul(1_000_000));
        let pair_passed = slow_passed && stall_passed;
        all_passed &= pair_passed;
        slow_bytes = slow_bytes.saturating_add(slow.bytes);
        slow_max_gap_ms = slow_max_gap_ms.max(slow.max_inter_frame_ms);
        slow_success = slow_success.saturating_add(u64::from(trial.slow_success_terminals));
        slow_idle = slow_idle.saturating_add(u64::from(trial.slow_idle_timeout_fired));
        stall_bytes = stall_bytes.saturating_add(trial.stall_bytes_yielded);
        stall_max_timeout_ms = stall_max_timeout_ms.max(stall_elapsed_ms);
        stall_failure = stall_failure.saturating_add(u64::from(trial.stall_failure_terminals));
        stall_outer_kill = stall_outer_kill.saturating_add(u64::from(trial.stall_outer_kill_used));
        details.push(json!({
            "repetition":trial.repetition,
            "slow":{
                "response_headers_ns":trial.slow_response_headers_ns,
                "terminal_ns":slow_terminal_ns,
                "bytes_yielded":slow.bytes,
                "max_inter_frame_ms":slow.max_inter_frame_ms,
                "terminal_count":trial.slow_terminal_count,
                "terminal_success":trial.slow_success_terminals,
                "idle_timeout_fired":trial.slow_idle_timeout_fired,
                "outer_kill_used":trial.slow_outer_kill_used,
                "outer_kill_ns":trial.slow_outer_kill_ns,
                "passed":slow_passed,
            },
            "stall":{
                "response_headers_ns":trial.stall_response_headers_ns,
                "terminal_ns":stall_terminal_ns,
                "bytes_yielded":trial.stall_bytes_yielded,
                "own_timeout_ms":stall_elapsed_ms,
                "terminal_count":trial.stall_terminal_count,
                "structured_failure":trial.stall_failure_terminals,
                "outer_kill_used":trial.stall_outer_kill_used,
                "outer_kill_ns":trial.stall_outer_kill_ns,
                "passed":stall_passed,
            },
            "passed":pair_passed,
        }));
    }
    SlowStreamStallEvaluation {
        metrics: BTreeMap::from([
            (
                "slow_stream_vs_stall.slow_bytes_yielded".to_owned(),
                slow_bytes as f64,
            ),
            (
                "slow_stream_vs_stall.slow_max_inter_frame_ms".to_owned(),
                slow_max_gap_ms,
            ),
            (
                "slow_stream_vs_stall.slow_terminal_success".to_owned(),
                slow_success as f64,
            ),
            (
                "slow_stream_vs_stall.slow_idle_timeout_fired".to_owned(),
                slow_idle as f64,
            ),
            (
                "slow_stream_vs_stall.stall_bytes_yielded".to_owned(),
                stall_bytes as f64,
            ),
            (
                "slow_stream_vs_stall.stall_own_timeout_ms".to_owned(),
                stall_max_timeout_ms,
            ),
            (
                "slow_stream_vs_stall.stall_structured_failure".to_owned(),
                stall_failure as f64,
            ),
            (
                "slow_stream_vs_stall.stall_outer_kill_used".to_owned(),
                stall_outer_kill as f64,
            ),
        ]),
        details: json!({"measurement_complete":true,"repetitions":details}),
        measurement_complete: true,
        passed: all_passed,
        measurement_error: None,
    }
}

fn nearest_rank(values: &[f64], percentile: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let rank = percentile.saturating_mul(sorted.len()).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn theil_sen_indexed(values: &[f64]) -> f64 {
    let mut slopes = Vec::new();
    for left in 0..values.len() {
        for right in (left + 1)..values.len() {
            slopes.push((values[right] - values[left]) / (right - left) as f64);
        }
    }
    median(&slopes)
}

/// Incremental, bounded capture of a tool's complete byte stream.
#[derive(Clone, Debug)]
pub struct BoundedToolCapture {
    capture_limit: usize,
    retained: Vec<u8>,
    original_bytes: u64,
    digest: Sha256,
}

impl BoundedToolCapture {
    /// Create a capture that never retains more than `capture_limit` payload bytes.
    pub fn new(capture_limit: usize) -> Self {
        Self {
            capture_limit,
            retained: Vec::with_capacity(capture_limit.min(64 * 1024)),
            original_bytes: 0,
            digest: Sha256::new(),
        }
    }

    /// Observe a streamed chunk without allocating in proportion to total output.
    pub fn push(&mut self, bytes: &[u8]) {
        let chunk_bytes = bytes.len() as u64;
        self.original_bytes = self.original_bytes.saturating_add(chunk_bytes);
        self.digest.update(bytes);
        let available = self.capture_limit.saturating_sub(self.retained.len());
        let count = available.min(bytes.len());
        self.retained.extend_from_slice(&bytes[..count]);
    }

    /// Finalize a model-visible payload and structured marker inside `encoded_limit`.
    pub fn finish(self, encoded_limit: usize) -> LargeOutputCapture {
        let digest = lower_hex(&self.digest.finalize());
        let mut payload_len = self.retained.len().min(encoded_limit);
        loop {
            let marker = render_truncation_marker(self.original_bytes, payload_len as u64, &digest);
            let separator = usize::from(payload_len > 0);
            let total = payload_len
                .saturating_add(separator)
                .saturating_add(marker.len());
            if total <= encoded_limit {
                let mut encoded = self.retained[..payload_len].to_vec();
                if payload_len > 0 {
                    encoded.push(b'\n');
                }
                encoded.extend_from_slice(marker.as_bytes());
                return LargeOutputCapture {
                    original_bytes: self.original_bytes,
                    payload_bytes: payload_len as u64,
                    encoded,
                    sha256: digest,
                };
            }
            if payload_len == 0 {
                return LargeOutputCapture {
                    original_bytes: self.original_bytes,
                    payload_bytes: 0,
                    encoded: marker.into_bytes(),
                    sha256: digest,
                };
            }
            payload_len = payload_len.saturating_sub(1);
        }
    }
}

/// Completed bounded tool capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LargeOutputCapture {
    /// Total bytes observed from the producer.
    pub original_bytes: u64,
    /// Retained producer bytes before the marker.
    pub payload_bytes: u64,
    /// Complete normalized model-visible content.
    pub encoded: Vec<u8>,
    /// Digest of the complete original stream.
    pub sha256: String,
}

/// Render the exact non-self-referential row-60 marker.
pub fn render_truncation_marker(original_bytes: u64, payload_bytes: u64, sha256: &str) -> String {
    format!(
        "TRUNCATED truncated=true original={original_bytes} payload={payload_bytes} sha256={sha256}"
    )
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

/// One complete row-60 trial.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LargeOutputTrial {
    pub repetition: u32,
    pub produced_bytes: u64,
    pub model_visible_bytes: u64,
    pub model_visible_encoded_bytes: u64,
    pub harness_output_limit_bytes: u64,
    pub evidence_captured_bytes: u64,
    pub evidence_capture_limit_bytes: u64,
    pub truncated: bool,
    pub marker_original_bytes: u64,
    pub marker_payload_bytes: u64,
    pub marker_sha256: String,
    pub external_sha256: String,
    pub call_id: String,
    pub result_call_id: String,
    pub result_in_next_request: bool,
    pub peak_rss_delta_mib: f64,
    pub terminal_success: bool,
    pub oom: bool,
}

/// Row-60 exact metrics, resource value, and typed details.
#[derive(Clone, Debug, PartialEq)]
pub struct LargeOutputEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub peak_rss_delta_mib: Option<f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate row 60 after all streaming/correlation observations have completed.
pub fn evaluate_large_tool_output(
    trials: &[LargeOutputTrial],
    expected_repetitions: u32,
) -> LargeOutputEvaluation {
    let incomplete = |detail: String| LargeOutputEvaluation {
        metrics: BTreeMap::new(),
        peak_rss_delta_mib: None,
        details: json!({"measurement_complete":false,"measurement_error":detail}),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(detail),
    };
    if u32::try_from(trials.len()).ok() != Some(expected_repetitions) {
        return incomplete(format!(
            "large-output evidence has {} trials; expected {expected_repetitions}",
            trials.len()
        ));
    }
    let repetitions = trials
        .iter()
        .map(|trial| trial.repetition)
        .collect::<BTreeSet<_>>();
    if repetitions.len() != trials.len()
        || repetitions.iter().next().copied() != Some(1)
        || repetitions.iter().next_back().copied() != Some(expected_repetitions)
    {
        return incomplete("large-output repetition keys are incomplete or duplicated".to_owned());
    }
    if trials.iter().any(|trial| {
        trial.marker_sha256.len() != 64
            || trial.external_sha256.len() != 64
            || !trial
                .marker_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !trial
                .external_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || trial.call_id.is_empty()
            || trial.result_call_id.is_empty()
            || !trial.peak_rss_delta_mib.is_finite()
            || trial.peak_rss_delta_mib < 0.0
    }) {
        return incomplete(
            "large-output byte, digest, identity, or memory evidence is invalid".to_owned(),
        );
    }
    let first = &trials[0];
    let invariant_counts = trials.iter().all(|trial| {
        trial.produced_bytes == first.produced_bytes
            && trial.model_visible_bytes == first.model_visible_bytes
            && trial.model_visible_encoded_bytes == first.model_visible_encoded_bytes
            && trial.harness_output_limit_bytes == first.harness_output_limit_bytes
            && trial.evidence_captured_bytes == first.evidence_captured_bytes
            && trial.evidence_capture_limit_bytes == first.evidence_capture_limit_bytes
    });
    if !invariant_counts {
        return incomplete("large-output exact byte/limit fields differ across trials".to_owned());
    }
    let peak = trials
        .iter()
        .map(|trial| trial.peak_rss_delta_mib)
        .fold(0.0_f64, f64::max);
    let memory_limit = (4.0 * first.harness_output_limit_bytes as f64 / 1_048_576.0).max(128.0);
    let passed = trials.iter().all(|trial| {
        trial.produced_bytes == 10_485_760
            && trial.truncated
            && trial.model_visible_encoded_bytes <= trial.harness_output_limit_bytes
            && trial.model_visible_bytes == trial.marker_payload_bytes
            && trial.marker_original_bytes == trial.produced_bytes
            && trial.marker_sha256 == trial.external_sha256
            && trial.call_id == trial.result_call_id
            && trial.result_in_next_request
            && trial.evidence_captured_bytes <= trial.evidence_capture_limit_bytes
            && trial.peak_rss_delta_mib <= memory_limit
            && trial.terminal_success
            && !trial.oom
    });
    let metrics = BTreeMap::from([
        (
            "large_tool_output.produced_bytes".to_owned(),
            first.produced_bytes as f64,
        ),
        (
            "large_tool_output.model_visible_bytes".to_owned(),
            first.model_visible_bytes as f64,
        ),
        (
            "large_tool_output.model_visible_encoded_bytes".to_owned(),
            first.model_visible_encoded_bytes as f64,
        ),
        (
            "large_tool_output.harness_output_limit_bytes".to_owned(),
            first.harness_output_limit_bytes as f64,
        ),
        (
            "large_tool_output.evidence_captured_bytes".to_owned(),
            first.evidence_captured_bytes as f64,
        ),
        (
            "large_tool_output.evidence_capture_limit_bytes".to_owned(),
            first.evidence_capture_limit_bytes as f64,
        ),
        (
            "large_tool_output.truncated".to_owned(),
            if trials.iter().all(|trial| trial.truncated) {
                1.0
            } else {
                0.0
            },
        ),
        (
            "large_tool_output.terminal_success".to_owned(),
            if trials.iter().all(|trial| trial.terminal_success) {
                1.0
            } else {
                0.0
            },
        ),
        (
            "large_tool_output.tool_result_correlated".to_owned(),
            if trials
                .iter()
                .all(|trial| trial.call_id == trial.result_call_id && trial.result_in_next_request)
            {
                1.0
            } else {
                0.0
            },
        ),
    ]);
    let detail_trials = trials
        .iter()
        .map(|trial| {
            json!({
                "repetition":trial.repetition,
                "truncation_marker":{
                    "truncated":trial.truncated,
                    "original_bytes":trial.marker_original_bytes,
                    "payload_bytes":trial.marker_payload_bytes,
                    "sha256":trial.marker_sha256,
                },
                "call_id":trial.call_id,
                "result_call_id":trial.result_call_id,
                "peak_rss_delta_mib":trial.peak_rss_delta_mib,
            })
        })
        .collect::<Vec<_>>();
    LargeOutputEvaluation {
        metrics,
        peak_rss_delta_mib: Some(peak),
        details: json!({"measurement_complete":true,"trials":detail_trials,"memory_baseline":"topology-profile-scoped"}),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

/// One row-61 workspace-fault trial from ordinary fixture/result evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkspaceFaultTrial {
    pub repetition: u32,
    pub kind: String,
    pub write_errno: Option<i32>,
    pub control_write_errno: Option<i32>,
    pub structured_failure: bool,
    pub terminal_count: u32,
    pub terminal_ms: f64,
    pub outside_writes: u32,
    pub residue_processes: u32,
    pub target_written: bool,
    pub target_existed_before: bool,
    pub snapshot_complete: bool,
    pub workspace_snapshot_before_sha256: String,
    pub workspace_snapshot_after_sha256: String,
    pub profile_snapshot_before_sha256: String,
    pub profile_snapshot_after_sha256: String,
    pub forbidden_snapshot_before_sha256: String,
    pub forbidden_snapshot_after_sha256: String,
    pub success_contradiction: bool,
    pub crashed: bool,
    pub hung: bool,
}

/// Row-61 exact evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkspaceFaultEvaluation {
    pub metrics: BTreeMap<String, f64>,
    pub details: Value,
    pub measurement_complete: bool,
    pub passed: bool,
    pub measurement_error: Option<String>,
}

/// Evaluate ordinary structured errno results cross-checked against snapshots.
pub fn evaluate_workspace_fault(
    trials: &[WorkspaceFaultTrial],
    expected_repetitions: u32,
    turn_timeout_ms: u64,
) -> WorkspaceFaultEvaluation {
    let incomplete = |detail: String| WorkspaceFaultEvaluation {
        metrics: BTreeMap::new(),
        details: json!({"measurement_complete":false,"measurement_error":detail}),
        measurement_complete: false,
        passed: false,
        measurement_error: Some(detail),
    };
    if u32::try_from(trials.len()).ok() != Some(expected_repetitions) {
        return incomplete("workspace-fault trial set is incomplete".to_owned());
    }
    let allowed_errno = |errno: i32| matches!(errno, libc::EACCES | libc::EROFS | libc::ENOSPC);
    for trial in trials {
        let Some(control_errno) = trial.control_write_errno else {
            return incomplete("workspace-fault control write has no errno boundary".to_owned());
        };
        if !allowed_errno(control_errno) {
            return incomplete(format!(
                "workspace-fault chmod/mount control was ineffective or unrelated (errno={control_errno})"
            ));
        }
        let valid_digest = |digest: &str| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        };
        if !trial.snapshot_complete
            || trial.target_existed_before
            || !valid_digest(&trial.workspace_snapshot_before_sha256)
            || !valid_digest(&trial.workspace_snapshot_after_sha256)
            || !valid_digest(&trial.profile_snapshot_before_sha256)
            || !valid_digest(&trial.profile_snapshot_after_sha256)
            || !valid_digest(&trial.forbidden_snapshot_before_sha256)
            || !valid_digest(&trial.forbidden_snapshot_after_sha256)
        {
            return incomplete(
                "workspace-fault target/profile/forbidden-root snapshot boundary is incomplete"
                    .to_owned(),
            );
        }
        if trial.target_written
            || trial.outside_writes > 0
            || trial.workspace_snapshot_before_sha256 != trial.workspace_snapshot_after_sha256
            || trial.forbidden_snapshot_before_sha256 != trial.forbidden_snapshot_after_sha256
        {
            return incomplete(
                "ordinary fixture errno contradicts target/profile snapshot evidence".to_owned(),
            );
        }
        if !trial.crashed && !trial.hung {
            let Some(write_errno) = trial.write_errno else {
                return incomplete(
                    "ordinary fixture structured result omitted write_errno".to_owned(),
                );
            };
            if !allowed_errno(write_errno) {
                return incomplete(
                    "ordinary fixture structured result returned an unrelated write_errno"
                        .to_owned(),
                );
            }
        }
    }
    let passed = trials.iter().all(|trial| {
        trial.structured_failure
            && trial.terminal_count == 1
            && trial.terminal_ms <= turn_timeout_ms as f64
            && trial.outside_writes == 0
            && trial.residue_processes == 0
            && !trial.target_written
            && !trial.success_contradiction
            && !trial.crashed
            && !trial.hung
    });
    let terminal_ms = trials
        .iter()
        .map(|trial| trial.terminal_ms)
        .fold(0.0_f64, f64::max);
    WorkspaceFaultEvaluation {
        metrics: BTreeMap::from([
            (
                "workspace_fault.structured_failure".to_owned(),
                trials
                    .iter()
                    .filter(|trial| trial.structured_failure)
                    .count() as f64,
            ),
            (
                "workspace_fault.terminal_count".to_owned(),
                trials
                    .iter()
                    .map(|trial| u64::from(trial.terminal_count))
                    .sum::<u64>() as f64,
            ),
            ("workspace_fault.terminal_ms".to_owned(), terminal_ms),
            (
                "workspace_fault.outside_writes".to_owned(),
                trials
                    .iter()
                    .map(|trial| u64::from(trial.outside_writes))
                    .sum::<u64>() as f64,
            ),
            (
                "workspace_fault.residue_processes".to_owned(),
                trials
                    .iter()
                    .map(|trial| u64::from(trial.residue_processes))
                    .sum::<u64>() as f64,
            ),
        ]),
        details: json!({
            "measurement_complete":true,
            "trials":trials.iter().map(|trial|json!({
                "repetition":trial.repetition,
                "kind":trial.kind,
                "write_errno":trial.write_errno,
                "control_write_errno":trial.control_write_errno,
                "target_existed_before":trial.target_existed_before,
                "workspace_snapshot_before_sha256":trial.workspace_snapshot_before_sha256,
                "workspace_snapshot_after_sha256":trial.workspace_snapshot_after_sha256,
                "profile_snapshot_before_sha256":trial.profile_snapshot_before_sha256,
                "profile_snapshot_after_sha256":trial.profile_snapshot_after_sha256,
                "forbidden_snapshot_before_sha256":trial.forbidden_snapshot_before_sha256,
                "forbidden_snapshot_after_sha256":trial.forbidden_snapshot_after_sha256,
            })).collect::<Vec<_>>(),
            "write_errno_source":"ordinary-fixture-structured-result-cross-checked-against-snapshots",
        }),
        measurement_complete: true,
        passed,
        measurement_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(repetition: u32, index: u32, bytes: u64) -> DiskTurnEvidence {
        DiskTurnEvidence {
            repetition,
            turn: index,
            disk_write_bytes: bytes,
            journal_growth_bytes: 128,
            log_growth_bytes: Some(64),
            counter_complete: true,
            file_identity_complete: true,
            retired_accounting_complete: true,
        }
    }

    #[test]
    fn row47_passes_bounded_complete_repetitions() {
        let evidence = (1..=3)
            .flat_map(|repetition| (1..=20).map(move |index| turn(repetition, index, 4_096)))
            .collect::<Vec<_>>();
        let evaluation =
            evaluate_disk_io_per_turn(&evidence, 3, 20, LogEvidenceKind::DeclaredPaths);
        assert!(evaluation.measurement_complete);
        assert!(evaluation.reference_envelope_pass);
        assert_eq!(evaluation.log_growth_bytes_per_turn, Some(64.0));
    }

    #[test]
    fn row47_omitted_logs_and_last_poll_retirement_are_errors() {
        let mut evidence = vec![turn(1, 1, 1)];
        let omitted = evaluate_disk_io_per_turn(&evidence, 1, 1, LogEvidenceKind::Omitted);
        assert!(!omitted.measurement_complete);
        evidence[0].retired_accounting_complete = false;
        let incomplete = evaluate_disk_io_per_turn(&evidence, 1, 1, LogEvidenceKind::DeclaredPaths);
        assert!(!incomplete.measurement_complete);
    }

    #[test]
    fn row47_verified_no_log_is_null_not_zero() {
        let evidence = (1..=4)
            .map(|index| {
                let mut value = turn(1, index, 1);
                value.log_growth_bytes = None;
                value
            })
            .collect::<Vec<_>>();
        let evaluation = evaluate_disk_io_per_turn(&evidence, 1, 4, LogEvidenceKind::VerifiedNoLog);
        assert!(evaluation.measurement_complete);
        assert_eq!(evaluation.log_growth_bytes_per_turn, None);
        assert!(
            !evaluation
                .resource_values
                .contains_key("log_growth_bytes_per_turn")
        );
    }

    #[test]
    fn row47_detects_unbounded_growth() {
        let evidence = (1..=20)
            .map(|index| turn(1, index, u64::from(index) * u64::from(index) * 16_384))
            .collect::<Vec<_>>();
        let evaluation =
            evaluate_disk_io_per_turn(&evidence, 1, 20, LogEvidenceKind::DeclaredPaths);
        assert!(evaluation.measurement_complete);
        assert!(evaluation.unbounded_disk_growth);
        assert!(!evaluation.reference_envelope_pass);
    }

    #[test]
    fn bounded_capture_streams_and_marker_is_not_self_referential() {
        let mut capture = BoundedToolCapture::new(1_048_576);
        for _ in 0..1_280 {
            capture.push(&[b'x'; 8_192]);
        }
        let output = capture.finish(1_048_576);
        assert_eq!(output.original_bytes, 10_485_760);
        assert!(output.encoded.len() <= 1_048_576);
        let marker = String::from_utf8_lossy(&output.encoded);
        assert!(marker.contains("TRUNCATED truncated=true"));
        assert!(!marker.contains("encoded="));
    }

    #[test]
    fn large_output_evaluator_checks_digest_correlation_and_memory() {
        let trial = LargeOutputTrial {
            repetition: 1,
            produced_bytes: 10_485_760,
            model_visible_bytes: 1_000,
            model_visible_encoded_bytes: 1_128,
            harness_output_limit_bytes: 1_048_576,
            evidence_captured_bytes: 1_048_576,
            evidence_capture_limit_bytes: 1_048_576,
            truncated: true,
            marker_original_bytes: 10_485_760,
            marker_payload_bytes: 1_000,
            marker_sha256: "a".repeat(64),
            external_sha256: "a".repeat(64),
            call_id: "call-large".to_owned(),
            result_call_id: "call-large".to_owned(),
            result_in_next_request: true,
            peak_rss_delta_mib: 20.0,
            terminal_success: true,
            oom: false,
        };
        let evaluation = evaluate_large_tool_output(std::slice::from_ref(&trial), 1);
        assert!(evaluation.measurement_complete);
        assert!(evaluation.passed);
        assert!(
            !evaluation
                .metrics
                .contains_key("large_tool_output.peak_rss_delta_mib")
        );

        let mut complete_failure = trial;
        complete_failure.truncated = false;
        let failed = evaluate_large_tool_output(&[complete_failure], 1);
        assert!(failed.measurement_complete);
        assert!(!failed.passed);
        assert_eq!(
            failed.details["trials"][0]["truncation_marker"]["truncated"],
            false
        );
    }

    #[test]
    fn ineffective_read_only_control_is_error_not_fail() {
        let trial = WorkspaceFaultTrial {
            repetition: 1,
            kind: "read-only-directory".to_owned(),
            write_errno: Some(libc::EACCES),
            control_write_errno: None,
            structured_failure: true,
            terminal_count: 1,
            terminal_ms: 10.0,
            outside_writes: 0,
            residue_processes: 0,
            target_written: false,
            target_existed_before: false,
            snapshot_complete: true,
            workspace_snapshot_before_sha256: "0".repeat(64),
            workspace_snapshot_after_sha256: "0".repeat(64),
            profile_snapshot_before_sha256: "1".repeat(64),
            profile_snapshot_after_sha256: "2".repeat(64),
            forbidden_snapshot_before_sha256: "3".repeat(64),
            forbidden_snapshot_after_sha256: "3".repeat(64),
            success_contradiction: false,
            crashed: false,
            hung: false,
        };
        let evaluation = evaluate_workspace_fault(&[trial], 1, 10_000);
        assert!(!evaluation.measurement_complete);
        assert!(!evaluation.passed);
    }

    #[test]
    fn complete_workspace_fault_behavioral_violation_is_fail() {
        let trial = WorkspaceFaultTrial {
            repetition: 1,
            kind: "read-only-directory".to_owned(),
            write_errno: Some(libc::EACCES),
            control_write_errno: Some(libc::EACCES),
            structured_failure: false,
            terminal_count: 1,
            terminal_ms: 10.0,
            outside_writes: 0,
            residue_processes: 0,
            target_written: false,
            target_existed_before: false,
            snapshot_complete: true,
            workspace_snapshot_before_sha256: "0".repeat(64),
            workspace_snapshot_after_sha256: "0".repeat(64),
            profile_snapshot_before_sha256: "1".repeat(64),
            profile_snapshot_after_sha256: "2".repeat(64),
            forbidden_snapshot_before_sha256: "3".repeat(64),
            forbidden_snapshot_after_sha256: "3".repeat(64),
            success_contradiction: false,
            crashed: false,
            hung: false,
        };
        let evaluation = evaluate_workspace_fault(&[trial], 1, 10_000);
        assert!(evaluation.measurement_complete);
        assert!(!evaluation.passed);
        assert!(evaluation.measurement_error.is_none());
    }

    fn paced_frames(headers_ns: u64, count: u32) -> Vec<ModelFrameObservation> {
        (1..=count)
            .map(|ordinal| ModelFrameObservation {
                scenario: "streaming".to_owned(),
                actor: "actor".to_owned(),
                checkpoint: "start".to_owned(),
                attempt: 1,
                ordinal,
                scheduled_ns: headers_ns + u64::from(ordinal) * 1_000_000_000,
                frame_yielded_ns: headers_ns + u64::from(ordinal) * 1_000_000_000 + 1_000_000,
                bytes: 1,
            })
            .collect()
    }

    fn cpu_samples(headers_ns: u64, final_ns: u64, cpu_ns: u64) -> Vec<ModelWaitCpuSample> {
        [
            (headers_ns - 100_000_000, 0),
            (headers_ns + 100_000_000, 0),
            (final_ns - 100_000_000, cpu_ns),
            (final_ns + 100_000_000, cpu_ns),
        ]
        .into_iter()
        .map(|(sample_ns, cpu_ns)| ModelWaitCpuSample {
            sample_started_ns: sample_ns,
            sample_finished_ns: sample_ns,
            cpu_ns,
        })
        .collect()
    }

    #[test]
    fn row48_distinguishes_complete_cpu_failure_from_pacing_error() {
        let headers_ns = 1_000_000_000;
        let frames = paced_frames(headers_ns, 5);
        let final_ns = frames[4].frame_yielded_ns;
        let trial = ModelWaitCpuTrialEvidence {
            repetition: 1,
            response_headers_ns: headers_ns,
            terminal_ns: Some(headers_ns + 5_100_000_000),
            cpu_ns: 10_000_000,
            cpu_samples: cpu_samples(headers_ns, final_ns, 10_000_000),
            frames,
            terminal_count: 1,
            success_terminals: 1,
            outer_kill_used: false,
            outer_kill_ns: None,
            outer_deadline_ms: 10_500,
        };
        let passing = evaluate_model_wait_cpu(std::slice::from_ref(&trial), 1, 5, 2_500);
        assert!(passing.measurement_complete);
        assert!(passing.passed);
        assert_eq!(passing.metrics["model_wait_cpu.bytes_yielded"], 5.0);

        let mut busy = trial.clone();
        busy.cpu_ns = 1_000_000_000;
        busy.cpu_samples = cpu_samples(headers_ns, final_ns, 1_000_000_000);
        let busy = evaluate_model_wait_cpu(&[busy], 1, 5, 2_500);
        assert!(busy.measurement_complete);
        assert!(!busy.passed);

        let mut late = trial;
        late.frames[1].frame_yielded_ns = late.frames[0].frame_yielded_ns + 1_300_000_000;
        let late = evaluate_model_wait_cpu(&[late], 1, 5, 2_500);
        assert!(!late.measurement_complete);
    }

    #[test]
    fn row59_requires_reset_on_byte_success_and_bounded_true_stall() {
        let headers_ns = 10_000_000_000;
        let trial = SlowStreamStallTrialEvidence {
            repetition: 1,
            slow_response_headers_ns: headers_ns,
            slow_terminal_ns: Some(headers_ns + 5_100_000_000),
            slow_frames: paced_frames(headers_ns, 5),
            slow_terminal_count: 1,
            slow_success_terminals: 1,
            slow_idle_timeout_fired: false,
            slow_outer_kill_used: false,
            slow_outer_kill_ns: None,
            slow_outer_deadline_ms: 10_500,
            stall_response_headers_ns: headers_ns,
            stall_terminal_ns: Some(headers_ns + 2_500_000_000),
            stall_bytes_yielded: 0,
            stall_terminal_count: 1,
            stall_failure_terminals: 1,
            stall_outer_kill_used: false,
            stall_outer_kill_ns: None,
            stall_outer_deadline_ms: 5_500,
        };
        let passing = evaluate_slow_stream_vs_stall(std::slice::from_ref(&trial), 1, 5, 2_500);
        assert!(passing.measurement_complete);
        assert!(passing.passed);
        assert_eq!(
            passing.metrics["slow_stream_vs_stall.stall_own_timeout_ms"],
            2_500.0
        );

        let mut early = trial;
        early.stall_terminal_ns = Some(headers_ns + 1_000_000_000);
        let early = evaluate_slow_stream_vs_stall(&[early], 1, 5, 2_500);
        assert!(early.measurement_complete);
        assert!(!early.passed);

        let mut killed = SlowStreamStallTrialEvidence {
            repetition: 1,
            slow_response_headers_ns: headers_ns,
            slow_terminal_ns: Some(headers_ns + 5_100_000_000),
            slow_frames: paced_frames(headers_ns, 5),
            slow_terminal_count: 1,
            slow_success_terminals: 1,
            slow_idle_timeout_fired: false,
            slow_outer_kill_used: false,
            slow_outer_kill_ns: None,
            slow_outer_deadline_ms: 10_500,
            stall_response_headers_ns: headers_ns,
            stall_terminal_ns: None,
            stall_bytes_yielded: 0,
            stall_terminal_count: 0,
            stall_failure_terminals: 0,
            stall_outer_kill_used: true,
            stall_outer_kill_ns: Some(headers_ns + 5_500_000_000),
            stall_outer_deadline_ms: 5_500,
        };
        let killed_evaluation =
            evaluate_slow_stream_vs_stall(std::slice::from_ref(&killed), 1, 5, 2_500);
        assert!(killed_evaluation.measurement_complete);
        assert!(!killed_evaluation.passed);
        assert_eq!(
            killed_evaluation.metrics["slow_stream_vs_stall.stall_outer_kill_used"],
            1.0
        );
        killed.stall_outer_kill_ns = None;
        assert!(!evaluate_slow_stream_vs_stall(&[killed], 1, 5, 2_500).measurement_complete);

        let invalid_idle = evaluate_slow_stream_vs_stall(&[], 1, 5, 1_000);
        assert!(!invalid_idle.measurement_complete);
    }
}
