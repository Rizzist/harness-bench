//! S4/S5 informational oracles. Missing observations never become measured zero.
use super::{StorageConfig, evidence::*};
use crate::{AhrbError, Result, evaluate::TestOutcome};

pub fn compaction_summary(before: u64, after: u64) -> CompactionSummary {
    CompactionSummary {
        compaction_before_allocated_bytes: Some(before as f64),
        compaction_after_allocated_bytes: Some(after as f64),
        compaction_freed_pct: (before != 0)
            .then(|| 100.0 * (before as i128 - after as i128) as f64 / before as f64),
    }
}

pub fn evaluate_close(
    config: &StorageConfig,
    expected_sessions: u64,
    diagnostics: &CloseDiagnostics,
) -> Result<(CloseSummary, TestOutcome)> {
    let bad = |reason: &str| AhrbError::Protocol(format!("S5 incomplete close evidence: {reason}"));
    if expected_sessions == 0 || diagnostics.store_baseline_allocated_bytes.is_none() {
        return Err(bad("missing warm store baseline or session budget"));
    }
    if diagnostics.close_receipts.len() as u64 != expected_sessions {
        return Err(bad("missing public close receipt"));
    }
    let mut identities = std::collections::BTreeSet::new();
    for receipt in &diagnostics.close_receipts {
        let hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if receipt.exit_code != 0
            || !hash(&receipt.receipt_sha256)
            || !hash(&receipt.session_id_hash)
            || !identities.insert(&receipt.session_id_hash)
        {
            return Err(bad("invalid/duplicate close receipt"));
        }
    }
    let immediate = diagnostics
        .checkpoints
        .iter()
        .filter(|c| c.phase == ClosePhase::Immediate)
        .collect::<Vec<_>>();
    let expected = (0..=expected_sessions)
        .filter(|n| n % 10 == 0 || *n == expected_sessions)
        .collect::<Vec<_>>();
    if immediate
        .iter()
        .map(|c| c.closed_sessions)
        .collect::<Vec<_>>()
        != expected
        || immediate
            .first()
            .is_none_or(|c| c.retained_allocated_bytes != 0)
        || diagnostics
            .checkpoints
            .iter()
            .any(|c| !c.elapsed_s.is_finite() || c.elapsed_s < 0.0)
    {
        return Err(bad("missing, unordered or invalid checkpoint"));
    }
    let post = diagnostics
        .checkpoints
        .iter()
        .filter(|c| c.phase == ClosePhase::PostSweep)
        .collect::<Vec<_>>();
    match config.sweep_interval_s {
        Some(interval)
            if post.len() != 1
                || post[0].closed_sessions != expected_sessions
                || post[0].elapsed_s < interval as f64 =>
        {
            return Err(bad("missing/early post-sweep checkpoint"));
        }
        None if !post.is_empty() => return Err(bad("undeclared post-sweep measurement")),
        _ => {}
    }
    let class = config.retention_cap_bytes.map(|cap| {
        let bounded = if config.sweep_interval_s.is_some() {
            post[0].retained_allocated_bytes <= cap
        } else {
            immediate.iter().all(|c| c.retained_allocated_bytes <= cap)
        };
        if bounded {
            BoundClass::Bounded
        } else {
            BoundClass::Unbounded
        }
    });
    let summary = CloseSummary {
        closed_sessions: Some(expected_sessions),
        close_retained_bytes_per_session: immediate
            .last()
            .map(|c| c.retained_allocated_bytes as f64 / expected_sessions as f64),
        close_retained_after_sweep_bytes_per_session: post
            .first()
            .map(|c| c.retained_allocated_bytes as f64 / expected_sessions as f64),
        retention_cap_bytes: config.retention_cap_bytes,
        close_retention_class: class,
    };
    let outcome = if class.is_some() {
        TestOutcome::Pass
    } else {
        TestOutcome::Unsupported("no declared retention cap; byte observations retained".into())
    };
    Ok((summary, outcome))
}

/// General storage precedence; informational classes do not affect outcomes.
pub fn aggregate_outcome(outcomes: impl IntoIterator<Item = TestOutcome>) -> TestOutcome {
    let rank = |o: &TestOutcome| match o {
        TestOutcome::Error(_) => 5,
        TestOutcome::Absent(_) => 4,
        TestOutcome::Unsupported(_) => 3,
        TestOutcome::Fail(_) => 2,
        TestOutcome::Pass => 1,
    };
    outcomes
        .into_iter()
        .max_by_key(rank)
        .unwrap_or_else(|| TestOutcome::Error("missing repetition evidence".into()))
}
