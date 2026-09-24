//! External matrix clocks. Timing context stays with the executing async task.
use crate::evaluate::TestResult;
use crate::events::{EventVocab, NormalizedEvent};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Default)]
pub(crate) struct Ledger(Arc<Mutex<State>>);
#[derive(Default)]
struct State {
    rows: BTreeMap<u8, Bounds>,
    sessions: BTreeMap<(usize, String), SessionClock>,
}
#[derive(Default)]
struct SessionClock {
    rows: Vec<u8>,
    seen: BTreeSet<String>,
    pending: bool,
}
struct Bounds {
    start: Instant,
    end: Option<Instant>,
    submitted: bool,
    finished: Option<Instant>,
}
#[derive(Clone)]
struct Context {
    ledger: Ledger,
    rows: Vec<u8>,
}
tokio::task_local! { static CONTEXT: Context; }

impl Ledger {
    pub(crate) fn scope<T>(&self, future: impl Future<Output = T>) -> impl Future<Output = T> {
        // Keep the large runner future out of each caller's async state. Poll it
        // on this task so timing attribution and cancellation stay unchanged.
        CONTEXT.scope(
            Context {
                ledger: self.clone(),
                rows: Vec::new(),
            },
            Box::pin(future),
        )
    }
    pub(crate) fn finish(&self, row: u8) {
        if let Some(bounds) = self
            .0
            .lock()
            .expect("row timing ledger poisoned")
            .rows
            .get_mut(&row)
        {
            bounds.finished.get_or_insert_with(Instant::now);
        }
    }

    /// Freeze a row whose public recovery/replay operation has completed.
    ///
    /// Some successful recovery surfaces return a durable replay rather than
    /// a newly minted terminal. The replay is still the operation's real
    /// completion boundary; retire every carried session clock for this row so
    /// unrelated later collectors cannot extend it.
    pub(crate) fn complete(&self, row: u8) {
        let now = Instant::now();
        let mut state = self.0.lock().expect("row timing ledger poisoned");
        for session in state.sessions.values_mut() {
            if session.rows.contains(&row) {
                session.pending = false;
            }
        }
        if let Some(bounds) = state.rows.get_mut(&row) {
            bounds.end = Some(now);
            bounds.finished = Some(now);
        }
    }

    pub(crate) fn apply(&self, results: &mut [TestResult]) {
        let state = self.0.lock().expect("row timing ledger poisoned");
        for result in results {
            let Some(bounds) = state.rows.get(&result.row) else {
                continue;
            };
            let pending = state
                .sessions
                .values()
                .any(|session| session.pending && session.rows.contains(&result.row));
            let end = if pending { bounds.finished } else { bounds.end };
            result.metadata.wall_duration_s = end
                .unwrap_or_else(Instant::now)
                .duration_since(bounds.start)
                .as_secs_f64();
            result.metadata.wall_duration_scope =
                match (bounds.submitted, !pending && bounds.end.is_some()) {
                    (true, true) => "submit-to-terminal",
                    (true, false) => "submit-to-interruption",
                    (false, _) => "operation",
                }
                .into();
        }
    }
}

/// Rows with shared trials deliberately share one first-submit/last-terminal span.
/// Public CLI-only operations use their launch/return span instead.
pub(crate) fn rows<T>(rows: &[u8], future: impl Future<Output = T>) -> impl Future<Output = T> {
    let future = Box::pin(future);
    async move {
        let Ok(mut context) = CONTEXT.try_with(Clone::clone) else {
            return future.await;
        };
        context.rows = rows.to_vec();
        let started = Instant::now();
        {
            let mut state = context.ledger.0.lock().expect("row timing ledger poisoned");
            for row in rows {
                state.rows.entry(*row).or_insert(Bounds {
                    start: started,
                    end: None,
                    submitted: false,
                    finished: None,
                });
            }
        }
        let output = CONTEXT.scope(context.clone(), future).await;
        let ended = Instant::now();
        let mut state = context.ledger.0.lock().expect("row timing ledger poisoned");
        for row in rows {
            if let Some(bounds) = state.rows.get_mut(row) {
                bounds.finished = Some(ended);
                if !bounds.submitted {
                    bounds.end = Some(ended);
                }
            }
        }
        output
    }
}

/// Set attribution for a submit without closing its still-running row clock.
pub(crate) fn with_rows<T>(
    rows: &[u8],
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    let future = Box::pin(future);
    async move {
        let Ok(mut context) = CONTEXT.try_with(Clone::clone) else {
            return future.await;
        };
        context.rows = rows.to_vec();
        CONTEXT.scope(context, future).await
    }
}

pub(crate) fn submitted(driver: usize, session: &str, key: &str) {
    let _ = CONTEXT.try_with(|context| {
        let rows = if context.rows.is_empty() {
            key.strip_prefix("row-")
                .and_then(|rest| rest.split('-').next())
                .and_then(|row| row.parse::<u8>().ok())
                .into_iter()
                .collect()
        } else {
            context.rows.clone()
        };
        if rows.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut state = context.ledger.0.lock().expect("row timing ledger poisoned");
        for row in &rows {
            let bounds = state.rows.entry(*row).or_insert(Bounds {
                start: now,
                end: None,
                submitted: true,
                finished: None,
            });
            if !bounds.submitted {
                bounds.start = now;
                bounds.submitted = true;
            }
            bounds.end = None;
            bounds.finished = None;
        }
        let session = state.sessions.entry((driver, session.into())).or_default();
        session.rows = rows;
        // A fresh public submit may legitimately return the same durable
        // terminal when it is an idempotent retry. Deduplicate repeated polls
        // only within one pending operation, never across submissions.
        session.seen.clear();
        session.pending = true;
    });
}

/// Move session timing ownership to a replacement driver after recovery.
///
/// Driver identity is deliberately part of the key so independent collectors
/// cannot collide. A cold recovery is the exception: it continues the same
/// public session and must carry its pending submit through to replay/terminal.
pub(crate) fn rebind_driver(from: usize, to: usize) {
    if from == to {
        return;
    }
    let _ = CONTEXT.try_with(|context| {
        let mut state = context.ledger.0.lock().expect("row timing ledger poisoned");
        let moved = state
            .sessions
            .keys()
            .filter(|(driver, _)| *driver == from)
            .cloned()
            .collect::<Vec<_>>();
        for key in moved {
            let session_id = key.1.clone();
            let Some(clock) = state.sessions.remove(&key) else {
                continue;
            };
            state.sessions.insert((to, session_id), clock);
        }
    });
}

pub(crate) fn observed(driver: usize, session: &str, events: &[NormalizedEvent]) {
    let _ = CONTEXT.try_with(|context| {
        let mut state = context.ledger.0.lock().expect("row timing ledger poisoned");
        let exact_key = (driver, session.to_owned());
        let key = if state.sessions.contains_key(&exact_key) {
            Some(exact_key)
        } else {
            // A recovered trait object can cross an allocation/erasure boundary
            // between construction and its first replay. Fall back only when
            // the logical session identifies exactly one pending clock; never
            // merge ambiguous independent drivers.
            let mut candidates = state
                .sessions
                .iter()
                .filter(|((_, candidate), clock)| candidate == session && clock.pending)
                .map(|(key, _)| key.clone());
            let candidate = candidates.next();
            if candidates.next().is_none() {
                candidate
            } else {
                None
            }
        };
        let Some(key) = key else {
            return;
        };
        let Some(session) = state.sessions.get_mut(&key) else {
            return;
        };
        let mut terminal = false;
        for event in events {
            if matches!(
                event.event,
                EventVocab::TerminalSuccess
                    | EventVocab::TerminalFailure
                    | EventVocab::TerminalCancelled
                    | EventVocab::TerminalTimeout
            ) {
                terminal |= session.seen.insert(event.id.clone());
            }
        }
        if terminal && session.pending {
            session.pending = false;
            let rows = session.rows.clone();
            let now = Instant::now();
            for row in rows {
                if let Some(bounds) = state.rows.get_mut(&row) {
                    bounds.end = Some(now);
                }
            }
        }
    });
}

#[cfg(test)]
#[test]
fn timing_scopes_do_not_embed_large_workload_futures() {
    fn workload() -> impl Future<Output = ()> {
        let payload = [0_u8; 64 * 1024];
        async move {
            std::hint::black_box(payload);
        }
    }
    let ledger = Ledger::default();
    let rows_to_time = [25, 46];
    // A small wrapper budget prevents repeated inline copies from exhausting
    // ordinary test/runtime stacks as collector state grows.
    assert!(std::mem::size_of_val(&ledger.scope(workload())) < 4096);
    assert!(std::mem::size_of_val(&rows(&rows_to_time, workload())) < 4096);
    assert!(std::mem::size_of_val(&with_rows(&rows_to_time, workload())) < 4096);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluate::{Pillar, TestOutcome, TestResultMetadata};

    fn result(row: u8) -> TestResult {
        TestResult {
            row,
            id: format!("row-{row}"),
            pillar: Pillar::ToolCallCorrectness,
            outcome: TestOutcome::Pass,
            evidence: vec![],
            metadata: TestResultMetadata::default(),
        }
    }

    #[tokio::test]
    async fn main_row_timeout_clock_freezes_and_new_submit_reopens_it() {
        let ledger = Ledger::default();
        ledger
            .scope(async {
                with_rows(&[7], async {
                    submitted(1, "main", "turn");
                })
                .await;
                ledger.finish(7);
                let mut first = [result(7)];
                ledger.apply(&mut first);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                ledger.finish(7);
                let mut later = [result(7)];
                ledger.apply(&mut later);
                assert_eq!(
                    first[0].metadata.wall_duration_s,
                    later[0].metadata.wall_duration_s
                );
                assert_eq!(
                    later[0].metadata.wall_duration_scope,
                    "submit-to-interruption"
                );
                with_rows(&[7], async {
                    submitted(1, "main", "retry");
                })
                .await;
                ledger.apply(&mut later);
                assert!(later[0].metadata.wall_duration_s > first[0].metadata.wall_duration_s);
            })
            .await;
    }

    #[tokio::test]
    async fn interrupted_collector_clock_does_not_include_later_rows() {
        let ledger = Ledger::default();
        ledger
            .scope(async {
                rows(&[53], async {
                    submitted(1, "killed-source", "turn");
                })
                .await;
                let mut first = [result(53)];
                ledger.apply(&mut first);
                assert_eq!(
                    first[0].metadata.wall_duration_scope,
                    "submit-to-interruption"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let mut later = [result(53)];
                ledger.apply(&mut later);
                assert_eq!(
                    first[0].metadata.wall_duration_s,
                    later[0].metadata.wall_duration_s
                );
            })
            .await;
    }

    #[tokio::test]
    async fn row_clock_remains_incomplete_until_every_client_has_a_terminal() {
        let ledger = Ledger::default();
        ledger
            .scope(async {
                rows(&[25], async {
                    submitted(1, "first", "turn");
                    submitted(1, "second", "turn");
                    let terminal = NormalizedEvent {
                        id: "terminal".into(),
                        cursor: 1,
                        session_id: "session".into(),
                        actor: "actor".into(),
                        event: EventVocab::TerminalSuccess,
                        payload: serde_json::json!({}),
                    };
                    observed(1, "first", std::slice::from_ref(&terminal));
                    let mut results = [result(25)];
                    ledger.apply(&mut results);
                    assert_eq!(
                        results[0].metadata.wall_duration_scope,
                        "submit-to-interruption"
                    );
                    observed(1, "second", &[terminal]);
                    ledger.apply(&mut results);
                    assert_eq!(
                        results[0].metadata.wall_duration_scope,
                        "submit-to-terminal"
                    );
                })
                .await;
            })
            .await;
    }

    #[tokio::test]
    async fn row_clocks_include_submit_delay_and_ignore_duplicate_terminal_polls() {
        let ledger = Ledger::default();
        let terminal = NormalizedEvent {
            id: "terminal".into(),
            cursor: 1,
            session_id: "session".into(),
            actor: "actor".into(),
            event: EventVocab::TerminalSuccess,
            payload: serde_json::json!({}),
        };
        ledger
            .scope(async {
                rows(&[1, 2], async {
                    submitted(1, "session", "turn");
                })
                .await;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                observed(1, "session", std::slice::from_ref(&terminal));
                let mut first = [result(1), result(2), result(3)];
                ledger.apply(&mut first);
                assert!(first[0].metadata.wall_duration_s >= 0.020);
                assert_eq!(
                    first[0].metadata.wall_duration_s,
                    first[1].metadata.wall_duration_s
                );
                assert_eq!(first[0].metadata.wall_duration_scope, "submit-to-terminal");
                assert_eq!(first[2].metadata.wall_duration_scope, "not-launched");
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                observed(1, "session", &[terminal]);
                let mut later = [result(1)];
                ledger.apply(&mut later);
                assert_eq!(
                    first[0].metadata.wall_duration_s,
                    later[0].metadata.wall_duration_s
                );
            })
            .await;
    }

    #[tokio::test]
    async fn recovered_driver_and_idempotent_terminal_close_the_original_clock() {
        let ledger = Ledger::default();
        let terminal = NormalizedEvent {
            id: "terminal".into(),
            cursor: 1,
            session_id: "session".into(),
            actor: "actor".into(),
            event: EventVocab::TerminalSuccess,
            payload: serde_json::json!({}),
        };
        ledger
            .scope(async {
                rows(&[35, 40], async {
                    submitted(1, "session", "row-35-turn-1");
                })
                .await;
                rebind_driver(1, 2);
                observed(2, "session", std::slice::from_ref(&terminal));

                with_rows(&[37], async {
                    submitted(2, "session", "row-37-turn-1");
                    observed(2, "session", std::slice::from_ref(&terminal));
                })
                .await;
                ledger.complete(40);

                let mut first = [result(35), result(37), result(40)];
                ledger.apply(&mut first);
                assert!(
                    first
                        .iter()
                        .all(|result| result.metadata.wall_duration_scope == "submit-to-terminal")
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let mut later = [result(35), result(37), result(40)];
                ledger.apply(&mut later);
                for (first, later) in first.iter().zip(later.iter()) {
                    assert_eq!(
                        first.metadata.wall_duration_s,
                        later.metadata.wall_duration_s
                    );
                }
            })
            .await;
    }
}
