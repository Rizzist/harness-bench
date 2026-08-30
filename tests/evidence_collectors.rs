use ahrb::evaluate::{TestOutcome, TestResult};
use ahrb::events::{EventVocab, NormalizedEvent};
use ahrb::evidence_collectors::{
    CollectionInput, ExitObservation, ExpectedToolCall, FileObservation, RoutingExpectation,
    TimingObservation, collect_automation, collect_correctness_functionality,
};
use ahrb::fake_model::{ModelRequest, ModelRequestRecord};
use ahrb::matrix_evidence::{ObservationSet, evaluate_row};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn manifest() -> ahrb::manifest::Manifest {
    ahrb::manifest::load(std::path::Path::new("adapters/mock/manifest.toml"))
        .expect("mock manifest")
}

fn event(cursor: u64, event: EventVocab, payload: Value) -> NormalizedEvent {
    NormalizedEvent {
        id: format!("session:{cursor}"),
        cursor,
        session_id: "session".to_owned(),
        actor: "root".to_owned(),
        event,
        payload,
    }
}

fn model_record(actor: &str) -> ModelRequestRecord {
    ModelRequestRecord {
        request: ModelRequest {
            dialect: "openai-chat-completions".to_owned(),
            endpoint: "/v1/chat/completions".to_owned(),
            model: "ahrb-fake-v1".to_owned(),
            scenario: "matrix".to_owned(),
            actor: actor.to_owned(),
            checkpoint: "start".to_owned(),
            canonical: json!({"model":"ahrb-fake-v1"}),
            credential_fingerprint: "credential-fingerprint".to_owned(),
            stream: false,
        },
        canonical_hash: format!("hash-{actor}"),
        attempts: 1,
        accepted: true,
    }
}

fn assert_pass(result: TestResult) {
    assert!(
        matches!(result.outcome, TestOutcome::Pass),
        "unexpected outcome: {:?}; evidence={:?}",
        result.outcome,
        result.evidence
    );
}

#[test]
fn collects_priority_row_1_from_real_model_records() {
    let records = [model_record("primary"), model_record("child")];
    let expected = RoutingExpectation {
        roles: BTreeSet::from(["primary".to_owned(), "child".to_owned()]),
        model: "ahrb-fake-v1".to_owned(),
        dialect: "openai-chat-completions".to_owned(),
        credential_fingerprint: Some("credential-fingerprint".to_owned()),
        secret: Some("raw-secret-never-recorded".to_owned()),
    };
    let supplemental = ObservationSet::new().with_bool("unexpected_egress", false);
    let input = CollectionInput {
        model_requests: &records,
        routing: Some(&expected),
        supplemental: Some(&supplemental),
        ..CollectionInput::default()
    };
    let evidence = collect_correctness_functionality(1, &input).expect("collect routing");
    assert_pass(evaluate_row(&manifest(), 1, Some(&evidence)));
}

#[test]
fn collects_priority_rows_2_and_3_with_exact_correlations() {
    let args = json!({"path":"one.txt","content":"one"});
    let calls = [ExpectedToolCall {
        call_id: "call-one".to_owned(),
        name: "write_fixture".to_owned(),
        arguments: args.clone(),
        dependency_value: None,
    }];
    let files = [FileObservation {
        path: "one.txt".to_owned(),
        actor: "root".to_owned(),
        call_id: "call-one".to_owned(),
        sha256: "same-hash".to_owned(),
        expected_sha256: "same-hash".to_owned(),
        writes: 1,
        outside_declared_roots: false,
    }];
    let events = [
        event(
            1,
            EventVocab::ToolCall,
            json!({"call_id":"call-one","name":"write_fixture","arguments":args}),
        ),
        event(
            2,
            EventVocab::ToolResult,
            json!({"call_id":"call-one","result":{"ok":true}}),
        ),
        event(3, EventVocab::TerminalSuccess, json!({"status":"success"})),
    ];
    let empty = ObservationSet::new();
    let input = CollectionInput {
        events: &events,
        expected_tools: &calls,
        files: &files,
        supplemental: Some(&empty),
        ..CollectionInput::default()
    };
    let evidence = collect_correctness_functionality(2, &input).expect("collect single tool");
    assert_pass(evaluate_row(&manifest(), 2, Some(&evidence)));

    let sequential_calls = [
        ExpectedToolCall {
            call_id: "call-a".to_owned(),
            name: "write_fixture".to_owned(),
            arguments: json!({"path":"a.txt","content":"A"}),
            dependency_value: None,
        },
        ExpectedToolCall {
            call_id: "call-b".to_owned(),
            name: "write_fixture".to_owned(),
            arguments: json!({"path":"b.txt","content":"A-output"}),
            dependency_value: Some("A-output".to_owned()),
        },
    ];
    let sequential_events = [
        event(
            1,
            EventVocab::ToolCall,
            json!({"call_id":"call-a","name":"write_fixture","arguments":{"path":"a.txt","content":"A"}}),
        ),
        event(
            2,
            EventVocab::ToolResult,
            json!({"call_id":"call-a","result":{"content":"A-output"}}),
        ),
        event(
            3,
            EventVocab::ToolCall,
            json!({"call_id":"call-b","name":"write_fixture","arguments":{"path":"b.txt","content":"A-output"}}),
        ),
        event(
            4,
            EventVocab::ToolResult,
            json!({"call_id":"call-b","result":{"ok":true}}),
        ),
        event(5, EventVocab::TerminalSuccess, json!({"status":"success"})),
    ];
    let sequential_input = CollectionInput {
        events: &sequential_events,
        expected_tools: &sequential_calls,
        supplemental: Some(&empty),
        ..CollectionInput::default()
    };
    let evidence =
        collect_correctness_functionality(3, &sequential_input).expect("collect sequential");
    assert_pass(evaluate_row(&manifest(), 3, Some(&evidence)));
}

#[test]
fn collects_priority_terminal_rows_9_and_10_from_events_and_os_exits() {
    let empty = ObservationSet::new();
    let success_events = [event(
        1,
        EventVocab::TerminalSuccess,
        json!({"status":"success"}),
    )];
    let success_exit = [ExitObservation {
        category: "success".to_owned(),
        code: 0,
        expected_code: 0,
        structured_terminal: true,
    }];
    let success_input = CollectionInput {
        events: &success_events,
        exits: &success_exit,
        supplemental: Some(&empty),
        ..CollectionInput::default()
    };
    let evidence =
        collect_correctness_functionality(9, &success_input).expect("collect terminal success");
    assert_pass(evaluate_row(&manifest(), 9, Some(&evidence)));

    let failure_events = [event(
        1,
        EventVocab::TerminalFailure,
        json!({"status":"failure"}),
    )];
    let failure_exit = [ExitObservation {
        category: "provider".to_owned(),
        code: 14,
        expected_code: 14,
        structured_terminal: true,
    }];
    let failure_input = CollectionInput {
        events: &failure_events,
        exits: &failure_exit,
        supplemental: Some(&empty),
        ..CollectionInput::default()
    };
    let evidence =
        collect_correctness_functionality(10, &failure_input).expect("collect terminal failure");
    assert_pass(evaluate_row(&manifest(), 10, Some(&evidence)));
}

#[test]
fn collects_priority_row_12_from_owned_timeout_and_boundaries() {
    let events = [event(
        1,
        EventVocab::TerminalFailure,
        json!({"status":"failure","category":"idle-timeout"}),
    )];
    let timings = [
        TimingObservation {
            name: "idle-deadline".to_owned(),
            elapsed_ms: 110,
            limit_ms: 120,
        },
        TimingObservation {
            name: "outer-deadline".to_owned(),
            elapsed_ms: 110,
            limit_ms: 2_000,
        },
    ];
    let empty = ObservationSet::new();
    let input = CollectionInput {
        events: &events,
        timings: &timings,
        supplemental: Some(&empty),
        ..CollectionInput::default()
    };
    let evidence = collect_correctness_functionality(12, &input).expect("collect idle deadline");
    assert_pass(evaluate_row(&manifest(), 12, Some(&evidence)));
}

#[test]
fn collects_priority_automation_rows_30_35_and_40() {
    let replay_events = [
        event(1, EventVocab::TurnAccepted, json!({"key":"a"})),
        event(2, EventVocab::TerminalSuccess, json!({"status":"success"})),
        event(3, EventVocab::TurnAccepted, json!({"key":"b"})),
        event(4, EventVocab::TerminalSuccess, json!({"status":"success"})),
    ];
    let replay_supplement = ObservationSet::new().with_bool("suffix_exact", true);
    let replay_input = CollectionInput {
        events: &replay_events,
        supplemental: Some(&replay_supplement),
        ..CollectionInput::default()
    };
    let evidence = collect_automation(30, &replay_input).expect("collect replay");
    assert_pass(evaluate_row(&manifest(), 30, Some(&evidence)));

    let crash_events = [
        event(1, EventVocab::TurnAccepted, json!({"key":"crash"})),
        event(2, EventVocab::TerminalSuccess, json!({"status":"success"})),
    ];
    let crash_timing = [TimingObservation {
        name: "crash-readiness".to_owned(),
        elapsed_ms: 250,
        limit_ms: 10_000,
    }];
    let crash_supplement = ObservationSet::new()
        .with_bool("sigkill_used", true)
        .with_bool("named_checkpoint", true);
    let crash_input = CollectionInput {
        events: &crash_events,
        timings: &crash_timing,
        supplemental: Some(&crash_supplement),
        ..CollectionInput::default()
    };
    let evidence = collect_automation(35, &crash_input).expect("collect crash");
    assert_pass(evaluate_row(&manifest(), 35, Some(&evidence)));

    let journal_supplement = ObservationSet::new()
        .with_bool("sigkill_used", true)
        .with_bool("named_post_commit_checkpoint", true)
        .with_bool("suffix_exact", true)
        .with_bool("tail_integrity", true)
        .with_bool("journal_replay_agree", true);
    let journal_input = CollectionInput {
        events: &replay_events,
        supplemental: Some(&journal_supplement),
        ..CollectionInput::default()
    };
    let evidence = collect_automation(40, &journal_input).expect("collect journal");
    assert_pass(evaluate_row(&manifest(), 40, Some(&evidence)));
}

#[test]
fn omitted_nonterminal_semantics_fail_even_with_a_success_terminal() {
    let events = [event(
        1,
        EventVocab::TerminalSuccess,
        json!({"status":"success"}),
    )];
    let empty = ObservationSet::new();
    for row in [2_u8, 3, 8, 12] {
        let input = CollectionInput {
            events: &events,
            supplemental: Some(&empty),
            ..CollectionInput::default()
        };
        let evidence =
            collect_correctness_functionality(row, &input).expect("collect incomplete row");
        let result = evaluate_row(&manifest(), row, Some(&evidence));
        assert!(
            matches!(result.outcome, TestOutcome::Fail(_)),
            "row {row} passed terminal-only evidence"
        );
    }

    for row in [30_u8, 35, 40] {
        let input = CollectionInput {
            events: &events,
            supplemental: Some(&empty),
            ..CollectionInput::default()
        };
        let evidence = collect_automation(row, &input).expect("collect incomplete automation");
        let result = evaluate_row(&manifest(), row, Some(&evidence));
        assert!(
            matches!(result.outcome, TestOutcome::Fail(_)),
            "row {row} passed terminal-only evidence"
        );
    }
}
