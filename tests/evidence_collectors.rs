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
        semantic_ordinal: 1,
        attempt: 1,
        received_ns: 1,
        body_bytes: 32,
        input_tokens: None,
        role: "primary".to_owned(),
        side_channel_kind: None,
        response_status: Some(200),
        response_headers_ns: Some(1),
        response_first_frame_yield_ns: None,
        response_last_frame_yield_ns: None,
        semantic_attempts_total: 1,
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

fn assert_fail(result: TestResult) {
    assert!(
        matches!(result.outcome, TestOutcome::Fail(_)),
        "unexpected outcome: {:?}; evidence={:?}",
        result.outcome,
        result.evidence
    );
}

fn sequential_result(a_result: Value, b_arguments: Value) -> TestResult {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let expected = [
        ExpectedToolCall {
            call_id: "call-a".to_owned(),
            name: "write_fixture".to_owned(),
            arguments: json!({"path":"a.txt","generate_content":"row3-dependency"}),
            dependency_value: None,
        },
        ExpectedToolCall {
            call_id: "call-b".to_owned(),
            name: "read_fixture".to_owned(),
            arguments: json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
            dependency_value: Some(DEPENDENCY.to_owned()),
        },
    ];
    let events = [
        event(
            1,
            EventVocab::ToolCall,
            json!({"call_id":"call-a","name":"write_fixture","arguments":{"path":"a.txt","generate_content":"row3-dependency"}}),
        ),
        event(
            2,
            EventVocab::ToolResult,
            json!({"call_id":"call-a","result":a_result}),
        ),
        event(
            3,
            EventVocab::ToolCall,
            json!({"call_id":"call-b","name":"read_fixture","arguments":b_arguments}),
        ),
        event(
            4,
            EventVocab::ToolResult,
            json!({"call_id":"call-b","result":{"ok":true}}),
        ),
    ];
    let evidence = collect_correctness_functionality(
        3,
        &CollectionInput {
            events: &events,
            expected_tools: &expected,
            ..CollectionInput::default()
        },
    )
    .expect("collect sequential dependency");
    evaluate_row(&manifest(), 3, Some(&evidence))
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
fn sequential_dependency_accepts_decorated_a_output() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let correct_b = || json!({"path":"a.txt","expected_from_a":DEPENDENCY});
    for decorated in [
        json!({"content":format!("{DEPENDENCY}\ncapture:effect-session-id-timestamp-1")}),
        json!({"stdout":format!("tool prefix: {DEPENDENCY}")}),
        json!({"content":{"wrapper":{"output":DEPENDENCY}}}),
    ] {
        assert_pass(sequential_result(decorated, correct_b()));
    }
}

#[test]
fn sequential_dependency_accepts_one_carrier_with_ordered_split_text_parts() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let split = DEPENDENCY.len() / 2;
    assert_pass(sequential_result(
        json!({
            "content":[
                {"type":"text","text":&DEPENDENCY[..split]},
                {"type":"text","text":&DEPENDENCY[split..]}
            ]
        }),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
}

#[test]
fn sequential_dependency_does_not_join_unrelated_result_fields() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let split = DEPENDENCY.len() / 2;
    assert_fail(sequential_result(
        json!({
            "content":{
                "first":&DEPENDENCY[..split],
                "second":&DEPENDENCY[split..]
            }
        }),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
}

#[test]
fn sequential_dependency_accepts_haider_971_capture_shape() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let output = format!(
        "{DEPENDENCY}\n[Capture: 1 bytes retained; at least 0 source bytes unavailable. Page the full secret-redacted capture with task_output({{\"cursor\":0,\"task_id\":\"capture:effect-session-ee3a49576bd05270b5c6037d27fbc0db-1-1789315543033-1\"}}); follow next_cursor until exhausted.]"
    );
    assert_pass(sequential_result(
        json!({
            "artifact":"blake3:e3a019bbb28110e6b007112d52974805e7cd1c605e6935faa62a20e91de7fe68",
            "preview": serde_json::to_string(&json!({"output":output})).expect("serialize preview"),
            "preview_record":{"output":output},
            "truncated":false
        }),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
}

#[test]
fn sequential_dependency_rejects_context_loss_and_unproduced_values() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let produced = json!({"content":format!("prefix {DEPENDENCY} footer")});

    assert_fail(sequential_result(produced.clone(), json!({"path":"a.txt"})));
    assert_fail(sequential_result(
        produced,
        json!({"path":"a.txt","expected_from_a":"AHRB row 3 different value"}),
    ));
    assert_fail(sequential_result(
        json!({"content":"prefix without the produced dependency value"}),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
    assert_fail(sequential_result(
        json!({"content":"AHRB row 3 non-credential nonce 000 001"}),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
}

#[test]
fn sequential_dependency_rejects_a_argument_echo_as_the_result() {
    const DEPENDENCY: &str =
        "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
    let invocation_arguments = json!({
        "path":"a.txt",
        "generate_content":"row3-dependency"
    });
    assert_fail(sequential_result(
        json!({"content":serde_json::to_string(&invocation_arguments).expect("serialize arguments")}),
        json!({"path":"a.txt","expected_from_a":DEPENDENCY}),
    ));
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
        json!({"status":"failure","category":"idle-timeout","elapsed_ms":2000}),
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
