use ahrb::determinism::{
    DeterminismRun, NormalizationContext, evaluate_cross_run_reproducibility,
    evaluate_nondeterministic_fields, normalize_canonical_request,
};
use ahrb::fake_model::{ModelRequest, ModelRequestRecord};
use serde_json::{Value, json};

fn record(
    dialect: &str,
    actor: &str,
    ordinal: u64,
    attempt: u64,
    canonical: Value,
) -> ModelRequestRecord {
    ModelRequestRecord {
        request: ModelRequest {
            dialect: dialect.to_owned(),
            endpoint: "/v1/test".to_owned(),
            model: "fixture-model".to_owned(),
            scenario: "determinism".to_owned(),
            actor: actor.to_owned(),
            checkpoint: "start".to_owned(),
            canonical,
            credential_fingerprint: "redacted".to_owned(),
            stream: false,
        },
        canonical_hash: "ignored-for-pairing".to_owned(),
        attempts: attempt,
        accepted: true,
        semantic_ordinal: ordinal,
        attempt,
        received_ns: 1,
        body_bytes: 1,
        role: "primary".to_owned(),
        side_channel_kind: None,
        response_status: Some(200),
        response_first_frame_yield_ns: Some(2),
        response_last_frame_yield_ns: Some(3),
        semantic_attempts_total: attempt,
    }
}

fn run(run: u32, records: Vec<ModelRequestRecord>) -> DeterminismRun {
    DeterminismRun {
        run,
        records,
        normalization: NormalizationContext::default(),
    }
}

#[test]
fn normalization_uses_exact_pointer_and_string_type_allowlist() {
    let context = NormalizationContext {
        credential: "secret-r1".to_owned(),
        profile_paths: vec!["/owned/profile-r1".to_owned()],
        workspace_paths: vec!["/owned/profile-r1/workspace".to_owned()],
        temporary_paths: vec!["/owned/tmp-r1".to_owned()],
        socket_paths: vec!["/owned/tmp-r1/provider.sock".to_owned()],
        run_markers: vec!["marker-r1".to_owned()],
        execution_id: "execution-r1".to_owned(),
    };
    let normalized = normalize_canonical_request(
        "openai-chat-completions",
        &json!({
            "messages": [{"role":"user", "content":"use /owned/profile-r1/workspace/a marker-r1"}],
            "metadata": {
                "ahrb_credential":"secret-r1",
                "ahrb_execution_id":"execution-r1"
            },
            "nonce":"/owned/profile-r1/workspace/a",
            "credential":"secret-r1"
        }),
        &context,
    );
    assert_eq!(
        normalized.pointer("/messages/0/content"),
        Some(&json!("use <AHRB_WORKSPACE_PATH>/a <AHRB_RUN_MARKER>"))
    );
    assert_eq!(
        normalized.pointer("/metadata/ahrb_credential"),
        Some(&json!("<AHRB_CREDENTIAL>"))
    );
    assert_eq!(
        normalized.pointer("/metadata/ahrb_execution_id"),
        Some(&json!("<AHRB_EXECUTION_ID>"))
    );
    assert_eq!(
        normalized.pointer("/nonce"),
        Some(&json!("/owned/profile-r1/workspace/a"))
    );
    assert_eq!(normalized.pointer("/credential"), Some(&json!("secret-r1")));
}

#[test]
fn row63_pairs_semantically_and_reports_exact_pointer_occurrences() {
    let baseline = vec![
        record(
            "openai-chat-completions",
            "direct",
            1,
            1,
            json!({"model":"fixture-model", "temperature":0, "messages":[{"role":"user","content":"same"}]}),
        ),
        record(
            "openai-chat-completions",
            "tool",
            1,
            1,
            json!({"model":"fixture-model", "messages":[{"role":"assistant","tool_calls":[{"id":"call-a"}]}]}),
        ),
    ];
    let comparison = vec![
        record(
            "openai-chat-completions",
            "tool",
            1,
            1,
            json!({"model":"fixture-model", "messages":[{"role":"assistant","tool_calls":[{"id":"call-b"}]}]}),
        ),
        record(
            "openai-chat-completions",
            "direct",
            1,
            1,
            json!({"model":"fixture-model", "temperature":1, "messages":[{"role":"user","content":"same"}]}),
        ),
    ];
    let evaluation = evaluate_nondeterministic_fields(&[run(1, baseline), run(2, comparison)], 2);
    assert!(evaluation.measurement_complete);
    assert_eq!(evaluation.varying_leaf_occurrences, 2);
    assert_eq!(evaluation.varying_pointer_count, 2);
    assert_eq!(evaluation.varying_critical_field_count, 1);
    assert!(!evaluation.reference_envelope_pass);
    let fields = evaluation.details["varying_fields"]
        .as_array()
        .expect("varying fields array");
    assert_eq!(fields[0]["pointer"], "/messages/0/tool_calls/0/id");
    assert_eq!(fields[0]["comparison_runs"], json!([2]));
    assert_eq!(fields[0]["before_types"], json!(["string"]));
    assert_eq!(fields[0]["after_types"], json!(["string"]));
    assert_eq!(fields[1]["pointer"], "/temperature");
}

#[test]
fn a_wholly_missing_request_is_exactly_one_denominator_occurrence() {
    let evaluation = evaluate_nondeterministic_fields(
        &[
            run(
                1,
                vec![record(
                    "openai-chat-completions",
                    "direct",
                    1,
                    1,
                    json!({"a":1,"b":2,"c":3}),
                )],
            ),
            run(2, Vec::new()),
        ],
        2,
    );
    assert!(evaluation.measurement_complete);
    assert_eq!(evaluation.comparable_leaf_occurrences, 1);
    assert_eq!(evaluation.varying_leaf_occurrences, 1);
    assert_eq!(evaluation.score, 0.0);
    assert_eq!(evaluation.details["varying_fields"][0]["pointer"], "");
    assert_eq!(
        evaluation.details["varying_fields"][0]["after_types"],
        json!(["missing"])
    );
}

#[test]
fn zero_comparable_denominator_is_an_error() {
    let evaluation = evaluate_nondeterministic_fields(&[run(1, Vec::new()), run(2, Vec::new())], 2);
    assert!(!evaluation.measurement_complete);
    assert!(
        evaluation
            .measurement_error
            .as_deref()
            .is_some_and(|detail| detail.contains("denominator is zero"))
    );
}

#[test]
fn responses_call_and_result_ids_use_dialect_specific_critical_pointers() {
    let evaluation = evaluate_nondeterministic_fields(
        &[
            run(
                1,
                vec![record(
                    "openai-responses",
                    "direct",
                    1,
                    1,
                    json!({"output":[{"id":"call-a"}]}),
                )],
            ),
            run(
                2,
                vec![record(
                    "openai-responses",
                    "direct",
                    1,
                    1,
                    json!({"output":[{"id":"call-b"}]}),
                )],
            ),
        ],
        2,
    );
    assert!(evaluation.measurement_complete);
    assert_eq!(evaluation.varying_critical_field_count, 1);
}

#[test]
fn row64_uses_complete_semantic_order_not_record_arrival_order() {
    let first = record(
        "openai-chat-completions",
        "actor-a",
        1,
        1,
        json!({"messages":[{"role":"user","content":"A"}]}),
    );
    let second = record(
        "openai-chat-completions",
        "actor-b",
        1,
        1,
        json!({"messages":[{"role":"user","content":"B"}]}),
    );
    let evaluation = evaluate_cross_run_reproducibility(
        &[
            run(1, vec![second.clone(), first.clone()]),
            run(2, vec![first, second]),
        ],
        2,
    );
    assert!(evaluation.measurement_complete);
    assert!(evaluation.identical);
    assert_eq!(evaluation.request_stream_count, 2);
    assert_eq!(evaluation.attempt_count, 2);
    assert!(evaluation.details["first_difference"].is_null());
    let hashes = evaluation.details["stream_sha256_by_run"]
        .as_object()
        .expect("hash map");
    assert_eq!(hashes["1"], hashes["2"]);

    let mut ordinal_one_before = record(
        "openai-chat-completions",
        "same-actor",
        1,
        1,
        json!({"value":"before-one"}),
    );
    ordinal_one_before.request.checkpoint = "z-late-name".to_owned();
    let mut ordinal_two_before = record(
        "openai-chat-completions",
        "same-actor",
        2,
        1,
        json!({"value":"before-two"}),
    );
    ordinal_two_before.request.checkpoint = "a-early-name".to_owned();
    let mut ordinal_one_after = ordinal_one_before.clone();
    ordinal_one_after.request.canonical = json!({"value":"after-one"});
    let mut ordinal_two_after = ordinal_two_before.clone();
    ordinal_two_after.request.canonical = json!({"value":"after-two"});
    let semantic_order = evaluate_cross_run_reproducibility(
        &[
            run(1, vec![ordinal_two_before, ordinal_one_before]),
            run(2, vec![ordinal_two_after, ordinal_one_after]),
        ],
        2,
    );
    assert_eq!(
        semantic_order.details["first_difference"]["semantic_key"]["semantic_ordinal"],
        1
    );
    assert_eq!(
        semantic_order.details["first_difference"]["semantic_key"]["checkpoint"],
        "z-late-name"
    );
}

#[test]
fn row64_reports_body_and_attempt_multiplicity_differences() {
    let body_difference = evaluate_cross_run_reproducibility(
        &[
            run(
                1,
                vec![record(
                    "openai-chat-completions",
                    "actor-a",
                    1,
                    1,
                    json!({"temperature":0}),
                )],
            ),
            run(
                2,
                vec![record(
                    "openai-chat-completions",
                    "actor-a",
                    1,
                    1,
                    json!({"temperature":1}),
                )],
            ),
        ],
        2,
    );
    assert!(body_difference.measurement_complete);
    assert!(!body_difference.identical);
    assert_eq!(
        body_difference.details["first_difference"]["kind"],
        "canonical-body"
    );

    let attempt_difference = evaluate_cross_run_reproducibility(
        &[
            run(
                1,
                vec![record(
                    "openai-chat-completions",
                    "actor-a",
                    1,
                    1,
                    json!({"same":true}),
                )],
            ),
            run(
                2,
                vec![
                    record(
                        "openai-chat-completions",
                        "actor-a",
                        1,
                        1,
                        json!({"same":true}),
                    ),
                    record(
                        "openai-chat-completions",
                        "actor-a",
                        1,
                        2,
                        json!({"same":true}),
                    ),
                ],
            ),
        ],
        2,
    );
    assert!(attempt_difference.measurement_complete);
    assert!(!attempt_difference.identical);
    assert_eq!(
        attempt_difference.details["first_difference"]["kind"],
        "attempt-multiplicity"
    );
}

#[test]
fn row64_inherits_row63_exact_normalization_allowlist() {
    let mut first = run(
        1,
        vec![record(
            "openai-chat-completions",
            "actor-a",
            1,
            1,
            json!({"messages":[{"role":"user","content":"/profile-one/workspace/file"}]}),
        )],
    );
    first.normalization.profile_paths = vec!["/profile-one".to_owned()];
    let mut second = run(
        2,
        vec![record(
            "openai-chat-completions",
            "actor-a",
            1,
            1,
            json!({"messages":[{"role":"user","content":"/profile-two/workspace/file"}]}),
        )],
    );
    second.normalization.profile_paths = vec!["/profile-two".to_owned()];
    let evaluation = evaluate_cross_run_reproducibility(&[first, second], 2);
    assert!(evaluation.measurement_complete);
    assert!(evaluation.identical);
}
