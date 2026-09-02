use ahrb::economy::{
    EconomyCompletion, REFERENCE_TARIFF_USD_PER_MILLION_TOKENS, REFERENCE_TOKENIZER_VERSION,
};
use ahrb::report::Report;
use std::path::Path;
use std::process::Command;

#[test]
fn reference_mock_pins_all_six_economy_columns() {
    let output =
        std::env::temp_dir().join(format!("ahrb-economy-integration-{}", std::process::id()));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale economy output");
    }
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--pillar",
            "economy",
            "--manifest",
            "adapters/mock/manifest.toml",
            "--profile",
            "quick",
            "--output",
        ])
        .arg(&output)
        .arg("--no-save")
        .output()
        .expect("run economy pillar against reference mock");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read economy report"),
    )
    .expect("parse economy report");
    let summary = report.economy_summary.expect("economy summary");

    // The six MVP columns: turns; total reference tokens; calls/batching;
    // peak/last context; scripted completion; reference cost.
    assert_eq!(summary.model_turns, 8);
    assert_eq!(summary.total_reference_tokens, 96_426);
    assert_eq!(summary.tool_calls, 17);
    assert_eq!(summary.tool_batching_factor, 17.0 / 7.0);
    assert_eq!(summary.last_context_size_tokens, 17_132);
    assert_eq!(summary.completion, EconomyCompletion::Completed);
    assert_eq!(summary.reference_cost_usd, 0.964_26);
    assert_eq!(summary.tokens_per_completed_task, Some(96_426));

    assert_eq!(
        summary.reference_tariff_usd_per_million_tokens,
        REFERENCE_TARIFF_USD_PER_MILLION_TOKENS
    );
    assert_eq!(
        summary.reference_tokenizer.version,
        REFERENCE_TOKENIZER_VERSION
    );
    assert_eq!(
        summary.reference_tokenizer.vocabulary_sha256,
        "bca069c1ed9a057d9adfd579bcaefeac940a799d27a7fd427eb83eac778fc826"
    );
    assert!(String::from_utf8_lossy(&command.stdout).contains("economy_summary"));
    std::fs::remove_dir_all(output).expect("remove economy output");
}

#[test]
fn reference_mock_pins_all_five_v3_full_economy_columns() {
    let output = std::env::temp_dir().join(format!(
        "ahrb-economy-v3-full-integration-{}",
        std::process::id()
    ));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale v3-full economy output");
    }
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--pillar",
            "economy",
            "--manifest",
            "adapters/mock/manifest.toml",
            "--profile",
            "quick",
            "--output",
        ])
        .arg(&output)
        .arg("--no-save")
        .output()
        .expect("run v3-full economy pillar against reference mock");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read v3-full economy report"),
    )
    .expect("parse v3-full economy report");
    let summary = report.economy_summary.expect("v3-full economy summary");

    assert_eq!(summary.schema, 2);
    assert_eq!(summary.cache_eligible_fraction, 0.817_066_502_631_399_1);
    assert_eq!(summary.redundant_tokens, 71_626);
    assert_eq!(
        summary.context_token_curve,
        vec![1_249, 6_422, 11_360, 14_331, 14_712, 15_108, 16_112, 17_132]
    );
    assert_eq!(summary.context_token_curve_slope, 1_730.5);
    assert!(summary.context_token_curve_last_matches_last_context_size);
    assert_eq!(summary.per_turn_fixed_overhead_tokens, 1_049);
    assert_eq!(summary.wasted_tool_call_count, 0);

    assert_eq!(summary.cache_control_breakpoints, 0);
    assert_eq!(summary.cache_control_breakpoints_per_request, vec![0; 8]);
    assert_eq!(summary.retry_attempts, 0);
    assert_eq!(summary.retry_reference_tokens, 0);
    assert!(
        String::from_utf8_lossy(&command.stdout)
            .contains("cache-eligible fraction (prefix upper bound)")
    );
    std::fs::remove_dir_all(output).expect("remove v3-full economy output");
}
