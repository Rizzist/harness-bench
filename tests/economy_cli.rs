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
