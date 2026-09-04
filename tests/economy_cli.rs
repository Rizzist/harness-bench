mod common;

use ahrb::economy::{
    CACHE_INPUT_DISCOUNT, CACHE_INPUT_DISCOUNT_LABEL, CACHE_REGIME_LABEL, COST_OUTCOME_LABEL,
    EFFECTIVE_COST_LABEL, EFFECTS_VERIFIED_LABEL, EconomyCompletion, PREFIX_STABILITY_LABEL,
    REFERENCE_TARIFF_USD_PER_MILLION_TOKENS, REFERENCE_TOKENIZER_VERSION,
};
use ahrb::report::Report;
use std::path::{Path, PathBuf};
use std::process::Command;

const ECONOMY_OUTPUT_SHA256: &str =
    "b97fe6d2349a0c3d4e49df9916f57fd83ff8473142eabf5b086f0263e8156375";

fn mock_variant_manifest(root: &Path, id: &str, serve_argument: &str) -> PathBuf {
    let source = std::fs::read_to_string("adapters/mock/manifest.toml")
        .expect("read reference mock manifest");
    let mut rendered = String::new();
    for line in source.lines() {
        let line = if line == "id = \"ahrb-mock\"" {
            format!("id = {id:?}")
        } else if line.contains("target/debug/ahrb-mock-harness\", \"serve\"") {
            let prefix = line
                .strip_suffix(']')
                .expect("mock serve argv is a TOML inline array");
            format!("{prefix}, {serve_argument:?}]")
        } else {
            line.to_owned()
        };
        rendered.push_str(&line);
        rendered.push('\n');
    }
    let path = root.join(format!("{id}.toml"));
    std::fs::write(&path, rendered).expect("write economy mock variant manifest");
    path
}

#[test]
fn reference_mock_pins_all_six_economy_columns() {
    let _guard = common::serialize_ahrb_subprocesses();
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
    assert_eq!(report.filesystem_snapshots.len(), 1);
    let filesystem_snapshot = &report.filesystem_snapshots[0];
    assert_eq!(filesystem_snapshot.boundary, "economy-after");
    assert_eq!(filesystem_snapshot.category, "economy-scripted-effect");
    assert_eq!(filesystem_snapshot.sha256, ECONOMY_OUTPUT_SHA256);
    assert!(
        filesystem_snapshot
            .path_under_profile
            .ends_with("economy-output.txt")
    );
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
    assert_eq!(summary.cost_outcome_label, COST_OUTCOME_LABEL);
    assert!(summary.effects_verified.all_verified);
    assert_eq!(summary.effects_verified.label, EFFECTS_VERIFIED_LABEL);
    assert_eq!(summary.effects_verified.expected.len(), 1);
    assert_eq!(summary.effects_verified.observed.len(), 1);
    let expected = &summary.effects_verified.expected[0];
    assert_eq!(expected.path, "economy-output.txt");
    assert_eq!(expected.content_sha256, ECONOMY_OUTPUT_SHA256);
    assert_eq!(expected.edit_call_id, "economy-edit");
    assert_eq!(expected.read_back_call_id, "economy-verify");
    let observed = &summary.effects_verified.observed[0];
    assert_eq!(observed.path, expected.path);
    assert_eq!(observed.before_content_sha256, None);
    assert_eq!(
        observed.after_content_sha256.as_deref(),
        Some(ECONOMY_OUTPUT_SHA256)
    );
    assert_eq!(
        observed.read_back_content_sha256.as_deref(),
        Some(ECONOMY_OUTPUT_SHA256)
    );
    assert_eq!(observed.edit_observations, 1);
    assert!(observed.edit_reported_success);
    assert!(observed.read_back_path_verified);
    assert_eq!(observed.read_back_observations, 1);
    assert_ne!(
        summary.effects_verified.workspace_receipt_before_sha256,
        summary.effects_verified.workspace_receipt_after_sha256
    );

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
    let _guard = common::serialize_ahrb_subprocesses();
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

    assert_eq!(summary.schema, 4);
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

#[test]
fn reference_mock_pins_schema_four_cache_economics() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = std::env::temp_dir().join(format!(
        "ahrb-economy-cache-integration-{}",
        std::process::id()
    ));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale cache-economy output");
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
        .expect("run cache-economy pillar against reference mock");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read cache-economy report"),
    )
    .expect("parse cache-economy report");
    let summary = report.economy_summary.expect("cache-economy summary");

    assert_eq!(summary.schema, 4);
    assert_eq!(summary.cache_regime, "automatic-prefix");
    assert_eq!(summary.cache_regime_label, CACHE_REGIME_LABEL);
    assert_eq!(summary.cache_input_discount, CACHE_INPUT_DISCOUNT);
    assert_eq!(
        summary.cache_input_discount_label,
        CACHE_INPUT_DISCOUNT_LABEL
    );
    assert_eq!(summary.effective_reference_tokens, 25_518.190_875_538_24);
    assert_eq!(summary.effective_cost_usd, 0.255_181_908_755_382_4);
    assert_eq!(summary.effective_cost_label, EFFECTIVE_COST_LABEL);
    assert_eq!(summary.stable_prefix_preserved_fraction, 1.0);
    assert_eq!(summary.cache_bust_count, 0);
    assert_eq!(summary.invalidated_prefix_tokens, 0);
    assert_eq!(summary.invalidated_prefix_tokens_per_turn, vec![0; 7]);
    assert_eq!(summary.prefix_stability_label, PREFIX_STABILITY_LABEL);

    std::fs::remove_dir_all(output).expect("remove cache-economy output");
}

#[test]
fn terminal_without_workspace_effect_is_not_a_completed_task() {
    let _guard = common::serialize_ahrb_subprocesses();
    let root = std::env::temp_dir().join(format!(
        "ahrb-economy-no-effect-integration-{}",
        std::process::id()
    ));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale no-effect economy output");
    }
    std::fs::create_dir_all(&root).expect("create no-effect economy root");
    let manifest = mock_variant_manifest(
        &root,
        "ahrb-mock-no-fixture-effects",
        "--suppress-fixture-effects",
    );
    let output = root.join("output");
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(["run", "--pillar", "economy", "--manifest"])
        .arg(&manifest)
        .args(["--profile", "quick", "--output"])
        .arg(&output)
        .arg("--no-save")
        .output()
        .expect("run economy pillar against no-effect mock variant");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read no-effect economy report"),
    )
    .expect("parse no-effect economy report");
    let summary = report.economy_summary.expect("no-effect economy summary");

    assert_eq!(summary.completion, EconomyCompletion::TerminalWithoutEffect);
    assert_eq!(summary.model_turns, 8);
    assert_eq!(summary.tokens_per_completed_task, None);
    assert!(!summary.effects_verified.all_verified);
    assert_eq!(summary.effects_verified.expected.len(), 1);
    assert_eq!(summary.effects_verified.observed.len(), 1);
    let observed = &summary.effects_verified.observed[0];
    assert_eq!(observed.before_content_sha256, None);
    assert_eq!(observed.after_content_sha256, None);
    assert_eq!(observed.edit_observations, 1);
    assert!(!observed.edit_reported_success);
    assert!(observed.read_back_path_verified);
    assert_eq!(observed.read_back_content_sha256, None);
    assert_eq!(observed.read_back_observations, 1);
    assert_eq!(
        summary.effects_verified.workspace_receipt_before_sha256,
        summary.effects_verified.workspace_receipt_after_sha256
    );
    assert!(report.filesystem_snapshots.is_empty());
    assert!(
        String::from_utf8_lossy(&command.stdout).contains("completion=terminal-without-effect")
    );

    std::fs::remove_dir_all(root).expect("remove no-effect economy output");
}

#[test]
fn per_invocation_mock_uses_its_declared_effect_workspace() {
    let _guard = common::serialize_ahrb_subprocesses();
    let output = std::env::temp_dir().join(format!(
        "ahrb-economy-mock-exec-integration-{}",
        std::process::id()
    ));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale mock-exec economy output");
    }
    let command = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args([
            "run",
            "--pillar",
            "economy",
            "--manifest",
            "adapters/mock-exec/manifest.toml",
            "--profile",
            "quick",
            "--output",
        ])
        .arg(&output)
        .arg("--no-save")
        .output()
        .expect("run economy pillar against per-invocation mock");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read mock-exec economy report"),
    )
    .expect("parse mock-exec economy report");
    let summary = report.economy_summary.expect("mock-exec economy summary");
    assert_eq!(summary.completion, EconomyCompletion::Completed);
    assert!(summary.effects_verified.all_verified);
    assert_eq!(
        summary.effects_verified.observed[0]
            .after_content_sha256
            .as_deref(),
        Some(ECONOMY_OUTPUT_SHA256)
    );
    assert_eq!(
        summary.tokens_per_completed_task,
        Some(summary.total_reference_tokens)
    );

    std::fs::remove_dir_all(output).expect("remove mock-exec economy output");
}
