mod common;

use ahrb::fidelity::{FidelityEndReason, FidelitySummary, HarnessExitStatus, WorkspaceState};
use ahrb::report::Report;
use std::path::{Path, PathBuf};
use std::process::Command;

struct FidelityRun {
    summary: FidelitySummary,
    serialized_summary: Vec<u8>,
}

fn run_fidelity(
    manifest: &Path,
    output: &Path,
    profile: &str,
    deadline_secs: Option<u64>,
) -> FidelityRun {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ahrb"));
    command
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .args(["run", "--pillar", "fidelity", "--manifest"])
        .arg(manifest)
        .args(["--profile", profile, "--output"])
        .arg(output)
        .arg("--no-save");
    if let Some(seconds) = deadline_secs {
        command.args(["--deadline", &seconds.to_string()]);
    }
    let command = command
        .output()
        .expect("run fidelity pillar against mock adapter");
    assert!(
        command.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&command.stdout),
        String::from_utf8_lossy(&command.stderr)
    );
    assert!(String::from_utf8_lossy(&command.stdout).contains("fidelity_summary"));
    let report_bytes = std::fs::read(output.join("report.json")).expect("read fidelity report");
    let report_value: serde_json::Value =
        serde_json::from_slice(&report_bytes).expect("parse fidelity report value");
    let serialized_summary = serde_json::to_vec(&report_value["fidelity_summary"])
        .expect("serialize complete fidelity summary block");
    let report: Report = serde_json::from_slice(&report_bytes).expect("parse fidelity report");
    assert!(report.economy_summary.is_none());
    FidelityRun {
        summary: report.fidelity_summary.expect("fidelity summary"),
        serialized_summary,
    }
}

fn mock_variant_manifest(
    root: &Path,
    id: &str,
    serve_arguments: &[&str],
    declared_turn_ceiling: u64,
    internal_cap_exit_codes: &[i32],
) -> PathBuf {
    let source = std::fs::read_to_string("adapters/mock/manifest.toml")
        .expect("read reference mock manifest");
    let arguments = serve_arguments
        .iter()
        .map(|argument| format!(", {argument:?}"))
        .collect::<String>();
    let mut rendered = String::new();
    for line in source.lines() {
        let line = if line == "id = \"ahrb-mock\"" {
            format!("id = {id:?}")
        } else if line.contains("target/debug/ahrb-mock-harness\", \"serve\"") {
            let prefix = line
                .strip_suffix(']')
                .expect("mock serve argv is a TOML inline array");
            format!("{prefix}{arguments}]")
        } else {
            line.to_owned()
        };
        rendered.push_str(&line);
        rendered.push('\n');
    }
    rendered.push_str(&format!(
        "\n[fidelity]\ndeclared_turn_ceiling = {declared_turn_ceiling}\ninternal_cap_exit_codes = {internal_cap_exit_codes:?}\n"
    ));
    let path = root.join(format!("{id}.toml"));
    std::fs::write(&path, rendered).expect("write compacting mock manifest");
    path
}

#[test]
fn reference_mock_is_deterministic_and_compacting_mock_has_a_cliff() {
    let _guard = common::serialize_ahrb_subprocesses();
    let root = std::env::temp_dir().join(format!("fidelity-cli-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale fidelity output");
    }
    std::fs::create_dir_all(&root).expect("create fidelity output root");
    let reference_manifest = Path::new("adapters/mock/manifest.toml");
    let first_run = run_fidelity(
        reference_manifest,
        &root.join("reference-one"),
        "quick",
        None,
    );
    let second_run = run_fidelity(
        reference_manifest,
        &root.join("reference-two"),
        "quick",
        None,
    );
    let first = first_run.summary;
    let second = second_run.summary;

    assert_eq!(
        first_run.serialized_summary, second_run.serialized_summary,
        "complete serialized fidelity block must be byte-identical"
    );
    assert_eq!(first, second);
    assert_eq!(first.schema, 1);
    assert_eq!(first.model_turns, 24);
    assert_eq!(first.turn_budget, 24);
    assert_eq!(first.needles.len(), 4);
    assert!(
        first
            .needles
            .iter()
            .all(|needle| needle.planted_turn == Some(3))
    );
    assert!(
        first
            .needles
            .iter()
            .all(|needle| needle.first_disappeared_turn.is_none() && !needle.ever_reappeared)
    );
    assert_eq!(first.needle_survival_fraction, 1.0);
    assert_eq!(&first.survival_curve[..2], &[0.0; 2]);
    assert_eq!(&first.survival_curve[2..], &[1.0; 22]);
    assert_eq!(first.first_loss_turn, None);
    assert_eq!(first.retained_tool_result_fraction, vec![1.0; 24]);
    assert_eq!(first.end_reason, FidelityEndReason::ReachedScriptedTerminal);
    assert_eq!(first.end_turn, 24);
    assert_eq!(first.harness_exit_status, HarnessExitStatus::Running);
    assert_eq!(first.harness_exit_code, None);
    assert!(!first.internal_cap_detected);
    assert_eq!(first.declared_turn_ceiling, None);
    assert_eq!(first.workspace_state, WorkspaceState::Mutated);
    assert_eq!(
        first.workspace_receipt_before_sha256,
        "e903988c4dfe00f44f302cd0dace2634d2a2d6991d654f1dd44d05a40b539843"
    );
    assert_eq!(
        first.workspace_receipt_after_sha256,
        "daacbccd6e36ea1bec6aca1bb88cc278e437e8ccdc12e107828fa93c8d8831f3"
    );

    let compact_manifest = mock_variant_manifest(
        &root,
        "ahrb-mock-compacting",
        &[
            "--compact-after-turn",
            "12",
            "--retain-recent-tool-results",
            "4",
        ],
        64,
        &[],
    );
    let compact = run_fidelity(&compact_manifest, &root.join("compacting"), "quick", None).summary;
    assert_eq!(compact.model_turns, 24);
    assert_eq!(
        compact.end_reason,
        FidelityEndReason::ReachedScriptedTerminal
    );
    assert_eq!(compact.first_loss_turn, Some(13));
    assert_eq!(compact.needle_survival_fraction, 0.0);
    assert_eq!(&compact.survival_curve[..2], &[0.0; 2]);
    assert_eq!(&compact.survival_curve[2..12], &[1.0; 10]);
    assert_eq!(&compact.survival_curve[12..], &[0.0; 12]);
    assert_eq!(
        compact.retained_tool_result_fraction,
        vec![
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            0.2,
            0.190_476_190_476_190_5,
            0.181_818_181_818_181_85,
            0.173_913_043_478_260_86,
            0.166_666_666_666_666_66,
            0.16,
            0.153_846_153_846_153_83,
            0.148_148_148_148_148_14,
            0.142_857_142_857_142_85,
            0.137_931_034_482_758_62,
            0.133_333_333_333_333_33,
            0.117_647_058_823_529_4,
        ]
    );
    assert_eq!(compact.declared_turn_ceiling, Some(64));
    assert!(!compact.internal_cap_detected);
    assert_eq!(
        compact.workspace_receipt_after_sha256,
        first.workspace_receipt_after_sha256
    );

    let capped_manifest = mock_variant_manifest(
        &root,
        "ahrb-mock-capped",
        &[
            "--model-request-ceiling",
            "12",
            "--model-request-cap-exit-code",
            "23",
        ],
        12,
        &[23],
    );
    let capped = run_fidelity(&capped_manifest, &root.join("capped"), "quick", None).summary;
    assert_eq!(capped.model_turns, 12);
    assert_eq!(capped.end_turn, 12);
    assert_eq!(capped.end_reason, FidelityEndReason::HarnessInternalCeiling);
    assert!(capped.internal_cap_detected);
    assert_eq!(capped.declared_turn_ceiling, Some(12));
    assert_eq!(capped.harness_exit_status, HarnessExitStatus::ExitCode);
    assert_eq!(capped.harness_exit_code, Some(23));
    assert_eq!(capped.workspace_state, WorkspaceState::Mutated);

    let stalled_manifest = mock_variant_manifest(
        &root,
        "ahrb-mock-stalled-cap",
        &[
            "--model-request-ceiling",
            "12",
            "--model-request-cap-stall-ms",
            "10000",
        ],
        12,
        &[],
    );
    let stalled = run_fidelity(
        &stalled_manifest,
        &root.join("stalled-cap"),
        "quick",
        Some(3),
    )
    .summary;
    assert_eq!(stalled.model_turns, 12);
    assert_eq!(
        stalled.end_reason,
        FidelityEndReason::HarnessInternalCeiling
    );
    assert!(stalled.internal_cap_detected);
    assert_eq!(stalled.harness_exit_status, HarnessExitStatus::Running);

    let cert = run_fidelity(reference_manifest, &root.join("cert"), "cert", None).summary;
    assert_eq!(cert.model_turns, 44);
    assert_eq!(cert.turn_budget, 44);
    assert_eq!(cert.end_reason, FidelityEndReason::ReachedScriptedTerminal);
    assert_eq!(cert.needle_survival_fraction, 1.0);

    std::fs::remove_dir_all(root).expect("remove fidelity output");
}
