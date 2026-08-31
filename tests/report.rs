use ahrb::evaluate::{Assertion, Pillar, TestOutcome, badge_label, certify, classify};
use ahrb::process::Sample;
use ahrb::report::{
    MembershipSample, Report, render_markdown, render_resource_summary, summarize_resources,
};
use std::time::SystemTime;

#[test]
fn markdown_sorts_rows_and_names_outcomes() {
    let mut report = Report {
        schema: 1,
        run_id: "deterministic".to_owned(),
        ..Report::default()
    };
    report.results.push(classify(
        2,
        "single-tool-call",
        Pillar::ToolCallCorrectness,
        Some(true),
        &[Assertion {
            name: "effect".to_owned(),
            passed: true,
            detail: "once".to_owned(),
        }],
        None,
    ));
    let markdown = render_markdown(&report);
    assert!(markdown.contains("| 2 | ToolCallCorrectness | `single-tool-call` | PASS |"));
}

#[test]
fn unsupported_is_never_silently_passed() {
    let result = classify(
        18,
        "native-delegation",
        Pillar::Functionality,
        Some(false),
        &[],
        None,
    );
    assert!(matches!(result.outcome, TestOutcome::Unsupported(_)));
}

#[test]
fn resource_class_is_derived_from_marginal_memory() {
    let manifest = ahrb::manifest::load(std::path::Path::new("adapters/mock/manifest.toml"))
        .expect("load mock manifest");
    let results: Vec<_> = ahrb::scenarios::all()
        .iter()
        .map(|definition| {
            classify(
                definition.row,
                definition.id,
                definition.pillar,
                Some(true),
                &[Assertion {
                    name: "criterion".to_owned(),
                    passed: true,
                    detail: "met".to_owned(),
                }],
                None,
            )
        })
        .collect();
    let badge = certify(&results, &manifest, "macos", 8, 64.0 * 1024.0 * 1024.0);
    assert!(badge.is_some());
    if let Some(badge) = badge {
        assert!(badge_label(&badge).contains("R96"));
    }
}

fn sample(elapsed_ns: u64, memory_mib: u64, cpu_ns: u64) -> Sample {
    let bytes = memory_mib * 1024 * 1024;
    Sample {
        elapsed_ns,
        wall_time: SystemTime::UNIX_EPOCH,
        phase: "external-sample".to_owned(),
        rss_bytes: bytes + 1024,
        pss_bytes: Some(bytes),
        private_bytes: None,
        footprint_bytes: Some(bytes),
        rss_crosscheck_bytes: None,
        cgroup_memory_bytes: None,
        cgroup_peak_bytes: None,
        cpu_ns,
        open_fds: None,
        thread_count: None,
        collection_ns: 1,
        collection_wall_ns: 1,
        processes: Vec::new(),
        process_samples: Vec::new(),
    }
}

#[test]
fn resource_summary_is_pure_post_processing_of_existing_external_evidence() {
    // The function accepts immutable samples, membership records, and already
    // captured wall clocks. It has no driver/harness handle, so surfacing the
    // summary cannot insert synchronous work into a harness turn.
    let samples = [
        sample(1_000_000_000, 10, 1_000_000_000),
        sample(3_000_000_000, 30, 5_000_000_000),
    ];
    let membership = [
        MembershipSample {
            elapsed_ns: 1_000_000_000,
            phase: "external-sample".to_owned(),
            discovery_wall_ns: 1,
            discovery_cpu_ns: 50_000_000,
            lane: 0,
        },
        MembershipSample {
            elapsed_ns: 3_000_000_000,
            phase: "external-sample".to_owned(),
            discovery_wall_ns: 1,
            discovery_cpu_ns: 50_000_000,
            lane: 0,
        },
    ];
    let summary = summarize_resources(
        &samples,
        &membership,
        4,
        &[10_000_000, 30_000_000],
        Some(8.0),
        Some(12.0),
        Some(0.75),
    );

    assert_eq!(summary.peak_rss_mib, 30.0);
    assert_eq!(summary.mean_rss_mib, 20.0);
    assert_eq!(summary.median_rss_mib, 20.0);
    assert_eq!(summary.cpu_total_s, 4.0);
    assert_eq!(summary.cpu_per_turn_ms, 1_000.0);
    assert_eq!(summary.wall_per_turn_ms, 20.0);
    assert_eq!(summary.sampler_overhead_pct, 5.0);
    let line = render_resource_summary(&summary);
    assert!(line.starts_with("resource_summary peak_rss_mib=30.000"));
    assert!(line.contains("idle_rss_mib=8.000"));
    assert!(line.contains("parallel_beta_mib_per_agent=12.000"));
    assert!(line.ends_with("sampler_overhead_pct=5.000"));
}
