use ahrb::driver::{Driver, PerInvocationConfig, PerInvocationDriver};
use ahrb::evaluate::TestOutcome;
use ahrb::evaluate::{Assertion, certify, classify};
use ahrb::events::EventVocab;
use ahrb::process::Sampler;
use ahrb::report::Report;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

fn run_directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ahrb-topology-resources-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

#[cfg(target_os = "macos")]
fn platform_sampler() -> Box<dyn Sampler> {
    Box::new(ahrb::process::macos::MacOsSampler::default())
}

#[cfg(target_os = "linux")]
fn platform_sampler() -> Box<dyn Sampler> {
    Box::new(ahrb::process::linux::LinuxSampler::default())
}

#[test]
fn per_invocation_resource_rows_measure_process_fanout_without_idle_penalty() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = run_directory();
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(repository.join("adapters/mock-exec/manifest.toml"))
        .arg("--output")
        .arg(&output)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg("20,21,22,23,24,25,26,27,28,29")
        .output()
        .expect("run per-invocation resource certification");
    assert_eq!(
        result.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read resource report"),
    )
    .expect("parse resource report");
    assert_eq!(report.results.len(), 10);
    assert!(
        report
            .results
            .iter()
            .all(|row| matches!(row.outcome, TestOutcome::Pass))
    );
    assert!(!report.resource_metrics.is_empty());
    assert!(report.resource_metrics.values().all(|metric| {
        metric.topology == "client-process-fanout"
            && metric.comparison_scope == "within-topology-only"
    }));
    assert!(
        report
            .resource_metrics
            .keys()
            .all(|name| !report.metrics.contains_key(name))
    );
    let markdown = std::fs::read_to_string(output.join("report.md")).expect("read markdown");
    assert!(markdown.contains("Resource metrics — `client-process-fanout`"));
    assert!(markdown.contains("comparable only within the same topology"));
    assert_eq!(
        report
            .resource_metrics
            .get("idle_median_bytes")
            .map(|metric| metric.value),
        Some(0.0)
    );
    assert!(
        report
            .resource_metrics
            .get("parallel_beta_bytes_per_agent")
            .is_some_and(|metric| metric.value > 0.0)
    );
    assert!(
        report.samples.iter().any(|sample| {
            sample.phase.contains("per-invocation") && !sample.processes.is_empty()
        })
    );
    for row in [28_u8, 29] {
        assert!(report.results.iter().any(|result| {
            result.row == row
                && result
                    .evidence
                    .iter()
                    .any(|item| item.contains("automatic PASS"))
        }));
    }
    std::fs::remove_dir_all(output).expect("remove resource output");
}

#[test]
fn per_invocation_badge_uses_process_marginal_and_labels_topology() {
    let manifest = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load exec manifest");
    let results = ahrb::scenarios::all()
        .iter()
        .map(|definition| {
            if matches!(
                definition.requirement(),
                ahrb::scenarios::RequirementKind::OptionalFacet { .. }
            ) {
                classify(
                    definition.row,
                    definition.id,
                    definition.pillar,
                    Some(false),
                    &[],
                    None,
                )
            } else {
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
            }
        })
        .collect::<Vec<_>>();
    let badge = certify(&results, &manifest, "macos", 4, 20.0 * 1024.0 * 1024.0)
        .expect("complete per-invocation result earns a badge");
    assert_eq!(badge.topology, "client-process-fanout");
    assert_eq!(badge.resource_class, "R32");
    assert_eq!(badge.comparison_scope, "within-topology-only");
    assert_eq!(badge.facets, vec!["replay", "crash", "resume"]);
}

#[tokio::test]
async fn launch_gate_and_process_group_retain_an_orphan_after_launcher_exit() {
    let profile = run_directory();
    std::fs::create_dir_all(&profile).expect("create orphan profile");
    let manifest = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load exec manifest");
    let script = concat!(
        "sleep 2 & ",
        "printf '%s\\n' '",
        "{\"id\":\"orphan-terminal\",\"cursor\":1,",
        "\"session_id\":\"external\",\"actor\":\"orphan\",",
        "\"type\":\"terminal-success\",\"payload\":{\"status\":\"success\"}}'"
    );
    let mut driver = PerInvocationDriver::new(PerInvocationConfig {
        command: vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            "ahrb-orphan".to_owned(),
            "{{prompt}}".to_owned(),
        ],
        resume_command: Vec::new(),
        release_command: Vec::new(),
        cancel_command: Vec::new(),
        replay_command: Vec::new(),
        environment: BTreeMap::new(),
        base_variables: BTreeMap::new(),
        profile_root: profile.clone(),
        events: manifest.events,
        exit: manifest.exit,
        session_id_pointer: String::new(),
        timeout: Duration::from_secs(3),
        max_output_bytes: 4096,
        gate_launch: true,
    });
    driver.start().await.expect("start gated driver");
    let session = driver
        .create_session("orphan-probe")
        .await
        .expect("create orphan session");
    driver
        .submit(&session, "orphan prompt", "orphan-turn")
        .await
        .expect("launch gated invocation");
    let roots = driver.owned_pids();
    assert_eq!(roots.len(), 1);
    let mut sampler = platform_sampler();
    let armed = sampler
        .discover(&roots)
        .expect("arm process-group ownership");
    assert!(!armed.members.is_empty());
    driver
        .release_invocations()
        .await
        .expect("release invocation");

    let started = Instant::now();
    loop {
        let events = driver
            .attach(&session, None)
            .await
            .expect("attach invocation");
        if events
            .iter()
            .any(|event| event.event == EventVocab::TerminalSuccess)
        {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
    let residual = sampler
        .discover(&roots)
        .expect("discover reparented process-group member");
    assert!(
        residual
            .members
            .keys()
            .any(|identity| !roots.contains(&identity.pid)),
        "background child escaped whole-tree membership"
    );
    for identity in residual.members.keys() {
        let pid = i32::try_from(identity.pid).expect("pid_t range");
        // SAFETY: these are freshly sampled members of the test's isolated
        // process group and are terminated only to avoid leaking the fixture.
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
    }
    tokio::time::sleep(Duration::from_millis(25)).await;
    std::fs::remove_dir_all(profile).expect("remove orphan profile");
}
