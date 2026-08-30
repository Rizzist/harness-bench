use ahrb::driver::{Driver, PerInvocationConfig, PerInvocationDriver};
use ahrb::evaluate::TestOutcome;
use ahrb::events::EventVocab;
use ahrb::report::Report;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

#[test]
fn per_invocation_cli_runs_against_fake_model_and_extracts_own_stdout() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = std::env::temp_dir().join(format!("ahrb-exec-transport-{}", std::process::id()));
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("remove stale exec transport output");
    }
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
        .arg("1,2,9,10,16")
        .output()
        .expect("run per-invocation reference CLI");
    assert_eq!(
        result.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Report = serde_json::from_slice(
        &std::fs::read(output.join("report.json")).expect("read exec report"),
    )
    .expect("parse exec report");
    assert_eq!(report.results.len(), 5);
    assert!(
        report
            .results
            .iter()
            .all(|row| matches!(row.outcome, TestOutcome::Pass))
    );
    assert!(report.events.iter().any(|event| {
        event.get("event").and_then(serde_json::Value::as_str) == Some("tool-call")
    }));
    let mut profiles = std::fs::read_dir(&output)
        .expect("read output directory")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("profile-"))
        })
        .collect::<Vec<_>>();
    profiles.sort();
    let profile = profiles.first().expect("fresh exec profile");
    let provider = std::fs::read_to_string(profile.join("config/provider.toml"))
        .expect("generated provider config");
    assert!(provider.contains("ahrb-fake-v1"));
    assert!(provider.contains("credential = 'ahrb-"));
    let session_root = profile.join("ahrb-exec-sessions");
    let maximum_turn_files = std::fs::read_dir(session_root)
        .expect("read persisted exec sessions")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .map(|path| {
            std::fs::read_dir(path)
                .into_iter()
                .flatten()
                .filter_map(std::result::Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.ends_with(".stdout"))
                })
                .count()
        })
        .max()
        .unwrap_or(0);
    assert_eq!(maximum_turn_files, 3, "row 16 must launch three fresh CLIs");
    std::fs::remove_dir_all(output).expect("remove exec transport output");
}

fn direct_driver(profile: &Path, journal_file: bool, delay_ms: u64) -> PerInvocationDriver {
    let manifest = ahrb::manifest::load(Path::new("adapters/mock-exec/manifest.toml"))
        .expect("load per-invocation reference manifest");
    let mut command = vec![
        env!("CARGO_BIN_EXE_ahrb-mock-harness").to_owned(),
        "exec-turn".to_owned(),
        "--state-dir".to_owned(),
        "{{profile}}/state".to_owned(),
        "--marker".to_owned(),
        "{{marker}}".to_owned(),
        "--session-id".to_owned(),
        "{{session_id}}".to_owned(),
        "--prompt".to_owned(),
        "{{prompt}}".to_owned(),
        "--key".to_owned(),
        "{{turn_key}}".to_owned(),
        "--post-output-delay-ms".to_owned(),
        delay_ms.to_string(),
    ];
    let mut events = manifest.events.clone();
    if journal_file {
        command.push("--event-journal".to_owned());
        command.push("{{journal}}".to_owned());
        events.source = "journal-file".to_owned();
        events.path = "{{journal}}".to_owned();
    }
    PerInvocationDriver::new(PerInvocationConfig {
        command: command.clone(),
        resume_command: command,
        release_command: Vec::new(),
        cancel_command: Vec::new(),
        replay_command: Vec::new(),
        environment: BTreeMap::from([("AHRB_MOCK_MODEL".to_owned(), "ahrb-fake-v1".to_owned())]),
        profile_root: profile.to_path_buf(),
        events,
        exit: manifest.exit,
        session_id_pointer: manifest.sessions.id_pointer,
        timeout: Duration::from_secs(5),
        max_output_bytes: 4096,
        gate_launch: false,
    })
}

#[tokio::test]
async fn terminal_is_withheld_until_the_invocation_really_exits() {
    let profile =
        std::env::temp_dir().join(format!("ahrb-exec-terminal-exit-{}", std::process::id()));
    if profile.exists() {
        std::fs::remove_dir_all(&profile).expect("remove stale delayed profile");
    }
    let mut driver = direct_driver(&profile, false, 300);
    driver.start().await.expect("start direct exec driver");
    let session = driver
        .create_session("delayed-terminal")
        .await
        .expect("create delayed session");
    driver
        .submit(&session, "offline prompt", "delayed-turn")
        .await
        .expect("launch delayed invocation");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let early = driver.attach(&session, None).await.expect("early attach");
    assert!(early.iter().all(|event| {
        !matches!(
            event.event,
            EventVocab::TerminalSuccess | EventVocab::TerminalFailure
        )
    }));
    let started = std::time::Instant::now();
    let complete = loop {
        let events = driver
            .attach(&session, None)
            .await
            .expect("completed attach");
        if events.iter().any(|event| {
            matches!(
                event.event,
                EventVocab::TerminalSuccess | EventVocab::TerminalFailure
            )
        }) {
            break events;
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        complete
            .iter()
            .any(|event| event.event == EventVocab::TerminalSuccess)
    );
    driver.shutdown().await.expect("shutdown direct driver");
    std::fs::remove_dir_all(profile).expect("remove delayed profile");
}

#[tokio::test]
async fn journal_file_source_is_extracted_and_persisted() {
    let profile =
        std::env::temp_dir().join(format!("ahrb-exec-journal-source-{}", std::process::id()));
    if profile.exists() {
        std::fs::remove_dir_all(&profile).expect("remove stale journal profile");
    }
    let mut driver = direct_driver(&profile, true, 0);
    driver.start().await.expect("start journal exec driver");
    let session = driver
        .create_session("journal-source")
        .await
        .expect("create journal session");
    driver
        .submit(&session, "offline prompt", "journal-turn")
        .await
        .expect("launch journal invocation");
    let started = std::time::Instant::now();
    let events = loop {
        let events = driver.attach(&session, None).await.expect("journal attach");
        if events.iter().any(|event| {
            matches!(
                event.event,
                EventVocab::TerminalSuccess | EventVocab::TerminalFailure
            )
        }) {
            break events;
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        events
            .iter()
            .any(|event| event.event == EventVocab::TurnAccepted)
    );
    assert!(
        profile
            .join("ahrb-exec-sessions")
            .join(&session.0)
            .join("harness-journal.jsonl")
            .is_file()
    );
    driver.shutdown().await.expect("shutdown journal driver");
    std::fs::remove_dir_all(profile).expect("remove journal profile");
}
