mod common;

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
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
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
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &result,
        &report_path,
        "exec-transport subprocess did not produce a report",
    );
    let report: Report = serde_json::from_slice(&report_bytes).expect("parse exec report");
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

#[test]
fn per_invocation_mock_executes_request_declared_native_tool_translation() {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::temp_dir().join(format!("ahrb-exec-native-tool-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove stale native-tool run");
    }
    std::fs::create_dir(&root).expect("create native-tool run directory");
    let source = std::fs::read_to_string(repository.join("adapters/mock-exec/manifest.toml"))
        .expect("read mock-exec manifest");
    let native_tools = r#"[tools.aliases]
write = "native_shell"
read = "native_shell"
fail = "native_shell"

[tools.bindings]
command = "command"

[tools.fixtures]
write = ["{{ahrb_fixture}}", "write", "--path", "{{path}}", "--content", "{{content}}"]
read = ["{{ahrb_fixture}}", "read", "--path", "{{path}}"]
fail = ["{{ahrb_fixture}}", "fail", "--message", "{{message}}"]
"#;
    let manifest_source = source.replacen("[tools]\n", native_tools, 1);
    assert_ne!(
        manifest_source, source,
        "mock-exec [tools] section not found"
    );
    let manifest = root.join("manifest.toml");
    std::fs::write(&manifest, manifest_source).expect("write native-tool manifest");
    let output = root.join("output");
    let result = Command::new(env!("CARGO_BIN_EXE_ahrb"))
        .current_dir(repository)
        .arg("run")
        .arg("--manifest")
        .arg(&manifest)
        .arg("--output")
        .arg(&output)
        .arg("--profile")
        .arg("quick")
        .arg("--tests")
        .arg("2,3,6,8")
        .output()
        .expect("run native-tool translation scenario");
    let report_path = output.join("report.json");
    let report_bytes = common::read_ahrb_run_report(
        &result,
        &report_path,
        "native-tool translation scenario did not produce a report",
    );
    let report: Report = serde_json::from_slice(&report_bytes).expect("parse native-tool report");
    assert_eq!(result.status.code(), Some(0));
    assert_eq!(report.results.len(), 4);
    assert!(
        report
            .results
            .iter()
            .all(|row| matches!(row.outcome, TestOutcome::Pass))
    );

    let call = report
        .events
        .iter()
        .find(|event| {
            event.get("event").and_then(serde_json::Value::as_str) == Some("tool-call")
                && event
                    .pointer("/payload/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some("call-r2")
        })
        .expect("native tool-call event");
    assert_eq!(
        call.pointer("/payload/name")
            .and_then(serde_json::Value::as_str),
        Some("write_fixture")
    );
    assert_eq!(
        call.pointer("/payload/native_name")
            .and_then(serde_json::Value::as_str),
        Some("native_shell")
    );
    assert_eq!(
        call.pointer("/payload/arguments/path")
            .and_then(serde_json::Value::as_str),
        Some("row-2.txt")
    );
    let command = call
        .pointer("/payload/native_arguments/command")
        .and_then(serde_json::Value::as_str)
        .expect("native shell command argument");
    assert!(command.contains("ahrb-fixture"));
    assert!(command.contains("row-2.txt"));
    let call_id = call
        .pointer("/payload/call_id")
        .and_then(serde_json::Value::as_str)
        .expect("native call ID");
    let tool_result = report
        .events
        .iter()
        .find(|event| {
            event.get("event").and_then(serde_json::Value::as_str) == Some("tool-result")
                && event
                    .pointer("/payload/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some("call-r2")
        })
        .expect("native tool-result event");
    assert_eq!(
        tool_result
            .pointer("/payload/call_id")
            .and_then(serde_json::Value::as_str),
        Some(call_id)
    );
    assert_eq!(
        tool_result
            .pointer("/payload/result/ok")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    let failed_result = report
        .events
        .iter()
        .find(|event| {
            event.get("event").and_then(serde_json::Value::as_str) == Some("tool-result")
                && event
                    .pointer("/payload/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some("call-fail")
        })
        .expect("native failed tool-result event");
    assert_eq!(
        failed_result
            .pointer("/payload/result/ok")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert_eq!(
        failed_result
            .pointer("/payload/result/exit_code")
            .and_then(serde_json::Value::as_i64),
        Some(1)
    );
    assert_eq!(
        failed_result
            .pointer("/payload/name")
            .and_then(serde_json::Value::as_str),
        Some("fail_fixture")
    );

    let mut profiles = std::fs::read_dir(&output)
        .expect("read native-tool output")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("profile-"))
        })
        .collect::<Vec<_>>();
    profiles.sort();
    let workspaces = profiles
        .first()
        .expect("native-tool profile")
        .join("state/workspaces");
    let effect = std::fs::read_dir(workspaces)
        .expect("read native-tool workspaces")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path().join("row-2.txt"))
        .find(|path| path.is_file())
        .expect("native shell filesystem effect");
    let content = std::fs::read_to_string(effect).expect("read native shell effect");
    assert!(content.contains("row-2"));
    std::fs::remove_dir_all(root).expect("remove native-tool run");
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
        base_variables: BTreeMap::new(),
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
