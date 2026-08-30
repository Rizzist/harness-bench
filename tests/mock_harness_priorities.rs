mod common;

use ahrb::driver::{Driver, GenericDriver, StdinRpcTransport};
use ahrb::events::{EventVocab, NormalizedEvent};
#[cfg(unix)]
use ahrb::fake_model::FakeModelUnixServer;
use ahrb::fake_model::{FakeModelEngine, FakeModelServer, ModelRequestRecord};
use ahrb::process::Sampler;
use ahrb::sampler::{MemoryMetric, SampleSeries, SweepMetrics, SweepPoint};
use ahrb::workflow::{Actor, Fault, ScriptedResponse, WORKFLOW_SCHEMA_VERSION, Workflow};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct RunEvidence {
    events: Vec<NormalizedEvent>,
    requests: Vec<ModelRequestRecord>,
    state_dir: PathBuf,
    elapsed: Duration,
}

enum LocalModelServer {
    Tcp(FakeModelServer),
    #[cfg(unix)]
    Unix(FakeModelUnixServer),
    Embedded(PathBuf),
}

impl LocalModelServer {
    async fn bind(
        state_dir: &std::path::Path,
        workflow: &Workflow,
        engine: Arc<FakeModelEngine>,
    ) -> Self {
        if loopback_is_explicitly_disabled() {
            let path = state_dir.join("embedded-workflow.json");
            std::fs::write(
                &path,
                serde_json::to_vec(workflow).expect("serialize embedded workflow"),
            )
            .expect("write embedded workflow");
            return Self::Embedded(path);
        }
        if !loopback_is_explicitly_disabled() {
            match FakeModelServer::bind(
                "127.0.0.1:0".parse().expect("loopback socket"),
                Arc::clone(&engine),
            )
            .await
            {
                Ok(server) => return Self::Tcp(server),
                Err(ahrb::AhrbError::Io(error))
                    if error.kind() == std::io::ErrorKind::PermissionDenied => {}
                Err(error) => panic!("start fake model: {error}"),
            }
        }
        #[cfg(unix)]
        {
            Self::Unix(
                FakeModelUnixServer::bind(state_dir.join("model.sock"), engine)
                    .await
                    .expect("start Unix fake model"),
            )
        }
        #[cfg(not(unix))]
        panic!("loopback is unavailable and Unix sockets are not supported");
    }

    fn environment(&self) -> (String, String) {
        match self {
            Self::Tcp(server) => ("AHRB_MOCK_BASE_URL".to_owned(), server.base_url()),
            #[cfg(unix)]
            Self::Unix(server) => (
                "AHRB_MOCK_UNIX_SOCKET".to_owned(),
                server.socket_path().to_string_lossy().into_owned(),
            ),
            Self::Embedded(path) => (
                "AHRB_MOCK_EMBEDDED_WORKFLOW".to_owned(),
                path.to_string_lossy().into_owned(),
            ),
        }
    }

    async fn shutdown(self) {
        match self {
            Self::Tcp(server) => server.shutdown().await.expect("shutdown TCP fake model"),
            #[cfg(unix)]
            Self::Unix(server) => server.shutdown().await.expect("shutdown Unix fake model"),
            Self::Embedded(_) => {}
        }
    }
}

fn temporary_directory(label: &str) -> PathBuf {
    let nonce = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ahrb-mock-matrix-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn marker(scenario: &str, checkpoint: &str) -> String {
    format!("[[AHRB:scenario={scenario};actor=root;checkpoint={checkpoint}]]")
}

fn workflow(scenario: &str, checkpoints: Vec<(&str, Value, Option<Fault>)>) -> Workflow {
    let initial = checkpoints.first().map_or("start", |item| item.0);
    Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.to_owned(),
        actors: BTreeMap::from([(
            "root".to_owned(),
            Actor {
                id: "root".to_owned(),
                parent: None,
                prompt: format!("exercise {scenario} {}", marker(scenario, initial)),
                workspace: "root".to_owned(),
            },
        )]),
        barriers: BTreeMap::new(),
        responses: checkpoints
            .into_iter()
            .map(|(checkpoint, response, fault)| ScriptedResponse {
                scenario: scenario.to_owned(),
                actor: "root".to_owned(),
                checkpoint: checkpoint.to_owned(),
                request_hash: String::new(),
                response,
                fault,
                barrier: None,
            })
            .collect(),
    }
}

async fn run(workflow: Workflow, idle_timeout_ms: u64) -> RunEvidence {
    let _subprocess_guard = common::serialize_ahrb_subprocesses();
    let state_dir = temporary_directory(&workflow.scenario);
    std::fs::create_dir_all(&state_dir).expect("create isolated mock state");
    let engine = Arc::new(FakeModelEngine::new(&workflow).expect("valid test workflow"));
    let server = LocalModelServer::bind(&state_dir, &workflow, Arc::clone(&engine)).await;
    let command = vec![
        env!("CARGO_BIN_EXE_ahrb-mock-harness").to_owned(),
        "serve".to_owned(),
        "--state-dir".to_owned(),
        state_dir.to_string_lossy().into_owned(),
        "--idle-timeout-ms".to_owned(),
        idle_timeout_ms.to_string(),
    ];
    let model_endpoint = server.environment();
    let environment = BTreeMap::from([
        model_endpoint,
        ("AHRB_MOCK_API_KEY".to_owned(), "matrix-secret".to_owned()),
        ("AHRB_MOCK_MODEL".to_owned(), "ahrb-fake-v1".to_owned()),
    ]);
    let transport =
        StdinRpcTransport::new(command, Duration::from_secs(4)).with_environment(environment);
    let mut driver = GenericDriver::new(transport);
    driver.start().await.expect("start mock transport");
    let actor = workflow.actors.get("root").expect("root actor");
    let session = driver
        .create_session(&format!("{}-root", workflow.scenario))
        .await
        .expect("create session");
    let started = Instant::now();
    driver
        .submit(&session, &actor.prompt, "turn-1")
        .await
        .expect("submit turn");
    let events = loop {
        let events = driver.attach(&session, None).await.expect("attach events");
        if events.iter().any(|event| {
            matches!(
                event.event,
                EventVocab::TerminalSuccess
                    | EventVocab::TerminalFailure
                    | EventVocab::TerminalCancelled
            )
        }) {
            break events;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "turn did not terminalize"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let elapsed = started.elapsed();
    driver.shutdown().await.expect("shutdown mock harness");
    let requests = engine.request_records().await;
    server.shutdown().await;
    RunEvidence {
        events,
        requests,
        state_dir,
        elapsed,
    }
}

fn events_of(events: &[NormalizedEvent], vocab: EventVocab) -> Vec<&NormalizedEvent> {
    events.iter().filter(|event| event.event == vocab).collect()
}

fn cleanup(path: &PathBuf) {
    std::fs::remove_dir_all(path).expect("remove isolated mock state");
}

fn loopback_is_explicitly_disabled() -> bool {
    std::env::var("CODEX_SANDBOX_NETWORK_DISABLED").as_deref() == Ok("1")
}

struct RpcProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    sequence: u64,
    state_dir: PathBuf,
}

impl RpcProcess {
    async fn spawn(label: &str) -> Self {
        let state_dir = temporary_directory(label);
        std::fs::create_dir_all(&state_dir).expect("create mock state");
        Self::spawn_with_state(state_dir).await
    }

    async fn spawn_with_state(state_dir: PathBuf) -> Self {
        Self::spawn_with_state_and_environment(state_dir, BTreeMap::new()).await
    }

    async fn spawn_with_state_and_environment(
        state_dir: PathBuf,
        environment: BTreeMap<String, String>,
    ) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ahrb-mock-harness"));
        command
            .arg("serve")
            .arg("--state-dir")
            .arg(&state_dir)
            .envs(environment)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().expect("spawn mock harness");
        let stdin = child.stdin.take().expect("mock stdin");
        let stdout = child.stdout.take().expect("mock stdout");
        Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            sequence: 0,
            state_dir,
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.sequence += 1;
        let id = self.sequence.to_string();
        let mut bytes = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))
        .expect("serialize RPC");
        bytes.push(b'\n');
        self.stdin.write_all(&bytes).await.expect("write RPC");
        self.stdin.flush().await.expect("flush RPC");
        let line = tokio::time::timeout(Duration::from_secs(2), self.stdout.next_line())
            .await
            .expect("RPC deadline")
            .expect("read RPC")
            .expect("RPC response line");
        let response: Value = serde_json::from_str(&line).expect("parse RPC response");
        assert_eq!(response["id"], id);
        assert!(response.get("error").is_none(), "RPC error: {response}");
        response["result"].clone()
    }

    fn pid(&self) -> u32 {
        self.child.id().expect("live mock PID")
    }

    async fn kill(mut self) -> PathBuf {
        self.child.kill().await.expect("kill mock harness");
        let _status = self.child.wait().await.expect("reap killed mock harness");
        self.state_dir.clone()
    }

    async fn shutdown(mut self) -> PathBuf {
        let _ = self.rpc("harness.shutdown", json!({})).await;
        drop(self.stdin);
        let _status = self.child.wait().await.expect("reap mock harness");
        self.state_dir.clone()
    }
}

async fn attach_events(
    process: &mut RpcProcess,
    session_id: &str,
    after: Option<u64>,
) -> Vec<NormalizedEvent> {
    let result = process
        .rpc(
            "session.attach",
            json!({"session_id": session_id, "after": after}),
        )
        .await;
    serde_json::from_value(result["events"].clone()).expect("normalized attach events")
}

#[tokio::test]
async fn priority_rows_1_9_and_10_route_and_terminalize_structurally() {
    let success = run(
        workflow(
            "routing-success",
            vec![("start", json!({"text": "{\"status\":\"SUCCESS\"}"}), None)],
        ),
        500,
    )
    .await;

    if let Some(record) = success.requests.first() {
        let request = &record.request;
        assert_eq!(request.model, "ahrb-fake-v1");
        assert_eq!(request.dialect, "openai-chat-completions");
        assert_eq!(request.scenario, "routing-success");
        assert_eq!(request.actor, "root");
        assert_eq!(request.checkpoint, "start");
        assert_ne!(request.credential_fingerprint, "absent");
        assert_eq!(record.attempts, 1);
        assert!(record.accepted);
    } else {
        let routed = events_of(&success.events, EventVocab::ModelRequest);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].payload["model"], "ahrb-fake-v1");
        assert_eq!(routed[0].payload["endpoint"], "/v1/chat/completions");
    }
    assert_eq!(
        events_of(&success.events, EventVocab::TerminalSuccess).len(),
        1
    );
    assert!(events_of(&success.events, EventVocab::TerminalFailure).is_empty());
    cleanup(&success.state_dir);

    let failure = run(
        workflow(
            "structured-failure",
            vec![(
                "start",
                json!({"text": "{\"status\":\"FAILURE\",\"reason\":\"fixture\"}"}),
                None,
            )],
        ),
        500,
    )
    .await;
    assert_eq!(
        events_of(&failure.events, EventVocab::TerminalFailure).len(),
        1
    );
    assert!(events_of(&failure.events, EventVocab::TerminalSuccess).is_empty());
    let terminal = events_of(&failure.events, EventVocab::TerminalFailure)[0];
    assert_eq!(terminal.payload["status"], "failure");
    cleanup(&failure.state_dir);
}

#[tokio::test]
async fn priority_row_2_executes_one_correlated_fixture_effect() {
    let next = marker("single-tool", "terminal");
    let evidence = run(
        workflow(
            "single-tool",
            vec![
                (
                    "start",
                    json!({"tool_calls": [{
                        "id": "call-single",
                        "name": "write_fixture",
                        "arguments": {"path": "one.txt", "content": format!("one{next}")}
                    }]}),
                    None,
                ),
                (
                    "terminal",
                    json!({"text": "{\"status\":\"SUCCESS\"}"}),
                    None,
                ),
            ],
        ),
        500,
    )
    .await;
    let calls = events_of(&evidence.events, EventVocab::ToolCall);
    let results = events_of(&evidence.events, EventVocab::ToolResult);
    assert_eq!(calls.len(), 1);
    assert_eq!(results.len(), 1);
    assert_eq!(calls[0].payload["call_id"], "call-single");
    assert_eq!(results[0].payload["call_id"], "call-single");
    assert_eq!(calls[0].payload["arguments"]["path"], "one.txt");
    assert_eq!(results[0].payload["result"]["ok"], true);
    let workspace_file = evidence
        .state_dir
        .join("workspaces")
        .join(&calls[0].session_id)
        .join("one.txt");
    assert_eq!(
        std::fs::read_to_string(workspace_file).expect("fixture effect"),
        format!("one{next}")
    );
    assert_eq!(
        events_of(&evidence.events, EventVocab::TerminalSuccess).len(),
        1
    );
    cleanup(&evidence.state_dir);
}

#[tokio::test]
async fn priority_row_3_preserves_dependent_sequential_tool_order() {
    let second = marker("sequential-tools", "second");
    let terminal = marker("sequential-tools", "terminal");
    let evidence = run(
        workflow(
            "sequential-tools",
            vec![
                (
                    "start",
                    json!({"tool_calls": [{
                        "id": "call-a",
                        "name": "write_fixture",
                        "arguments": {"path": "a.txt", "content": "A", "route": second}
                    }]}),
                    None,
                ),
                (
                    "second",
                    json!({"tool_calls": [{
                        "id": "call-b",
                        "name": "read_fixture",
                        "arguments": {"path": "a.txt", "expected_from_a": "A", "route": terminal}
                    }]}),
                    None,
                ),
                (
                    "terminal",
                    json!({"text": "{\"status\":\"SUCCESS\"}"}),
                    None,
                ),
            ],
        ),
        500,
    )
    .await;
    let calls = events_of(&evidence.events, EventVocab::ToolCall);
    let results = events_of(&evidence.events, EventVocab::ToolResult);
    assert_eq!(calls.len(), 2);
    assert_eq!(results.len(), 2);
    assert_eq!(calls[0].payload["call_id"], "call-a");
    assert_eq!(results[0].payload["call_id"], "call-a");
    assert_eq!(calls[1].payload["call_id"], "call-b");
    assert_eq!(results[1].payload["call_id"], "call-b");
    assert!(calls[0].cursor < results[0].cursor);
    assert!(results[0].cursor < calls[1].cursor);
    assert!(calls[1].cursor < results[1].cursor);
    assert!(
        results[1].payload["result"]["content"]
            .as_str()
            .is_some_and(|content| content.starts_with('A'))
    );
    cleanup(&evidence.state_dir);
}

#[tokio::test]
async fn priority_row_12_idle_deadline_is_owned_and_structured() {
    let evidence = run(
        workflow(
            "idle-deadline",
            vec![(
                "start",
                json!({"text": "never emitted"}),
                Some(Fault::Stall),
            )],
        ),
        120,
    )
    .await;
    let failures = events_of(&evidence.events, EventVocab::TerminalFailure);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].payload["category"], "idle-timeout");
    assert!(evidence.elapsed >= Duration::from_millis(100));
    assert!(
        evidence.elapsed < Duration::from_secs(2),
        "outer supervisor was not needed"
    );
    assert!(evidence.requests.iter().all(|record| record.accepted));
    cleanup(&evidence.state_dir);
}

#[tokio::test]
async fn priority_row_30_persists_and_replays_only_the_cursor_suffix() {
    let mut first = RpcProcess::spawn("session-replay").await;
    let created = first
        .rpc("session.create", json!({"marker": "row-30-root"}))
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("session ID")
        .to_owned();
    first
        .rpc(
            "session.submit",
            json!({"session_id": session_id, "prompt": "turn A", "key": "turn-a"}),
        )
        .await;
    let first_events = loop {
        let events = attach_events(&mut first, &session_id, None).await;
        if events
            .iter()
            .any(|event| event.event == EventVocab::TerminalSuccess)
        {
            break events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let cursor_k = first_events[0].cursor;
    let state_dir = first.shutdown().await;

    let mut second = RpcProcess::spawn_with_state(state_dir.clone()).await;
    let recreated = second
        .rpc("session.create", json!({"marker": "row-30-root"}))
        .await;
    assert_eq!(recreated["session_id"], session_id);
    let suffix = attach_events(&mut second, &session_id, Some(cursor_k)).await;
    assert_eq!(suffix.len(), first_events.len() - 1);
    assert!(suffix.iter().all(|event| event.cursor > cursor_k));
    assert!(
        suffix
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    let suffix_ids: BTreeSet<_> = suffix.iter().map(|event| &event.id).collect();
    assert_eq!(suffix_ids.len(), suffix.len(), "replay duplicated an event");
    second
        .rpc(
            "session.submit",
            json!({"session_id": session_id, "prompt": "turn B", "key": "turn-b"}),
        )
        .await;
    let all = loop {
        let events = attach_events(&mut second, &session_id, None).await;
        if events
            .iter()
            .filter(|event| event.event == EventVocab::TerminalSuccess)
            .count()
            == 2
        {
            break events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(events_of(&all, EventVocab::TurnAccepted).len(), 2);
    assert_eq!(events_of(&all, EventVocab::TerminalSuccess).len(), 2);
    let state_dir = second.shutdown().await;
    cleanup(&state_dir);
}

#[tokio::test]
async fn priority_row_35_recovers_a_post_accept_crash_idempotently() {
    let mut crashed = RpcProcess::spawn("crash-recovery").await;
    let created = crashed
        .rpc("session.create", json!({"marker": "row-35-root"}))
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("session ID")
        .to_owned();
    crashed
        .rpc(
            "session.submit",
            json!({
                "session_id": session_id,
                "prompt": "accepted before crash",
                "key": "crash-key"
            }),
        )
        .await;
    let state_dir = crashed.kill().await;

    let recovery_started = Instant::now();
    let mut recovered = RpcProcess::spawn_with_state(state_dir.clone()).await;
    let before_resume = attach_events(&mut recovered, &session_id, None).await;
    assert_eq!(events_of(&before_resume, EventVocab::TurnAccepted).len(), 1);
    recovered
        .rpc("session.resume", json!({"session_id": session_id}))
        .await;
    recovered
        .rpc(
            "session.submit",
            json!({
                "session_id": session_id,
                "prompt": "duplicate transport submit",
                "key": "crash-key"
            }),
        )
        .await;
    let events = loop {
        let events = attach_events(&mut recovered, &session_id, None).await;
        if events.iter().any(|event| is_terminal_vocab(&event.event)) {
            break events;
        }
        assert!(recovery_started.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(events_of(&events, EventVocab::TurnAccepted).len(), 1);
    assert_eq!(events_of(&events, EventVocab::TerminalSuccess).len(), 1);
    assert!(recovery_started.elapsed() < Duration::from_secs(10));
    let state_dir = recovered.shutdown().await;
    cleanup(&state_dir);
}

#[tokio::test]
async fn priority_row_40_journal_survives_kill_and_ignores_a_torn_tail() {
    use std::io::Write as _;

    let mut crashed = RpcProcess::spawn("durable-journal").await;
    let created = crashed
        .rpc("session.create", json!({"marker": "row-40-root"}))
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("session ID")
        .to_owned();
    crashed
        .rpc(
            "session.submit",
            json!({"session_id": session_id, "prompt": "commit me", "key": "durable-a"}),
        )
        .await;
    let committed = attach_events(&mut crashed, &session_id, None).await;
    assert!(!committed.is_empty());
    let cursor_k = committed[0].cursor;
    let state_dir = crashed.kill().await;

    let journal_path = state_dir
        .join("sessions")
        .join(&session_id)
        .join("journal.jsonl");
    let before = std::fs::read(&journal_path).expect("read durable journal");
    assert!(before.ends_with(b"\n"), "committed record was not complete");
    let mut journal = std::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .expect("open journal tail");
    journal
        .write_all(br#"{"id":"torn","cursor":999"#)
        .expect("write simulated torn record");
    journal.sync_all().expect("persist simulated torn tail");

    let mut recovered = RpcProcess::spawn_with_state(state_dir.clone()).await;
    let suffix = attach_events(&mut recovered, &session_id, Some(cursor_k)).await;
    let expected: Vec<_> = committed
        .iter()
        .filter(|event| event.cursor > cursor_k)
        .cloned()
        .collect();
    assert_eq!(
        suffix.iter().map(|event| &event.id).collect::<Vec<_>>(),
        expected.iter().map(|event| &event.id).collect::<Vec<_>>()
    );
    assert!(suffix.iter().all(|event| event.id != "torn"));
    assert!(
        suffix
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    let state_dir = recovered.shutdown().await;
    cleanup(&state_dir);
}

#[tokio::test]
async fn functional_and_control_surfaces_emit_ordered_normalized_evidence() {
    let mut process = RpcProcess::spawn("control-surfaces").await;
    let parent = process
        .rpc("session.create", json!({"marker": "row-15-parent-token"}))
        .await["session_id"]
        .as_str()
        .expect("parent session")
        .to_owned();
    let child = process
        .rpc(
            "agent.spawn",
            json!({
                "parent_session_id": parent,
                "marker": "row-18-child-token",
                "prompt": "child headless turn"
            }),
        )
        .await["session_id"]
        .as_str()
        .expect("child session")
        .to_owned();
    assert_ne!(parent, child, "row 17 actor namespaces must be isolated");

    process
        .rpc(
            "session.steer",
            json!({"session_id": parent, "prompt": "row-31-safe-boundary"}),
        )
        .await;
    process
        .rpc(
            "session.subturn",
            json!({"session_id": parent, "prompt": "row-32-before-tool"}),
        )
        .await;
    process
        .rpc(
            "session.queue",
            json!({"session_id": parent, "prompt": "row-33-turn-b", "key": "turn-b"}),
        )
        .await;
    process
        .rpc(
            "session.submit",
            json!({"session_id": parent, "prompt": "row-15-turn-a", "key": "turn-a"}),
        )
        .await;
    // Row 37: a duplicated semantic transport request must remain idempotent.
    process
        .rpc(
            "session.submit",
            json!({"session_id": parent, "prompt": "duplicate", "key": "turn-a"}),
        )
        .await;

    let parent_events = loop {
        let events = attach_events(&mut process, &parent, None).await;
        if events_of(&events, EventVocab::TerminalSuccess).len() == 2 {
            break events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let child_events = loop {
        let events = attach_events(&mut process, &child, None).await;
        if events
            .iter()
            .any(|event| event.event == EventVocab::TerminalSuccess)
        {
            break events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    assert_eq!(events_of(&parent_events, EventVocab::AgentSpawned).len(), 1);
    let inputs = events_of(&parent_events, EventVocab::InputAccepted);
    assert_eq!(inputs.len(), 3);
    assert_eq!(inputs[0].payload["phase"], "steer");
    assert_eq!(inputs[1].payload["phase"], "subturn");
    assert_eq!(inputs[2].payload["phase"], "queue");
    let accepted = events_of(&parent_events, EventVocab::TurnAccepted);
    let terminals = events_of(&parent_events, EventVocab::TerminalSuccess);
    assert_eq!(accepted.len(), 2, "A and queued B are distinct turns");
    assert_eq!(terminals.len(), 2);
    assert!(accepted[0].cursor < terminals[0].cursor);
    assert!(terminals[0].cursor < accepted[1].cursor);
    assert_eq!(accepted[0].payload["key"], "turn-a");
    assert_eq!(accepted[1].payload["key"], "turn-b");
    assert!(
        parent_events
            .iter()
            .all(|event| event.actor == "row-15-parent-token")
    );
    assert!(
        child_events
            .iter()
            .all(|event| event.actor == "row-18-child-token")
    );
    assert_eq!(events_of(&child_events, EventVocab::TurnAccepted).len(), 1);
    assert_eq!(
        events_of(&child_events, EventVocab::TerminalSuccess).len(),
        1
    );

    let cancel_session = process
        .rpc("session.create", json!({"marker": "row-36-cancel"}))
        .await["session_id"]
        .as_str()
        .expect("cancel session")
        .to_owned();
    process
        .rpc("session.cancel", json!({"session_id": cancel_session}))
        .await;
    let cancelled = attach_events(&mut process, &cancel_session, None).await;
    assert_eq!(
        events_of(&cancelled, EventVocab::TerminalCancelled).len(),
        1
    );

    let state_dir = process.shutdown().await;
    cleanup(&state_dir);
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[tokio::test]
async fn persistent_daemon_holds_n_sessions_eventfully_and_reclaims() {
    use ahrb::workflow::Barrier;

    const AGENTS: usize = 8;
    const MIB: u64 = 1024 * 1024;
    const RESERVATION_BYTES: u64 = 4 * MIB;

    let state_dir = temporary_directory("one-daemon-resource-surface");
    std::fs::create_dir_all(&state_dir).expect("create resource state");
    let scenario = "one-daemon-resource-surface";
    let marker = |actor: &str, checkpoint: &str| {
        format!("[[AHRB:scenario={scenario};actor={actor};checkpoint={checkpoint}]]")
    };
    let mut actors = BTreeMap::new();
    let mut responses = Vec::new();
    for index in 1..=AGENTS {
        let actor = format!("agent-{index}");
        actors.insert(
            actor.clone(),
            Actor {
                id: actor.clone(),
                parent: None,
                prompt: marker(&actor, "start"),
                workspace: actor.clone(),
            },
        );
        responses.push(ScriptedResponse {
            scenario: scenario.to_owned(),
            actor: actor.clone(),
            checkpoint: "start".to_owned(),
            request_hash: String::new(),
            response: json!({
                "tool_calls": [{
                    "id": format!("resource-call-{index}"),
                    "name": "write_fixture",
                    "arguments": {
                        "path": "resource.txt",
                        "content": format!("agent-{index} {}", marker(&actor, "terminal")),
                        "ahrb_checkpoint": {
                            "name": "resource-steady",
                            "phase": "after-commit"
                        }
                    }
                }]
            }),
            fault: None,
            barrier: None,
        });
        responses.push(ScriptedResponse {
            scenario: scenario.to_owned(),
            actor,
            checkpoint: "terminal".to_owned(),
            request_hash: String::new(),
            response: json!({"text": "AHRB_SUCCESS one daemon"}),
            fault: None,
            barrier: None,
        });
    }
    let workflow = Workflow {
        version: WORKFLOW_SCHEMA_VERSION,
        scenario: scenario.to_owned(),
        actors,
        barriers: BTreeMap::<String, Barrier>::new(),
        responses,
    };
    let workflow_path = state_dir.join("embedded-workflow.json");
    std::fs::write(
        &workflow_path,
        serde_json::to_vec(&workflow).expect("serialize resource workflow"),
    )
    .expect("write resource workflow");
    let environment = BTreeMap::from([(
        "AHRB_MOCK_EMBEDDED_WORKFLOW".to_owned(),
        workflow_path.to_string_lossy().into_owned(),
    )]);
    let mut process =
        RpcProcess::spawn_with_state_and_environment(state_dir.clone(), environment).await;
    let daemon_pid = process.pid();
    let readiness_session = process
        .rpc("session.create", json!({"marker": "resource-readiness"}))
        .await["session_id"]
        .as_str()
        .expect("readiness session")
        .to_owned();
    process
        .rpc("session.close", json!({"session_id": readiness_session}))
        .await;
    let located_pid: u32 = std::fs::read_to_string(state_dir.join("daemon.pid"))
        .expect("stable daemon PID locator")
        .parse()
        .expect("numeric daemon PID");
    assert_eq!(located_pid, daemon_pid);

    let mut sampler = platform_sampler();
    let baseline_tree = sampler
        .discover(&[daemon_pid])
        .expect("discover idle daemon");
    assert_eq!(baseline_tree.members.len(), 1, "one shared daemon process");
    let baseline = sampler
        .sample(&baseline_tree, "warm-idle")
        .expect("sample idle daemon");
    let effective =
        |sample: &ahrb::process::Sample| sample.footprint_bytes.unwrap_or(sample.rss_bytes);

    let mut sessions = Vec::new();
    for index in 1..=AGENTS {
        let actor = format!("agent-{index}");
        let session_id = process
            .rpc("session.create", json!({"marker": actor}))
            .await["session_id"]
            .as_str()
            .expect("resource session ID")
            .to_owned();
        process
            .rpc(
                "session.submit",
                json!({
                    "session_id": session_id,
                    "prompt": marker(&actor, "start"),
                    "key": "resource-turn"
                }),
            )
            .await;
        sessions.push(session_id);
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    let reached = loop {
        let mut reached = Vec::new();
        for session in &sessions {
            let events = attach_events(&mut process, session, None).await;
            if let Some(barrier) = events
                .iter()
                .find(|event| event.event == EventVocab::BarrierReached)
            {
                let result = events
                    .iter()
                    .find(|event| event.event == EventVocab::ToolResult)
                    .expect("tool result precedes held checkpoint");
                assert!(result.cursor < barrier.cursor);
                assert!(
                    events.iter().all(|event| !is_terminal_vocab(&event.event)),
                    "held session must remain nonterminal"
                );
                reached.push((session.clone(), barrier.clone()));
            }
        }
        if reached.len() == AGENTS {
            break reached;
        }
        assert!(Instant::now() < deadline, "all N sessions reached the hold");
        tokio::task::yield_now().await;
    };

    assert_eq!(
        process.pid(),
        daemon_pid,
        "daemon persists across N sessions"
    );
    let active_tree = sampler
        .discover(&[daemon_pid])
        .expect("rediscover active daemon");
    assert_eq!(active_tree.members.len(), 1, "agents are daemon tasks");
    let active = sampler
        .sample(&active_tree, "n8-barrier-steady")
        .expect("sample active sessions");
    assert!(
        effective(&active).saturating_sub(effective(&baseline))
            >= (AGENTS as u64 * RESERVATION_BYTES * 3 / 4),
        "page-touched session reservations must be visible"
    );

    for (session, barrier) in &reached {
        let release_token = barrier.payload["release_token"]
            .as_str()
            .expect("barrier release token");
        let released = process
            .rpc(
                "checkpoint.release",
                json!({"session_id": session, "release_token": release_token}),
            )
            .await;
        assert_eq!(released["released"], true);
        assert_eq!(released["idempotent"], false);
    }
    let first_token = reached[0].1.payload["release_token"]
        .as_str()
        .expect("first release token");
    let repeated = process
        .rpc(
            "checkpoint.release",
            json!({"session_id": reached[0].0, "release_token": first_token}),
        )
        .await;
    assert_eq!(repeated["idempotent"], true);

    for session in &sessions {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let events = attach_events(&mut process, session, None).await;
            if events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess)
            {
                break;
            }
            assert!(Instant::now() < deadline, "released session terminalizes");
            tokio::task::yield_now().await;
        }
    }

    let mut released_bytes = 0_u64;
    for session in &sessions {
        let closed = process
            .rpc("session.close", json!({"session_id": session}))
            .await;
        released_bytes = released_bytes.saturating_add(
            closed["released_bytes"]
                .as_u64()
                .expect("released reservation bytes"),
        );
    }
    assert_eq!(released_bytes, AGENTS as u64 * RESERVATION_BYTES);

    let reclaim_deadline = Instant::now() + Duration::from_secs(1);
    let post_close = loop {
        let tree = sampler
            .discover(&[daemon_pid])
            .expect("rediscover reclaimed daemon");
        let sample = sampler
            .sample(&tree, "post-close")
            .expect("sample reclaimed daemon");
        if effective(&active).saturating_sub(effective(&sample))
            >= AGENTS as u64 * RESERVATION_BYTES * 4 / 5
        {
            break sample;
        }
        assert!(
            Instant::now() < reclaim_deadline,
            "munmap reclaim reaches 80%"
        );
        tokio::task::yield_now().await;
    };
    assert_eq!(post_close.processes.len(), 1);
    assert_eq!(process.pid(), daemon_pid, "daemon returns to idle in place");
    assert_eq!(
        std::fs::read_to_string(state_dir.join("daemon.pid"))
            .expect("PID locator remains present")
            .parse::<u32>()
            .expect("numeric post-close PID"),
        daemon_pid
    );

    let state_dir = process.shutdown().await;
    cleanup(&state_dir);
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[tokio::test]
async fn priority_rows_20_and_26_measure_real_idle_and_n8_whole_trees() {
    let mut processes = Vec::new();
    for index in 0..8 {
        processes.push(RpcProcess::spawn(&format!("parallel-memory-{index}")).await);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut idle_series = SampleSeries::default();
    let idle_pid = processes[0].pid();
    let mut idle_sampler = platform_sampler();
    for _ in 0..6 {
        let tree = idle_sampler
            .discover(&[idle_pid])
            .expect("discover idle tree");
        assert!(tree.members.keys().any(|identity| identity.pid == idle_pid));
        idle_series
            .push(
                idle_sampler
                    .sample(&tree, "warm-idle")
                    .expect("sample idle tree"),
            )
            .expect("monotonic idle sample");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let plateau = idle_series
        .plateau("warm-idle", MemoryMetric::Effective, 1)
        .expect("idle plateau");
    assert!(plateau.median_bytes > 0);
    assert!(plateau.all_expected_present);
    assert!(plateau.relative_spread <= 0.05);

    let all_pids: Vec<u32> = processes.iter().map(RpcProcess::pid).collect();
    let mut points = Vec::new();
    for agents in [1_u32, 2, 4, 8] {
        let mut sampler = platform_sampler();
        let roots = &all_pids[..agents as usize];
        let tree = sampler.discover(roots).expect("discover parallel roots");
        assert!(
            tree.members.len() >= agents as usize,
            "all N roots must be present"
        );
        let sample = sampler
            .sample(&tree, "barrier-steady")
            .expect("sample N tree");
        points.push(SweepPoint {
            agents,
            baseline_bytes: 0,
            steady_bytes: sample.rss_bytes,
            workload_peak_bytes: sample.rss_bytes,
            cold_peak_bytes: sample.rss_bytes,
            post_turn_bytes: sample.rss_bytes,
            post_close_bytes: 0,
        });
    }
    let metrics = SweepMetrics::calculate(&points).expect("parallel sweep metrics");
    assert_eq!(metrics.points.last().expect("N8 point").0.agents, 8);
    assert!(metrics.maximum_cold_peak_bytes <= 4 * 1024 * 1024 * 1024_u64);
    assert!(
        metrics
            .headline_beta_mib_per_agent
            .is_some_and(|beta| beta <= 256.0),
        "measured mock harness marginal must fit the reference envelope"
    );

    for process in processes {
        let state_dir = process.shutdown().await;
        cleanup(&state_dir);
    }
}

fn is_terminal_vocab(event: &EventVocab) -> bool {
    matches!(
        event,
        EventVocab::TerminalSuccess | EventVocab::TerminalFailure | EventVocab::TerminalCancelled
    )
}

#[cfg(target_os = "macos")]
fn platform_sampler() -> ahrb::process::macos::MacOsSampler {
    ahrb::process::macos::MacOsSampler::default()
}

#[cfg(target_os = "linux")]
fn platform_sampler() -> ahrb::process::linux::LinuxSampler {
    ahrb::process::linux::LinuxSampler::default()
}

#[test]
fn matrix_definitions_are_not_a_metadata_only_substitute_for_execution() {
    let exercised: BTreeSet<u8> = [1, 2, 3, 9, 10, 12].into_iter().collect();
    for row in exercised {
        let definition = ahrb::scenarios::all()
            .iter()
            .find(|definition| definition.row == row)
            .expect("matrix row");
        assert_eq!(
            definition.requirement(),
            ahrb::scenarios::RequirementKind::Core
        );
        assert!(!definition.metric.is_empty());
    }
}
