//! Built-in headless reference harness used for AHRB self-tests.
//!
//! The mock intentionally has no database and no interactive fallback.  Every committed
//! event is appended to a per-session JSONL journal and `fsync`ed before an RPC response
//! can observe it.  Replay reads that journal again, making it the recoverable source of
//! truth rather than an in-memory event buffer.

use crate::driver::http_post;
#[cfg(unix)]
use crate::driver::unix_http_post;
use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{FakeModelEngine, OpenAiChatFrontend, ProtocolFrontend};
use crate::workflow::{Fault, Workflow};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Notify};

/// Default client-side model idle deadline used by the reference adapter.
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 1_000;

/// Resident memory committed for each live mock session.
///
/// The reservation is intentionally small enough for the v1 reference envelope while
/// remaining large enough for the process sampler to distinguish N=1,2,4,8. It is
/// backed by an anonymous mapping (rather than the allocator) so `session.close` can
/// deterministically return the pages to the operating system.
pub const DEFAULT_SESSION_MEMORY_MIB: u64 = 4;

const MIB: u64 = 1024 * 1024;
static RECONCILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static NEW_FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static REPLACE_FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Durable append-only event storage for one session.
#[derive(Clone, Debug)]
pub struct DurableJournal {
    path: PathBuf,
}

impl DurableJournal {
    /// Open or create a journal and synchronously make its directory entry durable.
    pub fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.sync_all()?;
        sync_parent(&path)?;
        Ok(Self { path })
    }

    /// Append one complete JSON record plus newline and fsync it before returning.
    pub fn append(&self, event: &NormalizedEvent) -> Result<()> {
        let mut bytes = serde_json::to_vec(event)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    /// Recover every complete valid record strictly after a cursor.
    ///
    /// A non-newline-terminated final record is treated as a torn tail and ignored. Any
    /// malformed record before that tail is corruption and therefore an error.
    pub fn read_after(&self, after: Option<u64>) -> Result<Vec<NormalizedEvent>> {
        let mut reader = StdBufReader::new(File::open(&self.path)?);
        let mut events = Vec::new();
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            if line.last() != Some(&b'\n') {
                break;
            }
            line.pop();
            if line.is_empty() {
                continue;
            }
            let event: NormalizedEvent = serde_json::from_slice(&line).map_err(|error| {
                AhrbError::Protocol(format!(
                    "corrupt durable journal {}: {error}",
                    self.path.display()
                ))
            })?;
            if after.map(|cursor| event.cursor > cursor).unwrap_or(true) {
                events.push(event);
            }
        }
        for pair in events.windows(2) {
            if pair[0].cursor >= pair[1].cursor {
                return Err(AhrbError::Protocol(format!(
                    "non-monotonic journal {}",
                    self.path.display()
                )));
            }
        }
        Ok(events)
    }

    fn all(&self) -> Result<Vec<NormalizedEvent>> {
        self.read_after(None)
    }
}

#[derive(Clone, Debug)]
struct MockConfig {
    state_dir: PathBuf,
    base_url: Option<String>,
    unix_socket: Option<PathBuf>,
    embedded_model: Option<Arc<FakeModelEngine>>,
    api_key: Option<String>,
    model: String,
    idle_timeout: Duration,
    session_memory_bytes: u64,
    acceptance_hook: Vec<String>,
    completion_hook: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SessionMeta {
    id: String,
    marker: String,
}

#[derive(Clone, Debug)]
struct PendingTurn {
    prompt: String,
    key: String,
}

#[derive(Debug, Serialize)]
struct ExecTemplateEvidence {
    base_url: String,
    base_url_matches_environment: bool,
    credential_fingerprint: String,
    credential_matches_environment: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckpointPhase {
    BeforeEffect,
    AfterCommit,
}

impl CheckpointPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::BeforeEffect => "before-effect",
            Self::AfterCommit => "after-commit",
        }
    }
}

#[derive(Clone, Debug)]
struct FixtureCheckpoint {
    name: String,
    phase: CheckpointPhase,
    wait_for_release: bool,
}

#[derive(Debug)]
struct CompletedToolCall {
    call_id: String,
    result: Value,
}

#[derive(Debug)]
struct SessionState {
    meta: SessionMeta,
    journal: DurableJournal,
    next_cursor: u64,
    keys: BTreeSet<String>,
    pending: Option<PendingTurn>,
    queued: VecDeque<PendingTurn>,
    injected: Vec<String>,
    active: bool,
    cancelled: bool,
    closed: bool,
    resource_reservation: Option<ResourceReservation>,
}

/// Page-backed memory owned by one live simulated agent/session.
///
/// Keeping the mapping independent of Rust's process allocator matters for the reclaim
/// rows: dropping a `Vec` only returns memory to the allocator and need not reduce the
/// process footprint. `munmap` gives the reference harness a deterministic close/delete
/// surface without adding worker processes or a busy background loop.
#[derive(Debug)]
struct ResourceReservation {
    #[cfg(unix)]
    address: Option<usize>,
    #[cfg(not(unix))]
    allocation: Option<Box<[u8]>>,
    len: usize,
}

impl ResourceReservation {
    fn new(bytes: u64, seed: u8) -> Result<Self> {
        let len = usize::try_from(bytes).map_err(|_| {
            AhrbError::Validation("mock session memory exceeds address space".to_owned())
        })?;
        if len == 0 {
            return Ok(Self {
                #[cfg(unix)]
                address: None,
                #[cfg(not(unix))]
                allocation: None,
                len,
            });
        }
        #[cfg(unix)]
        {
            // SAFETY: the mapping is anonymous, private, and has a checked nonzero
            // length. Its address is retained exclusively by this RAII value and is
            // unmapped exactly once by `release` or `Drop`.
            let pointer = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if pointer == libc::MAP_FAILED {
                return Err(std::io::Error::last_os_error().into());
            }
            let address = pointer as usize;
            let page_size = {
                // SAFETY: `sysconf` has no memory-safety preconditions for
                // `_SC_PAGESIZE` and does not retain pointers.
                let queried = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                usize::try_from(queried)
                    .ok()
                    .filter(|size| *size > 0)
                    .unwrap_or(4096)
            };
            // Touch every page with a session-specific nonzero byte. This commits the
            // pages and avoids counting an untouched virtual reservation as footprint.
            for offset in (0..len).step_by(page_size) {
                // SAFETY: `offset` is strictly below `len`, and the mapping is writable
                // for the full `[address, address + len)` range.
                unsafe {
                    std::ptr::write_volatile((address as *mut u8).add(offset), seed.max(1));
                }
            }
            Ok(Self {
                address: Some(address),
                len,
            })
        }
        #[cfg(not(unix))]
        {
            let mut allocation = vec![seed.max(1); len].into_boxed_slice();
            std::hint::black_box(&mut allocation);
            Ok(Self {
                allocation: Some(allocation),
                len,
            })
        }
    }

    fn len(&self) -> u64 {
        self.len as u64
    }

    fn release(mut self) -> Result<u64> {
        let released = self.len();
        #[cfg(unix)]
        if let Some(address) = self.address {
            // SAFETY: this is the same address and length returned by `mmap`. Ownership
            // is cleared only after success; on failure `Drop` retains it and retries.
            if unsafe { libc::munmap(address as *mut libc::c_void, self.len) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            self.address = None;
        }
        #[cfg(not(unix))]
        {
            self.allocation.take();
        }
        Ok(released)
    }
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(address) = self.address.take() {
            // SAFETY: this is the still-owned mapping returned by `mmap`. Drop cannot
            // report an OS error, but it still provides the no-leak fallback for every
            // path other than the explicit close operation.
            let _ = unsafe { libc::munmap(address as *mut libc::c_void, self.len) };
        }
    }
}

struct MockHarness {
    config: MockConfig,
    sessions: BTreeMap<String, SessionState>,
    checkpoint_waiters: BTreeMap<String, Arc<Notify>>,
    shutting_down: bool,
}

impl MockHarness {
    fn open(config: MockConfig) -> Result<Self> {
        Self::open_with_readiness(config, true)
    }

    fn open_per_invocation(config: MockConfig) -> Result<Self> {
        Self::open_with_readiness(config, false)
    }

    fn open_with_readiness(config: MockConfig, publish_daemon_pid: bool) -> Result<Self> {
        fs::create_dir_all(config.state_dir.join("sessions"))?;
        fs::create_dir_all(config.state_dir.join("workspaces"))?;
        let mut sessions = BTreeMap::new();
        let mut entries: Vec<_> = fs::read_dir(config.state_dir.join("sessions"))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let meta_path = entry.path().join("meta.json");
            if !meta_path.is_file() {
                continue;
            }
            let meta_bytes = fs::read(&meta_path)?;
            let meta: SessionMeta = serde_json::from_slice(&meta_bytes).map_err(|error| {
                AhrbError::Protocol(format!(
                    "decode session metadata {}: {error}",
                    meta_path.display()
                ))
            })?;
            let journal = DurableJournal::open(entry.path().join("journal.jsonl"))?;
            let events = journal.all()?;
            reconcile_durable_tool_effects(&config.state_dir, &meta.id, &events)?;
            let keys = events
                .iter()
                .filter(|event| event.event == EventVocab::TurnAccepted)
                .filter_map(|event| event.payload.get("key").and_then(Value::as_str))
                .map(str::to_owned)
                .collect();
            let pending = recover_pending(&events);
            let next_cursor = events
                .last()
                .map(|event| event.cursor.saturating_add(1))
                .unwrap_or(1);
            sessions.insert(
                meta.id.clone(),
                SessionState {
                    meta,
                    journal,
                    next_cursor,
                    keys,
                    pending,
                    queued: VecDeque::new(),
                    injected: Vec::new(),
                    active: false,
                    cancelled: false,
                    closed: false,
                    resource_reservation: None,
                },
            );
        }
        if publish_daemon_pid {
            // Publish readiness only after durable session recovery and fixture-effect
            // reconciliation have completed. The same file is the sampler's verified PID
            // locator, so a visible PID always denotes a daemon ready to accept RPCs.
            write_replace_synced(
                &config.state_dir.join("daemon.pid"),
                std::process::id().to_string().as_bytes(),
            )?;
            sync_directory(&config.state_dir)?;
        }
        Ok(Self {
            config,
            sessions,
            checkpoint_waiters: BTreeMap::new(),
            shutting_down: false,
        })
    }

    fn create_session(&mut self, marker: &str) -> Result<String> {
        self.create_session_with_id(marker, &stable_session_id(marker))
    }

    fn create_session_with_id(&mut self, marker: &str, id: &str) -> Result<String> {
        validate_marker(marker)?;
        validate_session_id(id)?;
        if let Some(session) = self.sessions.get(id) {
            if session.meta.marker != marker {
                return Err(AhrbError::Protocol("session hash collision".to_owned()));
            }
            return Ok(id.to_owned());
        }
        let directory = self.config.state_dir.join("sessions").join(id);
        fs::create_dir(&directory)?;
        let meta = SessionMeta {
            id: id.to_owned(),
            marker: marker.to_owned(),
        };
        write_new_synced(&directory.join("meta.json"), &serde_json::to_vec(&meta)?)?;
        let journal = DurableJournal::open(directory.join("journal.jsonl"))?;
        fs::create_dir(self.config.state_dir.join("workspaces").join(id))?;
        sync_directory(&directory)?;
        sync_directory(&self.config.state_dir.join("sessions"))?;
        self.sessions.insert(
            id.to_owned(),
            SessionState {
                meta,
                journal,
                next_cursor: 1,
                keys: BTreeSet::new(),
                pending: None,
                queued: VecDeque::new(),
                injected: Vec::new(),
                active: false,
                cancelled: false,
                closed: false,
                resource_reservation: None,
            },
        );
        Ok(id.to_owned())
    }

    fn append(&mut self, session_id: &str, event: EventVocab, payload: Value) -> Result<u64> {
        let session = self.session_mut(session_id)?;
        let cursor = session.next_cursor;
        let next_cursor = cursor.checked_add(1).ok_or_else(|| {
            AhrbError::Validation(format!("session {session_id:?} exhausted its cursor space"))
        })?;
        let normalized = NormalizedEvent {
            id: format!("{session_id}:{cursor}"),
            cursor,
            session_id: session_id.to_owned(),
            actor: session.meta.marker.clone(),
            event,
            payload,
        };
        session.journal.append(&normalized)?;
        session.next_cursor = next_cursor;
        Ok(cursor)
    }

    fn append_terminal(
        &mut self,
        session_id: &str,
        event: EventVocab,
        payload: Value,
    ) -> Result<bool> {
        if !is_terminal(&event) {
            return Err(AhrbError::Protocol(
                "append_terminal requires a terminal event".to_owned(),
            ));
        }
        if session_is_terminal(self.session_mut(session_id)?)? {
            return Ok(false);
        }
        self.append(session_id, event, payload)?;
        Ok(true)
    }

    fn session_mut(&mut self, id: &str) -> Result<&mut SessionState> {
        self.sessions
            .get_mut(id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))
    }
}

fn recover_pending(events: &[NormalizedEvent]) -> Option<PendingTurn> {
    let terminal_cursor = events
        .iter()
        .rev()
        .find(|event| is_terminal(&event.event))
        .map(|event| event.cursor)
        .unwrap_or(0);
    events
        .iter()
        .rev()
        .find(|event| event.event == EventVocab::TurnAccepted && event.cursor > terminal_cursor)
        .and_then(|event| {
            Some(PendingTurn {
                prompt: event.payload.get("prompt")?.as_str()?.to_owned(),
                key: event.payload.get("key")?.as_str()?.to_owned(),
            })
        })
}

#[derive(Deserialize)]
struct RpcRequest {
    #[allow(dead_code)]
    #[serde(default)]
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct RpcReply {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcReplyError>,
}

#[derive(Serialize)]
struct RpcReplyError {
    code: i64,
    message: String,
}

impl RpcReply {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn failure(id: Value, error: AhrbError) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(RpcReplyError {
                code: -32_000,
                message: error.to_string(),
            }),
        }
    }
}

/// Run the mock harness command-line process.
pub async fn run(args: &[String]) -> Result<i32> {
    let command = args.first().map(String::as_str).unwrap_or("serve");
    match command {
        "serve" => serve(parse_config(&args[1..])?, false).await,
        "rpc" => serve(parse_config(&args[1..])?, true).await,
        "exec-turn" => exec_turn(&args[1..]).await,
        "release-checkpoint" => release_checkpoint_command(&args[1..]).await,
        "cancel-session" => cancel_session_command(&args[1..]).await,
        "inspect-journal" => inspect_journal(&args[1..]),
        "hook" => hook_command(&args[1..]),
        "--help" | "help" => {
            println!(
                "ahrb-mock-harness serve|rpc --state-dir PATH [--idle-timeout-ms N] \
                 [--session-memory-mib N]\n\
                 ahrb-mock-harness exec-turn --state-dir PATH --marker MARKER \
                 --session-id ID --prompt PROMPT --key KEY \
                 [--base-url URL --credential TOKEN]\n\
                 ahrb-mock-harness release-checkpoint --state-dir PATH \
                 --session-id ID --release-token TOKEN\n\
                 ahrb-mock-harness cancel-session --state-dir PATH --session-id ID\n\
                 model endpoint comes from AHRB_MOCK_BASE_URL or AHRB_MOCK_UNIX_SOCKET; \
                 key/model come from AHRB_MOCK_API_KEY and AHRB_MOCK_MODEL"
            );
            Ok(0)
        }
        other => Err(AhrbError::Usage(format!(
            "unknown mock harness command {other:?}"
        ))),
    }
}

async fn exec_turn(args: &[String]) -> Result<i32> {
    let mut marker = None;
    let mut requested_session_id = None;
    let mut prompt = None;
    let mut key = None;
    let mut rendered_base_url = None;
    let mut rendered_credential = None;
    let mut event_journal = None;
    let mut post_output_delay_ms = 0_u64;
    let mut config_args = Vec::new();
    let mut index = 0_usize;
    while index < args.len() {
        let option = args[index].as_str();
        let value = args
            .get(index + 1)
            .ok_or_else(|| AhrbError::Usage(format!("{option} needs a value")))?;
        match option {
            "--marker" => marker = Some(value.clone()),
            "--session-id" => requested_session_id = Some(value.clone()),
            "--prompt" => prompt = Some(value.clone()),
            "--key" => key = Some(value.clone()),
            "--base-url" => rendered_base_url = Some(value.clone()),
            "--credential" => rendered_credential = Some(value.clone()),
            "--event-journal" => event_journal = Some(PathBuf::from(value)),
            "--post-output-delay-ms" => {
                post_output_delay_ms = value
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid post-output delay".to_owned()))?;
            }
            "--state-dir" | "--idle-timeout-ms" | "--session-memory-mib" => {
                config_args.push(option.to_owned());
                config_args.push(value.clone());
            }
            other => {
                return Err(AhrbError::Usage(format!(
                    "unknown exec-turn option {other:?}"
                )));
            }
        }
        index += 2;
    }
    let marker = marker.ok_or_else(|| AhrbError::Usage("--marker is required".to_owned()))?;
    let requested_session_id = requested_session_id
        .ok_or_else(|| AhrbError::Usage("--session-id is required".to_owned()))?;
    let prompt = prompt.ok_or_else(|| AhrbError::Usage("--prompt is required".to_owned()))?;
    let key = key.ok_or_else(|| AhrbError::Usage("--key is required".to_owned()))?;
    let exec_template_evidence = match (rendered_base_url, rendered_credential) {
        (Some(base_url), Some(credential)) => {
            let environment_base_url = std::env::var("AHRB_MOCK_BASE_URL").unwrap_or_default();
            let environment_credential = std::env::var("AHRB_MOCK_API_KEY").unwrap_or_default();
            Some(ExecTemplateEvidence {
                base_url_matches_environment: base_url == environment_base_url,
                credential_fingerprint: format!("{:x}", Sha256::digest(credential.as_bytes())),
                credential_matches_environment: credential == environment_credential,
                base_url,
            })
        }
        (None, None) => None,
        _ => {
            return Err(AhrbError::Usage(
                "--base-url and --credential must be provided together".to_owned(),
            ));
        }
    };
    let config = parse_config(&config_args)?;
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation(config)?));
    let turn = PendingTurn { prompt, key };
    let (session_id, journal, after) = {
        let mut guard = harness.lock().await;
        let id = guard.create_session_with_id(&marker, &requested_session_id)?;
        let journal = guard.session_mut(&id)?.journal.clone();
        let after = journal.all()?.last().map(|event| event.cursor);
        (id, journal, after)
    };
    let spawn = accept_turn(
        &harness,
        &session_id,
        turn.clone(),
        false,
        exec_template_evidence.as_ref(),
    )
    .await?;
    let resume_pending = if spawn {
        false
    } else {
        let mut guard = harness.lock().await;
        let session = guard.session_mut(&session_id)?;
        session
            .pending
            .as_ref()
            .is_some_and(|pending| pending.key == turn.key && pending.prompt == turn.prompt)
            && !session_is_terminal(session)?
    };
    if spawn || resume_pending {
        spawn_worker(Arc::clone(&harness), session_id.clone());
    }
    let terminal = if spawn || resume_pending {
        let started = std::time::Instant::now();
        loop {
            let events = journal.read_after(after)?;
            if let Some(terminal) = events.iter().rev().find(|event| is_terminal(&event.event)) {
                break terminal.clone();
            }
            if started.elapsed() >= Duration::from_secs(60) {
                return Err(AhrbError::Timeout("mock exec turn".to_owned()));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    } else {
        journal
            .all()?
            .into_iter()
            .rev()
            .find(|event| is_terminal(&event.event))
            .ok_or_else(|| {
                AhrbError::Protocol(
                    "idempotent exec retry found neither pending work nor a terminal".to_owned(),
                )
            })?
    };
    let events = if spawn || resume_pending {
        journal.read_after(after)?
    } else {
        vec![terminal.clone()]
    };
    if let Some(path) = event_journal {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        for event in &events {
            serde_json::to_writer(&mut file, event)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        sync_parent(&path)?;
    }
    for event in events {
        println!("{}", serde_json::to_string(&event)?);
    }
    std::io::stdout().flush()?;
    if post_output_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(post_output_delay_ms)).await;
    }
    Ok(if terminal.event == EventVocab::TerminalSuccess {
        0
    } else {
        14
    })
}

async fn release_checkpoint_command(args: &[String]) -> Result<i32> {
    let (config, session_id, release_token) = parse_session_control(args, true)?;
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation(config)?));
    let release_token = release_token.ok_or_else(|| {
        AhrbError::Usage("--release-token is required for release-checkpoint".to_owned())
    })?;
    release_checkpoint(&harness, &session_id, &release_token).await?;
    Ok(0)
}

async fn cancel_session_command(args: &[String]) -> Result<i32> {
    let (config, session_id, _) = parse_session_control(args, false)?;
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation(config)?));
    let workspace = {
        let mut guard = harness.lock().await;
        guard.append_terminal(
            &session_id,
            EventVocab::TerminalCancelled,
            json!({"status":"cancelled", "cleanup":"workspace-removed"}),
        )?;
        guard.config.state_dir.join("workspaces").join(&session_id)
    };
    if workspace.exists() {
        fs::remove_dir_all(&workspace)?;
    }
    fs::create_dir(&workspace)?;
    sync_parent(&workspace)?;
    Ok(0)
}

fn parse_session_control(
    args: &[String],
    allow_release_token: bool,
) -> Result<(MockConfig, String, Option<String>)> {
    let mut session_id = None;
    let mut release_token = None;
    let mut config_args = Vec::new();
    let mut index = 0_usize;
    while index < args.len() {
        let option = args[index].as_str();
        let value = args
            .get(index + 1)
            .ok_or_else(|| AhrbError::Usage(format!("{option} needs a value")))?;
        match option {
            "--session-id" => session_id = Some(value.clone()),
            "--release-token" if allow_release_token => release_token = Some(value.clone()),
            "--state-dir" | "--idle-timeout-ms" | "--session-memory-mib" => {
                config_args.push(option.to_owned());
                config_args.push(value.clone());
            }
            other => {
                return Err(AhrbError::Usage(format!(
                    "unknown session control option {other:?}"
                )));
            }
        }
        index += 2;
    }
    let session_id =
        session_id.ok_or_else(|| AhrbError::Usage("--session-id is required".to_owned()))?;
    validate_session_id(&session_id)?;
    Ok((parse_config(&config_args)?, session_id, release_token))
}

fn parse_config(args: &[String]) -> Result<MockConfig> {
    let mut state_dir = None;
    let mut idle_timeout_ms = DEFAULT_IDLE_TIMEOUT_MS;
    let mut session_memory_mib = DEFAULT_SESSION_MEMORY_MIB;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--state-dir" => {
                index += 1;
                state_dir = args.get(index).map(PathBuf::from);
            }
            "--idle-timeout-ms" => {
                index += 1;
                idle_timeout_ms = args
                    .get(index)
                    .ok_or_else(|| AhrbError::Usage("--idle-timeout-ms needs a value".to_owned()))?
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid idle timeout".to_owned()))?;
            }
            "--session-memory-mib" => {
                index += 1;
                session_memory_mib = args
                    .get(index)
                    .ok_or_else(|| {
                        AhrbError::Usage("--session-memory-mib needs a value".to_owned())
                    })?
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid session memory size".to_owned()))?;
            }
            option => {
                return Err(AhrbError::Usage(format!("unknown option {option:?}")));
            }
        }
        index += 1;
    }
    let state_dir = state_dir
        .or_else(|| std::env::var_os("AHRB_MOCK_STATE_DIR").map(PathBuf::from))
        .ok_or_else(|| AhrbError::Usage("--state-dir PATH is required".to_owned()))?;
    if !state_dir.is_absolute() {
        return Err(AhrbError::Validation(
            "mock state directory must be absolute".to_owned(),
        ));
    }
    let session_memory_bytes = session_memory_mib.checked_mul(MIB).ok_or_else(|| {
        AhrbError::Validation("mock session memory size overflows bytes".to_owned())
    })?;
    let base_url = std::env::var("AHRB_MOCK_BASE_URL")
        .ok()
        .map(|url| url.trim_end_matches('/').to_owned());
    let embedded_model = match std::env::var_os("AHRB_MOCK_EMBEDDED_WORKFLOW") {
        Some(path) => {
            let workflow: Workflow = serde_json::from_slice(&fs::read(PathBuf::from(path))?)?;
            Some(Arc::new(FakeModelEngine::new(&workflow)?))
        }
        None => None,
    };
    Ok(MockConfig {
        state_dir,
        base_url,
        unix_socket: std::env::var_os("AHRB_MOCK_UNIX_SOCKET").map(PathBuf::from),
        embedded_model,
        api_key: std::env::var("AHRB_MOCK_API_KEY").ok(),
        model: std::env::var("AHRB_MOCK_MODEL").unwrap_or_else(|_| "ahrb-fake-v1".to_owned()),
        idle_timeout: Duration::from_millis(idle_timeout_ms),
        session_memory_bytes,
        acceptance_hook: parse_hook_env("AHRB_MOCK_ACCEPTANCE_HOOK")?,
        completion_hook: parse_hook_env("AHRB_MOCK_COMPLETION_HOOK")?,
    })
}

fn parse_hook_env(name: &str) -> Result<Vec<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(serde_json::from_str(&value)?),
        Err(std::env::VarError::NotPresent) => Ok(Vec::new()),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(AhrbError::Validation(format!("{name} is not Unicode")))
        }
    }
}

async fn serve(config: MockConfig, one_request: bool) -> Result<i32> {
    let pid_path = config.state_dir.join("daemon.pid");
    let harness = Arc::new(Mutex::new(MockHarness::open(config)?));
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        let request: RpcRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                let reply = RpcReply::failure(Value::Null, AhrbError::Json(error));
                write_reply(&mut stdout, &reply).await?;
                if one_request {
                    return Ok(2);
                }
                continue;
            }
        };
        let id = request.id.clone();
        let outcome = handle_rpc(Arc::clone(&harness), request).await;
        let reply = match outcome {
            Ok(result) => RpcReply::success(id, result),
            Err(error) => RpcReply::failure(id, error),
        };
        write_reply(&mut stdout, &reply).await?;
        let shutdown = harness.lock().await.shutting_down;
        if one_request || shutdown {
            break;
        }
    }
    match fs::remove_file(&pid_path) {
        Ok(()) => sync_parent(&pid_path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(0)
}

async fn write_reply(stdout: &mut tokio::io::Stdout, reply: &RpcReply) -> Result<()> {
    let mut bytes = serde_json::to_vec(reply)?;
    bytes.push(b'\n');
    stdout.write_all(&bytes).await?;
    stdout.flush().await?;
    Ok(())
}

async fn handle_rpc(harness: Arc<Mutex<MockHarness>>, request: RpcRequest) -> Result<Value> {
    match request.method.as_str() {
        "session.create" => {
            let marker = required_str(&request.params, "marker")?;
            let id = harness.lock().await.create_session(marker)?;
            Ok(json!({ "session_id": id }))
        }
        "session.submit" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let prompt = required_str(&request.params, "prompt")?.to_owned();
            let key = required_str(&request.params, "key")?.to_owned();
            let spawn =
                accept_turn(&harness, &id, PendingTurn { prompt, key }, false, None).await?;
            if spawn {
                spawn_worker(Arc::clone(&harness), id);
            }
            Ok(json!({ "accepted": true, "idempotent": !spawn }))
        }
        "session.attach" => {
            let id = required_str(&request.params, "session_id")?;
            let after = request.params.get("after").and_then(Value::as_u64);
            let journal = {
                let guard = harness.lock().await;
                guard
                    .sessions
                    .get(id)
                    .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?
                    .journal
                    .clone()
            };
            Ok(json!({ "events": journal.read_after(after)? }))
        }
        "session.resume" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let spawn = {
                let mut guard = harness.lock().await;
                let session = guard.session_mut(&id)?;
                if session.pending.is_some() && !session.active {
                    session.active = true;
                    session.cancelled = false;
                    true
                } else {
                    false
                }
            };
            if spawn {
                spawn_worker(Arc::clone(&harness), id);
            }
            Ok(json!({ "resumed": spawn }))
        }
        "session.steer" | "session.subturn" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let prompt = required_str(&request.params, "prompt")?.to_owned();
            let phase = request.method.trim_start_matches("session.");
            let mut guard = harness.lock().await;
            guard.session_mut(&id)?.injected.push(prompt.clone());
            guard.append(
                &id,
                EventVocab::InputAccepted,
                json!({ "phase": phase, "prompt": prompt }),
            )?;
            Ok(json!({ "accepted": true }))
        }
        "session.queue" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let turn = PendingTurn {
                prompt: required_str(&request.params, "prompt")?.to_owned(),
                key: required_str(&request.params, "key")?.to_owned(),
            };
            let mut guard = harness.lock().await;
            guard.append(
                &id,
                EventVocab::InputAccepted,
                json!({ "phase": "queue", "prompt": turn.prompt, "key": turn.key }),
            )?;
            guard.session_mut(&id)?.queued.push_back(turn);
            Ok(json!({ "queued": true }))
        }
        "agent.spawn" => {
            let parent = required_str(&request.params, "parent_session_id")?.to_owned();
            let marker = required_str(&request.params, "marker")?.to_owned();
            let prompt = request
                .params
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let child = {
                let mut guard = harness.lock().await;
                let child = guard.create_session(&marker)?;
                guard.append(
                    &parent,
                    EventVocab::AgentSpawned,
                    json!({ "child_session_id": child, "marker": marker }),
                )?;
                child
            };
            if let Some(prompt) = prompt {
                let key = format!("native-spawn:{parent}:{child}");
                let spawn =
                    accept_turn(&harness, &child, PendingTurn { prompt, key }, false, None).await?;
                if spawn {
                    spawn_worker(Arc::clone(&harness), child.clone());
                }
            }
            Ok(json!({ "session_id": child }))
        }
        "session.cancel" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let mut guard = harness.lock().await;
            let (reservation, waiters) = {
                let session = guard.session_mut(&id)?;
                session.cancelled = true;
                session.active = false;
                session.pending = None;
                session.queued.clear();
                session.queued.shrink_to_fit();
                session.injected.clear();
                session.injected.shrink_to_fit();
                let reservation = session.resource_reservation.take();
                let waiters = checkpoint_waiters_for_session(&guard, &id);
                (reservation, waiters)
            };
            guard.append_terminal(
                &id,
                EventVocab::TerminalCancelled,
                json!({ "status": "cancelled" }),
            )?;
            for waiter in waiters {
                waiter.notify_one();
            }
            let released_bytes = match reservation {
                Some(reservation) => reservation.release()?,
                None => 0,
            };
            Ok(json!({ "cancelled": true, "released_bytes": released_bytes }))
        }
        "session.close" => {
            let id = required_str(&request.params, "session_id")?;
            let mut guard = harness.lock().await;
            let (reservation, waiters) = {
                let session = guard.session_mut(id)?;
                session.cancelled = true;
                session.closed = true;
                session.active = false;
                session.pending = None;
                session.queued.clear();
                session.queued.shrink_to_fit();
                session.injected.clear();
                session.injected.shrink_to_fit();
                session.keys.clear();
                let reservation = session.resource_reservation.take();
                let waiters = checkpoint_waiters_for_session(&guard, id);
                (reservation, waiters)
            };
            for waiter in waiters {
                waiter.notify_one();
            }
            let released_bytes = match reservation {
                Some(reservation) => reservation.release()?,
                None => 0,
            };
            Ok(json!({ "closed": true, "released_bytes": released_bytes }))
        }
        "checkpoint.release" => {
            let session_id = required_str(&request.params, "session_id")?.to_owned();
            let release_token = required_str(&request.params, "release_token")?.to_owned();
            release_checkpoint(&harness, &session_id, &release_token).await
        }
        "harness.shutdown" => {
            harness.lock().await.shutting_down = true;
            Ok(json!({ "shutdown": true }))
        }
        other => Err(AhrbError::Protocol(format!("unknown RPC method {other:?}"))),
    }
}

async fn accept_turn(
    harness: &Arc<Mutex<MockHarness>>,
    id: &str,
    turn: PendingTurn,
    from_queue: bool,
    exec_template_evidence: Option<&ExecTemplateEvidence>,
) -> Result<bool> {
    let hook = {
        let mut guard = harness.lock().await;
        let reservation_bytes = guard.config.session_memory_bytes;
        let session = guard.session_mut(id)?;
        if session.closed {
            return Err(AhrbError::Protocol("session is closed".to_owned()));
        }
        if session.keys.contains(&turn.key) {
            return Ok(false);
        }
        if session.active && !from_queue {
            return Err(AhrbError::Protocol(
                "session already has an active turn".to_owned(),
            ));
        }
        if session.resource_reservation.is_none() {
            let seed = Sha256::digest(session.meta.id.as_bytes())[0];
            session.resource_reservation = Some(ResourceReservation::new(reservation_bytes, seed)?);
        }
        session.keys.insert(turn.key.clone());
        session.pending = Some(turn.clone());
        session.active = true;
        session.cancelled = false;
        let mut payload = json!({ "prompt": turn.prompt, "key": turn.key });
        if let Some(evidence) = exec_template_evidence {
            payload["exec_template"] = serde_json::to_value(evidence)?;
        }
        guard.append(id, EventVocab::TurnAccepted, payload)?;
        guard.config.acceptance_hook.clone()
    };
    run_hook_and_record(harness, id, "acceptance", &turn.key, &hook).await?;
    Ok(true)
}

fn spawn_worker(harness: Arc<Mutex<MockHarness>>, id: String) {
    tokio::spawn(async move {
        if let Err(error) = worker_loop(Arc::clone(&harness), &id).await {
            let mut guard = harness.lock().await;
            let terminal = guard
                .sessions
                .get(&id)
                .and_then(|session| session_is_terminal(session).ok())
                .unwrap_or(false);
            if !terminal {
                let _ = guard.append_terminal(
                    &id,
                    EventVocab::TerminalFailure,
                    json!({ "status": "failure", "category": "harness-error", "message": error.to_string() }),
                );
            }
            if let Some(session) = guard.sessions.get_mut(&id) {
                session.active = false;
            }
        }
    });
}

async fn worker_loop(harness: Arc<Mutex<MockHarness>>, id: &str) -> Result<()> {
    loop {
        let turn = {
            let guard = harness.lock().await;
            guard
                .sessions
                .get(id)
                .and_then(|session| session.pending.clone())
        };
        let Some(turn) = turn else {
            return Ok(());
        };
        execute_turn(&harness, id, &turn).await?;
        let (next, completion_hook) = {
            let mut guard = harness.lock().await;
            let hook = guard.config.completion_hook.clone();
            let session = guard.session_mut(id)?;
            session.pending = None;
            session.active = false;
            let next = if session.cancelled || session.closed {
                None
            } else {
                session.queued.pop_front()
            };
            (next, hook)
        };
        run_hook_and_record(&harness, id, "completion", &turn.key, &completion_hook).await?;
        let Some(next) = next else {
            return Ok(());
        };
        if !accept_turn(&harness, id, next, true, None).await? {
            continue;
        }
    }
}

async fn execute_turn(
    harness: &Arc<Mutex<MockHarness>>,
    id: &str,
    turn: &PendingTurn,
) -> Result<()> {
    let config = harness.lock().await.config.clone();
    if config.base_url.is_none() && config.unix_socket.is_none() && config.embedded_model.is_none()
    {
        let mut guard = harness.lock().await;
        if session_should_stop(guard.session_mut(id)?)? {
            return Ok(());
        }
        guard.append_terminal(
            id,
            EventVocab::TerminalSuccess,
            json!({ "status": "success", "mode": "offline", "key": turn.key }),
        )?;
        return Ok(());
    }
    let mut messages = vec![json!({ "role": "user", "content": turn.prompt })];
    for checkpoint in 0..32_u64 {
        {
            let mut guard = harness.lock().await;
            let session = guard.session_mut(id)?;
            if session_should_stop(session)? {
                return Ok(());
            }
            for prompt in std::mem::take(&mut session.injected) {
                messages.push(json!({ "role": "user", "content": prompt }));
            }
            guard.append(
                id,
                EventVocab::ModelRequest,
                json!({ "model": config.model, "endpoint": "/v1/chat/completions", "checkpoint": checkpoint }),
            )?;
        }
        let request = json!({
            "model": config.model,
            "messages": messages,
            "tools": fixture_tools(),
            "stream": false
        });
        let mut headers = BTreeMap::new();
        if let Some(key) = &config.api_key {
            headers.insert("Authorization".to_owned(), format!("Bearer {key}"));
        }
        let body = serde_json::to_vec(&request)?;
        let response_result = model_http_post(&config, &headers, &body).await;
        let response = match response_result {
            Ok(response) => response,
            Err(AhrbError::Timeout(message)) => {
                let mut guard = harness.lock().await;
                guard.append_terminal(
                    id,
                    EventVocab::TerminalFailure,
                    json!({ "status": "failure", "category": "idle-timeout", "message": message }),
                )?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if !(200..300).contains(&response.status) {
            let mut guard = harness.lock().await;
            guard.append_terminal(
                id,
                EventVocab::TerminalFailure,
                json!({ "status": "failure", "category": "provider", "http_status": response.status }),
            )?;
            return Ok(());
        }
        let value: Value = serde_json::from_slice(&response.body)?;
        let message = value
            .pointer("/choices/0/message")
            .cloned()
            .ok_or_else(|| {
                AhrbError::Protocol("model response omitted choices[0].message".to_owned())
            })?;
        {
            let mut guard = harness.lock().await;
            if session_should_stop(guard.session_mut(id)?)? {
                return Ok(());
            }
            guard.append(
                id,
                EventVocab::ModelResponse,
                json!({ "checkpoint": checkpoint }),
            )?;
        }
        let tool_calls = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        messages.push(message.clone());
        if tool_calls.is_empty() {
            let content = message.get("content").cloned().unwrap_or(Value::Null);
            let (event, status) = terminal_from_content(&content);
            let mut guard = harness.lock().await;
            guard.append_terminal(id, event, json!({ "status": status, "content": content }))?;
            return Ok(());
        }
        let mut prepared_calls = Vec::new();
        for tool_call in tool_calls {
            let call_id = required_str(&tool_call, "id")?.to_owned();
            let name = tool_call
                .pointer("/function/name")
                .and_then(Value::as_str)
                .ok_or_else(|| AhrbError::Protocol("tool call omitted function.name".to_owned()))?
                .to_owned();
            let raw_args = tool_call
                .pointer("/function/arguments")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AhrbError::Protocol("tool call omitted function.arguments".to_owned())
                })?;
            let args: Value = serde_json::from_str(raw_args).map_err(|error| {
                AhrbError::Protocol(format!("malformed tool arguments for {call_id}: {error}"))
            })?;
            let duplicate = {
                let mut guard = harness.lock().await;
                let session = guard.session_mut(id)?;
                if session_should_stop(session)? {
                    return Ok(());
                }
                let duplicate = tool_result_for(session, &call_id)?;
                if duplicate.is_none() {
                    guard.append(
                        id,
                        EventVocab::ToolCall,
                        json!({ "call_id": call_id, "name": name, "arguments": args }),
                    )?;
                }
                duplicate
            };
            let checkpoint = fixture_checkpoint(&name, &args)?;
            prepared_calls.push((call_id, name, args, duplicate, checkpoint));
        }
        // Every complete call is durably visible before any result is committed. A
        // multi-call assistant frame therefore represents concurrent live calls rather
        // than accidentally serializing call discovery behind the first effect.
        let mut tasks = Vec::with_capacity(prepared_calls.len());
        for (call_id, name, args, duplicate, checkpoint) in prepared_calls {
            let task_harness = Arc::clone(harness);
            let task_config = config.clone();
            let task_session = id.to_owned();
            tasks.push(tokio::spawn(async move {
                execute_prepared_tool_call(
                    task_harness,
                    task_config,
                    task_session,
                    call_id,
                    name,
                    args,
                    duplicate,
                    checkpoint,
                )
                .await
            }));
        }
        let mut completed = Vec::with_capacity(tasks.len());
        for task in tasks {
            let Some(call) = task
                .await
                .map_err(|error| AhrbError::Protocol(format!("fixture task failed: {error}")))??
            else {
                return Ok(());
            };
            completed.push(call);
        }
        // Preserve assistant-frame order in the next request even when calls commit in
        // the reverse order selected by their release tokens.
        for call in completed {
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call.call_id,
                "content": serde_json::to_string(&call.result)?
            }));
        }
    }
    let mut guard = harness.lock().await;
    guard.append_terminal(
        id,
        EventVocab::TerminalFailure,
        json!({ "status": "failure", "category": "turn-limit" }),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_prepared_tool_call(
    harness: Arc<Mutex<MockHarness>>,
    config: MockConfig,
    session_id: String,
    call_id: String,
    name: String,
    args: Value,
    duplicate: Option<Value>,
    checkpoint: Option<FixtureCheckpoint>,
) -> Result<Option<CompletedToolCall>> {
    if checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.phase == CheckpointPhase::BeforeEffect)
        && !record_checkpoint_and_wait(
            &harness,
            &session_id,
            &call_id,
            &args,
            checkpoint
                .as_ref()
                .ok_or_else(|| AhrbError::Protocol("fixture checkpoint disappeared".to_owned()))?,
        )
        .await?
    {
        return Ok(None);
    }

    let prepared_result = match duplicate {
        Some(_) => None,
        None => Some(fixture_result(
            &config.state_dir,
            &session_id,
            &name,
            &args,
        )?),
    };
    let result = {
        let mut guard = harness.lock().await;
        if session_should_stop(guard.session_mut(&session_id)?)? {
            return Ok(None);
        }
        let (result, needs_reconcile) = match (duplicate, prepared_result) {
            (Some(result), _) => (result, false),
            (None, Some(result)) => {
                guard.append(
                    &session_id,
                    EventVocab::ToolResult,
                    json!({
                        "call_id": call_id,
                        "name": name,
                        "arguments": args,
                        "result": result
                    }),
                )?;
                (result, true)
            }
            (None, None) => {
                return Err(AhrbError::Protocol(
                    "tool result preparation was lost".to_owned(),
                ));
            }
        };
        if needs_reconcile {
            reconcile_fixture_effect(
                &config.state_dir,
                &session_id,
                &call_id,
                &name,
                &args,
                &result,
            )?;
        }
        result
    };

    if checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.phase == CheckpointPhase::AfterCommit)
        && !record_checkpoint_and_wait(
            &harness,
            &session_id,
            &call_id,
            &args,
            checkpoint
                .as_ref()
                .ok_or_else(|| AhrbError::Protocol("fixture checkpoint disappeared".to_owned()))?,
        )
        .await?
    {
        return Ok(None);
    }

    Ok(Some(CompletedToolCall { call_id, result }))
}

fn fixture_checkpoint(name: &str, args: &Value) -> Result<Option<FixtureCheckpoint>> {
    let explicit = args.get("ahrb_checkpoint");
    if let Some(explicit) = explicit {
        let object = explicit
            .as_object()
            .ok_or_else(|| AhrbError::Protocol("ahrb_checkpoint must be an object".to_owned()))?;
        let checkpoint_name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("ahrb_checkpoint.name is required".to_owned()))?;
        validate_checkpoint_name(checkpoint_name)?;
        let phase = match object
            .get("phase")
            .and_then(Value::as_str)
            .unwrap_or("before-effect")
        {
            "before-effect" => CheckpointPhase::BeforeEffect,
            "after-commit" => CheckpointPhase::AfterCommit,
            other => {
                return Err(AhrbError::Protocol(format!(
                    "unknown fixture checkpoint phase {other:?}"
                )));
            }
        };
        return Ok(Some(FixtureCheckpoint {
            name: checkpoint_name.to_owned(),
            phase,
            wait_for_release: true,
        }));
    }
    if name != "barrier" {
        return Ok(None);
    }
    let checkpoint_name = required_str(args, "name")?;
    validate_checkpoint_name(checkpoint_name)?;
    Ok(Some(FixtureCheckpoint {
        name: checkpoint_name.to_owned(),
        phase: CheckpointPhase::BeforeEffect,
        wait_for_release: args
            .get("wait_for_release")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }))
}

async fn record_checkpoint_and_wait(
    harness: &Arc<Mutex<MockHarness>>,
    session_id: &str,
    call_id: &str,
    args: &Value,
    checkpoint: &FixtureCheckpoint,
) -> Result<bool> {
    let state_dir = harness.lock().await.config.state_dir.clone();
    let (reached_path, release_path, release_relative) =
        checkpoint_paths(&state_dir, session_id, call_id, &checkpoint.name)?;
    let reached_record = json!({
        "session_id": session_id,
        "call_id": call_id,
        "name": checkpoint.name,
        "phase": checkpoint.phase.as_str(),
        "release_token": release_relative
    });
    if !reached_path.exists() {
        if let Some(parent) = reached_path.parent() {
            fs::create_dir_all(parent)?;
        }
        write_new_synced(&reached_path, &serde_json::to_vec(&reached_record)?)?;
    }
    {
        let mut guard = harness.lock().await;
        if session_should_stop(guard.session_mut(session_id)?)? {
            return Ok(false);
        }
        let already_recorded = guard
            .session_mut(session_id)?
            .journal
            .all()?
            .iter()
            .any(|event| {
                event.event == EventVocab::BarrierReached
                    && event.payload.get("call_id").and_then(Value::as_str) == Some(call_id)
                    && event.payload.get("name").and_then(Value::as_str)
                        == Some(checkpoint.name.as_str())
            });
        if !already_recorded {
            guard.append(
                session_id,
                EventVocab::BarrierReached,
                json!({
                    "call_id": call_id,
                    "name": checkpoint.name,
                    "marker": args.get("marker").cloned().unwrap_or(Value::Null),
                    "phase": checkpoint.phase.as_str(),
                    "release_token": release_relative
                }),
            )?;
        }
    }
    if !checkpoint.wait_for_release {
        return Ok(true);
    }
    let notify = {
        let mut guard = harness.lock().await;
        if session_should_stop(guard.session_mut(session_id)?)? {
            return Ok(false);
        }
        Arc::clone(
            guard
                .checkpoint_waiters
                .entry(release_relative.clone())
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    };
    // The durable token is checked after registering the waiter. A release racing this
    // check either leaves the file visible or stores a `notify_one` permit, so wakeups
    // cannot be lost and no cadence polling is needed.
    if release_token_exists(&release_path)? {
        remove_checkpoint_waiter(harness, &release_relative, &notify).await;
        return Ok(true);
    }
    loop {
        tokio::select! {
            () = notify.notified() => break,
            () = tokio::time::sleep(Duration::from_millis(10)) => {
                if release_token_exists(&release_path)? {
                    break;
                }
            }
        }
    }
    let stopped = {
        let mut guard = harness.lock().await;
        session_should_stop(guard.session_mut(session_id)?)?
    };
    if stopped {
        remove_checkpoint_waiter(harness, &release_relative, &notify).await;
        return Ok(false);
    }
    let released = release_token_exists(&release_path)?;
    remove_checkpoint_waiter(harness, &release_relative, &notify).await;
    if !released {
        return Err(AhrbError::Protocol(format!(
            "checkpoint was notified without a durable release token: {}",
            release_path.display()
        )));
    }
    Ok(true)
}

fn checkpoint_waiters_for_session(harness: &MockHarness, session_id: &str) -> Vec<Arc<Notify>> {
    let prefix = format!("checkpoints/{session_id}/");
    harness
        .checkpoint_waiters
        .iter()
        .filter(|(release_token, _)| release_token.starts_with(&prefix))
        .map(|(_, notify)| Arc::clone(notify))
        .collect()
}

async fn release_checkpoint(
    harness: &Arc<Mutex<MockHarness>>,
    session_id: &str,
    release_token: &str,
) -> Result<Value> {
    let release_path = {
        let guard = harness.lock().await;
        if !guard.sessions.contains_key(session_id) {
            return Err(AhrbError::Protocol(format!(
                "unknown session {session_id:?}"
            )));
        }
        validate_release_token_path(&guard.config.state_dir, session_id, release_token)?
    };
    validate_reached_checkpoint(&release_path, session_id, release_token)?;
    let newly_created = write_release_token(&release_path)?;
    let notify = {
        let guard = harness.lock().await;
        guard.checkpoint_waiters.get(release_token).cloned()
    };
    // The file is created before looking up the waiter. A worker that registers after
    // this lookup observes the durable token in its post-registration check; a worker
    // already registered is woken here. Therefore no notification can be lost.
    if let Some(notify) = notify {
        notify.notify_one();
    }
    Ok(json!({
        "released": true,
        "idempotent": !newly_created,
        "release_token": release_token
    }))
}

fn validate_reached_checkpoint(
    release_path: &Path,
    session_id: &str,
    release_token: &str,
) -> Result<()> {
    let reached_path = release_path
        .parent()
        .ok_or_else(|| AhrbError::Validation("release token has no parent".to_owned()))?
        .join("reached.json");
    let reached: Value = match fs::read(&reached_path) {
        Ok(bytes) => serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AhrbError::Protocol(format!(
                "checkpoint has not been reached for token {release_token:?}"
            )));
        }
        Err(error) => return Err(error.into()),
    };
    if reached.get("session_id").and_then(Value::as_str) != Some(session_id)
        || reached.get("release_token").and_then(Value::as_str) != Some(release_token)
    {
        return Err(AhrbError::Protocol(
            "checkpoint evidence does not match the release request".to_owned(),
        ));
    }
    Ok(())
}

async fn remove_checkpoint_waiter(
    harness: &Arc<Mutex<MockHarness>>,
    release_token: &str,
    notify: &Arc<Notify>,
) {
    let mut guard = harness.lock().await;
    let same_waiter = guard
        .checkpoint_waiters
        .get(release_token)
        .is_some_and(|registered| Arc::ptr_eq(registered, notify));
    if same_waiter {
        guard.checkpoint_waiters.remove(release_token);
    }
}

fn release_token_exists(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(AhrbError::Protocol(format!(
            "checkpoint release token is not a file: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_release_token_path(
    state_dir: &Path,
    session_id: &str,
    release_token: &str,
) -> Result<PathBuf> {
    let relative = safe_relative(release_token)?;
    let components: Vec<_> = relative.components().collect();
    let valid = components.len() == 5
        && components[0].as_os_str() == "checkpoints"
        && components[1].as_os_str() == session_id
        && components[4].as_os_str() == "release.token";
    if !valid {
        return Err(AhrbError::Validation(
            "release token does not belong to the requested session".to_owned(),
        ));
    }
    let checkpoint_name = components[2]
        .as_os_str()
        .to_str()
        .ok_or_else(|| AhrbError::Validation("checkpoint name is not Unicode".to_owned()))?;
    validate_checkpoint_name(checkpoint_name)?;
    let token = components[3]
        .as_os_str()
        .to_str()
        .ok_or_else(|| AhrbError::Validation("checkpoint token is not Unicode".to_owned()))?;
    if token.len() != 32 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AhrbError::Validation(
            "checkpoint token must be 32 hexadecimal characters".to_owned(),
        ));
    }
    Ok(state_dir.join(relative))
}

fn write_release_token(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match OpenOptions::new().create_new(true).write(true).open(path) {
        Ok(mut file) => {
            file.write_all(b"released\n")?;
            file.sync_all()?;
            sync_parent(path)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if release_token_exists(path)? {
                Ok(false)
            } else {
                Err(AhrbError::Protocol(format!(
                    "checkpoint release token is not a file: {}",
                    path.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn checkpoint_paths(
    state_dir: &Path,
    session_id: &str,
    call_id: &str,
    checkpoint_name: &str,
) -> Result<(PathBuf, PathBuf, String)> {
    validate_checkpoint_name(checkpoint_name)?;
    let mut hasher = Sha256::new();
    hasher.update(call_id.as_bytes());
    hasher.update([0]);
    hasher.update(checkpoint_name.as_bytes());
    let digest = hasher.finalize();
    let mut token = String::with_capacity(32);
    for byte in &digest[..16] {
        token.push_str(&format!("{byte:02x}"));
    }
    let relative = PathBuf::from("checkpoints")
        .join(session_id)
        .join(checkpoint_name)
        .join(token);
    let reached = state_dir.join(&relative).join("reached.json");
    let release_relative = relative.join("release.token");
    let release = state_dir.join(&release_relative);
    Ok((
        reached,
        release,
        release_relative.to_string_lossy().into_owned(),
    ))
}

fn validate_checkpoint_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AhrbError::Validation(
            "checkpoint name must use 1..128 ASCII letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    Ok(())
}

async fn model_http_post(
    config: &MockConfig,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> Result<crate::driver::HttpResponse> {
    if let Some(engine) = &config.embedded_model {
        let frontend = OpenAiChatFrontend;
        let future = async {
            let request = frontend.parse("/v1/chat/completions", headers, body)?;
            let response = engine.handle(request).await?;
            match &response.fault {
                Some(Fault::HttpStatus { status, body }) => Ok(crate::driver::HttpResponse {
                    status: *status,
                    body: body.as_bytes().to_vec(),
                }),
                Some(Fault::Stall) => {
                    std::future::pending::<Result<crate::driver::HttpResponse>>().await
                }
                Some(Fault::MidStreamDisconnect { .. }) => Err(AhrbError::Protocol(
                    "embedded fake model injected a mid-stream disconnect".to_owned(),
                )),
                None | Some(Fault::Fragment { .. }) | Some(Fault::RepeatFrame { .. }) => {
                    let rendered = frontend.render(&response)?;
                    Ok(crate::driver::HttpResponse {
                        status: rendered.status,
                        body: rendered.body,
                    })
                }
            }
        };
        return tokio::time::timeout(config.idle_timeout, future)
            .await
            .map_err(|_| AhrbError::Timeout("embedded model idle deadline".to_owned()))?;
    }
    #[cfg(unix)]
    if let Some(socket_path) = &config.unix_socket {
        return unix_http_post(
            socket_path,
            "/v1/chat/completions",
            headers,
            body,
            config.idle_timeout,
        )
        .await;
    }
    let base_url = config
        .base_url
        .as_ref()
        .ok_or_else(|| AhrbError::Validation("mock model endpoint is not configured".to_owned()))?;
    let endpoint = format!("{base_url}/v1/chat/completions");
    http_post(&endpoint, headers, body, config.idle_timeout).await
}

fn fixture_tools() -> Value {
    json!([
        {"type":"function","function":{"name":"write_fixture","parameters":{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"},"ahrb_checkpoint":{"$ref":"#/$defs/ahrb_checkpoint"}},"required":["path","content"],"$defs":{"ahrb_checkpoint":{"type":"object","properties":{"name":{"type":"string"},"phase":{"type":"string","enum":["before-effect","after-commit"]}},"required":["name"]}}}}},
        {"type":"function","function":{"name":"read_fixture","parameters":{"type":"object","properties":{"path":{"type":"string"},"ahrb_checkpoint":{"$ref":"#/$defs/ahrb_checkpoint"}},"required":["path"],"$defs":{"ahrb_checkpoint":{"type":"object","properties":{"name":{"type":"string"},"phase":{"type":"string","enum":["before-effect","after-commit"]}},"required":["name"]}}}}},
        {"type":"function","function":{"name":"fail_fixture","parameters":{"type":"object","properties":{"message":{"type":"string"}}}}},
        {"type":"function","function":{"name":"barrier","parameters":{"type":"object","properties":{"name":{"type":"string"},"wait_for_release":{"type":"boolean"}},"required":["name"]}}}
    ])
}

fn fixture_result(state_dir: &Path, session: &str, name: &str, args: &Value) -> Result<Value> {
    match name {
        "write_fixture" | "fixture_write" => {
            safe_relative(required_str(args, "path")?)?;
            let content = required_str(args, "content")?;
            Ok(json!({ "ok": true, "bytes": content.len(), "path": required_str(args, "path")? }))
        }
        "read_fixture" | "fixture_read" => {
            let relative = safe_relative(required_str(args, "path")?)?;
            let content =
                fs::read_to_string(state_dir.join("workspaces").join(session).join(relative))?;
            Ok(json!({ "ok": true, "content": content }))
        }
        "fail_fixture" | "fixture_fail" => Ok(json!({
            "ok": false,
            "error": args.get("message").and_then(Value::as_str).unwrap_or("fixture failure")
        })),
        "barrier" => Ok(json!({
            "ok": true,
            "barrier": required_str(args, "name")?,
            "marker": args.get("marker").cloned().unwrap_or(Value::Null)
        })),
        other => Ok(json!({ "ok": false, "error": "unknown tool", "name": other })),
    }
}

fn reconcile_durable_tool_effects(
    state_dir: &Path,
    session: &str,
    events: &[NormalizedEvent],
) -> Result<()> {
    let mut reconciled_paths = BTreeSet::new();
    for event in events.iter().rev() {
        if event.event != EventVocab::ToolResult {
            continue;
        }
        let Some(call_id) = event.payload.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(name) = event.payload.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(args) = event.payload.get("arguments") else {
            // Journals created before redo records included arguments remain readable.
            continue;
        };
        let Some(result) = event.payload.get("result") else {
            continue;
        };
        if matches!(name, "write_fixture" | "fixture_write") {
            let relative = safe_relative(required_str(args, "path")?)?;
            if !reconciled_paths.insert(relative) {
                continue;
            }
        }
        reconcile_fixture_effect(state_dir, session, call_id, name, args, result)?;
    }
    Ok(())
}

fn reconcile_fixture_effect(
    state_dir: &Path,
    session: &str,
    call_id: &str,
    name: &str,
    args: &Value,
    result: &Value,
) -> Result<()> {
    if !matches!(name, "write_fixture" | "fixture_write") {
        return Ok(());
    }
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }
    let relative = safe_relative(required_str(args, "path")?)?;
    let content = required_str(args, "content")?;
    let path = state_dir.join("workspaces").join(session).join(relative);
    match fs::read(&path) {
        Ok(existing) if existing == content.as_bytes() => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = path.parent().ok_or_else(|| {
        AhrbError::Validation("fixture destination has no parent directory".to_owned())
    })?;
    fs::create_dir_all(parent)?;
    let digest = Sha256::digest(call_id.as_bytes());
    let mut suffix = String::with_capacity(16);
    for byte in &digest[..8] {
        suffix.push_str(&format!("{byte:02x}"));
    }
    let sequence = RECONCILE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".ahrb-write-{suffix}-{}-{sequence}.tmp",
        std::process::id()
    ));
    write_replace_synced(&temporary, content.as_bytes())?;
    #[cfg(not(unix))]
    if path.exists() {
        fs::remove_file(&path)?;
    }
    fs::rename(&temporary, &path)?;
    sync_directory(parent)?;
    Ok(())
}

fn tool_result_for(session: &SessionState, call_id: &str) -> Result<Option<Value>> {
    Ok(session
        .journal
        .all()?
        .into_iter()
        .find(|event| {
            event.event == EventVocab::ToolResult
                && event.payload.get("call_id").and_then(Value::as_str) == Some(call_id)
        })
        .and_then(|event| event.payload.get("result").cloned()))
}

fn terminal_from_content(content: &Value) -> (EventVocab, &'static str) {
    let status = content
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|value| {
            value
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            content
                .get("status")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| content.as_str().unwrap_or("SUCCESS").to_owned());
    if status.to_ascii_uppercase().contains("FAIL") {
        (EventVocab::TerminalFailure, "failure")
    } else {
        (EventVocab::TerminalSuccess, "success")
    }
}

async fn run_hook_and_record(
    harness: &Arc<Mutex<MockHarness>>,
    id: &str,
    kind: &str,
    turn_key: &str,
    argv: &[String],
) -> Result<()> {
    if argv.is_empty() {
        return Ok(());
    }
    let already = {
        let guard = harness.lock().await;
        guard
            .sessions
            .get(id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?
            .journal
            .all()?
            .iter()
            .any(|event| {
                event.event == EventVocab::HookCompleted
                    && event.payload.get("kind").and_then(Value::as_str) == Some(kind)
                    && event.payload.get("turn_key").and_then(Value::as_str) == Some(turn_key)
            })
    };
    if already {
        return Ok(());
    }
    let (claim_path, hook_id) = {
        let guard = harness.lock().await;
        let hook_id = stable_hook_id(id, kind, turn_key);
        let claim_path = guard
            .config
            .state_dir
            .join("sessions")
            .join(id)
            .join("hook-claims")
            .join(format!("{hook_id}.claim"));
        (claim_path, hook_id)
    };
    if !claim_hook(&claim_path)? {
        return Err(AhrbError::Protocol(format!(
            "{kind} hook has an incomplete durable claim; refusing to fire it again"
        )));
    }
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| AhrbError::Validation("hook argv is empty".to_owned()))?;
    let status = tokio::process::Command::new(program)
        .args(args)
        .env_clear()
        .env("AHRB_SESSION_ID", id)
        .env("AHRB_HOOK_KIND", kind)
        .env("AHRB_TURN_KEY", turn_key)
        .env("AHRB_HOOK_ID", hook_id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !status.success() {
        return Err(AhrbError::Protocol(format!(
            "{kind} hook exited with {status}"
        )));
    }
    harness.lock().await.append(
        id,
        EventVocab::HookCompleted,
        json!({ "kind": kind, "turn_key": turn_key }),
    )?;
    Ok(())
}

fn stable_hook_id(session_id: &str, kind: &str, turn_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(session_id.as_bytes());
    hasher.update([0]);
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(turn_key.as_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(32);
    for byte in &digest[..16] {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn claim_hook(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match OpenOptions::new().create_new(true).write(true).open(path) {
        Ok(file) => {
            file.sync_all()?;
            sync_parent(path)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn inspect_journal(args: &[String]) -> Result<i32> {
    let (state_dir, session, after) = parse_inspect_args(args)?;
    let journal = DurableJournal::open(
        state_dir
            .join("sessions")
            .join(session)
            .join("journal.jsonl"),
    )?;
    for event in journal.read_after(after)? {
        println!("{}", serde_json::to_string(&event)?);
    }
    Ok(0)
}

fn parse_inspect_args(args: &[String]) -> Result<(PathBuf, String, Option<u64>)> {
    let mut state_dir = None;
    let mut session = None;
    let mut after = None;
    let mut index = 0;
    while index < args.len() {
        let option = &args[index];
        index += 1;
        let value = args
            .get(index)
            .ok_or_else(|| AhrbError::Usage(format!("{option} needs a value")))?;
        match option.as_str() {
            "--state-dir" => state_dir = Some(PathBuf::from(value)),
            "--session" => session = Some(value.clone()),
            "--after" => {
                after = Some(
                    value
                        .parse()
                        .map_err(|_| AhrbError::Usage("invalid cursor".to_owned()))?,
                )
            }
            other => return Err(AhrbError::Usage(format!("unknown option {other:?}"))),
        }
        index += 1;
    }
    Ok((
        state_dir.ok_or_else(|| AhrbError::Usage("--state-dir is required".to_owned()))?,
        session.ok_or_else(|| AhrbError::Usage("--session is required".to_owned()))?,
        after,
    ))
}

fn hook_command(args: &[String]) -> Result<i32> {
    let path = args
        .windows(2)
        .find(|pair| pair[0] == "--path")
        .map(|pair| PathBuf::from(&pair[1]))
        .ok_or_else(|| AhrbError::Usage("hook --path FILE is required".to_owned()))?;
    let record = json!({
        "session_id": std::env::var("AHRB_SESSION_ID").unwrap_or_default(),
        "kind": std::env::var("AHRB_HOOK_KIND").unwrap_or_default()
    });
    let mut bytes = serde_json::to_vec(&record)?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent(&path)?;
    Ok(0)
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AhrbError::Protocol(format!("missing string field {key:?}")))
}

fn session_is_terminal(session: &SessionState) -> Result<bool> {
    let events = session.journal.all()?;
    let accepted_cursor = events
        .iter()
        .rev()
        .find(|event| event.event == EventVocab::TurnAccepted)
        .map(|event| event.cursor)
        .unwrap_or(0);
    Ok(events
        .iter()
        .any(|event| event.cursor > accepted_cursor && is_terminal(&event.event)))
}

fn session_should_stop(session: &SessionState) -> Result<bool> {
    Ok(session.cancelled || session.closed || session_is_terminal(session)?)
}

fn is_terminal(event: &EventVocab) -> bool {
    matches!(
        event,
        EventVocab::TerminalSuccess | EventVocab::TerminalFailure | EventVocab::TerminalCancelled
    )
}

fn stable_session_id(marker: &str) -> String {
    let digest = Sha256::digest(marker.as_bytes());
    let mut encoded = String::with_capacity(21);
    encoded.push_str("mock-");
    for byte in &digest[..8] {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn validate_marker(marker: &str) -> Result<()> {
    if marker.is_empty() || marker.len() > 4096 || marker.contains('\0') {
        return Err(AhrbError::Validation("invalid actor marker".to_owned()));
    }
    Ok(())
}

fn validate_session_id(id: &str) -> Result<()> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid {
        return Err(AhrbError::Validation(
            "session ID must use 1..128 ASCII letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    Ok(())
}

fn safe_relative(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(AhrbError::Validation(
            "fixture path must be non-empty and relative".to_owned(),
        ));
    }
    let safe = path
        .components()
        .all(|component| matches!(component, Component::Normal(_)));
    if !safe {
        return Err(AhrbError::Validation(
            "fixture path escapes the session workspace".to_owned(),
        ));
    }
    Ok(path.to_path_buf())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let sequence = NEW_FILE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(format!(".ahrb-new-{}-{sequence}.tmp", std::process::id()));
    let temporary = PathBuf::from(temporary);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let publish_result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, path)?;
        Ok(())
    })();
    let cleanup_result = fs::remove_file(&temporary);
    publish_result?;
    cleanup_result?;
    sync_parent(path)
}

fn write_replace_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let sequence = REPLACE_FILE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(format!(
        ".ahrb-replace-{}-{sequence}.tmp",
        std::process::id()
    ));
    let temporary = PathBuf::from(temporary);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let publish_result = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        #[cfg(not(unix))]
        if path.exists() {
            fs::remove_file(path)?;
        }
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if publish_result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    publish_result
}

fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ahrb-mock-{name}-{}", std::process::id()))
    }

    fn test_config(state_dir: PathBuf) -> MockConfig {
        MockConfig {
            state_dir,
            base_url: None,
            unix_socket: None,
            embedded_model: None,
            api_key: None,
            model: "ahrb-fake-v1".to_owned(),
            idle_timeout: Duration::from_millis(250),
            session_memory_bytes: DEFAULT_SESSION_MEMORY_MIB * MIB,
            acceptance_hook: Vec::new(),
            completion_hook: Vec::new(),
        }
    }

    #[test]
    fn synced_replacement_leaves_only_the_complete_published_file() {
        let directory = temporary_dir("atomic-replace");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir(&directory).expect("create atomic-replace test directory");
        let path = directory.join("daemon.pid");
        write_replace_synced(&path, b"12345").expect("publish initial PID locator");
        write_replace_synced(&path, b"67890").expect("replace PID locator");
        assert_eq!(
            fs::read(&path).expect("read replaced PID locator"),
            b"67890"
        );
        let entries = fs::read_dir(&directory)
            .expect("read atomic-replace test directory")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect atomic-replace test entries");
        assert_eq!(entries.len(), 1, "replacement temporary file leaked");
        std::fs::remove_dir_all(directory).expect("remove atomic-replace test directory");
    }

    #[test]
    fn journal_replay_is_strictly_after_cursor_and_ignores_torn_tail() {
        let directory = temporary_dir("journal");
        let _ = fs::remove_dir_all(&directory);
        let journal = DurableJournal::open(directory.join("journal.jsonl")).expect("journal");
        for cursor in 1..=2 {
            journal
                .append(&NormalizedEvent {
                    id: format!("s:{cursor}"),
                    cursor,
                    session_id: "s".to_owned(),
                    actor: "a".to_owned(),
                    event: EventVocab::ModelRequest,
                    payload: json!({}),
                })
                .expect("append");
        }
        OpenOptions::new()
            .append(true)
            .open(&journal.path)
            .expect("open")
            .write_all(b"{\"torn\":")
            .expect("torn tail");
        let events = journal.read_after(Some(1)).expect("replay");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn fixture_paths_cannot_escape() {
        assert!(safe_relative("safe/file.txt").is_ok());
        assert!(safe_relative("../outside").is_err());
        assert!(safe_relative("/absolute").is_err());
    }

    #[test]
    fn checkpoint_release_tokens_are_session_scoped() {
        let state = Path::new("/tmp/mock-state");
        let valid = "checkpoints/session-a/steady/0123456789abcdef0123456789abcdef/release.token";
        assert!(validate_release_token_path(state, "session-a", valid).is_ok());
        assert!(validate_release_token_path(state, "session-b", valid).is_err());
        assert!(
            validate_release_token_path(
                state,
                "session-a",
                "checkpoints/session-a/steady/not-hex/release.token"
            )
            .is_err()
        );
        assert!(validate_release_token_path(state, "session-a", "../release.token").is_err());
    }

    #[test]
    fn session_ids_are_deterministic() {
        assert_eq!(stable_session_id("actor"), stable_session_id("actor"));
        assert_ne!(stable_session_id("actor"), stable_session_id("other"));
    }

    #[test]
    fn reference_manifest_parses() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("adapters/mock/manifest.toml");
        let manifest = crate::manifest::load(&path).expect("parse reference manifest");
        assert_eq!(manifest.identity.id, "ahrb-mock");
    }

    #[test]
    fn startup_redoes_a_committed_tool_effect_without_duplicate_result() -> Result<()> {
        let directory = temporary_dir("redo-tool-effect");
        let _ = fs::remove_dir_all(&directory);
        let config = test_config(directory.clone());
        let mut harness = MockHarness::open(config.clone())?;
        let session = harness.create_session("redo-actor")?;
        let args = json!({ "path": "nested/effect.txt", "content": "durable" });
        let result = fixture_result(&directory, &session, "write_fixture", &args)?;
        harness.append(
            &session,
            EventVocab::ToolCall,
            json!({ "call_id": "call-redo", "name": "write_fixture", "arguments": args }),
        )?;
        harness.append(
            &session,
            EventVocab::ToolResult,
            json!({
                "call_id": "call-redo",
                "name": "write_fixture",
                "arguments": args,
                "result": result
            }),
        )?;
        let effect_path = directory
            .join("workspaces")
            .join(&session)
            .join("nested/effect.txt");
        assert!(
            !effect_path.exists(),
            "test must model the pre-effect kill window"
        );
        drop(harness);

        let reopened = MockHarness::open(config.clone())?;
        assert_eq!(fs::read_to_string(&effect_path)?, "durable");
        let events = reopened
            .sessions
            .get(&session)
            .ok_or_else(|| AhrbError::Protocol("reopened session missing".to_owned()))?
            .journal
            .all()?;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count(),
            1
        );
        drop(reopened);
        fs::write(&effect_path, "damaged-after-crash")?;
        let reopened_again = MockHarness::open(config)?;
        assert_eq!(fs::read_to_string(&effect_path)?, "durable");
        assert_eq!(
            reopened_again
                .sessions
                .get(&session)
                .ok_or_else(|| AhrbError::Protocol("reopened session missing".to_owned()))?
                .journal
                .all()?
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count(),
            1
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn concurrent_reconciliation_uses_unique_temporary_files() -> Result<()> {
        let directory = temporary_dir("concurrent-reconcile");
        let _ = fs::remove_dir_all(&directory);
        let barrier = Arc::new(std::sync::Barrier::new(33));
        let args = json!({ "path": "nested/effect.txt", "content": "durable" });
        let result = json!({ "ok": true });
        let mut workers = Vec::new();
        for _ in 0..32 {
            let worker_directory = directory.clone();
            let worker_barrier = Arc::clone(&barrier);
            let worker_args = args.clone();
            let worker_result = result.clone();
            workers.push(std::thread::spawn(move || {
                worker_barrier.wait();
                reconcile_fixture_effect(
                    &worker_directory,
                    "session",
                    "shared-call",
                    "write_fixture",
                    &worker_args,
                    &worker_result,
                )
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().expect("reconciliation worker panicked")?;
        }
        assert_eq!(
            fs::read_to_string(
                directory
                    .join("workspaces/session")
                    .join("nested/effect.txt")
            )?,
            "durable"
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_hook_claim_is_never_refired() -> Result<()> {
        let directory = temporary_dir("hook-claim");
        let _ = fs::remove_dir_all(&directory);
        let config = test_config(directory.clone());
        let mut harness = MockHarness::open(config)?;
        let session = harness.create_session("hook-actor")?;
        let hook_id = stable_hook_id(&session, "completion", "turn-1");
        let claim_path = directory
            .join("sessions")
            .join(&session)
            .join("hook-claims")
            .join(format!("{hook_id}.claim"));
        assert!(claim_hook(&claim_path)?);
        assert!(!claim_hook(&claim_path)?);
        let shared = Arc::new(Mutex::new(harness));
        let error = run_hook_and_record(
            &shared,
            &session,
            "completion",
            "turn-1",
            &["this-program-must-not-be-launched".to_owned()],
        )
        .await
        .expect_err("an indeterminate hook claim must stop replay");
        assert!(error.to_string().contains("refusing to fire it again"));
        let events = shared
            .lock()
            .await
            .sessions
            .get(&session)
            .ok_or_else(|| AhrbError::Protocol("hook test session missing".to_owned()))?
            .journal
            .all()?;
        assert!(
            events
                .iter()
                .all(|event| event.event != EventVocab::HookCompleted)
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_wins_over_a_late_model_timeout() -> Result<()> {
        use crate::workflow::{Actor, ScriptedResponse, WORKFLOW_SCHEMA_VERSION};

        let directory = temporary_dir("cancel-race");
        let _ = fs::remove_dir_all(&directory);
        let marker = "[[AHRB:scenario=cancel-race;actor=root;checkpoint=start]]";
        let workflow = Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario: "cancel-race".to_owned(),
            actors: BTreeMap::from([(
                "root".to_owned(),
                Actor {
                    id: "root".to_owned(),
                    parent: None,
                    prompt: marker.to_owned(),
                    workspace: "root".to_owned(),
                },
            )]),
            barriers: BTreeMap::new(),
            responses: vec![ScriptedResponse {
                scenario: "cancel-race".to_owned(),
                actor: "root".to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({ "text": "too late" }),
                fault: Some(Fault::Stall),
                barrier: None,
            }],
        };
        let mut config = test_config(directory.clone());
        config.embedded_model = Some(Arc::new(FakeModelEngine::new(&workflow)?));
        let harness = Arc::new(Mutex::new(MockHarness::open(config)?));
        let session = harness.lock().await.create_session("cancel-actor")?;
        assert!(
            accept_turn(
                &harness,
                &session,
                PendingTurn {
                    prompt: marker.to_owned(),
                    key: "turn-1".to_owned(),
                },
                false,
                None,
            )
            .await?
        );
        spawn_worker(Arc::clone(&harness), session.clone());
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                let has_request = {
                    let guard = harness.lock().await;
                    guard
                        .sessions
                        .get(&session)
                        .ok_or_else(|| {
                            AhrbError::Protocol("cancel test session missing".to_owned())
                        })?
                        .journal
                        .all()?
                        .iter()
                        .any(|event| event.event == EventVocab::ModelRequest)
                };
                if has_request {
                    return Ok::<(), AhrbError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| AhrbError::Timeout("waiting for model request".to_owned()))??;
        handle_rpc(
            Arc::clone(&harness),
            RpcRequest {
                jsonrpc: "2.0".to_owned(),
                id: json!(1),
                method: "session.cancel".to_owned(),
                params: json!({ "session_id": session }),
            },
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let events = harness
            .lock()
            .await
            .sessions
            .get(&session)
            .ok_or_else(|| AhrbError::Protocol("cancel test session missing".to_owned()))?
            .journal
            .all()?;
        let terminals: Vec<_> = events
            .iter()
            .filter(|event| is_terminal(&event.event))
            .collect();
        assert_eq!(terminals.len(), 1);
        assert_eq!(terminals[0].event, EventVocab::TerminalCancelled);
        assert!(
            events
                .iter()
                .all(|event| event.event != EventVocab::ModelResponse)
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    async fn wait_for_barriers(
        harness: &Arc<Mutex<MockHarness>>,
        session: &str,
        count: usize,
    ) -> Result<Vec<NormalizedEvent>> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = {
                    let guard = harness.lock().await;
                    guard
                        .sessions
                        .get(session)
                        .ok_or_else(|| {
                            AhrbError::Protocol("checkpoint test session missing".to_owned())
                        })?
                        .journal
                        .all()?
                };
                if events
                    .iter()
                    .filter(|event| event.event == EventVocab::BarrierReached)
                    .count()
                    >= count
                {
                    return Ok::<_, AhrbError>(events);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| AhrbError::Timeout("waiting for fixture checkpoints".to_owned()))?
    }

    async fn release_checkpoint_event(
        harness: &Arc<Mutex<MockHarness>>,
        event: &NormalizedEvent,
    ) -> Result<()> {
        let release_token = event
            .payload
            .get("release_token")
            .and_then(Value::as_str)
            .ok_or_else(|| AhrbError::Protocol("barrier omitted release token".to_owned()))?;
        let result = release_checkpoint(harness, &event.session_id, release_token).await?;
        if result.get("released").and_then(Value::as_bool) != Some(true) {
            return Err(AhrbError::Protocol(
                "checkpoint release was not acknowledged".to_owned(),
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn two_fixture_calls_are_live_and_can_finish_in_reverse_order() -> Result<()> {
        let directory = temporary_dir("parallel-checkpoints");
        let _ = fs::remove_dir_all(&directory);
        let config = test_config(directory.clone());
        let mut harness = MockHarness::open(config.clone())?;
        let session = harness.create_session("parallel-checkpoint-actor")?;
        harness.append(
            &session,
            EventVocab::TurnAccepted,
            json!({ "prompt": "parallel", "key": "turn-1" }),
        )?;
        let shared = Arc::new(Mutex::new(harness));
        let first_args = json!({ "name": "first", "wait_for_release": true });
        let second_args = json!({ "name": "second", "wait_for_release": true });
        let first_checkpoint = fixture_checkpoint("barrier", &first_args)?
            .ok_or_else(|| AhrbError::Protocol("first checkpoint was not parsed".to_owned()))?;
        let second_checkpoint = fixture_checkpoint("barrier", &second_args)?
            .ok_or_else(|| AhrbError::Protocol("second checkpoint was not parsed".to_owned()))?;
        let first = tokio::spawn(execute_prepared_tool_call(
            Arc::clone(&shared),
            config.clone(),
            session.clone(),
            "call-first".to_owned(),
            "barrier".to_owned(),
            first_args,
            None,
            Some(first_checkpoint),
        ));
        let second = tokio::spawn(execute_prepared_tool_call(
            Arc::clone(&shared),
            config,
            session.clone(),
            "call-second".to_owned(),
            "barrier".to_owned(),
            second_args,
            None,
            Some(second_checkpoint),
        ));
        let reached = wait_for_barriers(&shared, &session, 2).await?;
        assert!(
            reached
                .iter()
                .all(|event| event.event != EventVocab::ToolResult),
            "both calls must be live before either release"
        );
        let second_barrier = reached
            .iter()
            .find(|event| {
                event.payload.get("call_id").and_then(Value::as_str) == Some("call-second")
            })
            .ok_or_else(|| AhrbError::Protocol("second barrier missing".to_owned()))?;
        release_checkpoint_event(&shared, second_barrier).await?;
        let after_second = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = {
                    let guard = shared.lock().await;
                    guard
                        .sessions
                        .get(&session)
                        .ok_or_else(|| AhrbError::Protocol("parallel session missing".to_owned()))?
                        .journal
                        .all()?
                };
                if events.iter().any(|event| {
                    event.event == EventVocab::ToolResult
                        && event.payload.get("call_id").and_then(Value::as_str)
                            == Some("call-second")
                }) {
                    return Ok::<_, AhrbError>(events);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| AhrbError::Timeout("waiting for reverse completion".to_owned()))??;
        assert!(!after_second.iter().any(|event| {
            event.event == EventVocab::ToolResult
                && event.payload.get("call_id").and_then(Value::as_str) == Some("call-first")
        }));
        let first_barrier = reached
            .iter()
            .find(|event| {
                event.payload.get("call_id").and_then(Value::as_str) == Some("call-first")
            })
            .ok_or_else(|| AhrbError::Protocol("first barrier missing".to_owned()))?;
        release_checkpoint_event(&shared, first_barrier).await?;
        first
            .await
            .map_err(|error| AhrbError::Protocol(format!("first fixture task: {error}")))??;
        second
            .await
            .map_err(|error| AhrbError::Protocol(format!("second fixture task: {error}")))??;
        let final_events = shared
            .lock()
            .await
            .sessions
            .get(&session)
            .ok_or_else(|| AhrbError::Protocol("parallel session missing".to_owned()))?
            .journal
            .all()?;
        let result_ids: Vec<_> = final_events
            .iter()
            .filter(|event| event.event == EventVocab::ToolResult)
            .filter_map(|event| event.payload.get("call_id").and_then(Value::as_str))
            .collect();
        assert_eq!(result_ids, vec!["call-second", "call-first"]);
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn closing_a_held_checkpoint_wakes_without_file_polling() -> Result<()> {
        let directory = temporary_dir("close-held-checkpoint");
        let _ = fs::remove_dir_all(&directory);
        let mut config = test_config(directory.clone());
        config.session_memory_bytes = MIB;
        let mut harness = MockHarness::open(config.clone())?;
        let session = harness.create_session("close-held-actor")?;
        harness.append(
            &session,
            EventVocab::TurnAccepted,
            json!({ "prompt": "held", "key": "turn-1" }),
        )?;
        {
            let state = harness.session_mut(&session)?;
            state.active = true;
            state.resource_reservation = Some(ResourceReservation::new(MIB, 1)?);
        }
        let args = json!({
            "path": "held.txt",
            "content": "committed",
            "ahrb_checkpoint": { "name": "held-close", "phase": "after-commit" }
        });
        let checkpoint = fixture_checkpoint("write_fixture", &args)?
            .ok_or_else(|| AhrbError::Protocol("held-close checkpoint missing".to_owned()))?;
        let shared = Arc::new(Mutex::new(harness));
        let worker = tokio::spawn(execute_prepared_tool_call(
            Arc::clone(&shared),
            config,
            session.clone(),
            "call-held-close".to_owned(),
            "write_fixture".to_owned(),
            args,
            None,
            Some(checkpoint),
        ));
        let reached = wait_for_barriers(&shared, &session, 1).await?;
        assert!(reached.iter().any(|event| {
            event.event == EventVocab::BarrierReached
                && event.payload.get("phase").and_then(Value::as_str) == Some("after-commit")
        }));
        let closed = handle_rpc(
            Arc::clone(&shared),
            RpcRequest {
                jsonrpc: "2.0".to_owned(),
                id: json!(1),
                method: "session.close".to_owned(),
                params: json!({ "session_id": session }),
            },
        )
        .await?;
        assert_eq!(closed["released_bytes"], MIB);
        let outcome = tokio::time::timeout(Duration::from_millis(100), worker)
            .await
            .map_err(|_| AhrbError::Timeout("closing held checkpoint task".to_owned()))?
            .map_err(|error| AhrbError::Protocol(format!("held checkpoint task: {error}")))??;
        assert!(outcome.is_none());
        assert!(
            reached.iter().all(|event| {
                event
                    .payload
                    .get("release_token")
                    .and_then(Value::as_str)
                    .is_none_or(|relative| !directory.join(relative).exists())
            }),
            "close must wake the task without fabricating release evidence"
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn post_commit_checkpoint_survives_kill_and_resume() -> Result<()> {
        let directory = temporary_dir("post-commit-resume");
        let _ = fs::remove_dir_all(&directory);
        let config = test_config(directory.clone());
        let mut harness = MockHarness::open(config.clone())?;
        let session = harness.create_session("post-commit-actor")?;
        harness.append(
            &session,
            EventVocab::TurnAccepted,
            json!({ "prompt": "post commit", "key": "turn-1" }),
        )?;
        let args = json!({
            "path": "committed.txt",
            "content": "once",
            "ahrb_checkpoint": { "name": "post-commit", "phase": "after-commit" }
        });
        let checkpoint = fixture_checkpoint("write_fixture", &args)?
            .ok_or_else(|| AhrbError::Protocol("post-commit checkpoint missing".to_owned()))?;
        let shared = Arc::new(Mutex::new(harness));
        let held = tokio::spawn(execute_prepared_tool_call(
            Arc::clone(&shared),
            config.clone(),
            session.clone(),
            "call-post-commit".to_owned(),
            "write_fixture".to_owned(),
            args.clone(),
            None,
            Some(checkpoint.clone()),
        ));
        let before_kill = wait_for_barriers(&shared, &session, 1).await?;
        assert_eq!(
            before_kill
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count(),
            1,
            "post-commit checkpoint must follow the durable result"
        );
        held.abort();
        let _ = held.await;
        drop(shared);

        let reopened = MockHarness::open(config.clone())?;
        let duplicate = tool_result_for(
            reopened
                .sessions
                .get(&session)
                .ok_or_else(|| AhrbError::Protocol("recovered session missing".to_owned()))?,
            "call-post-commit",
        )?
        .ok_or_else(|| AhrbError::Protocol("durable result missing".to_owned()))?;
        let recovered = Arc::new(Mutex::new(reopened));
        let resumed = tokio::spawn(execute_prepared_tool_call(
            Arc::clone(&recovered),
            config,
            session.clone(),
            "call-post-commit".to_owned(),
            "write_fixture".to_owned(),
            args,
            Some(duplicate),
            Some(checkpoint),
        ));
        let recovered_events = wait_for_barriers(&recovered, &session, 1).await?;
        let barrier = recovered_events
            .iter()
            .find(|event| event.event == EventVocab::BarrierReached)
            .ok_or_else(|| AhrbError::Protocol("recovered barrier missing".to_owned()))?;
        release_checkpoint_event(&recovered, barrier).await?;
        resumed
            .await
            .map_err(|error| AhrbError::Protocol(format!("resumed fixture task: {error}")))??;
        let final_events = recovered
            .lock()
            .await
            .sessions
            .get(&session)
            .ok_or_else(|| AhrbError::Protocol("recovered session missing".to_owned()))?
            .journal
            .all()?;
        assert_eq!(
            final_events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .count(),
            1
        );
        assert_eq!(
            final_events
                .iter()
                .filter(|event| event.event == EventVocab::BarrierReached)
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(
                directory
                    .join("workspaces")
                    .join(&session)
                    .join("committed.txt")
            )?,
            "once"
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_sessions_do_fixture_work_hold_and_reclaim() -> Result<()> {
        use crate::workflow::{Actor, Barrier, ScriptedResponse, WORKFLOW_SCHEMA_VERSION};

        const AGENTS: usize = 4;
        const TEST_RESERVATION_BYTES: u64 = 2 * MIB;

        let directory = temporary_dir("resource-surface");
        let _ = fs::remove_dir_all(&directory);
        let scenario = "mock-resource-surface";
        let marker = |actor: &str, checkpoint: &str| {
            format!("[[AHRB:scenario={scenario};actor={actor};checkpoint={checkpoint}]]")
        };
        let mut actors = BTreeMap::new();
        let mut responses = Vec::new();
        let mut actor_ids = Vec::new();
        for index in 1..=AGENTS {
            let actor = format!("agent-{index}");
            actor_ids.push(actor.clone());
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
                            "path": "resource-fixture.txt",
                            "content": format!(
                                "agent-{index} {}",
                                marker(&actor, "held")
                            ),
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
                checkpoint: "held".to_owned(),
                request_hash: String::new(),
                response: json!({ "text": "AHRB_SUCCESS resource surface" }),
                fault: None,
                barrier: Some("resource-steady".to_owned()),
            });
        }
        let workflow = Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario: scenario.to_owned(),
            actors,
            barriers: BTreeMap::from([(
                "resource-steady".to_owned(),
                Barrier {
                    name: "resource-steady".to_owned(),
                    actors: actor_ids.clone(),
                    checkpoint: "held".to_owned(),
                },
            )]),
            responses,
        };
        let engine = Arc::new(FakeModelEngine::new(&workflow)?);
        let mut config = test_config(directory.clone());
        config.embedded_model = Some(Arc::clone(&engine));
        config.idle_timeout = Duration::from_secs(2);
        config.session_memory_bytes = TEST_RESERVATION_BYTES;
        let harness = Arc::new(Mutex::new(MockHarness::open(config)?));

        let mut sessions = Vec::new();
        for actor in &actor_ids {
            let id = harness.lock().await.create_session(actor)?;
            let prompt = workflow
                .actors
                .get(actor)
                .ok_or_else(|| AhrbError::Protocol("resource actor missing".to_owned()))?
                .prompt
                .clone();
            assert!(
                accept_turn(
                    &harness,
                    &id,
                    PendingTurn {
                        prompt,
                        key: "resource-turn".to_owned(),
                    },
                    false,
                    None,
                )
                .await?
            );
            spawn_worker(Arc::clone(&harness), id.clone());
            sessions.push(id);
        }

        // This first state barrier belongs to the harness and works even when the
        // runner and daemon each host a separate embedded fake-model engine. Every
        // reached event is durable and includes the run-local release-token path.
        let mut checkpoint_events = Vec::new();
        for id in &sessions {
            let events = wait_for_barriers(&harness, id, 1).await?;
            let checkpoint = events
                .iter()
                .find(|event| event.event == EventVocab::BarrierReached)
                .cloned()
                .ok_or_else(|| {
                    AhrbError::Protocol("resource checkpoint event missing".to_owned())
                })?;
            let tool_result_cursor = events
                .iter()
                .find(|event| event.event == EventVocab::ToolResult)
                .map(|event| event.cursor)
                .ok_or_else(|| AhrbError::Protocol("resource tool result missing".to_owned()))?;
            assert!(tool_result_cursor < checkpoint.cursor);
            checkpoint_events.push(checkpoint);
        }
        {
            let guard = harness.lock().await;
            for id in &sessions {
                let session = guard.sessions.get(id).ok_or_else(|| {
                    AhrbError::Protocol("resource session disappeared".to_owned())
                })?;
                assert!(
                    session.active,
                    "agent must remain live at the model barrier"
                );
                assert_eq!(
                    session.resource_reservation.as_ref().map(|item| item.len()),
                    Some(TEST_RESERVATION_BYTES)
                );
                let events = session.journal.all()?;
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| event.event == EventVocab::ToolResult)
                        .count(),
                    1,
                    "every agent must finish its fixture op before the shared hold"
                );
                assert!(
                    directory
                        .join("workspaces")
                        .join(id)
                        .join("resource-fixture.txt")
                        .is_file()
                );
            }
        }

        for checkpoint in &checkpoint_events {
            release_checkpoint_event(&harness, checkpoint).await?;
        }
        // The same sessions can then reach a fake-model-owned shared barrier when the
        // engine is in-process. This exercises both resource-runner integration modes.
        tokio::time::timeout(
            Duration::from_secs(1),
            engine.barriers().wait_until_ready("resource-steady"),
        )
        .await
        .map_err(|_| AhrbError::Timeout("resource agents reaching model barrier".to_owned()))??;
        {
            let guard = harness.lock().await;
            assert!(sessions.iter().all(|id| {
                guard
                    .sessions
                    .get(id)
                    .is_some_and(|session| session.resource_reservation.is_some())
            }));
        }

        engine.barriers().release("resource-steady").await?;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let complete = {
                    let guard = harness.lock().await;
                    sessions.iter().try_fold(true, |complete, id| {
                        let session = guard.sessions.get(id).ok_or_else(|| {
                            AhrbError::Protocol("resource session disappeared".to_owned())
                        })?;
                        Ok::<_, AhrbError>(complete && session_is_terminal(session)?)
                    })?
                };
                if complete {
                    return Ok::<_, AhrbError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| AhrbError::Timeout("resource agents terminalizing".to_owned()))??;

        for (request_id, id) in sessions.iter().enumerate() {
            let result = handle_rpc(
                Arc::clone(&harness),
                RpcRequest {
                    jsonrpc: "2.0".to_owned(),
                    id: json!(request_id),
                    method: "session.close".to_owned(),
                    params: json!({ "session_id": id }),
                },
            )
            .await?;
            assert_eq!(result["released_bytes"], TEST_RESERVATION_BYTES);
        }
        {
            let guard = harness.lock().await;
            assert!(
                guard
                    .sessions
                    .values()
                    .all(|session| { session.closed && session.resource_reservation.is_none() })
            );
        }
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mock_turn_reaches_fake_model_over_unix_http() -> Result<()> {
        use crate::fake_model::{FakeModelEngine, FakeModelUnixServer};
        use crate::workflow::{Actor, ScriptedResponse, WORKFLOW_SCHEMA_VERSION, Workflow};

        let _listener_guard = crate::fake_model::LOCAL_SERVER_TEST_LOCK.lock().await;
        let _process_guard = crate::fake_model::acquire_process_server_test_lock()?;
        #[cfg(target_os = "macos")]
        let temporary_root = Path::new("/private/tmp");
        #[cfg(not(target_os = "macos"))]
        let temporary_root = Path::new("/tmp");
        let directory = temporary_root.join(format!("ahrb-mhu-{}", std::process::id()));
        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::create_dir(&directory)?;
        let marker = "[[AHRB:scenario=unix-mock;actor=root;checkpoint=start]]";
        let workflow = Workflow {
            version: WORKFLOW_SCHEMA_VERSION,
            scenario: "unix-mock".to_owned(),
            actors: BTreeMap::from([(
                "root".to_owned(),
                Actor {
                    id: "root".to_owned(),
                    parent: None,
                    prompt: marker.to_owned(),
                    workspace: "root".to_owned(),
                },
            )]),
            barriers: BTreeMap::new(),
            responses: vec![ScriptedResponse {
                scenario: "unix-mock".to_owned(),
                actor: "root".to_owned(),
                checkpoint: "start".to_owned(),
                request_hash: String::new(),
                response: json!({"text":"AHRB_SUCCESS unix-mock"}),
                fault: None,
                barrier: None,
            }],
        };
        let engine = Arc::new(FakeModelEngine::new(&workflow)?);
        let bound = FakeModelUnixServer::bind(directory.join("fm.sock"), Arc::clone(&engine)).await;
        let server = match bound {
            Ok(server) => server,
            Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                // Some managed sandboxes grant only one listener bind per test process.
                fs::remove_dir_all(&directory)?;
                return Ok(());
            }
            Err(error) => {
                return Err(AhrbError::Protocol(format!("test Unix bind: {error}")));
            }
        };
        let config = MockConfig {
            state_dir: directory.join("state"),
            base_url: None,
            unix_socket: Some(server.socket_path().to_path_buf()),
            embedded_model: None,
            api_key: Some("unix-secret".to_owned()),
            model: "ahrb-fake-v1".to_owned(),
            idle_timeout: Duration::from_secs(2),
            session_memory_bytes: DEFAULT_SESSION_MEMORY_MIB * MIB,
            acceptance_hook: Vec::new(),
            completion_hook: Vec::new(),
        };
        let harness =
            Arc::new(Mutex::new(MockHarness::open(config).map_err(|error| {
                AhrbError::Protocol(format!("test mock open: {error}"))
            })?));
        let session = harness.lock().await.create_session("root")?;
        let accepted = accept_turn(
            &harness,
            &session,
            PendingTurn {
                prompt: marker.to_owned(),
                key: "turn-1".to_owned(),
            },
            false,
            None,
        )
        .await?;
        assert!(accepted);
        spawn_worker(Arc::clone(&harness), session.clone());
        let events = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let journal = {
                    let guard = harness.lock().await;
                    guard
                        .sessions
                        .get(&session)
                        .map(|state| state.journal.clone())
                        .ok_or_else(|| AhrbError::Protocol("test session disappeared".to_owned()))?
                };
                let events = journal.all()?;
                if events.iter().any(|event| is_terminal(&event.event)) {
                    return Ok::<_, AhrbError>(events);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| AhrbError::Timeout("Unix mock turn test".to_owned()))??;
        assert!(
            events
                .iter()
                .any(|event| event.event == EventVocab::TerminalSuccess)
        );
        assert_eq!(engine.request_records().await.len(), 1);
        server.shutdown().await?;
        fs::remove_dir_all(&directory)?;
        Ok(())
    }
}
