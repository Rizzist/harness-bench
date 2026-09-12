//! Built-in headless reference harness used for AHRB self-tests.
//!
//! The mock intentionally has no database and no interactive fallback.  Every committed
//! event is appended to a per-session JSONL journal and `fsync`ed before an RPC response
//! can observe it.  Replay reads that journal again, making it the recoverable source of
//! truth rather than an in-memory event buffer.

mod storage_lifecycle;

use crate::driver::{http_post, http_post_with_connector};
#[cfg(unix)]
use crate::driver::{preconnected_unix_http_post, unix_http_post};
use crate::events::{EventVocab, NormalizedEvent};
use crate::fake_model::{
    FakeModelEngine, OpenAiChatFrontend, ProtocolFrontend, ProviderMailboxRequest,
    ProviderMailboxResponse,
};
use crate::workflow::{Fault, Workflow};
use crate::{AhrbError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::net::{IpAddr, SocketAddr};
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
// Keep the reference long-session service time above ordinary host-scheduler
// noise. Row 49 permits one percent of the first-decile median per 100 turns;
// 350 ms leaves a small deterministic envelope without making the 100-turn
// quick fixture approach certification-scale duration.
const LONG_HORIZON_MIN_TURN_MS: u64 = 350;
const TURN_LATENCY_MIN_TURN_MS: u64 = 100;
const MAX_MODEL_REQUESTS_PER_TURN: u64 = 64;
const OWNED_EGRESS_BOUNDARY: &str = "reference-mock-loopback-connector-v1";
static RECONCILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static NEW_FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static REPLACE_FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static PROVIDER_MAILBOX_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static PERMISSION_LEDGER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn reference_terminal_floor_ms(prompt: &str) -> Option<u64> {
    if prompt.starts_with("AHRB long horizon turn ") {
        Some(LONG_HORIZON_MIN_TURN_MS)
    } else if prompt.starts_with("AHRB turn latency ") {
        Some(TURN_LATENCY_MIN_TURN_MS)
    } else {
        None
    }
}

/// One durable decision emitted by the reference mock's owned connector.
///
/// Row 62 supplies the nonce and ledger path out of band, then binds this PID
/// to the independently sampled harness tree and this boundary to the resolved
/// reference-mock executable digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OwnedEgressLedgerRecord {
    pub schema: u32,
    pub sequence: u64,
    pub nonce: String,
    pub pid: u32,
    pub boundary: String,
    pub destination: String,
    pub category: String,
    pub allowed: bool,
    pub outcome: String,
}

#[derive(Clone, Debug)]
struct OwnedEgressGuard {
    ledger_path: PathBuf,
    nonce: String,
    forbidden_address: SocketAddr,
    write_lock: Arc<Mutex<()>>,
    sequence: Arc<AtomicU64>,
}

impl OwnedEgressGuard {
    fn from_environment(state_dir: &Path) -> Result<Option<Self>> {
        let ledger_path = optional_unicode_environment("AHRB_MOCK_OWNED_EGRESS_LEDGER")?;
        let nonce = optional_unicode_environment("AHRB_MOCK_OWNED_EGRESS_NONCE")?;
        let forbidden = optional_unicode_environment("AHRB_MOCK_OWNED_EGRESS_FORBIDDEN")?;
        if ledger_path.is_none() && nonce.is_none() && forbidden.is_none() {
            return Ok(None);
        }
        let (Some(ledger_path), Some(nonce), Some(forbidden)) = (ledger_path, nonce, forbidden)
        else {
            return Err(AhrbError::Validation(
                "owned egress guard environment is incomplete".to_owned(),
            ));
        };
        let ledger_path = PathBuf::from(ledger_path);
        let profile_root = state_dir.parent().ok_or_else(|| {
            AhrbError::Validation("mock state directory has no profile parent".to_owned())
        })?;
        if !ledger_path.is_absolute()
            || !ledger_path.starts_with(profile_root)
            || ledger_path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(AhrbError::Validation(
                "owned egress ledger must be an absolute path under the mock profile".to_owned(),
            ));
        }
        if nonce.len() != 64
            || !nonce
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AhrbError::Validation(
                "owned egress nonce must be 64 lowercase hexadecimal characters".to_owned(),
            ));
        }
        let forbidden_address = forbidden.parse::<SocketAddr>().map_err(|_| {
            AhrbError::Validation(
                "owned egress forbidden destination must be an IP socket address".to_owned(),
            )
        })?;
        if forbidden_address.ip().is_loopback() {
            return Err(AhrbError::Validation(
                "owned egress forbidden destination must not be loopback".to_owned(),
            ));
        }
        Ok(Some(Self {
            ledger_path,
            nonce,
            forbidden_address,
            write_lock: Arc::new(Mutex::new(())),
            sequence: Arc::new(AtomicU64::new(1)),
        }))
    }

    async fn record(
        &self,
        destination: String,
        category: &str,
        allowed: bool,
        outcome: &str,
    ) -> Result<()> {
        let _write_guard = self.write_lock.lock().await;
        if let Some(parent) = self.ledger_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let record = OwnedEgressLedgerRecord {
            schema: 1,
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            nonce: self.nonce.clone(),
            pid: std::process::id(),
            boundary: OWNED_EGRESS_BOUNDARY.to_owned(),
            destination,
            category: category.to_owned(),
            allowed,
            outcome: outcome.to_owned(),
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.ledger_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    async fn connect_socket(
        &self,
        address: SocketAddr,
        category: &str,
    ) -> Result<tokio::net::TcpStream> {
        let destination = address.to_string();
        if !address.ip().is_loopback() {
            self.record(destination, category, false, "blocked-permission-denied")
                .await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reference mock owned egress boundary denied non-loopback connect",
            )
            .into());
        }
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => {
                self.record(destination, category, true, "connected-loopback")
                    .await?;
                Ok(stream)
            }
            Err(error) => {
                self.record(destination, category, true, "loopback-connect-error")
                    .await?;
                Err(error.into())
            }
        }
    }

    async fn connect_provider(&self, host: String, port: u16) -> Result<tokio::net::TcpStream> {
        let parsed = host.parse::<IpAddr>().map_err(|_| {
            AhrbError::Validation(
                "owned egress guard requires the injected TCP provider to use a literal loopback address"
                    .to_owned(),
            )
        })?;
        self.connect_socket(SocketAddr::new(parsed, port), "provider")
            .await
    }

    async fn verify_forbidden_probe(&self) -> Result<()> {
        match self
            .connect_socket(self.forbidden_address, "control-probe")
            .await
        {
            Err(AhrbError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                Ok(())
            }
            Err(error) => Err(AhrbError::Protocol(format!(
                "owned egress control probe returned an unrelated error: {error}"
            ))),
            Ok(stream) => {
                drop(stream);
                Err(AhrbError::Protocol(
                    "owned egress control probe escaped the reference mock boundary".to_owned(),
                ))
            }
        }
    }

    async fn record_local_provider(&self, destination: String) -> Result<()> {
        self.record(destination, "provider", true, "connected-local-ipc")
            .await
    }
}

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

    /// Append the exact one-MiB row-53 fixture record and make it durable.
    fn append_row53_large_record(&self) -> Result<()> {
        const TOTAL: usize = 1_048_576;
        const PREFIX: &[u8] = br#"{"type":"ahrb-large","payload":""#;
        const SUFFIX: &[u8] = b"\"}\n";
        let payload_len = TOTAL
            .checked_sub(PREFIX.len().saturating_add(SUFFIX.len()))
            .ok_or_else(|| AhrbError::Protocol("row-53 record framing overflow".to_owned()))?;
        let mut bytes = Vec::with_capacity(TOTAL);
        bytes.extend_from_slice(PREFIX);
        bytes.resize(PREFIX.len().saturating_add(payload_len), b'A');
        bytes.extend_from_slice(SUFFIX);
        if bytes.len() != TOTAL {
            return Err(AhrbError::Protocol(
                "row-53 large record did not total exactly 1,048,576 bytes".to_owned(),
            ));
        }
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
            let event = match serde_json::from_slice::<NormalizedEvent>(&line) {
                Ok(event) => event,
                Err(event_error) => {
                    // Row 53's exact full-size fixture is an intentionally
                    // synthetic non-event record. Parse a generic value only
                    // on this exceptional path; ordinary growing-session
                    // resumes must not deserialize every durable event twice.
                    let raw: Value = serde_json::from_slice(&line).map_err(|error| {
                        AhrbError::Protocol(format!(
                            "corrupt durable journal {}: {error}",
                            self.path.display()
                        ))
                    })?;
                    if raw.get("type").and_then(Value::as_str) == Some("ahrb-large")
                        && raw.get("payload").and_then(Value::as_str).is_some()
                    {
                        continue;
                    }
                    return Err(AhrbError::Protocol(format!(
                        "corrupt durable journal {}: {event_error}",
                        self.path.display()
                    )));
                }
            };
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
    workspace_override: Option<PathBuf>,
    base_url: Option<String>,
    unix_socket: Option<PathBuf>,
    provider_mailbox: Option<PathBuf>,
    #[cfg(unix)]
    provider_stream: Option<Arc<Mutex<tokio::net::UnixStream>>>,
    embedded_model: Option<Arc<FakeModelEngine>>,
    api_key: Option<String>,
    model: String,
    tariff_input_microusd_per_token: u64,
    tariff_output_microusd_per_token: u64,
    idle_timeout: Duration,
    turn_timeout: Duration,
    retry_max_attempts: u32,
    retry_base_delay_ms: u64,
    retry_max_delay_ms: u64,
    max_output_bytes: usize,
    context_window_tokens: Option<u64>,
    model_request_ceiling: u64,
    model_request_cap_exit_code: Option<i32>,
    model_request_cap_stall_ms: Option<u64>,
    compact_after_turn: Option<u64>,
    retain_recent_tool_results: Option<usize>,
    suppress_fixture_effects: bool,
    suppress_narrative: bool,
    silent_compaction: bool,
    disable_compaction: bool,
    session_memory_bytes: u64,
    acceptance_hook: Vec<String>,
    completion_hook: Vec<String>,
    declare_native_shell: bool,
    owned_egress_guard: Option<OwnedEgressGuard>,
}

impl MockConfig {
    fn workspace_path(&self, session: &str) -> PathBuf {
        self.workspace_override
            .clone()
            .unwrap_or_else(|| self.state_dir.join("workspaces").join(session))
    }
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
    /// Cursor-ordered live index rebuilt from the durable journal at startup.
    /// Attach must be proportional to the requested suffix, not total session age.
    events: Vec<NormalizedEvent>,
    /// Durable tool results indexed for constant-session-age dedup checks.
    tool_results: BTreeMap<String, Value>,
    /// Durable hook completions indexed by semantic hook identity.
    completed_hooks: BTreeSet<(String, String)>,
    /// Whether a terminal event follows the most recent accepted turn.
    ///
    /// This is rebuilt from the journal on startup and updated only after the
    /// corresponding durable append. Hot-path cancellation/terminal checks
    /// therefore do not rescan an ever-growing session history.
    terminal_since_last_accept: bool,
    /// Reference-fixture pacing deadline for the row-29/49 growing session.
    terminal_not_before: Option<tokio::time::Instant>,
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
        Self::open_with_readiness(config, true, None)
    }

    fn open_per_invocation(config: MockConfig) -> Result<Self> {
        Self::open_with_readiness(config, false, None)
    }

    fn open_per_invocation_session(config: MockConfig, session_id: &str) -> Result<Self> {
        Self::open_with_readiness(config, false, Some(session_id))
    }

    fn open_with_readiness(
        config: MockConfig,
        publish_daemon_pid: bool,
        session_filter: Option<&str>,
    ) -> Result<Self> {
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
            if session_filter.is_some_and(|session_id| meta.id != session_id) {
                continue;
            }
            let journal = DurableJournal::open(entry.path().join("journal.jsonl"))?;
            let events = journal.all()?;
            reconcile_durable_tool_effects(&config, &meta.id, &events)?;
            let keys = events
                .iter()
                .filter(|event| event.event == EventVocab::TurnAccepted)
                .filter_map(|event| event.payload.get("key").and_then(Value::as_str))
                .map(str::to_owned)
                .collect();
            let pending = recover_pending(&events);
            let tool_results = events
                .iter()
                .filter(|event| event.event == EventVocab::ToolResult)
                .filter_map(|event| {
                    Some((
                        event.payload.get("call_id")?.as_str()?.to_owned(),
                        event.payload.get("result")?.clone(),
                    ))
                })
                .collect();
            let completed_hooks = events
                .iter()
                .filter(|event| event.event == EventVocab::HookCompleted)
                .filter_map(|event| {
                    Some((
                        event.payload.get("kind")?.as_str()?.to_owned(),
                        event.payload.get("turn_key")?.as_str()?.to_owned(),
                    ))
                })
                .collect();
            let terminal_since_last_accept = events
                .iter()
                .rev()
                .find(|event| event.event == EventVocab::TurnAccepted || is_terminal(&event.event))
                .is_some_and(|event| is_terminal(&event.event));
            let next_cursor = events
                .last()
                .map(|event| event.cursor.saturating_add(1))
                .unwrap_or(1);
            sessions.insert(
                meta.id.clone(),
                SessionState {
                    meta,
                    journal,
                    events,
                    tool_results,
                    completed_hooks,
                    terminal_since_last_accept,
                    terminal_not_before: None,
                    next_cursor,
                    keys,
                    pending,
                    queued: VecDeque::new(),
                    injected: Vec::new(),
                    active: false,
                    cancelled: false,
                    closed: entry.path().join("closed.json").is_file(),
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
        fs::create_dir_all(self.config.workspace_path(id))?;
        sync_directory(&directory)?;
        sync_directory(&self.config.state_dir.join("sessions"))?;
        self.sessions.insert(
            id.to_owned(),
            SessionState {
                meta,
                journal,
                events: Vec::new(),
                tool_results: BTreeMap::new(),
                completed_hooks: BTreeSet::new(),
                terminal_since_last_accept: false,
                terminal_not_before: None,
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

    fn append(&mut self, session_id: &str, event: EventVocab, mut payload: Value) -> Result<u64> {
        let session = self.session_mut(session_id)?;
        let cursor = session.next_cursor;
        let next_cursor = cursor.checked_add(1).ok_or_else(|| {
            AhrbError::Validation(format!("session {session_id:?} exhausted its cursor space"))
        })?;
        if let Some(object) = payload.as_object_mut() {
            object
                .entry("schema_version".to_owned())
                .or_insert_with(|| json!(1));
            object
                .entry("timestamp_ns".to_owned())
                .or_insert_with(|| json!(crate::fake_model::monotonic_timestamp_ns()));
        }
        let normalized = NormalizedEvent {
            id: format!("{session_id}:{cursor}"),
            cursor,
            session_id: session_id.to_owned(),
            actor: session.meta.marker.clone(),
            event,
            payload,
        };
        session.journal.append(&normalized)?;
        if normalized.event == EventVocab::TurnAccepted {
            session.terminal_since_last_accept = false;
        } else if is_terminal(&normalized.event) {
            session.terminal_since_last_accept = true;
            session.terminal_not_before = None;
        }
        if normalized.event == EventVocab::ToolResult
            && let (Some(call_id), Some(result)) = (
                normalized.payload.get("call_id").and_then(Value::as_str),
                normalized.payload.get("result"),
            )
        {
            session
                .tool_results
                .insert(call_id.to_owned(), result.clone());
        }
        if normalized.event == EventVocab::HookCompleted
            && let (Some(kind), Some(turn_key)) = (
                normalized.payload.get("kind").and_then(Value::as_str),
                normalized.payload.get("turn_key").and_then(Value::as_str),
            )
        {
            session
                .completed_hooks
                .insert((kind.to_owned(), turn_key.to_owned()));
        }
        session.events.push(normalized);
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

    fn close_delete_session(&mut self, id: &str) -> Result<(u64, Vec<Arc<Notify>>)> {
        let mut session = self
            .sessions
            .remove(id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?;
        let waiter_prefix = format!("checkpoints/{id}/");
        let waiter_keys = self
            .checkpoint_waiters
            .keys()
            .filter(|release_token| release_token.starts_with(&waiter_prefix))
            .cloned()
            .collect::<Vec<_>>();
        let waiters = waiter_keys
            .iter()
            .filter_map(|release_token| self.checkpoint_waiters.remove(release_token))
            .collect::<Vec<_>>();
        let released_bytes = match session.resource_reservation.take() {
            Some(reservation) => reservation.release()?,
            None => 0,
        };

        let session_directory = self.config.state_dir.join("sessions").join(id);
        remove_directory_if_present(&session_directory)?;
        sync_directory(&self.config.state_dir.join("sessions"))?;
        // A workspace override is actor/user-owned input, not a session store.
        // Close-delete must reclaim only the harness-owned default workspace.
        if self.config.workspace_override.is_none() {
            let workspace = self.config.workspace_path(id);
            remove_directory_if_present(&workspace)?;
            if let Some(parent) = workspace.parent()
                && parent.is_dir()
            {
                sync_directory(parent)?;
            }
        }
        let checkpoints = self.config.state_dir.join("checkpoints").join(id);
        remove_directory_if_present(&checkpoints)?;
        if let Some(parent) = checkpoints.parent()
            && parent.is_dir()
        {
            sync_directory(parent)?;
        }
        Ok((released_bytes, waiters))
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
        "status" => status_command(&args[1..]),
        // Storage regression controller: stays warm while separate exec clients
        // own the session work and its observable storage-payload.bin writes.
        "storage-controller" => {
            let config = parse_config(&args[1..])?;
            let _harness = MockHarness::open(config)?;
            std::future::pending::<()>().await;
            Ok(0)
        }
        "exec-turn" => exec_turn(&args[1..]).await,
        "budget-trial" => budget_trial_command(&args[1..]).await,
        "session-close" => storage_lifecycle::close(&args[1..]),
        "storage-auto-sweep" => storage_lifecycle::sweep(&args[1..]).await,
        "session-create" => session_create_command(&args[1..]),
        "session-list" => session_list_command(&args[1..]),
        "session-resume" => session_resume_command(&args[1..]),
        "session-fork" => session_fork_command(&args[1..]),
        "session-delete" => session_delete_command(&args[1..]),
        "permission-trial" => permission_trial_command(&args[1..]),
        "release-checkpoint" => release_checkpoint_command(&args[1..]).await,
        "cancel-session" => cancel_session_command(&args[1..]).await,
        "close-delete-session" => close_delete_session_command(&args[1..]).await,
        "inspect-journal" => inspect_journal(&args[1..]),
        "hook" => hook_command(&args[1..]),
        "--help" | "help" => {
            println!(
                "ahrb-mock-harness serve|rpc|status --state-dir PATH [--idle-timeout-ms N] \
                 [--session-memory-mib N] [--retry-max-attempts N] \
                 [--retry-base-delay-ms N] [--retry-max-delay-ms N] \
                 [--model-request-ceiling N] \
                 [--model-request-cap-exit-code N|--model-request-cap-stall-ms N] \
                 [--compact-after-turn N --retain-recent-tool-results N] \
                 [--suppress-fixture-effects]\n\
                 ahrb-mock-harness exec-turn --state-dir PATH --marker MARKER \
                 --session-id ID --prompt PROMPT --key KEY \
                 [--base-url URL]\n\
                 ahrb-mock-harness release-checkpoint --state-dir PATH \
                 --session-id ID --release-token TOKEN\n\
                 ahrb-mock-harness cancel-session --state-dir PATH --session-id ID\n\
                 ahrb-mock-harness close-delete-session --state-dir PATH --session-id ID\n\
                 ahrb-mock-harness budget-trial --state-dir PATH \
                 (--max-tokens N|--max-cost USD|--max-time-ms N)\n\
                 ahrb-mock-harness session-create --state-dir PATH --marker MARKER\n\
                 ahrb-mock-harness session-close --state-dir PATH --session-id ID\n\
                 ahrb-mock-harness session-list --state-dir PATH\n\
                 ahrb-mock-harness session-resume|session-fork|session-delete \
                 --state-dir PATH --session-id ID\n\
                 ahrb-mock-harness permission-trial --state-dir PATH \
                 --case allow --workspace PATH\n\
                 ahrb-mock-harness permission-trial --state-dir PATH \
                 --case deny-filesystem --outside-path PATH\n\
                 ahrb-mock-harness permission-trial --state-dir PATH \
                 --case deny-network --blocked-host HOST --blocked-port PORT\n\
                 model endpoint comes from AHRB_MOCK_BASE_URL, AHRB_MOCK_UNIX_SOCKET, \
                 or AHRB_MOCK_PROVIDER_MAILBOX; \
                 key/model come from AHRB_MOCK_API_KEY and AHRB_MOCK_MODEL"
            );
            Ok(0)
        }
        other => Err(AhrbError::Usage(format!(
            "unknown mock harness command {other:?}"
        ))),
    }
}

fn status_command(args: &[String]) -> Result<i32> {
    let config = parse_config(args)?;
    let pid = std::fs::read_to_string(config.state_dir.join("daemon.pid"))?
        .trim()
        .parse::<u32>()
        .map_err(|error| AhrbError::Protocol(format!("invalid mock daemon PID: {error}")))?;
    println!("{}", json!({"daemon":{"pid":pid,"ready":true}}));
    Ok(0)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BudgetTrial {
    Tokens(u64),
    CostMicrousd(u64),
    TimeMs(u64),
}

#[derive(Debug)]
struct BudgetProviderObservation {
    request_id: String,
    input_tokens: u64,
    output_tokens: u64,
    elapsed_ms: u64,
}

async fn budget_trial_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(
        args,
        &[
            "--state-dir",
            "--max-tokens",
            "--max-cost",
            "--max-time-ms",
            "--idle-timeout-ms",
        ],
    )?;
    let state_dir = absolute_cli_path(required_cli_option(&options, "--state-dir")?, "state")?;
    fs::create_dir_all(&state_dir)?;
    let mut trials = Vec::new();
    if let Some(value) = options.get("--max-tokens") {
        trials.push(BudgetTrial::Tokens(parse_positive_u64(
            value,
            "token budget",
        )?));
    }
    if let Some(value) = options.get("--max-cost") {
        trials.push(BudgetTrial::CostMicrousd(parse_usd_microusd(value)?));
    }
    if let Some(value) = options.get("--max-time-ms") {
        trials.push(BudgetTrial::TimeMs(parse_positive_u64(
            value,
            "time budget",
        )?));
    }
    if trials.len() != 1 {
        return Err(AhrbError::Usage(
            "budget-trial requires exactly one budget control".to_owned(),
        ));
    }
    let mut config_args = vec![
        "--state-dir".to_owned(),
        state_dir.to_string_lossy().into_owned(),
    ];
    if let Some(idle_timeout_ms) = options.get("--idle-timeout-ms") {
        config_args.push("--idle-timeout-ms".to_owned());
        config_args.push(idle_timeout_ms.clone());
    }
    let config = parse_config(&config_args)?;
    if config.base_url.is_none()
        && config.unix_socket.is_none()
        && config.provider_mailbox.is_none()
        && config.embedded_model.is_none()
    {
        return Err(AhrbError::Validation(
            "budget-trial requires an injected fake-provider endpoint".to_owned(),
        ));
    }
    let input_price = config.tariff_input_microusd_per_token;
    let output_price = config.tariff_output_microusd_per_token;
    if input_price != 2 || output_price != 3 {
        return Err(AhrbError::Validation(
            "budget certification requires the 2/3 micro-USD tariff".to_owned(),
        ));
    }
    let case = match trials[0] {
        BudgetTrial::Tokens(_) => "tokens",
        BudgetTrial::CostMicrousd(_) => "cost",
        BudgetTrial::TimeMs(_) => "time",
    };
    let evidence_path = state_dir.join("wave4-budget-evidence.jsonl");
    let repetition = next_budget_repetition(&evidence_path, case)?;
    let terminal = match trials[0] {
        BudgetTrial::Tokens(limit) => token_budget_trial(&config, limit, repetition).await?,
        BudgetTrial::CostMicrousd(limit) => cost_budget_trial(&config, limit, repetition).await?,
        BudgetTrial::TimeMs(limit) => {
            let observation =
                budget_provider_request(&config, "time", repetition, 1, limit).await?;
            if observation.elapsed_ms < limit {
                return Err(AhrbError::Protocol(format!(
                    "time budget provider response completed at {} ms before limit {limit}",
                    observation.elapsed_ms
                )));
            }
            let total_tokens = observation
                .input_tokens
                .checked_add(observation.output_tokens)
                .ok_or_else(|| AhrbError::Protocol("time trial usage overflow".to_owned()))?;
            let cost_microusd =
                usage_cost(&config, observation.input_tokens, observation.output_tokens)?;
            json!({
                "schema_version": 1,
                "event": "terminal-budget-exceeded",
                "terminal_type": "budget-exceeded",
                "status": "budget-exceeded",
                "case": "time",
                "repetition": repetition,
                "limit_ms": limit,
                "observed_ms": observation.elapsed_ms,
                "usage": {
                    "input_tokens":observation.input_tokens,
                    "output_tokens":observation.output_tokens,
                    "total_tokens":total_tokens,
                    "cost_microusd":cost_microusd,
                    "turns":1
                },
                "provider_requests": [{"request_id":observation.request_id,"boundary":"crossing"}],
                "overrun_count": 0,
                "outer_kill": false
            })
        }
    };
    append_jsonl_synced(&evidence_path, &terminal)?;
    println!("{}", serde_json::to_string(&terminal)?);
    std::io::stdout().flush()?;
    Ok(0)
}

async fn token_budget_trial(config: &MockConfig, limit: u64, repetition: u64) -> Result<Value> {
    let fixture = limit
        .checked_sub(16)
        .ok_or_else(|| AhrbError::Validation("token budget must be at least 16".to_owned()))?;
    let before = budget_provider_request(config, "tokens", repetition, 1, limit).await?;
    let before_total = before
        .input_tokens
        .checked_add(before.output_tokens)
        .ok_or_else(|| AhrbError::Protocol("token fixture usage overflow".to_owned()))?;
    if before_total != fixture {
        return Err(AhrbError::Protocol(format!(
            "token fixture provider reported {before_total}, expected {fixture}"
        )));
    }
    let crossing = budget_provider_request(config, "tokens", repetition, 2, limit).await?;
    if crossing.input_tokens != 8 || crossing.output_tokens != 16 {
        return Err(AhrbError::Protocol(format!(
            "token crossing response reported {}/{}, expected 8/16",
            crossing.input_tokens, crossing.output_tokens
        )));
    }
    let input_tokens = before
        .input_tokens
        .checked_add(crossing.input_tokens)
        .ok_or_else(|| AhrbError::Protocol("token trial input usage overflow".to_owned()))?;
    let output_tokens = before
        .output_tokens
        .checked_add(crossing.output_tokens)
        .ok_or_else(|| AhrbError::Protocol("token trial output usage overflow".to_owned()))?;
    let observed = input_tokens
        .checked_add(output_tokens)
        .ok_or_else(|| AhrbError::Protocol("token trial total usage overflow".to_owned()))?;
    let expected = limit
        .checked_add(8)
        .ok_or_else(|| AhrbError::Validation("token budget observation overflow".to_owned()))?;
    if observed != expected {
        return Err(AhrbError::Protocol(format!(
            "token trial observed {observed}, expected {expected}"
        )));
    }
    let cost_microusd = usage_cost(config, input_tokens, output_tokens)?;
    Ok(json!({
        "schema_version":1,"event":"terminal-budget-exceeded",
        "terminal_type":"budget-exceeded","status":"budget-exceeded","case":"tokens",
        "repetition":repetition,
        "limit":limit,"observed":observed,
        "boundary":{"fixture_tokens":fixture,"crossing_response":{"input_tokens":8,"output_tokens":16}},
        "usage":{"input_tokens":input_tokens,"output_tokens":output_tokens,"total_tokens":observed,"cost_microusd":cost_microusd,"turns":1},
        "provider_requests":[
            {"request_id":before.request_id,"boundary":"fixture"},
            {"request_id":crossing.request_id,"boundary":"crossing"}
        ],
        "overrun_count":0,"outer_kill":false
    }))
}

async fn cost_budget_trial(config: &MockConfig, limit: u64, repetition: u64) -> Result<Value> {
    let fixture_cost = limit.checked_sub(25).ok_or_else(|| {
        AhrbError::Validation("cost budget must be at least 25 micro-USD".to_owned())
    })?;
    let before = budget_provider_request(config, "cost", repetition, 1, limit).await?;
    if usage_cost(config, before.input_tokens, before.output_tokens)? != fixture_cost {
        return Err(AhrbError::Protocol(
            "cost fixture provider usage did not equal limit-25".to_owned(),
        ));
    }
    let crossing = budget_provider_request(config, "cost", repetition, 2, limit).await?;
    if usage_cost(config, crossing.input_tokens, crossing.output_tokens)? != 50 {
        return Err(AhrbError::Protocol(
            "cost crossing provider response did not cost exactly 50 micro-USD".to_owned(),
        ));
    }
    let input_tokens = before
        .input_tokens
        .checked_add(crossing.input_tokens)
        .ok_or_else(|| AhrbError::Protocol("cost trial input usage overflow".to_owned()))?;
    let output_tokens = before
        .output_tokens
        .checked_add(crossing.output_tokens)
        .ok_or_else(|| AhrbError::Protocol("cost trial output usage overflow".to_owned()))?;
    let total_tokens = input_tokens
        .checked_add(output_tokens)
        .ok_or_else(|| AhrbError::Protocol("cost trial total usage overflow".to_owned()))?;
    let observed = usage_cost(config, input_tokens, output_tokens)?;
    let expected = limit
        .checked_add(25)
        .ok_or_else(|| AhrbError::Validation("cost budget observation overflow".to_owned()))?;
    if observed != expected {
        return Err(AhrbError::Protocol(format!(
            "cost trial observed {observed}, expected {expected}"
        )));
    }
    Ok(json!({
        "schema_version":1,"event":"terminal-budget-exceeded",
        "terminal_type":"budget-exceeded","status":"budget-exceeded","case":"cost",
        "repetition":repetition,
        "limit_microusd":limit,"observed_microusd":observed,
        "tariff":{"input_microusd_per_token":2,"output_microusd_per_token":3},
        "boundary":{"fixture_cost_microusd":fixture_cost,"crossing_response":{"input_tokens":crossing.input_tokens,"output_tokens":crossing.output_tokens,"cost_microusd":50}},
        "usage":{"input_tokens":input_tokens,"output_tokens":output_tokens,"total_tokens":total_tokens,"cost_microusd":observed,"turns":1},
        "provider_requests":[
            {"request_id":before.request_id,"boundary":"fixture"},
            {"request_id":crossing.request_id,"boundary":"crossing"}
        ],
        "overrun_count":0,"outer_kill":false
    }))
}

async fn budget_provider_request(
    config: &MockConfig,
    case: &str,
    repetition: u64,
    sequence: u64,
    limit: u64,
) -> Result<BudgetProviderObservation> {
    let request_id = stable_budget_request_id(case, repetition, sequence, limit);
    let checkpoint = if case == "time" || sequence == 2 {
        "crossing"
    } else {
        "fixture"
    };
    let actor = format!("{case}-r{repetition}");
    let route_marker =
        format!("[[AHRB:scenario=ahrb-matrix-v1;actor={actor};checkpoint={checkpoint}]]");
    let prompt = format!(
        "AHRB-WAVE4-BUDGET case={case} repetition={repetition} sequence={sequence} limit={limit} request_id={request_id} {route_marker}"
    );
    let request = json!({
        "model": config.model,
        "messages": [{"role":"user","content":prompt}],
        "tools": [],
        "stream": false,
        "metadata": {
            "ahrb": {
                "scenario":"ahrb-matrix-v1",
                "actor":actor,
                "checkpoint":checkpoint,
                "request_id":request_id.clone()
            }
        }
    });
    let mut headers = BTreeMap::new();
    if let Some(key) = &config.api_key {
        headers.insert("Authorization".to_owned(), format!("Bearer {key}"));
    }
    let body = serde_json::to_vec(&request)?;
    let started = std::time::Instant::now();
    let response = model_http_post(config, &headers, &body).await?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    if !(200..300).contains(&response.status) {
        return Err(AhrbError::Protocol(format!(
            "budget fake provider returned HTTP {} for {request_id}",
            response.status
        )));
    }
    let value: Value = serde_json::from_slice(&response.body)?;
    let (input_tokens, output_tokens) = chat_response_usage(&value)?;
    Ok(BudgetProviderObservation {
        request_id,
        input_tokens,
        output_tokens,
        elapsed_ms,
    })
}

fn stable_budget_request_id(case: &str, repetition: u64, sequence: u64, limit: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ahrb-wave4-budget-v1\0");
    hasher.update(case.as_bytes());
    hasher.update([0]);
    hasher.update(repetition.to_le_bytes());
    hasher.update(sequence.to_le_bytes());
    hasher.update(limit.to_le_bytes());
    let digest = hasher.finalize();
    let mut encoded = String::from("budget-");
    for byte in &digest[..12] {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn usage_cost(config: &MockConfig, input_tokens: u64, output_tokens: u64) -> Result<u64> {
    let input = input_tokens
        .checked_mul(config.tariff_input_microusd_per_token)
        .ok_or_else(|| AhrbError::Protocol("input usage cost overflow".to_owned()))?;
    let output = output_tokens
        .checked_mul(config.tariff_output_microusd_per_token)
        .ok_or_else(|| AhrbError::Protocol("output usage cost overflow".to_owned()))?;
    input
        .checked_add(output)
        .ok_or_else(|| AhrbError::Protocol("total usage cost overflow".to_owned()))
}

fn append_jsonl_synced(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_parent(path)?;
    Ok(())
}

fn next_budget_repetition(path: &Path, case: &str) -> Result<u64> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(1),
        Err(error) => return Err(error.into()),
    };
    let mut completed = 0_u64;
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(line).map_err(|error| {
            AhrbError::Protocol(format!(
                "corrupt budget evidence ledger {}: {error}",
                path.display()
            ))
        })?;
        if value.get("case").and_then(Value::as_str) == Some(case) {
            completed = completed.checked_add(1).ok_or_else(|| {
                AhrbError::Validation("budget repetition count overflow".to_owned())
            })?;
        }
    }
    completed
        .checked_add(1)
        .ok_or_else(|| AhrbError::Validation("budget repetition count overflow".to_owned()))
}

#[cfg(test)]
fn token_budget_terminal(limit: u64) -> Result<Value> {
    let fixture = limit
        .checked_sub(16)
        .ok_or_else(|| AhrbError::Validation("token budget must be at least 16".to_owned()))?;
    let observed = limit
        .checked_add(8)
        .ok_or_else(|| AhrbError::Validation("token budget observation overflow".to_owned()))?;
    let input_tokens = observed
        .checked_sub(16)
        .ok_or_else(|| AhrbError::Protocol("token budget fixture underflow".to_owned()))?;
    Ok(json!({
        "schema_version": 1,
        "event": "terminal-budget-exceeded",
        "terminal_type": "budget-exceeded",
        "status": "budget-exceeded",
        "case": "tokens",
        "limit": limit,
        "observed": observed,
        "boundary": {"fixture_tokens":fixture,"crossing_response":{"input_tokens":8,"output_tokens":16}},
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": 16,
            "total_tokens": observed,
            "cost_microusd": input_tokens.saturating_mul(2).saturating_add(48),
            "turns": 1
        },
        "overrun_count": 0,
        "outer_kill": false
    }))
}

#[cfg(test)]
fn cost_budget_terminal(limit: u64) -> Result<Value> {
    let fixture_cost = limit.checked_sub(25).ok_or_else(|| {
        AhrbError::Validation("cost budget must be at least 25 micro-USD".to_owned())
    })?;
    if fixture_cost < 3 || (fixture_cost - 3) % 2 != 0 {
        return Err(AhrbError::Validation(
            "cost budget cannot represent the exact tariff fixture".to_owned(),
        ));
    }
    let fixture_input = (fixture_cost - 3) / 2;
    let input_tokens = fixture_input
        .checked_add(10)
        .ok_or_else(|| AhrbError::Protocol("cost budget input usage overflow".to_owned()))?;
    let output_tokens = 11_u64;
    let total_tokens = input_tokens
        .checked_add(output_tokens)
        .ok_or_else(|| AhrbError::Protocol("cost budget total usage overflow".to_owned()))?;
    let observed = limit
        .checked_add(25)
        .ok_or_else(|| AhrbError::Validation("cost budget observation overflow".to_owned()))?;
    Ok(json!({
        "schema_version": 1,
        "event": "terminal-budget-exceeded",
        "terminal_type": "budget-exceeded",
        "status": "budget-exceeded",
        "case": "cost",
        "limit_microusd": limit,
        "observed_microusd": observed,
        "tariff": {"input_microusd_per_token":2,"output_microusd_per_token":3},
        "boundary": {
            "fixture_cost_microusd": fixture_cost,
            "crossing_response": {"input_tokens":10,"output_tokens":10,"cost_microusd":50}
        },
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cost_microusd": observed,
            "turns": 1
        },
        "overrun_count": 0,
        "outer_kill": false
    }))
}

fn session_create_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(args, &["--state-dir", "--marker"])?;
    let marker = required_cli_option(&options, "--marker")?;
    let mut harness = open_session_cli_harness(&options)?;
    let session_id = harness.create_session(marker)?;
    println!(
        "{}",
        json!({"schema_version":1,"operation":"create","terminal_type":"success","session_id":session_id})
    );
    Ok(0)
}

fn session_list_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(args, &["--state-dir"])?;
    let harness = open_session_cli_harness(&options)?;
    let sessions = harness
        .sessions
        .values()
        .map(|session| {
            json!({
                "id": session.meta.id,
                "marker": session.meta.marker,
                "committed_cursor": session.events.last().map(|event| event.cursor).unwrap_or(0)
            })
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        json!({"schema_version":1,"operation":"list","terminal_type":"success","sessions":sessions})
    );
    Ok(0)
}

fn session_resume_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(args, &["--state-dir", "--session-id"])?;
    let session_id = required_cli_option(&options, "--session-id")?;
    validate_session_id(session_id)?;
    let harness = open_session_cli_harness(&options)?;
    let Some(session) = harness.sessions.get(session_id) else {
        println!(
            "{}",
            json!({
                "schema_version":1,"operation":"resume","terminal_type":"not-found",
                "session_id":session_id,"not_found":true
            })
        );
        return Ok(4);
    };
    let history_hashes = event_history_hashes(&session.events)?;
    println!(
        "{}",
        json!({
            "schema_version":1,"operation":"resume","terminal_type":"success",
            "session_id":session_id,
            "committed_cursor":session.events.last().map(|event| event.cursor).unwrap_or(0),
            "history_hashes":history_hashes,"events":session.events
        })
    );
    Ok(0)
}

fn session_fork_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(args, &["--state-dir", "--session-id"])?;
    let source_id = required_cli_option(&options, "--session-id")?;
    validate_session_id(source_id)?;
    let mut harness = open_session_cli_harness(&options)?;
    let source = harness
        .sessions
        .get(source_id)
        .ok_or_else(|| AhrbError::Protocol(format!("cannot fork unknown session {source_id:?}")))?;
    if source.events.is_empty() {
        return Err(AhrbError::Validation(
            "cannot fork a session with empty committed history".to_owned(),
        ));
    }
    let source_marker = source.meta.marker.clone();
    let source_events = source.events.clone();
    let mut ordinal = 1_u64;
    let fork_id = loop {
        let candidate = stable_session_id(&format!("fork:{source_id}:{ordinal}"));
        if !harness.sessions.contains_key(&candidate) {
            break candidate;
        }
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| AhrbError::Validation("session fork ordinal overflow".to_owned()))?;
    };
    harness.create_session_with_id(&source_marker, &fork_id)?;
    for event in &source_events {
        harness.append(&fork_id, event.event.clone(), event.payload.clone())?;
    }
    let history_hashes = event_history_hashes(&source_events)?;
    println!(
        "{}",
        json!({
            "schema_version":1,"operation":"fork","terminal_type":"success",
            "source_session_id":source_id,"fork_session_id":fork_id,
            "committed_cursor":source_events.last().map(|event| event.cursor).unwrap_or(0),
            "history_hashes":history_hashes
        })
    );
    Ok(0)
}

fn session_delete_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(args, &["--state-dir", "--session-id"])?;
    let session_id = required_cli_option(&options, "--session-id")?.to_owned();
    validate_session_id(&session_id)?;
    let mut harness = open_session_cli_harness(&options)?;
    let existed = harness.sessions.contains_key(&session_id);
    let released_bytes = if existed {
        let (released, waiters) = harness.close_delete_session(&session_id)?;
        for waiter in waiters {
            waiter.notify_one();
        }
        released
    } else {
        0
    };
    println!(
        "{}",
        json!({
            "schema_version":1,"operation":"delete","terminal_type":"success",
            "session_id":session_id,"deleted":existed,"already_absent":!existed,
            "released_bytes":released_bytes
        })
    );
    Ok(0)
}

fn permission_trial_command(args: &[String]) -> Result<i32> {
    let options = parse_cli_options(
        args,
        &[
            "--state-dir",
            "--case",
            "--workspace",
            "--outside-path",
            "--blocked-host",
            "--blocked-port",
        ],
    )?;
    let state_dir = absolute_cli_path(required_cli_option(&options, "--state-dir")?, "state")?;
    fs::create_dir_all(&state_dir)?;
    let terminal = match required_cli_option(&options, "--case")? {
        "allow" => {
            let workspace =
                absolute_cli_path(required_cli_option(&options, "--workspace")?, "workspace")?;
            fs::create_dir_all(&workspace)?;
            let effect_path = workspace.join("ahrb-permission-allowed.txt");
            write_replace_synced(&effect_path, b"AHRB permission allow-list effect\n")?;
            json!({
                "schema_version":1,"operation":"permission-trial","case":"allow",
                "terminal_type":"success","effect":"workspace-write-committed",
                "effect_path":effect_path
            })
        }
        "deny-filesystem" => {
            let outside_path = absolute_cli_path(
                required_cli_option(&options, "--outside-path")?,
                "outside path",
            )?;
            if outside_path.exists() {
                return Err(AhrbError::Validation(
                    "deny-filesystem fixture path must not preexist".to_owned(),
                ));
            }
            json!({
                "schema_version":1,"operation":"permission-trial","case":"deny-filesystem",
                "terminal_type":"permission-denied","effect":"filesystem-write-denied",
                "effect_path":outside_path,"scope_violation":false
            })
        }
        "deny-network" => {
            let blocked_host = required_cli_option(&options, "--blocked-host")?;
            let blocked_port = parse_positive_u64(
                required_cli_option(&options, "--blocked-port")?,
                "blocked port",
            )?;
            let blocked_port = u16::try_from(blocked_port)
                .map_err(|_| AhrbError::Validation("blocked port does not fit u16".to_owned()))?;
            let blocked_ip = blocked_host.parse::<IpAddr>().map_err(|_| {
                AhrbError::Validation("blocked host must be a literal IP address".to_owned())
            })?;
            let destination = SocketAddr::new(blocked_ip, blocked_port);
            let ledger_sequence = deny_network_at_permission_boundary(&state_dir, destination)?;
            json!({
                "schema_version":1,"operation":"permission-trial","case":"deny-network",
                "terminal_type":"permission-denied","effect":"network-connect-denied",
                "destination":destination.to_string(),"scope_violation":false,
                "connector_boundary":"wave4-permission-connector-v1",
                "attempted":true,"os_connect_attempted":false,"ledger_sequence":ledger_sequence
            })
        }
        other => {
            return Err(AhrbError::Usage(format!(
                "unknown permission trial case {other:?}"
            )));
        }
    };
    println!("{}", serde_json::to_string(&terminal)?);
    std::io::stdout().flush()?;
    Ok(0)
}

fn deny_network_at_permission_boundary(state_dir: &Path, destination: SocketAddr) -> Result<u64> {
    let sequence = PERMISSION_LEDGER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let decision = json!({
        "schema_version":1,
        "sequence":sequence,
        "boundary":"wave4-permission-connector-v1",
        "destination":destination.to_string(),
        "attempted":true,
        "allowed":false,
        "outcome":"blocked-permission-denied",
        "os_connect_attempted":false
    });
    append_jsonl_synced(&state_dir.join("wave4-permission-ledger.jsonl"), &decision)?;
    Ok(sequence)
}

fn parse_cli_options(args: &[String], allowed: &[&str]) -> Result<BTreeMap<String, String>> {
    if args.len() % 2 != 0 {
        return Err(AhrbError::Usage(
            "every command option requires exactly one value".to_owned(),
        ));
    }
    let mut options = BTreeMap::new();
    for pair in args.chunks_exact(2) {
        let option = pair[0].as_str();
        if !allowed.contains(&option) {
            return Err(AhrbError::Usage(format!("unknown option {option:?}")));
        }
        if options.insert(pair[0].clone(), pair[1].clone()).is_some() {
            return Err(AhrbError::Usage(format!(
                "option {option:?} was supplied more than once"
            )));
        }
    }
    Ok(options)
}

fn required_cli_option<'a>(options: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| AhrbError::Usage(format!("{name} is required")))
}

fn absolute_cli_path(value: &str, label: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(AhrbError::Validation(format!(
            "{label} path must be absolute"
        )));
    }
    Ok(path)
}

fn parse_positive_u64(value: &str, label: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| AhrbError::Usage(format!("invalid {label}")))?;
    if parsed == 0 {
        return Err(AhrbError::Validation(format!("{label} must be positive")));
    }
    Ok(parsed)
}

fn parse_usd_microusd(value: &str) -> Result<u64> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 6
    {
        return Err(AhrbError::Usage(
            "--max-cost must be decimal USD with at most 6 fractional digits".to_owned(),
        ));
    }
    let whole = whole
        .parse::<u64>()
        .map_err(|_| AhrbError::Usage("invalid --max-cost whole dollars".to_owned()))?;
    let fractional = if fraction.is_empty() {
        0
    } else {
        let exponent = u32::try_from(6_usize.saturating_sub(fraction.len()))
            .map_err(|_| AhrbError::Protocol("cost precision does not fit u32".to_owned()))?;
        fraction
            .parse::<u64>()
            .map_err(|_| AhrbError::Usage("invalid --max-cost fraction".to_owned()))?
            .checked_mul(10_u64.pow(exponent))
            .ok_or_else(|| AhrbError::Validation("cost fraction overflow".to_owned()))?
    };
    let microusd = whole
        .checked_mul(1_000_000)
        .and_then(|scaled| scaled.checked_add(fractional))
        .ok_or_else(|| AhrbError::Validation("cost budget overflow".to_owned()))?;
    if microusd == 0 {
        return Err(AhrbError::Validation(
            "cost budget must be positive".to_owned(),
        ));
    }
    Ok(microusd)
}

fn open_session_cli_harness(options: &BTreeMap<String, String>) -> Result<MockHarness> {
    let state_dir = required_cli_option(options, "--state-dir")?;
    let config_args = vec!["--state-dir".to_owned(), state_dir.to_owned()];
    MockHarness::open_per_invocation(parse_config(&config_args)?)
}

fn event_history_hashes(events: &[NormalizedEvent]) -> Result<Vec<String>> {
    events
        .iter()
        .map(|event| {
            let semantic = json!({
                "cursor":event.cursor,"event":event.event,"payload":event.payload
            });
            Ok(format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&semantic)?)
            ))
        })
        .collect()
}

async fn exec_turn(args: &[String]) -> Result<i32> {
    let mut marker = None;
    let mut requested_session_id = None;
    let mut prompt = None;
    let mut key = None;
    let mut rendered_base_url = None;
    let mut event_journal = None;
    let mut post_output_delay_ms = 0_u64;
    let mut pre_terminal_child_ms = 0_u64;
    let mut lingering_child_ms = 0_u64;
    let mut plaintext_stdout = false;
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
            "--event-journal" => event_journal = Some(PathBuf::from(value)),
            "--stdout-format" => {
                plaintext_stdout = match value.as_str() {
                    "jsonl" => false,
                    "plaintext" => true,
                    _ => return Err(AhrbError::Usage("invalid stdout format".to_owned())),
                };
            }
            "--pre-terminal-child-ms" => {
                pre_terminal_child_ms = value
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid pre-terminal child delay".into()))?;
                if pre_terminal_child_ms > 1000 {
                    return Err(AhrbError::Usage(
                        "pre-terminal child delay exceeds 1000 ms".into(),
                    ));
                }
            }
            "--lingering-child-ms" => {
                lingering_child_ms = value
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid lingering child delay".into()))?;
                if lingering_child_ms > 10000 {
                    return Err(AhrbError::Usage(
                        "lingering child delay exceeds 10000 ms".into(),
                    ));
                }
            }
            "--post-output-delay-ms" => {
                post_output_delay_ms = value
                    .parse()
                    .map_err(|_| AhrbError::Usage("invalid post-output delay".to_owned()))?;
            }
            "--state-dir"
            | "--idle-timeout-ms"
            | "--session-memory-mib"
            | "--retry-max-attempts"
            | "--retry-base-delay-ms"
            | "--retry-max-delay-ms" => {
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
    if plaintext_stdout {
        println!("AHRB mock plaintext client");
        std::io::stdout().flush()?;
    }
    let use_indexed_terminal_observer = prompt.starts_with("AHRB long horizon turn ");
    let exec_template_evidence = match rendered_base_url {
        Some(base_url) => {
            let environment_base_url = std::env::var("AHRB_MOCK_BASE_URL").unwrap_or_default();
            let environment_credential = std::env::var("AHRB_MOCK_API_KEY").unwrap_or_default();
            Some(ExecTemplateEvidence {
                base_url_matches_environment: base_url == environment_base_url,
                credential_fingerprint: format!(
                    "{:x}",
                    Sha256::digest(environment_credential.as_bytes())
                ),
                credential_matches_environment: !environment_credential.is_empty(),
                base_url,
            })
        }
        None => None,
    };
    let mut config = parse_config(&config_args)?;
    config.declare_native_shell = true;
    // An exec client owns exactly one requested session. Loading every other
    // profile session here couples independent concurrent turns and can make a
    // client observe another session between its durable append boundaries.
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation_session(
        config,
        &requested_session_id,
    )?));
    let turn = PendingTurn { prompt, key };
    let (session_id, journal, after) = {
        let mut guard = harness.lock().await;
        let id = guard.create_session_with_id(&marker, &requested_session_id)?;
        let session = guard.session_mut(&id)?;
        let journal = session.journal.clone();
        let after = session.events.last().map(|event| event.cursor);
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
        #[cfg(unix)]
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        #[cfg(unix)]
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        #[cfg(unix)]
        let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
        loop {
            let observed = if use_indexed_terminal_observer {
                let guard = harness.lock().await;
                let session = guard.sessions.get(&session_id).ok_or_else(|| {
                    AhrbError::Protocol(format!("unknown session {session_id:?}"))
                })?;
                session
                    .events
                    .iter()
                    .filter(|event| after.is_none_or(|cursor| event.cursor > cursor))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                // Ordinary exec turns can be cancelled or released by a
                // separate public control process, so their durable journal
                // remains the cross-process observation boundary.
                journal.read_after(after)?
            };
            if let Some(terminal) = observed
                .iter()
                .rev()
                .find(|event| is_terminal(&event.event))
            {
                break terminal.clone();
            }
            if started.elapsed() >= Duration::from_secs(60) {
                return Err(AhrbError::Timeout("mock exec turn".to_owned()));
            }
            #[cfg(unix)]
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
                signal = sigterm.recv() => {
                    if signal.is_some() {
                        terminalize_active_for_control(&harness, "sigterm").await?;
                    }
                }
                signal = sigint.recv() => {
                    if signal.is_some() {
                        terminalize_active_for_control(&harness, "sigint").await?;
                    }
                }
                signal = sighup.recv() => {
                    if signal.is_some() {
                        terminalize_active_for_control(&harness, "sighup").await?;
                    }
                }
            }
            #[cfg(not(unix))]
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    } else {
        harness
            .lock()
            .await
            .session_mut(&session_id)?
            .events
            .iter()
            .cloned()
            .rev()
            .find(|event| is_terminal(&event.event))
            .ok_or_else(|| {
                AhrbError::Protocol(
                    "idempotent exec retry found neither pending work nor a terminal".to_owned(),
                )
            })?
    };
    let events = if (spawn || resume_pending) && use_indexed_terminal_observer {
        let guard = harness.lock().await;
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {session_id:?}")))?;
        session
            .events
            .iter()
            .filter(|event| after.is_none_or(|cursor| event.cursor > cursor))
            .cloned()
            .collect()
    } else if spawn || resume_pending {
        journal.read_after(after)?
    } else {
        vec![terminal.clone()]
    };
    if pre_terminal_child_ms > 0 {
        // Adverse lifecycle fixture: the owned child is discoverable, but its
        // parent reaps it before publishing the invocation's terminal.
        #[cfg(unix)]
        {
            let status = tokio::process::Command::new("/bin/sleep")
                .arg(format!("{:.3}", pre_terminal_child_ms as f64 / 1000.0))
                .env_clear()
                .status()
                .await?;
            if !status.success() {
                return Err(AhrbError::Protocol("pre-terminal child failed".into()));
            }
        }
        #[cfg(not(unix))]
        return Err(AhrbError::Unsupported(
            "pre-terminal child fixture requires Unix".into(),
        ));
    }
    if lingering_child_ms > 0 {
        // Deliberately outlive the terminal/client while retaining its owned
        // process group. Give live discovery a bounded pre-terminal window.
        #[cfg(unix)]
        {
            let child = tokio::process::Command::new("/bin/sleep")
                .arg(format!("{:.3}", lingering_child_ms as f64 / 1000.0))
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(child);
        }
        #[cfg(not(unix))]
        return Err(AhrbError::Unsupported(
            "lingering child fixture requires Unix".into(),
        ));
    }
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
    if plaintext_stdout {
        println!("Turn completed: {:?}", terminal.event);
    } else {
        for event in events {
            println!("{}", serde_json::to_string(&event)?);
        }
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
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation_session(
        config,
        &session_id,
    )?));
    let release_token = release_token.ok_or_else(|| {
        AhrbError::Usage("--release-token is required for release-checkpoint".to_owned())
    })?;
    release_checkpoint(&harness, &session_id, &release_token).await?;
    Ok(0)
}

async fn cancel_session_command(args: &[String]) -> Result<i32> {
    let (config, session_id, _) = parse_session_control(args, false)?;
    let harness = Arc::new(Mutex::new(MockHarness::open_per_invocation_session(
        config,
        &session_id,
    )?));
    let workspace = {
        let mut guard = harness.lock().await;
        guard.append_terminal(
            &session_id,
            EventVocab::TerminalCancelled,
            json!({"status":"cancelled", "cleanup":"workspace-removed"}),
        )?;
        guard.config.workspace_path(&session_id)
    };
    if workspace.exists() {
        fs::remove_dir_all(&workspace)?;
    }
    fs::create_dir(&workspace)?;
    sync_parent(&workspace)?;
    Ok(0)
}

async fn close_delete_session_command(args: &[String]) -> Result<i32> {
    let (config, session_id, _) = parse_session_control(args, false)?;
    let mut harness = MockHarness::open_per_invocation_session(config, &session_id)?;
    let (released_bytes, waiters) = harness.close_delete_session(&session_id)?;
    for waiter in waiters {
        waiter.notify_one();
    }
    println!(
        "{}",
        json!({
            "closed": true,
            "deleted": true,
            "session_id": session_id,
            "released_bytes": released_bytes
        })
    );
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
    let mut retry_max_attempts = None;
    let mut retry_base_delay_ms = None;
    let mut retry_max_delay_ms = None;
    let mut model_request_ceiling = MAX_MODEL_REQUESTS_PER_TURN;
    let mut model_request_cap_exit_code = None;
    let mut model_request_cap_stall_ms = None;
    let mut compact_after_turn = None;
    let mut retain_recent_tool_results = None;
    let mut suppress_fixture_effects = false;
    let mut suppress_narrative = false;
    let mut silent_compaction = false;
    let mut disable_compaction = false;
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
            "--retry-max-attempts" => {
                index += 1;
                retry_max_attempts = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage("--retry-max-attempts needs a value".to_owned())
                        })?
                        .parse::<u64>()
                        .map_err(|_| AhrbError::Usage("invalid retry maximum".to_owned()))?,
                );
            }
            "--retry-base-delay-ms" => {
                index += 1;
                retry_base_delay_ms = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage("--retry-base-delay-ms needs a value".to_owned())
                        })?
                        .parse::<u64>()
                        .map_err(|_| AhrbError::Usage("invalid retry base delay".to_owned()))?,
                );
            }
            "--retry-max-delay-ms" => {
                index += 1;
                retry_max_delay_ms = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage("--retry-max-delay-ms needs a value".to_owned())
                        })?
                        .parse::<u64>()
                        .map_err(|_| AhrbError::Usage("invalid retry maximum delay".to_owned()))?,
                );
            }
            "--compact-after-turn" => {
                index += 1;
                compact_after_turn = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage("--compact-after-turn needs a value".to_owned())
                        })?
                        .parse::<u64>()
                        .map_err(|_| AhrbError::Usage("invalid compaction turn".to_owned()))?,
                );
            }
            "--model-request-ceiling" => {
                index += 1;
                model_request_ceiling = args
                    .get(index)
                    .ok_or_else(|| {
                        AhrbError::Usage("--model-request-ceiling needs a value".to_owned())
                    })?
                    .parse::<u64>()
                    .map_err(|_| AhrbError::Usage("invalid model-request ceiling".to_owned()))?;
            }
            "--model-request-cap-exit-code" => {
                index += 1;
                model_request_cap_exit_code = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage(
                                "--model-request-cap-exit-code needs a value".to_owned(),
                            )
                        })?
                        .parse::<i32>()
                        .map_err(|_| AhrbError::Usage("invalid cap exit code".to_owned()))?,
                );
            }
            "--model-request-cap-stall-ms" => {
                index += 1;
                model_request_cap_stall_ms = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage(
                                "--model-request-cap-stall-ms needs a value".to_owned(),
                            )
                        })?
                        .parse::<u64>()
                        .map_err(|_| AhrbError::Usage("invalid cap stall duration".to_owned()))?,
                );
            }
            "--retain-recent-tool-results" => {
                index += 1;
                retain_recent_tool_results = Some(
                    args.get(index)
                        .ok_or_else(|| {
                            AhrbError::Usage(
                                "--retain-recent-tool-results needs a value".to_owned(),
                            )
                        })?
                        .parse::<usize>()
                        .map_err(|_| {
                            AhrbError::Usage("invalid retained tool-result count".to_owned())
                        })?,
                );
            }
            "--suppress-fixture-effects" => {
                suppress_fixture_effects = true;
            }
            "--suppress-narrative" => {
                suppress_narrative = true;
            }
            "--silent-compaction" => {
                silent_compaction = true;
            }
            "--disable-compaction" => {
                disable_compaction = true;
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
    let provider_mailbox = std::env::var_os("AHRB_MOCK_PROVIDER_MAILBOX")
        .map(PathBuf::from)
        .or_else(|| {
            base_url.as_deref().and_then(|url| {
                url.strip_prefix("ahrb+mailbox://")
                    .filter(|path| !path.is_empty())
                    .map(PathBuf::from)
            })
        });
    let context_window_tokens = match std::env::var("AHRB_MOCK_CONTEXT_WINDOW_TOKENS") {
        Ok(value) => Some(value.parse::<u64>().map_err(|_| {
            AhrbError::Validation(
                "AHRB_MOCK_CONTEXT_WINDOW_TOKENS must be an unsigned integer".to_owned(),
            )
        })?),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(AhrbError::Validation(
                "AHRB_MOCK_CONTEXT_WINDOW_TOKENS is not Unicode".to_owned(),
            ));
        }
    };
    if context_window_tokens == Some(0) {
        return Err(AhrbError::Validation(
            "mock context window must be positive".to_owned(),
        ));
    }
    let embedded_model = match std::env::var_os("AHRB_MOCK_EMBEDDED_WORKFLOW") {
        Some(path) => {
            let workflow: Workflow = serde_json::from_slice(&fs::read(PathBuf::from(path))?)?;
            Some(Arc::new(
                FakeModelEngine::with_request_roles_and_context_window(
                    &workflow,
                    &BTreeMap::new(),
                    &[],
                    context_window_tokens,
                )?,
            ))
        }
        None => None,
    };
    let retry_max_attempts = retry_max_attempts.map_or_else(
        || parse_u64_environment("AHRB_MOCK_RETRY_MAX_ATTEMPTS", 1),
        Ok,
    )?;
    let retry_max_attempts = u32::try_from(retry_max_attempts)
        .map_err(|_| AhrbError::Validation("mock retry maximum does not fit u32".to_owned()))?;
    let retry_base_delay_ms = retry_base_delay_ms.map_or_else(
        || parse_u64_environment("AHRB_MOCK_RETRY_BASE_DELAY_MS", 50),
        Ok,
    )?;
    let retry_max_delay_ms = retry_max_delay_ms.map_or_else(
        || parse_u64_environment("AHRB_MOCK_RETRY_MAX_DELAY_MS", 50),
        Ok,
    )?;
    let max_output_bytes = parse_u64_environment("AHRB_MOCK_MAX_OUTPUT_BYTES", 1_048_576)?;
    let turn_timeout_ms = parse_u64_environment("AHRB_MOCK_TURN_TIMEOUT_MS", 10_000)?;
    let tariff_input_microusd_per_token =
        parse_u64_environment("AHRB_MOCK_INPUT_PRICE_MICROUSD", 2)?;
    let tariff_output_microusd_per_token =
        parse_u64_environment("AHRB_MOCK_OUTPUT_PRICE_MICROUSD", 3)?;
    let max_output_bytes = usize::try_from(max_output_bytes).map_err(|_| {
        AhrbError::Validation("mock maximum output bytes do not fit usize".to_owned())
    })?;
    if retry_max_attempts == 0 || retry_max_attempts > 6 {
        return Err(AhrbError::Validation(
            "mock retry maximum must be in 1..=6".to_owned(),
        ));
    }
    if retry_max_attempts > 1
        && (retry_base_delay_ms < 50
            || retry_max_delay_ms == 0
            || retry_base_delay_ms > retry_max_delay_ms)
    {
        return Err(AhrbError::Validation(
            "mock retry delays require base >= 50 ms and 0 < base <= max".to_owned(),
        ));
    }
    if !(1..=1_048_576).contains(&max_output_bytes) {
        return Err(AhrbError::Validation(
            "mock maximum output bytes must be in 1..=1,048,576".to_owned(),
        ));
    }
    match (compact_after_turn, retain_recent_tool_results) {
        (Some(0), _) | (_, Some(0)) => {
            return Err(AhrbError::Validation(
                "mock compaction turn and retained tool-result count must be positive".to_owned(),
            ));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(AhrbError::Validation(
                "mock compaction requires both --compact-after-turn and --retain-recent-tool-results"
                    .to_owned(),
            ));
        }
        (Some(_), Some(_)) | (None, None) => {}
    }
    if model_request_ceiling == 0 {
        return Err(AhrbError::Validation(
            "mock model-request ceiling must be positive".to_owned(),
        ));
    }
    if model_request_cap_exit_code == Some(0) {
        return Err(AhrbError::Validation(
            "mock cap exit code must be non-zero".to_owned(),
        ));
    }
    if model_request_cap_stall_ms == Some(0) {
        return Err(AhrbError::Validation(
            "mock cap stall duration must be positive".to_owned(),
        ));
    }
    if model_request_cap_exit_code.is_some() && model_request_cap_stall_ms.is_some() {
        return Err(AhrbError::Validation(
            "mock request cap accepts either exit or stall behavior, not both".to_owned(),
        ));
    }
    #[cfg(unix)]
    let provider_stream = inherited_provider_stream()?;
    let owned_egress_guard = OwnedEgressGuard::from_environment(&state_dir)?;
    Ok(MockConfig {
        state_dir,
        workspace_override: std::env::var_os("AHRB_MOCK_WORKSPACE_OVERRIDE")
            .or_else(|| std::env::var_os("AHRB_FIDELITY_WORKSPACE"))
            .map(PathBuf::from),
        base_url,
        unix_socket: std::env::var_os("AHRB_MOCK_UNIX_SOCKET").map(PathBuf::from),
        provider_mailbox,
        #[cfg(unix)]
        provider_stream,
        embedded_model,
        api_key: std::env::var("AHRB_MOCK_API_KEY").ok(),
        model: std::env::var("AHRB_MOCK_MODEL").unwrap_or_else(|_| "ahrb-fake-v1".to_owned()),
        tariff_input_microusd_per_token,
        tariff_output_microusd_per_token,
        idle_timeout: Duration::from_millis(idle_timeout_ms),
        turn_timeout: Duration::from_millis(turn_timeout_ms),
        retry_max_attempts,
        retry_base_delay_ms,
        retry_max_delay_ms,
        max_output_bytes,
        context_window_tokens,
        model_request_ceiling,
        model_request_cap_exit_code,
        model_request_cap_stall_ms,
        compact_after_turn,
        retain_recent_tool_results,
        suppress_fixture_effects,
        suppress_narrative,
        silent_compaction,
        disable_compaction,
        session_memory_bytes,
        acceptance_hook: parse_hook_env("AHRB_MOCK_ACCEPTANCE_HOOK")?,
        completion_hook: parse_hook_env("AHRB_MOCK_COMPLETION_HOOK")?,
        declare_native_shell: false,
        owned_egress_guard,
    })
}

#[cfg(unix)]
fn inherited_provider_stream() -> Result<Option<Arc<Mutex<tokio::net::UnixStream>>>> {
    use std::os::fd::FromRawFd as _;

    let Some(value) = std::env::var_os("AHRB_MOCK_PROVIDER_FD") else {
        return Ok(None);
    };
    let fd = value
        .to_string_lossy()
        .parse::<libc::c_int>()
        .map_err(|_| AhrbError::Validation("AHRB_MOCK_PROVIDER_FD is not an integer".to_owned()))?;
    if fd < 3 {
        return Err(AhrbError::Validation(
            "AHRB_MOCK_PROVIDER_FD must not alias stdio".to_owned(),
        ));
    }
    // SAFETY: the runner passes one inherited, non-CLOEXEC descriptor and the
    // child claims sole ownership exactly once while constructing MockConfig.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    let stream = tokio::net::UnixStream::from_std(stream)?;
    Ok(Some(Arc::new(Mutex::new(stream))))
}

fn parse_u64_environment(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|_| AhrbError::Validation(format!("{name} must be an unsigned integer"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(AhrbError::Validation(format!("{name} is not Unicode")))
        }
    }
}

fn optional_unicode_environment(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(AhrbError::Validation(format!("{name} is not Unicode")))
        }
    }
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
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    #[cfg(unix)]
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    loop {
        #[cfg(unix)]
        let line = tokio::select! {
            line = lines.next_line() => line?,
            signal = sigterm.recv() => {
                if signal.is_some() {
                    terminalize_active_for_control(&harness, "sigterm").await?;
                }
                None
            }
            signal = sigint.recv() => {
                if signal.is_some() {
                    terminalize_active_for_control(&harness, "sigint").await?;
                }
                None
            }
            signal = sighup.recv() => {
                if signal.is_some() {
                    terminalize_active_for_control(&harness, "sighup").await?;
                }
                None
            }
        };
        #[cfg(not(unix))]
        let line = lines.next_line().await?;
        let Some(line) = line else {
            terminalize_active_for_control(&harness, "stdin-eof").await?;
            break;
        };
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

async fn terminalize_active_for_control(
    harness: &Arc<Mutex<MockHarness>>,
    cause: &str,
) -> Result<()> {
    let mut guard = harness.lock().await;
    let active = guard
        .sessions
        .iter()
        .filter(|(_, session)| session.active)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for id in active {
        guard.append_terminal(
            &id,
            EventVocab::TerminalCancelled,
            json!({"status":"cancelled","cause":cause,"structured":true}),
        )?;
        if let Some(session) = guard.sessions.get_mut(&id) {
            session.active = false;
            session.cancelled = true;
            session.pending = None;
        }
    }
    guard.shutting_down = true;
    Ok(())
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
            let (id, workspace_path) = {
                let mut guard = harness.lock().await;
                let id = guard.create_session(marker)?;
                let workspace_path = guard.config.workspace_path(&id);
                (id, workspace_path)
            };
            Ok(json!({
                "session_id": id,
                "workspace_path": workspace_path.to_string_lossy(),
            }))
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
            let events = {
                let guard = harness.lock().await;
                let session = guard
                    .sessions
                    .get(id)
                    .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?;
                let start = after.map_or(0, |cursor| {
                    session
                        .events
                        .partition_point(|event| event.cursor <= cursor)
                });
                session.events[start..].to_vec()
            };
            Ok(json!({ "events": events }))
        }
        "sessions.wait-ready" => {
            let expected = request
                .params
                .get("session_ids")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let guard = harness.lock().await;
            let ready = expected
                .iter()
                .filter_map(Value::as_str)
                .filter(|id| {
                    guard
                        .sessions
                        .get(*id)
                        .is_some_and(|session| session_is_terminal(session).unwrap_or(false))
                })
                .map(str::to_owned)
                .collect::<Vec<_>>();
            Ok(json!({
                "schema":"haider.sessions.ready.v1",
                "ready":ready.len() == expected.len(),
                "timed_out":false,
                "daemon_ready":true,
                "expected_count":expected.len(),
                "ready_count":ready.len(),
                "total_session_count":guard.sessions.len(),
                "expected_session_ids":expected,
                "ready_session_ids":ready,
                "state_counts":{},
                "sessions":[],
                "error":Value::Null
            }))
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
            let parent_operation_started = tokio::time::Instant::now();
            let parent = required_str(&request.params, "parent_session_id")?.to_owned();
            let marker = required_str(&request.params, "marker")?.to_owned();
            let prompt = request
                .params
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let scripted_crash = marker.contains("row56-crash");
            let scripted_hang = marker.contains("row56-hang");
            let child = {
                let mut guard = harness.lock().await;
                let child = guard.create_session(&marker)?;
                guard.append(
                    &parent,
                    EventVocab::AgentSpawned,
                    json!({ "child_session_id": child, "marker": marker }),
                )?;
                if scripted_crash || scripted_hang {
                    guard.append(
                        &parent,
                        EventVocab::TurnAccepted,
                        json!({"operation":"agent.spawn","child_session_id":child}),
                    )?;
                    guard.append(
                        &child,
                        EventVocab::TurnAccepted,
                        json!({"operation":"native-child","parent_session_id":parent}),
                    )?;
                }
                child
            };
            if scripted_crash {
                let mut guard = harness.lock().await;
                guard.append_terminal(
                    &child,
                    EventVocab::TerminalFailure,
                    json!({"status":"failure","category":"scripted-child-crash","checkpoint":"row56-crash"}),
                )?;
                guard.append_terminal(
                    &parent,
                    EventVocab::TerminalFailure,
                    json!({"status":"failure","category":"child-failure","child_session_id":child}),
                )?;
                return Ok(json!({"session_id":child}));
            }
            if scripted_hang {
                let timeout = harness.lock().await.config.turn_timeout;
                {
                    let mut guard = harness.lock().await;
                    guard.append(
                        &child,
                        EventVocab::ModelRequest,
                        json!({"checkpoint":"row56-hang","fault":"stall"}),
                    )?;
                    guard.append(
                        &child,
                        EventVocab::ModelResponse,
                        json!({"checkpoint":"row56-hang","response_headers":true,"fault":"stall"}),
                    )?;
                }
                let remaining = timeout.saturating_sub(parent_operation_started.elapsed());
                let task_harness = Arc::clone(&harness);
                let task_parent = parent.clone();
                let task_child = child.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(remaining).await;
                    let mut guard = task_harness.lock().await;
                    let _ = guard.append_terminal(
                        &task_child,
                        EventVocab::TerminalCancelled,
                        json!({"status":"cancelled","category":"parent-deadline"}),
                    );
                    let _ = guard.append_terminal(
                        &task_parent,
                        EventVocab::TerminalFailure,
                        json!({"status":"failure","category":"child-deadline","child_session_id":task_child}),
                    );
                });
                return Ok(json!({"session_id":child}));
            }
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
        "session.close-delete" => {
            let id = required_str(&request.params, "session_id")?.to_owned();
            let (released_bytes, waiters) = harness.lock().await.close_delete_session(&id)?;
            for waiter in waiters {
                waiter.notify_one();
            }
            Ok(json!({
                "closed": true,
                "deleted": true,
                "session_id": id,
                "released_bytes": released_bytes
            }))
        }
        "checkpoint.release" => {
            let session_id = required_str(&request.params, "session_id")?.to_owned();
            let release_token = required_str(&request.params, "release_token")?.to_owned();
            release_checkpoint(&harness, &session_id, &release_token).await
        }
        "harness.shutdown" => {
            harness.lock().await.shutting_down = true;
            Ok(json!({
                "schema":"haider.daemon-stop.v1",
                "outcome":"stopped_cleanly",
                "daemon":{"shutdown_acknowledged":true,"process_exited":true}
            }))
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
    let terminal_not_before = reference_terminal_floor_ms(&turn.prompt).map(|floor_ms| {
        tokio::time::Instant::now()
            .checked_add(Duration::from_millis(floor_ms))
            .unwrap_or_else(tokio::time::Instant::now)
    });
    let hook = {
        let mut guard = harness.lock().await;
        // The Wave-3 fanout fixture models a bounded shared cache while
        // preserving the ordinary row-26/27 per-session fixture unchanged.
        let fanout_width = turn
            .prompt
            .split_whitespace()
            .find_map(|word| word.strip_prefix("AHRB-FANOUT-WIDTH="))
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0);
        let reservation_bytes = match fanout_width {
            Some(width) if guard.config.declare_native_shell => {
                // One-shot clients contribute their executable footprint to
                // the measured whole-tree total. Rebalance a fixed reference
                // cache budget across clients so the actual out-of-band RSS
                // curve remains smooth without normalizing the measurement.
                const EXEC_FANOUT_SHARED_BYTES: u64 = 320 * MIB;
                const EXEC_FANOUT_CLIENT_ALLOWANCE_BYTES: u64 = 8 * MIB;
                EXEC_FANOUT_SHARED_BYTES
                    .checked_div(width)
                    .unwrap_or(0)
                    .saturating_sub(EXEC_FANOUT_CLIENT_ALLOWANCE_BYTES)
            }
            Some(width) => guard.config.session_memory_bytes / width,
            None => guard.config.session_memory_bytes,
        };
        let closed_on_disk = guard
            .config
            .state_dir
            .join("sessions")
            .join(id)
            .join("closed.json")
            .is_file();
        let session = guard.session_mut(id)?;
        if session.closed || closed_on_disk {
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
        session.terminal_not_before = terminal_not_before;
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
    if turn.prompt.contains("AHRB-ROW53-LARGE-JOURNAL") {
        {
            let mut guard = harness.lock().await;
            if session_should_stop(guard.session_mut(id)?)? {
                return Ok(());
            }
            // The daemon stays alive independently of the worker, so a
            // committed terminal record is useful prefix evidence there. A
            // per-invocation client exits as soon as it observes a terminal;
            // keep that client alive until the collector kills it instead.
            if !config.declare_native_shell {
                guard.append_terminal(
                    id,
                    EventVocab::TerminalSuccess,
                    json!({"status":"success","fixture":"row53-prefix-committed"}),
                )?;
            }
            let session = guard.session_mut(id)?;
            session.journal.append_row53_large_record()?;
        }
        std::future::pending::<()>().await;
        return Ok(());
    }
    if let Some(guard) = &config.owned_egress_guard {
        guard.verify_forbidden_probe().await?;
    }
    if config.base_url.is_none()
        && config.unix_socket.is_none()
        && config.provider_mailbox.is_none()
        && config.embedded_model.is_none()
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
    let context_fixture = config.context_window_tokens.filter(|_| {
        turn.prompt
            .starts_with("ahrb-row51 deterministic context recovery ")
    });
    let committed_context_events = if context_fixture.is_some() {
        let guard = harness.lock().await;
        let session = guard
            .sessions
            .get(id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?;
        let recovery_accept = session
            .events
            .iter()
            .rposition(|event| {
                event.event == EventVocab::TurnAccepted
                    && event.payload.get("key").and_then(Value::as_str) == Some(turn.key.as_str())
            })
            .ok_or_else(|| {
                AhrbError::Protocol("context recovery turn lacks its durable acceptance".to_owned())
            })?;
        session.events[..recovery_accept].to_vec()
    } else {
        Vec::new()
    };
    let mut messages = match context_fixture {
        Some(window_tokens) => build_context_overlimit_messages(
            &config,
            &turn.prompt,
            window_tokens,
            &committed_context_events,
        )?,
        None => vec![json!({ "role": "user", "content": turn.prompt })],
    };
    if context_fixture.is_some() {
        storage_lifecycle::compaction(&config, id, false)?;
    }
    let mut turn_input_tokens = 0_u64;
    let mut turn_output_tokens = 0_u64;
    for checkpoint in 0..config.model_request_ceiling {
        {
            let mut guard = harness.lock().await;
            let session = guard.session_mut(id)?;
            if session_should_stop(session)? {
                return Ok(());
            }
            for prompt in std::mem::take(&mut session.injected) {
                messages.push(json!({ "role": "user", "content": prompt }));
            }
        }
        if config
            .compact_after_turn
            .is_some_and(|turn| checkpoint.saturating_add(1) > turn)
            && let Some(retain) = config.retain_recent_tool_results
        {
            messages = retain_recent_tool_result_pairs(&messages, retain);
        }
        let mut request = json!({
            "model": config.model,
            "messages": messages,
            "tools": fixture_tools(config.declare_native_shell),
            "stream": false
        });
        let mut headers = BTreeMap::new();
        if let Some(key) = &config.api_key {
            headers.insert("Authorization".to_owned(), format!("Bearer {key}"));
        }
        let mut body = serde_json::to_vec(&request)?;
        let mut physical_attempt = 0_u32;
        let mut compacted_after_context_error = false;
        let response = loop {
            physical_attempt = physical_attempt.saturating_add(1);
            {
                let mut guard = harness.lock().await;
                guard.append(
                    id,
                    EventVocab::ModelRequest,
                    json!({
                        "model": config.model,
                        "endpoint": "/v1/chat/completions",
                        "checkpoint": checkpoint,
                        "physical_attempt": physical_attempt,
                    }),
                )?;
            }
            let request_started = std::time::Instant::now();
            let response_result = model_http_post(&config, &headers, &body).await;
            let response = match response_result {
                Ok(response) => response,
                Err(AhrbError::Timeout(message)) => {
                    let mut guard = harness.lock().await;
                    guard.append_terminal(
                        id,
                        EventVocab::TerminalFailure,
                        json!({
                            "status": "failure",
                            "category": "idle-timeout",
                            "message": message,
                            "elapsed_ms": u64::try_from(request_started.elapsed().as_millis()).unwrap_or(u64::MAX)
                        }),
                    )?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            if response.status == 400
                && context_fixture.is_some()
                && !compacted_after_context_error
                && is_context_length_error(&response.body)
            {
                if config.disable_compaction {
                    let mut guard = harness.lock().await;
                    guard.append_terminal(
                        id,
                        EventVocab::TerminalFailure,
                        json!({
                            "status": "failure",
                            "category": "context-limit",
                            "message": "context compaction is disabled",
                        }),
                    )?;
                    return Ok(());
                }
                let (compacted_messages, omitted_markers) = compact_context_messages(&messages)?;
                let dropped_count = u64::try_from(omitted_markers.len())
                    .map_err(|_| AhrbError::Protocol("compaction scope exceeds u64".to_owned()))?;
                let dropped_span = json!({
                    "first": omitted_markers.first(),
                    "last": omitted_markers.last(),
                });
                messages = compacted_messages;
                if !config.silent_compaction {
                    let mut guard = harness.lock().await;
                    guard.append(
                        id,
                        EventVocab::ContextCompacted,
                        json!({
                            "turn_key": turn.key,
                            "dropped_count": dropped_count,
                            "dropped_span": dropped_span,
                        }),
                    )?;
                }
                request = json!({
                    "model": config.model,
                    "messages": messages,
                    "tools": fixture_tools(config.declare_native_shell),
                    "stream": false
                });
                body = serde_json::to_vec(&request)?;
                let window_tokens = context_fixture.ok_or_else(|| {
                    AhrbError::Protocol("context fixture window disappeared".to_owned())
                })?;
                let compacted_tokens = crate::wave3_long_horizon::fake_context_input_tokens(
                    "openai-chat-completions",
                    &request,
                );
                if compacted_tokens > window_tokens
                    || body.len() as u64 > window_tokens.saturating_mul(8)
                {
                    return Err(AhrbError::Protocol(format!(
                        "deterministic compaction produced {compacted_tokens} tokens/{} bytes for W={window_tokens}",
                        body.len()
                    )));
                }
                storage_lifecycle::compaction(&config, id, true)?;
                compacted_after_context_error = true;
                continue;
            }
            let retryable = matches!(response.status, 429 | 500);
            if retryable && physical_attempt < config.retry_max_attempts {
                let delay_ms = jittered_retry_delay_ms(&config, physical_attempt);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                continue;
            }
            break response;
        };
        if !(200..300).contains(&response.status) {
            let mut guard = harness.lock().await;
            guard.append_terminal(
                id,
                EventVocab::TerminalFailure,
                json!({
                    "status": "failure",
                    "category": "provider",
                    "http_status": response.status,
                    "physical_attempts": physical_attempt,
                }),
            )?;
            return Ok(());
        }
        let (message, response_input_tokens, response_output_tokens) =
            if is_trickle_success_body(&response.body) {
                (json!({"role":"assistant","content":"SUCCESS"}), 0, 0)
            } else {
                let value: Value = serde_json::from_slice(&response.body)?;
                let (input_tokens, output_tokens) = chat_response_usage(&value)?;
                let message = value
                    .pointer("/choices/0/message")
                    .cloned()
                    .ok_or_else(|| {
                        AhrbError::Protocol("model response omitted choices[0].message".to_owned())
                    })?;
                (message, input_tokens, output_tokens)
            };
        turn_input_tokens = turn_input_tokens
            .checked_add(response_input_tokens)
            .ok_or_else(|| AhrbError::Protocol("turn input-token usage overflow".to_owned()))?;
        turn_output_tokens = turn_output_tokens
            .checked_add(response_output_tokens)
            .ok_or_else(|| AhrbError::Protocol("turn output-token usage overflow".to_owned()))?;
        {
            let mut guard = harness.lock().await;
            if session_should_stop(guard.session_mut(id)?)? {
                return Ok(());
            }
            let mut response_payload = json!({
                "checkpoint": checkpoint,
                "usage": {
                    "input_tokens": response_input_tokens,
                    "output_tokens": response_output_tokens,
                    "total_tokens": response_input_tokens.saturating_add(response_output_tokens)
                }
            });
            if !config.suppress_narrative
                && message.get("reasoning_content").is_some()
                && let Some(object) = response_payload.as_object_mut()
            {
                object.insert(
                    "assistant_text".to_owned(),
                    message.get("content").cloned().unwrap_or(Value::Null),
                );
                object.insert(
                    "reasoning".to_owned(),
                    message
                        .get("reasoning_content")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
            }
            guard.append(id, EventVocab::ModelResponse, response_payload)?;
        }
        let tool_calls = message
            .get("tool_calls")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        messages.push(message.clone());
        if tool_calls.is_empty() {
            storage_fixture_write(&config, &turn.prompt)?;
            let content = message.get("content").cloned().unwrap_or(Value::Null);
            let (event, status) = terminal_from_content(&content);
            if let Some(width) = turn
                .prompt
                .split_whitespace()
                .find_map(|word| word.strip_prefix("AHRB-FANOUT-WIDTH="))
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
            {
                // Model a deterministic logarithmic scheduler quantum around
                // the reference fixture's fsync-backed terminal commit. This
                // keeps the mock's service curve smooth across both geometric
                // quick widths and every integer certification width without
                // changing the durable event work performed by each actor.
                let padding_ms =
                    100_u64.saturating_add(((width as f64).ln() * 100.0).round().max(0.0) as u64);
                tokio::time::sleep(Duration::from_millis(padding_ms)).await;
            }
            wait_for_reference_terminal_floor(harness, id).await?;
            let total_tokens = turn_input_tokens
                .checked_add(turn_output_tokens)
                .ok_or_else(|| AhrbError::Protocol("turn total-token usage overflow".to_owned()))?;
            let input_cost = turn_input_tokens
                .checked_mul(config.tariff_input_microusd_per_token)
                .ok_or_else(|| AhrbError::Protocol("turn input cost overflow".to_owned()))?;
            let output_cost = turn_output_tokens
                .checked_mul(config.tariff_output_microusd_per_token)
                .ok_or_else(|| AhrbError::Protocol("turn output cost overflow".to_owned()))?;
            let cost_microusd = input_cost
                .checked_add(output_cost)
                .ok_or_else(|| AhrbError::Protocol("turn total cost overflow".to_owned()))?;
            let mut guard = harness.lock().await;
            guard.append_terminal(
                id,
                event,
                json!({
                    "status": status,
                    "content": content,
                    "usage": {
                        "input_tokens": turn_input_tokens,
                        "output_tokens": turn_output_tokens,
                        "total_tokens": total_tokens,
                        "cost_microusd": cost_microusd,
                        "turns": 1
                    }
                }),
            )?;
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
    if let Some(code) = config.model_request_cap_exit_code {
        std::process::exit(code);
    }
    if let Some(stall_ms) = config.model_request_cap_stall_ms {
        tokio::time::sleep(Duration::from_millis(stall_ms)).await;
        return Ok(());
    }
    let mut guard = harness.lock().await;
    guard.append_terminal(
        id,
        EventVocab::TerminalFailure,
        json!({ "status": "failure", "category": "turn-limit" }),
    )?;
    Ok(())
}

fn retain_recent_tool_result_pairs(messages: &[Value], retain: usize) -> Vec<Value> {
    let result_ids = messages
        .iter()
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("tool"))
        .filter_map(|message| message.get("tool_call_id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let retained_ids = result_ids
        .iter()
        .skip(result_ids.len().saturating_sub(retain))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut compacted = Vec::new();
    for message in messages {
        match message.get("role").and_then(Value::as_str) {
            Some("tool") => {
                if message
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| retained_ids.contains(id))
                {
                    compacted.push(message.clone());
                }
            }
            Some("assistant")
                if message
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .is_some() =>
            {
                let mut retained_message = message.clone();
                let Some(calls) = retained_message
                    .get_mut("tool_calls")
                    .and_then(Value::as_array_mut)
                else {
                    continue;
                };
                calls.retain(|call| {
                    call.get("id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| retained_ids.contains(id))
                });
                if !calls.is_empty() {
                    compacted.push(retained_message);
                }
            }
            _ => compacted.push(message.clone()),
        }
    }
    compacted
}

async fn wait_for_reference_terminal_floor(
    harness: &Arc<Mutex<MockHarness>>,
    id: &str,
) -> Result<()> {
    let deadline = {
        let guard = harness.lock().await;
        guard
            .sessions
            .get(id)
            .ok_or_else(|| AhrbError::Protocol(format!("unknown session {id:?}")))?
            .terminal_not_before
    };
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    }
    Ok(())
}

fn build_context_overlimit_messages(
    config: &MockConfig,
    prompt: &str,
    window_tokens: u64,
    committed_events: &[NormalizedEvent],
) -> Result<Vec<Value>> {
    let (ordinary_turns, tool_pairs) = if window_tokens <= 4_096 {
        (16_u32, 4_u32)
    } else {
        (128_u32, 32_u32)
    };
    let mut messages = vec![json!({"role":"system","content":"AHRB-CONTEXT-INSTRUCTIONS-v1"})];
    let mut observed_history = 0_u32;
    let mut observed_calls = 0_u32;
    let mut observed_results = 0_u32;
    for event in committed_events {
        match event.event {
            EventVocab::TurnAccepted
                if event
                    .payload
                    .get("key")
                    .and_then(Value::as_str)
                    .is_some_and(|key| key.starts_with("row-51-history-")) =>
            {
                let content = event
                    .payload
                    .get("prompt")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AhrbError::Protocol(
                            "row-51 committed history acceptance omitted prompt".to_owned(),
                        )
                    })?;
                messages.push(json!({"role":"user","content":content}));
                observed_history = observed_history.saturating_add(1);
            }
            EventVocab::ToolCall => {
                let call_id = required_str(&event.payload, "call_id")?;
                let name = required_str(&event.payload, "name")?;
                let arguments = event.payload.get("arguments").ok_or_else(|| {
                    AhrbError::Protocol("row-51 committed tool call omitted arguments".to_owned())
                })?;
                messages.push(json!({
                    "role":"assistant",
                    "tool_calls":[{
                        "id":call_id,
                        "type":"function",
                        "function":{
                            "name":name,
                            "arguments":serde_json::to_string(arguments)?,
                        }
                    }]
                }));
                observed_calls = observed_calls.saturating_add(1);
            }
            EventVocab::ToolResult => {
                let call_id = required_str(&event.payload, "call_id")?;
                let result = event.payload.get("result").ok_or_else(|| {
                    AhrbError::Protocol("row-51 committed tool result omitted result".to_owned())
                })?;
                messages.push(json!({
                    "role":"tool",
                    "tool_call_id":call_id,
                    "content":serde_json::to_string(result)?,
                }));
                observed_results = observed_results.saturating_add(1);
            }
            _ => {}
        }
    }
    if observed_history != ordinary_turns
        || observed_calls != tool_pairs
        || observed_results != tool_pairs
    {
        return Err(AhrbError::Protocol(format!(
            "row-51 recovery reconstructed {observed_history} history turns and {observed_calls}/{observed_results} tool calls/results; expected {ordinary_turns} and {tool_pairs}/{tool_pairs}"
        )));
    }
    // The active turn is last so the provider routes on the recovery marker,
    // after observing the exact committed transcript that precedes it.
    messages.push(json!({"role":"user","content":format!("AHRB-GOAL-MARKER {prompt}")}));
    messages.push(json!({"role":"user","content":"AHRB-CONTEXT-PADDING z"}));
    let padding_index = messages.len().saturating_sub(1);
    let mut request = json!({
        "model":config.model,
        "messages":messages,
        "tools":fixture_tools(config.declare_native_shell),
        "stream":false,
    });
    let target_tokens = window_tokens.saturating_add(256);
    let base_tokens =
        crate::wave3_long_horizon::fake_context_input_tokens("openai-chat-completions", &request);
    if base_tokens > target_tokens {
        return Err(AhrbError::Protocol(format!(
            "context fixture base token count {base_tokens} exceeds target {target_tokens}"
        )));
    }
    let deficit = target_tokens.saturating_sub(base_tokens);
    let mut padding = String::from("AHRB-CONTEXT-PADDING ");
    if deficit == 0 {
        // Keep the existing final `z` token already counted in the base request.
        padding.push('z');
    } else {
        // The placeholder contributed one token, so replace it with exactly
        // `deficit + 1` tokens to increase the request by `deficit`.
        for _ in 0..deficit {
            padding.push_str("z ");
        }
        padding.push('z');
    }
    let padding_slot = request
        .pointer_mut(&format!("/messages/{padding_index}/content"))
        .ok_or_else(|| AhrbError::Protocol("context padding slot is absent".to_owned()))?;
    *padding_slot = Value::String(padding);
    let measured_tokens =
        crate::wave3_long_horizon::fake_context_input_tokens("openai-chat-completions", &request);
    if measured_tokens != target_tokens {
        return Err(AhrbError::Protocol(format!(
            "context token padding measured {measured_tokens}, expected {target_tokens}"
        )));
    }
    let target_body = window_tokens
        .checked_mul(8)
        .and_then(|value| value.checked_add(1_024))
        .ok_or_else(|| AhrbError::Protocol("context body target overflow".to_owned()))?;
    let current_body = serde_json::to_vec(&request)?.len() as u64;
    if current_body > target_body {
        return Err(AhrbError::Protocol(format!(
            "context fixture base body {current_body} exceeds target {target_body}"
        )));
    }
    let extra = usize::try_from(target_body - current_body)
        .map_err(|_| AhrbError::Protocol("context padding length does not fit usize".to_owned()))?;
    let padding = request
        .pointer_mut(&format!("/messages/{padding_index}/content"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| AhrbError::Protocol("context padding content is not a string".to_owned()))?
        .to_owned();
    *request
        .pointer_mut(&format!("/messages/{padding_index}/content"))
        .ok_or_else(|| AhrbError::Protocol("context padding slot disappeared".to_owned()))? =
        Value::String(format!("{padding}{}", "z".repeat(extra)));
    let final_body = serde_json::to_vec(&request)?;
    let final_tokens =
        crate::wave3_long_horizon::fake_context_input_tokens("openai-chat-completions", &request);
    if final_body.len() as u64 != target_body || final_tokens != target_tokens {
        return Err(AhrbError::Protocol(format!(
            "context fixture ended at {final_tokens} tokens/{} bytes, expected {target_tokens}/{target_body}",
            final_body.len()
        )));
    }
    request
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| AhrbError::Protocol("context fixture messages are absent".to_owned()))
}

fn compact_context_messages(messages: &[Value]) -> Result<(Vec<Value>, Vec<String>)> {
    let mut ordinary = Vec::<(u32, Value)>::new();
    let mut output = Vec::new();
    let mut current_turn = Vec::new();
    for message in messages {
        let content = message.get("content").and_then(Value::as_str).unwrap_or("");
        if let Some(rest) = content.strip_prefix("AHRB-HISTORY-") {
            let ordinal = rest
                .split('-')
                .next()
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| {
                    AhrbError::Protocol("context history marker lacks an ordinal".to_owned())
                })?;
            ordinary.push((ordinal, message.clone()));
        } else if content.starts_with("AHRB-GOAL-MARKER ") {
            current_turn.push(message.clone());
        } else if !content.starts_with("AHRB-CONTEXT-PADDING") {
            output.push(message.clone());
        }
    }
    ordinary.sort_by_key(|(ordinal, _)| *ordinal);
    let retain_from = ordinary.len().saturating_sub(4);
    let omitted = ordinary[..retain_from]
        .iter()
        .filter_map(|(_, message)| {
            message
                .get("content")
                .and_then(Value::as_str)
                .and_then(|content| {
                    content
                        .split_whitespace()
                        .find(|token| token.starts_with("AHRB-HISTORY-"))
                })
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let retained = ordinary[retain_from..]
        .iter()
        .map(|(_, message)| message.clone())
        .collect::<Vec<_>>();
    let summary = json!({
        "role":"system",
        "content":format!("AHRB-COMPACTION-SUMMARY {}", omitted.join(" ")),
    });
    let insertion = output
        .iter()
        .position(|message| message.get("role").and_then(Value::as_str) != Some("system"))
        .unwrap_or(output.len());
    output.insert(insertion, summary);
    output.extend(retained);
    output.extend(current_turn);
    Ok((output, omitted))
}

fn is_context_length_error(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .is_some_and(|value| {
            value.pointer("/error/type").and_then(Value::as_str) == Some("context_length_exceeded")
                && value.pointer("/error/code").and_then(Value::as_str)
                    == Some("context_length_exceeded")
        })
}

fn is_trickle_success_body(body: &[u8]) -> bool {
    const MARKER: &[u8] = b"AHRB-TRICKLE-SUCCESS";
    matches!(body.len(), 5 | 20) && body == &MARKER[..body.len()]
}

fn jittered_retry_delay_ms(config: &MockConfig, completed_attempt: u32) -> u64 {
    let exponent = completed_attempt.saturating_sub(1).min(63);
    let nominal = config
        .retry_base_delay_ms
        .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
        .min(config.retry_max_delay_ms);
    if completed_attempt % 2 == 1 {
        nominal.saturating_mul(3) / 5
    } else {
        nominal.saturating_mul(13) / 10
    }
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
        None if name == "native_shell" => {
            Some(native_shell_result(&config, &session_id, &args).await?)
        }
        None if matches!(name.as_str(), "large_output" | "large_output_fixture") => {
            Some(large_output_result(config.max_output_bytes, &args).await?)
        }
        None => Some(fixture_result(&config, &session_id, &name, &args)?),
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
                &config.workspace_path(&session_id),
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
    storage_retain_request(config, body)?;
    #[cfg(unix)]
    if let Some(stream) = &config.provider_stream {
        if let Some(guard) = &config.owned_egress_guard {
            guard
                .record_local_provider("inherited-unix-stream".to_owned())
                .await?;
        }
        let mut stream = stream.lock().await;
        return preconnected_unix_http_post(
            &mut stream,
            "/v1/chat/completions",
            headers,
            body,
            config.idle_timeout,
        )
        .await;
    }
    if let Some(directory) = &config.provider_mailbox {
        if let Some(guard) = &config.owned_egress_guard {
            guard
                .record_local_provider(format!("mailbox:{}", directory.display()))
                .await?;
        }
        return provider_mailbox_post(directory, headers, body, config.idle_timeout).await;
    }
    if let Some(engine) = &config.embedded_model {
        if let Some(guard) = &config.owned_egress_guard {
            guard
                .record_local_provider("embedded-provider".to_owned())
                .await?;
        }
        let frontend = OpenAiChatFrontend;
        let request = frontend.parse("/v1/chat/completions", headers, body)?;
        let response = tokio::time::timeout(config.idle_timeout, engine.handle(request))
            .await
            .map_err(|_| AhrbError::Timeout("embedded model idle deadline".to_owned()))??;
        return match &response.fault {
            Some(
                Fault::HttpStatus { status, body } | Fault::SustainedHttpStatus { status, body },
            ) => Ok(crate::driver::HttpResponse {
                status: *status,
                body: body.as_bytes().to_vec(),
            }),
            Some(Fault::ContextLength { window_tokens }) => Ok(crate::driver::HttpResponse {
                status: 400,
                body: serde_json::to_vec(&json!({
                    "error": {
                        "type": "context_length_exceeded",
                        "code": "context_length_exceeded",
                        "context_window": window_tokens,
                    }
                }))?,
            }),
            Some(Fault::Stall) => tokio::time::timeout(
                config.idle_timeout,
                std::future::pending::<Result<crate::driver::HttpResponse>>(),
            )
            .await
            .map_err(|_| AhrbError::Timeout("embedded model idle deadline".to_owned()))?,
            Some(Fault::Trickle { cadence_ms, count }) => {
                let cadence = Duration::from_millis(*cadence_ms);
                for _ in 0..*count {
                    tokio::time::timeout(config.idle_timeout, tokio::time::sleep(cadence))
                        .await
                        .map_err(|_| {
                            AhrbError::Timeout("embedded model idle deadline".to_owned())
                        })?;
                }
                let rendered = frontend.render(&response)?;
                Ok(crate::driver::HttpResponse {
                    status: rendered.status,
                    body: rendered.body,
                })
            }
            Some(Fault::Delay { delay_ms }) => {
                tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                let rendered = frontend.render(&response)?;
                Ok(crate::driver::HttpResponse {
                    status: rendered.status,
                    body: rendered.body,
                })
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
        };
    }
    #[cfg(unix)]
    if let Some(socket_path) = &config.unix_socket {
        if let Some(guard) = &config.owned_egress_guard {
            guard
                .record_local_provider(format!("unix:{}", socket_path.display()))
                .await?;
        }
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
    if let Some(guard) = &config.owned_egress_guard {
        let guard = guard.clone();
        return http_post_with_connector(
            &endpoint,
            headers,
            body,
            config.idle_timeout,
            move |host, port| async move { guard.connect_provider(host, port).await },
        )
        .await;
    }
    http_post(&endpoint, headers, body, config.idle_timeout).await
}

async fn provider_mailbox_post(
    directory: &Path,
    headers: &BTreeMap<String, String>,
    body: &[u8],
    timeout: Duration,
) -> Result<crate::driver::HttpResponse> {
    let sequence = PROVIDER_MAILBOX_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let id = format!("{}-{sequence}", std::process::id());
    let envelope = ProviderMailboxRequest {
        id: id.clone(),
        method: "POST".to_owned(),
        path: "/v1/chat/completions".to_owned(),
        headers: headers.clone(),
        body: body.to_vec(),
    };
    let temporary = directory.join(format!("{id}.request.tmp"));
    let request_path = directory.join(format!("{id}.request.json"));
    tokio::fs::write(&temporary, serde_json::to_vec(&envelope)?).await?;
    tokio::fs::rename(&temporary, &request_path).await?;
    let response_path = directory.join(format!("{id}.response.json"));
    let response = tokio::time::timeout(timeout, async {
        loop {
            match tokio::fs::read(&response_path).await {
                Ok(bytes) => break Ok::<Vec<u8>, AhrbError>(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                Err(error) => break Err(error.into()),
            }
        }
    })
    .await
    .map_err(|_| AhrbError::Timeout("provider mailbox idle deadline".to_owned()))??;
    tokio::fs::remove_file(response_path).await?;
    let response: ProviderMailboxResponse = serde_json::from_slice(&response)?;
    if response.id != id {
        return Err(AhrbError::Protocol(
            "provider mailbox response correlation mismatch".to_owned(),
        ));
    }
    if let Some(error) = response.error {
        return Err(AhrbError::Protocol(error));
    }
    Ok(crate::driver::HttpResponse {
        status: response.status,
        body: response.body,
    })
}

fn fixture_tools(declare_native_shell: bool) -> Value {
    let mut tools = json!([
        {"type":"function","function":{"name":"write_fixture","parameters":{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"},"ahrb_checkpoint":{"$ref":"#/$defs/ahrb_checkpoint"}},"required":["path","content"],"$defs":{"ahrb_checkpoint":{"type":"object","properties":{"name":{"type":"string"},"phase":{"type":"string","enum":["before-effect","after-commit"]}},"required":["name"]}}}}},
        {"type":"function","function":{"name":"read_fixture","parameters":{"type":"object","properties":{"path":{"type":"string"},"ahrb_checkpoint":{"$ref":"#/$defs/ahrb_checkpoint"}},"required":["path"],"$defs":{"ahrb_checkpoint":{"type":"object","properties":{"name":{"type":"string"},"phase":{"type":"string","enum":["before-effect","after-commit"]}},"required":["name"]}}}}},
        {"type":"function","function":{"name":"fail_fixture","parameters":{"type":"object","properties":{"message":{"type":"string"}}}}},
        {"type":"function","function":{"name":"large_output","parameters":{"type":"object","properties":{"bytes":{"type":"integer","const":10485760}},"required":["bytes"]}}},
        {"type":"function","function":{"name":"native_shell","parameters":{"type":"object","properties":{"command":{"type":"string"},"route":{"type":"string"},"expected_from_a":{"type":"string"},"ahrb_checkpoint":{"$ref":"#/$defs/ahrb_checkpoint"}},"required":["command"],"additionalProperties":false,"$defs":{"ahrb_checkpoint":{"type":"object","properties":{"name":{"type":"string"},"phase":{"type":"string","enum":["before-effect","after-commit"]}},"required":["name"]}}}}},
        {"type":"function","function":{"name":"barrier","parameters":{"type":"object","properties":{"name":{"type":"string"},"wait_for_release":{"type":"boolean"}},"required":["name"]}}}
    ]);
    if !declare_native_shell {
        if let Some(tools) = tools.as_array_mut() {
            tools.retain(|tool| {
                tool.pointer("/function/name").and_then(Value::as_str) != Some("native_shell")
            });
        }
    }
    tools
}

async fn native_shell_result(config: &MockConfig, session: &str, args: &Value) -> Result<Value> {
    let command = required_str(args, "command")?;
    let workspace = config.workspace_path(session);
    fs::create_dir_all(&workspace)?;
    let output = tokio::process::Command::new("/bin/sh")
        .args(["-c", command])
        .current_dir(&workspace)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    Ok(json!({
        "ok": output.status.success(),
        "exit_code": output.status.code(),
        "stdout": String::from_utf8_lossy(&output.stdout),
        "stderr": String::from_utf8_lossy(&output.stderr)
    }))
}

fn fixture_result(config: &MockConfig, session: &str, name: &str, args: &Value) -> Result<Value> {
    match name {
        "write_fixture" | "fixture_write" => {
            let relative = safe_relative(required_str(args, "path")?)?;
            let content = required_str(args, "content")?;
            if config.suppress_fixture_effects {
                return Ok(json!({
                    "ok": false,
                    "error": "fixture effects suppressed by mock configuration",
                    "path": required_str(args, "path")?
                }));
            }
            let workspace = config.workspace_path(session);
            let path = workspace.join(&relative);
            let parent = path.parent().ok_or_else(|| {
                AhrbError::Validation("fixture destination has no parent directory".to_owned())
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                let parent_is_read_only = match parent.metadata() {
                    Ok(metadata) => metadata.permissions().mode() & 0o222 == 0,
                    Err(_) => false,
                };
                if parent_is_read_only {
                    let attempted = OpenOptions::new().write(true).create_new(true).open(&path);
                    return match attempted {
                        Ok(file) => {
                            drop(file);
                            let _ = fs::remove_file(&path);
                            Ok(json!({
                                "ok": true,
                                "bytes": content.len(),
                                "path": required_str(args, "path")?,
                                "write_errno": Value::Null,
                                "workspace_fault_control_bypassed": true
                            }))
                        }
                        Err(error) => Ok(json!({
                            "ok": false,
                            "error": error.to_string(),
                            "write_errno": error.raw_os_error(),
                            "path": required_str(args, "path")?
                        })),
                    };
                }
            }
            Ok(json!({ "ok": true, "bytes": content.len(), "path": required_str(args, "path")? }))
        }
        "read_fixture" | "fixture_read" => {
            let relative = safe_relative(required_str(args, "path")?)?;
            match fs::read_to_string(config.workspace_path(session).join(relative)) {
                Ok(content) => Ok(json!({ "ok": true, "content": content })),
                Err(error)
                    if config.suppress_fixture_effects
                        && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    Ok(json!({
                        "ok": false,
                        "error": "fixture effects suppressed by mock configuration",
                        "path": required_str(args, "path")?
                    }))
                }
                Err(error) => Err(error.into()),
            }
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

async fn large_output_result(max_output_bytes: usize, args: &Value) -> Result<Value> {
    use tokio::io::AsyncReadExt as _;
    const PRODUCED_BYTES: u64 = 10_485_760;
    const CHUNK_BYTES: usize = 16 * 1024;
    let requested_bytes = args
        .get("bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| AhrbError::Validation("large_output.bytes must be an integer".to_owned()))?;
    if requested_bytes != PRODUCED_BYTES {
        return Err(AhrbError::Validation(format!(
            "large_output.bytes must be {PRODUCED_BYTES}, got {requested_bytes}"
        )));
    }
    // The OpenAI tool message contains `serde_json::to_string(result)`.  A JSON
    // string adds two quotes and escapes the marker separator newline, so keep
    // those three bytes inside the declared complete encoded limit.
    let content_limit = max_output_bytes.checked_sub(3).ok_or_else(|| {
        AhrbError::Validation(
            "large-output limit is too small for a JSON string truncation marker".to_owned(),
        )
    })?;
    let mut capture = crate::wave2::BoundedToolCapture::new(content_limit);
    let executable = std::env::current_exe()?;
    let fixture = executable
        .parent()
        .map(|parent| parent.join("ahrb-fixture"))
        .filter(|path| path.is_file())
        .ok_or_else(|| {
            AhrbError::Protocol(
                "declared large-output fixture is not next to the mock harness".to_owned(),
            )
        })?;
    let mut child = tokio::process::Command::new(fixture)
        .args(["emit", "--bytes", &requested_bytes.to_string()])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut stdout = child.stdout.take().ok_or_else(|| {
        AhrbError::Protocol("declared large-output fixture has no stdout pipe".to_owned())
    })?;
    let mut chunk = [0_u8; CHUNK_BYTES];
    loop {
        let count = stdout.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        capture.push(&chunk[..count]);
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(AhrbError::Protocol(format!(
            "declared large-output fixture exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let captured = capture.finish(content_limit);
    let content = String::from_utf8(captured.encoded).map_err(|error| {
        AhrbError::Protocol(format!(
            "large-output fixture produced non-UTF-8 content: {error}"
        ))
    })?;
    Ok(Value::String(content))
}

fn reconcile_durable_tool_effects(
    config: &MockConfig,
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
        reconcile_fixture_effect(&config.workspace_path(session), call_id, name, args, result)?;
    }
    Ok(())
}

fn reconcile_fixture_effect(
    workspace: &Path,
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
    let path = workspace.join(relative);
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
    Ok(session.tool_results.get(call_id).cloned())
}

fn chat_response_usage(response: &Value) -> Result<(u64, u64)> {
    let Some(usage) = response.get("usage") else {
        return Ok((0, 0));
    };
    let input_tokens = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            AhrbError::Protocol("model response usage omitted integer input tokens".to_owned())
        })?;
    let output_tokens = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            AhrbError::Protocol("model response usage omitted integer output tokens".to_owned())
        })?;
    let expected_total = input_tokens
        .checked_add(output_tokens)
        .ok_or_else(|| AhrbError::Protocol("model response usage overflow".to_owned()))?;
    if let Some(total) = usage.get("total_tokens") {
        let total = total.as_u64().ok_or_else(|| {
            AhrbError::Protocol("model response total_tokens is not an integer".to_owned())
        })?;
        if total != expected_total {
            return Err(AhrbError::Protocol(format!(
                "model response total_tokens {total} does not equal input+output {expected_total}"
            )));
        }
    }
    Ok((input_tokens, output_tokens))
}

// These knobs change real file writes only for the storage fixture. Ordinary
// matrix defaults and evaluator outcomes are untouched.
fn storage_fixture_write(config: &MockConfig, prompt: &str) -> Result<()> {
    if !prompt.contains(crate::storage::TASK) {
        return Ok(());
    }
    storage_auxiliary_write(config)?;
    let mode = std::env::var("AHRB_MOCK_STORAGE_WRITE_MODE").unwrap_or_else(|_| "append".into());
    let growth = std::env::var("AHRB_MOCK_STORAGE_GROWTH").unwrap_or_else(|_| "linear".into());
    let turn = prompt
        .rsplit("checkpoint=t")
        .next()
        .and_then(|s| s.get(..4))
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| AhrbError::Protocol("mock storage turn marker missing".into()))?;
    let size = match growth.as_str() {
        // A reserved store budget is consumed as journals grow. This models a
        // finite preallocated store using real truncation and real allocated blocks.
        "bounded" => 8_388_608_u64.saturating_sub(storage_other_allocated(&config.state_dir)?),
        "linear" => turn * 65_536,
        // This coefficient clears the quick shape threshold while keeping
        // the cert fixture bounded in size (256 MB at turn 1000).
        "quadratic" => turn * turn * 256,
        _ => {
            return Err(AhrbError::Usage(
                "AHRB_MOCK_STORAGE_GROWTH must be bounded/linear/quadratic".into(),
            ));
        }
    };
    let path = config.state_dir.join("storage-payload.bin");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    let previous = file.metadata()?.len();
    let start = match mode.as_str() {
        "append" => previous.min(size),
        "rewrite" => 0,
        _ => {
            return Err(AhrbError::Usage(
                "AHRB_MOCK_STORAGE_WRITE_MODE must be append/rewrite".into(),
            ));
        }
    };
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(start))?;
    let block = [b'S'; 65_536];
    let mut remaining = size.saturating_sub(start);
    while remaining > 0 {
        let n = remaining.min(block.len() as u64) as usize;
        file.write_all(&block[..n])?;
        remaining -= n as u64;
    }
    file.set_len(size)?;
    file.sync_all()?;
    Ok(())
}

fn storage_auxiliary_write(config: &MockConfig) -> Result<()> {
    if let Ok(mode) = std::env::var("AHRB_MOCK_STORAGE_AUX_MODE") {
        let dir = config.state_dir.join("storage-aux");
        fs::create_dir_all(&dir)?;
        for family in ["logs", "store", "history"] {
            let active = dir.join(format!("{family}.log"));
            match mode.as_str() {
                "capped" => {
                    let old = dir.join(format!("{family}.log.1"));
                    let older = dir.join(format!("{family}.log.2"));
                    if old.exists() {
                        fs::rename(&old, &older)?;
                    }
                    if active.exists() {
                        fs::rename(&active, &old)?;
                    }
                    fs::write(&active, vec![b'A'; 4096])?;
                }
                "grow" => {
                    let mut f = OpenOptions::new().create(true).append(true).open(active)?;
                    f.write_all(&[b'A'; 8192])?;
                }
                _ => {
                    return Err(AhrbError::Usage(
                        "AHRB_MOCK_STORAGE_AUX_MODE must be capped/grow".into(),
                    ));
                }
            }
        }
    }
    if std::env::var("AHRB_MOCK_STORAGE_UNRELATED_GROWTH").as_deref() == Ok("1") {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(config.state_dir.join("unrelated-growth.bin"))?;
        f.write_all(&[b'X'; 16384])?;
    }
    Ok(())
}

fn storage_retain_request(config: &MockConfig, body: &[u8]) -> Result<()> {
    let mode =
        std::env::var("AHRB_MOCK_STORAGE_REQUEST_RETENTION").unwrap_or_else(|_| "none".into());
    if mode == "none" {
        return Ok(());
    }
    if !body
        .windows(crate::storage::TASK.len())
        .any(|w| w == crate::storage::TASK.as_bytes())
    {
        return Ok(());
    }
    let directory = config.state_dir.join("storage-requests");
    fs::create_dir_all(&directory)?;
    let request: Value = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(_) => return Ok(()),
    };
    match mode.as_str() {
        "full" => {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join("bodies.jsonl"))?;
            file.write_all(body)?;
            file.write_all(b"\n")?;
        }
        "deduplicated" | "partial" => {
            let blocks =
                crate::economy::canonical_message_blocks(&request, "openai-chat-completions");
            for block in blocks
                .into_iter()
                .take(if mode == "partial" { 1 } else { usize::MAX })
            {
                let bytes = serde_json::to_vec(&block)?;
                // Partial retains only the first observed block across the session.
                let name = if mode == "partial" {
                    "first.block".into()
                } else {
                    format!("{}.block", crate::storage::retention::digest(&bytes))
                };
                let path = directory.join(name);
                if !path.exists() {
                    fs::write(path, bytes)?;
                }
            }
        }
        "opaque" => {
            let mut bytes = vec![0, 255];
            bytes.extend(body.iter().map(|b| b ^ 0x80));
            fs::write(directory.join("opaque.bin"), bytes)?;
        }
        _ => {
            return Err(AhrbError::Usage(
                "AHRB_MOCK_STORAGE_REQUEST_RETENTION must be none/deduplicated/full/partial/opaque"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn storage_other_allocated(root: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let meta = fs::symlink_metadata(entry.path())?;
        if meta.is_dir() {
            total += storage_other_allocated(&entry.path())?;
        } else if meta.is_file() && entry.file_name() != "storage-payload.bin" {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt as _;
                total += meta.blocks() * 512;
            }
        }
    }
    Ok(total)
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
            .completed_hooks
            .contains(&(kind.to_owned(), turn_key.to_owned()))
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

fn remove_directory_if_present(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AhrbError::Protocol(format!("missing string field {key:?}")))
}

fn session_is_terminal(session: &SessionState) -> Result<bool> {
    Ok(session.terminal_since_last_accept)
}

fn session_should_stop(session: &SessionState) -> Result<bool> {
    Ok(session.cancelled || session.closed || session_is_terminal(session)?)
}

fn is_terminal(event: &EventVocab) -> bool {
    matches!(
        event,
        EventVocab::TerminalSuccess
            | EventVocab::TerminalFailure
            | EventVocab::TerminalCancelled
            | EventVocab::TerminalTimeout
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

    #[test]
    fn wave4_budget_terminals_use_exact_crossing_boundaries() -> Result<()> {
        let tokens = token_budget_terminal(128)?;
        assert_eq!(tokens["observed"], 136);
        assert_eq!(tokens["boundary"]["fixture_tokens"], 112);
        assert_eq!(tokens["terminal_type"], "budget-exceeded");

        let cost = cost_budget_terminal(1_000)?;
        assert_eq!(cost["observed_microusd"], 1_025);
        assert_eq!(cost["boundary"]["fixture_cost_microusd"], 975);
        assert_eq!(cost["usage"]["cost_microusd"], 1_025);
        assert_eq!(parse_usd_microusd("0.001000")?, 1_000);
        Ok(())
    }

    #[test]
    fn wave4_event_metadata_is_added_monotonically() -> Result<()> {
        let directory = temporary_dir("wave4-event-metadata");
        let _ = fs::remove_dir_all(&directory);
        let mut harness = MockHarness::open_per_invocation(test_config(directory.clone()))?;
        let session = harness.create_session("metadata-actor")?;
        harness.append(&session, EventVocab::ToolCall, json!({"call_id":"call-1"}))?;
        harness.append(
            &session,
            EventVocab::ToolResult,
            json!({"call_id":"call-1"}),
        )?;
        let events = harness.session_mut(&session)?.events.clone();
        assert_eq!(events[0].payload["schema_version"], 1);
        let first = events[0].payload["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| AhrbError::Protocol("first timestamp absent".to_owned()))?;
        let second = events[1].payload["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| AhrbError::Protocol("second timestamp absent".to_owned()))?;
        assert!(second > first);
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn wave4_session_fork_copies_nonempty_committed_history() -> Result<()> {
        let directory = temporary_dir("wave4-session-fork");
        let _ = fs::remove_dir_all(&directory);
        let mut harness = MockHarness::open_per_invocation(test_config(directory.clone()))?;
        let original = harness.create_session("session-cli-actor")?;
        harness.append(
            &original,
            EventVocab::ToolCall,
            json!({"call_id":"seed-call","name":"write_fixture"}),
        )?;
        harness.append(
            &original,
            EventVocab::ToolResult,
            json!({"call_id":"seed-call","result":{"ok":true}}),
        )?;
        harness.append_terminal(
            &original,
            EventVocab::TerminalSuccess,
            json!({"status":"success"}),
        )?;
        let expected_hashes = event_history_hashes(&harness.session_mut(&original)?.events)?;
        drop(harness);

        let args = vec![
            "--state-dir".to_owned(),
            directory.to_string_lossy().into_owned(),
            "--session-id".to_owned(),
            original.clone(),
        ];
        assert_eq!(session_fork_command(&args)?, 0);
        let fork_id = stable_session_id(&format!("fork:{original}:1"));
        let reopened = MockHarness::open_per_invocation(test_config(directory.clone()))?;
        let fork = reopened
            .sessions
            .get(&fork_id)
            .ok_or_else(|| AhrbError::Protocol("fork was not persisted".to_owned()))?;
        assert_eq!(event_history_hashes(&fork.events)?, expected_hashes);
        assert_ne!(fork_id, original);
        drop(reopened);
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn wave4_permission_trials_commit_only_the_allowed_effect() -> Result<()> {
        let directory = temporary_dir("wave4-permissions");
        let _ = fs::remove_dir_all(&directory);
        let state = directory.join("state");
        let workspace = directory.join("workspace");
        let outside = directory.join("outside.txt");
        let allow = vec![
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "--case".to_owned(),
            "allow".to_owned(),
            "--workspace".to_owned(),
            workspace.to_string_lossy().into_owned(),
        ];
        assert_eq!(permission_trial_command(&allow)?, 0);
        assert!(workspace.join("ahrb-permission-allowed.txt").is_file());
        let deny = vec![
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "--case".to_owned(),
            "deny-filesystem".to_owned(),
            "--outside-path".to_owned(),
            outside.to_string_lossy().into_owned(),
        ];
        assert_eq!(permission_trial_command(&deny)?, 0);
        assert!(!outside.exists());
        let deny_network = vec![
            "--state-dir".to_owned(),
            state.to_string_lossy().into_owned(),
            "--case".to_owned(),
            "deny-network".to_owned(),
            "--blocked-host".to_owned(),
            "127.0.0.1".to_owned(),
            "--blocked-port".to_owned(),
            "9".to_owned(),
        ];
        assert_eq!(permission_trial_command(&deny_network)?, 0);
        let ledger = fs::read_to_string(state.join("wave4-permission-ledger.jsonl"))?;
        let decision: Value = serde_json::from_str(ledger.trim_end())?;
        assert_eq!(decision["attempted"], true);
        assert_eq!(decision["allowed"], false);
        assert_eq!(decision["os_connect_attempted"], false);
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn reference_terminal_floors_are_fixture_scoped() {
        assert_eq!(
            reference_terminal_floor_ms("AHRB long horizon turn 1 marker"),
            Some(LONG_HORIZON_MIN_TURN_MS)
        );
        assert_eq!(
            reference_terminal_floor_ms("AHRB turn latency direct terminal turn 1 marker"),
            Some(TURN_LATENCY_MIN_TURN_MS)
        );
        assert_eq!(reference_terminal_floor_ms("ordinary prompt"), None);
    }

    #[test]
    fn compact_trickle_success_marker_accepts_only_defined_profiles() {
        assert!(is_trickle_success_body(b"AHRB-"));
        assert!(is_trickle_success_body(b"AHRB-TRICKLE-SUCCESS"));
        assert!(!is_trickle_success_body(b"AHRB"));
        assert!(!is_trickle_success_body(b"AHRB-TRICKLE-SUCCES"));
        assert!(!is_trickle_success_body(b"AHRB-TRICKLE-SUCCESS!"));
    }

    fn temporary_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ahrb-mock-{name}-{}", std::process::id()))
    }

    fn test_config(state_dir: PathBuf) -> MockConfig {
        MockConfig {
            state_dir,
            workspace_override: None,
            base_url: None,
            unix_socket: None,
            provider_mailbox: None,
            #[cfg(unix)]
            provider_stream: None,
            embedded_model: None,
            api_key: None,
            model: "ahrb-fake-v1".to_owned(),
            tariff_input_microusd_per_token: 2,
            tariff_output_microusd_per_token: 3,
            idle_timeout: Duration::from_millis(250),
            turn_timeout: Duration::from_secs(10),
            retry_max_attempts: 1,
            retry_base_delay_ms: 50,
            retry_max_delay_ms: 50,
            max_output_bytes: 1_048_576,
            context_window_tokens: None,
            model_request_ceiling: MAX_MODEL_REQUESTS_PER_TURN,
            model_request_cap_exit_code: None,
            model_request_cap_stall_ms: None,
            compact_after_turn: None,
            retain_recent_tool_results: None,
            suppress_fixture_effects: false,
            suppress_narrative: false,
            silent_compaction: false,
            disable_compaction: false,
            session_memory_bytes: DEFAULT_SESSION_MEMORY_MIB * MIB,
            acceptance_hook: Vec::new(),
            completion_hook: Vec::new(),
            declare_native_shell: false,
            owned_egress_guard: None,
        }
    }

    #[tokio::test]
    async fn public_close_delete_removes_daemon_session_store_and_workspace() -> Result<()> {
        let directory = temporary_dir("public-close-delete");
        let _ = fs::remove_dir_all(&directory);
        let mut harness = MockHarness::open(test_config(directory.clone()))?;
        let session = harness.create_session("close-delete-actor")?;
        let workspace = directory.join("workspaces").join(&session);
        fs::write(workspace.join("residue.txt"), b"residue")?;
        let shared = Arc::new(Mutex::new(harness));

        let result = handle_rpc(
            Arc::clone(&shared),
            RpcRequest {
                jsonrpc: "2.0".to_owned(),
                id: json!(1),
                method: "session.close-delete".to_owned(),
                params: json!({"session_id": session}),
            },
        )
        .await?;
        assert_eq!(result["closed"], true);
        assert_eq!(result["deleted"], true);
        let guard = shared.lock().await;
        assert!(!guard.sessions.contains_key(&session));
        drop(guard);
        assert!(!directory.join("sessions").join(&session).exists());
        assert!(!workspace.exists());
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn public_close_delete_preserves_external_workspace_override() -> Result<()> {
        let directory = temporary_dir("public-close-delete-override");
        let _ = fs::remove_dir_all(&directory);
        let override_path = directory.join("actor-workspace");
        fs::create_dir_all(&override_path)?;
        fs::write(override_path.join("actor-owned.txt"), b"preserve")?;
        let mut config = test_config(directory.clone());
        config.workspace_override = Some(override_path.clone());
        let mut harness = MockHarness::open(config)?;
        let session = harness.create_session("close-delete-override-actor")?;
        let shared = Arc::new(Mutex::new(harness));

        let result = handle_rpc(
            Arc::clone(&shared),
            RpcRequest {
                jsonrpc: "2.0".to_owned(),
                id: json!(1),
                method: "session.close-delete".to_owned(),
                params: json!({"session_id": session}),
            },
        )
        .await?;
        assert_eq!(result["deleted"], true);
        assert!(!directory.join("sessions").join(&session).exists());
        assert_eq!(
            fs::read(override_path.join("actor-owned.txt"))?,
            b"preserve"
        );
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn exec_close_delete_command_removes_persisted_harness_session() -> Result<()> {
        let directory = temporary_dir("exec-close-delete");
        let _ = fs::remove_dir_all(&directory);
        let mut harness = MockHarness::open_per_invocation(test_config(directory.clone()))?;
        let session = harness.create_session("exec-close-delete-actor")?;
        drop(harness);

        let args = vec![
            "--state-dir".to_owned(),
            directory.to_string_lossy().into_owned(),
            "--session-id".to_owned(),
            session.clone(),
        ];
        assert_eq!(close_delete_session_command(&args).await?, 0);
        assert!(!directory.join("sessions").join(session).exists());
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn retry_fixture_uses_bounded_observable_jitter() {
        let mut config = test_config(PathBuf::new());
        config.retry_base_delay_ms = 100;
        config.retry_max_delay_ms = 200;
        assert_eq!(jittered_retry_delay_ms(&config, 1), 60);
        assert_eq!(jittered_retry_delay_ms(&config, 2), 260);
        assert_eq!(jittered_retry_delay_ms(&config, 3), 120);
    }

    #[tokio::test]
    async fn owned_egress_connector_really_refuses_public_control_probe() -> Result<()> {
        let directory = temporary_dir("owned-egress-probe");
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory)?;
        let ledger_path = directory.join("egress.jsonl");
        let guard = OwnedEgressGuard {
            ledger_path: ledger_path.clone(),
            nonce: "a".repeat(64),
            forbidden_address: "203.0.113.1:9".parse().map_err(|_| {
                AhrbError::Protocol("test control destination did not parse".to_owned())
            })?,
            write_lock: Arc::new(Mutex::new(())),
            sequence: Arc::new(AtomicU64::new(1)),
        };
        guard.verify_forbidden_probe().await?;
        let record: OwnedEgressLedgerRecord = serde_json::from_slice(
            fs::read(&ledger_path)?
                .split(|byte| *byte == b'\n')
                .next()
                .ok_or_else(|| AhrbError::Protocol("owned egress ledger was empty".to_owned()))?,
        )?;
        assert_eq!(record.pid, std::process::id());
        assert_eq!(record.boundary, OWNED_EGRESS_BOUNDARY);
        assert_eq!(record.destination, "203.0.113.1:9");
        assert_eq!(record.category, "control-probe");
        assert!(!record.allowed);
        assert_eq!(record.outcome, "blocked-permission-denied");
        fs::remove_dir_all(directory)?;
        Ok(())
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
        let result = fixture_result(&config, &session, "write_fixture", &args)?;
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
            let worker_workspace = directory.join("workspaces/session");
            let worker_barrier = Arc::clone(&barrier);
            let worker_args = args.clone();
            let worker_result = result.clone();
            workers.push(std::thread::spawn(move || {
                worker_barrier.wait();
                reconcile_fixture_effect(
                    &worker_workspace,
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
            workspace_override: None,
            base_url: None,
            unix_socket: Some(server.socket_path().to_path_buf()),
            provider_mailbox: None,
            #[cfg(unix)]
            provider_stream: None,
            embedded_model: None,
            api_key: Some("unix-secret".to_owned()),
            model: "ahrb-fake-v1".to_owned(),
            tariff_input_microusd_per_token: 2,
            tariff_output_microusd_per_token: 3,
            idle_timeout: Duration::from_secs(2),
            turn_timeout: Duration::from_secs(10),
            retry_max_attempts: 1,
            retry_base_delay_ms: 50,
            retry_max_delay_ms: 50,
            max_output_bytes: 1_048_576,
            context_window_tokens: None,
            model_request_ceiling: MAX_MODEL_REQUESTS_PER_TURN,
            model_request_cap_exit_code: None,
            model_request_cap_stall_ms: None,
            compact_after_turn: None,
            retain_recent_tool_results: None,
            suppress_fixture_effects: false,
            suppress_narrative: false,
            silent_compaction: false,
            disable_compaction: false,
            session_memory_bytes: DEFAULT_SESSION_MEMORY_MIB * MIB,
            acceptance_hook: Vec::new(),
            completion_hook: Vec::new(),
            declare_native_shell: false,
            owned_egress_guard: None,
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
