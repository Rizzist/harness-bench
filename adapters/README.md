# Adapter data

Adapters are declarative TOML; executable-name lookup in `hbench` contains no provider
or event logic. `mock/manifest.toml` is the complete schema-2 reference. A doctor pass
means the manifest and installed version are compatible, not that benchmark rows pass.
Missing capabilities remain ABSENT/UNSUPPORTED under the row's contract.

The nine named real adapters are `claude-code`, `codex`, `haider-agent` (`hbench haider`),
`opencode`, `pi`, `rick`, `aider`, `goose`, and `cline`. The six older manifests retain
their existing declarations. `oh-my-pi` and `deepseek-harness` remain legacy placeholders
and are not part of the nine-harness reference. Cline's directory and identity are now
`cline`, matching its executable and shorthand; `hbench cline-cli` remains an alias.
Direct `--manifest` users must use `adapters/cline/manifest.toml`.

## Extra adapters (schema 2)

These manifests target Aider **0.86.2**, Goose **1.49.0**, and Cline CLI **3.0.61**.
Version probes require the corresponding release string. Start with
`ahrb doctor --manifest adapters/<name>/manifest.toml`, then a short selection such as
`hbench <name> --profile quick --tests 1-3,9,10,15 --output /absolute/fresh/path`.
Run harnesses serially on a quiet machine. Full quick runs can expose real failures
and resource costs; these adapters do not promise certification or storage coverage.

Every adapter isolates HOME, XDG config/data/state/cache/runtime, and TMPDIR. Generated
files are profile-contained and mode 0600. Only disposable fake-provider credentials
belong in these profiles. Do not copy account files from a developer installation.
Cleanup names only the owned profile; there are no global removal commands. Process
ownership is per invocation and descendants, with executable names as discovery hints.
The AHRB turn/idle deadlines and capture caps are observation bounds, not claims of
harness-native resource enforcement. Retry controls, budgets, tariffs, hooks, native
delegation, complete session-operation CLIs, and permission-denial controls are omitted
unless their complete contract is established; a related flag alone does not prove it.

### Aider

`--message` and `--yes-always` select a single closed-stdin invocation. All main/weak/editor
models are pinned to `openai/ahrb-fake-v1`; `--openai-api-base` points at the fake `/v1`
endpoint and `OPENAI_API_KEY` carries the fake credential. AHRB's fresh disposable HOME
and workspace provide isolation: those roots contain no real dotenv or account config
files. The adapter supplies an empty env file and YAML config. `--env-file` is an
additional dotenv source, loaded after HOME, git-root (when present), and CWD `.env`
files with `override=True`; an empty file cannot undo variables loaded earlier.
Local model metadata plus LiteLLM's
local cost map avoid startup price downloads. Update checks, analytics, URL discovery,
git integration and auto-commits are disabled in the benchmark profile.

Chat and input histories use per-session filenames rooted in profile state. Aider's
`--restore-chat-history` reloads prior user and assistant content. The adapter uses
the AHRB handle in the history filename and declares that public restore command as
resume; it does not claim a native event journal or replay cursor. Aider exposes text output rather than a
native JSONL event journal. No fabricated event rules, narrative pointers, native tool
schemas or typed exit categories are declared. AHRB can observe the process exit, but
that does not establish native structured terminal or tool-workflow compliance.

### Goose

`goose run --text ... --output-format stream-json` uses `GOOSE_PROVIDER=openai` and
`GOOSE_MODEL=ahrb-fake-v1`. Passing only `--provider openai` overrides model selection
with the provider default in this release; the adapter uses the environment instead.
`OPENAI_HOST` and explicit `OPENAI_BASE_PATH=v1/chat/completions` select only the fake
endpoint; `OPENAI_API_KEY` supplies its credential. `GOOSE_PATH_ROOT` contains Goose's
config/data/state trees, including `data/sessions/sessions.db` and its SQLite sidecars.
The system keyring and telemetry are disabled. `--no-profile --with-builtin developer`
loads the bundled developer tools without user extensions. Auto mode avoids approval
prompts. Automatic session naming is disabled and the auxiliary model is also fake.

Goose's `run --resume --name` reopens the same named session and prior conversation
in a fresh process. The adapter supplies the AHRB handle as that name. `session remove
--name` prompts and exits one with closed stdin, so `close_delete` is absent. The
interactive `session --fork` and list/export commands do not establish `session_ops_cli`. SQLite persistence alone does not establish
AHRB's durable event replay contract. Historical roots include the macOS
`~/Library/Application Support/Block/goose` tree as well as Linux XDG locations.

### Cline CLI

`cline --json --auto-approve true <prompt>` selects one-shot execution. The installed
3.0.61 help is authoritative: its bundled README contains older `--yolo`/`-y` examples.
Explicit `--config`, `--data-dir` and `--cwd` place configuration, state and work in
owned roots. A generated `settings/providers.json` selects `openai-compatible`, the
fake model, fake `/v1` endpoint and private fake credential. Its version-1 provider
entry requires `updatedAt`; omitting that field causes the CLI to ignore the file. Keys never appear in the
manifest's argv. `--timeout 25` configures the task timeout; AHRB separately bounds
the complete invocation, including CLI startup.

`--id` rejects JSON mode with a prompt and closed stdin in both tested flag positions.
`history delete --session-id` works with explicit state-root environment bindings, but
the one-shot JSON stream does not expose the persistent session ID needed to bind it
to AHRB handles. Resume, close-delete and `session_ops_cli` therefore remain undeclared.
Session JSON/messages and SQLite indexes are retained under the declared data root.
The optional background hub (`--zen`) is not used. No daemon topology is inferred from
its availability. The npm launcher starts the bundled native Cline executable; both
that `.cline` process and the Node launcher belong to the invocation tree. Automatic
CLI updating is disabled to preserve the installed version.

## Native command shapes and events

`tools.bindings` distinguishes a shell string (`command`), process argv (`command_argv`),
and an array of complete shell scripts (`command_list`). The latter wraps each AHRB
fixture script in a one-element array; interpreting it as argv would run `/bin/sh`,
`-c`, and the script as three independent commands. Cline uses this generic binding
for `run_commands.commands`; Goose uses `shell.command`.

Goose normalizes `message.content` toolRequest/toolResponse/text/error blocks; shell
results use the native `structuredContent` exit code, stdout and stderr. Cline maps
each content_end tool record once, preserving its native result array or protocol-error
object. The generic normalizer unwraps a singleton object array for status/text checks
while retaining the original carrier as native_result; it does not collapse multiple
command results. Command-list scripts put inert hex metadata first so short echoed
previews cannot cut route markers in command arguments. The full command retains the
route annotation, and the fake router still rejects malformed markers.

Goose's rejected/malformed tool calls use a separate `toolResult.status=error`
record without `structuredContent`; that error carrier has its own mapping.
For stdout adapters, AHRB's generic process-exit fallback is scoped to the current
invocation so a cached terminal from an earlier turn cannot suppress the next one.

Goose's `complete` record closes the stream even after failure, so it is not independently
mapped to SUCCESS. The exit contract also checks for native error content. Both Goose
and Aider exit zero after an injected HTTP 401; this remains a benchmark finding.
Aider has no native JSONL events. Cline maps final content_end records and exactly one
run_result terminal; progress updates and the duplicate done notification are ignored.
Cline emits an RFC3339 timestamp; Goose's seconds timestamp does not match schema 2's
supported timestamp units. Neither adapter claims unobserved reasoning or compaction
scope, usage accounting with a controlled tariff, or a complete native exit taxonomy.

Cline's generated provider base URL needs `/v1` for AHRB's fake frontend. Row 65 replaces
a generated-config field with the raw fake origin, without a value-template transform.
That probe limitation does not establish that Cline lacks custom-endpoint support;
the ordinary generated profile and native routing probes exercise that support.

## Storage declarations

The storage pillar ([SPEC-v4](../docs/SPEC-v4-storage.md)) measures the whole disposable
profile with or without a `[storage]` table. Declarations only label files into named
areas and name public verbs; they never exclude bytes. The six storage adapters were
declared from disposable-profile storage runs of the installed binaries; aider, goose and
cline carry no storage declarations (owner scope decision).

| Adapter (version) | Session continuation | Areas | Verbs |
|---|---|---|---|
| codex (0.156.1) | `exec … resume` | `store` rollouts + `state_*`/`thread_history_*` SQLite; `logs` `logs_*.sqlite*`; `transient` `codex/.tmp/**` | `session_delete` = `codex delete --force <UUID>` |
| claude-code (2.1.283) | `--resume` | `store` `claude/projects/**` | none |
| opencode (1.17.20) | `run --session` | `store` `opencode.db*`; `logs` `data/opencode/log/**`; `native_libs` `tmp/.*.dylib` | `session_delete` = `opencode session delete <id>` |
| pi (0.84.4) | `--session` in `--session-dir` | `store` `home/.pi/agent/sessions/**` | none |
| rick (v0.1.18) | none headless (`rick --resume` is the TUI) | none | none |
| haider-agent (0.0.970–0.0.972) | none bound (`run --session` exists from 0.0.971) | none | none |

No adapter documents a close-without-delete verb, a retention or auxiliary cap, or a
sweep interval, so S5 and S7's cap assessment stay `UNSUPPORTED`. No adapter has a
disposable-profile uninstall: `opencode uninstall` removes the real binary on package or
script installations, `rick uninstall` removes Rick itself, `pi uninstall` removes an
extension source, and codex, claude-code and haider have none. `claude rm` only deletes
background (`--bg`) sessions by short id, and `claude project purge` removes a whole
project's state.

The codex and opencode delete commands take their store only from the environment, not
from an argument. SPEC-v4 §3 allows that scope when the isolated environment is honoured:
AHRB runs every verb with a cleared environment containing only the profile-contained
isolation roots, records `scope_basis="isolated-environment"` in the S6 command receipt,
and turns the operation into `ERROR` unless the verb visibly changed a file in the
declared `sessions.store_paths`. Uninstall and close verbs still need a `{{profile}}` or
`{{workspace}}` argument. For both adapters the declared store includes the SQLite files
that hold session data, so a delete that leaves those files behind is S6 residue. Both
commands receive the harness-native session id learned from the adapter's `id_pointer`, the
same value continuation commands receive, not AHRB's internal session handle.

Codex runs with `--disable shell_snapshot -c allow_login_shell=false` (both documented in
0.156.1). Otherwise codex starts host login shells (`/etc/zprofile`, `path_helper`,
snapshot helpers) and reaps them before its terminal; macOS keeps no disk counters for a
descendant reaped by the harness, so S1/S10 could only report `ERROR`. Without the
protected bare launcher, Claude Code also spawns and reaps helpers every turn (`git`,
`security`, shells). The documented bare mode is intended to remove the forbidden
`security` lookup; the launcher below enforces that promise for 2.1.283. Any remaining
reaped helpers can still make S1/S10 physical counters `ERROR` on macOS by the same rule.

Claude Code 2.1.283 must run with its documented `--bare` option on every AHRB launch,
including the version probe, initial transport and resume command. A disposable `HOME`
does not isolate the macOS login keychain; without `--bare`, Claude Code probes the
owner's keychain before using AHRB's fake `ANTHROPIC_API_KEY`. The installed help states
that bare mode never reads OAuth or keychain authentication and accepts Anthropic auth
only from `ANTHROPIC_API_KEY` or an explicit `apiKeyHelper` supplied by `--settings`.
The initial and resume environments also pre-set the documented bare-mode variable
`CLAUDE_CODE_SIMPLE=1`: in 2.1.283, relying on the flag to set it after process startup
still allowed an early keychain prefetch before argument handling. Keeping both the flag
and its documented environment effect active from process creation closes that startup
window in versions that honor the documented behavior. Version 2.1.283 did not: a sampled
run still observed the legacy prefetch. AHRB therefore generates an owner-private launcher
inside each disposable profile. On macOS it applies a narrow `sandbox-exec` rule that
allows all normal behavior but denies execution of `/usr/bin/security`; on other platforms
it directly launches the documented bare command. The macOS branch fails closed with a
nonzero exit and a clear diagnostic if `/usr/bin/sandbox-exec` is unavailable or not
executable; it never falls through to an unsandboxed Claude launch. This blocks the
defective macOS prefetch before a child exists or any keychain request can occur without
making the adapter macOS-only; sampler evidence remains the authority.

Bare mode also skips settings/plugin hooks, LSP, plugin sync, attribution, auto-memory,
background prefetches and automatic `CLAUDE.md` discovery. Of the 73 matrix rows, only
row 39 directly measures one of those features (hooks); Claude Code's manifest declares
no hook, so it remains `UNSUPPORTED`. The matrix has no direct plugin, LSP, attribution,
auto-memory, prefetch, `CLAUDE.md`, or MCP-discovery row. Skills are not disabled by bare
mode, and explicit context remains available through flags such as `--mcp-config`,
`--settings`, `--agents` and `--plugin-dir`; this manifest supplies none of them. Removing
implicit customization can still alter request bytes, helper processes, timing and disk
observations, so candidate-versus-baseline runs must report any resulting class changes
rather than assuming equivalence.

Rick and Haider stay `ABSENT` in storage. Rick's `exec` has no headless resume. Haider's
shared manifest binds no continuation: `haider run --session ID` is documented from 0.0.971,
0.0.970's `run --help` does not list it, and binding it changes every multi-turn matrix row. Even with that
binding, Haider hard-links `daemon.log` to its active `daemon-logs/haiderd-*.log`
generation, and a repeated hard-link identity is a storage capture `ERROR` (SPEC-v4 §2).
Haider's `resources.journal_paths` declares `store.sqlite` and `store.sqlite-wal` for
row-47 journal attribution.

## Evidence and limits

Implementation and separate Astra verification evidence live outside Git in the lane's
assigned state/results directories. Preserve FAIL rows and exact infrastructure blockers.
Do not turn unobserved behavior into capability declarations or cite a doctor pass as SHIP.
The version-pinned Goose source describes [provider URL resolution](https://github.com/aaif-goose/goose/blob/v1.49.0/crates/goose/src/providers/openai_def.rs)
and [isolated paths](https://github.com/aaif-goose/goose/blob/v1.49.0/crates/goose/src/config/paths.rs).
Installed CLI help, Aider's installed Python source, and Cline's generated disposable
provider file are the local configuration references for this lane.

Haider's `0.0.970-measurement-capability-audit-v3` declaration was checked against the pinned
0.0.970 executable (SHA-256 `dd40b86e7334ed87dae04d14b032dc5912f83f050980e0fbb97594e6b76c503b`,
dev/debug preflight build). `run --help` lists JSONL, timeout, start/status/stop/replay
and describes allow-writes/allow-exec as compatibility aliases. It does not document
ordinary session continuation. No continuation or close binding is inferred.
Disposable-profile observations establish `home/.haider/dev-profile/daemon.log`
and `daemon-logs/haiderd-<pid>-<timestamp>-0.log`. The active generation and
`daemon.log` share device/inode, so the manifest declares the stable alias once.
Row 47 accepts file paths, not globs; archived-generation retention is not declared
bounded. Observed native
result previews have `truncated=false` and BLAKE3 transcript digests, without a
verified row-60 marker containing original bytes, retained payload bytes, and the
full-stream SHA-256. Row 60 therefore remains ABSENT pending an honest marker declaration.
