# AHRB handoff — running the reference benchmark on a dedicated Mac mini

Purpose: produce the first CLEAN reference table of the Agent Harness Readiness Benchmark on a quiet, dedicated machine — six harnesses × four pillars: the 73-row matrix, economy, fidelity and storage — and preserve the evidence for future runs. Every number measured on the development laptop so far was taken under load and is indicative only.

## 1 · Machine setup
1. macOS (Apple silicon), ≥ 16 GB RAM, ≥ 40 GB free disk. Nothing else scheduled on the box during runs: no browser, no IDE indexing, no other agents. Keep the machine awake (`caffeinate -dims` in a tmux/screen session).
2. Tooling: Xcode CLT, Homebrew, stable Rust (measured here with rustc/cargo 1.95), Python 3, Node 22+ (for the npm-distributed harnesses), `gh` optional.
3. Clone and build:
   ```console
   git clone https://github.com/Rizzist/harness-bench.git && cd harness-bench
   cargo build --locked
   ./target/debug/ahrb list-tests          # expect 73 rows
   ```
4. Install the six harnesses on PATH. Versions measured on the development machine (pin these, or record what you install):

   | harness | binary | version here | install |
   |---|---|---|---|
   | codex | `codex` | codex-cli 0.153.4 | `npm i -g @openai/codex` |
   | Claude Code | `claude` | 2.1.263 | `npm i -g @anthropic-ai/claude-code` |
   | opencode | `opencode` | 1.17.20 | per its README (npm/brew) |
   | pi | `pi` | 0.84.4 | per its README |
   | rick | `rick` | v0.1.18 | from its releases |
   | haider | `haider` | 0.0.969 | `curl -fsSL https://haidercode.ai/install \| bash` — use 0.0.970+ when available (rows 60/61 and request-kind headers need it) |

   No real provider keys are needed: the benchmark's fake model is the endpoint (the manifests inject the base URL). Prefer a profile with NO real keys configured so nothing can leak out.
5. The reference command in §2 runs `ahrb doctor` for all six adapters; every doctor must be clean.
6. That command also runs `scripts/mock-cert.sh` before any real harness: daemon mock all PASS except the spec-sanctioned UNSUP on row 68; mock-exec row 68 PASS, zero FAIL/ERROR, badge emitted. A failure stops preflight. These checks are part of the one-command procedure; separate manual runs are optional diagnostics.

## 2 · One-command reference (serial, quiet)

Build the candidate first, then reserve the quiet machine using the owner's machine-sharing
gate. Keep it awake for the entire run. On the owner's mini, source the external `env.sh`
before commands and use the assigned external evidence directory for the gate log.

```sh
source /Users/rizzist/Developer/ahrbharness/env.sh
EVIDENCE_DIR=/Users/rizzist/Developer/ahrbharness/state/lanes/l8-references-macmini
mkdir -p "$EVIDENCE_DIR"
AHRB_GATE_LOG="$EVIDENCE_DIR/gate.log" ahrb-gate wait-exclusive l8-reference || exit $?
AHRB_OUT_ROOT="$HOME/ahrb-results" scripts/reference-run.sh
# Copy the absolute RUN_DIR printed by the command:
scripts/reference-tables.py "$RUN_DIR" > "$RUN_DIR/reference-tables.md"
```

The fresh timestamped directory under `AHRB_OUT_ROOT/reference/` contains the immutable
`provenance.json`, append-only `reference-run.log`, load samples, each invocation's preflight
logs, and `<harness>/<pillar>/` bundles with command logs and exit receipts. Controller
logs are staged beside each output until the CLI returns, preserving storage’s requirement
that its output directory not exist at launch. All four quick
pillars run serially for claude-code · opencode · pi · rick · haider-agent · codex (codex last).
Matrix/economy/fidelity budgets are 3,600 s for codex, 1,800 s for opencode and 1,500 s for
others. Storage delegates to the normative quick budget **10,509 + 3W seconds**, where W is
the declared sweep interval; `AHRB_DEADLINE` is cleared to preserve that budget. No real model
credentials are needed. Haider gets `HAIDER_RUN_DAEMON_IDLE_TTL_MS=0` for owned-tree accounting.

Preflight records executable versions and SHA-256 (including haiderd), AHRB binary/source
identity, manifest hashes and OS; runs every selected doctor; and runs `scripts/mock-cert.sh`.
A doctor, inventory or certification failure stops before the reference steps. The default
load guard requires one-minute load <2, polls every 30 s and stops after 900 s. The default
disk guard requires 40,000 MiB free. Load guards run before certification and each harness; disk is checked again before each pillar.
Overrides are `AHRB_MAX_LOAD`, `AHRB_LOAD_WAIT_SECONDS`, `AHRB_LOAD_POLL_SECONDS` and
`AHRB_MIN_FREE_MB`; load samples retain the actual thresholds. For an explicitly justified
certification skip, set both `AHRB_SKIP_MOCK_CERT=1` and `AHRB_SKIP_MOCK_CERT_REASON`; the
reason is logged. Such a skip is not a certification PASS.

Resume after an interruption using the same built candidate and environment:

```sh
scripts/reference-run.sh --resume "$RUN_DIR"
```

Resume rechecks preflight and binary/source/manifest/OS identity and inherits the original
harness selection unless explicitly overridden. Each `step-provenance.json` binds its
step, pillar, harness, quick profile, manifest hash, candidate revision, full provenance
hash and report hash. A matching report fingerprint and receipt are required to skip;
the receipt keeps the raw manifest-file hash separate from the canonical manifest hash
returned by preflight's AHRB doctor and binds both to the fresh invocation.
The append-only log records each adopted report hash and the matching identity. Unbound,
changed or misplaced reports are re-run, with the old bundle archived under `attempt-*`.
If original provenance is missing, every existing step is re-run. Changed run-level
provenance is refused: start a fresh run for a different candidate. Exit/cleanup failures
from adopted steps remain failures; a report alone is not proof of success.
A lock prevents two writers. After a hard crash, inspect the lock's PID and confirm the old
run and its children are gone before manually removing `.reference-lock`.

Each step records `RESULT` exit codes and assessment reasons. Final `REFERENCE PASS`
(exit 0) requires every scheduled report, complete current typed summaries, task/profile
scope, all required rows and claimed measurements. `REFERENCE PARTIAL` (exit 1) includes
incomplete reports/typed fields, non-PASS row findings, or an unfinished scripted task.
Nullable/inapplicable measurements stay null; missing required fields never certify PASS.
`REFERENCE FAIL` (exit 2) means preflight, command infrastructure, deadline, receipt or
cleanup failure. **Any disk/load guard stop is FAIL**, even after some reports completed:
the message names the guard, measured disk/floor where applicable, and unrun remaining
steps. It does not mean the completed benchmark observations failed. Completed benchmark
FAIL/ERROR findings with their normal CLI exit 1 remain PARTIAL. All PARTIAL/FAIL outcomes
are nonzero for automation. The daemon mock's sanctioned row-68 UNSUPPORTED therefore
makes a mock reference PARTIAL.

Cleanup runs only after a step exits and only for report-owned, freshly created direct
`ahrb-*` temporary children and that step's local `profile-*` directories. Older roots,
symlinks and unrelated runs are preserved. `AHRB_KEEP_PROFILES=1` keeps disposable profiles
for diagnosis. No cleanup runs when skipping a report. Raw evidence remains outside Git.
Abnormal command exits retain profiles because process shutdown may be incomplete.

For runner development only, `AHRB_REFERENCE_HARNESSES="mock mock-exec"` selects the two
built-in mocks. The default is exactly the six original harnesses; aider/goose/cline are
outside this reference scope. Do not present mock measurements as real harness references.

The table generator uses Python 3.9+ stdlib and emits sanitized Markdown to stdout, with
explicit missing/null/ERROR/UNSUPPORTED values, matrix class counts/resources, economy,
fidelity, storage S1–S10 outcomes and D/G classes, and provenance. It reads selected report
fields rather than profiles or raw request bodies. It emits descriptive tables with scope
and measurement pins; compare only matching OS/topology/profile/task/pins and storage
declaration hashes, never cross-topology rankings. Review tables before publishing.
Archive the entire run directory and the saved `results/` store off the machine. The runner
never deletes earlier reference evidence; `hbench diff` remains available for indexed runs.

## 3 · Reading a report
- Classes: PASS · FAIL · ERROR · UNSUPPORTED (declared) · ABSENT (capability not declared). The badge is the top-level `badge` field of `report.json` and is withheld when any core row fails.
- Universal FAILs on one-shot CLIs are real findings, not bugs: row 10 (terminal-failure) and row 12 (idle-deadline — one-shot CLIs don't self-abort).
- Timing rows are load-sensitive: 47/48 (disk-io, model-wait CPU), 49 (latency slope), 54 (fanout cliff), 58 (retry budget), 59 (slow-stream vs stall). A FAIL there is believed only after a re-run at load < 2. "spawned process PID disappeared before ownership registration" ERRORs appear at load > 7 — load, not the harness; re-run.
- Per-invocation harnesses (codex, opencode) are deadline-limited on the full 72-row v2 suite below ~3600 s — that is a real automation-cost finding; report it as such.

## 4 · Traps (each cost a day on the laptop)
- ALWAYS pass an absolute `--output`; a relative path makes the env roots unresolvable and the harness dies with "did not terminalize".
- Never delete `/private/tmp/ahrb-*` while ANY run is live — it is the running harness's workspace. The script cleans only after a harness finishes.
- haider: `HAIDER_RUN_DAEMON_IDLE_TTL_MS=0` for one-shot whole-tree resource accounting; the manifest default TTL for functional/replay/latency rows. The script sets this.
- codex's tool-call workspace grows to ~3.5 GB per run under `/private/tmp/ahrb-<id>`; that is why per-harness reclaim exists.
- Socket harnesses need short profile paths (row-42 sun_path fix is in; keep `--output` roots short anyway).
- Do not run two benchmark processes at once; do not run `cargo test --all-targets` on the box while a harness run is live (the heavy matrix tests are memory-hungry).

## 5 · Numbers from the development laptop (under load — indicative only, NOT the reference)
72-row quick, PASS counts: claude-code 38 · pi 35 · rick 33 · haider 0.0.969 36 (full 72, reproducible) · codex / opencode deadline-limited at 1500 s. Economy (same scripted task, reference tokens): pi 240 k · rick 304 k (over budget) · haider 388 k · opencode 440 k · codex 523 k · claude-code 688 k; per-turn fixed overhead is the driver (claude-code 55 k tokens/turn vs pi 2.4 k). The mini's clean run replaces all of these.

## 6 · What comes next on this machine
- The **storage pillar** (v4): proposal in `docs/PROPOSAL-v4-storage.md` — write volume and amplification, footprint curve over 100 turns, compaction vs disk, close retention, delete/uninstall residue, request-body retention, fsync cost. Spec first, then per-row implementation; the mini is where its first six-harness table is measured.
- A harness-facing contract so new harnesses are a manifest preset plus a few overrides (parked; see the adapters README for today's declarative manifests).

## 7 · Checklist

- [ ] Machine quiet, awake, ≥40,000 MiB free; exclusive machine gate cleared
- [ ] `cargo build --locked` · `list-tests` = 73
- [ ] Six binaries plus haiderd inventoried with versions and SHA-256; optimized Haider pair
- [ ] `scripts/reference-run.sh`: doctors clean ×6 and mock self-certification green
- [ ] Matrix ×6 · economy ×6 · fidelity ×6 · storage ×6 in fresh absolute run directory
- [ ] Review all `RESULT` lines and final PASS/PARTIAL/FAIL; label incomplete/non-PASS findings
- [ ] `scripts/reference-tables.py "$RUN_DIR"` generated and sanitized tables reviewed
- [ ] Full run directory plus saved `results/` archived outside Git; independent Astra verification
