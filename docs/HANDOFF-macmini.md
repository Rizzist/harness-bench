# AHRB handoff — running the reference benchmark on a dedicated Mac mini

Purpose: produce the first CLEAN reference table of the Agent Harness Readiness Benchmark on a quiet, dedicated machine — six harnesses × the 73-row matrix, plus the economy and fidelity pillars — and keep that machine as the place future runs (including the planned storage pillar) happen. Every number measured on the development laptop so far was taken under load and is indicative only.

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
5. Pre-flight per adapter: `./target/debug/ahrb doctor --manifest adapters/<harness>/manifest.toml` must be clean for all six.
6. Self-certify the build on the built-in mocks before any real harness: `scripts/mock-cert.sh` — daemon mock all PASS except the spec-sanctioned UNSUP on row 68; mock-exec row 68 PASS, zero FAIL/ERROR, badge emitted. If this is not green, the machine or the build is wrong — stop.

## 2 · The run plan (serial, quiet; load < 2 before each harness)
1. **Matrix (functional + resource, 73 rows, quick profile):** `AHRB_OUT_ROOT=$HOME/ahrb-results scripts/six-harness.sh` — order claude-code · opencode · pi · rick · haider-agent · codex (codex last: it is the slow per-invocation long pole; deadlines 3600 s codex / 1800 s opencode / 1500 s others). The script reclaims each harness's temp under `/private/tmp/ahrb-*` after its run and stops if free disk drops below 10 GB.
2. **Economy pillar:** `./target/debug/hbench economy <harness> --profile quick` for each harness (absolute `--output` under `$HOME/ahrb-results/economy/<harness>`).
3. **Fidelity pillar:** `./target/debug/hbench fidelity <harness> --profile quick` likewise.
4. **haider full-72 on 0.0.970** when it ships (the 969 table is known: 36 PASS; rows 60/61 need the 970 truncation marker and effects list).
5. Save everything: `results/` (the saved-results store + `results/index.jsonl`, gitignored) and `$HOME/ahrb-results/` are the deliverable — archive them off the machine when done. `hbench diff` compares runs from the index.

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
- [ ] Machine quiet, awake, ≥ 40 GB free
- [ ] `cargo build --locked` · `list-tests` = 73
- [ ] six binaries on PATH, versions recorded
- [ ] `ahrb doctor` clean ×6
- [ ] `scripts/mock-cert.sh` green
- [ ] `scripts/six-harness.sh` — six reports, `RESULT` lines in `six-harness.log`
- [ ] economy ×6 · fidelity ×6
- [ ] `results/` + `$HOME/ahrb-results/` archived; table assembled with `hbench diff` / `hbench results`
