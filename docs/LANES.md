# AHRB lanes — Mac mini storage-pillar run (started 2026-09-06)

Orchestrator: Fable 5.1 (`claude-fable-5-1`). Workers: GPT6-Astra (`gpt-6-astra`) through the
harness's pinned `astra-task` wrapper (Codex CLI). UI work, if any, is Fable 5.1 or Opus 5.
Every lane follows **clean code pass -> implement -> Astra code verification AND Astra
computer-use verification -> repair until SHIP -> complete (commit, reconcile, push)**.

Computer-use for this CLI project means the verifier actually runs the affected user-facing
commands (`ahrb`, `hbench`, `scripts/*.sh`) and inspects the real terminal output and
produced artifacts; unit tests, a build, or a predicted result do not count. Any browser or
graphical behavior additionally needs actual interaction and visual inspection.

Raw worker transcripts, benchmark profiles and evidence live outside Git under the harness
`state/lanes/<lane>/` and `results/` directories. This ledger records only sanitized
summaries: candidate commits/digests, commands, exit codes, verdicts and worker session IDs.

## Conventions

- Lane worktree: `worktrees/<lane-id>` on branch `lane/<lane-id>`, based on the latest
  reconciled `master`.
- Implementation and verification always use distinct worker contexts (separate wrapper
  invocations and evidence directories).
- SHIP binds to the exact candidate: `git rev-parse HEAD` of the worktree plus the SHA-256 of
  `git diff HEAD` (empty diff when committed), plus hashes of any untracked candidate files
  (which Git diff omits). Any later source change returns the lane to
  verification.
- Heavy builds, test suites, benchmark runs, VMs and emulators are serialized on this 16 GiB
  machine. Reference measurements run alone on a quiet machine.
- Per-stage boxes: `[ ]` pending, `[x]` done with evidence, `[!]` blocked (reason recorded).

## Lane list and dependencies

| Lane | Scope | Depends on | Status |
|---|---|---|---|
| L0 `l0-setup-runners` | Land the portable instruction files (`AGENTS.md`, `CLAUDE.md`, `docs/DEVELOPMENT.md`, this ledger); fix `scripts/mock-cert.sh` and `scripts/six-harness.sh`: failure/exit-code propagation, fresh timestamped output paths, ownership-scoped temp cleanup, preservation of earlier evidence; regression coverage for those; baseline build/test; honest mock self-certification | — | **complete** (SHIP, landed `1c286b5`) |
| L1 `l1-storage-spec` | Normative `docs/SPEC-v4-storage.md` for S1–S10 (S4 included): operational definitions, units, accounting rules, manifest `[storage]` contract, unsupported/ABSENT/ERROR cases, report/CLI behavior, evidence bundle, badge classes, six-adapter declaration requirements | L0 | in verification (iteration 3) |
| L2 `l2-storage-core-s1-s3` | Storage pillar plumbing: `ahrb run --pillar storage`, `hbench storage <harness>`, manifest `[storage]` parsing/validation, run-root allocated-block accounting with settle/sync, standardized 100-turn storage driver, S1 write volume + amplification, S3 footprint curve/shape; `storage_summary` in `report.json`/`report.md`; mock positive/negative evidence and tests | L1 | pending |
| L3 `l3-storage-s7-s8` | S7 bounded auxiliaries per declared file family; S8 request-body retention classification and ratio from fake-model request bytes versus run-root growth; mock evidence for none/deduplicated/full | L2 | pending |
| L4 `l4-storage-s5-s4` | S5 close retention after N create/close cycles and after the declared sweep interval; S4 compaction-versus-disk around the row-51 context-limit trigger; mock evidence | L2 | pending |
| L5 `l5-storage-s2-durability` | S2 fsync/fdatasync/F_FULLFSYNC counting per turn with honest OS limits (macOS interpose shim where the binary permits, Linux tracing where available, otherwise `UNSUPPORTED: os-limited` with evidence); estimated durability wall labelled as an estimate | L2 | pending |
| L6 `l6-storage-s6-s9-s10` | S6 declared `session_delete`/`uninstall_cleanup` residue on disposable benchmark profiles only; S9 crash residue after the row-57 kill; S10 resume read cost around the row-52 resume; mock evidence | L2, L4 | pending |
| L7 `l7-storage-adapters-docs` | `[storage]` declarations for the six bundled adapters from journal/disk evidence, README/adapters docs, `hbench diff`/`results` index integration for storage fields, mock full-matrix and CLI regression | L3, L4, L5, L6 | pending |
| L9 `l9-extra-adapters` | Owner-added 2026-09-06: adapter manifests, doctor and mock-free CLI verification for aider 0.86.2, goose 1.49.0 and cline 3.0.61 (installed pinned in the harness tools directory) so the v4 reference covers nine harnesses; runs in parallel with L2, storage blocks added by L7 | L0 | in progress |
| L8 `l8-references-macmini` | Quiet-machine references on the mini: mock self-certification, nine-harness matrix, economy, fidelity and storage per harness; sanitized `docs/REFERENCE-macmini-2026-09.md`; raw evidence archived outside Git | L7, L9 | pending |

Wave mapping to the proposal: W1 = L2 + L3 + L4 (S1, S3, S5, S7, S8, plus S4 which the
proposal's wave list omitted); W2 = L5 + L6 (S2, S6, S9, S10).

## L0 `l0-setup-runners`

Acceptance criteria:
1. `AGENTS.md`, `CLAUDE.md`, `docs/DEVELOPMENT.md`, `docs/LANES.md` reviewed and landed.
2. `scripts/mock-cert.sh` exits nonzero when any `ahrb` invocation fails, when a report is
   missing, or when the expected classification is not met (daemon mock: all PASS except
   the sanctioned UNSUPPORTED on row 68; mock-exec: row 68 PASS, zero FAIL/ERROR, badge).
3. `scripts/six-harness.sh` propagates per-harness failures into its final exit code and
   summary, never reuses or deletes an earlier output directory (fresh timestamped run
   directory under `AHRB_OUT_ROOT`), and removes only temp/profile directories owned by
   the run it just finished (never a blanket `/tmp/ahrb-*` sweep).
4. Focused regression coverage for failure propagation and ownership-scoped cleanup.
5. Baseline: `cargo build --locked`, `ahrb list-tests` = 73 rows, existing tests pass or
   pre-existing failures are recorded, and the fixed mock-cert script passes with an honest
   exit code on this machine.

- [x] Worktree created: `worktrees/l0-setup-runners`, branch `lane/l0-setup-runners`, base `9d9b5c2`
- [x] Clean code pass + implement (GPT6-Astra, thread `01a0776d-e1fa-71f1-8ba6-8fdbc99eed34`, exit 0; baseline 454 tests pass, 7 new runner-script tests pass, real `scripts/mock-cert.sh` -> `MOCK_CERT PASS`, exit 0, 1140 s)
- [x] Code verification (GPT6-Astra, separate context; thread `01a07795-e230-7f02-9988-c278a7d15381`: full diff review, `cargo build --locked`, `list-tests` = 73, `cargo test --locked` 454 passed in 977 s, `zsh -n`)
- [x] Computer-use verification (same verifier: real `scripts/mock-cert.sh` run -> `MOCK_CERT PASS` exit 0 with report inspection, stub-driven failure provocation of both scripts -> nonzero exits with named failures, fresh run directories, unrelated `ahrb-*` directory and earlier run directory preserved)
- [x] Repair loop until SHIP (iteration 1: NO-SHIP on one P2 ledger inconsistency, fixed; iteration 2 thread `01a077bb-dbcb-7390-868c-eb221e89692a`: hash-bound re-verification plus re-run fast checks -> **SHIP**)
- [x] Complete: focused commit `1c286b5`, reconciled with `origin/master`, pushed

Record: implementation thread `01a0776d-e1fa-71f1-8ba6-8fdbc99eed34` (gpt-6-astra); verified source tree = base `9d9b5c2` + diff sha256 `bb041f7e03f4c073977da7a8fe3808312835c4919a5b57077b74efc679602e36` + new files (`tests/runner_scripts.rs` `3975b8ea…`); landing commit `1c286b5`; this ledger update is a docs-only status commit outside the verified candidate per `docs/DEVELOPMENT.md`. Raw evidence: harness `state/lanes/l0-setup-runners/{impl-1,verify-1,verify-2}` and `results/mock-cert/20260906T161422Z-0952` (outside Git).

## L1 `l1-storage-spec`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L2 `l2-storage-core-s1-s3`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L3 `l3-storage-s7-s8`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L4 `l4-storage-s5-s4`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L5 `l5-storage-s2-durability`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L6 `l6-storage-s6-s9-s10`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L7 `l7-storage-adapters-docs`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra)
- [ ] Repair loop until SHIP
- [ ] Complete

## L9 `l9-extra-adapters`

- [ ] Worktree
- [ ] Clean code pass + implement (GPT6-Astra)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra: real doctor/run flows for each new adapter)
- [ ] Repair loop until SHIP
- [ ] Complete

## L8 `l8-references-macmini`

- [ ] Worktree
- [ ] Pre-flight: all nine harness binaries, versions and hashes recorded, `ahrb doctor` clean for all nine adapters, mock self-certification (quiet machine)
- [ ] Matrix, economy, fidelity, storage for all nine harnesses (serial, quiet, fresh timestamped roots)
- [ ] Sanitized reference tables written (GPT6-Astra implementation context)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra: inspects the real run artifacts and rebuilt tables)
- [ ] Repair loop until SHIP
- [ ] Complete
