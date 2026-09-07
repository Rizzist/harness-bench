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


## Status 2026-09-07 22:10 UTC — blocked again on GPT6-Astra capacity

Capacity was restored at 17:16 UTC and all six lanes advanced (L3 repaired and re-verifying, L4 repaired, L5 verifying with one confirmed persistence finding, L6 repaired twice and re-verifying, L7a's codex collector fix proven on real codex/pi/claude-code runs, L8a repaired after three findings) until the usage limit was exhausted again at 22:06 UTC ("try again at Sep 14th, 2026"). The owner's standing rule is to wait for GPT6-Astra rather than substitute any other model. The harness checklist's BLOCKER #2 section lists exact per-lane resume steps.

## Status 2026-09-07 16:50 UTC — blocked on GPT6-Astra capacity (superseded)

OpenAI's Codex usage limit was exhausted mid-run ("try again at Sep 14th, 2026"), ending every active
GPT6-Astra worker. Per the owner's rules no capacity was purchased, no model was substituted and no
SHIP was claimed. Landed and pushed: L0, L1, L9, L2. State of the unfinished lanes, each with its
worktree, candidate and evidence preserved in the harness (outside Git):

| Lane | State when blocked |
|---|---|
| L3 `l3-storage-s7-s8` | Implemented (494 tests, 10 mock bundles; thread `01a079c9-5902-71c1-bd84-102595d9d5f4`), rebased onto `6f4f2e5` as `9f198a5`; verify-1 (`01a07b98-08ef-7451-bbd8-9791f84bece0`) NO-SHIP: F1 plaintext feasibility not propagated to S7/S8; F2 whole-crate suite failed on an unrelated one-ULP float assertion in `tests/mock_full_matrix.rs` |
| L4 `l4-storage-s5-s4` | Implemented (500 tests, 4 mock bundles; `01a079c9-5971-75b3-9f12-cccac6742516`), rebased as `de78fa2`; verify-1 (`01a07c1e-b0c6-7d81-a00b-7af0ce721348`) found the saved-results copy of the daemon bundle missing its S4 context receipts before the session ended |
| L5 `l5-storage-s2-durability` | Implementation (`01a079c9-59d0-7392-aaa1-d663663fae64`) in its final full-suite run after a focused regression repair; S1/S2/S3 pass in its mock bundles; not yet rebased or verified |
| L6 `l6-storage-s6-s9-s10` | Implemented (502 tests; `01a079c9-59c9-7320-801c-258cd4f0d489`), rebased as `b22e569`; integration repair applied (`01a07c22-239d-7593-a8f0-0c03b5eaac11`); verify-2 (`01a07c71-9fc6-75f3-8068-6af535b76c8b`) interim NO-SHIP with stages pending |
| L7a `l7a-storage-collector-results` (split from L7) | Codex terminal-before-reap collector fix implemented with a new immediate-exit mock fixture passing 300 turns (`01a07be6-8d64-79a3-95f4-ae5c8900933d`); real-adapter runs and report pending |
| L8a `l8a-reference-runner` (split from L8) | `scripts/reference-run.sh`, `scripts/reference-tables.py`, tests and handoff docs written (`01a07be6-8d76-7852-a69b-fa8a318baa4b`); mock reference exercise pending |
| L7b, L8 | Not started |

Resume procedure: the harness checklist's BLOCKER section lists the per-lane resume steps.

## Lane list and dependencies

| Lane | Scope | Depends on | Status |
|---|---|---|---|
| L0 `l0-setup-runners` | Land the portable instruction files (`AGENTS.md`, `CLAUDE.md`, `docs/DEVELOPMENT.md`, this ledger); fix `scripts/mock-cert.sh` and `scripts/six-harness.sh`: failure/exit-code propagation, fresh timestamped output paths, ownership-scoped temp cleanup, preservation of earlier evidence; regression coverage for those; baseline build/test; honest mock self-certification | — | **complete** (SHIP, landed `1c286b5`) |
| L1 `l1-storage-spec` | Normative `docs/SPEC-v4-storage.md` for S1–S10 (S4 included): operational definitions, units, accounting rules, manifest `[storage]` contract, unsupported/ABSENT/ERROR cases, report/CLI behavior, evidence bundle, badge classes, six-adapter declaration requirements | L0 | **complete** (SHIP, landed `bec2475`) |
| L2 `l2-storage-core-s1-s3` | Storage pillar plumbing: `ahrb run --pillar storage`, `hbench storage <harness>`, manifest `[storage]` parsing/validation, run-root allocated-block accounting with settle/sync, standardized 100-turn storage driver, S1 write volume + amplification, S3 footprint curve/shape; `storage_summary` in `report.json`/`report.md`; mock positive/negative evidence and tests | L1 | **complete** (SHIP, landed `e24aeba`) |
| L3 `l3-storage-s7-s8` | S7 bounded auxiliaries per declared file family; S8 request-body retention classification and ratio from fake-model request bytes versus run-root growth; mock evidence for none/deduplicated/full | L2 | pending |
| L4 `l4-storage-s5-s4` | S5 close retention after N create/close cycles and after the declared sweep interval; S4 compaction-versus-disk around the row-51 context-limit trigger; mock evidence | L2 | pending |
| L5 `l5-storage-s2-durability` | S2 fsync/fdatasync/F_FULLFSYNC counting per turn with honest OS limits (macOS interpose shim where the binary permits, Linux tracing where available, otherwise `UNSUPPORTED: os-limited` with evidence); estimated durability wall labelled as an estimate | L2 | pending |
| L6 `l6-storage-s6-s9-s10` | S6 declared `session_delete`/`uninstall_cleanup` residue on disposable benchmark profiles only; S9 crash residue after the row-57 kill; S10 resume read cost around the row-52 resume; mock evidence | L2, L4 | pending |
| L7a `l7a-storage-collector-results` | Split out 2026-09-07: codex terminal-before-reap collector fix, codex volatile-area rule, pi sessions revisit, saved-index/`hbench diff` storage integration | L2 | in progress (blocked) |
| L8a `l8a-reference-runner` | Split out 2026-09-07: `scripts/reference-run.sh` (six harnesses x four pillars, pre-flight, resumable, honest exit), `scripts/reference-tables.py`, tests, handoff docs; verified on mocks | L2 | in progress (blocked) |
| L7 `l7-storage-adapters-docs` | `[storage]` declarations for the six original adapters (codex, claude-code, opencode, pi, rick, haider-agent) from journal/disk evidence, README/adapters docs, `hbench diff`/`results` index integration for storage fields, mock full-matrix and CLI regression. Owner decision 2026-09-07: aider/goose/cline stay as landed adapters without storage declarations | L3, L4, L5, L6 | pending |
| L9 `l9-extra-adapters` | Owner-added 2026-09-06: adapter manifests, doctor and mock-free CLI verification for aider 0.86.2, goose 1.49.0 and cline 3.0.61 (installed pinned in the harness tools directory) so the v4 reference covers nine harnesses; storage blocks added by L7 | L0 | **complete** (SHIP, landed `c33237e`) |
| L8 `l8-references-macmini` | Quiet-machine references on the mini: mock self-certification, then matrix, economy, fidelity and storage for the six original harnesses (owner decision 2026-09-07); sanitized `docs/REFERENCE-macmini-2026-09.md`; raw evidence archived outside Git | L7 | pending |

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

- [x] Worktree `worktrees/l1-storage-spec`, branch `lane/l1-storage-spec`, base `9d9b5c2`, rebased onto `b7405ff`
- [x] Clean code pass + implement (GPT6-Astra; threads `01a0776f-7c52-7963-9079-73a1e87b3cd7`, repair `01a07798-379f-7f52-b206-f9baf85da335`, repair `01a077ba-b81b-7b22-bc2c-a44f0b933169`); documentation-only lane, no build/test claimed
- [x] Code verification (GPT6-Astra; three independent iterations: `01a07789-fcf3-78b3-b646-fa1bf6b7c563` NO-SHIP F1 S6 aggregation/exit contradiction, F2 no feasible storage deadline budget, F3 exec-resume claim; `01a0779f-f8b2-7560-a58b-2b668bb93b37` NO-SHIP F4 S10 timing branches for daemon+exec+control-resume; `01a077bf-8c82-7fb1-8515-ad3d82281280` **SHIP**): full citation audit against runner/driver/manifest code, contract arithmetic recomputed, links and rendering checked
- [x] Computer-use verification (same verifiers: real `ahrb list-tests`, `ahrb doctor`, the not-yet-implemented `ahrb run --pillar storage` error text, and a real `hbench economy mock --profile quick` bundle inspected for the report/index conventions the spec mirrors)
- [x] Repair loop until SHIP (three iterations)
- [x] Complete: commit `bec2475`, fast-forwarded onto `master`, pushed

Record: verified spec sha256 `42066ae544de43f71ec84d4f411abe9b55875b9f06f06164a5eabfd30413816a` (574 lines), README/DEVELOPMENT pointer diffs byte-identical to the verified candidate after rebase. Raw evidence: harness `state/lanes/l1-storage-spec/{impl-1..3,verify-1..3}` and `results/l1-verify/` (outside Git).

## L2 `l2-storage-core-s1-s3`

- [x] Worktree `worktrees/l2-storage-core-s1-s3`, branch `lane/l2-storage-core-s1-s3`, base `692d54b`, rebased onto `c72f2a5`
- [x] Clean code pass + implement (GPT6-Astra threads `01a077c8-ad90-7d81-a9f5-5e11d6959744`, repair `01a07919-5c04-7631-abbc-376e6f2a8d26`, repair `01a07a15-2e37-7d62-bcb8-61c82dce8add`): pillar plumbing, `[storage]` contract, task fixture, block accounting, S1/S3, mock knobs, 8 mock bundles, tests
- [x] Code verification (GPT6-Astra, four independent iterations: `01a07891-e7ad-7d20-82c5-6534cdc2e50e` NO-SHIP F1 daemon+exec clients omitted from S1, F2 conflicting no-log declarations accepted; `01a07972-6684-7dc2-b630-94170f5a912f` SHIP; `01a079c9-58d1-7fb3-9055-ee4a43bb2e5f` integration NO-SHIP after the L9 rebase, aider plaintext stdout turned into an infrastructure ERROR; `01a07af8-524a-76a2-ba23-9f10880d0539` **SHIP**): full diff review against the spec, 477→491 tests, `clippy -D warnings`, fmt, `list-tests` = 73
- [x] Computer-use verification (same verifiers: real `ahrb run --pillar storage` and `hbench storage` bundles on both mock transports for bounded/linear/quadratic growth and append/rewrite modes with artifact audits, quadratic FAIL and nonzero exit, combined daemon+exec fixture with 100 retired clients per repetition, plaintext fixture UNSUPPORTED, conflicting-declaration rejection, parser isolation, `cline-cli` alias; real adapters: pi and rick honest ABSENT, codex spec-conformant capture ERROR from a volatile git temp pack, recorded for L7/L8)
- [x] Repair loop until SHIP (four iterations)
- [x] Complete: commit `e24aeba`, fast-forwarded onto `master`, pushed

Record: verified tree = `b201bab` + diff sha256 `f8cb55b3577d9fcd185525edcfcaae57b1268351c4bd4ac8bf9bc6b9f421fd89` + `adapters/mock-exec-plaintext/manifest.toml` `ef507e30…`, squashed into `e24aeba`. Open notes for later lanes: codex's volatile `.tmp/.../.git/objects/pack` files trip the spec's capture-stability rule (L7 may need a declared volatile area or settle policy); pi's manifest declares no sessions so storage is ABSENT until L7 revisits. Raw evidence: harness `state/lanes/l2-storage-core-s1-s3/{impl-1..3,verify-1..4}` and `results/l2-*` (outside Git).

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

- [x] Worktree `worktrees/l9-extra-adapters`, branch `lane/l9-extra-adapters`, base `9d9b5c2`, rebased onto `692d54b`
- [x] Clean code pass + implement (GPT6-Astra threads `01a07797-961e-7811-af89-b744dc6c1e25`, continuation `01a0785a-272f-7d33-afc0-2440a4be2832`, doc repair `01a07994-4549-76c3-907a-b1b0a52fe9ed`): schema-2 manifests verified against the installed CLIs, generic runtime bindings, tests; pass 1 stopped honestly at its machine-sharing gate and pass 2 completed the outstanding native runs and full suite (460 passed)
- [x] Code verification (GPT6-Astra `01a07929-7afa-7ff0-be18-fb0b0c90c28e`: line-range manifest review against pinned CLI help/config schemas, full suite 467 passed, shared-Rust review for regressions; NO-SHIP on one P2 documentation finding about aider `--env-file`)
- [x] Computer-use verification (same verifier: `ahrb doctor` clean x3; real short and expanded quick matrix runs for aider, goose and cline with per-row outcomes recorded, fake-only routing and empty egress attempts confirmed, real `~/.aider*`, goose and `~/.cline` directories proven untouched by metadata snapshots; `hbench pi` regression check attributed a row-3 FAIL to a pre-existing oracle gap, not to this lane)
- [x] Repair loop until SHIP (iteration 2 `01a07996-a33a-77d0-b507-392a1cb99d07`: hash-bound re-verification of the wording fix plus a fresh aider doctor and run -> **SHIP**)
- [x] Complete: commit `c33237e`, fast-forwarded onto `master`, pushed

Record: candidate `e1f1583` + doc diff sha256 `a9aa118f86438eeff010a3aa28c38f3ef665cb88b359854f3e8daf70d7366eca` -> `c33237e`. Incident (contained): the first cline probe's generated config lacked the provider `updatedAt` field, so cline ignored it and sent a disposable fake credential to the real OpenAI endpoint once (authentication failure, zero fake-model requests); no real secret was involved, the template was fixed, and both later passes and the verifier proved fake-only routing. Real-harness FAIL/ERROR rows observed in these runs are benchmark findings kept in the lane evidence. Raw evidence: harness `state/lanes/l9-extra-adapters/{impl-1..3,verify-1,verify-2}` and `results/l9-*` (outside Git).

## L8 `l8-references-macmini`

- [ ] Worktree
- [ ] Pre-flight: all nine harness binaries, versions and hashes recorded, `ahrb doctor` clean for all nine adapters, mock self-certification (quiet machine)
- [ ] Matrix, economy, fidelity, storage for all nine harnesses (serial, quiet, fresh timestamped roots)
- [ ] Sanitized reference tables written (GPT6-Astra implementation context)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra: inspects the real run artifacts and rebuilt tables)
- [ ] Repair loop until SHIP
- [ ] Complete
