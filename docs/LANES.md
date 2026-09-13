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


## Status 2026-09-08 11:55 UTC — paused for orchestrator move

The owner paused the run to move orchestration to the Haider harness. Landed: L0, L1, L9, L2, L8a.
In verification when paused: L3 (`verify-2`, both findings fixed, 4 mock fault cases left), L4
(`verify-2`, real-adapter forms left), L5 (`verify-2`, suite passed, last CLI cases left), L6
(`verify-5`, Part A green, Part B left), L7a (`verify-1`, real runs left). L10 (measurement fixes from
the Haider analysis) implementing. Queued: L7b, L11 (ERROR rows), L8. Every worker thread, candidate
digest and resume command is recorded in the harness `HANDOFF-RESUME-2026-09-08.md` and
`state/checklist.md` (outside Git).

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
| L3 `l3-storage-s7-s8` | S7 bounded auxiliaries per declared file family; S8 request-body retention classification and ratio from fake-model request bytes versus run-root growth; mock evidence for none/deduplicated/full | L2 | **complete** (SHIP, landed `2241d82`) |
| L4 `l4-storage-s5-s4` | S5 close retention after N create/close cycles and after the declared sweep interval; S4 compaction-versus-disk around the row-51 context-limit trigger; mock evidence | L2 | **complete** (SHIP, landed `18e3e60`) |
| L5 `l5-storage-s2-durability` | S2 fsync/fdatasync/F_FULLFSYNC counting per turn with honest OS limits (macOS interpose shim where the binary permits, Linux tracing where available, otherwise `UNSUPPORTED: os-limited` with evidence); estimated durability wall labelled as an estimate | L2 | pending |
| L6 `l6-storage-s6-s9-s10` | S6 declared `session_delete`/`uninstall_cleanup` residue on disposable benchmark profiles only; S9 crash residue after the row-57 kill; S10 resume read cost around the row-52 resume; mock evidence | L2, L4 | pending |
| L7a `l7a-storage-collector-results` | Split out 2026-09-07: codex terminal-before-reap collector fix, codex volatile-area rule, pi sessions revisit, saved-index/`hbench diff` storage integration | L2 | in progress (blocked) |
| L8a `l8a-reference-runner` | Split out 2026-09-07: `scripts/reference-run.sh` (six harnesses x four pillars, pre-flight, resumable, honest exit), `scripts/reference-tables.py`, tests, handoff docs; verified on mocks | L2 | **complete** (SHIP, landed `8cc1e8d`) |
| L10 `l10-measurement-fixes` | From the GPT6-Astra analysis of the Haider/rick ad-hoc run: cross-collector CPU aggregation defect, missing thin-client roots in daemon+exec trials, tool-argument normalization (false `terminal-without-effect`, rows 29/61), collector reread overhead (rows 43/49), stream frame identity (48/59), haider-agent declarations, per-row wall timing | L2 | in progress (paused) |
| L11 `l11-error-rows` | Make the eight ERROR rows (47, 48, 52, 53, 59, 60, 61, 62) measurable for the six harnesses | after the storage pass | pending |
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

- [x] Worktree `worktrees/l3-storage-s7-s8`, branch `lane/l3-storage-s7-s8`, based on the L2 candidate, rebased onto master
- [x] Clean code pass + implement (GPT6-Astra thread `01a079c9-5902-71c1-bd84-102595d9d5f4`, repair `01a07ce0-c03c-76d0-aed6-d88d42b14705`): S7 per-family growth with rotated siblings, S8 retention classification by digest and byte search with stored/unique ratio, `AHRB_MOCK_STORAGE_AUX_MODE` / `REQUEST_RETENTION` / `UNRELATED_GROWTH` knobs, ten mock bundles, tests
- [x] Code verification (GPT6-Astra `01a07b98-08ef-7451-bbd8-9791f84bece0` NO-SHIP: plaintext feasibility not propagated to S7/S8, whole-crate float assertion; `01a07d39-6fb3-70a0-9828-9b6849ce6ba8` **SHIP** after the repair: full suite passed, both findings confirmed fixed)
- [x] Computer-use verification (same verifiers: positive/adverse S7/S8 mock cases on both transports incl. fault injection, plaintext mock UNSUPPORTED across S1/S3/S7/S8, real codex honest collector ERROR, real pi ABSENT)
- [x] Repair loop until SHIP (two iterations; interrupted by provider outages and resumed on the same threads)
- [x] Complete: commit `2241d82`, fast-forwarded onto `master`, pushed
- [x] Post-integration verification (GPT6-Astra `01a0816f-e2d6-7860-b668-74b8c2d647a2`, 2026-09-12): first pass NO-SHIP on one finding — `cargo fmt --check` failed in `tests/runner_scripts.rs` (upstream L8a test formatting, not L3 code); rustfmt-only repair by GPT6-Astra (`01a095a2-71f8-79f1-9d78-f854f2196150`) landed as `894b9df`; second pass **SHIP** bound to `894b9df`: full suite 510 passed / 0 failed, mock fault cases ERROR as specified, real codex honest ERROR receipt, real pi ABSENT, no NOT EXECUTED rows. Known docs-only defect: the `2241d82` commit title says "volatile-area retention and cleanup"; the implementation is S7 bounded auxiliaries + S8 request-body retention (history not rewritten).

Record: verified tree = `9f198a5` + repair diff sha256 `4801dee3f9ad2c33d5bfd6a23a5767e76caf0e5a8b4091985dae7872d952e388`, squashed and rebased onto `5cd41b4` by the interim orchestrator; integrated tree `894b9df` re-verified post-landing as above. Raw evidence: harness `state/lanes/l3-storage-s7-s8/` (outside Git).

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

## L8a `l8a-reference-runner`

- [x] Worktree `worktrees/l8a-reference-runner`, branch `lane/l8a-reference-runner`, base `6f4f2e5`, rebased onto `bc4f467`
- [x] Clean code pass + implement (GPT6-Astra thread `01a07be6-8d76-7852-a69b-fa8a318baa4b`, repair `01a07d9b-6593-7a01-846d-55936190c9f5`; both resumed once across provider outages): `scripts/reference-run.sh`, `scripts/reference-support.py`, `scripts/reference-tables.py`, 20 runner-script tests, handoff/README docs
- [x] Code verification (GPT6-Astra `01a07d22-3906-75e0-83d5-b91f1e5d5cbc` NO-SHIP on three findings: incomplete typed summaries could yield PASS, matrix badge lost in tables, resume adopted reports without provenance; `01a07eef-7d41-7e21-9738-25dc1683136a` **SHIP**: line review, `zsh -n`, `py_compile`, all 20 tests, build)
- [x] Computer-use verification (same verifiers: real `scripts/reference-run.sh` on the two built-in mocks producing all eight reports and an honest `REFERENCE PARTIAL`, stub-driven pre-flight failure / step failure / resume runs, `scripts/reference-tables.py` output inspected for sanitization and labelling)
- [x] Repair loop until SHIP (two iterations)
- [x] Complete: commit `8cc1e8d`, fast-forwarded onto `master`, pushed

Record: verified tree = `6f4f2e5` + diff sha256 `64fe9c137d0e4f0f7785c6565cbc11ac0af0188d6f66f286427e8140c895b403` + new scripts (`2613fc98…`, `e5bf2ae8…`, `a6cf719f…`), rebased onto `bc4f467` without conflict. Raw evidence: harness `state/lanes/l8a-reference-runner/{impl-1,impl-2,verify-1,verify-2}` and `results/l8a-*` (outside Git).

## L4 `l4-storage-s5-s4` — completion record (2026-09-13)

- [x] Verification history: verify-1 and verify-2 NO-SHIP with repaired findings (F2: missing context-error boundary must classify ERROR, never UNSUPPORTED/PASS with a zero figure); verify-3 across passes 2026-09-12 (GPT6-Astra `01a081ce-89c2-7bb1-9def-e9c0b51f4e0e`) **SHIP**: full serial suite exit 0, F2 repro correct, fresh daemon/exec S4/S5 mock runs PASS both transports, auto-save parity (217 identical files, 147 evidence references), real codex honest ABSENT/UNSUPPORTED, real pi ABSENT, cleanup audits clean
- [x] Integration: squashed verified tree (`0549a7b`), rebase onto `c46a5bd` conflicted in 4 files vs L3's S7/S8 work → GPT6-Astra integrate (`01a096bf-bc41-7361-8257-e56298333d44`) resolved; its row-65 injection-surface regression (malformed request bodies carrying the storage marker leaked JSON parse errors) was caught by post-integration verification and repaired; final rebase onto `12f2ca2` picked up the L12 codex fix
- [x] Post-integration verification (GPT6-Astra `01a0971a-c309-7673-a2fe-71c0ca9dbec6` **SHIP** bound to `18e3e60`): full serial suite exit 0 (incl. mock_exec_full_matrix 657s, mock_full_matrix 246s), daemon+exec mock S4/S5 PASS / S7 UNSUPPORTED / S8 PASS, real pi honest ABSENT, real codex honest S4 ABSENT / S5 UNSUPPORTED with pre-existing S7/S8 protocol receipts matching the master baseline, no plugins contamination
- [x] Complete: commit `18e3e60`, fast-forwarded onto `master`, pushed

Record: pre-integration verified tree = `de78fa2` + diff sha256 `3206ba87…` (squashed as `0549a7b`, tree `6c91b06a…`); integrated tree `4889e8ab…` at `18e3e60`. Raw evidence: harness `state/lanes/l4-storage-s5-s4/` (outside Git).

## L7a `l7a-storage-collector-results` — completion record (2026-09-13)

- [x] Implement (GPT6-Astra, passes 1-5 incl. repairs): storage collector result handling with fatal-capture retention of transient receipts; `hbench diff` refuses numeric cross-pillar comparisons (exit 2, both operand orders; same-pillar unchanged); process registration tolerates a child exiting between spawn and registration (bounded identity-checked retry; external roots strict); mock-exec immediate-exit and reaped-child fixtures
- [x] Code + computer-use verification (GPT6-Astra, six passes; final verify-6 `01a096c4-ce0d-7fc0-a9c1-020c3e4dc558` **SHIP**: full serial suite pass, F1 refusal both orders on real report files, mock fixtures both command forms, real codex/pi vs pass-1 evidence, probes, save/no-save parity)
- [x] Integration onto `e4c868a`: conflict resolution across the storage core and codex manifest required four repair rounds (two storage_cli regressions; S7 trial chain dropped by the merge — checkpoint family snapshots, auxiliaries::evaluate, row 6/7 serialization — restored and adapted to this lane's typed RowDetails; interrupted-path S8 reconstruction with evidence_refs; interrupted-run ordering reconciled so both parents' semantics hold). Fresh-context battery: all seven focused checks exit 0
- [x] Post-integration verification (GPT6-Astra `01a097b2-8a44-7921-a438-02c9b9a55405` **SHIP** bound to `ab014c5`, zero NOT EXECUTED rows: repair audit, full serial suite, fresh daemon/exec mocks, cross-pillar refusal + same-pillar sanity, real codex (no plugins contamination, baseline receipts), real pi honest)
- [x] Complete: commit `ab014c5`, fast-forwarded onto `master`, pushed

Record: pre-integration verified tree = `6f4f2e5` + diff sha256 `846494ec…` (committed `a1d77b0`); integrated tree at `ab014c5`. Raw evidence: harness `state/lanes/l7a-storage-collector-results/` (outside Git).

## L12 `l12-codex-plugins-capture` (unplanned environment fix, 2026-09-13)

- [x] Worktree `worktrees/l12-codex-plugins-capture`, branch `lane/l12-codex-plugins-capture`, base `c46a5bd`
- [x] Implement (GPT6-Astra `01a09748-573e-7d62-88d3-6abd225386b1`): codex-cli silently updated 0.153.4 → 0.154.0 and began cloning plugins into fresh disposable profiles mid-run (`.tmp/plugins-clone-*`), tripping storage capture change detection in every real codex run (4 failures across L4/L6 verifications). Fix: `--disable plugins` (documented 0.154.0 switch) added to both codex transport commands in `adapters/codex/manifest.toml`; change detection untouched; owner binary untouched
- [x] Code verification (GPT6-Astra `01a0974c-58ed-7510-a960-3b83b7d6f78a` **SHIP**: manifest-only diff confirmed, switch documented, build/fmt/clippy/focused adapter tests pass)
- [x] Computer-use verification (same verifier: three fresh real `hbench storage codex` runs, zero contamination, honest declarations unchanged; matrix probe NOT EXECUTED — hung without output, recorded honestly)
- [x] Complete: commit `49d98eb`, fast-forwarded onto `master`, pushed (branch tip = master tip, so the landed tree is byte-identical to the verified tree)

Record: verified tree = `c46a5bd` + diff sha256 `f6cc507e9984266eaa6833ccbeec33482a6720aae2fc50fdd1587e81d9f52aca`. Raw evidence: harness `state/lanes/l12-codex-plugins-capture/` (outside Git). Pending lanes must rebase to pick up the fix for their codex rows.

## L8 `l8-references-macmini`

- [ ] Worktree
- [ ] Pre-flight: all nine harness binaries, versions and hashes recorded, `ahrb doctor` clean for all nine adapters, mock self-certification (quiet machine)
- [ ] Matrix, economy, fidelity, storage for all nine harnesses (serial, quiet, fresh timestamped roots)
- [ ] Sanitized reference tables written (GPT6-Astra implementation context)
- [ ] Code verification (GPT6-Astra)
- [ ] Computer-use verification (GPT6-Astra: inspects the real run artifacts and rebuilt tables)
- [ ] Repair loop until SHIP
- [ ] Complete
