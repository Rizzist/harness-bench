# AHRB v4 — storage pillar

Status: normative implementation specification, revision 4.0. This document defines requirements for implementing storage; it does not claim that the current binary supports them.
MUST, MUST NOT, and REQUIRED are normative. [SPEC-v2](SPEC-v2.md) §§3–7 supply the outcome, identity, evidence, and comparison conventions except for explicit overrides below.
The [proposal](PROPOSAL-v4-storage.md) supplies motivation, not additional oracles.

## 1. Pillar identity and standardized task

Storage is independent of matrix, economy, and fidelity. Its command forms are:

```console
ahrb run --pillar storage --manifest M --profile quick --output /absolute/fresh/bundle
# Uses the storage cert default below: 174979 + 7 * sweep_interval_s seconds.
hbench storage <harness> --profile cert --output /absolute/fresh/bundle
```

Both accept `quick|cert`, `--deadline`, `--no-save`, and existing isolated-pillar output
options. Matrix row filters (`--tests`) are invalid. Ordinary `ahrb run` remains the matrix and neither executes storage nor emits `storage_summary`. Storage executes all
ten rows; unavailable rows remain visible. The task ID is `ahrb-storage-tiny-turns-v1`.

| Parameter | Quick | Cert | Rationale |
|---|---:|---:|---|
| Completed user turns per growing session, N | 100 turns | 1,000 turns | Reuse row 49's short and long horizons. |
| Independent repetitions | 3 repetitions | 7 repetitions | Reuse v2's median and dispersion sampling. |
| Footprint checkpoints | 0, 1, 10, 50, 100 turns | 0, 1, 10, 50, 100, 500, 1,000 turns | Separate startup constant, early growth, and late growth. |
| Fixture tool cadence | Every 10th turn | Every 10th turn | Preserve row 49's periodic tool work. |
| Settling / stability probe / maximum settle | 2,000 ms / 100 ms / 10,000 ms | Same | Admit ordinary writeback while bounding an unstable collector. |
| Ordinary / outer turn deadline | `resources.turn_timeout_ms` / that value + 3,000 ms | Same | Preserve the v2 outer-supervisor allowance. |

Storage MUST resolve its own run deadline; the current generic 900 s quick / 1,800 s cert
defaults in `src/cli.rs:167–183` cannot accommodate this schedule. Let R=3/7, N=100/1,000,
C=20/200 (S5 sessions), Q=1/3 (S9 trials), and W=`storage.sweep_interval_s` in seconds,
or 0 if absent. The following serialized boundary allowances define K; shared rows are
counted once. Reserve both S6 verbs, S2 instrumentation and an S5 post-sweep boundary even
when unavailable, so feasibility probes do not shrink the default during a run.

| Work | Budgeted filesystem boundaries |
|---|---:|
| Shared S1/S3/S7/S8 task, including checkpoint 0 | R × (N + 1) |
| Separate S2 task, including checkpoint 0 | R × (N + 1) |
| S4 before/after compaction | 2 × R |
| S5 baseline, every tenth close, and post-sweep | R × (2 + C/10) |
| S6 two independent operations, growing-session preparation and operation boundary | 2 × R × (N + 2) |
| S9 growing-session preparation and three crash/recovery boundaries | Q × (N + 3) |
| S10 growing-session preparation and detach/resume boundary allowance | R × (N + 2) |

K is the sum: 1,645 quick / 38,233 cert. The preparation allowances conservatively reserve
per-turn settling for S6/S9/S10; they do not add snapshots inside S10's timed read gate.
The default is `ceil(2 × 2.1 s × K + H + R × W)`, with fixed overhead H=3,600 s quick /
14,400 s cert: **10,509 + 3W seconds quick; 174,979 + 7W seconds cert**. The 2.1 s term is
§2's minimum 2,000 ms settle plus 100 ms stability probe; doubling reserves additional
settling/collector and turn work, while H reserves startup, context-recovery turns, control
commands, sync/digests, scans and reporting. R×W covers sequential S5 sweep waits in full,
without overlap credit. These are budget choices derived from the serialized workload,
not measured runtimes or guarantees that a slow harness will finish. The shared cert task
alone has a 14,700 s per-turn settle lower bound; quick shared plus S2 has 1,260 s, before
baseline samples or any harness work. Both profiles retain the full settle window and
cert retains 1,000 turns × 7 repetitions; quick MUST NOT shorten settling to fit a deadline.

Resolution order in both command forms is explicit `--deadline` seconds, then `AHRB_DEADLINE`,
then the storage default above (the example assumes no environment override). Overrides
remain non-negative integer seconds, including 0; invalid/overflowing budgets are usage
errors. Never silently extend an override, truncate repetitions, or skip samples to fit it.
Log the resolved seconds, source (`flag/environment/storage-default`), K/H/W and formula
before starting. The deadline covers the whole serialized workload, including preparation,
settling, instrumentation and sweep waits; expiry stops further stimulus and performs owned
cleanup. Write `report.json`, `report.md`, diagnostics and partial evidence even on expiry:
preserve finalized rows, mark active/pending rows `ERROR: deadline` (terminalized but not
finally evaluated rows `ERROR: deadline interrupted final evaluation`), null incomplete
aggregates, withhold the badge, and return **2**. This adopts the matrix deadline/report
semantics (`src/runner.rs:2617–2635,4171–4187,4222–4280,4334`); the current isolated economy
wrapper at `src/runner.rs:2651–2663` times the workload but only writes a failure diagnostic
on expiry, so L2 MUST implement storage's full interrupted report explicitly.

Reuse `runner::run_long_horizon`'s submit/terminal loop and session identity, not its matrix close-delete or latency oracle. Each repetition gets a fresh profile and workspace;
there is no unmeasured model warm-up in storage, so the first-turn constant remains visible. Start/readiness and seed setup precede checkpoint 0. All provider traffic, including
auxiliaries and retries, goes exclusively to AHRB's fake model; no real inference participates.
A turn is one submitted user input through its successful terminal, not one model POST. Retries and tool continuations increase physical requests without increasing N.

The deterministic payload contract reuses `economy_workflow`'s exact five contents:
`"alpha architecture notes and stable constraints\n"`, `"bravo interface notes and deterministic inputs\n"`,
`"charlie verification notes and expected outputs\n"`, `"delta edge cases and bounded failure behavior\n"`,
and `"echo integration notes and terminal conditions\n"`, each repeated 16 times. Their UTF-8 lengths are respectively 768, 752, 768, 736, and 752 bytes; repetition fixes
the seed size rather than allowing adapter-selected payloads. L2 MUST verify these lengths
against the source strings, pin content SHA-256s, and reject a mismatch before measurement.
Seed `context-a.txt` through `context-e.txt` in the resolved actor workspace before checkpoint 0. Each submitted prompt consists of content a followed by the ASCII route marker
`[[AHRB:scenario=ahrb-storage-tiny-turns-v1;actor=storage;checkpoint=tNNNN]]`;
`NNNN` is the four-digit turn ordinal. This fixes prompt length within the task.
Ordinary fake responses carry the exact economy edit bytes `AHRB economy fixture edit v1\n`
as 29 bytes of assistant text and terminal success; this fixes the response content budget.
Every tenth turn first scripts the mapped `read` of `context-a.txt` with call ID `storage-read-NNNN` and route checkpoint `tNNNN-terminal`,
then returns that same terminal response after the correlated result. The fixed content sizes are fixture requirements, not expected storage measurements.
L2 MUST pin the prompt, read-call, and rendered response bytes/hashes per supported dialect.
Economy today fixes content, not complete HTTP bodies: tools, paths, protocol framing, instructions, and accumulated history remain harness-generated and MUST NOT be padded away.
Record actual request/response lengths separately; never claim equal full HTTP sizes across adapters.

Per-invocation means v2's nonpersistent client/worker topology; each turn uses the public
continue/resume surface in the same persisted session. A daemon retains its owned controller
and session between turns. Missing required `sessions`/`resume` capability declarations or
operation bindings yield `ABSENT` for dependent rows; proven inability to persist/continue is
`UNSUPPORTED`, never a sequence of fresh sessions presented as a growing one.
Every value carries OS, topology, profile, task, and `comparison_scope="within-topology-only"`.
Never cross-rank OSes, topologies, profiles, fixture revisions, or instrumented/uninstrumented runs.

## 2. Accounting and outcome rules

The run root is the union of the hermetic profile (including HOME/config/state/cache/tmp) and the actor workspace. L2 MUST obtain the workspace from the driver or a typed
`storage.workspace_path` template, using fidelity's locator validation. It MUST be inside
the profile on the same device. Collapse lexically nested roots before traversal, so the workspace is counted once. AHRB bookkeeping, fake-provider bodies, collectors, shim logs,
and report bundles MUST live outside this union; relocate existing in-profile AHRB artifacts. Harness writes and the deterministic workspace fixtures remain inside it.

Use row-50 no-follow traversal: lstat each entry, record relative path, kind, device/inode,
and digest; verify opened and final identities with no-follow file opens. Symlinks (including
internal ones), repeated hard-link identities, mount/device escapes, cycles, root escapes, unreadable files, and identity/size/block changes during capture are `ERROR`. Never follow
links or deduplicate aliases to manufacture a smaller footprint. Count regular files only;
record directories and special entries separately. APFS clones are distinct identities counted
by their reported blocks, even when extents are shared. Sparse files count allocated blocks.
This is per-file allocated footprint, not unique-volume consumption or recoverable free space.

`allocated_bytes = st_blocks * 512 bytes`; 512 bytes is the stat block unit, independent
of filesystem allocation size. Record `apparent_bytes = st_size` only as a diagnostic. Never use apparent size, `du -h`, or compressed-size guesses as the footprint metric.
“Logical growth” below means growth of allocated footprint; it is not logical write syscall bytes.
Before every filesystem boundary sample, stop submitting, allow 2,000 ms to settle, call AHRB-owned
`sync`, and take two matching metadata inventories 100 ms apart; retry within 10,000 ms
from the terminal/boundary. Digest the stable inventory with identity checks. Failure to settle
or sync is `ERROR`. Record sync start/end and settle duration; sync is neither harness fsync
nor proof that hardware committed bytes. Keep harness latency timers outside these pauses.

Physical I/O is separately the OS-accounted whole-owned-tree byte delta: macOS
`ri_diskio_byteswritten` / `ri_diskio_bytesread`; Linux `/proc/<pid>/io` `write_bytes` /
`read_bytes`, or isolated cgroup `io.stat` `wbytes` / `rbytes`. Never use `wchar`/`rchar`.
These counters are OS attribution, not direct device-sector telemetry; cache hits may read
0 physical bytes. Keep per-process and cgroup series separate and select one complete source,
never sum both. Include descendants and retired identities by `(PID,start-time)`; exclude
AHRB/fake model/sync/sampler. A final terminal-before-reap sample or durable quiet-cgroup accounting is mandatory for every retired identity, as in row 47. Missing a short-lived
child's counter is `ERROR`; absence of an entire platform facility proven before collection
is `UNSUPPORTED: os-limited`. A last live poll is insufficient. Never substitute footprint for I/O.

At every turn boundary, let A(t) be total allocated bytes. For each path with unchanged device/inode, growth is `max(0, allocated_after-allocated_before)`; if its apparent size
decreased, growth is 0 bytes even if block allocation increased. A new/replaced identity's first observation contributes 0 bytes, exactly `row47_file_growth`'s baseline reset rule.
Sum these path growths as L(t). Deletion contributes 0 bytes. Separately report signed net
change `A(t)-A(t-1)` and full A(t), which include creation, replacement, and reclamation. Write amplification is `sum(physical_write_bytes)/sum(L(t))`, a dimensionless proxy over
the full repetition. A zero denominator gives JSON null for both 0/0 and positive/0; record `zero-denominator`, never zero, infinity, or an inferred perfect amplification.

All repetitions must be complete before publishing row aggregates. Report medians of per-repetition scalar values, nearest-rank p95 (`ceil(0.95*n)`), sums of counters,
and maxima where explicitly named. Evidence remains repetition-scoped. A failing repetition
cannot be hidden by a median. General state precedence is `ERROR > ABSENT > UNSUPPORTED > FAIL > PASS`.
S6 explicitly overrides this: aggregate only DECLARED operations across repetitions using
`ERROR > FAIL > ABSENT > UNSUPPORTED > PASS`. Missing-operation UNSUPPORTED receipts live
separately in typed details and never enter this aggregation. If neither verb is declared,
S6 is UNSUPPORTED for missing operations (subject to common prerequisite ABSENT or collection
ERROR); a missing verb alone cannot make S6 UNSUPPORTED when another verb is declared.
Preserve every subcase's status even when the row is unavailable. `measurement_complete=false`
for ERROR/ABSENT; verified UNSUPPORTED describes completed feasibility evidence only. Only S3 superlinear growth and S6 residue after a declared verb can produce `FAIL`.
Other complete observations are informational `PASS`, including adverse classes; a PASS there means measurement completed, not that behavior is desirable. Attempted ordinary-task
failure or missing evidence is `ERROR: task-incomplete` (except S9/S10's observed resume outcomes); optional operation unavailability
must be established before stimulus, not inferred from a failed attempt. Compute scalar MAD for Markdown from typed trial values; S3 alone stores curve `mad_bytes`.
Unless a row states otherwise, any required null trial scalar makes its aggregate null;
do not silently aggregate only favorable available repetitions. Even-size medians average the middle pair.

## 3. Optional manifest contract

Add a typed optional `[storage]` table, accepted additively by existing manifest schemas. Omitted and explicitly empty arrays remain distinguishable in the parsed representation.

| Exact key / type | Meaning and validation |
|---|---|
| `areas.<name>: string[]` | Run-root-relative globs. Reserved families are `store`, `cas`, `views`, `pipes`, `logs`, `other`; additional names match `[a-z][a-z0-9_]*`. `other` is computed remainder and may only be omitted or `[]`. |
| `session_delete: string[]`, `uninstall_cleanup: string[]` | Optional public harness argv; empty/omitted means no declared verb. Delete requires `{{session_id}}` exactly once. Scope must be proven by `{{profile}}`/`{{workspace}}` arguments or the isolated environment actually honored by the public command. |
| `sweep_interval_s: u64?` | Positive seconds until a documented automatic retention sweep; measure elapsed time, never invoke an invented sweep. Missing means no expiry claim. |
| `session_close: string[]` | NEW extension required for S5: public close-without-delete argv with `{{session_id}}` exactly once, or `[]` for unavailable. Never alias a deleting operation. |
| `retention_cap_bytes: u64?` | NEW extension required to claim S5 bounded: declared total allocated-byte cap above the warm store baseline across all closed sessions, not a cap per session. Zero bytes is a valid no-retention policy. |
| `auxiliary_cap_bytes.<name>: u64` | NEW extension for S7: total allocated-byte cap for a declared family, including rotated siblings. No cap may be inferred from a filename or `max_output_bytes`. |
| `workspace_path: string?` | NEW locator extension, normalized profile-contained template; absent uses the public driver workspace. |

The three cap/close extensions resolve omissions in the proposal: there is currently no
typed close-only operation or storage cap in `Manifest`. Caps are adapter policy declarations,
not reference measurements. L2 validates them; L4/L3 implement their respective consumers.
Globs use case-sensitive `/` components: `*` matches within one component, `?` one character,
`**` an entire component matching zero or more directories; other characters are literal.
Reject absolute paths, empty globs, `.`, `..`, backslashes, templates inside globs, unknown
storage keys, and overlapping family matches. Multiple globs within one family are unioned.
A glob matching no file is valid with an explicit empty-match receipt; it cannot prove absence
outside that area. Declarations label files; they never restrict the exhaustive root audit. Only manifest-declared areas assign families; unmatched files go to `other`.

Argv is executed directly, never shell-parsed; reject shell interpreters/wrappers and unresolved
variables. Allowed variables are `profile`, `workspace`, `session_id`, and resolved `harness`
(the discovered executable); path-valued arguments other than the executable must stay inside the disposable root.
Reject secrets and credential templates in argv even if legacy `allow_credential_argv` is true.
Close/delete/uninstall use only the benchmark-created profile and fake account. A command
that acts on a real installation, global package, real account, or unscoped HOME is invalid; omission is the correct declaration when no safely scoped public command exists.

Existing `sessions.store_paths` remains the profile-contained S5/S6 store locator;
`resources.journal_paths` and `events.path` identify journals, and `resources.log_paths`
identifies logs for cross-checks. These locators do not silently become area globs or permit
exclusions. Omitted log paths do not block whole-root S1/S3; explicit `[]` is only a no-log
claim after exhaustive corroboration, as row 47 requires. Conflicting declarations are ERROR.
`sessions.close_delete`/`sessions.delete` do not imply either storage verb: the adapter must
explicitly declare the storage argv. `[cleanup].command/paths` is AHRB teardown after evidence,
never S6 evidence. Do not execute teardown before final snapshots or use it to erase residue. Use existing capability rationale maps (`sessions`, `resume`, `durable_journal`,
`context_limit_recovery`) and driver bindings; never parse free-text rationales as booleans.

Without `[storage]`: S1/S3 still measure the complete root; S2 still probes OS feasibility;
S4 still uses the context-recovery declaration; S5 is UNSUPPORTED (no close/cap/sweep claim);
S6 is UNSUPPORTED (no deletion verbs); S7 reports `other` and an unsupported cap assessment;
S8 still scans the complete root; S9/S10 still use journal/resume capabilities. Missing common
session/workspace prerequisites retains the ABSENT/UNSUPPORTED policy in §1 for every row.

## 4. Ten storage rows

The following vertical tables use v2's row-table fields. Every row belongs to Storage and
is NEW (new evidence or stimulus), with S3 CORE and S6 conditional CORE. Others are INFORMATIONAL. All topology rules in §§1–2 apply independently to each row. `s.<field>`
below expands exactly to `storage_summary.<field>`; detail blocks use the stable slug ID. Each row's finite numeric summary scalars have the exact mirror `metrics.storage.<field>`;
arrays, enums, booleans, nulls, and integers nested in objects are never flattened into metrics.

### S1 `write-volume`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL, required D input. Shared N-turn task, 3/7 reps. Per-invocation includes each launch through retirement; daemon includes the warm controller's turn and settle deltas. No idle subtraction. |
| Evidence | `s.write_bytes_per_turn_p50`, `s.write_bytes_per_turn_p95`, `s.write_bytes_per_turn_max`, `s.logical_growth_bytes_per_turn`, `s.net_growth_bytes_per_turn`, `s.write_amplification_ratio`, `s.disk_class`; `details.write-volume.trials`; `storage-samples.jsonl`, `storage-files.jsonl`, `processes.jsonl`. Snapshot every turn, not just S3 checkpoints. |
| Oracle | PASS iff all counters, identities, and N terminals are complete. Per repetition compute physical p50/p95/max, `sum(L)/N`, `(A(N)-A(0))/N`, and amplification (§2); headlines are medians except global max. D uses headline p95: `D64` ≤64 KiB/turn; `D256` >64 and ≤256 KiB/turn; `D1024` >256 and ≤1,024 KiB/turn; `D4096` >1,024 and ≤4,096 KiB/turn; `D4096+` >4,096 KiB/turn. One KiB is 1,024 bytes; fourfold bands describe scale without a performance FAIL threshold. |
| Manifest / feasibility | No storage declarations required. Missing sessions/workspace is ABSENT; proven unavailable counters are UNSUPPORTED; incomplete retirement is ERROR. Reuse `collect_disk_io_trials`, `TreeDiskTracker`, and `row47_file_growth`; implement allocated blocks and complete per-turn root snapshots. Footprint/amplification describe allocation, not write syscall volume. |

### S2 `durability-cost`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Separate instrumented copy of N-turn task in fresh 3/7 profiles; do not contaminate S1/S3 measurements. Count all owned client/worker/daemon calls in submit-to-settled intervals. |
| Evidence | `s.fsync_calls_per_turn`, `s.fdatasync_calls_per_turn`, `s.fullfsync_calls_per_turn`, `s.durability_calls_per_turn`, `s.assumed_fsync_cost_ms`, `s.estimated_durability_wall_ms_per_turn`, `s.durability_class`; `details.durability-cost.trials`, `.instrumentation`; `fsync-events.jsonl`. |
| Oracle | PASS means complete instrumentation. Count successful calls separately by primitive; record failed calls and errno as diagnostics. An OS-inapplicable primitive is null with an applicability receipt and excluded from the sum, not a missing hook. `durability_calls_per_turn` is the applicable sum/N; estimate = calls/turn × **4 ms/call**, a fixed comparison assumption from the proposal, not measured latency or calibrated device cost. F class: `F0` =0 calls/turn, `F1` >0–1, `F10` >1–10, `F10+` >10 calls/turn. These per-turn/decade bands describe durability frequency, not reliability. Never claim parallel fsync wall times add or fsync proves crash safety. |
| Manifest / feasibility | No storage key or ABSENT specific to instrumentation. macOS requires a new interpose shim via `DYLD_INSERT_LIBRARIES` only on binaries permitting injection; Linux requires available owned-tree tracing (strace/eBPF) and loss accounting. Otherwise UNSUPPORTED: `os-limited`, with the observed reason. `mock_harness::DurableJournal::{open,append}` calls `sync_all`; no fsync-event collector exists today. |

S2 is an explicit storage-only exception to v2's prohibition on turn-path instrumentation.
Record resolved executable/hash, OS, backend/version, shim hash, launch environment key names,
per-image load handshake bound to PID/start-time, and an isolated self-test intercepting each
supported primitive. The handshake/self-test is excluded from counts. Verify every owned
image's coverage through exec/fork/exit and a clean end-of-stream/drop counter. Environment presence alone proves nothing. Stripped DYLD state, refused loading/library validation,
static/direct-syscall paths outside shim coverage, trace permission denial, or an unprovable
coverage mechanism is UNSUPPORTED with diagnostics, never a zero fsync count. Do not disable SIP, re-sign, patch, or change harness permissions to obtain a number. After successful
preflight, trace loss, missing image receipts, or collector failure is ERROR. Nested wrappers
must not double-count one fsync; count syscall-equivalent successful primitive completions.

### S3 `footprint-curve`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; CORE. Share S1's uninstrumented task and repetitions, with the exact checkpoint set in §1. Both topologies keep one accumulating session; retained daemon state is included. |
| Evidence | `s.footprint_curve`, `s.first_turn_allocated_bytes`, `s.footprint_slope_bytes_per_turn`, `s.growth_class`; `details.footprint-curve.trials`; `storage-samples.jsonl`, `storage-files.jsonl`. Curve entries contain `{turn,allocated_bytes,mad_bytes}` across repetitions; first-turn constant is A(1), including baseline, with A(0) separately visible. |
| Oracle | Per repetition compute Theil–Sen slope of `(t,A(t))` over nonzero checkpoints (median of all pairwise slopes). Let H=N/2, E=N/10, late=(A(N)-A(H))/(N-H), early=(A(H)-A(E))/(H-E). Bounded iff late checkpoint range `max(A(H),A(N))-min(A(H),A(N)) ≤65,536 bytes` and global slope ≤65,536/N bytes/turn. Otherwise superlinear iff slope >0 bytes/turn AND late >max(1.25×max(0,early), max(0,early)+4,096 bytes/turn); otherwise linear. Record early, late, and nullable late/early ratio (null when early≤0). The 64 KiB allocation allowance absorbs small block churn; the 25% dimensionless increase plus 4 KiB/turn floor requires both proportional and block-scale acceleration. |
| Outcome / feasibility | FAIL iff any repetition is superlinear; otherwise PASS. Headline G is worst of bounded < linear < superlinear, never the median's class. “Bounded/linear” describes this sampled horizon, not asymptotic proof. No area declaration needed; missing common locator/session is ABSENT, platform block-stat absence is UNSUPPORTED, missing checkpoint is ERROR. Reuse `run_long_horizon`, `row50_store_snapshot`, and row-49 observations; add blocks and this deterministic shape evaluator. |

### S4 `compaction-vs-disk`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Separate row-51 context-limit fixture, its quick/cert W, history/tool-pair counts, exact padded request limits, and 3/7 reps. Per-invocation resumes; daemon recovers the same session. |
| Evidence | `s.compaction_before_allocated_bytes`, `s.compaction_after_allocated_bytes`, `s.compaction_freed_pct`; `details.compaction-vs-disk.trials` includes row-51 candidate hashes and recovery judgement; `storage-files.jsonl`, `storage-samples.jsonl`, `model-requests.jsonl`. |
| Oracle | Gate the fake provider before yielding its context error; settle/sync/snapshot B. After row 51's first accepted compacted request and terminal, settle/sync/snapshot C. `freed_pct=100*(B-C)/B`; B=0 bytes gives null, negative percent means growth. Report exactly “compaction frees N% of allocated disk” with signed N, or “compaction frees unavailable% of allocated disk (zero baseline)”. A verified no-compaction recovery is UNSUPPORTED: `no-compaction-observed`; collection failures are ERROR. PASS for a measured compacted pair regardless of percentage. |
| Manifest / feasibility | Missing `context_limit_recovery` or context-window binding is ABSENT; proven unavailable recovery is UNSUPPORTED. Reuse `collect_context_recovery_repetition` and its row-51 provider boundaries; new snapshot gates are required. The byte difference is measured; attribution to compaction is an interval association. Reports MUST explain that context shrinking need not reclaim journals; the non-normative appendix states the append-only hypothesis. |

### S5 `close-retention`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Row-50 create/use/close loop: 20 sessions quick, 200 cert, 3/7 sweeps; one §1 tiny turn per session. Counts preserve v2's lifecycle stress. Daemon stays ready; per-invocation waits for each client exit and invokes close-only if declared. |
| Evidence | `s.closed_sessions`, `s.close_retained_bytes_per_session`, `s.close_retained_after_sweep_bytes_per_session`, `s.retention_cap_bytes`, `s.close_retention_class`; `details.close-retention.trials` includes checkpoints at 0/every 10/final session, immediate and post-sweep bytes, validated close receipts; `storage-samples.jsonl`, `storage-files.jsonl`. |
| Oracle | B is warm declared-store baseline before creating sessions. Retained R=max(0,store_allocated-B). Publish R/N immediately after final close and after `sweep_interval_s` elapsed from final close plus §2 settling. With an interval, bounded iff every repetition's final post-sweep R≤cap; without an interval require every immediate checkpoint R≤cap. Otherwise unbounded when a cap is exceeded; no cap yields null class and row UNSUPPORTED, retaining byte observations. Labels mean within/exceeds declared bound, never proof of infinite growth. Missing sweep leaves the post-sweep field null. |
| Manifest / feasibility | Missing close-only verb is UNSUPPORTED: `no-close-without-delete`; missing store_paths when close declared is ABSENT. Pure client exit is not proof of a session-close policy. Declared close failure or missing receipt is ERROR. Reuse `collect_session_residue_trials` and row-50 traversal, but replace `Driver::close`/close-delete with the NEW `storage.session_close` contract. Close-delete results cannot be relabelled as close retention. |

### S6 `delete-uninstall-residue`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; conditional CORE once either storage verb is declared. Exercise each declared verb independently in fresh 3/7 profiles after one N-turn session. Per-invocation clients exit; daemon uses its declared shutdown before uninstall, included in the measured operation sequence. |
| Evidence | `s.delete_residue_allocated_bytes`, `s.delete_residue_files`, `s.uninstall_residue_allocated_bytes`, `s.uninstall_residue_files`; `details.delete-uninstall-residue.operations` records verb, scope, baseline, receipts, outcomes, and leftovers; `storage-files.jsonl`, `storage-samples.jsonl`. |
| Oracle | Session-delete scope is `sessions.store_paths`; baseline is initialized empty store before session creation. Uninstall scope is the entire run root; baseline is generated AHRB configuration/seed files before harness initialization. After successful argv exit and settle/sync, a surviving regular file is residue if new, identity-replaced, or content-different from its baseline path. Count its full allocated bytes and one file, including zero-block files. Report whole-root leftovers alongside delete-scope residue. A declared operation is FAIL iff its successful verb leaves >0 bytes OR >0 regular files of scoped residue in any repetition; row aggregation follows §2 (ERROR overrides FAIL). Zero bytes/files is required because the declared verb claims removal; unchanged baseline files are not session residue. |
| Manifest / feasibility | Missing verb → separate typed missing-operation receipt, UNSUPPORTED: `no way to delete a session` / `no disposable-profile uninstall cleanup`; it is excluded from row aggregation. Both missing → row UNSUPPORTED, subject to common prerequisite ABSENT/collection ERROR (§2). Missing store locator for declared delete is ABSENT. Failed command, scope guard, or audit is ERROR. Aggregate declared operations only: ERROR first, then any residue FAIL, then ABSENT, then proven UNSUPPORTED, otherwise PASS. One declared clean verb plus one missing verb is row PASS; one declared residue verb plus one missing verb is core row FAIL and completed-report exit 1. ABSENT on a declared operation with no FAIL/ERROR yields row ABSENT; declared ERROR yields row ERROR and exit 1. Preserve unavailable-operation details even alongside FAIL. Use manifest argv/rendering, row-50 identity/digest snapshots, row-68 public delete handling; no implicit use of `[cleanup]` or generic recursive deletion. |

### S7 `bounded-auxiliaries`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Share S3's task/checkpoints, both topologies. Audit all declared families, especially logs, WAL/history, views and pipes, including rotated siblings. |
| Evidence | `s.auxiliaries[]`; `details.bounded-auxiliaries.trials`; `storage-files.jsonl`, `storage-samples.jsonl`. Each family has `{name,declared,cap_bytes,peak_allocated_bytes,final_allocated_bytes,slope_bytes_per_turn,rotation_observed,class}`; cap/class nullable. |
| Oracle | A declared cap gives bounded iff every checkpoint in every repetition is ≤cap allocated bytes, otherwise unbounded. No cap gives null class/UNSUPPORTED family assessment; any such family makes the row UNSUPPORTED, with measured curves retained. Rotation is measured identity turnover at the same declared paths, not proof of a cap. Family peak is global maximum, final is maximum final, slope is median Theil–Sen. Empty observed family is 0 bytes only after successful exhaustive audit; absence of a glob declaration is not a no-files claim. |
| Manifest / feasibility | `areas` and `auxiliary_cap_bytes` supply family/cap data; `other` always permits a cap. Unknown cap family is invalid; no declarations → `other` measurements with unsupported cap assessment, not ABSENT. Whole-root read failures are ERROR. Reuse S3 snapshots and row-47 log/journal locators for corroboration; add glob family attribution without filename heuristics. Unbounded is informational, never FAIL. |

### S8 `request-body-retention`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Share the uninstrumented N-turn task, 3/7 reps. Search the same exhaustive root in both topologies after final turn, before close/cleanup. |
| Evidence | `s.request_retention_class`, `s.stored_request_bytes`, `s.unique_request_content_bytes`, `s.stored_unique_ratio`; `details.request-body-retention.trials`, `.matches`; `request-body-matches.jsonl`, `model-requests.jsonl`, `storage-files.jsonl`. Match receipts include representation, request/block SHA-256, relative file, device/inode, offset, and matched length in bytes. |
| Oracle | Apply the exact byte classifier below; PASS means classification completed, never privacy safety. Disk growth alone cannot establish retention. None/full/deduplicated are observational byte classes, not storage architecture claims. Inconsistent classes across reps yield null headline and per-repetition classes, not a majority vote. |
| Manifest / feasibility | No storage block required, no row-specific ABSENT. `fake_model::ModelRequestRecord.request.canonical` retains parsed request JSON and raw byte length, not exact raw HTTP bodies. L3 MUST retain original bodies out of root before parsing, and reuse economy's canonical block projection. Encrypted/compressed/unreadable representations without a verified decoder are UNSUPPORTED: `representation-limited`, never inferred none; interrupted/truncated capture is ERROR. |

Freeze the complete raw-body and canonical-JSON byte needles for all physical requests
(primary, side-channel, retries), and distinct canonical message/content blocks using economy's
redundant-block projection. Unique content bytes U is the sum of byte lengths of distinct block
SHA-256s across the run; duplicate blocks count once. Exclude fixture files and unchanged
baseline ranges from matching, with explicit path/range receipts, to avoid counting seed echoes.
Scan regular-file bytes by exact search or verified decoded-record digest, never filename/hash
names alone. Digest comparison requires hashing actual file/record bytes. Decoders, if any,
must be data-format-specific, versioned, and lossless; report decoded and allocated bytes separately.
Within each file identity, stored request bytes S is the union length of matching raw/canonical
body and block ranges: overlapping matches count once; distinct copies count again. S is content bytes, not allocated blocks. `stored_unique_ratio=S/U`, null if U=0 bytes.
Class `full` if every distinct complete captured request has a raw or canonical body match;
otherwise `deduplicated` if every unique block is present and S/U≤1 (dimensionless, one stored
copy per unique byte). `none` requires zero matching bytes and complete coverage of supported
representations; it means “no matching request bytes found”, not proof against transformed storage.
Partial block/body coverage or duplicate fragments without full-body coverage is UNSUPPORTED:
`partial-retention-unclassified`, with S/U and matches retained and class null. No forced three-way guess.

### S9 `crash-residue`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Build N turns, hold the next turn at the fake provider, then SIGKILL the verified owned active group/controller. Exactly 1 quick / 3 cert trials, preserving row 57's fault repetition budget. Restart daemon if needed; invoke public resume. |
| Evidence | `s.crash_residue_allocated_bytes`, `s.crash_residue_files`, `s.crash_resume_outcome`; `details.crash-residue.trials` includes hold/delivery/exit receipts, committed cursor/hash, pre-kill/post-kill/post-resume inventories, and recovered suffix; `storage-files.jsonl`, `storage-samples.jsonl`, `events.jsonl`. |
| Oracle | Snapshot before held turn, after confirmed kill/exit plus settle, and after recovery. Residue counts new/changed surviving regular files relative to pre-hold snapshot using S6's identity/content rule; report all candidates, not “proven orphan” solely from a name. Resume class `preserved` requires row-52 same session/exact next committed cursor and row-53 committed-prefix integrity with no duplicate/lost event/effect; otherwise `corrupt` for trustworthy contradiction, `failed` for observed refusal/deadline. These are informational PASS findings, not FAIL. Missing collection or failed kill is ERROR. |
| Manifest / feasibility | Missing `resume`/`durable_journal` or journal locator is ABSENT; proven unavailable recovery is UNSUPPORTED. Reuse `collect_signal_matrix_case`'s held fixture/ownership and `deliver_registered_tree_signal`, plus row-52 resume and row-53 prefix checks. Row 57 today has no SIGKILL case: L6 MUST add it. Do not expect a catchable-signal terminal from SIGKILL or claim a kill occurred inside fsync. |

### S10 `resume-read-cost`

| Field | Normative contract |
|---|---|
| Type / topology / fixture | NEW; Storage; INFORMATIONAL. Separate 3/7 fresh N-turn sessions; detach without deleting. Per-invocation launches its actual resume client; daemon remains warm and measures reattach-submit, excluding daemon cold start. |
| Evidence | `s.resume_read_bytes_p50`, `s.resume_read_bytes_p95`, `s.resume_latency_p50_ms`, `s.resume_latency_p95_ms`, `s.resume_outcome`; `details.resume-read-cost.trials`; `storage-samples.jsonl`, `processes.jsonl`, `model-requests.jsonl`. |
| Oracle | Use the topology/control-path brackets below; end latency at fake provider completion of the first submitted continuation body. Hold that response until the final counter receipt, before terminal work. Reads span actual counter brackets; disclose skew and intervening background I/O, never instantaneous attribution. Summarize reads/latency across repetitions, then verify row-52 same-session/exact-cursor on the resumed suffix. Observed bad resume is class `failed` with unavailable cost, not zero or FAIL. PASS means complete observation. No latency/read threshold; cache-warm physical zero is valid with complete counters. |
| Manifest / feasibility | Missing public resume binding/capability is ABSENT; proven lack of resume/read-counter facility is UNSUPPORTED; lost boundary/counter is ERROR. Exec `Driver::resume` reloads metadata and may refresh the journal on the no-control path (`src/driver.rs:3408–3414,3443–3463`); a declared `resume_control_command` instead runs a real public command and retains its JSON evidence (`src/driver.rs:3415–3442`). Haider declares it at `adapters/haider-agent/manifest.toml:98`. L6 MUST extend macOS/Linux write-only counter APIs to reads and capture every applicable bracket, including retired control-command identities. Do not flush OS caches or call this a cold-read benchmark. |

Select timing from the validated lifecycle/topology independently of transport and control
operations. Let P be the exact `per_invocation_topology(manifest)` predicate in
`src/runner.rs:2565–2570`: `!daemon.persistent` AND topology family `PerInvocation`
(`client-process-fanout` or `worker-processes`, `src/manifest.rs:516–523`). Manifest validation
pairs nonpersistent/per-invocation and persistent/shared-controller at `src/manifest.rs:1403–1413`.
Let E mean `transport.kind="exec"` and C mean nonempty `sessions.resume_control`, bound to
`resume_control_command` for exec at `src/runner.rs:7734–7751`. Record P, transport kind and C.
For measurable paths these declarations select exactly one row below; never select by harness name.

`reattach_start_ns` is the boundary immediately before `Driver::resume`. For exec,
`continuation_start_ns` is the actual subsequent submitted client launch; submit selects
continuation/resume argv after prior turns (`src/runner.rs:7736–7741`, `src/driver.rs:3006–3010`)
and records its launch at `src/driver.rs:3067–3084`. `control_start_ns/control_end_ns` are the
actual declared control-command launch/exit when E AND C. AHRB's metadata/journal preparation
is excluded from owned-process reads by §2, even when it lies inside the elapsed interval.

| P | Transport | C | `resume_path` | `read_start_ns` | Headline `resume_start_ns` |
|---|---|---|---|---|---|
| true | exec | false | `exec-continuation` | `continuation_start_ns`, after metadata/journal preparation | `continuation_start_ns` |
| true | exec | true | `exec-control-continuation` | `control_start_ns` | `continuation_start_ns` |
| false | exec | false | `daemon-exec-continuation` | `reattach_start_ns` | `reattach_start_ns` |
| false | exec | true | `daemon-exec-control-continuation` | `reattach_start_ns` | `reattach_start_ns` |
| false | non-exec | either | `daemon-reattach` | `reattach_start_ns` | `reattach_start_ns` |

The non-exec driver binds `sessions.resume` as a protocol operation, not `sessions.resume_control`
(`src/runner.rs:7809–7833`, `src/driver.rs:1548–1551`); C alone therefore proves no control-process
execution in that row. P=true/non-exec has no completed continuation-launch receipt in the
current generic driver (`src/driver.rs:1292–1299`): establish `UNSUPPORTED: resume-boundary-unavailable`
before collection until a public driver can supply the row-52 launch boundary. Do not substitute
the reattach origin for it. A promised boundary lost after collection starts is ERROR.

For every exec-control path, retain control launch/exit and JSON receipts, complete retired
control-process counters, and continuation launch receipts. Keep one uninterrupted whole-owned-tree
read interval through control exit, continuation and the provider gate; never reset at continuation
launch or omit control reads. Control output alone is not a resumed turn. For every P=false path,
arm before resume and retain the warm controller and all owned clients/descendants together,
including clients that retire before submit; daemon cold start is outside the interval.

The bundled Haider declaration is P=false, E=true, C=true: persistent daemon at
`adapters/haider-agent/manifest.toml:51`, exec at `:87`, control at `:98`, shared-daemon topology
at `:108`. The runner builds its exec driver with a managed daemon and control command at
`src/runner.rs:7734–7751`. It therefore uses `daemon-exec-control-continuation`:
`read_start_ns=resume_start_ns=reattach_start_ns`, before control launch, with BOTH control
and continuation timestamps required. Headline latency includes the control command.

Headline timing preserves row 52's actual P-based selection: `src/runner.rs:15142–15163`
records the pre-resume boundary and selects continuation launch only when P=true; provider
receipt and latency are at `15188–15203`. Report diagnostic
`total_resume_latency_ms=(first_request_ns-read_start_ns)/1e6`. Only P=true AND E AND C has
continuation-only headline latency paired with control-plus-continuation total latency/read
cost; label that distinction explicitly. For every P=false path, headline and total latency
share the pre-resume origin, including exec-control; label them reattach-through-provider,
including control-plus-continuation when present. For P=true exec without control they share
the continuation origin. `start_skew_ns=read_start_ns-counter_start_ns` and
`end_skew_ns=counter_end_ns-first_request_ns` are non-negative; the counter bracket MUST
enclose those boundaries. Record intervening owned background I/O; snapshot settling and
post-gate continuation/terminal reads are outside this bracket. Missing any applicable
control/continuation/retirement boundary is ERROR, not a partial cost estimate.

## 5. Typed reports, bundle, badge, and history

Storage emits `report.schema=4`, `report.spec_version=4`, `pillar="storage"`; existing pillar
schemas retain their meanings. `results[]` has ten rows with numeric `row=1..10`, displayed
as S1..S10, the exact slug `id` above, `pillar="storage"`, v2 outcome/evidence fields, and
the specified requirement. Join by `(pillar,id)`, never numeric row alone. S6 is informational
when neither verb exists. Scores and `reference_envelope_pass` are null: storage uses classes.
Do not manufacture matrix outcomes, economy/fidelity summaries, or an Automation Ready badge.

`storage_summary` is a typed object with **only** the following schema-1 fields. All scalar
measurement/class fields in the last ten lines are nullable when their source is unavailable;
null reasons live in the corresponding details trial. A legitimate zero follows a complete audit.

| Exact fields | Types / meaning |
|---|---|
| `schema`, `task`, `profile`, `os`, `topology`, `comparison_scope` | u32=1; strings as §1. |
| `turn_budget`, `repetitions`, `completed_turns`, `physical_requests` | u32, u32, u64, u64; completed_turns/physical_requests total the shared S1/S3 task only. |
| `measurement_label`, `counter_source`, `allocation_source` | Strings: OS-accounted I/O versus allocated footprint; backend name or `unavailable`; `stat-st_blocks-512` or `unavailable`. |
| `declarations_sha256` | String: SHA-256 of sorted canonical JSON `{areas,retention_cap_bytes,auxiliary_cap_bytes,sweep_interval_s}` from the parsed storage declaration; absent maps normalize to `{}`, absent scalars to null. Pins comparison policy without needing a live manifest. |
| `write_bytes_per_turn_p50`, `write_bytes_per_turn_p95`, `write_bytes_per_turn_max`, `logical_growth_bytes_per_turn`, `net_growth_bytes_per_turn`, `write_amplification_ratio`, `disk_class` | Six f64?, string?; S1. |
| `fsync_calls_per_turn`, `fdatasync_calls_per_turn`, `fullfsync_calls_per_turn`, `durability_calls_per_turn`, `assumed_fsync_cost_ms`, `estimated_durability_wall_ms_per_turn`, `durability_class` | Six f64?, string?; S2; assumed cost is always 4 ms, even if observation unavailable. |
| `footprint_curve`, `first_turn_allocated_bytes`, `footprint_slope_bytes_per_turn`, `growth_class` | Array of §S3 entries (empty if unavailable); f64?, f64?, enum? `bounded/linear/superlinear`. |
| `compaction_before_allocated_bytes`, `compaction_after_allocated_bytes`, `compaction_freed_pct` | f64? medians; S4. |
| `closed_sessions`, `close_retained_bytes_per_session`, `close_retained_after_sweep_bytes_per_session`, `retention_cap_bytes`, `close_retention_class` | u64?, f64?, f64?, u64?, enum? `bounded/unbounded`; S5; closed count summed, retained ratios median. |
| `delete_residue_allocated_bytes`, `delete_residue_files`, `uninstall_residue_allocated_bytes`, `uninstall_residue_files` | u64? maxima per operation; S6. |
| `auxiliaries` | Array of §S7 objects; name string, declared/rotation bool, cap u64?, peak/final u64, slope f64, class enum?; empty if audit unavailable. |
| `request_retention_class`, `stored_request_bytes`, `unique_request_content_bytes`, `stored_unique_ratio` | enum? `none/deduplicated/full`, u64?, u64?, f64?; S8 byte totals sum across reps, ratio is summed S / summed U. |
| `crash_residue_allocated_bytes`, `crash_residue_files`, `crash_resume_outcome` | u64? maxima; enum? `preserved/corrupt/failed`; worst outcome across S9 trials (failed worse than corrupt). |
| `resume_read_bytes_p50`, `resume_read_bytes_p95`, `resume_latency_p50_ms`, `resume_latency_p95_ms`, `resume_outcome` | Four f64?; enum? `preserved/failed`, failed if any S10 trial fails. |

`details.<slug>` has `{measurement_label,reason,trials}`; reason is string or null and trials
are ordered repetition records containing `{repetition,outcome,measurement_complete,reason,
summary,diagnostics,evidence_refs}`. `summary` is the typed subset owned by that row before
aggregation. Exact `diagnostics` keys follow; bytes/counts/ns are u64, signed deltas i64, slopes/ratios/ms/pct f64, names/hashes strings, flags bool, and unavailable scalars null.

| Row | `trials[].diagnostics` exact fields |
|---|---|
| S1 | `physical_write_bytes,logical_growth_bytes,net_growth_bytes,amplification_reason,counter_complete,identities`; identity receipts: `{pid,start_time,source,first_bytes,last_bytes,retirement_method,complete}`. |
| S2 | `failed_calls,primitive_applicability,instrumentation_ref`; applicability maps each primitive to `measured/not-applicable`, refs index `instrumentation`. |
| S3 | `checkpoints,early_bytes_per_turn,late_bytes_per_turn,late_early_ratio,late_range_bytes`; checkpoints: `{turn,allocated_bytes}`. |
| S4 | `before_boundary,after_boundary,compaction_request_sha256,recovery_outcome,accepted_input_tokens,accepted_body_bytes`; recovery outcome `compacted/not-compacted`. |
| S5 | `store_baseline_allocated_bytes,checkpoints,close_receipts`; checkpoints: `{closed_sessions,elapsed_s,phase,retained_allocated_bytes}`, phase `immediate/post-sweep`; close receipts: `{session_id_hash,exit_code,receipt_sha256}`. |
| S6 | `operation_refs`; indices into `operations`, defined below. |
| S7 | `checkpoints`; entries: `{turn,family,allocated_bytes,identity_replacements}`. |
| S8 | `coverage,body_blobs,baseline_exclusions,match_refs`; coverage `complete/representation-limited/partial/capture-error`; blobs: `{semantic_ordinal,attempt,role,raw_sha256,canonical_sha256,path}`; exclusions: `{path,offset_bytes,length_bytes,sha256}`; refs index `matches`. |
| S9 | `hold_ns,kill_ns,exit_ns,committed_cursor,committed_prefix_sha256,resumed_session_id_hash,expected_session_id_hash,resumed_cursor,expected_cursor,lost_events,duplicate_events,duplicate_effects,residue_paths`; paths string array. |
| S10 | `per_invocation_topology,transport_kind,resume_control_declared,resume_path,reattach_start_ns,read_start_ns,control_start_ns,control_end_ns,continuation_start_ns,counter_start_ns,resume_start_ns,first_request_ns,counter_end_ns,start_skew_ns,end_skew_ns,total_resume_latency_ms,first_read_bytes,last_read_bytes,session_id_hash,expected_session_id_hash,cursor,expected_cursor,identities`; first/third fields are bool P/C, transport_kind is the manifest transport enum string, and resume_path is exactly one of S10's five table values (null for an unavailable path with reason). On complete trials reattach_start_ns is required for every path; continuation_start_ns is required iff E, otherwise null; control_start_ns/control_end_ns are required iff E AND C (both exec-control paths, including P=false), otherwise null. Required but unavailable timestamps remain null with reason and measurement_complete=false; a lost required receipt is ERROR, never an inapplicable field. Identity receipts use S1's type for read bytes; evidence_refs bind control JSON/launch/exit and continuation launch receipts to owned identities, retaining retired control counters and the warm daemon together. |

`operations[]` is `{repetition,operation,declared,scope,baseline_boundary,after_boundary,exit_code,
receipt_sha256,outcome,reason,residue_paths,whole_root_residue_allocated_bytes,whole_root_residue_files}`;
operation is `session-delete/uninstall-cleanup`, declared is bool, scope and residue_paths are relative-path arrays.
Declared-operation records have a repetition ordinal and feed S6's trials/row aggregation.
Each missing verb instead has exactly one `declared=false,outcome=UNSUPPORTED` receipt with
its missing-operation reason, repetition/boundaries/exit_code/receipt_sha256/residue counts
null, and empty scope/residue_paths arrays. These receipts are retained in operations but
excluded from trials' operation_refs and from declared-operation aggregates; they are not
measured zero residue. Both absent receipts produce the no-declared-operation row case in §S6.
`matches[]` uses the match JSONL record type below. `instrumentation[]` is
`{repetition,backend,version,executable_sha256,shim_sha256,environment_keys,images,drop_count,reason}`;
images are `{pid,start_time,executable_sha256,load_verified,self_test_verified,exit_seen}`.
Shim hash is nullable for tracing; verified loader refusal uses false flags and a reason.
L2 MUST validate these typed records; consuming lanes supply them before claiming complete evidence.
`evidence_refs[]` is `{file,sha256,first_record,last_record}`: bundle-relative file, content hash, and inclusive one-based JSONL record bounds (both null for a whole-file receipt).
Metrics mirrors include only finite non-null top-level numeric measurement fields from the ten
row lines, exactly `storage.<field>`; no aliases, bools, classes, or null-as-zero. Their comparison scope comes from storage_summary even when the badge is absent.

Required JSONL schemas (u64 counts/timestamps/bytes unless signed/f64 or nullable below):

| File | Exact record keys |
|---|---|
| `storage-samples.jsonl` | `row_id,repetition,session_ordinal,turn,boundary,monotonic_ns,settle_ms,sync_start_ns,sync_end_ns,allocated_bytes,apparent_bytes,regular_files,families,physical_write_bytes,physical_read_bytes,counter_source,counter_complete` — byte counters nullable; `families` maps name to allocated bytes; row/boundary/source strings, complete bool. |
| `storage-files.jsonl` | `row_id,repetition,boundary,path,kind,device_id,inode_or_file_id,allocated_bytes,apparent_bytes,sha256,family` — root-relative path; SHA-256 null for nonregular entries; kind/family strings. |
| `fsync-events.jsonl` | `repetition,turn,pid,start_time,sequence,primitive,enter_ns,exit_ns,return_code,errno,backend,self_test` — primitive `fsync/fdatasync/fullfsync`; return/errno signed integers; self_test bool. |
| `request-body-matches.jsonl` | `repetition,representation,request_sha256,block_sha256,path,device_id,inode_or_file_id,offset_bytes,length_bytes,excluded_baseline` — either digest nullable when inapplicable; representation string, exclusion bool. |

Retain existing `events.jsonl`, `turns.jsonl`, `model-requests.jsonl`, `samples.jsonl`,
`processes.jsonl`, and `membership.jsonl`, plus lossless captured bodies under private
`request-bodies/<sha256>.bin` with an explicit request-to-blob mapping in S8 trials.
All paths are bundle-relative; evidence refs bind file SHA-256s and record ranges. Emit an
empty file plus typed unsupported reason for a collector not run; empty is never zero events
without coverage proof. Preserve partial raw records on error but null incomplete aggregates.
No real account profiles, credential/signing material, raw bodies, or private transcripts go in Git.
`report.md` adds “Storage v4”, rows S1–S10, allocated/physical columns, D/G/facets, checkpoint
curve, family/cap and residue tables, S2's explicit estimate label, S8's byte-retention limitation,
unsupported/error reasons and evidence links. Render null as unavailable, never `0`.

The storage badge is a separate typed variant `{spec_version:4,os,topology,profile, comparison_scope,disk_class,growth_class,facets,label}`. Its exact label is
`Storage v4 · <os> · <topology> · D<class> · G<bounded|linear|superlinear> · <facets>`.
`disk_class` stores `64/256/1024/4096/4096+` (prefix D only in display); durability_class
stores `0/1/10/10+` (prefix F in display). Facets in fixed order are `F<class>` when S2 measured, `close-bounded` when S5 bounded, `delete-clean`, `uninstall-clean` for measured
clean declared S6 operations, `aux-bounded` when all observed families have passing caps,
`requests-<class>` for an S8 headline class, and `crash-preserved` when S9 preserved.
Join facets with `+`; omit the final separator if none. Profile is required adjacent report
metadata although absent from the mandated badge text. No cross-profile comparison follows.
Withhold badge on any ERROR, on S1 missing D, on S3 other than PASS, or on any declared S6 operation other than PASS. Informational ABSENT/UNSUPPORTED does not imply a facet. S3/S6
are the only behavioral FAILs; missing evidence can still withhold a badge without a FAIL.
Exit codes match existing runner/evaluator boundaries: 0 for completed runs without FAIL/ERROR
(including ABSENT/UNSUPPORTED); 1 for completed reports containing core FAIL or row ERROR.
S6 declared residue MUST reach `results[]` as conditional-core FAIL (or ERROR if a declared
operation errors), even with missing-verb receipts, and therefore returns 1 on a completed run.
The reused `src/evaluate.rs:546–561` inspects actual row FAIL/ERROR, not details or badge
eligibility; L2 MUST resolve storage requirements by `(pillar,id)` rather than the current
matrix-only numeric-row lookup at lines 553–557. Neither hidden operation FAIL nor a missing
badge substitutes for the S6 row outcome. Usage errors or aborted infrastructure/run-deadline
errors return 2; §1 requires full interrupted reports, diagnostics and partial evidence on
deadline expiry. A completed-report row ERROR does not become exit 2 merely by its name.

Saved `index.jsonl` advances line schema to 3, retains every existing `IndexEntry` field, and adds `pillar`, `storage_summary` (the complete typed object or null), `badge_label`
(string/null), and `outcome_counts` (`PASS/FAIL/UNSUPPORTED/ERROR`, with ABSENT in ERROR,
as current history counting). Storage entry spec_version is 4; no-save creates no index line.
Legacy schema-2 lines remain readable; unavailable storage is null, never reconstructed
from row 47. `hbench results [HARNESS] [--all]` displays pillar, D/G, outcome counts and badge
from persisted data. `hbench diff` emits `storage_summary_deltas` iff either input has storage:
scalar differences are right minus left; enums show transitions; curves/families join exact
turn/name keys with unmatched values unavailable. Numeric deltas require equal storage schema,
task, profile, OS, topology, allocation/counter source, and declared caps/area definitions. Otherwise report `not-comparable-storage-scope`; missing block reports `unavailable`.
Compare declared areas/caps through `declarations_sha256`; unavailable hashes prohibit numeric deltas.
Only S3/S6 PASS→FAIL is a storage behavioral regression; informational class shifts are changes.
`diff --latest HARNESS` must select the latest comparable pair within one pillar; if latest
occurrences span pillars, require explicit run paths instead of comparing storage with matrix.

## 6. Implementer evidence and lane completion

Every row needs positive and adverse mock CLI/report bundles for both mock transports, each
bound to candidate hashes and actual commands/exit codes. The following `AHRB_MOCK_STORAGE_*`
knobs are NEW requirements for the named lane, not existing flags. Defaults must preserve the existing mock matrix. Knobs change the mock's behavior, never the evaluator's verdict.

| Row / lane | Required positive and negative evidence |
|---|---|
| S1 / L2 | `WRITE_MODE=append` vs `rewrite`; physical counter/allocated separation, sparse file and clone cases, same-identity truncate and replacement reset; deliberately missing retirement receipt → ERROR, never low D. |
| S3 / L2 | `GROWTH=bounded`, `linear`, `quadratic`; last must cross the exact shape threshold and FAIL. Boundary-equality and missing-checkpoint cases exercise the classifier and ERROR. |
| S7 / L3 | `AUX_MODE=capped` vs `grow`; rotated siblings remain in cap accounting; missing cap → unsupported assessment, failed traversal → ERROR. |
| S8 / L3 | `REQUEST_RETENTION=none`, `deduplicated`, `full`, `partial`, `opaque`; verify byte matches/ratios and unsupported partial/opaque cases. `UNRELATED_GROWTH=1` must not turn none into full. Missing body capture → ERROR. |
| S5 / L4 | `CLOSE_RETENTION=capped` vs `grow`, `SWEEP=on` vs `off`; record before/after declared interval, cap breach remains informational, missing close/store evidence follows S5. |
| S4 / L4 | Existing context-limit fixture plus `COMPACTION_DISK=reclaim` vs `append`; both complete with measured signed percentage, append has no invented reclamation. Missing accepted compacted candidate cannot become zero-percent PASS. |
| S2 / L5 | `FSYNC_MODE=per-turn` vs `extra`; independent known-call control exercises all available primitives/failed calls; denied injection/tracing → UNSUPPORTED with proof, lost trace records → ERROR. Do not fake OS receipts with a mock knob. |
| S6 / L6 | `DELETE_MODE=clean` vs `residue`, `UNINSTALL_MODE=clean` vs `residue`; successful verb leaving even an empty regular file FAILs. Missing verb → separate UNSUPPORTED receipt; test one-declared-clean → row PASS/exit 0, one-declared-residue → row FAIL/exit 1, both-missing → row UNSUPPORTED/exit 0, and declared ABSENT/ERROR precedence; nonzero command → ERROR; escaping argv/globs rejected before execution. |
| S9 / L6 | `CRASH_RECOVERY=preserve` vs `corrupt`, `CRASH_TEMP=clean` vs `leave`; demonstrate SIGKILL receipt, preserved or corrupt committed cursor/prefix, and observed residue without claiming every file is an orphan. |
| S10 / L6 | `RESUME_READ=normal` vs `extra`, `RESUME_ID=preserve` vs `wrong`; cover all five timing-table paths: nonpersistent exec with/without control, persistent shared-daemon exec with/without control, and native daemon reattach. Verify P-based headline origins, uninterrupted read gates, applicable/null timestamps, complete control retirement, total/headline latency and identity oracle. Persistent exec-control MUST include pre-control warm-daemon reads plus control/continuation receipts and headline time before resume; nonpersistent exec-control MUST retain its distinct headline/total origins. Missing any applicable control/continuation/retirement receipt → ERROR; non-exec control declarations alone must not fabricate control timestamps, and unavailable per-invocation launch boundaries need preflight UNSUPPORTED evidence. Extra reads need not imply a minimum physical delta under warm caches; unavailable/read-loss cases must be honest. |

Implementing lanes run focused unit/fixture regressions plus real `ahrb run --pillar storage`
and `hbench storage` commands on fresh absolute outputs; inspect JSON, Markdown, JSONL, badge,
null handling, and exit codes. L2 first proves parser isolation from matrix/economy/fidelity, storage default/flag/environment
deadline precedence, serialized budget arithmetic, and insufficient/zero-deadline interrupted
reports with pending ERROR rows and exit 2;
L7 verifies `hbench results`, same-scope/legacy/mismatched `diff`, auto-save and no-save. A separate GPT6-Astra verifier must review the actual candidate and exercise each affected
row against at least one installed real adapter in both command forms, preserving honest unsupported evidence if needed; mocks alone do not satisfy real-adapter verification.
OS-specific collectors require platform evidence or an explicit outstanding platform check,
never fabricated parity. Use available serialized build capacity; a missing test cannot be SHIP.
L1 is documentation-only: no Cargo build/test or claim that future storage commands ran.
Its separate verifier checks the actual document, links, code citations and contract consistency.

| Lane | Dependency / implementation wave |
|---|---|
| L2 | L1 → plumbing, typed manifest/report/evidence, S1 + S3; W1. |
| L3 | L2 → S7 + S8; W1. |
| L4 | L2 → S5 + S4; W1 (S4 explicitly included). |
| L5 | L2 → S2 instrumentation; W2. |
| L6 | L2 + L4 → S6 + S9 + S10; W2. |
| L7 | L3–L6 → six adapter declarations, docs, saved index/diff, full mock/CLI regression. |
| L8 | L7 → quiet-machine references after mock self-certification and adapter doctors. |

Feasibility is based on source at `9d9b5c2`: `cli.rs`/`hbench.rs` currently dispatch only matrix/economy/fidelity; `runner.rs` owns the cited drivers; `evidence_collectors.rs` and
`matrix_evidence.rs` separate typed observations from strict judgements and contain no
storage collector. Disk tracking actually lives in `process/{mod,macos,linux}.rs`; snapshots in `runner.rs`/`report.rs` currently record size/identity/digest, not allocated blocks.
`report::Report` needs the new typed summary/badge; `results::IndexEntry` currently has no
pillar or isolated-pillar summary. These are implementing requirements, not claims of reuse
without changes. L7 covers exactly `adapters/{claude-code,codex,haider-agent,opencode,pi,rick}`;
other placeholder adapters are outside the six-harness declaration scope. Existing public
manifests omit storage areas/caps/verbs; Pi uses `--no-session`, Rick has no declared resume,
and Haider's control resume declaration does not by itself prove repeated persisted turns.
Verify installed public CLI behavior before adding declarations; do not branch on harness names.

**Never measured rule:** this specification contains no expected real-harness values or measured
reference numbers. Fixture constants, class boundaries, and adapter caps are contracts only.
L8's first serial six-harness storage run establishes the references, including Haider, with
binary versions/hashes, task/manifest hashes, topology/OS/profile, and unsupported coverage.
Publish only reviewed aggregate tables; keep raw profiles, request bodies, and evidence outside Git.

## Appendix A. Expected shape (non-normative)

For a journal that only appends and never reclaims records during compaction, the hypothesis is
zero percent reclaimed journal allocation. Concurrent metadata/log writes can make total disk
grow. Measure S4's signed percentage; this hypothesis supplies no expected real-harness value.
