# AHRB v2 — Agent Harness Readiness Benchmark specification

Status: implementation specification, **revision 2.6 (2026-09-05)**.
`docs/SPEC.md` remains the authoritative v1 specification; this document defines the
additive v2 contract. Revision 2.1 is a normative amendment: it does not renumber a row
or change `spec_version = 2`, but implementations claiming v2 MUST implement this
revision.

Revision 2.2 incorporates the Wave-2 independent re-verification corrections for rows
47, 48, and 56–62 without changing row IDs or `spec_version`.

Revision 2.3 incorporates the Wave-3 independent re-verification corrections for rows
49–55 without changing row IDs or `spec_version`: a topology-neutral long-session
latency-slope allowance, typed profile-contained session-store roots and identity-safe
traversal, exact fake-provider context counting and recovery boundaries, null resume-
ratio handling, exact torn-tail truncation targets, aggregate-then-cliff fanout
evaluation, censored fairness latency, and a positive monotonic-clock resolution
requirement.

Revision 2.4 incorporates the Wave-4 independent re-verification corrections for rows
66–72 without changing row IDs or `spec_version`: exact budget-overrun counting,
typed usage carrier/scope selection, nonempty committed session seeds and
repetition-scoped lifecycle records, exact event-component arithmetic and permission
trial counts, nonempty credential-carrier declarations, and non-vacuous tool-role check
counts.

Revision 2.5 is the observability version bump from revision 2.4. It adds a seventh,
model-narrative component to row 69, makes row 69 CORE, and adds CORE row 73 for
context-compaction transparency. The report continues to use `spec_version = 2` and
schema 3 because this is an additive v2 revision: old reports remain readable, while
their missing row/component is unavailable rather than synthesized. The bar moved
because tool-only metadata cannot explain whether a failing model turn hallucinated or
lost relevant context, and silent compaction hid exactly that distinction.

Revision 2.6 wires row-69 narrative declarations to captured real-adapter evidence and
permits an adapter to declare only the assistant-text or reasoning side it actually
exposes. Such a partial declaration makes the row measurable, but it does not weaken the
oracle: when the fixture emits both sides, the undeclared/missing side keeps narrative
reconstructability at zero and the CORE row fails. A block with neither side is invalid;
omitting the entire block remains `UNSUPPORTED`. `ahrb replay --manifest MANIFEST
--input REPORT` re-evaluates row 69 from a saved report without starting the harness.

### Revision 2.1 changelog

- Added one authoritative result-state precedence, repetition, aggregation, and
  missing-evidence contract for rows 42–73.
- Made rows 50, 51, 66, and 67 independently certifiable and corrected the row 46,
  50, 53, 56, and 69 implementation-path descriptions.
- Reconciled numeric resource mirrors with typed `resource_summary` and `details`
  values, and moved schema-2 manifest parsing/typed blocks to their first consumers.
- Versioned `results/index.jsonl`, specified legacy-line migration, corrected `diff`
  compatibility/profile rules, and fixed the informational transition example.
- Locked the row-specific clarifications recorded in the row tables below, including
  rows 42–49 and 52–73.

## 1. Scope and compatibility

AHRB v2 revision 2.5 contains **73 matrix rows**: v1 rows 1–41, unchanged, plus rows 42–73
defined here. It also adds the `hbench diff` command and a topology-scoped automation
score with latency and CPU badge classes. The implementation is split into four ordered
waves so that each wave can be implemented and independently verified before the next
one starts.

### Rows 1–41 are unchanged from v1

The IDs, pillars, fixtures, outcomes, thresholds, topology policy, capability policy,
badge role, and evidence meaning of rows 1–41 are **normatively unchanged** from
`docs/SPEC.md`. A v2 implementation may add fields to their evidence records but MUST
NOT weaken, renumber, reinterpret, or silently auto-pass any v1 oracle. In particular,
the v1 41-row results remain independently addressable in `report.json`, and a v2 badge
cannot be awarded unless every v1 CORE requirement that applied in v1 still passes.

Old reports remain readable. A missing v2 field in an old report is `unavailable`, not
zero and not a regression. `hbench diff` joins rows by stable ID and uses row number only
as display metadata.

### Normative terms

- **CHEAP**: no new harness stimulus, provider fault, tool fixture, or OS isolation
  mechanism is required. The result is an aggregation of an observation point v1
  already has. A few CHEAP rows require persisting a boundary that v1 observes but
  currently discards; this does not make the harness execute a new path. Existing v1
  bundles cannot be retroactively populated when that boundary was not serialized.
- **NEW**: a new fake-model behavior, filesystem/tool fixture, process operation,
  confinement mechanism, or workload is required.
- **Per-invocation**: topology family `client-process-fanout` or `worker-processes` with
  `daemon.persistent = false`.
- **Daemon**: topology family `shared-daemon-sessions` or `native-sibling-fanout` with
  `daemon.persistent = true`.
- **CORE**: must be `PASS` for a v2 badge. Missing required declaration/evidence is
  `ABSENT` or `ERROR`, never an invented `PASS`.
- **OPTIONAL FACET**: an undeclared capability is `UNSUPPORTED` and is non-gating. Once
  declared, its row must pass and its suffix is shown on the badge.
- **INFORMATIONAL**: the row has a reproducible reference-envelope boolean and may show
  `PASS` or `FAIL`, but it does not gate the badge or the default suite exit status.
  An infrastructure `ERROR` still makes the suite exit nonzero because no trustworthy
  measurement was produced.

`UNSUPPORTED` means a capability or architectural operation is honestly unavailable;
it never means the harness tried and failed. For an undeclared OPTIONAL capability it is
non-gating. For a CORE operation such as resume, durable journal, or required width it is
badge-blocking, as in v1. A required manifest declaration that is simply missing is
`ABSENT`. Topology by itself never turns an otherwise measurable row into
`UNSUPPORTED`. An inapplicable subcase is `not_applicable`, omitted from the row's
conjunction, and never counted as a successful observation. No complete v2 row is
auto-PASS solely because of topology, and an unavailable capability is never `FAIL`.

## 2. Design rules

1. Measure out of band. AHRB may timestamp the runner, fake provider, process sampler,
   filesystem snapshots, and supervisor operations; it MUST NOT add code, tracing calls,
   preload libraries, or callbacks to the harness's model/tool turn path.
2. The fake model, AHRB runner, deny-egress service, and sampler are outside the owned
   harness tree. Sampler CPU and wall overhead remain reported. Resource rows are
   `ERROR: sampler overload` if sampler CPU exceeds 10% of one core or cadence coverage
   is untrustworthy.
3. Whole-tree ownership is still `(PID,start-time)`, never PID alone. Reparented workers
   remain owned by cgroup/process-group/verified isolated-root evidence.
4. Fake responses, fault schedules, IDs, usage, AHRB-owned sweep seeds, fixture bytes,
   and canonical serialization are deterministic. Harness-owned retry jitter is measured
   rather than seeded. Measurements are ordered by semantic
   actor/checkpoint, not arrival order.
5. The adapter is data. Harness-specific branching in the runner is forbidden; all
   operations, pointers, paths, limits, and capability claims come from the manifest.
6. There is no cross-topology, cross-OS, or cross-profile resource ranking. Every
   resource delta, class, and automation score carries `topology`, `profile`, and
   `comparison_scope = "within-topology-only"`. A quick result is never resource-
   comparable with a cert result.
7. A missing observation is `ERROR` or `ABSENT`, never a suspiciously favorable zero.
   An old-schema report uses JSON `null`/`unavailable` in a diff.
8. Each scenario runs in a fresh isolated profile unless the row explicitly requires a
   persistent session. Quick uses 3 repetitions and cert uses 7 unless a row overrides
   the count. Medians are the headline, MAD is median absolute deviation, and p95 is
   nearest-rank `ceil(0.95*n)` after numeric sorting.
9. Linear growth uses Theil–Sen unless a row says otherwise. Ratios with a zero
   denominator are `null` and cannot pass a ratio oracle. Memory integration uses the
   trapezoidal rule over monotonic sample times and effective memory (macOS footprint;
   Linux PSS, falling back to RSS with reduced-confidence evidence).
10. Deadlines have two levels: the harness's declared idle/turn deadline and AHRB's
    strictly later outer deadline. Use of the outer kill is evidence of failure whenever
    the row requires harness-owned terminalization.
11. Captured secrets are redacted only after exact leak scanning. Redaction MUST NOT hide
    a row-71 occurrence. Raw evidence is bounded and deterministically truncated.
12. All report maps and lists have stable ordering. Timestamps live in evidence/index
    records and do not change fake response bytes or canonical request hashes.

## 3. Shared v2 profile and oracle conventions

Unless overridden below:

| Parameter | Quick | Cert |
|---|---:|---:|
| repetitions | 3 | 7 |
| warm-up per fresh profile | 1 unmeasured turn | 1 unmeasured turn |
| ordinary turn deadline | manifest `resources.turn_timeout_ms` | same |
| outer deadline | harness deadline + 3,000 ms | harness deadline + 3,000 ms |
| process residue audit | 2,000 ms after terminal/exit | 2,000 ms after terminal/exit |
| filesystem snapshot roots | all isolated roots plus declared log/journal paths | same |
| sampler cadence | v1 platform cadence | v1 platform cadence |

For an INFORMATIONAL row, “oracle PASS” below means its reference-envelope result.
Every `results[]` row also records the exact boolean `measurement_complete`; incomplete evidence is `ERROR`, not an
envelope failure. INFORMATIONAL failures do not suppress a badge; their class/score is
still printed.

### Authoritative outcome precedence and scored state

This table is authoritative for every v2 row. Apply the first matching condition from
top to bottom; a row-specific table may identify which declaration is required or which
operation is architecturally optional, but may not change this precedence.

| Precedence | Condition | Row outcome | `measurement_complete` | Row score / badge effect |
|---:|---|---|---:|---|
| 1 | Infrastructure or measurement is untrustworthy: missing/invalid external boundaries, bad sampler coverage, ineffective fault injection, missing required evidence after execution began, outer collector failure, or row 62 lacks a reviewed same-confinement OS or owned-boundary proof | `ERROR` | false | score `null`; suite exits nonzero; badge unavailable |
| 2 | A required manifest declaration is absent | `ABSENT` | false | score `null`; badge-blocking, but not an observed behavior failure |
| 3 | A capability/operation is explicitly and honestly architecturally unavailable where the row permits that state | `UNSUPPORTED` | true only when the unavailability itself is verified | optional rows use the stated zero; CORE rows are badge-blocking; never convert this state to `FAIL` |
| 4 | The behavior was attempted with trustworthy evidence and violated an oracle, including a timeout handled by the harness or an unclassified/contradictory event | `FAIL` | true | deterministic row score from the evidence, or zero when the row is boolean |
| 5 | Every required repetition and conjunct passed | `PASS` | true | deterministic row score from the evidence |

`not_applicable` applies only to a named subcase and is omitted from both numerator and
denominator. It is not a row outcome. `reference_envelope_pass` is non-null only for an
INFORMATIONAL row whose measurement is complete; it is false on an envelope miss. A
row outcome of `ERROR` or `ABSENT` always has `score:null`. An explicitly undeclared
OPTIONAL facet is the sole routine `UNSUPPORTED` case and has its row-defined zero score.

### Authoritative row execution and aggregation table

“All” means every required repetition/trial must satisfy the row's behavioral oracle;
a favorable aggregate never hides one failed required repetition. Unless a row below
says otherwise, numeric headline fields are the median of the named per-repetition
statistic, integer counters are sums, maxima are maxima, and evidence absent after the
fixture starts is `ERROR` under the precedence table.

| Rows | Repetitions and aggregation | Declaration/unavailability rule |
|---|---|---|
| 42 | Exactly 2 fresh-profile repetitions in quick and cert. Sum request/count numerators and completed-turn denominators; pool body/context observations in semantic-key order; report the median of the two per-repetition slopes and require both repetitions to pass. | Missing role evidence is `ERROR`; a present `unknown-side-channel` is a measured `FAIL`. |
| 43 | 3 quick / 7 cert. Compute p50, p95, MAD, and jitter per repetition; headline is their median, while `max` is the maximum interval over all repetitions. All repetitions must meet the measurement and envelope rules. | A missing external boundary is `ERROR`; a measured deadline/envelope miss is informational `FAIL`. |
| 44 | The same exactly 2 repetitions and turn sequence as row 42. Residue fields are the maximum over all audits; churn distribution fields are pooled in semantic turn order. | Any missing audit is `ERROR`; only residue conjuncts gate this CORE row. |
| 45 | 3 quick / 7 cert one-turn fresh profiles. The headline distribution is over the repetition latencies. | A missing launch/request pair is `ERROR`; a measured envelope miss is informational `FAIL`. |
| 46 | 3 quick / 7 cert continuous 20/100-turn collectors. Per-turn integrals/CPU are pooled after each repetition is validated; coverage is the minimum repetition coverage and max gap is the maximum. | Any unbracketed turn, invalid counter, or bad coverage is `ERROR`; a complete resource-envelope miss is informational `FAIL`. |
| 47 | 3 quick / 7 cert, 20/100 turns each. Validate counter completeness per repetition, then take medians of per-repetition quantiles/slopes and the global maximum where named. | Incomplete live/retired accounting, lack of terminal-before-reap or durable cgroup accounting for an exited identity, missing file identity, or an omitted (not explicitly empty) log declaration is `ERROR`. An explicitly empty log declaration is distinct from omission and may yield verified-no-log evidence, never a synthesized numeric zero. |
| 48 | 3 quick / 7 cert independent trials. Summary CPU/wall are medians and the ratio/gap are maxima; every trial must complete without outer kill. | Any missing scheduled frame/boundary, invalid row-local deadline, or AHRB frame pacing outside its own 1,250 ms envelope is `ERROR`; observed harness busy-wait or terminal violation is `FAIL`. |
| 49 | 3 quick / 7 cert growing sessions. Compute each slope/decile ratio per repetition, report their medians, and require every repetition to pass. | Missing turn boundaries are `ERROR`; measured growth beyond the bounds is `FAIL`. |
| 50 | 3 quick / 7 cert sweeps of N=20/200. Report median slopes and maximum post-reclaim residue; every official close/delete and every sweep must pass. | Missing `sessions.close_delete` declaration is `ABSENT`; an explicitly unavailable required lifecycle is badge-blocking `UNSUPPORTED`; missing storage/process audits are `ERROR`. |
| 51 | 3 quick / 7 cert independent growing sessions. All retained-history checks pass per repetition and the normalized compacted stream must be identical across repetitions. | Missing context-recovery declaration is `ABSENT`; explicit architectural unavailability is badge-blocking `UNSUPPORTED`; incomplete accepted-request evidence is `ERROR`. |
| 52 | 3 quick / 7 cert fresh sessions per length. Aggregate at each length first; headline p50/p95 are the longest-length distribution and slope is Theil–Sen over per-length medians. Every trial preserves identity/cursor. | Missing required resume declaration is `ABSENT`; explicitly unavailable resume is badge-blocking `UNSUPPORTED`; absent external timers are `ERROR`. |
| 53 | Exactly 5 quick / 25 cert cut trials; this overrides 3/7. Cuts rotate through the five specified positions. Counters are sums and `recovery_p95_ms` is the nearest-rank p95 over all 5/25 trial recovery durations. Any bad trial fails. | Missing required journal declaration is `ABSENT`; explicitly unavailable journal is badge-blocking `UNSUPPORTED`; no observed growth or failed injection is `ERROR`. |
| 54 | 3 quick / 7 cert trials at every required N; aggregate each N by median before cliff tests. Any missing N/trial is `ERROR`. | Missing width declaration is `ABSENT`; explicitly lower architectural maximum is badge-blocking `UNSUPPORTED`. |
| 55 | Inherit every row-54 trial. Headline CV/spread are maxima of group values, nullable ratio is the maximum non-null value, and starved count is summed. | Missing release clock is `ERROR`; a scheduled actor lacking a terminal by deadline is measured starvation and `FAIL`. |
| 56 | 1 quick / 3 cert fresh crash+hang pairs. Latency fields are maxima; terminal/residue/kill counters are sums; every pair passes. The hang global deadline is anchored at the parent public-operation start, not response headers. | Undeclared optional capability is `UNSUPPORTED` score 0; missing declared operation is `ABSENT`; missing public-operation timestamps/events are `ERROR`. |
| 57 | Exactly 1 quick / 3 cert trials per applicable case. Latency/residue fields are maxima and case counts are sums. | Signal delivery uses the generic owned-tree supervisor and has no manifest operation whose absence can be `ABSENT`. A missing typed stdin declaration is `ABSENT`; only proven non-control stdin is `not_applicable`; unresolved tree ownership, failed signal delivery/EOF close, or a missing origin/terminal boundary is `ERROR`. |
| 58 | Exactly 3 repetitions per status in both profiles, overriding cert's 7. Bounds apply per trial; headline requests/elapsed are maxima, terminals/effects are sums, and jitter is true if any eligible interval differs. `elapsed_ms` is first faulting-request receipt through structured-terminal receipt. | Missing retry declaration is `ABSENT`; failed timer calibration or incomplete post-terminal window is `ERROR`; an excessive observed retry is `FAIL`. |
| 59 | 3 quick / 7 cert independent trickle and stall pairs. Gap/timeout headlines are worst case and counts are sums; every pair passes. | Missing provider frame or timeout boundary, `idle_timeout_ms<1,250`, an invalid row-local deadline, or AHRB pacing outside 1,250 ms is `ERROR`; a trustworthy harness early/late timeout or outer kill is `FAIL`. |
| 60 | 1 quick / 3 cert trials. Byte/counter fields must agree in every trial; memory is the maximum trial delta. | Missing output-limit/marker declaration is `ABSENT`; incomplete byte or digest evidence is `ERROR`. |
| 61 | 1 quick / 3 cert trials. Every faulted write trial passes; result counts are summed and terminal latency is the maximum. | If the AHRB control write proves chmod ineffective, the row is infrastructure `ERROR`, not harness `FAIL`. |
| 62 | 1 quick / 3 cert trials. Every run and independently challenged same-confinement probe passes; attempt counters are summed. | The only non-PASS outcome is infrastructure `ERROR`; never `FAIL`, `UNSUPPORTED`, `ABSENT`, or proxy-only proof. |
| 63–64 | 2 quick / 7 cert executions. Run 1 is baseline; compare independently with every later run, sum comparable/varying occurrences, and require every comparison for row 64. | Missing run or incomplete request-collector evidence, an invalid/duplicate attempt ledger, or a zero denominator is `ERROR`. After a run's collector is complete, an added/missing semantic request is observed variation: informational `FAIL` for 63 and CORE `FAIL` for 64. |
| 65 | 1 verified trial per component in both profiles. Aggregate the three component scores arithmetically. | A wholly impossible provider-injection architecture is row `UNSUPPORTED` with score 0; partial, runnable but unverified components are informational `FAIL`, not a fabricated run. |
| 66 | 3 quick / 7 cert independent token/cost/time cases. Cases pass per repetition; subscore is passed case kinds/3 only when each repetition of that kind passes. `overrun_count` is the sum of the exact per-case/per-repetition forbidden observations defined by row 66; it is never a median, maximum, or count of excess token/micro-USD units. | Undeclared optional capability is `UNSUPPORTED` score 0; missing declared controls/tariff is `ABSENT`; absent stop/effect boundaries are `ERROR`. |
| 67 | 3 quick / 7 cert complete usage runs. Each repetition must equal the exact fixture totals; headline values are one per-repetition value (never sums across repetitions). Every completed semantic turn has exactly one declared usage carrier. Per-turn carriers are summed within a repetition; cumulative-run carriers are differenced for turn cross-checks and the final carrier supplies the headline. | Undeclared optional capability is `UNSUPPORTED` score 0; missing declared pointer, carrier, or scope is `ABSENT`; missing event evidence is `ERROR`. |
| 68 | 1 quick / 5 cert lifecycles. Each lifecycle commits the required nonempty seed tool turn before resume or fork. Each `*_ok` is 1 only if every repetition passed that operation; score is the five booleans/5. | Undeclared optional capability is `UNSUPPORTED` score 0; missing declared operation is `ABSENT`; incomplete operation output is `ERROR`. |
| 69 | 3 quick / 7 cert success+failure pairs. A component is 1 only if it passes every repetition; score is passed components/7. | Missing optional timestamp/schema metadata is a measured component zero. No declared durable narrative capture points is badge-blocking `UNSUPPORTED`; a declared but missing/wrong narrative is measured zero/`FAIL`; missing required event/correlation evidence is `ERROR`. |
| 70 | 1 quick / 5 cert trials per required case. Effect counters are sums and must equal repetitions for allow and each denial; score is the verified mode score only if every trial passes. | Undeclared optional capability is `UNSUPPORTED` score 0; missing declared command is `ABSENT`; missing effect evidence is `ERROR`. |
| 71 | 1 quick / 3 cert success+failure pairs. Scan every required artifact in every repetition; byte/file/match counters are sums. | Missing required row capability declaration or an empty `capture.credential_carrier_paths` declaration is `ABSENT`. An unreadable/incomplete scan is `ERROR`; any observed leak is `FAIL`. |
| 72 | 1 quick / 5 cert repetitions, each with one successful and one failed call. Counters are sums and `checks == 2 * repetitions`. | Missing next-request evidence is `ERROR`; a present malformed/missing/duplicate/untyped result is `FAIL`. |
| 73 | Reuse row 51's 3 quick / 7 cert long-horizon repetitions, but retain one independent row-73 observation from every completed run even when row 51 cannot form a recovery trial. Count externally observed compactions and journal/stream announcements; every repetition must occupy the same tier. | If no compaction occurs in the complete forced run, the row is badge-blocking `UNSUPPORTED`, never a vacuous PASS. Partial compaction or incomplete provider/event evidence is `ERROR`; observed compaction without an accurate announcement is `FAIL`. |

## 4. New matrix rows

### A. Efficiency — cost per unit of work

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **42 `model-request-efficiency`** | CHEAP; Resource; INFORMATIONAL | Measured for both. Per-invocation attribution is child-launch/semantic-turn; daemon attribution is session/semantic-turn. No N/A. | A direct-terminal, no-tool prompt so one primary request is sufficient. Quick 20 turns; cert 100; **exactly two** fresh-profile repetitions in either profile. Classify every POST as `primary`, `title`, `summary`, `compaction`, `reviewer`, `child`, or `unknown-side-channel`; retries are physical attempts of the same semantic request. A request matching the current scripted checkpoint is primary; otherwise the first matching `[[request_role_rules]]` entry by ascending priority assigns the side-channel kind. | `metrics`: `model_request_efficiency.requests_per_semantic_turn`, `.primary_requests_per_turn`, `.side_channel_requests_per_turn`, `.retry_attempts_per_turn`, `.request_body_bytes_p50`, `.request_body_bytes_p95`, `.request_body_bytes_max`, `.context_tax_bytes_p50`, `.context_tax_bytes_p95`, `.context_tax_bytes_max`, `.context_tax_slope_bytes_per_turn`. The first three numerators are physical POST counts (total, primary-role, non-primary-role); retry attempts are `sum(max(semantic_attempts_total-1,0))`, so they are a diagnostic subset rather than an additive request class. Every rate divides by completed scripted turns after summing the two repetition numerators and denominators. Body/context observations are pooled in semantic-key order; the slope field is the median of the two independently computed slopes. `details.model-request-efficiency.side_channel_requests_by_role`, `.unclassified_requests`, and per-attempt records in `model-requests.jsonl` provide attribution. Total body bytes are measured before JSON parsing. Context tax is the UTF-8 byte length of canonical JSON containing only the exact dialect instruction/tool fields, preserving arrays: Chat `{"messages":<system/developer messages>,"tools":<tools-or-[]>}`, Responses `{"instructions":<instructions-or-null>,"tools":<tools-or-[]>}`, Anthropic `{"system":<system-or-null>,"tools":<tools-or-[]>}`. | PASS envelope iff every completed turn has **exactly one** primary physical request, all requests are classified, every turn terminalizes, `side_channel_requests_per_turn <=0.05`, `retry_attempts_per_turn=0` in this fault-free fixture, `context_tax_bytes_p95<=1,048,576`, and `abs(context_tax_slope_bytes_per_turn)<=1,024`, in both repetitions. Any present `unknown-side-channel`/unclassified request is an informational `FAIL`, not missing evidence; genuinely missing role evidence is `ERROR`. Counts above the envelope remain visible rather than being normalized away. | Add zero or more ordered `[[request_role_rules]]`; `[model_roles]` alone only distinguishes provider model IDs and is insufficient when primary/title share a model. `FakeModelEngine::handle` and `ModelRequestRecord` in `src/fake_model.rs` already aggregate canonical requests/attempts and contain exact canonical bodies. `handle_http` has raw body bytes but v1 discards size/timing, so CHEAP v2 adds serialization at that existing interception point. |
| **43 `turn-latency-distribution`** | CHEAP; Resource; INFORMATIONAL; supplies badge **L** class | Measured separately for both; never combine topologies. Per-invocation includes process launch through exit. Daemon includes submit through structured terminal but not cold daemon launch. | Reuse row-29 tiny turns: quick 100, cert 1,000 per repetition, excluding warm-up/fault/tool-output turns. Fake responses are immediate; use 3/7 repetitions. | `resource_summary.wall_per_turn_p50_ms`, `.wall_per_turn_p95_ms`, `.wall_per_turn_max_ms`, `.wall_per_turn_mad_ms`, `.wall_per_turn_jitter_ratio`, `.latency_class`. Only the five numeric fields are topology-labelled `resource_metrics`; the string `latency_class` lives in `resource_summary` and typed details, never the numeric mirror. Persist each external interval in `turns.jsonl` as `turn_wall_ns`. Jitter is `MAD/p50`; compute distributions per repetition, headline each non-max statistic as the median of repetition statistics, and take `max` over all intervals. | Missing either external boundary in any required interval is `ERROR`. With complete evidence, a harness-owned timeout or `max >= resources.turn_timeout_ms` is an informational `FAIL`; otherwise the reference envelope is PASS iff every repetition has `p95 <= 1,000 ms` and jitter `<= 0.25`. Class the headline p95: `L100` ≤100 ms, `L250` ≤250, `L500` ≤500, `L1000` ≤1,000, otherwise `L1000+`. | No new manifest key. `Driver::completed_turn_wall_ns`, `ResourceEvidence.turn_wall_ns`, and `report::summarize_resources` already compute the mean; v2 persists the vector and uses the existing deterministic distribution convention. Real quick means in `results/` range from about 169 ms to 2,608 ms, so L is deliberately a class, not a CORE gate. |
| **44 `process-hygiene`** | CHEAP; Resource; **CORE** (residue); churn/growth diagnostics remain informational | Measured for both. Per-invocation requires zero owned identity after every child exit. Daemon compares post-session state to warm baseline and also requires zero owned identity after official daemon shutdown. No N/A. | Reuse row 42's exactly two 20/100-turn repetitions, with a 2 s audit after each per-invocation turn and after daemon session close/shutdown. Each repetition has exactly `K=T+1` ordered growth checkpoints: warm baseline checkpoint 0 and one post-audit checkpoint after each of T=20/100 turns. For each turn, process churn is new `(pid,start_time)` identities; thread/FD churn is the sum of positive deltas in each stable process's sampled counts plus the initial count of each newly observed process. | `metrics`: `process_hygiene.observed_processes_spawned_per_turn_p50`, `.observed_processes_spawned_per_turn_max`, `.observed_threads_created_per_turn_p50`, `.observed_threads_created_per_turn_max`, `.observed_fds_opened_per_turn_p50`, `.observed_fds_opened_per_turn_max`, `.peak_live_processes`, `.peak_threads`, `.peak_fds`, `.residue_processes`, `.residue_threads_delta`, `.residue_fds_delta`, `.unique_process_identities`. `details.process-hygiene.residue_identities` contains sorted `{pid,start_time,command,ownership}`; `.audits[]` retains every repetition/turn residue and `.monotonic_growth_ok` is the informational K-checkpoint diagnostic. Residue metrics are maxima across all audits/repetitions; churn distributions pool turns in semantic order. | The CORE row PASS is determined only by complete residue evidence: per-invocation residue is zero after every 2 s audit; daemon post-close has no new process identities, `threads<=baseline+2`, `fds<=baseline+4`, and shutdown residue is zero after 2 s. Separately, informational `monotonic_growth_ok` is true iff live process/thread/FD counts are not strictly higher than the preceding checkpoint in `ceil((K-1)/2)` or more adjacent pairs in either repetition; this diagnostic does not change the CORE outcome. Observed churn is a sampler lower bound and has no ceiling. | No new key. `Sample`/`ProcessSample` already carry identities, FDs, threads, and phase; `collect_per_invocation_resource_observations` already finds residual children. Activity shorter than the 10 ms membership cadence may escape churn counts, so v2 labels every churn metric `observed`. Current evidence records Codex residue around nine helpers, proving the residue path is feasible. |
| **45 `time-to-first-model-request`** | CHEAP; Resource; INFORMATIONAL | Measured cold for both. Per-invocation starts at one-shot child spawn. Daemon starts at cold controller spawn, so it includes readiness and first submit. No warm-daemon substitution and no N/A. | Fresh profile and one direct-terminal prompt; quick 3, cert 7. Timestamp at launch boundary and when the fake provider finishes reading the first inference request body of **any** role, including an earlier title/summary request. | `resource_summary.time_to_first_model_request_p50_ms`, `.time_to_first_model_request_p95_ms`, `.time_to_first_model_request_max_ms`; `turns.jsonl.launch_ns` and `.first_model_request_ns`. `details.time-to-first-model-request.first_request_role` identifies the classified role. | PASS envelope iff all pairs exist, `p95 <= 2,000 ms`, `max <= 10,000 ms`, and `max < resources.turn_timeout_ms`. | No new key. Row 24 already owns launch/readiness clocks in `runner::collect_resource_evidence`, and `fake_model::handle_http` observes body completion. v1 does not persist one shared monotonic timestamp, so old bundles cannot derive this row; the v2 change is boundary plumbing, not a new fixture. |
| **46 `memory-time-integral`** | CHEAP; Resource; INFORMATIONAL; supplies badge **C** class from CPU/turn | Measured separately. Per-invocation baseline is zero. Daemon integrates `max(0,effective_memory-B)` where B is the fresh warm-idle median. No N/A. | A dedicated collector runs continuously across each whole repetition: it starts before the first launch/submit, tracks membership and cumulative CPU without resetting between turns, records every one of the quick 20 turns / 3 reps or cert 100 / 7 reps, and stops only after the final terminal/exit is bracketed. This is not the row-25 N=1 single-turn bracket. Each turn window still has its own launch/submit and terminal/exit boundaries. | `resource_summary.memory_time_integral_mib_s_per_turn`, `.memory_time_integral_coverage_ratio`, `.memory_time_integral_max_sample_gap_ms`, `.cpu_per_turn_p50_ms`, `.cpu_per_turn_p95_ms`, `.cpu_class`. Only the five numeric values are topology-labelled `resource_metrics`; string `cpu_class` is excluded. Integral/CPU observations are pooled only after validating each repetition; coverage is the minimum repetition ratio and max gap is the maximum. `details.memory-time-integral` contains `{integration:"trapezoidal",collector:"continuous-long-horizon",repetitions:[...]}`. | Any unbracketed turn, nonmonotonic counter, coverage below `0.99`, or maximum gap above twice the recorded platform counter cadence is `ERROR`, because the measurement is untrustworthy. With complete evidence, the reference envelope is PASS iff median memory integral `<=1,024 MiB*s/turn` and `cpu_per_turn_p95_ms<=250` in **both quick and cert**. C class by headline p95 CPU: `C10` ≤10 ms, `C50` ≤50, `C250` ≤250, otherwise `C250+`. | No new manifest key. `Sample.elapsed_ns`, effective memory, cumulative whole-tree CPU, and `sampler::phase_coverage` are reusable, but row 46 requires a new continuously sampled 20/100-turn association collector. `report::mean_rss_mib` is arithmetic rather than time-weighted and MUST NOT be reused as the integral; the N=1 row-25 bracket is not row-46 evidence. |
| **47 `disk-io-per-turn`** | NEW; Resource; INFORMATIONAL | Measured for both. Per-invocation sums counter deltas for the live and retired tree. Daemon uses the owned-tree counter delta per turn and includes persistent journal/log growth. A platform that cannot account for exited children is `ERROR`, not `UNSUPPORTED`. | Tiny journaled turns: quick 20, cert 100 in 3/7 repetitions. Snapshot every declared journal/log path at turn boundaries. macOS sums `ri_diskio_byteswritten`; Linux sums `/proc/<pid>/io:write_bytes`, with cgroup `io.stat` when available. Each snapshot includes filesystem device and inode/file ID. Growth is `max(0,end_size-start_size)` only when identity is unchanged; creation or replacement starts at zero, while same-identity size reduction is truncation and contributes zero growth. Journal/log per-turn growth is the sum for their respective path sets. Every retired identity must have either a final counter sample after its structured terminal and before reap, or a durable cgroup `io.stat` delta captured after membership becomes quiet; a last live poll alone is incomplete. | `resource_summary.disk_write_bytes_per_turn_p50`, `.disk_write_bytes_per_turn_p95`, `.disk_write_bytes_per_turn_max`, `.session_journal_growth_bytes_per_turn`, nullable `.log_growth_bytes_per_turn`, `.disk_write_growth_slope_bytes_per_turn2`, `.disk_io_counter_complete`, `.unbounded_disk_growth`. Compute each repetition's disk p50/p95/max, journal per-turn median, declared-log per-turn median, and slope first; publish the median of repetition values except disk max is the global max, counter-complete is true only if all repetitions are complete, and unbounded is true if any repetition is unbounded. `resources.log_paths` omitted is incomplete evidence and `ERROR`; explicit `log_paths=[]` plus an exhaustive isolated-root snapshot with no non-journal log artifact is `details.disk-io-per-turn.log_evidence="verified-no-log"`, leaves the log-growth field JSON null/absent from numeric mirrors, and never synthesizes zero. A nonempty declaration records `log_evidence="declared-paths"`. Only non-null numeric f64 fields are topology-labelled mirrors; the bools and log-evidence state live in `resource_summary`/typed details. `filesystem-snapshots.jsonl` records path, device ID, inode/file ID, size, digest, and boundary. The slope is Theil–Sen of whole-tree disk-write bytes for turn i against i. `unbounded_disk_growth` is true iff that slope exceeds 4,096 bytes/turn² or the second-half disk-write median exceeds `max(1.25*first-half median, first-half+65,536)`. | PASS envelope iff every repetition's live/retired counters and file identities are complete, disk-write p95 `<=67,108,864` bytes/turn, each repetition's journal median and each applicable declared-log median is `<=1,048,576` bytes/turn, each slope `<=4,096 bytes/turn²`, and `unbounded_disk_growth=false`. | Add explicitly present `resources.log_paths` and optional `resources.journal_paths`; `events.path` is automatically a journal path. Parsers retain omitted-versus-explicit-empty state. `RusageInfoV4` already declares disk read/write fields but `MacProcessCounters` discards them; Linux has no `/proc/<pid>/io` reader. A cumulative retired-process tracker analogous to `TreeCpuTracker` is required, integrated with terminal-before-reap/cgroup retirement. Very short-lived process completeness remains a known implementation risk and must produce `ERROR`, never a favorable zero. |
| **48 `model-wait-cpu`** | NEW; Resource; **CORE** | Measured for both while the provider is the only pending dependency. No N/A. Daemon baseline CPU is subtracted only if measured in an immediately adjacent equal-length quiet window; negative corrected CPU is clamped to zero and reported in details. | New trickle response: after response headers at t=0, emit exactly T one-byte body frames with ordinals `1..=T` at scheduled times `ordinal*1,000 ms`, T=5 quick /20 cert, and complete immediately after frame T. Three/seven repetitions. Whole-tree sampling is phase-labelled `model-wait`; the dedicated row-local outer deadline is exactly `T*1,000 + resources.idle_timeout_ms + 3,000 ms` from response-header completion and overrides any shorter ordinary turn deadline. Before launch AHRB validates that its scheduled task/collector deadline can cover that full interval, including the 20 s cert stream; inability to do so is infrastructure `ERROR`. | `resource_summary.model_wait_cpu_p50_ms`, `.model_wait_wall_p50_ms`, `.model_wait_cpu_one_core_max_ratio`; `metrics.model_wait_cpu.bytes_yielded`, `.max_inter_frame_ms`; topology-labelled numeric resource mirrors. Per trial, CPU is the owned-tree CPU delta during response headers through the fake provider's final `Body::poll_frame` yield, wall is that monotonic interval, and one-core ratio is `cpu_ns/wall_ns`; summary CPU/wall are medians and ratio is the maximum. | PASS iff exactly one structured success follows the final frame, every repetition completes without outer kill, every scheduled ordinal is yielded exactly once, and maximum one-core ratio `<=0.05`. Missing frame/timing evidence, an invalid row-local deadline, or any AHRB frame yield gap `>1,250 ms` is infrastructure `ERROR`; only a complete harness CPU/terminal violation is `FAIL`. Sampler CPU is reported separately and excluded. | No new manifest key. Requires `Fault::Trickle` and a timer-wakeable body; current `DeterministicBody` has only immediate frames, disconnect, and permanent stall. Existing `Sample.cpu_ns` provides out-of-band CPU. `frame_yielded_ns` is observable in current Hyper code; kernel write completion and harness read receipt are intentionally not claimed. This fixture is shared with row 59. |

### B. Long-horizon behavior

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **49 `latency-vs-turn-index`** | CHEAP; Resource; **CORE** | Measured for both using one growing session. Per-invocation invokes official resume/continue for each turn; daemon keeps the same session. No N/A. | Extend v1 row 29 exactly: 100 turns quick, 1,000 cert, fixture tool every tenth turn, in 3/7 repetitions. Use each external turn wall interval, not batch wall time. | `resource_summary.latency_slope_ms_per_100_turns`, `.latency_last_first_decile_ratio`; `metrics.latency_vs_turn_index.first_decile_p50_ms`, `.last_decile_p50_ms`, `.theil_sen_ms_per_turn`; raw `turns.jsonl.turn_index`. Compute each repetition first and publish the median of repetition fields. `latency_last_first_decile_ratio` is exactly `last_decile_p50_ms / first_decile_p50_ms`; if the first median is zero the ratio is JSON null in `resource_summary`/typed details, is absent from numeric `resource_metrics`, and cannot pass. | PASS iff all turns terminalize in every repetition, `latency_slope_ms_per_100_turns <= max(0.05*first_decile_p50_ms,25.0 ms)` (equivalently the per-turn Theil–Sen slope is at most that bound divided by 100), `latency_last_first_decile_ratio<=1.25`, and last-decile median `<=1.25*first-decile median + 50 ms`. The 5% term admits proportional fresh-process startup and persisted-resume drift at a high wall baseline; the 25 ms floor admits bounded host scheduling/startup variance at a low baseline. This slope conjunct remains a monotonic-runaway guard, while the unchanged ratio and last-decile conjuncts independently cap absolute end-to-end growth. The formula is identical for both topologies and MUST NOT branch on topology name. | No new key. `runner::run_long_horizon` already timestamps turns and row 29 already has the correct session; v1 reports only aggregate wall mean, so v2 preserves per-turn values. |
| **50 `session-residue-sweep`** | NEW; Resource; **CORE** | Measured for both. Per-invocation creates each persisted session and invokes a real harness-owned close-and-delete surface, then audits profile-owned processes and the declared session store; changing only AHRB metadata is not a close. Daemon repeatedly creates/closes/deletes sessions without restarting the daemon, then compares to warm baseline B. No N/A. | Create/use/close-delete N sessions sequentially in 3/7 independent sweeps: N=20 quick, N=200 cert. Measure process and recursive session-store checkpoints at 0, every 10, and N; run a final 10 s reclaim window. Each file snapshot records device/inode, size, and digest so replacement cannot masquerade as truncation. | `resource_summary.session_residue_slope_mib_per_session`, `.session_residue_final_mib`, `.session_store_byte_slope_per_session`, `.session_store_file_count_slope_per_session`, `.session_store_final_residue_bytes`, `.session_store_final_residue_files`; `metrics.session_residue_sweep.fd_slope_per_session`, `.thread_slope_per_session`, `.process_slope_per_session`, `.unretired_sessions`, `.closed_sessions`, `.created_sessions`. Slopes are Theil–Sen per sweep; published slopes are their medians and final residues are maxima after reclaim. `details.session-residue-sweep.store_checkpoints[]` contains repetition, session count, bytes and files relative to baseline. | PASS iff all N public close-delete commands succeed in every sweep; memory slope `<=0.03125 MiB/session` (32 KiB); final memory residual `<=max(64 MiB,20% of maximum active delta)`; FD/thread/process slopes are each `<=0`; `unretired_sessions=0`; store byte and file-count slopes are each `<=0`; and final store residue is exactly 0 bytes and 0 files relative to baseline. These total slope rules replace any separate informal monotonic-growth test. | Require an official `sessions.close_delete` command/operation. `PerInvocationDriver::close` MUST invoke that harness surface and verify its result before marking AHRB metadata closed; daemon drivers invoke the declared transport equivalent. Existing row-23/28 baselines and identities plus row-29 checkpoint structs are reusable, but the create/close-delete and recursive storage sweep are new. |
| **51 `context-limit-recovery`** | NEW; AutomationReadiness; **CORE** | Measured for both. Recovery may be in-process, a daemon session operation, or a fresh per-invocation resume, but the same session identity and tool correlations must survive. Missing the required declaration is badge-blocking `ABSENT`, never `FAIL` or auto-PASS. | Fake `/v1/models` advertises deterministic context limit W; the named pre-error request is dialect-padded and externally asserted to have exactly `W+256` fake-provider input tokens and exactly `8*W+1,024` raw body bytes, then receives one dialect-native context-length error. Quick: W=4,096, 16 turns, 4 tool pairs. Cert: W=16,384, 128 turns, 32 tool pairs. Every ordinary-history fixture message contains a deterministic ordinal+SHA-256 history marker. The fake provider continues enforcing W after the one-shot error, so the first post-error primary request it accepts is the compaction candidate. Failure to construct/observe either exact pre-error total is `ERROR`. | `metrics.context_limit_recovery.context_errors`, `.extra_requests`, `.recovery_ms`, `.terminal_success`, `.tool_pairs_before`, `.tool_pairs_after`, `.orphan_tool_calls`, `.orphan_tool_results`, `.duplicate_effects`; `details.context-limit-recovery` adds `.pre_error_input_tokens`, `.pre_error_body_bytes`, `.compaction_request_hashes`, `.accepted_input_tokens`, `.accepted_body_bytes`, `.retained_markers`, `.omitted_markers`, `.summary_markers`, and `.normalized_compacted_stream_sha256_by_repetition`. | “Compacted” means all of the following: (1) the accepted candidate has fake-provider input tokens `<=W` and raw body bytes `<=8*W`; (2) dialect system/developer instructions and tool definitions are unchanged after only the row-63 normalization; (3) the initial user-goal marker, the latest four non-tool history markers, **every** 4/32 tool-call+result pair with exact correlation/content/order, and every committed-effect marker remain; (4) at least one older ordinary message from the exact over-limit request is omitted, and **all and only** omitted ordinary-history markers are represented by exactly one textual summary item in a dialect-native system/developer context item, each exactly once in increasing ordinal order; no summary may replace or synthesize a tool call/result; and (5) the normalized length-prefixed canonical candidate stream is byte-identical across all 3/7 repetitions. PASS further requires exactly one context error, success within the turn deadline, no more than two extra requests, no orphan/duplicate pair/effect, and unchanged session identity. An unchanged/no-op or history-discarding retry FAILs. | Add `capabilities.required.context_limit_recovery` and `[resources.context_window]`; the row itself is CORE. Requires a context-window field in `/v1/models`, a one-shot `ContextLength` fault, continued W enforcement, and a workflow transition accepting only candidates satisfying this oracle. Current fake `/v1/models` returns only an ID and current faults cannot change after one attempt. |
| **52 `resume-latency-vs-length`** | NEW; AutomationReadiness; **CORE** | Measured for both through the official persisted resume surface. Per-invocation starts a fresh client; daemon detaches the client while the controller stays ready, excluding daemon cold start. A missing required resume declaration is `ABSENT`; an explicitly unavailable architectural resume is badge-blocking `UNSUPPORTED`, never `FAIL`. | Build lengths `{1,10,50}` quick and `{1,50,100,250,500}` cert. Close/detach, then invoke one deterministic continuation through the manifest's resume path. Time from resume-command spawn/daemon reattach-submit to completion of the first resumed request body at the fake provider; the provider returns an immediate terminal. Three/seven fresh sessions per length. | `resource_summary.resume_latency_p50_ms`, `.resume_latency_p95_ms`, `.resume_latency_slope_ms_per_turn`; `metrics.resume_latency_vs_length.short_p50_ms`, `.mid_p50_ms`, `.long_p50_ms`, `.long_short_ratio`. `short/mid/long` mean L=1/10/50 in quick and L=1/100/500 in cert. Aggregate repetitions at each length first; headline resource p50/p95 are the p50/p95 distribution at the **longest** length, and slope is Theil–Sen over per-length median latencies. All raw lengths remain in `details.resume-latency-vs-length.points[]` as `{length,repetition,resume_start_ns,first_request_ns,latency_ms,session_id_hash,cursor}`. | PASS iff every first request resumes the same session at the exact cursor, longest-length p95 `<=5,000 ms`, Theil–Sen slope `<=5 ms/turn`, and `T(long)/T(short) <=2.5`. The fixed continuation cost is deliberately present at every L and cancels in the slope/ratio. | Existing `sessions.resume` supplies adapter data, but `PerInvocationDriver::resume` currently reloads only AHRB metadata and the real resume argv is launched by the next `submit`. Therefore this row requires a new external timer spanning that submit to `fake_model::handle_http`; timing `Driver::resume` alone is invalid. |
| **53 `journal-torn-tail-sweep`** | NEW; AutomationReadiness; **CORE** | Measured for both. Kill the active one-shot worker for per-invocation or the persistent daemon for daemon topology. The recoverable source remains the manifest-declared journal/session store. A missing required journal declaration is `ABSENT`; explicit architectural unavailability is badge-blocking `UNSUPPORTED`. | Run exactly N=5 quick / 25 cert trials, overriding 3/7. The fixture's final record is exactly 1,048,576 ASCII bytes including its final newline: prefix `{"type":"ahrb-large","payload":"`, then enough `A` bytes to make the total exact, then suffix `"}\n`. A 1 ms watcher records the first growth and waits until size is at least `pre_size+1,048,576`, then SIGSTOPs/SIGKILLs; evidence claims **kill after complete growth was observed**, never inside a syscall. On five killed-profile copies apply cyclic record-relative cut offsets exactly `{0,262144,524288,786432,1048575}` bytes (0%,25%,50%,75%, immediately before the newline delimiter), restart, and attach after the last committed cursor. Failure to observe the exact full growth before terminal/timeout is `ERROR`. | `metrics.journal_torn_tail_sweep.trials`, `.kill_after_growth_observed_trials`, `.clean_recoveries`, `.corrupt_recoveries`, `.lost_committed_events`, `.duplicate_events`, `.duplicate_effects`, `.recovery_p95_ms`; `details.journal-torn-tail-sweep.cut_positions[]` has `{trial,pre_size,observed_size,cut_offset,growth_observed_ns,kill_ns}` plus recovered hashes. | PASS iff `kill_after_growth_observed_trials=trials`; the record digest and observed length match the exact fixture before copying; every trial recovers within 10 s; committed prefix/suffix agreement is exact; the cut final record is fully present or cleanly absent; and no parse corruption, gap, fabrication, duplicate event, or duplicate effect occurs. Any one bad trial fails. | No new manifest key; reuse `events.path`, `events.cursor_pointer`, replay command, and row-40 validation concepts. The existing helper appends one synced synthetic malformed tail; it does **not** implement profile copies, five cut positions, the 1 ms live watcher, or repeated kills. Those are all required NEW work. Polling proves only kill after observed growth and never a syscall/fsync boundary. |

#### Revision-2.3 Wave-3 clarifications (normative)

These rules amend the row table above and take precedence where older wording is less
specific.

- **Row 50 storage locator and traversal.** A selected row 50 requires nonempty
  `sessions.store_paths`, with every rendered root lexically and physically contained in
  `{{profile}}`. Traverse recursively without following symlinks. Record every directory
  entry by relative path, file type, device, and inode; count only regular files and
  their logical bytes. A symlink, hard-linked identity reachable through multiple
  declared roots, mount/device escape, traversal cycle, or any resolved identity outside
  the isolated profile is `ERROR`, not a favorable deduplication. Every public
  `sessions.close_delete` result MUST be received and validated before its residue
  checkpoint; AHRB metadata-only close is never evidence of deletion.
- **Row 51 fake token counting.** Parse the request JSON and walk string values, never
  keys, in the dialect context projection: Chat `messages` then `tools`; Responses
  `instructions`, `input`, then `tools`; Anthropic `system`, `messages`, then `tools`.
  Arrays retain order and object values use UTF-8 lexical key order. Within each decoded
  string, every maximal ASCII `[A-Za-z0-9_]+` run is one token, ASCII whitespace is a
  zero-token separator, and every other Unicode scalar is one token. Missing or null
  projection members contribute zero. The dedicated ordinary-history padding string
  uses `z ` runs plus one lengthened final `z...z` run: the run count supplies exactly
  `W+256` tokens and the final-run length supplies exactly `8*W+1,024` received body
  bytes. The provider recomputes both totals from the received bytes before faulting;
  inability to hit either exact value is `ERROR`.
- **Row 51 recovery window.** `recovery_ms` starts when the fake provider yields the
  final byte of the one context-length error response and ends when it completes reading
  the first post-error primary request body satisfying both accepted limits.
  `extra_requests` counts every physical model POST body completed strictly after the
  error-byte boundary and no later than AHRB receipt of the successful structural
  terminal. It includes the accepted candidate, side-channel requests, and retries, but
  excludes the faulting request. PASS requires
  `recovery_ms<=resources.turn_timeout_ms` and `extra_requests<=2` in addition to the
  row-table conjuncts.
- **Row 52 zero denominator.** If the aggregated short-length p50 is zero,
  `long_short_ratio` is JSON null in typed details and omitted from numeric metric maps;
  zero is never substituted. The measurement is complete, but the required ratio
  conjunct cannot pass, so the row is `FAIL` rather than `ERROR`, `PASS`, or
  `UNSUPPORTED`. Haider's list/resume surfaces are declared feasibility inputs but their
  behavior is unverified and cannot be treated as evidence or an automatic PASS.
- **Row 53 exact cuts.** Each copied journal is truncated to exactly
  `pre_size+cut_offset` bytes, with cyclic offsets
  `{0,262144,524288,786432,1048575}`. The offset is never subtracted from observed size
  or applied relative to a previous cut. Record `truncated_size`; failure to obtain the
  exact target size is `ERROR`.
- **Row 54 aggregate then detect.** First aggregate all required repetitions at each N
  by median into one RSS and one wall point. Only those per-N aggregate points feed local
  elasticity, prior-increment, global-alpha, and cliff tests; trial-level values never
  feed a cliff test directly.
- **Row 55 censorship and clock.** A scheduled actor without a terminal is assigned
  latency exactly equal to the declared turn deadline for CV, spread, and ratio and is
  also starved. `clock_getres(CLOCK_MONOTONIC)` must succeed and return a strictly
  positive resolution; failure or a nonpositive result is infrastructure `ERROR`.

### C. Concurrency

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **54 `fanout-cliff`** | NEW; Resource; **CORE** | Measured within both topologies. Quick requires declared width >=8; cert requires `concurrency.max_agents >=32`. A missing width declaration is `ABSENT`; a lower explicitly declared architectural width is badge-blocking `UNSUPPORTED`, not a reduced-width PASS. No cross-topology comparison. | Same state-barrier actor/tool workload as rows 26–27. Quick widths `{1,2,4,8}`. Cert widths every integer `1..=32`; 3/7 fresh-profile reps per N with deterministic seeded N order. Cert has exactly 224 width trials. Its row-level scheduling deadline is `224 * (resources.turn_timeout_ms + 3,000) + 60,000 ms`; each trial still has its own `turn_timeout_ms+3,000 ms` outer deadline. | `resource_summary.fanout_cliff_n_rss`, `.fanout_cliff_n_wall`, `.fanout_max_local_rss_alpha`, `.fanout_max_local_wall_alpha`, `.fanout_global_rss_alpha`, `.fanout_max_measured_n`. The three f64 alpha fields are mirrored; nullable cliff-N and integer max-N fields remain in `resource_summary`/typed details and are never numeric mirrors. `details.fanout-cliff.trials[]` has `{repetition,n,steady_bytes,peak_bytes,active_memory_delta_bytes,wall_p95_ms}`; `.points[]` has one median-aggregated `{n,y_rss_bytes,y_wall_ms,local_rss_alpha,local_wall_alpha,rss_increment_bytes,wall_increment_ms}` per N. JSON null means no cliff; zero MUST NOT mean no cliff. | Aggregate repetitions at each N first: `y_rss=median(active_memory_delta_bytes)` and `y_wall=median(wall_p95_ms)`. For adjacent positive points compute local elasticity `log(y2/y1)/log(n2/n1)`; nonpositive y is ERROR. The first interval has no previous elasticity/prior-increment test and cannot alone declare a cliff. Starting at the second interval, cliff N is the smallest upper N where elasticity `>1.50` and exceeds the immediately previous elasticity by `>0.35`; starting at the third interval, the alternative increment test is current increment `>2.0x` the median of all earlier **positive** increments. If no earlier positive increments exist, that alternative is false for the interval (not zero, PASS, or ERROR). `fanout_global_rss_alpha` is a new OLS log-log slope over aggregated N points, using the same formula as legacy `scaling_alpha` but not aliasing it. PASS iff both cliff fields are null, global RSS alpha `<=1.20`, all widths terminalize, and N8 peak `<=4 GiB`. | No new manifest key. Existing `SweepObservation`, state barrier, seeded width order, and row-27 alpha/marginals are reusable. The integer cert sweep, local wall elasticity, and retained point arrays are new. |
| **55 `fairness-under-fanout`** | NEW; Resource; **CORE** | Measured for both from row-54 trials. Use the same release boundary and semantic work for every actor. No N/A. | At every N>=2, timestamp barrier release and each actor's structural terminal. Compute statistics separately for each `(repetition,N)` group; headline CV/ratio/spread are the maximum non-null values over all required groups, and starved count is their total. A scheduled actor with no terminal by the turn deadline is assigned a censored latency exactly equal to the turn deadline for CV/spread/ratio and is also counted starved; a missing release timestamp is instead `ERROR`. | `resource_summary.fairness_latency_cv`, nullable `.fairness_latency_max_min_ratio`, `.fairness_latency_spread_ms`, `.fairness_starved_agents`. Only f64 CV and spread are topology-labelled numeric mirrors; nullable ratio and integer starved count are omitted from `resource_metrics` and retained in `resource_summary` plus `details.fairness-under-fanout`. `.actor_latencies[]` is keyed by repetition/N/stable actor and records censorship. CV is population standard deviation divided by group mean. `details` also records `clock_resolution_ns` from `clock_getres(CLOCK_MONOTONIC)` on the test host; failure to obtain a positive resolution is `ERROR`. | An actor is starved if it misses the turn deadline or latency is `>max(3*group median, group median+1,000 ms)`. PASS iff starved count=0, worst population CV `<=0.35`, worst spread `<=500 ms`, and every non-null group max/min ratio `<=3.0`. A group ratio is null only if its minimum is below the recorded clock resolution; for that group the ratio conjunct is omitted but CV/spread still apply. If every ratio is null, the headline is JSON null and cannot appear in the numeric mirror. | No new key. Existing sweep evidence knows actors at the barrier but not release-to-terminal time per actor; add those external timestamps in `runner::run_resource_group`. |
| **56 `child-failure-propagation`** | NEW; Functionality; **OPTIONAL FACET** `native_delegation` (shares `native-delegation` suffix with row 18) | Capability-gated only by manifest `native_delegation`, never by topology. Undeclared is `UNSUPPORTED`; declared is measured in either topology. | Native parent actually waits for one child. CRASH: child exits/terminalizes failure at a named checkpoint. HANG: child receives permanent stall after provider headers. Quick one pair; cert three pairs in fresh sessions. AHRB records the parent public-operation start immediately before accepted submit/spawn. The HANG harness deadline is `parent_operation_start + turn_timeout_ms`; its outer deadline is `parent_operation_start + turn_timeout_ms + 2,000 ms`. Response headers prove child-hang injection but are not the global-timeout origin. Crash latency begins at AHRB receipt of the child failure boundary and ends at receipt of the parent failure; hang latency begins at parent-operation start and ends at the parent terminal. | `metrics.child_failure_propagation.crash_parent_terminal_ms`, `.hang_parent_terminal_ms`, `.crash_parent_failure_terminals`, `.hang_parent_failure_terminals`, `.hang_deadline_fired`, `.child_terminal_count`, `.child_residue_count`, `.outer_kill_used`; normalized parent/child events retain IDs plus parent-operation, response-header, and AHRB receipt bounds. | CRASH PASS: exactly one child failure and one parent failure, parent latency `<=min(turn_timeout,5,000 ms)`, no success contradiction, residue zero. HANG PASS: the child response headers occur before the parent terminal; the parent terminal lies in `[parent_operation_start+turn_timeout_ms,parent_operation_start+turn_timeout_ms+1,000 ms]` and strictly before the outer deadline; exactly one parent failure/cancel; child is cancelled and residue zero. A terminal that is timely relative to headers but too early/late relative to parent-operation start fails. Any parent waiting until outer kill fails. | Extend the `Driver` contract with public `agent_status` and `agent_collect` operations alongside spawn/cancel. A manifest may normatively map status/collect to its public attach operation, but the mapping and result locators must be explicit; merely listing unused commands is invalid. Current `Driver::spawn_agent` exists, while status/collect are not wired and the mock child is unrelated, so wait/propagation machinery is NEW. `ahrb-mock-exec` legitimately reports `UNSUPPORTED`; daemon mock must pass. |

### D. Failure semantics

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **57 `signal-matrix`** | NEW; AutomationReadiness; **CORE** | Signals are measured in both topologies against the owning active tree: active one-shot group or daemon/controller group. stdin EOF is measured only when stdin is an input/control surface (`stdin-rpc` or `[input].prompt_uses_stdin=true`). When the driver proves stdin was `/dev/null` from launch and both transport and typed prompt declaration say it is not a control surface, only that subcase is `not_applicable`; the row still tests signals. | One fresh held turn for SIGTERM, SIGINT twice (second after 250 ms only if alive), SIGHUP, and stdin EOF. Quick one each; cert three each. Signal latency starts only when `kill(2)`/group delivery returns success; EOF latency starts when AHRB successfully closes the harness stdin writer. | `metrics.signal_matrix.sigterm_terminal_ms`, `.sigint2_terminal_ms`, `.sighup_terminal_ms`, `.stdin_eof_terminal_ms`, `.sigterm_residue_processes`, `.sigint2_residue_processes`, `.sighup_residue_processes`, `.stdin_eof_residue_processes`, `.applicable_cases`, `.passed_cases`; `details.signal-matrix.cases[]` includes signal, origin/terminal timestamps, terminal type/count, exit code, `exit_was_signal`, and N/A reason. For an N/A EOF subcase, its latency/residue metric is omitted from `metrics` and typed details uses JSON null; zero is forbidden. Published latency/residue metrics are maxima over repetitions. | Every applicable case must emit exactly one structured failure/cancel terminal, exit normally rather than only by `WIFSIGNALED`, do so within `daemon.grace_ms+250 ms`, and leave zero owned residue after 2 s. Before execution AHRB validates `daemon.grace_ms+250 < applicable outer deadline`; an invalid deadline, unresolved ownership, failed `kill(2)`/group delivery or EOF close, or missing origin/terminal boundary is infrastructure `ERROR`, not `ABSENT`/`FAIL`. The scheduling tolerance is fixed at 250 ms and is not substituted for the declared grace. SIGINTx2 must not emit duplicate terminals. At least the three signal cases must be applicable. | Add typed `[input].prompt_uses_stdin`; absence of that typed declaration is the row's only new manifest `ABSENT` condition. Transport alone cannot infer prompt use. Extend the driver with generic successful-delivery and EOF-close operations exposing the external origin time. Signal delivery itself is a generic supervisor operation over the resolved owned tree, not a manifest command. Current private process-group helpers/global AHRB handlers are not evidence of harness semantics. Both mocks need durable signal terminals. |
| **58 `retry-budget`** | NEW; ToolCallCorrectness; **CORE** | Measured for both. A missing documented retry policy is `ABSENT`, not an excuse to retry indefinitely. | Fake model returns sustained 429 in quick; sustained 429 and 500 in cert, **exactly three repetitions per status in either profile**, overriding cert's global seven. Every attempt is timestamped. The harness, not AHRB, owns retry delay and jitter. `retry_base_delay_ms` is at least 50 ms. Immediately before each status group AHRB calibrates its timer with 20 monotonic sleeps of `min(retry_base_delay_ms,100)` ms; record requested/actual intervals. The calibratable tolerance is exactly `max(5 ms,2% of requested)`: calibration passes when median absolute error is at most that value, and `timer_tolerance_ms=max(calibratable_tolerance,p95 absolute error)`. The 5 ms absolute floor covers ordinary sub-single-digit-millisecond wake-up latency from a loaded general-purpose OS scheduler; it does not relax the separate 50%–150% retry-ladder envelope and therefore still detects a materially mis-scheduled ladder. | `metrics.retry_budget.requests_total`, `.declared_max_requests`, `.declared_worst_case_ms`, `.elapsed_ms`, `.backoff_jittered`, `.failure_terminals`, `.committed_effects`; `details.retry-budget.attempts[]` contains status, repetition, 1-based attempt, received_ns, and nullable previous_backoff_ms; `.timer_calibration[]`, `.timer_tolerance_ms`, and `.post_terminal_observation_ms` are required. Per-trial `elapsed_ms` begins when AHRB finishes reading the first faulting request and ends at AHRB receipt of the single structured provider-failure terminal. Each `previous_backoff_ms` is the difference between consecutive completed request-receipt boundaries. For max attempts M, `declared_worst_case_ms = 1.5 * sum(i=0..M-2, min(base*2^i,max_delay)) + 1,000`. Headline requests/elapsed are maxima across trials; failure terminals and effects are sums; `backoff_jittered=1` if any eligible interval across the full repetition set differs from nominal by at least `max(5% of nominal,timer_tolerance_ms)`. | **Per trial**, requests are in `2..=M` with M<=6; elapsed `<=declared_worst_case_ms<=10,000`; for nominal delay N before k, measured delay lies in `[max(0,0.5*N-timer_tolerance_ms),1.5*N+timer_tolerance_ms]`; exactly one structured provider failure occurs; and effects `<=1`. Across the status repetition set at least one interval differs from nominal by `>=max(5% of nominal,timer_tolerance_ms)`. After each terminal AHRB keeps the provider and process observation alive for `max(1,000,retry_max_delay_ms+250)` ms and requires no later request/effect. Failed calibration or incomplete observation is `ERROR`; an observed out-of-budget retry is `FAIL`. | Add `resources.retry_max_attempts`, `resources.retry_base_delay_ms`, `resources.retry_max_delay_ms`. Wave 1 already records every physical attempt; Wave 2 adds the sustained per-attempt status schedule, retry-bound evaluation, and post-terminal observation. Determinism applies to evidence ordering, not the harness's runtime jitter values. |
| **59 `slow-stream-vs-stall`** | NEW; ToolCallCorrectness; **CORE** | Measured for both. No N/A. Use declared `D_idle=resources.idle_timeout_ms`; validation requires `D_idle>=1,250 ms`. A configurable adapter may set D_idle=2,500 ms for this row. | Trickle uses the row-48 schedule exactly: headers at t=0, then T one-byte frames numbered `1..=T` at t=`ordinal*1,000 ms`, T=5 quick /20 cert, completing immediately after frame T. Stall completes headers at t=0 and yields no body frame. Fresh session for each, 3/7 reps. Dedicated row-local outer deadlines override any shorter ordinary turn deadline: trickle=`T*1,000+D_idle+3,000 ms` from headers; stall=`D_idle+3,000 ms` from headers. Before launch AHRB validates that its scheduled tasks and collectors cover those full intervals, including the 20 s cert trickle; inability to do so is infrastructure `ERROR`. | `metrics.slow_stream_vs_stall.slow_bytes_yielded`, `.slow_max_inter_frame_ms`, `.slow_terminal_success`, `.slow_idle_timeout_fired`, `.stall_bytes_yielded`, `.stall_own_timeout_ms`, `.stall_structured_failure`, `.stall_outer_kill_used`; `stream-chunks.jsonl` records scheduled and actual `Body::poll_frame` yield boundaries. Worst-case gap/timeout fields and summed counters are published. | Trickle PASS iff all T scheduled ordinals are yielded exactly once, no idle timeout fires, and success terminalizes after the provider's final frame yield but before the trickle outer deadline. A missing frame/boundary, invalid row-local deadline, or AHRB inter-frame gap `>1,250 ms` is infrastructure `ERROR`, not a harness failure. Stall timeout origin is provider response-header completion (equivalently last body progress, since no body frame exists). Stall PASS iff body bytes yielded=0 and exactly one harness failure occurs in `[D_idle,D_idle+1,000 ms]`, strictly before the outer deadline without outer kill. Trustworthy early/late harness terminalization or outer kill is `FAIL`. Both subcases pass in every repetition. | Existing `resources.idle_timeout_ms` applies and must validate as at least 1,250 ms for this row. Requires `Fault::Trickle`. Current HTTP and mock clients wrap the entire read in a total timeout, so they will incorrectly kill the valid trickle; reads must reset an idle timer on each byte/chunk. Kernel write completion and harness receipt remain unobservable and are intentionally not claimed. |
| **60 `large-tool-output`** | NEW; ToolCallCorrectness; **CORE** | Measured for both. Daemon model-visible output includes the worker result; per-invocation includes the result passed by the child. AHRB artifact capture is a separate bounded observation and is never treated as the harness's output limit. No N/A. | New fixture `large_output` streams exactly 10,485,760 deterministic bytes without building one giant buffer. Quick one run; cert three. Require `resources.max_output_bytes` in `1..=1,048,576` so truncation is exercised. The next model request must contain the correlated bounded result. | `metrics.large_tool_output.produced_bytes`, `.model_visible_bytes` (retained original payload bytes, **excluding** marker), `.model_visible_encoded_bytes` (complete normalized tool-result content including marker), `.harness_output_limit_bytes`, `.evidence_captured_bytes`, `.evidence_capture_limit_bytes`, `.truncated`, `.terminal_success`, `.tool_result_correlated`; topology/profile-scoped `resource_summary.large_tool_output_peak_rss_delta_mib` and its numeric resource mirror; `details.large-tool-output.truncation_marker` has `{truncated,original_bytes,payload_bytes,sha256}` and call/result IDs per repetition. `encoded_bytes` is deliberately not rendered inside the marker because it would count its own decimal representation; AHRB computes `model_visible_encoded_bytes` externally from the complete normalized content after rendering the other fixed fields exactly once. The exact byte/limit fields are per-trial invariants and are published once only if identical across trials; peak RSS is the maximum; `truncated`, `terminal_success`, and `tool_result_correlated` are 1 only if every repetition satisfies them. They are not summed. | PASS iff every repetition produced exactly 10,485,760 bytes; externally counted complete encoded bytes `<=resources.max_output_bytes`; `truncated=1`; the marker yields original=10,485,760 and payload count/full-stream SHA-256 equal AHRB's external counts; result ID equals call ID; result reaches the next model request; `evidence_captured_bytes<=capture.max_bytes`; peak effective-memory delta `<=max(128 MiB,4*resources.max_output_bytes/1,048,576 MiB)`; and the run succeeds without OOM. Memory sampling spans immediately before fixture-tool launch through structured terminal/exit. The baseline is zero for a verified zero-process per-invocation topology and the immediately adjacent warm-idle effective-memory median for a daemon; the delta is `max(0,trial_peak-baseline)`. | Add `[tools.fixtures].large_output` and `[capture.truncation_marker]`. The marker regex is applied to normalized model-visible tool-result content and has named captures `truncated`, `original_bytes`, `payload_bytes`, and `sha256`; external encoded length is not a marker capture. The daemon mock uses `.output()` and holds all bytes in memory; both mocks require streaming bounded capture with a structured marker. |
| **61 `workspace-fault`** | NEW; ToolCallCorrectness; **CORE** | Measured for both. Session state roots remain writable; only the actor workspace is faulted. No N/A. | Create workspace directory mode 0555, then have AHRB itself attempt the exact target creation under the same uid/gid/ACL context before launching the harness. The control must fail with EACCES/EROFS. Only then ask the ordinary declared write fixture to write; a platform fixture may instead provide a separately proven ENOSPC mount. Quick one; cert three. | `details.workspace-fault.kind`, `.write_errno`, `.control_write_errno`; `metrics.workspace_fault.structured_failure`, `.terminal_count`, `.terminal_ms`, `.outside_writes`, `.residue_processes`. `write_errno` is parsed from the ordinary fixture's structured tool result and cross-checked against target/profile/forbidden-root before/after snapshots; it is not collected with preload, syscall interception, or harness turn-path instrumentation. | If AHRB's post-chmod control write succeeds, or fails for an unrelated reason, the fixture is ineffective and the row is infrastructure `ERROR` without judging the harness. Otherwise PASS iff the ordinary fixture's structured result reports EACCES, EROFS, or proven ENOSPC and snapshots independently confirm no target/outside effect; exactly one structured tool/run failure occurs within turn deadline; there is no success contradiction, crash, or hang; no target/outside write occurs; residue is zero. Missing/contradictory structured errno or snapshot evidence is `ERROR`, not a favorable zero. | No new manifest key. Existing write fixture and workspace hashes are reusable, but the runner must make the actor workspace read-only and prove that restriction with the control write. Privileges/ACLs that bypass 0555 produce `ERROR`, never harness `FAIL`. No preload/syscall instrumentation of the harness is permitted. |
| **62 `offline-mode`** | NEW; AutomationReadiness; **CORE** | Measured for both under either (1) a reviewed same-confinement OS guard or (2) a reviewed owned connector boundary for a harness/fixture whose complete process and network implementation AHRB controls. If neither proof is available, the sole outcome is `ERROR: egress enforcement unavailable`, never harness `UNSUPPORTED`, `ABSENT`, or `FAIL`. | Ordinary successful tool workflow. Deny every egress destination except the injected provider endpoint. Under an OS guard, the AHRB probe process is outside the owned harness tree but uses the **exact same** namespace/sandbox/rule set. Under the owned-boundary mode, AHRB injects a per-trial 256-bit challenge and `203.0.113.1:9`; the challenged harness root invokes its real connector `connect` operation from inside the owned tree, and that same connector is the only code path allowed to open its provider IP socket. Quick one; cert three. | `metrics.offline_mode.provider_requests`, `.blocked_egress_attempts`, `.successful_non_provider_connections`, `.offline_run_success`, `.control_probe_blocked`; `details.offline-mode.egress_enforcement`, `.confinement_identity`, `.attempts[]` with destination, category (`update-check`, `model-catalog`, `telemetry`, `other`, or `control-probe`), and outcome. Totals and category counts are reported; blocked count may validly be zero only when the harness makes no auxiliary attempt. Owned-boundary evidence additionally binds the challenge, monotonically sequenced fsync-backed decision ledger, root PID, resolved executable SHA-256, provider connects, and refused control connect to `reference-mock-loopback-connector-v1`. | PASS iff provider requests>=1, terminal success occurs, non-provider successful connections=0, the independently challenged same-confinement probe is blocked, all observed auxiliary attempts are denied, and evidence binds probe/harness to one reviewed guard identity. In owned-boundary mode the public connect must return `PermissionDenied` at the connector before any OS connect, every successful provider request must have one allowed loopback/local-IPC record, and the ledger PID must be the independently captured owned root. Any inability to establish those observations is infrastructure `ERROR`; proxy compliance, declaration-only evidence, or a probe under different confinement is never proof, and an observed escape is also `ERROR` because the purported guard is untrustworthy for certification. | No adapter key beyond allowed fake paths. `runner::isolated_environment` constructs environment only and is not socket confinement. The owned-boundary mode is deliberately restricted to the compiled-in `ahrb-mock`/`ahrb-mock-exec`: AHRB resolves the exact sibling executable, records its SHA-256 in the confinement identity, captures its root PID before releasing an exec launch, and audits the challenge ledger after the terminal. The reference mock's reviewed network surface accepts only the injected literal loopback provider (or local Unix/mailbox IPC), while the row-62 workflow cannot invoke its native shell. Other adapters still require a reviewed OS guard; Linux may use a user/network namespace plus syscall/cgroup evidence, while macOS PF/NetworkExtension commonly requires privileges and nested Seatbelt may be unavailable. |

### E. Determinism scoring

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **63 `nondeterministic-field-report`** | CHEAP; Functionality; INFORMATIONAL | Measured for both in fresh isolated executions. Compare each OS/topology/profile only to itself. | Identical direct-terminal plus one tool-call workflow. Quick 2 runs; cert 7. Run 1 is baseline; compare independently with each run 2..R. Pair and sort physical requests by the total semantic key `(scenario UTF-8 lexicographic, actor UTF-8 lexicographic, semantic_ordinal numeric, checkpoint UTF-8 lexicographic, attempt numeric)`, never hash, receipt time, or network order. Flatten paired canonical requests to JSON Pointer leaves, preserving array indices. Leaf additions/removals/type/value/array-order changes each count once. A wholly missing/added request contributes exactly one comparable and one varying occurrence at synthetic root pointer `""`; it therefore remains in the denominator rather than disappearing. | `metrics.nondeterministic_field_report.score`, `.comparable_leaf_occurrences`, `.varying_leaf_occurrences`, `.varying_pointer_count`, `.varying_critical_field_count`; `details.nondeterministic-field-report.varying_fields[]` is sorted by `(pointer, dialect)` and has exact `{pointer,occurrences,comparison_runs,before_types,after_types,dialects}`; `.run_hashes[]`. | Normalize **only string values** at this exact allowlist. Metadata exact pointers: `/metadata/ahrb_credential`, `/metadata/ahrb_profile_path`, `/metadata/ahrb_workspace_path`, `/metadata/ahrb_tmp_path`, `/metadata/ahrb_socket_path`, `/metadata/ahrb_run_marker`, `/metadata/ahrb_execution_id`. In dialect content, replace only exact AHRB-owned profile/workspace/tmp/socket/run-marker substrings at Chat `/messages/*/content`, `/messages/*/content/*/text`, `/messages/*/tool_calls/*/function/arguments`; Responses `/instructions`, `/input`, `/input/*/content`, `/input/*/content/*/text`, `/input/*/arguments`; Anthropic `/system`, `/system/*/text`, `/messages/*/content`, `/messages/*/content/*/text`, `/messages/*/content/*/input`. Each category has its own typed sentinel. No key, number, bool, null, array order, harness nonce/timestamp/session ID, credential outside its exact metadata pointer, or tool call/result ID is normalized. Score=`1-varying/comparable`; zero denominator is `ERROR`. Critical pointers are `/model`, `/messages`, `/input`, `/tools`, `/tool_choice` plus Chat `/messages/*/tool_calls/*/id`, `/messages/*/tool_call_id`, `/tool_calls/*/id`; Responses `/input/*/call_id`, `/input/*/id`, `/output/*/call_id`, `/output/*/id`; Anthropic `/messages/*/content/*/id`, `/messages/*/content/*/tool_use_id`. PASS envelope iff score `>=0.99` and critical variation count=0 in every comparison. | No new key. Canonical bodies and sorted object keys already exist in `ModelRequestRecord`; add stable semantic ordinals because the current BTreeMap key contains the varying hash and cannot align changed requests. `Report.metrics` cannot hold lists, hence typed `details`. |
| **64 `cross-run-reproducibility`** | CHEAP; Functionality; **CORE** | Measured for both using the row-63 executions. Concurrent arrival order is ignored, but semantic order is not. | Same executions and exact pointer/type normalization allowlist as row 63. Sort by the complete key `(scenario,actor,semantic_ordinal,checkpoint,attempt)` with the comparison rules stated there. Serialize each full canonical request as an unsigned 64-bit big-endian byte length followed by canonical UTF-8 JSON bytes. Physical attempts remain separate ordered records. | `metrics.cross_run_reproducibility.identical`, `.request_stream_count`, `.attempt_count`; `details.cross-run-reproducibility.stream_sha256_by_run`, `.first_difference`, `.collector_complete_by_run`. | PASS iff every run has equal semantic keys, equal contiguous attempt sets `1..=semantic_attempts_total`, and byte-identical normalized length-prefixed stream SHA-256. An incomplete collector ledger, duplicate key, inconsistent total, or noncontiguous attempt set is `ERROR`; once each ledger is complete, a missing/added semantic request or any other differing stream is `FAIL`. Use full canonical bodies, **not** retry-identity canonicalization, because the latter intentionally removes delivery controls. | No new key. `canonicalize_json` and request logs make comparison cheap. `deterministic_run_id` is manifest+row based and repeats across identical runs, so evidence executions need a separate index occurrence key. |

#### Row-63 normalization pointer-pattern grammar

The dialect content allowlists above use a non-RFC JSON-pointer **pattern** grammar. It is
normative for rows 63 and 64:

```text
pattern  = "" | "/" psegment *("/" psegment)
psegment = "*" | literal
literal  = *(unescaped | "~0" | "~1" | "~2")
```

`unescaped` is any Unicode scalar except `/`, `~`, or `*`. Pattern escapes decode as
`~0` = `~`, `~1` = `/`, and the pattern-only extension `~2` = literal `*`; any other
escape or a nonempty pattern without a leading `/` is invalid. The actual canonical JSON
pointer uses RFC-6901 `~0`/`~1` escaping. A `*` psegment matches exactly one decoded object
key or array-index segment—never zero segments, multiple segments, or part of a segment.
A content pattern matches its named value and, when that value is an object or array,
every descendant string leaf whose pointer has the matched segments as a prefix. Metadata
pointers remain exact string-value pointers and do not acquire descendant matching. Only
string values are ever normalized.

Comparison walks objects by the sorted union of decoded keys and arrays positionally by
zero-based index. There is one exception and therefore one array-reorder occurrence rule:
if two arrays differ in order but have equal length and equal multisets of canonical JSON
element bytes (including multiplicity), the pair contributes exactly one comparable and
one varying occurrence at the array container pointer, with types `array`/`array`; no
descendant occurrence is also emitted. All other array additions, removals, type changes,
and value changes recurse positionally. This single container occurrence is included in
the historically named `comparable_leaf_occurrences`/`varying_leaf_occurrences` counters.

Each execution records `collector_complete_by_run[run]`. It becomes true only after the
external provider collector has observed execution terminalization, shut down cleanly,
and snapshotted its complete request ledger. A false/missing value is `ERROR`. Once true,
absence or addition of a semantic request is measured behavior: row 63 contributes its
single synthetic-root occurrence and row 64 produces `identical=false`/`FAIL`.

### F. Automation-interface ergonomics

Rows 65–70 are explicitly capability-aware. An adapter must declare its surface; AHRB
verifies behavior rather than trusting the declaration. Rows 69 and 71–73 are CORE
observability/security/correctness extensions and cannot earn a badge when unsupported.

| Row / stable ID | Type, pillar, badge impact | Topology handling | Fixture and profile | Exact evidence | Oracle | Manifest and feasibility |
|---|---|---|---|---|---|---|
| **65 `injection-surface`** | NEW; AutomationReadiness; INFORMATIONAL; automation-score component | Measured the same way for both. An impossible/unverified component scores zero. If the base route/auth cannot be injected sufficiently to run any fake-provider trial, row 65 is explicitly `UNSUPPORTED` with score 0; never emit the invalid combination “run unavailable but diagnostic zero.” | Establish a fresh baseline, then isolate one carrier per trial. Provider selector: substitute unique model `ahrb-trap-model`, require it at the fake endpoint, and require the baseline model absent. Base URL: point the declared carrier to a separate AHRB trap listener, require exactly the expected request there and zero at the baseline listener. Credential: fake endpoint accepts only fresh credential B, rejects baseline A, and the harness receives B solely through the declared carrier. Quick/cert run one verified trial per component. | `metrics.injection_surface.provider_score`, `.base_url_score`, `.credential_score`, `.score`, `.verified_components`; `details.injection-surface.verification_cases[]` has exact `{component,method,carrier,baseline_provider_requests,perturbed_provider_requests,expected_endpoint_reached,unexpected_endpoint_requests,credential_accepted,baseline_credential_rejected,secret_in_argv}`. | Component scores: verified environment=1.00, verified CLI flag=0.75, verified generated private config=0.50, impossible/unverified=0.00. Aggregate arithmetic mean. With a runnable baseline, PASS envelope iff all three exact trap trials verify, all component scores are nonzero, and aggregate `>=0.50`; a partial runnable result is informational `FAIL`. Credential in argv fails v1 policy and scores zero regardless of nominal method. | Add typed nested `[capabilities.injection_surface.{provider,base_url,credential}]` method plus carrier locator. Do not infer actual injection from `fake_model.base_url_env`: some adapters receive that env from AHRB but render it into generated config. Existing binding/templates supply the execution path, but behavior must be perturbed. |
| **66 `budget-enforcement`** | NEW; AutomationReadiness; **OPTIONAL FACET** `budgets` | Capability `budget_enforcement` absent -> `UNSUPPORTED` score 0 for either topology. Declared -> test all three budgets through the topology's public headless operation. | Independent token, cost, and time trials in 3/7 repetitions. Limits are quick `{total_tokens:128,cost:1,000 micro-USD,time:2,000 ms}` and cert `{1,024,10,000,5,000}`. “Token” is the fake provider's integer `input_tokens+output_tokens` across completed accepted responses in the semantic turn; cached/reasoning fields do not count unless already included in those two values. Token fixture is at `limit-16`, then one indivisible response `{input:8,output:16}`, so exact observed total at enforcement is `limit+8`. Cost is integer micro-USD `sum(input_tokens*2 + output_tokens*3)` with no rounding; fixture is at `limit-25`, then one response costing 50, so exact observed cost is `limit+25`. The harness receives the same `{input:2,output:3}` micro-USD/token tariff through the declared `[resources.budget_controls.tariff]` carrier; an AHRB-only price is invalid. Time starts immediately before one-shot spawn or accepted daemon submit and stops at AHRB receipt of the single typed terminal. Provider pacing crosses the limit at exactly the configured millisecond. The token/cost stop boundary is completion of the crossing response at the fake provider; the time stop boundary is `public_operation_start + time_limit_ms`. | `metrics.budget_enforcement.token_limit`, `.token_observed`, `.cost_limit_microusd`, `.cost_observed_microusd`, `.time_limit_ms`, `.time_observed_ms`, `.overrun_count`, `.structured_failures`, `.score`; `details.budget-enforcement.cases[]` records repetition, public-operation start, each usage boundary, tariff delivered, terminal receipt, effects, outer kill, and `overrun_observations[]`. For one case/repetition, an overrun observation is either (a) one distinct semantic provider request body completed strictly after that case's stop boundary or (b) one distinct fixture tool effect committed strictly after that boundary. Deduplicate each by its stable semantic request/effect ID; a provider request and an effect are two observations. `overrun_count` is the sum of these per-case counts across all three case kinds and all required repetitions. It does not count the crossing response, typed terminal, excess token units, excess micro-USD units, or elapsed milliseconds. Headline observed values are maxima; `structured_failures` and `overrun_count` are sums; score is the number of case kinds for which every repetition passes divided by 3. | Token/cost PASS only if the one expected crossing response is the last accepted unit, observed totals are exactly `limit+8` / `limit+25`, exactly one typed `budget-exceeded` terminal follows, and no post-boundary request/tool effect occurs. Time PASS iff the terminal receipt lies in `[time_limit_ms, time_limit_ms + max(250 ms, 0.10*time_limit_ms)]` from the defined start, no paced response triggers a later semantic request/effect after the limit, and no outer kill occurs. The observation remains open 1,000 ms after terminal. Every passing case has per-case `overrun_count=0`, and the passing row has aggregate `overrun_count=0`. All cases/repetitions are required. A missing declared control/tariff is `ABSENT`; if the architecture exposes token/time but cannot receive a harness-side tariff, cost is `UNSUPPORTED` and the whole optional row/facet is `UNSUPPORTED` score 0 while token/time diagnostics remain in details. | Add `[resources.budget_controls]` public argv/transport templates, its typed harness-side `tariff` carrier, and the capability key. The runner's outer deadline is only a guard and cannot satisfy this row. |
| **67 `usage-reporting`** | NEW; AutomationReadiness; **OPTIONAL FACET** `usage` | Undeclared `usage_reporting` -> `UNSUPPORTED` score 0; declared -> measured for both. | Run 3 quick / 7 cert repetitions. Each direct turn has one response `{input_tokens:100,output_tokens:20,total:120}`. Exactly one tool turn has a tool-call response `{100,20,120}` plus final response `{140,30,170}`, so its per-turn usage is `{240,50,290}` and counts as one semantic turn. Quick has 3 turns (2 direct + tool): exact per-repetition totals `{input:440,output:90,total:530,cost_microusd:1,150,turns:3}`. Cert has 20 turns (19 direct + tool): `{input:2,140,output:430,total:2,570,cost_microusd:5,570,turns:20}`. Cost uses the same harness-delivered tariff `{input:2,output:3}` micro-USD/token as row 66, with exact integer arithmetic. Extract from structured event/output only, never request count. `[events.metadata].usage_event` names the normalized event vocabulary carrier and `usage_scope` is `turn` or `cumulative-run`; resolve all five pointers against the unmodified raw event that produced that normalized carrier. Exactly one carrier is required after every completed semantic turn. With `turn`, its values describe only that turn and are summed within the repetition. With `cumulative-run`, values describe the run through that turn, must be component-wise nondecreasing, consecutive differences describe each turn, and the last carrier supplies the repetition headline. | `metrics.usage_reporting.input_tokens`, `.output_tokens`, `.total_tokens`, `.cost_microusd`, `.turns`, `.crosscheck_errors`, `.score`; `details.usage-reporting.source_pointers`, `.usage_event`, `.usage_scope`, `.repetitions[]`, and ordered per-turn/per-response extracted values. Headline metrics are the one exact **per-repetition** total above, not a sum across repetitions; because all repetitions must agree, the report emits that common value. | Each of the five fields is correct only if it is a machine-readable nonnegative integer and equals the exact profile value in every repetition; total must also equal input+output and the tool-turn value (direct for `turn`, consecutive difference for `cumulative-run`) must equal its two-response sum. Cost must equal the harness-visible tariff calculation. A declared pointer, carrier, or scope whose field/event is absent or non-readable is missing evidence and produces `ERROR`; a present, readable but wrong value is `FAIL` with that field's score zero. Multiple carriers for one turn are contradictory evidence and `ERROR`. Score is correct fields/5 only for complete evidence; any crosscheck error prevents PASS. Repetitions must be byte/value identical after evidence ordering. | Add independent `[events.metadata]` usage pointers, `usage_event`, and `usage_scope`; reuse the declared harness-side tariff carrier and remove the ambiguous AHRB-only `resources.test_price_microusd_per_token`. Usage extraction must coexist with terminal normalization. |
| **68 `session-ops-cli`** | NEW; AutomationReadiness; **OPTIONAL FACET** `session-cli` | Undeclared `session_ops_cli` -> `UNSUPPORTED`. A private daemon RPC is not a CLI. Declared operations are invoked as direct argv in both topology families. | One lifecycle quick, five cert: create -> submit and durably commit one nonempty seed tool turn -> list -> resume at the committed cursor and verify that seed -> fork from that committed cursor -> diverge original/fork -> delete both -> second delete. The seed contains a correlated tool call/result plus terminal, not an empty transcript, metadata-only session, or uncommitted pending call. | `metrics.session_ops_cli.create_ok`, `.list_ok`, `.resume_ok`, `.fork_ok`, `.delete_ok`, `.score`; each `*_ok` is 1 only if **every** required repetition passes that operation. `details.session-ops-cli.lifecycles[]` contains one record per repetition with `{repetition,original_id,fork_id,seed_call_id,seed_result_digest,committed_cursor,original_history_hashes,fork_history_hashes,operation_results[]}`; each operation result includes `{operation,exit_code,terminal_type,extracted_ids,cursor}`. Singular top-level `original_id`/`fork_id` fields are forbidden because they cannot represent cert repetitions. | PASS iff create returns stable ID; the seed tool turn is committed and replayable; list extraction yields an array containing the original ID exactly once; resume preserves identity/cursor and reproduces the committed seed call/result; fork returns a different ID whose nonempty transcript/event history at fork time is an **exact ordered prefix** of both later original and fork histories, followed by isolated divergence; delete removes each ID from list and prevents resume; repeated delete matches declared `delete_missing_semantics` (`typed-not-found` or `idempotent-success`). An empty history/cursor can never satisfy resume or fork. All five booleans must be 1; score=sum/5. | Extend `[sessions]` with `fork`, `delete`, `fork_id_pointer`, `list_array_pointer`, `list_item_id_pointer`, `delete_missing_semantics`, and, for typed failure, `not_found_pointer/value`. Existing create/list/resume/close are starting points. `Driver` lacks generic list/fork/delete-by-id. Daemon mock may honestly be UNSUPPORTED if it exposes only stdin-RPC; mock-exec must pass. |
| **69 `event-stream-completeness`** | CHEAP; AutomationReadiness; **CORE**; automation-score component | Measured for both on their declared durable journal/event stream. No machine event stream is `ABSENT`. A stream whose adapter declares neither assistant-text nor reasoning capture points is badge-blocking `UNSUPPORTED`, not a fabricated narrative zero. | One successful tool turn plus one typed failure; quick/cert 3/7 repetitions. The fake provider emits deterministic assistant text on every response and deterministic reasoning/thinking content through every supported provider frontend; the latter is required only because this fixture emits it. The external normalizer records receipt monotonic/wall bounds as collector metadata. | Seven 0/1 metrics: `metrics.event_stream_completeness.tool_call_id`, `.correlated_result`, `.timestamps`, `.usage`, `.terminal_typing`, `.schema_version`, `.narrative_reconstructability`; plus `.score`. A component is 1 only if every repetition passes it. `details.event-stream-completeness.missing_components`, `.component_failures[]`, and `.narrative_declaration` retain receipt bounds and whether text/reasoning were declared. | The original six component definitions are unchanged. Narrative reconstructability=1 iff the durable stream reconstructs byte-exact assistant text and every provider-emitted reasoning/thinking item, in response order, with the declared turn locator resolving to the producing turn for both success and failure. A `complete-event` carrier contributes one complete item per record; `item-deltas` concatenates records in durable order per declared item identity and preserves first-item order. Optional `*_match_fields` apply scalar JSON-pointer predicates before extracting a side, allowing one normalized event class to carry differently typed text and reasoning records. A text-only or reasoning-only declaration is valid evidence but scores this component zero whenever the fixture emitted the missing side; it is a measured `FAIL`, not `UNSUPPORTED`. Merely recording tool calls/results or terminal metadata is insufficient. `passed_components` is the integer sum and score=`passed_components/7`. PASS iff `passed_components >= 5` and the hard quartet tool-call ID, correlated result, terminal typing, and narrative reconstructability are all 1. Thus a previously perfect metadata-only stream changes from 1.0 to 6/7 and fails the new CORE gate, while each legacy facet retains exactly its old value and meaning. | Add `[events.narrative]` with at least one complete assistant-text or reasoning event/value pair, its aggregation (`complete-event` or `item-deltas`), the conditional item-ID pointer for `item-deltas`, optional side-specific `assistant_text_match_fields` / `reasoning_match_fields`, and `turn_pointer`. Declare both sides only when both are evidenced. All event names are normalized vocabulary values; all pointers and predicates resolve against the unmodified raw durable record. `[events.metadata]` remains optional exactly as before. No engine branch may name a harness. |
| **70 `headless-permission-model`** | NEW; AutomationReadiness; **OPTIONAL FACET** `permissions` | Undeclared `headless_permission_model` -> `UNSUPPORTED`; v1 row 34 remains CORE and still tests no prompting. Declared -> measured for both. | Closed stdin/no PTY. Run three independent cases: allowed write under `{{workspace}}`; denied write to `{{outside_path}}`; denied TCP connection to the AHRB-owned `{{blocked_host}}:{{blocked_port}}`. Quick once each, cert five each. Test scoped yolo in a fourth case only when declared; never grant host-wide access. Permission arrays are complete replacement argv commands, not appended fragments. | `metrics.headless_permission_model.score`, `.tty_prompts`, `.allowed_effects`, `.denied_filesystem_effects`, `.denied_network_effects`, `.scope_violations`; effect counters are sums over repetitions. `details.headless-permission-model.mode`, `.cases[]` with `{repetition,case,argv,exit_code,terminal_type,effect}` (credentials redacted). | Score 1.00 iff allow-list granularity is demonstrated and both filesystem/network denial cases pass; 0.75 iff a sandbox denies both classes but has no per-operation allow list; 0.50 iff workspace/profile-scoped yolo permits the allowed case without widening outside/network scope; 0.25 iff merely noninteractive with no enforceable control; 0 on prompt/hang. PASS iff score>=0.50, `allowed_effects == repetitions`, `denied_filesystem_effects == repetitions`, `denied_network_effects == repetitions`, TTY prompts=0, and scope violations=0. | Add `[permissions]` mode and complete replacement argv `allow`, `deny_filesystem`, `deny_network`, plus optional `yolo`. Each template has the matching required placeholder. Current `next_input`/tools do not describe permission flags. |
| **71 `secrets-hygiene-on-disk`** | CHEAP; AutomationReadiness; **CORE** | Measured for both after successful and provider-failure runs. Daemon scan occurs after session close and again after shutdown. No N/A. | Inject a unique high-entropy credential. Before redaction, byte-scan stdout, stderr, journal, session files, and declared logs; scan regular files only and do not follow symlinks outside isolated roots. Quick one success+failure, cert three each. | `metrics.secrets_hygiene_on_disk.files_scanned`, `.bytes_scanned`, `.stdout_matches`, `.stderr_matches`, `.journal_matches`, `.session_matches`, `.log_matches`, `.declared_carrier_files`; `details.secrets-hygiene-on-disk.matches[]` contains category/path/offset with secret bytes omitted. | An omitted or empty `capture.credential_carrier_paths` is `ABSENT`, not a verified zero-carrier claim. Otherwise PASS iff every match count except `declared_carrier_files` is zero and credential is absent from argv. Exact 0600 generated provider/auth files whose templates contain `{{credential}}` are declared carriers and may be excluded by exact path only; no directory/glob exemption and no stdout/journal/session/log carrier is allowed. | Add nonempty `capture.credential_carrier_paths`, each profile-contained and matching a generated file. Existing profile roots, generated files, capture redaction, and raw artifacts make the scan cheap. `mock-exec` currently permits credential argv and must move it to environment/config injection. |
| **72 `tool-result-role-fidelity`** | CHEAP; ToolCallCorrectness; **CORE** | Measured for both from what the fake model receives; no topology N/A. | Each repetition has one successful and one failed tool call; quick one repetition, cert five. Inspect the next accepted primary request after each. | `metrics.tool_result_role_fidelity.checks`, `.violations`, `.plain_user_text_violations`, `.missing_results`, `.duplicate_results`; counters are sums and `checks` must equal exactly `2 * repetitions`. `details.tool-result-role-fidelity.observations[]` has repetition/dialect/call ID/semantic role/raw pointer. | PASS iff `checks == 2*repetitions`, every call has exactly one matching structured tool result in the next legal request, and there are zero violations. Protocol-native TOOL semantics are: Chat `role:"tool"`; Responses `function_call_output`; Anthropic typed `tool_result` content (although its wire envelope has `role:"user"`). ID mismatch, duplicate, missing, or untyped pasted user text fails; omitted calls cannot yield vacuous PASS. | No new key. Canonical fake request logs already contain the needed bodies. The oracle must be dialect-aware; requiring the literal Chat role would incorrectly fail Anthropic. |
| **73 `compaction-transparency`** | NEW; AutomationReadiness; **CORE**; not an automation-score component | Measured for both by reusing row 51's topology-neutral forced context-limit recovery. The provider request delta proves compaction independently of the harness signal. If all complete repetitions avoid compaction, the row is badge-blocking `UNSUPPORTED`; absence is never a PASS. | Quick/cert reuse the 3/7 row-51 growing sessions, provider request records, and durable events. Each completed repetition produces a row-73 observation even when no context error or accepted retry exists; row 51 may independently be `ERROR`. No second long workload is run when rows 51 and 73 are both selected. | `metrics.compaction_transparency.compactions_observed`, `.announcements`, `.scoped_announcements`, `.correlated_announcements`, `.score`; `details.compaction-transparency` has `measurement_complete`, `compaction_observed`, and `observations[]` entries `{repetition,announcement_id,correlated,scoped}`. | A compaction is externally observed when an accepted smaller request omits at least one old history marker. Tier 0 (score 0): no signal, duplicates, or a signal not correlated to its turn. Tier 1 (score 0.5): exactly one declared durable/stream marker per compaction, correlated to the affected turn, but none or not all accurately state what was affected. Tier 2 (score 1): every correlated announcement's declared count equals the externally omitted-marker count and/or its declared span endpoints equal the first/last omitted markers; if both forms are declared, both must agree. PASS requires tier 2 in every repetition: all four counts equal repetitions and score=1. Partial compaction is `ERROR`; a complete run with zero observed compactions is badge-blocking `UNSUPPORTED`; silent, announcement-only, or false-scope compaction is `FAIL`. | Add optional `[events.compaction]` with `event="context-compacted"`, required `turn_pointer`, optional `dropped_count_pointer`, and paired optional `dropped_span_start_pointer` / `dropped_span_end_pointer`. An empty scope declaration is valid announced-only data. Native signals or markers normalize to `context-compacted` through ordinary adapter rules. Omission does not suppress the trial: an observed compaction then scores tier 0. Engine code never special-cases a harness. |

#### Row-69 real-adapter declarations

The cited full-matrix reports below predate revision 2.5. Their row-69 fixture has the
same three success/failure actor pairs but did not emit the new pre-tool text or
reasoning. Offline replay must therefore report the historical evidence as it exists;
it is not a prediction that a newer harness build will discard the new fixture. Later
saved reports for these adapters are incomplete runs and replay at 0/7; the table
records the result across every saved report. All declared narrative carriers are
complete events. None of these historical reports contains a reasoning/thinking
narrative record. Codex, Claude Code, and Pi nevertheless declare the reasoning shape
documented by their event dialect because the current row-69 fake response emits it;
replay still reports only what the old journals contain.

| Harness | Declaration | Stored evidence | Offline row 69 | Expected next live run |
|---|---|---|---|---|
| codex | text and reasoning `model-response` at `/item/text`; `complete-event`; turn `/actor` | `results/codex/2026-09-02T13:22:50Z-0815bb8f/report.json`: raw `item.completed`, `item.type=agent_message`; exec schema also defines `item.type=reasoning` with `text` | 3/3 `FAIL`: cited report 2/7, two incomplete reports 0/7; narrative 0 throughout | `PASS` narrative when current `reasoning` items are journaled |
| claude-code | text at `/_ahrb_expanded/text`, reasoning at `/_ahrb_expanded/thinking`; both `model-response`, `complete-event`; turn `/actor` | `results/claude-code/2026-09-02T12:59:20Z-4774d870/report.json`: raw `assistant`, expanded `type=text`; documented Messages content also has `type=thinking` | 3/3 `FAIL`: cited report 2/7, two incomplete reports 0/7; narrative 0 throughout | `PASS` narrative when current thinking blocks are journaled |
| opencode | text `model-response` at `/part/text`; `complete-event`; turn `/actor`; no reasoning | `results/opencode/2026-09-02T14:13:54Z-d5342cc3/report.json`: raw `type=text`, `part.type=text` | 4/4 `FAIL`: cited report 2/7, three incomplete reports 0/7; narrative 0 throughout | partial unless a captured reasoning part is declared |
| pi | text at `/_ahrb_expanded/text`, reasoning at `/_ahrb_expanded/thinking`; both `model-response`, `complete-event`; turn `/actor` | `results/pi/2026-09-02T13:13:12Z-ca742db2/report.json`: authoritative raw `message_end` with typed content; Pi 0.84.4 documents both `text` and `thinking` blocks | 3/3 `FAIL`: cited report 2/7, two incomplete reports 0/7; narrative 0 throughout | `PASS` narrative when current thinking blocks are journaled |
| rick | text `model-response` at `/text`; `complete-event`; turn `/actor`; no reasoning | `results/rick/2026-09-02T13:19:20Z-6208e81c/report.json`: raw `type=text` | 3/3 `FAIL`: cited report 2/7, two incomplete reports 0/7; narrative 0 throughout | partial unless a reasoning event is captured and documented |
| haider-agent | no narrative declaration | `results/haider-agent/2026-09-02T17:06:20Z-3eac4351/report.json`: only final completed `agent_message` at `/payload/item/text`; no pre-tool text/reasoning; `state=thinking` is lifecycle metadata | 21/21 `UNSUPPORTED` | `UNSUPPORTED` until the journal exposes the complete narrative |
| oh-my-pi | none | no stored report and no documented event rules | not replayed | `UNSUPPORTED` pending evidence |
| goose | none | no stored report and no documented event rules | not replayed | `UNSUPPORTED` pending evidence |
| cline-cli | none | no stored report and no documented event rules | not replayed | `UNSUPPORTED` pending evidence |
| aider | none | no stored report and no documented event rules | not replayed | `UNSUPPORTED` pending evidence |
| deepseek-harness | none | no stored report and no documented event rules | not replayed | `UNSUPPORTED` pending evidence |

### G. CLI features (not matrix rows)

#### G0. Offline row-69 replay

`ahrb replay --manifest MANIFEST --input REPORT` loads the adapter declaration and the
saved schema-3 report, deserializes its persisted normalized `events`, selects the
profile's normative 3/7 repetitions, and runs the same row-69 evaluator used by a live
matrix. It starts no harness and mutates neither the report nor the result store. The
JSON result reports `PASS`, `FAIL`, `ERROR`, or `UNSUPPORTED` plus the seven metrics and
details when measured. This path exists specifically so declaration changes can be
checked against captured real streams in a socket-restricted environment.

#### G1. `hbench diff`

The commands are:

```text
hbench diff <harness>[@<run-key|version>] <harness>[@<run-key|version>]
hbench diff --latest <harness>
```

`results/index.jsonl` remains append-only and is the resolver source of truth. New
writers emit **index schema 2** canonical JSON with these exact fields:

```text
schema, run_key, completed_at, harness, harness_version, report_path,
report_schema, spec_version, profile, os, topology, resource_summary, metrics,
manifest_sha256, workflow_sha256, ahrb_revision
```

The historical index-v1 format is an untagged line with
`harness_id,harness_version,manifest_hash,ahrb_revision,platform,profile,timestamp,badge,results_dir`.
Readers MUST accept both forms without rewriting or truncating the file. In memory, a
legacy line is migrated to schema 2 as follows: `harness=harness_id`,
`report_path=results_dir + "/report.json"`, `completed_at=timestamp`, default
`report_schema=2`, `spec_version=1`, `workflow_sha256="legacy-unavailable"`, OS from
badge then platform, and topology from badge or `"unknown"`. If its report is readable,
the report's schema/spec/profile/topology/fingerprints override those defaults. Its
stable key is `"legacy-" + sha256("ahrb-index-legacy-v1\\0" || little_endian_u64(one_based_line_number) || "\\0" || exact_raw_line)`.
Malformed/unknown tagged lines are errors; migration never appends a replacement line.
While holding the exclusive index append lock, a writer allocates the one-based physical
line ordinal `occurrence` (the number of existing newline-terminated lines plus one).
Schema-2 entries use `"run-" + sha256("ahrb-index-v2-occurrence\\0" ||
little_endian_u64(occurrence) || "\\0" || harness || "\\0" || completed_at || "\\0" ||
report_path || "\\0" || sha256(exact persisted report.json bytes))`. The ordinal makes
the key occurrence-unique even when two byte-identical reports finish in the same second;
the report digest also binds the key to the persisted body.
`completed_at` for new entries is captured only after the report bundle has been fully
persisted; it is not the run-start timestamp. It is exactly 20 ASCII bytes in UTC Gregorian
form `YYYY-MM-DDTHH:MM:SSZ`: four-digit year 1970–9999, zero-padded fields, literal `T`/`Z`,
no offset or fractional seconds, and seconds 00–59. Readers reject a non-canonical or
invalid calendar value. Selection compares parsed Unix seconds numerically, then compares
`run_key` lexicographically only as the same-second tie-break.

`run_key` is unique per completed occurrence; the deterministic report `run_id` is not,
because identical manifest+row selections may repeat it. Resolution after `@` first
tries exact `run_key`, then exact `harness_version`; a version selects the latest
`completed_at`, ties broken lexicographically by `run_key`. An unresolved or ambiguous
selector is exit 2. An unqualified operand selects that harness's latest completed record
by `completed_at`, with the same run-key tie-break. `--latest` first selects the latest
record, then selects the newest earlier record for the harness with equal OS, topology,
profile, and report schema; it never silently crosses those boundaries. Fewer than two
compatible records is exit 2.

Reports with schema 2 and 3 are both readable. Explicit operands may compare schema
2↔3 row states; fields absent from schema 2 render `unavailable`. `--latest` deliberately
requires equal report schema in addition to OS/topology/profile so it chooses a genuine
like-for-like predecessor. Other report schemas are exit 2.

Diff output is canonical JSON by default:

```json
{
  "schema": 1,
  "left": {},
  "right": {},
  "comparison_scope": "within-topology-only",
  "rows": [
    {"row": 42, "id": "model-request-efficiency", "badge_impact": "informational", "before": "PASS", "after": "FAIL", "change": "informational-regression"}
  ],
  "resource_summary_deltas": {
    "wall_per_turn_p95_ms": {"before": 120.0, "after": 150.0, "delta": 30.0, "delta_pct": 25.0}
  }
}
```

Rows are joined by stable ID and sorted by `(right.row if present else left.row, id)`, so
removed rows have a deterministic position; additions/removals are explicit. State
changes use this total table (before is left, after is right):

| Condition | `change` |
|---|---|
| identical state/evidence class | `unchanged` |
| CORE or declared OPTIONAL `PASS -> FAIL|ERROR|ABSENT`, or `PASS -> UNSUPPORTED` while still declared | `regression` |
| CORE or declared OPTIONAL `FAIL|ERROR|ABSENT -> PASS`, or declared `UNSUPPORTED -> PASS` | `improvement` |
| OPTIONAL becomes declared and passes | `facet-added` (improvement) |
| OPTIONAL becomes declared but ends `FAIL`, `ERROR`, `ABSENT`, or declared `UNSUPPORTED` | `facet-added-nonpass` (neutral) |
| OPTIONAL becomes undeclared and changes to honest `UNSUPPORTED` | `facet-removed` (regression only if it previously PASSed; otherwise neutral) |
| transitions among `FAIL`, `ERROR`, `ABSENT`, and declared `UNSUPPORTED` without reaching/leaving PASS | `changed-nonpass` |
| INFORMATIONAL envelope `PASS -> FAIL` / `FAIL -> PASS` | `informational-regression` / `informational-improvement` |
| INFORMATIONAL `PASS|FAIL -> ERROR|ABSENT` / `ERROR|ABSENT -> PASS|FAIL` | `evidence-regression` / `evidence-improvement` |
| INFORMATIONAL `PASS|FAIL -> UNSUPPORTED` / `UNSUPPORTED -> PASS|FAIL` | `evidence-regression` / `evidence-improvement` |
| INFORMATIONAL transitions among `ERROR`, `ABSENT`, and `UNSUPPORTED` | `changed-nonpass` |
| row only on right / only on left | `added` / `removed` (gating added-PASS is an improvement; removed-PASS is a regression; other additions/removals are neutral) |

Honest undeclared optional `UNSUPPORTED -> UNSUPPORTED` is neutral. Resource fields are
sorted by exact name; old missing values are `null` with `change:"unavailable"`.
Absolute and percentage deltas are emitted only when both numbers exist; zero baseline
has `delta_pct:null`. Incompatible or unknown OS, topology, or **profile** reports still
receive a row-state diff, but every resource delta is `not-comparable` and no ranking is
emitted. The empty string and exact migrated sentinel `"unknown"` are unknown; two unknown
values never make a resource comparison eligible. Resource comparison never crosses
quick/cert even for explicit selectors. Exit is 0
with no gating regression, 1 with at least one gating regression, and 2 for
resolution/schema/I/O errors.

#### G2. Composite automation score and badge classes

Each row 65–72 supplies a `[0,1]` subscore in its own topology:

Revision 2.5 deliberately leaves this eight-component mean unchanged. Row 69's
denominator changes from six to seven because narrative is a new component; row 73 is a
direct CORE badge gate and does not become a ninth automation-score component.

| Row | Subscore |
|---:|---|
| 65 | declared/verified injection aggregate |
| 66 | passed token/cost/time cases divided by 3; undeclared=0 |
| 67 | correct input/output/total/cost/turn fields divided by 5; undeclared=0 |
| 68 | passed create/list/resume/fork/delete operations divided by 5; undeclared=0 |
| 69 | seven event components divided by 7 |
| 70 | permission granularity score; undeclared=0 |
| 71 | 1 on complete `PASS`, 0 on complete `FAIL`; otherwise unavailable |
| 72 | 1 on complete `PASS`, 0 on complete `FAIL`; otherwise unavailable |

The automation score is the equal-weight arithmetic mean times 100, rounded half up to
an integer: `floor(mean*100+0.5)`, `A0` through `A100`. Score-state mapping is exact:

| Component row state | Value used by A |
|---|---:|
| complete `PASS` or `FAIL` with the row-defined valid `[0,1]` score | that score |
| honest row-permitted `UNSUPPORTED` on rows 65–68/70 | 0 |
| complete boolean row 71/72 `PASS` / `FAIL` | 1 / 0 |
| `ERROR`, `ABSENT`, missing result, invalid/out-of-range score, or a score absent where the row requires one | A is `null`/unavailable |

Row 65 retains a legitimately partial informational score on `FAIL`; CORE row 69 also
retains its exact partial diagnostic score on `FAIL`, but that failure withholds the badge.
Row-69 `UNSUPPORTED` makes A unavailable rather than silently inserting zero.
declared optional rows 66–68/70 retain their passed-case score on a complete behavioral
`FAIL`; undeclared optional capability `UNSUPPORTED` is zero. A missing component never
silently becomes zero. Any unavailable A prevents a v2 badge.

L and C come from rows 43 and 46. The topology-scoped marginal peak effective-memory input
to R is measured in MiB, and its bounds are normative and inclusive: `R32` iff the input
is `<=32`; `R96` iff it is `>32` and `<=96`; `R256` iff it is `>96` and `<=256`; otherwise
`R256+`. Numeric mirror maps
contain only f64 values; L/C strings and nullable A live in typed details/badge fields.
All four are tied to the report's profile and printed OS/topology and MUST NOT be placed
on a topology-, OS-, or profile-erasing leaderboard.

## 5. Badge v2 and result policy

The badge label is:

```text
Automation Ready v2 · <os> · <topology> · <profile> · N<width> · R<class> · L<class> · C<class> · A<0..100> · <facets>
```

Example:

```text
Automation Ready v2 · macos · client-process-fanout · quick · N8 · R96 · L500 · C50 · A82 · replay+crash+resume+budgets
```

The v1 R class names and the exact normative G2 bounds remain
`R32 | R96 | R256 | R256+`. L classes are
`L100 | L250 | L500 | L1000 | L1000+`; C classes are
`C10 | C50 | C250 | C250+`. The A integer is defined by G2. If no facet passes, omit the
final separator and facet segment rather than printing an empty suffix.

### New-row certification roles

- New CORE rows: **44, 48–55, 57–62, 64, 69, 71–73**.
- New OPTIONAL FACET rows: **56** (`native_delegation`), **66** (`budgets`),
  **67** (`usage`), **68** (`session-cli`), **70** (`permissions`).
- New INFORMATIONAL rows: **42, 43, 45–47, 63, 65**.

Row 56 shares the existing `native-delegation` badge suffix with row 18: both must pass
when the capability is declared. New suffixes appear after the v1 stable suffixes in this
order: `budgets`, `usage`, `session-cli`, `permissions`. Undeclared optional facets are
omitted.

A v2 badge is awarded iff:

1. every applicable v1 CORE row 1–41 passes under the unchanged v1 rules;
2. every new CORE row passes;
3. every declared OPTIONAL row passes and every undeclared optional non-PASS is exactly
   an honest `UNSUPPORTED`;
4. every class/score input was measured without infrastructure `ERROR`;
5. width is at least N8 and topology/lifecycle declarations are consistent.

An INFORMATIONAL `FAIL` does not suppress the badge and does not make the default suite
exit nonzero; its class/reference-envelope miss remains visible. Any `ERROR`, or any
CORE/declared-optional `FAIL`, makes the suite exit nonzero. As in v1, a badge-blocking
`UNSUPPORTED`/`ABSENT` without observed `FAIL`/`ERROR` does not by itself change exit 0.
This distinction lets automation separate “harness behavior failed” from “this build
cannot claim the complete badge.”

The badge JSON object adds exact fields:

```text
spec_version: 2
profile: "quick" | "cert"
latency_class: string
cpu_class: string
automation_score: integer 0..100
```

It retains `os`, `topology`, `parallel_width`, `resource_class`, `facets`, and
`comparison_scope`. Class and score comparisons require equal OS, topology, and profile.

## 6. `report.json` and evidence bundle v2

AHRB spec version and report schema version are independent. Real v1 bundles in this
repository already use `report.schema = 2`; therefore a v2 report emits
**`report.schema = 3`** and `report.spec_version = 2`. Readers MUST continue to accept
schema 2 as v1 evidence.

### Top-level and row changes

`report.json` retains every v1 field and adds:

```text
spec_version: u32                         # exactly 2
details: ReportDetails                    # named typed row-ID blocks plus reserved globals
turns: Vec<TurnObservation>
stream_chunks: Vec<StreamChunkObservation>
filesystem_snapshots: Vec<FilesystemSnapshot>
egress_attempts: Vec<EgressAttempt>
```

`ReportDetails` is a named-block JSON object. Row blocks use stable row IDs; the reserved
global block names are `resource-summary` and `automation-score`. Each known key has a
schema-defined typed object; it is not an unvalidated dumping ground. A writer MUST
serialize only fields defined by the consuming row/global block, and a reader validates
their types while retaining unknown future keys for forward compatibility. Wave 1 introduces typed blocks for
`model-request-efficiency`, `process-hygiene`, `time-to-first-model-request`,
`memory-time-integral`, `nondeterministic-field-report`,
`cross-run-reproducibility`, `resource-summary`, and `automation-score`; each later
wave adds its blocks before its evaluator. Strings, bools, lists, nullable ratios/N
values, and classes belong here or in typed `resource_summary`, never in an f64 mirror.

Independent of badge award, `details.automation-score` is exactly
`{profile,topology,comparison_scope,score}` where comparison scope is
`"within-topology-only"` and score is the G2 integer or null on `ERROR`, `ABSENT`, a
missing component/result, or an invalid/missing required component score. Thus an
unbadged report never loses automation-score scope.

The same raw vectors are written as `turns.jsonl`, `stream-chunks.jsonl`,
`filesystem-snapshots.jsonl`, and `egress-attempts.jsonl`. Embedded and JSONL forms are
generated from the same in-memory vector and must hash to equivalent records.

Each `results[]` object adds:

```text
requirement: "core" | "optional-facet" | "informational"
capability: string | null
capability_declared: boolean | null         # optional facet declaration; null otherwise
measurement_complete: boolean              # false requires outcome ERROR or ABSENT
score: number | null                      # [0,1] when the row is graded
reference_envelope_pass: boolean | null   # null for non-informational rows
```

For every schema-3 row whose `capability` is non-null, `capability_declared` is `true` iff
that exact key occurs in manifest `capabilities.required` or `capabilities.optional`, and
is `false` otherwise; it records declaration independently of whether the declared
operation surface is usable. `hbench diff` uses only this structured field for OPTIONAL
transition classification and never parses `evidence`. `evidence` remains a
deterministically sorted list of human-readable references. Numeric
machine comparisons use the exact `metrics`/`resource_summary` fields, not parsed prose.
Booleans in the existing numeric `metrics` map are encoded as `0.0` or `1.0`; structured
strings/lists belong in `details`.

`TurnObservation` fields are exactly:

```text
repetition, turn_index, actor, session_id_hash, phase, launch_ns, submit_ns,
first_model_request_ns, terminal_ns, exit_ns, turn_wall_ns
```

Secret/session values are hashed or replaced with non-secret stable evidence IDs.
`StreamChunkObservation` fields are `repetition, actor, case, ordinal, scheduled_ns,
frame_yielded_ns, bytes`. These are fake-provider `Body::poll_frame` boundaries and make
no claim about kernel write completion or when the black-box harness read the bytes.
`FilesystemSnapshot` fields are `repetition, boundary, category,
path_under_profile, device_id, inode_or_file_id, size_bytes, sha256`; the two identity
fields are unsigned integers and are required on supported Unix certification hosts.
`EgressAttempt` fields are `repetition,
monotonic_ns, destination, category, allowed, enforcement`.

`model-requests.jsonl` keeps the v1 record and adds exact fields:

```text
semantic_ordinal, attempt, received_ns, body_bytes, role, side_channel_kind,
response_status, response_first_frame_yield_ns, response_last_frame_yield_ns,
semantic_attempts_total
```

Unlike v1's aggregate-only `attempts`, v2 writes one record per physical attempt while
repeating the final total number of physical attempts for that semantic request as
`semantic_attempts_total` on every attempt record. Arrival-time
records are sorted for output by semantic key then attempt; their timestamps remain raw
evidence and never determine workflow routing.

### New `resource_summary` fields

The following names and types are exact. `resource_metrics` is
`BTreeMap<String,TopologyMetric>`, where `TopologyMetric` is exactly
`{value:f64,profile:string,topology:string,comparison_scope:"within-topology-only"}`.
Only non-null numeric f64 resource fields are copied into that map with the same name.
Strings, bools, integer counts/N values, and nullable ratios/N values are never placed
in `resource_metrics`; they remain typed in `resource_summary` and/or `details`.
Fields typed `f64` below are required when their source row has
`measurement_complete=true`; a current incomplete/`ERROR` row omits its derived field
instead of serializing a favorable zero, and its typed details retain the diagnostic.

| Exact field | Type | Source row |
|---|---|---:|
| `topology` | string | all resource rows |
| `profile` | string, `quick` or `cert` | all resource rows |
| `comparison_scope` | string, exactly `within-topology-only` | all resource rows |
| `wall_per_turn_p50_ms` | f64 | 43 |
| `wall_per_turn_p95_ms` | f64 | 43 |
| `wall_per_turn_max_ms` | f64 | 43 |
| `wall_per_turn_mad_ms` | f64 | 43 |
| `wall_per_turn_jitter_ratio` | f64 | 43 |
| `latency_class` | string | 43 |
| `time_to_first_model_request_p50_ms` | f64 | 45 |
| `time_to_first_model_request_p95_ms` | f64 | 45 |
| `time_to_first_model_request_max_ms` | f64 | 45 |
| `memory_time_integral_mib_s_per_turn` | f64 | 46 |
| `memory_time_integral_coverage_ratio` | f64 | 46 |
| `memory_time_integral_max_sample_gap_ms` | f64 | 46 |
| `cpu_per_turn_p50_ms` | f64 | 46 |
| `cpu_per_turn_p95_ms` | f64 | 46 |
| `cpu_class` | string | 46 |
| `disk_write_bytes_per_turn_p50` | f64 | 47 |
| `disk_write_bytes_per_turn_p95` | f64 | 47 |
| `disk_write_bytes_per_turn_max` | f64 | 47 |
| `session_journal_growth_bytes_per_turn` | f64 | 47 |
| `log_growth_bytes_per_turn` | f64 or null | 47 |
| `disk_write_growth_slope_bytes_per_turn2` | f64 | 47 |
| `disk_io_counter_complete` | bool | 47 |
| `unbounded_disk_growth` | bool | 47 |
| `model_wait_cpu_p50_ms` | f64 | 48 |
| `model_wait_wall_p50_ms` | f64 | 48 |
| `model_wait_cpu_one_core_max_ratio` | f64 | 48 |
| `large_tool_output_peak_rss_delta_mib` | f64 | 60 |
| `latency_slope_ms_per_100_turns` | f64 | 49 |
| `latency_last_first_decile_ratio` | f64 or null | 49 |
| `session_residue_slope_mib_per_session` | f64 | 50 |
| `session_residue_final_mib` | f64 | 50 |
| `session_store_byte_slope_per_session` | f64 | 50 |
| `session_store_file_count_slope_per_session` | f64 | 50 |
| `session_store_final_residue_bytes` | f64 | 50 |
| `session_store_final_residue_files` | u64 | 50 |
| `resume_latency_p50_ms` | f64 | 52 |
| `resume_latency_p95_ms` | f64 | 52 |
| `resume_latency_slope_ms_per_turn` | f64 | 52 |
| `fanout_cliff_n_rss` | u32 or null | 54 |
| `fanout_cliff_n_wall` | u32 or null | 54 |
| `fanout_max_local_rss_alpha` | f64 | 54 |
| `fanout_max_local_wall_alpha` | f64 | 54 |
| `fanout_global_rss_alpha` | f64 | 54 |
| `fanout_max_measured_n` | u32 | 54 |
| `fairness_latency_cv` | f64 | 55 |
| `fairness_latency_max_min_ratio` | f64 or null | 55 |
| `fairness_latency_spread_ms` | f64 | 55 |
| `fairness_starved_agents` | u32 | 55 |

Legacy `peak_rss_mib` retains its name for compatibility even though its effective
comparison value is footprint on macOS and PSS/RSS on Linux. New code must document that
fact rather than renaming old data.

All non-null f64 topology-scoped fields named in this table are mirrored in
`resource_metrics`. In particular `latency_class`, `cpu_class`, bools, nullable row-49/
55 ratios, row-54 optional cliff Ns, and integer counts are excluded. There are no
row-42–73 numeric resource-only aliases outside this exhaustive list.

### Exact new `metrics` keys

These are full keys inside the existing top-level numeric `metrics` map. Implementations
MUST NOT synthesize dynamic keys for actors, N values, signals, or attempts; those
records belong in `details` or the raw vectors.

```text
# 42
model_request_efficiency.requests_per_semantic_turn
model_request_efficiency.primary_requests_per_turn
model_request_efficiency.side_channel_requests_per_turn
model_request_efficiency.retry_attempts_per_turn
model_request_efficiency.request_body_bytes_p50
model_request_efficiency.request_body_bytes_p95
model_request_efficiency.request_body_bytes_max
model_request_efficiency.context_tax_bytes_p50
model_request_efficiency.context_tax_bytes_p95
model_request_efficiency.context_tax_bytes_max
model_request_efficiency.context_tax_slope_bytes_per_turn

# 44
process_hygiene.observed_processes_spawned_per_turn_p50
process_hygiene.observed_processes_spawned_per_turn_max
process_hygiene.observed_threads_created_per_turn_p50
process_hygiene.observed_threads_created_per_turn_max
process_hygiene.observed_fds_opened_per_turn_p50
process_hygiene.observed_fds_opened_per_turn_max
process_hygiene.peak_live_processes
process_hygiene.peak_threads
process_hygiene.peak_fds
process_hygiene.residue_processes
process_hygiene.residue_threads_delta
process_hygiene.residue_fds_delta
process_hygiene.unique_process_identities

# 48
model_wait_cpu.bytes_yielded
model_wait_cpu.max_inter_frame_ms

# 49
latency_vs_turn_index.first_decile_p50_ms
latency_vs_turn_index.last_decile_p50_ms
latency_vs_turn_index.theil_sen_ms_per_turn

# 50
session_residue_sweep.fd_slope_per_session
session_residue_sweep.thread_slope_per_session
session_residue_sweep.process_slope_per_session
session_residue_sweep.unretired_sessions
session_residue_sweep.closed_sessions
session_residue_sweep.created_sessions

# 51
context_limit_recovery.context_errors
context_limit_recovery.extra_requests
context_limit_recovery.recovery_ms
context_limit_recovery.terminal_success
context_limit_recovery.tool_pairs_before
context_limit_recovery.tool_pairs_after
context_limit_recovery.orphan_tool_calls
context_limit_recovery.orphan_tool_results
context_limit_recovery.duplicate_effects

# 52
resume_latency_vs_length.short_p50_ms
resume_latency_vs_length.mid_p50_ms
resume_latency_vs_length.long_p50_ms
resume_latency_vs_length.long_short_ratio

# 53
journal_torn_tail_sweep.trials
journal_torn_tail_sweep.kill_after_growth_observed_trials
journal_torn_tail_sweep.clean_recoveries
journal_torn_tail_sweep.corrupt_recoveries
journal_torn_tail_sweep.lost_committed_events
journal_torn_tail_sweep.duplicate_events
journal_torn_tail_sweep.duplicate_effects
journal_torn_tail_sweep.recovery_p95_ms

# 56
child_failure_propagation.crash_parent_terminal_ms
child_failure_propagation.hang_parent_terminal_ms
child_failure_propagation.crash_parent_failure_terminals
child_failure_propagation.hang_parent_failure_terminals
child_failure_propagation.hang_deadline_fired
child_failure_propagation.child_terminal_count
child_failure_propagation.child_residue_count
child_failure_propagation.outer_kill_used

# 57
signal_matrix.sigterm_terminal_ms
signal_matrix.sigint2_terminal_ms
signal_matrix.sighup_terminal_ms
signal_matrix.stdin_eof_terminal_ms
signal_matrix.sigterm_residue_processes
signal_matrix.sigint2_residue_processes
signal_matrix.sighup_residue_processes
signal_matrix.stdin_eof_residue_processes
signal_matrix.applicable_cases
signal_matrix.passed_cases

# 58
retry_budget.requests_total
retry_budget.declared_max_requests
retry_budget.declared_worst_case_ms
retry_budget.elapsed_ms
retry_budget.backoff_jittered
retry_budget.failure_terminals
retry_budget.committed_effects

# 59
slow_stream_vs_stall.slow_bytes_yielded
slow_stream_vs_stall.slow_max_inter_frame_ms
slow_stream_vs_stall.slow_terminal_success
slow_stream_vs_stall.slow_idle_timeout_fired
slow_stream_vs_stall.stall_bytes_yielded
slow_stream_vs_stall.stall_own_timeout_ms
slow_stream_vs_stall.stall_structured_failure
slow_stream_vs_stall.stall_outer_kill_used

# 60
large_tool_output.produced_bytes
large_tool_output.model_visible_bytes
large_tool_output.model_visible_encoded_bytes
large_tool_output.harness_output_limit_bytes
large_tool_output.evidence_captured_bytes
large_tool_output.evidence_capture_limit_bytes
large_tool_output.truncated
large_tool_output.terminal_success
large_tool_output.tool_result_correlated

# 61
workspace_fault.structured_failure
workspace_fault.terminal_count
workspace_fault.terminal_ms
workspace_fault.outside_writes
workspace_fault.residue_processes

# 62
offline_mode.provider_requests
offline_mode.blocked_egress_attempts
offline_mode.successful_non_provider_connections
offline_mode.offline_run_success
offline_mode.control_probe_blocked

# 63
nondeterministic_field_report.score
nondeterministic_field_report.comparable_leaf_occurrences
nondeterministic_field_report.varying_leaf_occurrences
nondeterministic_field_report.varying_pointer_count
nondeterministic_field_report.varying_critical_field_count

# 64
cross_run_reproducibility.identical
cross_run_reproducibility.request_stream_count
cross_run_reproducibility.attempt_count

# 65
injection_surface.provider_score
injection_surface.base_url_score
injection_surface.credential_score
injection_surface.score
injection_surface.verified_components

# 66
budget_enforcement.token_limit
budget_enforcement.token_observed
budget_enforcement.cost_limit_microusd
budget_enforcement.cost_observed_microusd
budget_enforcement.time_limit_ms
budget_enforcement.time_observed_ms
budget_enforcement.overrun_count
budget_enforcement.structured_failures
budget_enforcement.score

# 67
usage_reporting.input_tokens
usage_reporting.output_tokens
usage_reporting.total_tokens
usage_reporting.cost_microusd
usage_reporting.turns
usage_reporting.crosscheck_errors
usage_reporting.score

# 68
session_ops_cli.create_ok
session_ops_cli.list_ok
session_ops_cli.resume_ok
session_ops_cli.fork_ok
session_ops_cli.delete_ok
session_ops_cli.score

# 69
event_stream_completeness.tool_call_id
event_stream_completeness.correlated_result
event_stream_completeness.timestamps
event_stream_completeness.usage
event_stream_completeness.terminal_typing
event_stream_completeness.schema_version
event_stream_completeness.narrative_reconstructability
event_stream_completeness.score

# 70
headless_permission_model.score
headless_permission_model.tty_prompts
headless_permission_model.allowed_effects
headless_permission_model.denied_filesystem_effects
headless_permission_model.denied_network_effects
headless_permission_model.scope_violations

# 71
secrets_hygiene_on_disk.files_scanned
secrets_hygiene_on_disk.bytes_scanned
secrets_hygiene_on_disk.stdout_matches
secrets_hygiene_on_disk.stderr_matches
secrets_hygiene_on_disk.journal_matches
secrets_hygiene_on_disk.session_matches
secrets_hygiene_on_disk.log_matches
secrets_hygiene_on_disk.declared_carrier_files

# 72
tool_result_role_fidelity.checks
tool_result_role_fidelity.violations
tool_result_role_fidelity.plain_user_text_violations
tool_result_role_fidelity.missing_results
tool_result_role_fidelity.duplicate_results

# 73
compaction_transparency.compactions_observed
compaction_transparency.announcements
compaction_transparency.scoped_announcements
compaction_transparency.correlated_announcements
compaction_transparency.score
```

Rows 43, 45–50, 52, and 54–55 put their headline values directly in
`resource_summary`; the eligible non-null f64 subset is mirrored in `resource_metrics`.
They do not need redundant flat `metrics` aliases.
All row-specific non-numeric fields named in the matrix tables live under
`details.<stable-row-id>` exactly.

## 7. Manifest schema v2

Manifest `identity.schema = 2`. Every v1 field retains its meaning. Commands remain
direct argv arrays; credentials remain forbidden in argv; generated secret/config files
remain mode 0600 and profile-contained. A v1 manifest is readable for v1 rows but is
`ABSENT` for v2 typed declarations and cannot earn a v2 badge.

Schema-2 parsing is a Wave-1 foundation, not a Wave-4 task. The base parser plus
`request_role_rules` ship before row 42. Typed blocks ship no later than their first
consumer: Wave 2 adds log/journal paths, retry, prompt-stdin, output/truncation, and
large-output declarations; Wave 3 adds context-window and `sessions.close_delete`;
Wave 4 adds injection, budget/tariff, event metadata, permissions, fork/delete, and
credential carriers. The six public adapters may complete their empirical declarations
in Wave 4, but no earlier wave may use untyped TOML/JSON lookups or defer validation.

### Exact additions

```toml
[[request_role_rules]]
kind = "title"                 # title | summary | compaction | reviewer | child
priority = 10                  # unique; ascending order wins
model_ids = []                 # optional exact provider model IDs
json_pointer = "/messages/0/content"
regex = "(?i)title"            # Rust regex over scalar/canonical pointed JSON

[capabilities.injection_surface.provider]
method = "environment"         # environment | cli | generated-config | impossible
environment = "HARNESS_MODEL"
argv = []                       # argv fragment containing {{provider}} when method=cli
argv_position = "suffix"       # prefix (after executable) | suffix
generated_path = ""
json_pointer = ""               # inside generated_path

[capabilities.injection_surface.base_url]
method = "environment"
environment = "HARNESS_BASE_URL"
argv = []                       # when cli, contains {{base_url}}
argv_position = "suffix"
generated_path = ""
json_pointer = ""

[capabilities.injection_surface.credential]
method = "environment"
environment = "HARNESS_API_KEY"
argv = []                       # credential=cli is invalid; retained for typed shape
argv_position = "suffix"
generated_path = ""
json_pointer = ""

[resources]
retry_max_attempts = 6
retry_base_delay_ms = 100
retry_max_delay_ms = 2000
log_paths = ["{{profile}}/state/harness.log"]
journal_paths = []              # only additional paths; events.path is implicit

[input]
prompt_uses_stdin = false       # typed fact independent of transport

[resources.context_window]
surface = "provider-metadata"   # provider-metadata | environment | cli | generated-config
tokens = 4096                   # v2 runner replaces with profile value
environment = ""               # env name only for environment surface
argv = []                       # direct argv containing {{context_window_tokens}} for cli
generated_path = ""             # exact profile-contained GeneratedFile path when used
json_pointer = ""               # destination inside generated JSON/TOML object

[resources.budget_controls]
max_tokens = ["harness", "--max-tokens", "{{budget_tokens}}"]
max_cost = ["harness", "--max-cost", "{{budget_cost_usd}}"]
max_time = ["harness", "--max-time", "{{budget_time_ms}}"]

[resources.budget_controls.tariff]
surface = "environment"        # environment | cli | generated-config | unsupported
input_microusd_per_token = 2   # exact row-66/67 fixture tariff
output_microusd_per_token = 3
input_environment = "HARNESS_INPUT_PRICE_MICROUSD_PER_TOKEN"
output_environment = "HARNESS_OUTPUT_PRICE_MICROUSD_PER_TOKEN"
argv = []                       # cli: contains both tariff placeholders exactly once
generated_path = ""            # generated-config: profile-contained file
input_json_pointer = ""
output_json_pointer = ""

[events.metadata]
timestamp_pointer = "/timestamp"
timestamp_format = "rfc3339"   # rfc3339 | unix-ms | unix-ns | monotonic-ns
schema_version_pointer = "/schema_version"
schema_version_value = "1"
usage_event = "terminal-success" # normalized event vocabulary carrier
usage_scope = "cumulative-run"   # turn | cumulative-run
input_tokens_pointer = "/usage/input_tokens"
output_tokens_pointer = "/usage/output_tokens"
total_tokens_pointer = "/usage/total_tokens"
cost_microusd_pointer = "/usage/cost_microusd"
turns_pointer = "/usage/turns"

[events.narrative]
assistant_text_event = "model-response"
assistant_text_pointer = "/payload/assistant_text"
# assistant_text_match_fields = { "/payload/type" = "text" } # optional predicates
assistant_text_aggregation = "complete-event" # complete-event | item-deltas
# assistant_text_item_pointer = "/payload/item_id" # required for item-deltas
# The reasoning pair may be omitted when the journal does not expose it. Such a
# text-only declaration is measurable but cannot pass a fixture that emits reasoning.
reasoning_event = "model-response"
reasoning_pointer = "/payload/reasoning"
# reasoning_match_fields = { "/payload/type" = "thinking" } # optional predicates
reasoning_aggregation = "complete-event"      # complete-event | item-deltas
# reasoning_item_pointer = "/payload/item_id" # required for item-deltas
turn_pointer = "/actor"

[events.compaction]
event = "context-compacted"
turn_pointer = "/payload/turn_key"
dropped_count_pointer = "/payload/dropped_count"
dropped_span_start_pointer = "/payload/dropped_span/first"
dropped_span_end_pointer = "/payload/dropped_span/last"

[permissions]
mode = "allow-list-and-sandbox" # allow-list-and-sandbox | allow-list | sandbox | workspace-yolo | none
allow = ["harness", "--allow", "{{workspace}}"]                  # complete replacement argv
deny_filesystem = ["harness", "--deny", "{{outside_path}}"]     # complete replacement argv
deny_network = ["harness", "--deny-net", "{{blocked_host}}:{{blocked_port}}"]
yolo = []                       # complete argv; if nonempty contains {{workspace}} or {{profile}}

[sessions]
# Existing fields remain.
close_delete = ["harness", "session", "delete", "{{session_id}}"]
store_paths = ["{{profile}}/state/sessions"]
fork = ["harness", "session", "fork", "{{session_id}}"]
delete = ["harness", "session", "delete", "{{session_id}}"]
fork_id_pointer = "/session_id"
list_array_pointer = "/sessions"
list_item_id_pointer = "/id"   # relative to each array item
delete_missing_semantics = "typed-not-found" # typed-not-found | idempotent-success
not_found_pointer = "/error/type"
not_found_value = "not-found"

[capture]
# Existing fields remain.
credential_carrier_paths = ["{{profile}}/config/provider-auth.json"]

[capture.truncation_marker]
# Applied to normalized model-visible tool-result text; all named captures required.
# Encoded length is measured externally and is intentionally not self-rendered.
regex = 'TRUNCATED truncated=(?P<truncated>true) original=(?P<original_bytes>[0-9]+) payload=(?P<payload_bytes>[0-9]+) sha256=(?P<sha256>[0-9a-f]{64})'
```

`[tools.fixtures]` adds the semantic key `large_output`, whose argv must contain
`{{bytes}}`. The built-in fixture invocation is equivalent to
`ahrb-fixture emit --bytes {{bytes}}` and must stream rather than allocate all output.

The capability rationale maps gain these exact keys:

| Map | Key | Rows |
|---|---|---|
| `capabilities.required` | `context_limit_recovery` | 51 |
| `capabilities.required` | `secrets_hygiene_on_disk` | 71 |
| `capabilities.required` | `tool_result_role_fidelity` | 72 |
| `capabilities.optional` | `native_delegation` | 18, 56 |
| `capabilities.optional` | `budget_enforcement` | 66 |
| `capabilities.optional` | `usage_reporting` | 67 |
| `capabilities.optional` | `session_ops_cli` | 68 |
| `capabilities.optional` | `headless_permission_model` | 70 |

Rows 65, 69, and 73 use typed structural declarations rather than capability-rationale
map entries. Row 69 is CORE: omitted narrative capture points are badge-blocking
`UNSUPPORTED`. Row 73 still runs when `[events.compaction]` is omitted so silent
compaction is measured. Row 52 uses existing required `resume`; row 53 uses existing
required `durable_journal`. A missing required declaration is `ABSENT`; an explicit
architectural unavailability is badge-blocking `UNSUPPORTED`; neither is `FAIL`.

Validation rules:

1. A schema-2 parser accepts omitted later-wave blocks as typed absence; selecting their
   consuming row then yields `ABSENT`/`UNSUPPORTED` by the authoritative table, and a
   complete v2 certification manifest must include all required blocks. Row 65 requires
   all three injection blocks. `environment` requires a
   nonempty env name; `cli` requires the matching placeholder exactly once in an argv
   fragment plus `argv_position` (`prefix` inserts after executable, `suffix` appends);
   `generated-config` requires an exact profile-contained path and JSON Pointer;
   `impossible` requires all carrier fields empty. `credential.method="cli"` is invalid.
   Each request-role rule must have a unique priority, an allowed side-channel kind, and
   at least one predicate; every specified predicate is ANDed.
2. Declaring `budget_enforcement` requires all three nonempty templates and each required
   placeholder exactly once. `{{budget_cost_usd}}` renders integer micro-USD as an ASCII
   fixed-point USD decimal with exactly six fractional digits and no exponent (for
   example 1,000 micro-USD renders `0.001000`). Tariff prices must be exactly 2 input and
   3 output micro-USD/token for certification. `environment` requires two distinct env
   names and all other carrier fields empty; `cli` requires one argv containing
   `{{input_price_microusd_per_token}}` and
   `{{output_price_microusd_per_token}}` exactly once; `generated-config` requires a
   profile-contained generated path plus both JSON pointers; `unsupported` requires all
   carrier fields empty and makes the cost subcase/row `UNSUPPORTED` as row 66 states.
3. Declaring `usage_reporting` requires `usage_event`, `usage_scope`, the five usage
   pointers (input/output/total/cost/turns), and a non-unsupported harness-side tariff.
   `usage_event` must be a normalized event vocabulary value and `usage_scope` must be
   `turn` or `cumulative-run`. Timestamp and schema metadata remain optional row-69
   components. Independent extraction from the unmodified raw carrier permits usage to
   coexist with terminal normalization.
4. Declaring `session_ops_cli` requires create, list, resume, fork, delete, base ID,
   fork ID, list-array/item-ID locators, and delete-missing semantics. Typed not-found
   also requires its pointer/value.
5. Declaring `headless_permission_model` requires a non-`none` permissions mode and
   complete replacement argv for allow, deny-filesystem, and deny-network. A yolo argv
   must be workspace/profile scoped.
6. Every session-store/log/journal/carrier/generated path must be lexically under
   `{{profile}}`. `sessions.store_paths` contains no duplicate rendered root and is
   traversed with the row-50 no-follow/device/identity rules. Declaring the required
   `secrets_hygiene_on_disk` capability requires a nonempty
   `capture.credential_carrier_paths`; every entry names an exact mode-0600 generated
   file whose template contains `{{credential}}`.
7. Retry maximum is 2..=6; base delay is at least 50 ms, max delay is positive, and base<=max. The row-58
   declared worst case formula must be <= both 10,000 ms and `turn_timeout_ms`.
8. `events.path` is automatically scanned and measured; manifests cannot exclude it.
9. `resources.max_output_bytes` and `capture.max_bytes` are each in 1..=1,048,576 for a
   v2 certification manifest; the truncation-marker regex must compile and expose
   `truncated`, `original_bytes`, `payload_bytes`, and `sha256` named captures. It must
   not expose or render `encoded_bytes`; AHRB measures the complete encoded content
   externally after marker rendering.
10. A declared timestamp component requires pointer+format; a declared schema component
    requires pointer+expected value. Missing pairs are legal but score zero in row 69.
    A present `[events.narrative]` requires at least one complete event/value pair for
    assistant text or reasoning plus a non-root turn pointer. Each declared event must
    be normalized, each value pointer must be non-root, and each `item-deltas`
    aggregation requires its matching non-root item pointer. Each optional match-field
    key must also be a non-root pointer and is legal only for a declared side. One
    declared side is valid partial evidence and makes row 69 measurable, but a
    fixture-emitted missing side keeps narrative reconstructability zero. A block with
    neither side is invalid; an absent block makes CORE row 69 `UNSUPPORTED`. A present
    `[events.compaction]` requires `event="context-compacted"`, a non-root turn pointer,
    optional non-root count pointer, and either both or neither non-root span endpoint
    pointers. Declaring no scope pointers is valid announced-only data and can score only
    0.5; omitting the block does not skip row 73 or convert observed silent compaction
    into `UNSUPPORTED`.
11. `resources.context_window.tokens` is positive. For `provider-metadata`, environment,
    argv, generated path, and JSON Pointer are empty. For `environment`, only a nonempty
    environment name is allowed and AHRB sets it to the profile token count in unsigned
    decimal. For `cli`, only argv is nonempty and contains
    `{{context_window_tokens}}` exactly once. For `generated-config`, path and JSON
    Pointer are nonempty, argv/environment are empty, the path names an existing
    profile-contained `GeneratedFile`, and its template contains
    `{{context_window_tokens}}` exactly once at that pointer. AHRB substitutes the quick
    or cert row-51 token count before launch.
12. Row 50 requires nonempty `sessions.close_delete` with `{{session_id}}` exactly once
    and nonempty profile-contained `sessions.store_paths`; close-delete is a public
    harness command/transport operation and may not alias AHRB's metadata close. Its
    successful result is validated before every residue measurement. Row 68's `delete`
    may use the same argv only when both typed fields are explicitly populated and
    independently validated.
13. `[input].prompt_uses_stdin` is required when row 57 is selected. If true, launch must
    retain an AHRB-owned stdin writer; if false, stdin EOF is N/A only when transport
    also proves stdin is not a control channel.

### Six bundled real adapters

Wave 4 updates exactly the six public `hbench` adapters:

```text
adapters/claude-code/manifest.toml
adapters/codex/manifest.toml
adapters/haider-agent/manifest.toml
adapters/opencode/manifest.toml
adapters/pi/manifest.toml
adapters/rick/manifest.toml
```

The remaining legacy/placeholder adapter directories are not part of this v2 bundled
declaration lane. Each of the six moves to manifest schema 2 and explicitly declares all
typed blocks. The implementing lane MUST verify actual command/config behavior and use
`impossible` or omit an OPTIONAL capability when it cannot be proven; it MUST NOT infer
support from product name or an environment variable that is merely an AHRB rendering
input. Current code establishes that Claude, Codex, OpenCode are per-invocation with
resume surfaces; Pi and Rick currently lack generic session operations; Haider is the
shared-daemon adapter with max 32 and declared list/resume surfaces whose behavior has
not been verified. Those are feasibility inputs, not permission to auto-PASS any v2 row.

## 8. Four implementation waves

Each wave is one independent **implement -> verify** lane. A wave may start only after
the prior wave's report schema and public collector interfaces are verified. Verification
must run both reference mocks, targeted unit tests for every oracle boundary, and at
least one intentional failing fixture per new evaluator.

### Wave 1 — cheap evidence, comparison, and badge plumbing

Rows/features: **42, 43, 44, 45, 46, 63, 64, G1, G2**.

Dependencies and owned deliverables:

1. Report schema 3, `details`, `turns`, per-attempt model-request evidence, new
   ResourceSummary fields, f64-only profile/topology-labelled resource mirrors,
   TestResult requirement/score metadata, and badge v2 fields.
2. The base manifest-schema-2 parser/validator and typed `request_role_rules`; schema-2
   parsing is not deferred to Wave 4.
3. One shared monotonic origin connecting driver launch/submit, fake request receipt,
   terminal, and exit boundaries.
4. Deterministic request leaf comparator and stream hasher with the exact pointer/type
   normalization table.
5. `results/index.jsonl`, legacy v1 normalization, selector resolution, `hbench diff`, and compatible-schema
   diagnostics.
6. L/C/A class evaluation and the informational-row exit/badge policy.

Reference mock additions:

- **Daemon mock (`ahrb-mock`)**: deterministic direct-terminal turns; external
  launch/submit/terminal boundaries; no auxiliary request; post-close process/FD/thread
  state equal to baseline; canonically identical fresh runs.
- **Per-invocation mock (`ahrb-mock-exec`)**: one request per direct-terminal turn;
  zero process residue after every exit; environment-based credential injection rather
  than an argv secret; identical normalized streams across fresh profiles.
- Both must PASS every Wave 1 row. Neither has a legitimate Wave 1 `UNSUPPORTED`.

Verification exit: golden schema/JSONL round trips; percentile/MAD/integral boundary
tests; a deliberately varying request field fails rows 63/64 as specified; a fake
residual child fails 44; diff old schema renders `unavailable`, and cross-topology
resource deltas render `not-comparable`.

### Wave 2 — new fault fixtures and failure semantics

Rows: **47, 48, 57, 58, 59, 60, 61, 62, 56** (implemented in the stated order except
that 48 and 59 share the trickle fixture).

Dependencies and owned deliverables:

1. Wave 1 timestamps, per-attempt logs, process identities, report details, and
   topology wrappers.
2. Disk counters with retired-process accounting; fake `Trickle` and per-attempt status
   schedules; reset-on-byte idle client; bounded streaming tool capture; read-only
   workspace setup; signal/EOF supervisor operations.
   Their Wave-2 typed manifest blocks are parsed and validated before collectors run.
3. Real parent/child wait/failure propagation through generic agent operations.
4. Reviewed platform egress guard plus independent control probe, or the restricted
   reference-mock owned connector plus an AHRB-challenged in-tree control connect and
   executable/PID-bound decision ledger. If neither can be delivered, row 62 remains
   an infrastructure ERROR; proxy-only or declaration-only PASS is forbidden.

Reference mock additions:

- **Both mocks**: observable bounded journal/log writes; `idle_timeout_ms=2500` and an
  idle-resetting trickle client; bounded jittered 429/500 retry satisfying the declared
  worst case; structured signal/EOF terminalization; 10 MiB streaming fixture with
  1 MiB model-visible truncation and declared marker; typed read-only failure; clean offline run.
- **Daemon mock** additionally propagates child crash and deadline cancellation and
  passes row 56.
- **Per-invocation mock** has no native delegation and legitimately marks **row 56
  `UNSUPPORTED`**. It must PASS every other Wave 2 row.

Verification exit: trickle succeeds while stall self-aborts; sustained faults never
exceed six attempts; large output stays inside capture/memory envelopes; signals leave
zero residue; a disk control fixture produces a known nonzero delta; the offline control
probe cannot connect.

### Wave 3 — long horizon and concurrency

Rows: **49, 50, 51, 52, 53, 54, 55**.

Dependencies and owned deliverables:

1. Wave 1 per-turn evidence and Wave 2 deterministic faults/process accounting.
2. Indexed long-session clocks, sequential create/close sweep, context-error-once
   workflow, length-parameterized resume, repeated torn-tail harness, integer fanout
   sweep, and per-actor release-to-terminal clocks.
   Wave-3 parsing includes typed context-window and real `sessions.close_delete`.
3. Deadline budgeting for the cert 32x7 fanout matrix and deterministic seeded width
   rotation.

Reference mock additions:

- **Daemon mock**: retire closed supervisors; compact after the one-shot context error
  without breaking tool pairs; bounded resume lookup; resettable torn-tail trials;
  stable N1..N32 barriers and actor timestamps.
- **Per-invocation mock**: the same context/recovery/torn-tail semantics; official resume
  at all lengths; add and invoke a real public `sessions.close_delete` command (never an
  AHRB metadata-only close), prove zero process and session-store residue per delete;
  raise `concurrency.max_agents` to 32 and pass the integer cert sweep.
- Both mocks must PASS rows 49–55. Neither has a legitimate Wave 3 OPTIONAL
  `UNSUPPORTED`; missing core resume/journal evidence is badge-blocking.

Verification exit: inject O(n) delay and prove 49/52 fail; inject a >32 KiB/session leak
and prove 50 fails; orphan one result and prove 51 fails; corrupt one of 25 tails and
prove 53 fails; inject a known cliff/starvation and prove 54/55 identify it.

### Wave 4 — ergonomics and all six bundled declarations

Rows: **65–73** plus schema-2 declarations for the six adapters named above.

Dependencies and owned deliverables:

1. Wave 1 score/badge/report plumbing; Wave 2 time/usage fault fixtures; Wave 3 session
   lifecycle and forkable transcript setup.
2. Typed manifest validation for injection, budgets/tariff, event metadata, narrative
   and compaction capture points, permission modes, session fork/delete, and credential
   carriers. Large-output validation already shipped in Wave 2 and is only consumed,
   never re-owned, here.
3. Generic budget, usage, CLI session-op, permission, secret-scan, and dialect-aware
   tool-result evaluators.
4. Empirical declaration review for Claude Code, Codex, Haider, OpenCode, Pi, and Rick.

Reference mock additions:

- **Both mocks**: explicit injection declarations; token/cost/time budgets; exact usage;
  timestamp/schema event metadata; durable assistant text and provider-emitted reasoning;
  scoped compaction announcements; scoped permissions; clean secret scan; protocol-native
  tool results.
- **Per-invocation mock** adds CLI create/list/resume/fork/delete and passes row 68.
- **Daemon mock** may legitimately mark **row 68 `UNSUPPORTED`** because its public
  automation contract is stdin-RPC rather than CLI. This tests honest optional gating.
- Across the complete v2 matrix, the only intended mock `UNSUPPORTED` results are
  **row 56 for mock-exec** and **row 68 for the daemon mock**. All other new rows pass on
  a certification-capable host; informational rows also record their classes.

Verification exit: v1->v2 manifest parse/validation tests; spoofed declarations fail
behavioral trials; a secret in every artifact category fails 71 before redaction; plain
user-text tool result fails 72; metadata-only narrative fails CORE row 69; silent
compaction fails CORE row 73; badge A/L/C/R and facet omission match goldens.

## 9. Implementation feasibility summary by current code path

This section describes the repository state used to author the specification; the row
tables remain normative.

| Area | Existing usable path | Required v2 extension / uncertainty |
|---|---|---|
| Fake requests | `fake_model::ModelRequest`, `ModelRequestRecord`, `FakeModelEngine::handle`, canonical JSON/hash | Physical attempts, body size/timing/roles; context-once and paced body. |
| Turn clocks | `Driver::completed_turn_wall_ns`, `ResourceEvidence.turn_wall_ns`, `runner::run_long_horizon` | Preserve repetition/turn/actor and all boundaries instead of only mean. |
| Process/CPU/memory | `process::Sample`, `ProcessSample`, `TreeCpuTracker`, platform samplers | Disk counters/retired disk tracker; per-actor interval association. |
| Resource analysis | `ResourceTimingPlan`, `SweepObservation`, row-29 checkpoints, distribution helpers | Integrals, slopes, cliff/fairness arrays, session sweep. |
| Sessions | Generic create/submit/attach/replay/resume/close and per-session PIDs | Generic list/fork/delete CLI and length sweep. |
| Durable journal | Existing kill/torn-tail injection and exact recovered-suffix validation | Repeated live growth kills plus deterministic post-kill tail cuts; no claim of syscall/fsync interception. |
| Output capture | Per-invocation file-backed bounded reads; daemon mock native `.output()` | Common streaming cap, complete-record truncation, structured marker. |
| Capability/badge | `scenarios::RequirementKind`, `matrix_evidence::capability_for_row`, `evaluate::certify` | Informational role, score, shared row-18/56 facet, v2 classes. |
| Reporting | Numeric `metrics`, topology-wrapped `resource_metrics`, raw bundle | Schema 3 structured `details` and four raw vectors. |
| Egress | v1 intent only; current isolated environment has no direct-socket guard | Reviewed OS guard, or executable/PID-bound owned connector for the compiled-in mocks; otherwise ERROR. Proxy-only evidence is invalid. |

## 10. Completion criteria for AHRB v2

V2 is complete only when:

1. `list-tests` returns exactly 73 rows with the stable IDs in v1 plus this document;
2. every new row emits its required metrics/details/raw evidence and has unit-tested
   boundary oracles for PASS, FAIL, ERROR, ABSENT, and applicable UNSUPPORTED behavior;
3. both reference mocks satisfy the Wave 4 matrix with only the two intentional optional
   UNSUPPORTED results stated above;
4. all six bundled real manifests parse as schema 2 and make explicit, behaviorally
   reviewable declarations;
5. `hbench diff` is deterministic, accepts mixed legacy-v1/current index lines, and
   never compares resources across topology, OS, or profile;
6. the v2 badge carries R, L, C, and A under the exact gating rules above;
7. sampler overhead and confidence remain visible, no harness turn-path instrumentation
   was introduced, and row 62 never claims proxy-only confinement as proof.
