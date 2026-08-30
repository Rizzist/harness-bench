# AHRB — Agent Harness Readiness Benchmark (build spec v1)

A standalone, open-source **Rust** benchmark that certifies a coding-agent HARNESS is
correct, safe, controllable, deterministic, recoverable, and resource-predictable when
operated by AUTOMATION. It drives harnesses through deterministic SIMULATED workflows
against a FAKE model (no real inference) so it measures the harness's own behavior and
overhead — not model intelligence, coding quality, tokens, or provider latency.

Crate/binary: `ahrb`. License: dual MIT OR Apache-2.0. Rust stable. No DB, container,
Python, or external service required. Deps limited to async HTTP/runtime (tokio +
hyper/axum-lite or hyper directly), serde/toml/json, a hash (sha2/blake3), and thin
platform FFI (libc / mach for macOS, procfs reads for Linux). Cross-platform: macOS
(arm64/x86-64) and Linux (x86-64/aarch64). This machine is macOS arm64 — the macOS
sampler MUST work; gate Linux-only code behind `#[cfg(target_os="linux")]`.

## Four pillars (all badge-gating; single combined test suite drives all four)
1. **tool-call correctness** — exact model/provider/credential routing; tool-call
   execution/args/results; streamed-arg reassembly; error normalization; retry safety;
   structured terminal success AND failure; filesystem/patch effects; sandbox isolation;
   and the **client-side idle-deadline self-abort** (harness must terminalize a stalled
   upstream on its own before an outer supervisor kill — the exact Haider 963→966 regress).
2. **functionality** — deterministic end-to-end orchestration, concurrent-actor
   isolation, native delegation, deterministic exit codes.
3. **simulated-workflow resource** — idle footprint (RSS/CPU/drift/return-to-baseline/
   cold-start), single-agent footprint, **parallel-agent memory delta + total peak +
   scaling curve + reclaim**, long-horizon stability.
4. **automation-readiness** — unattended lifecycle, session persist + resume/replay,
   steer/pre-tool/queue next-prompt, cancellation+cleanup, crash-recovery, resume
   idempotency, resource-bound honoring, hooks, deny-egress isolation, **durable journal**.

AHRB is a SUPERSET of the existing Python conformance gate (it ABSORBS correctness);
correctness is first-class and cannot be compensated for by good resource numbers.

## Architecture (single crate, modules)
- `main` + `cli`: subcommands `doctor`, `run`, `report`, `list-tests`.
- `manifest`: TOML schema + validation, template rendering, capabilities, adapter hashing.
- `driver`: generic daemon/client/session/prompt/attach/cancel/cleanup operations over a
  transport (exec | stdin-RPC | socket JSON-RPC | HTTP) selected by the manifest.
- `workflow`: deterministic scenario state machine + assertions.
- `fake_model`: local HTTP server + shared scripted-response engine; built-in protocol
  frontends: OpenAI Chat Completions (v1 minimum), OpenAI Responses, Anthropic Messages.
- `events`: table-driven normalization into AHRB's event vocabulary.
- `process` (`process/linux`, `process/macos`): whole-tree ownership discovery + memory/
  CPU sampling + terminal rusage.
- `sampler`: phase-aware time series + plateau detection.
- `evaluate`: four-pillar PASS/FAIL/UNSUPPORTED/ERROR/ABSENT classification + cert rules.
- `report`: Markdown + JSON + raw evidence (samples.jsonl, processes.jsonl, events.jsonl,
  model-requests.jsonl), fingerprints, optional JUnit.
- `scenarios`: versioned declarative workflow definitions.
- `adapters`: reference manifests for the 9 harnesses (haider-agent, pi, rick, cline-cli,
  opencode, goose, oh-my-pi, deepseek-harness, aider) — as data.
- **`mock_harness`** (REQUIRED, see below): a built-in reference harness so AHRB tests
  itself end-to-end with zero external harness.

### Deterministic fake-model engine
Local HTTP server returning precomputed responses (no inference). A workflow defines
logical actors (parent/root/child), initial prompts + stable actor IDs, expected
model-request checkpoints, deterministic text/tool-calls/faults/terminal responses, and
NAMED BARRIERS where the server registers an actor reaching a state and waits for the
runner to release. Every prompt carries an opaque scenario+actor marker; child-spawn tool
args carry the child marker. The server ROUTES BY MARKER + canonicalized conversation
state, NEVER by arrival order (concurrent requests arrive in any order). Per request:
extract scenario/actor/checkpoint → hash canonical request → validate legal transition →
return exact scripted response → identical retry returns byte-equivalent output WITHOUT
advancing → reject unexpected transitions as an infra diagnostic. IDs/timestamps/usage/
fragmentation are deterministic. The same engine drives all four pillars: one/many/
parallel tool calls, fragmented arg streams, repeated frames/IDs, malformed calls,
429/500, mid-stream disconnect, and indefinite STALL at a checkpoint (accept then emit no
further bytes) so a harness-enforced idle deadline is distinguishable from an outer kill.

### Barriers are STATE-BASED, never sleeps/arrival-order. Measure only after all N named
actors reach the same validated transition.

## Parallel-agent memory methodology (centerpiece)
Sweep N=1,2,4,8 (opt 16/32); cert requires N≥8. Per N/rep on a fresh isolated profile:
start daemon/controller → one unmeasured warm-up + close → measure warm idle baseline B
(3s) → release N starts together → wait until all N did their tool op and reached the same
barrier → hold 3s, use last 2s as steady window (discard first 1s) → release all → measure
post-turn retention → close/delete N sessions via official surface → wait ≤10s reclaim,
measure post-close Rₙ → shutdown + orphan scan. Plateau trustworthy only if all N present
AND final window P95–P5 ≤5% of median (else repeat / classify unstable).

**Whole-tree ownership** includes client(s), persistent daemon/controller, session
workers, agent subprocesses, harness-launched tool/hook procs, reparented children;
excludes the AHRB runner + fake-model + deny-egress servers. Dedup by `(PID, start-time)`,
never PID alone.
- Linux: dedicated cgroup v2 (cgroup.procs durable membership across reparent); sample
  memory.current/peak, cpu.stat; per-PID `/proc/<pid>/smaps_rollup` → Rss/Pss/private;
  cross-check `/proc/<pid>/stat` PPID+start-time. Fallback = declared roots + procfs
  ancestry closure (reduced-confidence). Report BOTH aggregate RSS and PSS (RSS
  double-counts shared pages; PSS is the preferred comparison metric).
- macOS: `proc_listpids`/`proc_pidinfo` (PPID+start-time); `proc_pid_rusage(RUSAGE_INFO_V4)`
  for resident + physical footprint + cumulative CPU; `task_info(TASK_VM_INFO)` cross-check;
  root = launcher PID + verified daemon-PID locator + recursive descendants.
- Terminal rusage: also `wait4` direct children, normalize ru_maxrss (bytes on macOS, KiB×1024
  on Linux) — supplemental cross-check ONLY, never a substitute for the sampled tree metric.
Sampling cadence: membership 10ms; Linux cgroup counters 10ms; smaps_rollup 50ms + at
boundaries; macOS rusage 20ms + at boundaries. If sampler >10% of one core or overruns →
`ERROR: sampler overload` (never a suspiciously-low result).

Reported metrics (RSS and, where available, PSS/footprint): avg added `(Sₙ−B)/N`; adjacent
marginal `(Sₙ−Sₚ)/(N−P)`; headline slope β = Theil–Sen of steady vs N (MiB/agent); scaling
exponent α = slope of log(Sₙ−B) vs log(N); workload peak Pₙ; cold peak Cₙ; peak amp Pₙ/Sₙ;
post-turn retained Iₙ−B; post-close residual max(0,Rₙ−B); residual/agent; reclaim ratio
`(Sₙ−Rₙ)/(Sₙ−B)` clamped 0–1; tree CPU/scripted-turn; barrier idle CPU. Keep raw series +
PID membership.

v1 reference envelope (small-host): N=8 completes; cold whole-tree peak ≤4 GiB; β ≤256
MiB/agent; α ≤1.20; reclaim ≥80%; residual ≤max(64 MiB, 20% of active delta); barrier idle
CPU <5% of one core; CPU ≤250 ms/scripted turn.

## Manifest (adapter = DATA, not harness-specific code)
Groups: schema+identity; availability (exec paths, version probe/pattern); fake-model
(protocol dialect, base-URL + credential binding, model ID, allowed paths, provider config
templates); model roles (primary/planner/title/compaction/reviewer/child); isolation
(HOME/XDG/config/data/state/session/runtime/tmp, generated config files+modes, forbidden
historical state roots); daemon lifecycle (embedded/persistent, start, readiness probe,
PID locator, shutdown, grace); automation transport; session ops (create/submit/attach/
resume/close-delete/list/id-extraction); next-input ops (steer/subturn/queue + capability
flags); agent ops (create/native-spawn/child-id/status/cancel/collect); concurrency
(topology, max N, fanout mode, barrier evidence); tool semantics (aliases, schema bindings
for shell/spawn/input, safe fixture command templates); events (source/framing/predicates/
extractors/stable-ids/cursors/terminal maps); exit contract; process ownership; resource
controls; hooks; cleanup; redaction+capture caps; capabilities (required/optional + reason).
Credentials NEVER in argv; credential files private; commands are argv arrays not shell.

## Exit classes: PASS | FAIL | UNSUPPORTED | ERROR | ABSENT. Unsupported MANDATORY rows in
any pillar block the core badge. Facets: native delegation, parallel tool exec, pre-tool
intervention, hooks.

## Determinism/fairness: identical semantic workflows/fixtures/turns/text; each harness
keeps its own system prompt/architecture (that IS overhead); unique per-run creds; reject
unexpected paths/models/transitions; unique per-agent workspace/marker namespace;
state-based barriers; fake server outside measured domain; never arrival-order state. Run
controls: 1 warm-up; 7 reps cert / 3 quick; median headline + MAD/P95; fresh profile per
rep; seeded N rotation; no real network (proxy env → deny listener, loopback only for AHRB);
before/after outside-state snapshots. Load guard (5s observe: non-bench CPU <10%, 1-min
load/cores <0.25, no swap, ≥2× expected peak free min 4 GiB; wait ≤60s else `ERROR:
environment unstable` — never blame the harness). Wall time is diagnostic only (barrier +
scheduler contaminated). Evidence fingerprints: harness artifact/version, manifest,
workflows, fake-model engine ver, normalizer, AHRB revision, OS/kernel/arch/host-mem,
profile.

## Badge: `Automation Ready v1 · <os> · <topology> · N8 · R<class> · replay+crash+steer+queue`.
Resource classes by marginal effective RSS: R32/R96/R256/R256+. Topologies:
native-sibling-fanout | shared-daemon-sessions | worker-processes | client-process-fanout.
Never put different OS/topology numbers on one unlabeled leaderboard.

## THE TEST MATRIX (implement all; each: method=simulated, metric, pass-criteria)

### tool-call correctness
1. Exact model+endpoint+credential routing — every role uses exactly the configured local tuple, no fallback; every declared role reaches the fake server; deny all other egress.
2. Single tool call in one turn — one deterministic fixture write; exactly-once effect; args byte-match; result correlates to call ID; structural terminal.
3. Multiple SEQUENTIAL tool calls — A then B where B's args include A's output; each once, in order, correct correlation.
4. PARALLEL tool calls in one turn — two independent calls held at a barrier to observe overlap, released in reversed order; both live concurrently, execute once, correct ID/result assoc, assistant-frame order preserved. Honest absence = UNSUPPORTED.
5. FRAGMENTED streamed args — JSON split across boundaries (inside names/escapes/nested); reconstruct exactly, invoke once only after complete; no partial/dup/lost-escape/crash/hang.
6. FAILED tool execution as structured result — fixture tool exits nonzero/typed error; one structured failed result assoc to call ID; failure reaches next model request; no crash/silent-swallow.
7. MALFORMED/unknown tool call — invalid JSON args + unknown tool name; structured tool/protocol failure; no unintended tool runs; no crash/hang/silent-ignore.
8. Tool-call-id dedup + order preservation — stable IDs, force retry/replay of identical frame at a disconnect checkpoint, complete out of emission order; IDs preserved end-to-end; duplicate transport frames ≠ duplicate semantic calls/effects.
9. Structured terminal SUCCESS — exactly one machine-parseable SUCCESS; exit/category per contract; no later contradiction.
10. Structured terminal FAILURE — exactly one FAILURE distinct from success/crash/cancel/timeout; nonzero exit documented+stable; not masqueraded as text completion.
11. Transient upstream retry + normalization — 429/500/mid-stream-disconnect at checkpoints; bounded documented policy; finite terminalization; ≤1 tool effect; exhausted/non-retried → structured failure; no double-committed partial frame.
12. Client-side timeout / idle-deadline self-abort — configure harness idle deadline D_idle < outer D_outer; fake model accepts then emits nothing; measure from last byte to the harness's OWN terminal; PASS iff it self-terminalizes within D_idle and strictly before D_outer with a structured failure; hang / outer-kill / terminate-only-at-D_outer / no-terminal = FAIL.
13. Workspace/patch effects independent of stdout — deterministic file create + patch, exceed stdout cap; effects match by hash despite truncation; pass/fail from actual effects not stdout.
14. Per-run state + network confinement — attempts to touch undeclared state / non-loopback; all undeclared access fails closed; fake server is the only reachable service; no outside write/leak/surviving state.

### functionality
15. Headless tool workflow — one fixture read + one unique marker write + terminal; correct sole effect; exit 0; no human input.
16. Multi-turn transcript determinism — replay a fixed 3-turn workflow 5×; identical semantic hashes+counts.
17. Concurrent actor isolation — N actors, unique tokens, isolated namespaces, interleaved requests; each effect belongs to exactly one actor; no foreign token cross-talk.
18. Native delegation — parent spawns per manifest semantic; child follows its own script; exactly one durable child per spawn; each report once; UNSUPPORTED explicit.
19. Deterministic exit codes — success/denied/invalid/timeout/cancel/provider-failure over 5 reps; success=0; others nonzero+documented+structured+invariant.

### simulated-workflow resource
20. Idle RSS baseline at rest — detect persistent-daemon vs one-shot; sample quiescent owned tree (or zero-process between-turn); stable (P95–P5 ≤5%); classify model correctly.
21. Idle CPU + busy-poll detection — 10s quiet window whole-tree CPU; ≤1% of one core, no periodic polling signature (an ungated ~500ms loop hitting a store/session file = explicit FAIL).
22. Idle memory drift / leak-at-rest — 120s cert / 30s quick untouched; slope ≤1 MiB/min, net ≤8 MiB; no unexplained worker/thread growth.
23. Return-to-idle after workflow — pre B vs post-workload idle (ordinary AND after N=8 fan-out, all sessions closed); stable within 10s; residual ≤max(64 MiB,20% active delta).
24. Cold-start → steady idle — launch→readiness→idle plateau; declared startup bound met; cold peak within profile.
25. Single-agent footprint + CPU/turn — B, S₁, cold peak, deltas, CPU/turn ≤250ms, barrier CPU <5%.
26. Parallel-agent memory delta + total peak — N=1,2,4,8 each tool-call + same barrier; all N present; N=8 completes; cold peak ≤4 GiB; β ≤256 MiB/agent.
27. Parallel scaling curve — fit sweep; α ≤1.20; no adjacent marginal >2× preceding median without instability flag.
28. Post-completion reclaim — close/delete all sessions, sample ≤10s; reclaim ≥80%; residual bound; no owned worker remains.
29. Long-horizon stability — 1000 tiny turns (fixture tool every 10th), sample every 100; drift ≤64 KiB/turn; final residual ≤max(64 MiB,5% of B); no monotonic FD/thread leak.

### automation-readiness
30. Session persist + replay — turn A, detach/stop, restart, attach after cursor K, continue B; ordered suffix (K,head] exactly once; same session; no gap/fabrication.
31. Safe-boundary next prompt (steer) — hold at boundary, inject U, release; U once at next safe boundary, affects active run.
32. Pre-tool next prompt (subturn) — hold before pending tool completes, inject U; U observed before the effect; no effect before intervention.
33. Queued next turn — enqueue B during A, release A, service B; A terminates before B; B runs once as distinct turn.
34. Autonomous no-interactive-prompt — closed stdin, no PTY; allowed effect succeeds; denied fails closed; no prompt/stdin wait.
35. Crash recovery kill/resume — kill daemon/worker at named post-accept/tool checkpoint, restart, attach, continue; readiness ≤10s; finite; committed effect at most once.
36. Cancellation + cleanup — cancel while N held; all terminal ≤5s; no owned PID after 2s grace; no undeclared socket/file/state; reclaim met.
37. Resume idempotency — repeat submit/resume + attach incl. one simulated lost response; one semantic turn + one tool effect; transport dups only if normalizer dedups by stable identity.
38. Resource-bound honoring — request L+1 agents / turn>max / oversized output / held>deadline; limits never exceeded; excess queued/typed-refused; deadline terminates within grace.
39. Hook firing — isolated marker hook for acceptance + completion; each committed event fires once after commit; replay doesn't refire; hook child exits; no secret/env leak.
40. **Durable journal** [MANDATORY ADDITION] — certify an append-only DURABLE event journal is the recoverable source of truth: drive several committed events, hard `kill -9` the harness/daemon at a named post-commit checkpoint, restart, read the journal + attach-after-cursor; assert the committed suffix is present exactly once, ordered, no gap/fabrication, and a partially-written final record is either fully present or cleanly absent (no torn/corrupt tail). Metric: recovered-event completeness/order, torn-record handling, journal-vs-replay agreement, durability across kill -9. Pass: no committed event lost or duplicated; tail integrity preserved. UNSUPPORTED (itself a finding) if the harness has no durable event store. Persistent-daemon journal vs one-shot session-file models both handled.
41. Profile/network isolation — unique HOME/profile + deny egress while all roles execute; no direct external conn / undeclared outside write / credential in argv/logs; all aux calls hit the fake endpoint.

## Hard parts (design for these): portable whole-tree attribution (verified daemon-PID
locators + start-time identity mandatory); RSS not additive (publish RSS but compare on
PSS/footprint); topology ≠ architecture (label + test native delegation separately);
keep the fake-model contract minimal (one engine, few protocol frontends, no per-harness
servers); barriers state-based; cache vs leak (measure post-turn / post-close / repeated-
cycle separately).

## Open questions for the owner (leave configurable; don't hard-block on them):
N8 vs N16 cert; 4 GiB/N8 envelope vs class-only; hooks/pre-tool mandatory vs facet; Linux
cgroup mandatory vs ancestry-diagnostic; ship all 3 fake dialects vs Chat-only v1; whether
client-process-fanout earns a limited "Headless Ready" badge.
