# AHRB — Agent Harness Readiness Benchmark

AHRB is a standalone Rust benchmark for coding-agent harnesses. It replaces real model
inference with deterministic local responses, then checks tool-call correctness,
functionality, whole-process-tree resources, and unattended automation readiness.
The separate harness-economy pillar measures how economically a harness drives one
standardized fake-model task to its scripted terminal. The context-fidelity pillar
measures exact long-horizon context retention at the fake-provider boundary.

The benchmark never needs a model credential, database, container, Python runtime, or
external service. A built-in reference harness exercises the complete pipeline.

## Five-minute quickstart

Requirements: stable Rust on macOS or Linux.

```console
cargo build --locked
cargo run --locked --bin ahrb -- doctor --manifest adapters/mock/manifest.toml
cargo run --locked --bin ahrb -- list-tests
cargo run --locked --bin ahrb -- run --manifest adapters/mock/manifest.toml --profile quick
cargo run --locked --bin ahrb -- run --pillar economy \
  --manifest adapters/mock/manifest.toml --profile quick
cargo run --locked --bin ahrb -- run --pillar fidelity \
  --manifest adapters/mock/manifest.toml --profile quick
```

The run produces `report.md`, `report.json`, `samples.jsonl`, `processes.jsonl`,
`membership.jsonl`, `events.jsonl`, and `model-requests.jsonl`. Add `--junit` for
`junit.xml`.

For a selected quick smoke run:

```console
cargo run --locked --bin ahrb -- run \
  --manifest adapters/mock/manifest.toml \
  --output ./ahrb-smoke \
  --tests 1-3,9,10,12,20,26,30,35,40
```

The `hbench` shorthand accepts the same row selection, for example
`hbench codex --tests 1-19,30-41`. Run the resource pillar (rows 20-29) on a
quiet host: its wall-clock and process measurements are intentionally sensitive
to machine contention.

Run the economy pillar independently with either command form:

```console
hbench economy mock --profile quick
ahrb run --pillar economy --manifest adapters/mock/manifest.toml --profile quick
```

Economy runs use no real inference. They emit a typed `economy_summary` containing
model turns, total reference request tokens, tool calls and batching factor, peak
carried context, scripted-terminal completion, and reference cost. Reference-token
and cost values are deterministic comparison proxies, not provider usage or a bill.
The complete contract is in [`docs/SPEC-v3-economy.md`](docs/SPEC-v3-economy.md).

Run the long-horizon fidelity pillar independently with `hbench fidelity mock` or
`ahrb run --pillar fidelity`. Its 24-request quick and 44-request cert variants emit
a typed `fidelity_summary` with structural-needle survival, byte-identical tool-result
retention, and typed end/workspace evidence. These are serialized-request observations,
not claims about model understanding or task success. The complete contract is in
[`docs/SPEC-v3-fidelity.md`](docs/SPEC-v3-fidelity.md).

## Storage pillar (v4)

The storage pillar measures what a harness writes and keeps on disk while one session
grows turn by turn. It is independent of the matrix, economy and fidelity:

```console
hbench storage mock --profile quick --output "$PWD/results/storage-$(date -u +%Y%m%dT%H%M%SZ)"
ahrb run --pillar storage --manifest adapters/mock/manifest.toml --profile quick \
  --output /absolute/fresh/bundle
```

Both forms accept `--profile quick|cert`, `--deadline SECS`, `--no-save` and `--output`;
`--tests` is rejected because all ten rows always run. Quick is 100 turns x 3 repetitions,
cert 1,000 turns x 7. The default deadline is derived from that serialized workload
(10,509 s + 3 x `sweep_interval_s` quick; 174,979 s + 7 x `sweep_interval_s` cert).
On expiry AHRB still writes the full report, marks unfinished rows `ERROR: deadline`
and exits 2. The rows are:

| Row | Measures |
|---|---|
| S1 `write-volume` | OS-accounted physical bytes written per turn by the whole owned process tree, allocated-footprint growth, write amplification and the D class |
| S2 `durability-cost` | `fsync`/`fdatasync`/`F_FULLFSYNC` calls per turn in an instrumented copy of the task, plus an *estimated* wall cost at a fixed 4 ms/call |
| S3 `footprint-curve` | Allocated bytes at checkpoints 0/1/10/50/100 (cert adds 500/1,000) and the G class `bounded/linear/superlinear`; superlinear growth FAILs |
| S4 `compaction-vs-disk` | Allocated bytes before and after a context-limit compaction |
| S5 `close-retention` | Bytes retained after a declared close-without-delete, against a declared cap and sweep |
| S6 `delete-uninstall-residue` | Files left after a declared `session_delete` or `uninstall_cleanup` verb; residue FAILs |
| S7 `bounded-auxiliaries` | Per-family growth of declared areas (logs, caches, ...) against declared caps |
| S8 `request-body-retention` | Whether captured request bytes are stored verbatim (`none/deduplicated/full`) |
| S9 `crash-residue` | Files left by a SIGKILL during a held turn, and whether resume preserves the committed prefix |
| S10 `resume-read-cost` | Physical bytes read and latency to resume a grown session |

Honesty rails: physical I/O (OS counters) and allocated footprint (`st_blocks x 512`) are
separate measurements and never substitute for each other. The whole disposable profile
is audited without following links; a symlink, a repeated hard-link identity or a file
that keeps changing is an `ERROR`, never a smaller number. A declared `transient` area
only permits a bounded re-inventory; its bytes stay in every total. S2's wall time is an
estimate, not measured latency. S8 classes describe exact byte matches, not privacy
safety. A missing declaration yields `ABSENT` or `UNSUPPORTED`, never zero. The only
behavioural FAILs are S3 superlinear growth and residue after a declared S6 verb; the
other classes are informational.

A completed run with no ERROR, a D class, S3 PASS and every declared S6 verb clean earns
the separate badge `Storage v4 · <os> · <topology> · D<class> · G<class> · <facets>`.
Values compare only within one OS, topology, profile and declaration set
(`comparison_scope="within-topology-only"`). Saved storage runs appear in `hbench results`
with their pillar, D/G classes, outcome counts and badge. `hbench diff` compares two
storage runs of the same scope, reports `not-comparable-storage-scope` otherwise, and
refuses to compare storage with another pillar. Adapters opt in through an optional
`[storage]` table; see [adapter storage declarations](adapters/README.md#storage-declarations)
and the normative [`docs/SPEC-v4-storage.md`](docs/SPEC-v4-storage.md).

## Deadlines and saved results

Every run has an internal run-level deadline: 15 minutes for `quick` and 30
minutes for `cert`. Override it with `--deadline SECS` or `AHRB_DEADLINE`.
When it expires, AHRB stops launching rows, cleans up its complete owned process
tree, writes a nonzero-exit `report.json`, and marks pending rows
`ERROR: deadline`. A row's manifest `turn_timeout_ms` remains a row-local error
and does not stop later rows.

By default each run is saved under:

```text
results/<harness_id>/<UTC-ISO-timestamp>-<short-run-id>/
```

The directory contains the full report and JSONL evidence bundle (plus
`junit.xml` when requested and `run-error.txt` on an aborted/deadline run).
`--output DIR` writes the primary bundle there and also mirrors it into
`results/`. Use `--no-save` or `AHRB_NO_SAVE=1` to opt out of that durable copy.

`results/index.jsonl` receives one append-only JSON object per saved run. Line
schema 3 records `schema`, `pillar`, `run_key`, `completed_at`, `harness`,
`harness_version`, `report_path`, `report_schema`, `spec_version`, `profile`, `os`,
`topology`, `outcome_counts` (`PASS`, `FAIL`, `UNSUPPORTED`, `ERROR`; `ABSENT` is
counted as `ERROR`), `badge_label`, `resource_summary` (`peak_rss_mib`,
`cpu_per_turn_p50_ms`, `cpu_per_turn_p95_ms`, `wall_per_turn_ms`,
`sampler_overhead_pct`, row-47/48/60 disk and model-wait fields when measured),
`metrics`, `manifest_sha256`, `workflow_sha256`, `ahrb_revision`, and
`storage_summary` (the complete typed storage object for storage runs, otherwise null).
Legacy schema-1/2 lines remain readable; storage data is never reconstructed for them.

Legacy `resource_summary.cpu_total_s` and `cpu_per_turn_ms` are retired: samples from
different collectors have independent cumulative counters and workload windows. CPU
comparisons use validated row-46 `cpu_per_turn_p50_ms` / `cpu_per_turn_p95_ms` only,
within the same OS, topology and profile. These optional fields are also in the saved
index and `hbench diff`; absent row-46 evidence stays absent. Historical unmeasured Wave-4 null detail blocks remain null when read; they do not become measurements. Historical legacy fields
remain readable but are excluded from numeric comparisons and are not recomputed.

Show the latest saved run per harness, or history, with:

```console
hbench results
hbench results codex --all
ahrb results --all
```

## Adapter manifests

Adapters are TOML data, not harness-specific Rust. They describe executable discovery,
isolated profiles, fake-model bindings, lifecycle and session operations, transport,
events, process ownership, limits, hooks, cleanup, and capabilities. The complete
reference is [`adapters/mock/manifest.toml`](adapters/mock/manifest.toml).

The nine named real harnesses are available through `hbench`:

| Harness | `hbench` name | Manifest |
|---|---|---|
| Claude Code | `claude-code` | [manifest](adapters/claude-code/manifest.toml) |
| Codex | `codex` | [manifest](adapters/codex/manifest.toml) |
| Haider Agent | `haider` | [manifest](adapters/haider-agent/manifest.toml) |
| OpenCode | `opencode` | [manifest](adapters/opencode/manifest.toml) |
| Pi | `pi` | [manifest](adapters/pi/manifest.toml) |
| Rick | `rick` | [manifest](adapters/rick/manifest.toml) |
| Aider | `aider` | [manifest](adapters/aider/manifest.toml) |
| Goose | `goose` | [manifest](adapters/goose/manifest.toml) |
| Cline CLI | `cline` | [manifest](adapters/cline/manifest.toml) |

Adapter availability is not certification. See [adapter notes](adapters/README.md)
for pinned versions, event and session limitations, and undeclared capabilities.
`cline-cli` is accepted as a compatibility alias for `cline`.
Start with a small selection and a fresh absolute output directory:

```sh
hbench goose --profile quick --tests 1-3,9,10,15 \
  --output "$PWD/results/goose/$(date -u +%Y%m%dT%H%M%SZ)"
hbench cline --profile quick --tests 1-3,9,10,15 \
  --output "$PWD/results/cline/$(date -u +%Y%m%dT%H%M%SZ)"
hbench aider --profile quick --tests 1-3,9,10,15 \
  --output "$PWD/results/aider/$(date -u +%Y%m%dT%H%M%SZ)"
```

Credentials are generated per run, supplied through private environment/config
bindings, redacted from evidence, and never placed in argv. Commands are direct argv
arrays, never shell strings.

## Built-in mock harness

`ahrb-mock-harness` is a deliberately small headless agent used to test AHRB itself. It
supports JSONL RPC, persistent sessions, cursor replay, safe-boundary steer, pre-tool
subturn, queued prompts, native child creation, cancellation, hooks, and an append-only
fsync'd journal. Its default client-side idle deadline is 1000 ms and is configurable by
the adapter. Terminal events and exit categories are structured and deterministic.

## Certification

All mandatory rows across the four pillars must pass; resource results cannot compensate
for correctness failures. A certification label includes OS, process topology, N=8,
resource class, and supported automation facets. Raw process membership and time series
remain part of the report so results can be audited.

The normative behavior and 41-row matrix live in [`docs/SPEC.md`](docs/SPEC.md).

## License

Licensed under the Kingdom of Abraham Permissive License (KOA-P-1.0), an MIT-equivalent
license for the AI Agents Era — see [`LICENSE.md`](LICENSE.md). The license must be
included in full (notice, preamble, and terms) in all copies or substantial portions.

For the quiet Mac mini six-harness reference across matrix, economy, fidelity and storage,
use [`scripts/reference-run.sh`](scripts/reference-run.sh) and
[`scripts/reference-tables.py`](scripts/reference-tables.py); see the
[one-command procedure and resume rules](docs/HANDOFF-macmini.md#2--one-command-reference-serial-quiet).
