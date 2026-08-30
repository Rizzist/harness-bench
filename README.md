# AHRB — Agent Harness Readiness Benchmark

AHRB is a standalone Rust benchmark for coding-agent harnesses. It replaces real model
inference with deterministic local responses, then checks tool-call correctness,
functionality, whole-process-tree resources, and unattended automation readiness.

The benchmark never needs a model credential, database, container, Python runtime, or
external service. A built-in reference harness exercises the complete pipeline.

## Five-minute quickstart

Requirements: stable Rust on macOS or Linux.

```console
cargo build --locked
cargo run --locked --bin ahrb -- doctor --manifest adapters/mock/manifest.toml
cargo run --locked --bin ahrb -- list-tests
cargo run --locked --bin ahrb -- run --manifest adapters/mock/manifest.toml --output ./ahrb-results --profile quick
```

The run produces `report.md`, `report.json`, `samples.jsonl`, `processes.jsonl`,
`events.jsonl`, and `model-requests.jsonl`. Add `--junit` for `junit.xml`.

For a selected quick smoke run:

```console
cargo run --locked --bin ahrb -- run \
  --manifest adapters/mock/manifest.toml \
  --output ./ahrb-smoke \
  --tests 1,2,3,9,10,12,20,26,30,35,40
```

## Adapter manifests

Adapters are TOML data, not harness-specific Rust. They describe executable discovery,
isolated profiles, fake-model bindings, lifecycle and session operations, transport,
events, process ownership, limits, hooks, cleanup, and capabilities. The complete
reference is [`adapters/mock/manifest.toml`](adapters/mock/manifest.toml).

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

Dual-licensed under Apache-2.0 or MIT, at your option.
