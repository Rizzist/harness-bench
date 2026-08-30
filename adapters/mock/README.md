# AHRB reference mock harness

Build both binaries, then start the mock with a private run directory:

```sh
cargo build
AHRB_MOCK_BASE_URL=http://127.0.0.1:PORT \
AHRB_MOCK_API_KEY=run-local-secret \
AHRB_MOCK_MODEL=ahrb-fake-v1 \
target/debug/ahrb-mock-harness serve --state-dir /absolute/run/state \
  --session-memory-mib 4
```

If the environment prohibits TCP listeners, set `AHRB_MOCK_UNIX_SOCKET` to the fake
server's Unix-domain socket path and omit `AHRB_MOCK_BASE_URL`. The protocol and request
path remain ordinary HTTP (`POST /v1/chat/completions`) over that socket.

The process speaks newline-delimited JSON-RPC 2.0 on stdin/stdout. The operations are
`session.create`, `session.submit`, `session.attach`, `session.resume`, `session.steer`,
`session.subturn`, `session.queue`, `agent.spawn`, `session.cancel`, `session.close`, and
`checkpoint.release`, plus `harness.shutdown`. `session.submit` acknowledges after
durably appending `turn-accepted`; work continues asynchronously and is observed with
`session.attach`.

Each session owns `sessions/<id>/journal.jsonl`. Every complete event record is followed
by an `fsync`; attach-after-cursor rereads this file. A final partial record after a hard
kill is ignored, while malformed earlier records are reported as corruption. The PID
locator is `daemon.pid`.

The model HTTP client has a documented default **1,000 ms idle deadline** (overridable
with `--idle-timeout-ms`). A stalled model request is terminalized by the harness as a
structured `terminal-failure` with category `idle-timeout`; no outer kill is required.
Only relative, non-traversing paths under a session workspace are accepted by the built-in
fixture tools. No arbitrary shell tool is exposed.

Each submitted live session commits a 4 MiB anonymous, page-touched reservation by
default. The reservation remains present through the terminal turn so post-turn
retention can be measured; `session.close` or `session.cancel` unmaps it immediately and
returns `released_bytes`. This creates a deterministic N-agent footprint and reclaim
surface without a polling thread. Override the size with `--session-memory-mib` when a
test needs a different visible profile.

For a resource hold that works when the runner and daemon use separate embedded model
engines, return a `write_fixture` call with:

```json
"ahrb_checkpoint": {"name": "resource-steady", "phase": "after-commit"}
```

After committing the fixture result, the daemon appends `barrier-reached` with a
run-local `release_token` path and waits for that file to exist. Waiting until all N
sessions expose both `tool-result` and `barrier-reached` is state-based evidence that all
agents are live. Call `checkpoint.release` with the session ID and release token to
durably create the evidence file and wake the held task through an in-daemon notification;
there is no timer-driven file polling. No N-way OS-process fan-out is expected for the
declared `shared-daemon-sessions` topology.
