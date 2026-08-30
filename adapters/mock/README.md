# AHRB reference mock harness

Build both binaries, then start the mock with a private run directory:

```sh
cargo build
AHRB_MOCK_BASE_URL=http://127.0.0.1:PORT \
AHRB_MOCK_API_KEY=run-local-secret \
AHRB_MOCK_MODEL=ahrb-fake-v1 \
target/debug/ahrb-mock-harness serve --state-dir /absolute/run/state
```

If the environment prohibits TCP listeners, set `AHRB_MOCK_UNIX_SOCKET` to the fake
server's Unix-domain socket path and omit `AHRB_MOCK_BASE_URL`. The protocol and request
path remain ordinary HTTP (`POST /v1/chat/completions`) over that socket.

The process speaks newline-delimited JSON-RPC 2.0 on stdin/stdout. The operations are
`session.create`, `session.submit`, `session.attach`, `session.resume`, `session.steer`,
`session.subturn`, `session.queue`, `agent.spawn`, `session.cancel`, `session.close`, and
`harness.shutdown`. `session.submit` acknowledges after durably appending `turn-accepted`;
work continues asynchronously and is observed with `session.attach`.

Each session owns `sessions/<id>/journal.jsonl`. Every complete event record is followed
by an `fsync`; attach-after-cursor rereads this file. A final partial record after a hard
kill is ignored, while malformed earlier records are reported as corruption. The PID
locator is `daemon.pid`.

The model HTTP client has a documented default **1,000 ms idle deadline** (overridable
with `--idle-timeout-ms`). A stalled model request is terminalized by the harness as a
structured `terminal-failure` with category `idle-timeout`; no outer kill is required.
Only relative, non-traversing paths under a session workspace are accepted by the built-in
fixture tools. No arbitrary shell tool is exposed.
