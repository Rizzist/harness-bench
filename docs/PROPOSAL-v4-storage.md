# AHRB v4 proposal — the STORAGE pillar (no code) · 2026-09-06

## Why
The peer's disk analysis of haider: append-only journal + full request bodies in provider views ⇒ expected linear growth per session with a large constant; never measured. AHRB has correctness, resource, automation, economy, fidelity — nothing measures on-disk footprint, growth, retention or durability cost. Row 47 (disk-io/turn, `ri_diskio_byteswritten`) and the isolation check (nothing written outside per-run roots) exist; they are the seed, not the pillar.

## What every harness must be measured on (same fixture, same machine, paired, fake model = fixed payload sizes)
| row | metric | how (harness-agnostic) | verdict |
|---|---|---|---|
| S1 write volume | physical bytes written per turn (row 47) AND logical growth of the run root per turn (allocated blocks, not `du` apparent size) → write amplification = physical ÷ logical | whole-tree sampler already owns the process tree; run root = hermetic HOME + workspace | class D (KiB/turn steps), amplification reported |
| S2 durability cost | fsync / fdatasync / F_FULLFSYNC count per turn + estimated durability wall (×4 ms on this Mac) | Linux: strace/eBPF on the owned tree; macOS: interpose shim via DYLD_INSERT_LIBRARIES on the harness binary (not SIP-protected); if neither possible → UNSUP-os-limited with evidence, never a guess | informational + F class |
| S3 footprint curve | on-disk size after 1 / 10 / 50 / 100 turns of the standardized task; slope + shape (bounded / linear / superlinear); the "large constant" (size after turn 1) | same 100-turn driver as row 49 | class G (bounded / linear / superlinear); FAIL only on superlinear |
| S4 compaction vs disk | footprint before / after the harness's context-limit event (row 51 trigger) | reuse row 51 | informational: "compaction frees N % of disk" (expect 0 for append-only journals — say so) |
| S5 close retention | bytes retained per CLOSED session after N create/close cycles (row 50 driver), then after the declared expiry sweep interval | manifest `[storage].sweep_interval_s` optional | bounded (retained ≤ declared cap) / unbounded |
| S6 delete + uninstall residue | after the harness's own `session delete` verb (if declared) and after `uninstall_cleanup` (if declared): bytes + file count left in the profile | manifest verbs; missing verb → UNSUP (honest: "no way to delete a session") | residue reported; core if a delete verb exists and leaves > 0 |
| S7 bounded auxiliaries | logs, WAL, history files over 100 turns: rotated / capped or growing | file-family growth from S3's samples | bounded / unbounded per family |
| S8 request-body retention | does the harness persist full model request bodies? none / deduplicated / full; stored request bytes ÷ unique content bytes | fake model knows every request byte → compare with run-root growth | classification + ratio; privacy note (prompt bodies on disk) |
| S9 crash residue | after row 57's kill -9 mid-turn: orphaned temp files, journal integrity as judged by the harness's own resume (row 52) | reuse | count + resume outcome |
| S10 resume read cost | bytes read to resume a 100-turn session (`ri_diskio_bytesread` around row 52) | reuse | informational, pairs with resume latency |

## Declarations (adapter = data, as always)
Optional `[storage]` block per manifest: named areas as globs relative to the run root (`store`, `cas`, `views`, `pipes`, `logs`, `other` = the remainder) for the per-family split; `session_delete`, `uninstall_cleanup` commands; `sweep_interval_s`. Undeclared areas fold into `other`; undeclared verbs → UNSUP rows, never inferred.

## Honesty rails
- Physical (what hit the disk) and logical (allocated blocks) reported separately; APFS clones/sparse files counted by blocks; settle + `sync` before sampling so writeback lag doesn't move a number between harnesses.
- Fixed request/response sizes from the economy fixture so bytes compare across harnesses; the fake model is the endpoint, zero real inference.
- Topology-fair: per-invocation harnesses that rewrite a whole transcript file each turn show it as amplification; daemons show it as WAL/fsync — both are the truth of the design, reported in one row.
- Badge addition: `D<KiB/turn> · G<bounded|linear|superlinear>`; S3 superlinear or S6 residue-with-verb are the only FAILs; everything else informational, with classes.
- "Never measured" rule: no expected values in the spec; the first six-harness run establishes the reference, haider included.

## Expected shape of the first table (hypotheses to be measured, not claims)
codex: per-session rollout JSONL + history.jsonl, append-only, low fsync · claude-code: per-project JSONL transcripts + rewritten todo/state JSON (amplification candidate) + debug logs · opencode: on-disk store per message (JSON or SQLite) · pi: one JSONL per session, likely the smallest constant · rick: unknown · haider: SQLite WAL journal + content-addressed provider views (largest constant, dedup ratio is the interesting number) + pipes.

## Cost / sequencing
Two waves: W1 = S1, S3, S5, S7, S8 (no new OS hooks; reuse rows 47/49/50/51 drivers + block accounting) → first six-harness storage table. W2 = S2 (fsync hooks, OS-specific), S6 (manifest verbs), S9/S10 (reuse 52/57). Spec first (`docs/SPEC-v4-storage.md`), gpt-5.6/astra implements per item with the verify-loop; the rows are cheap (no new fixtures, same 100-turn driver). Storage isolation stays where it is.
