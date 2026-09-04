# AHRB v3 long-horizon context fidelity

This document defines the independent context-fidelity pillar implemented by
`hbench fidelity <harness>` and `ahrb run --pillar fidelity`. Ordinary
`ahrb run` and `--pillar economy` do not execute this task and do not emit a
`fidelity_summary` field.

## Standardized task

The task ID is `ahrb-harness-fidelity-longhorizon-v1`. It is deterministic and
uses the same portability pattern as the economy task: the first scripted fake-model
response calls adapter-mapped write fixtures, so the harness materializes all seed
inputs inside its own isolated workspace. No real inference participates.

For exec adapters the driver exposes the exact per-session working directory used
for the invocation. A non-exec adapter must either return an absolute
`workspace_path` in its public `session.create` result or declare
`fidelity.workspace_path = "{{profile}}/.../{{session_id}}"`. The resolved path
must be a nonempty normalized child of the fresh profile and must not traverse a
symlink. The built-in non-exec reference fixture returns the assigned
`AHRB_FIDELITY_WORKSPACE` path in `session.create`. Workspace receipts therefore
follow public driver/adapter data rather than guessing from a harness name.

The request sequence is:

1. bootstrap six seed files, including one deterministic applied edit, through
   the ordinary adapter-mapped write fixture contract;
2. read four structural-needle files in one tool batch;
3. repeat five (`quick`) or ten (`cert`) read → edit → verify → iterate cycles;
4. audit every fixture added by those cycles in one final read batch;
5. follow the scripted terminal.

Each cycle reads a prior fixture, writes one new fixture, reads the new file back,
and revisits an older non-needle fixture. Each cycle output carries a deterministic
digest derived from all four structural contracts, so its verification depends on
the planted signature, path, edit receipt, and ordinal rather than treating those
needles as decorative labels. The fixture set and serialized conversation therefore
grow throughout an ordinary append-only run. `quick` has exactly 24 primary
requests. `cert` has exactly 44 primary requests.

The four needles are exact, unique byte strings carried by the batched read results:

- an exact Rust function signature;
- the virtual task-root absolute path `/ahrb/fidelity/fixtures/ledger-v1.toml`,
  materialized at `ahrb/fidelity/fixtures/ledger-v1.toml` inside the isolated
  actor workspace;
- the SHA-256 of the deterministic edit applied during bootstrap;
- an ordinal checkpoint marker.

The analyzer records a needle's `planted_turn` at the first canonical request whose
tool-result carrier contains its exact bytes. In the reference workflow that is turn
3, after the required batched reads. Earlier scripted tool-call arguments do not
plant a needle. From the planting turn forward, presence is tested against the
complete serialized canonical primary request, so a retained copy elsewhere in the
conversation still counts. `checkpoint-0004` is the actual fourth scripted
checkpoint, and the signature is a declaration in the materialized API contract.

## Analysis and honesty boundary

All context values are derived after the run from canonical request bodies captured
at the fake-provider boundary and from normalized events. Primary-request ordering
is `(received_ns, semantic_ordinal, attempt)`, the same physical-request ordering used
by economy. Retries therefore remain visible. AHRB does not use provider usage fields
or harness self-reporting for request-content retention.

The fake model scripts every tool call. Consequently this pillar measures the
**cause surface**—what context the harness sent—not whether a real model understood
it, chose the right action, or completed a real coding task. The workspace receipt
compares the complete pre-cleanup tree of the isolated actor workspace. It cannot
demonstrate that a model independently chose to edit, and it must never be reported
as model competence or task success.

Needle presence is byte presence, not semantic equivalence. Rephrasing, summarizing,
tokenizing, or otherwise transforming a needle counts as loss unless the original
exact bytes are also sent. This strictness is intentional and reproducible.

## `fidelity_summary` schema 1

| Field | Exact computation and interpretation |
|---|---|
| `schema` | Fidelity schema, currently `1`. |
| `task`, `profile`, `turn_budget` | Task identity and its 24-request quick or 44-request cert envelope. |
| `model_turns` | Number of captured physical requests classified `primary`. |
| `measurement_label` | States that values are fake-provider byte observations, not model understanding, competence, or success. |
| `needles` | Ordered planted set. Each entry contains the stable `id`, exact `token`, first tool-result-bearing `planted_turn` (null if never delivered), first later absent `first_disappeared_turn`, and `ever_reappeared` after that loss. Reappearance can reveal reinjection from harness-managed storage, but does not prove why it occurred. |
| `needle_survival_fraction` | At the final primary request, exact-byte-present needles divided by the complete four-needle task set. An undelivered needle counts as absent, so an early ending before planting reports `0.0`, not apparent perfect retention. |
| `needle_survival_fraction_label` | Honest label for the headline fraction. |
| `survival_curve` | One value per primary request. At turn N, exact-byte-present delivered needles are divided by the complete four-needle task set. Undelivered needles count as absent. A cliff is evidence of a serialized-context discontinuity; it is not a claim about comprehension. |
| `survival_curve_label` | Honest label for the complete-set denominator and byte-presence rule. |
| `first_loss_turn` | Minimum non-null per-needle `first_disappeared_turn`; null if none disappeared. |
| `retained_tool_result_fraction` | One value per primary request. Canonical tool-result carriers are keyed by their correlated call ID and frozen on first appearance. The numerator is the number of those byte-identical carriers present in the current request; the denominator is all unique carriers delivered through that request. Before any carrier, the value is `1.0`. |
| `retained_tool_result_fraction_label` | Honest label for the carrier-level byte-retention proxy. |
| `end_reason` | `reached-scripted-terminal`, `harness-internal-ceiling`, `ahrb-deadline`, or `crashed`, using the classification below. |
| `end_reason_label` | States that the field is a script-orchestration outcome, not real-task success. |
| `end_turn` | Last captured primary-request ordinal; zero if none was captured. |
| `harness_exit_status`, `harness_exit_code` | Pre-cleanup process evidence. Status is `not-applicable`, `running`, `exit-code`, or `signal-or-unknown`; the code is present only when exposed. |
| `internal_cap_detected` | True only with typed adapter evidence: a declared internal-cap exit code, or the request count reached `declared_turn_ceiling` and then aborted, failed, or remained unchanged until AHRB's deadline before the scripted terminal. It is never inferred from harness identity or slowness below the declared ceiling. |
| `declared_turn_ceiling` | Adapter manifest declaration, or null. The optional `[fidelity]` block can also declare typed `internal_cap_exit_codes` and a data-driven `workspace_path` template for a non-exec topology. |
| `workspace_state` | `mutated` when the stable post-run, pre-cleanup full-tree receipt of the isolated actor workspace differs from the pre-run receipt; otherwise `untouched`. Evidence is captured before cancellation, close-delete, or shutdown. |
| `workspace_state_label` | States that the full actor-workspace receipt observes scripted effects, not model choice or task success. |
| `workspace_receipt_before_sha256`, `workspace_receipt_after_sha256` | SHA-256 of the existing tree-snapshot representation: sorted directory, regular-file `(path, size, content SHA-256)`, symlink `(path, target)`, and other-kind entries rooted at the isolated actor workspace. Task fixture files are also captured with the existing `filesystem_snapshots` report type. |

### End-reason precedence

Classification is deterministic:

1. an accepted `terminal` request plus normalized terminal success is
   `reached-scripted-terminal`;
2. typed internal-cap evidence is `harness-internal-ceiling`;
3. any other failure terminal, non-cap exit, or collection failure is `crashed`;
4. expiration of AHRB's run-level fidelity deadline without stronger exit evidence
   is `ahrb-deadline`.

The deadline clock starts before profile/task setup and bounds fake-model startup,
harness startup/readiness, session creation, submit, and terminal collection.
Harness-stage startup and submission errors still persist a `crashed` summary; a
deadline at those stages persists `ahrb-deadline`.

For an undocumented ceiling, `declared_turn_ceiling` remains null. AHRB can still
set `internal_cap_detected` only if the adapter declares the corresponding exit code.
Without typed evidence, a deadline is labeled as AHRB's deadline and a
process/collection failure as crashed. A nonzero exit alone is not guessed to be a
cap because it can equally be a crash.

## Diff behavior

`hbench diff` emits `fidelity_summary_deltas` only when at least one input contains
the block. Numeric and curve deltas are comparable only when task ID and profile are
equal. Equal-length survival and tool-result-retention curves receive element-wise
deltas. A mismatch is labeled `not-comparable-task-or-profile`; no numeric delta is
manufactured.
