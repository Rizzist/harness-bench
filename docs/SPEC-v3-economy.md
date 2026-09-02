# AHRB v3 harness-economy MVP

This document defines the independent harness-economy pillar implemented by
`hbench economy <harness>` and `ahrb run --pillar economy`. Ordinary `ahrb run`
continues to execute the v1/v2 matrix and does not include economy work or fields.

## Standardized task

The task ID is `ahrb-harness-economy-mvp-v1`. A deterministic bootstrap response
materializes five fixture files using one five-call tool batch. The measured harness
then follows these checkpoints in one session:

1. context build: read all five fixtures in one response;
2. batch probe: issue three independent reads in one response;
3. edit: write `economy-output.txt`;
4. verify: read that file back;
5. iterate under accumulated context: read a retained fixture;
6. iterate once more, then follow the scripted terminal.

The reference mock reaches the terminal on primary request 8. The primary-request
budget is 8 for `quick` and 20 for `cert`. Bootstrap is part of the measured request
stream and makes fixture materialization portable across harness-owned workspaces.

## Analysis boundary

All economy values are derived after the run from the canonical request bodies
already retained by the fake model and from its normalized terminal events. No
harness instrumentation or provider-reported usage is used. Primary versus
side-channel classification is the same classification used by v2 row 42.

The pinned tokenizer implementation is
`ahrb-o200k-base-style-bpe-v1`. Its complete merge vocabulary is
`assets/ahrb_o200k_base_style_v1.tiktoken`; every byte token is normative and
implicit, and the file pins the additional merge ranks. Every report records the
vocabulary SHA-256 and entry count. This is an intentionally provider-neutral,
o200k-style reference BPE rather than OpenAI's full `o200k_base` vocabulary, so the
public label is `reference tokens (o200k_base-style)`.

The fixed reference tariff is USD 10.00 per million reference request tokens. It is
a stable comparison tariff, not a bill or a claim about any provider's price.

## `economy_summary` schema 1

The report's optional `economy_summary` object has the following fields:

| Field | Meaning |
|---|---|
| `schema` | Economy schema, currently `1`. |
| `task`, `profile`, `turn_budget` | Task identity and execution envelope. |
| `reference_tokenizer` | Encoding label, implementation version, vocabulary SHA-256, and vocabulary entry count. |
| `reference_token_label` | Honest label for token-derived values. |
| `model_turns` | Count of primary model requests. |
| `total_reference_tokens` | Sum of reference-BPE tokens over every captured request body, including classified side-channel requests. |
| `tool_calls` | Tool calls emitted by unique accepted scripted responses. |
| `tool_results` | Unique correlated tool-result IDs observed in primary requests. |
| `tool_result_requests` | Following requests that first carry one or more results. |
| `tool_batching_factor` | `tool_results / tool_result_requests`. |
| `last_context_size_tokens` | Largest primary request in reference tokens: the carried-context peak. |
| `completion` | `completed`, `aborted`, `stalled`, `looped`, or `over-budget`. |
| `completion_label` | Always `followed scripted terminal`; this is not real-task success. |
| `reference_tariff_usd_per_million_tokens` | Fixed comparison tariff. |
| `reference_cost_usd` | `total_reference_tokens * tariff / 1_000_000`. |
| `tokens_per_completed_task` | Total reference tokens for `completed`; otherwise null. |

The six headline columns are `model_turns`, `total_reference_tokens`, the paired
`tool_calls`/`tool_batching_factor`, `last_context_size_tokens`, `completion`, and
the paired `reference_cost_usd`/`tokens_per_completed_task`. Reports render them as
a table suitable for cross-harness comparison. `hbench diff` adds typed economy
deltas and suppresses token/cost deltas when tokenizer or tariff pins differ.

## Boundary from v2 row 42

Row 42 remains physical HTTP POST accounting in bytes per turn. Economy reuses the
same captured bodies but measures the token and cost proxy across one realistic,
multi-step task. Economy does not add a cache-eligibility column.
