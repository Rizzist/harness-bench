# AHRB v3 harness economy

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
a stable comparison tariff, not a bill or a claim about any provider's price. The
cache input discount is separately pinned at `0.90`: for the cache-adjusted proxy,
an eligible token is assumed to be billed at 10% of full input price. This is a
stated benchmark assumption, not a measured rate or a claim about a provider's
current price.

## `economy_summary` schema 3

The report's optional `economy_summary` object has the following fields:

| Field | Meaning |
|---|---|
| `schema` | Economy schema, currently `3`. Schema 3 only adds fields to schemas 1 and 2. |
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
| `cache_eligible_fraction` | **Cache-eligible fraction (prefix upper bound)**. For each ordered primary physical request, tokenize the canonical serialization of its message array. Sum the reference-token longest common prefix with the preceding primary request (zero for the first), then divide by all message-array reference tokens in the primary request stream. This is not a realized cache hit rate. |
| `cache_eligible_fraction_label`, `cache_eligibility_note` | The normative honest label and a topology-aware cache-persistence caveat. A per-invocation harness has no cross-turn server cache, so the number only describes repeated prefixes serialized within its requests. |
| `cache_control_breakpoints`, `cache_control_breakpoints_per_request`, `cache_control_breakpoints_label` | Recursive counts of harness-declared JSON fields named `cache_control`, totaled and ordered per primary physical request. Property-name occurrences inside JSON Schema `properties`/`$defs` maps are excluded. These declarations are reported separately and never treated as observed cache hits. |
| `redundant_tokens` | **Redundant reference tokens**. Split each primary request's message/input array into top-level message blocks; when a message uses an array-valued `content`, use each content segment as its block. A block is redundant when its canonical JSON bytes appeared in an earlier primary request. Sum the reference tokens of every re-sent identical block. Repetition first seen within the same request is not cross-turn redundancy. |
| `context_token_curve` | **Context token curve**: the ordered full-canonical-request reference-token count of every primary physical request. Its length equals `model_turns`. On the standardized accumulating task its final point cross-checks `last_context_size_tokens`; `context_token_curve_last_matches_last_context_size` records the check without changing the schema-1 peak semantics. |
| `context_token_curve_slope`, `context_token_curve_label` | The deterministic Theil-Sen median of all pairwise curve slopes, in reference tokens per request, and its honest label. |
| `per_turn_fixed_overhead_tokens` | **Fixed overhead / turn**, in reference tokens. Take the exact common block prefix, across every primary physical request, independently for leading system/developer instructions and tool-schema definitions; serialize those invariant parts in the dialect's canonical instruction/tool envelope and tokenize it. If the run changes dialect, no invariant overhead is claimed. |
| `wasted_tool_call_count` | **Wasted tool calls (deterministic task-proven only)**. A unique accepted scripted call is counted only when a successful normalized result proves execution and the known scripted effect has neither a successful workspace mutation nor a result carried into a later primary request. Missing or failed evidence is not guessed to be waste. |
| `retry_attempts`, `retry_reference_tokens`, `retry_label` | Retry attribution kept separate from the economy columns. `retry_attempts` reuses row 42's maximum declared attempt count per semantic request; `retry_reference_tokens` sums captured physical request attempts after attempt one. Schema-1 totals intentionally still include all captured physical attempts. |
| `cache_regime` | Provider cache behavior derived from the captured primary-request dialect: OpenAI chat completions or Responses is `automatic-prefix`; Anthropic Messages is `explicit-cache-control`; mixed, empty, or unknown dialects are `none`. |
| `cache_regime_label` | `provider cache regime; 0 cache_control breakpoints under automatic-prefix is expected, NOT no caching`. Explicit breakpoints are not expected for an automatic-prefix provider path. |
| `cache_input_discount` | Pinned `0.90`, the stated fraction of full input price assumed not paid for a cache-eligible token. |
| `cache_input_discount_label` | `STATED assumption: fraction of full input price not paid for a cache-eligible token; NOT a measured provider rate`. |
| `effective_reference_tokens` | `total_reference_tokens * (1 - cache_eligible_fraction * cache_input_discount)`. This is a floating-point billable-equivalent token proxy, not a provider usage value. |
| `effective_cost_usd` | `effective_reference_tokens * reference_tariff_usd_per_million_tokens / 1_000_000`. `reference_cost_usd` remains the unchanged worst-case uncached comparison value. |
| `effective_cost_label` | `effective cost assuming automatic prefix caching of the eligible prefix at 90% input discount; UPPER-BOUND proxy from serialized-prefix reuse, NOT a measured server cache-hit rate`. |
| `stable_prefix_preserved_fraction` | Number of measured consecutive primary-request transitions with zero invalidation divided by the number of transitions. It is `0.0` when there are fewer than two primary requests. |
| `cache_bust_count` | Number of measured consecutive primary-request transitions with nonzero prefix invalidation. |
| `invalidated_prefix_tokens` | Sum of prior-request reference tokens invalidated by cache busts. |
| `invalidated_prefix_tokens_per_turn` | Ordered invalidation values for primary request transitions N>=2; its length is `model_turns - 1` when at least one primary request exists. |
| `prefix_stability_label` | `consecutive primary-request serialized-message-prefix stability (reference-token LCP; ordered invalidation array covers turns N>=2)`. |

The six MVP headline columns remain `model_turns`, `total_reference_tokens`, the paired
`tool_calls`/`tool_batching_factor`, `last_context_size_tokens`, `completion`, and
the paired `reference_cost_usd`/`tokens_per_completed_task`. Reports render them as
a table suitable for cross-harness comparison. Schema 2 adds the five advanced
headline columns `cache_eligible_fraction`, `redundant_tokens`,
`context_token_curve` (with `context_token_curve_slope`),
`per_turn_fixed_overhead_tokens`, and `wasted_tool_call_count`.
Schema 3 leaves all eleven headline columns unchanged and adds cache-regime,
cache-adjusted-cost, and prefix-stability diagnostics.

Every advanced calculation uses the same fake-provider canonical bodies and
normalized events as schema 1. Primary-request ordering is
`(received_ns, semantic_ordinal, attempt)`. Physical retries remain visible in
`model_turns`, totals, and the curve because those are request-stream measures;
their count and token contribution are also disclosed explicitly. No real-model
inference, provider usage field, server cache observation, or harness
instrumentation participates.

Schema 3's prefix-stability calculation uses the same canonical message/input
array selection, deterministic JSON serializer, and reference tokenizer as
`cache_eligible_fraction`: the complete array is serialized and tokenized. For
transition N, the benchmark takes the token LCP of N-1 and N. A structurally
append-only array is assigned the prior array's full token length as its effective
LCP, so the serializer's closing `]` framing cannot create a false one-token bust.
If the effective LCP is shorter than N-1, invalidation is
`tokens(N-1) - LCP`; otherwise it is zero. Reordering, deleting, or mutating an old
message therefore invalidates the changed suffix, while clean append-only growth
does not. The metric is still a provider-neutral serialized-prefix proxy; it does
not observe a server cache.

`hbench diff` adds typed scalar deltas for all five advanced columns and an
element-wise typed delta for equal-length context curves. Schema 3 adds typed
deltas for effective reference tokens, effective cost, stable-prefix fraction,
cache-bust count, invalidated-prefix tokens, and the ordered per-turn invalidation
array, plus before/after/change values for `cache_regime`. Economy numeric and
array deltas are suppressed when the tokenizer or tariff pins differ. Effective
reference-token and cost deltas are additionally suppressed when
`cache_input_discount` differs. Schema-1 and schema-2 reports deserialize with
absent later-schema fields and do not manufacture those values.

## Boundary from v2 row 42

Row 42 remains physical HTTP POST accounting in bytes per turn. Economy reuses the
same captured bodies but measures the token and cost proxy across one realistic,
multi-step task. Economy's cache-eligibility value is only a serialized-prefix
upper bound; explicit `cache_control` declarations are separate, and neither is
a realized cache-hit measurement.
