//! Versioned declarative definitions for implemented benchmark rows.

use crate::evaluate::Pillar;

/// Metadata for one row in the authoritative test matrix.
#[derive(Clone, Copy, Debug)]
pub struct TestDefinition {
    /// One-based row number.
    pub row: u8,
    /// Stable machine-readable ID.
    pub id: &'static str,
    /// Human-readable name.
    pub name: &'static str,
    /// Badge-gating pillar.
    pub pillar: Pillar,
    /// Measurement named by the specification.
    pub metric: &'static str,
    /// Concise deterministic pass criterion.
    pub pass_criteria: &'static str,
}

/// Topology-relative certification role carried by a matrix row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequirementKind {
    /// A topology-independent row that every certifiable harness must pass.
    Core,
    /// An architectural facet that may be honestly unsupported.
    OptionalFacet {
        /// Manifest capability declaration associated with this facet.
        capability: &'static str,
    },
    /// A reference-envelope measurement that does not gate the badge.
    Informational,
}

/// One deterministic suffix component shown when its matrix row passes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BadgeFacet {
    /// Matrix row proving this facet.
    pub row: u8,
    /// Stable badge-label component.
    pub label: &'static str,
    /// Explicit display order independent of matrix row order.
    pub order: u8,
    /// Topology families on which this label is shown.
    pub scope: BadgeFacetScope,
}

/// Topology scope for one badge suffix component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BadgeFacetScope {
    /// Show whenever the proving row passes.
    All,
    /// Show only for fresh-process-per-turn architectures.
    PerInvocation,
}

impl TestDefinition {
    /// Return this row's certification role from authoritative matrix metadata.
    pub fn requirement(self) -> RequirementKind {
        if matches!(self.row, 42 | 43 | 45 | 46 | 47 | 63 | 65 | 69) {
            return RequirementKind::Informational;
        }
        OPTIONAL_FACETS
            .iter()
            .find(|facet| facet.row == self.row)
            .map_or(RequirementKind::Core, |facet| {
                RequirementKind::OptionalFacet {
                    capability: facet.capability,
                }
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OptionalFacet {
    row: u8,
    capability: &'static str,
}

const OPTIONAL_FACETS: [OptionalFacet; 11] = [
    OptionalFacet {
        row: 4,
        capability: "parallel_tool_execution",
    },
    OptionalFacet {
        row: 18,
        capability: "native_delegation",
    },
    OptionalFacet {
        row: 56,
        capability: "native_delegation",
    },
    OptionalFacet {
        row: 31,
        capability: "steer",
    },
    OptionalFacet {
        row: 32,
        capability: "pre_tool_intervention",
    },
    OptionalFacet {
        row: 33,
        capability: "queue",
    },
    OptionalFacet {
        row: 39,
        capability: "hooks",
    },
    OptionalFacet {
        row: 66,
        capability: "budget_enforcement",
    },
    OptionalFacet {
        row: 67,
        capability: "usage_reporting",
    },
    OptionalFacet {
        row: 68,
        capability: "session_ops_cli",
    },
    OptionalFacet {
        row: 70,
        capability: "headless_permission_model",
    },
];

/// Badge suffix components and their normative display order.
pub const BADGE_FACETS: &[BadgeFacet] = &[
    BadgeFacet {
        row: 30,
        label: "replay",
        order: 10,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 35,
        label: "crash",
        order: 20,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 37,
        label: "resume",
        order: 25,
        scope: BadgeFacetScope::PerInvocation,
    },
    BadgeFacet {
        row: 31,
        label: "steer",
        order: 30,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 33,
        label: "queue",
        order: 40,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 18,
        label: "native-delegation",
        order: 50,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 32,
        label: "subturn",
        order: 60,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 39,
        label: "hooks",
        order: 70,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 66,
        label: "budgets",
        order: 80,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 67,
        label: "usage",
        order: 90,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 68,
        label: "session-cli",
        order: 100,
        scope: BadgeFacetScope::All,
    },
    BadgeFacet {
        row: 70,
        label: "permissions",
        order: 110,
        scope: BadgeFacetScope::All,
    },
];

/// Return all matrix definitions in row order.
pub fn all() -> &'static [TestDefinition] {
    &TESTS
}

const TC: Pillar = Pillar::ToolCallCorrectness;
const FN: Pillar = Pillar::Functionality;
const RS: Pillar = Pillar::Resource;
const AR: Pillar = Pillar::AutomationReadiness;

/// Matrix rows that must pass early in the implementation and verification loop.
pub const PRIORITIZED_ROWS: &[u8] = &[1, 2, 3, 9, 10, 12, 20, 26, 30, 35, 40];

const TESTS: [TestDefinition; 72] = [
    test(
        1,
        "routing",
        "Exact model+endpoint+credential routing",
        TC,
        "observed model routes",
        "all roles use only the configured local tuple",
    ),
    test(
        2,
        "single-tool-call",
        "Single tool call in one turn",
        TC,
        "tool effects and correlations",
        "one byte-matching call, effect, result, and terminal",
    ),
    test(
        3,
        "sequential-tool-calls",
        "Multiple sequential tool calls",
        TC,
        "ordered tool effects",
        "A then dependent B execute exactly once",
    ),
    test(
        4,
        "parallel-tool-calls",
        "Parallel tool calls in one turn",
        TC,
        "overlap and result association",
        "both calls overlap and correlate despite reversed release",
    ),
    test(
        5,
        "fragmented-tool-args",
        "Fragmented streamed args",
        TC,
        "reassembled arguments",
        "exact JSON is invoked once after complete assembly",
    ),
    test(
        6,
        "failed-tool-result",
        "Failed tool execution as structured result",
        TC,
        "structured failed result",
        "typed failure reaches the next model request",
    ),
    test(
        7,
        "malformed-tool-call",
        "Malformed or unknown tool call",
        TC,
        "protocol failure",
        "structured failure with no unintended tool run",
    ),
    test(
        8,
        "tool-id-dedup",
        "Tool-call ID dedup and order preservation",
        TC,
        "IDs, order, and effects",
        "replayed frames never duplicate semantic effects",
    ),
    test(
        9,
        "terminal-success",
        "Structured terminal success",
        TC,
        "terminal events and exit",
        "exactly one SUCCESS with the documented success exit",
    ),
    test(
        10,
        "terminal-failure",
        "Structured terminal failure",
        TC,
        "terminal events and exit",
        "exactly one distinct FAILURE with stable nonzero exit",
    ),
    test(
        11,
        "upstream-retry",
        "Transient upstream retry and normalization",
        TC,
        "retry count and effects",
        "bounded retry terminalizes with at most one effect",
    ),
    test(
        12,
        "idle-deadline",
        "Client-side idle-deadline self-abort",
        TC,
        "last-byte to own terminal",
        "structured self-failure occurs before the outer deadline",
    ),
    test(
        13,
        "workspace-effects",
        "Workspace and patch effects independent of stdout",
        TC,
        "fixture hashes",
        "file effects match even when stdout truncates",
    ),
    test(
        14,
        "state-network-confinement",
        "Per-run state and network confinement",
        TC,
        "outside writes and connections",
        "all undeclared access fails closed",
    ),
    test(
        15,
        "headless-workflow",
        "Headless tool workflow",
        FN,
        "effect, terminal, and exit",
        "sole marker effect succeeds without human input",
    ),
    test(
        16,
        "transcript-determinism",
        "Multi-turn transcript determinism",
        FN,
        "five semantic transcript hashes",
        "all hashes and event counts are identical",
    ),
    test(
        17,
        "actor-isolation",
        "Concurrent actor isolation",
        FN,
        "per-actor effects",
        "no token or workspace crosses actor boundaries",
    ),
    test(
        18,
        "native-delegation",
        "Native delegation",
        FN,
        "durable children and reports",
        "one native child and one report per spawn",
    ),
    test(
        19,
        "exit-codes",
        "Deterministic exit codes",
        FN,
        "six exits over five repetitions",
        "each category has one invariant documented exit",
    ),
    test(
        20,
        "idle-rss",
        "Idle RSS baseline at rest",
        RS,
        "whole-tree RSS plateau",
        "topology is classified and relative spread is at most 5%",
    ),
    test(
        21,
        "idle-cpu",
        "Idle CPU and busy-poll detection",
        RS,
        "whole-tree CPU over quiet window",
        "at most 1% of one core with no polling signature",
    ),
    test(
        22,
        "idle-drift",
        "Idle memory drift at rest",
        RS,
        "RSS slope and net growth",
        "at most 1 MiB/min and 8 MiB net",
    ),
    test(
        23,
        "return-to-idle",
        "Return to idle after workflow",
        RS,
        "post-workload residual",
        "residual is within max(64 MiB, 20% active delta)",
    ),
    test(
        24,
        "cold-start",
        "Cold start to steady idle",
        RS,
        "readiness, cold peak, plateau",
        "startup bound and profile cold peak are met",
    ),
    test(
        25,
        "single-agent-resource",
        "Single-agent footprint and CPU per turn",
        RS,
        "B, S1, peak, and CPU",
        "CPU is at most 250ms and barrier CPU below 5%",
    ),
    test(
        26,
        "parallel-memory",
        "Parallel-agent memory delta and total peak",
        RS,
        "N=1,2,4,8 sweep",
        "N8 completes, peak at most 4 GiB, beta at most 256 MiB",
    ),
    test(
        27,
        "scaling-curve",
        "Parallel scaling curve",
        RS,
        "alpha and adjacent marginals",
        "alpha at most 1.20 with no unexplained marginal jump",
    ),
    test(
        28,
        "post-close-reclaim",
        "Post-completion reclaim",
        RS,
        "reclaim and residual",
        "at least 80% reclaim and no owned worker remains",
    ),
    test(
        29,
        "long-horizon",
        "Long-horizon stability",
        RS,
        "1000-turn drift and leaks",
        "at most 64 KiB/turn with bounded final residual",
    ),
    test(
        30,
        "session-replay",
        "Session persist and replay",
        AR,
        "cursor-addressed recovered suffix",
        "ordered suffix appears exactly once in the same session",
    ),
    test(
        31,
        "steer",
        "Safe-boundary next prompt",
        AR,
        "injected safe-boundary input",
        "input affects the active run exactly once",
    ),
    test(
        32,
        "subturn",
        "Pre-tool next prompt",
        AR,
        "input/effect ordering",
        "input is observed before the pending effect",
    ),
    test(
        33,
        "queued-turn",
        "Queued next turn",
        AR,
        "turn order and counts",
        "A terminal precedes one distinct execution of B",
    ),
    test(
        34,
        "noninteractive",
        "Autonomous no-interactive-prompt",
        AR,
        "closed-stdin execution",
        "allowed succeeds and denied fails without a prompt",
    ),
    test(
        35,
        "crash-recovery",
        "Crash recovery kill and resume",
        AR,
        "recovery latency and effects",
        "ready within 10s and committed effect occurs at most once",
    ),
    test(
        36,
        "cancel-cleanup",
        "Cancellation and cleanup",
        AR,
        "terminal latency, orphans, residual",
        "all cancel within 5s and no owned PID remains",
    ),
    test(
        37,
        "resume-idempotency",
        "Resume idempotency",
        AR,
        "semantic turns and effects",
        "duplicate transport requests yield one turn and effect",
    ),
    test(
        38,
        "resource-bounds",
        "Resource-bound honoring",
        AR,
        "observed concurrency and deadlines",
        "limits are never exceeded and excess is typed",
    ),
    test(
        39,
        "hooks",
        "Hook firing",
        AR,
        "committed hook events",
        "acceptance and completion hooks each fire once",
    ),
    test(
        40,
        "durable-journal",
        "Durable journal",
        AR,
        "recovered order and tail integrity",
        "no committed loss or duplicate and no torn corrupt tail",
    ),
    test(
        41,
        "profile-network-isolation",
        "Profile and network isolation",
        AR,
        "outside access and secret scans",
        "all roles use isolated roots and only fake endpoint egress",
    ),
    test(
        42,
        "model-request-efficiency",
        "Model request efficiency",
        RS,
        "physical model requests per completed semantic turn",
        "all requests classified; primary <=1/turn; side channels <=0.05/turn; no retries; bounded context tax and growth",
    ),
    test(
        43,
        "turn-latency-distribution",
        "Turn latency distribution",
        RS,
        "external turn latency distribution",
        "all external boundaries present; max below turn timeout; p95 <=1000ms and jitter <=0.25",
    ),
    test(
        44,
        "process-hygiene",
        "Process hygiene",
        RS,
        "observed process, thread, and FD churn plus post-exit residue",
        "no topology-specific residue and no sustained increase in live process, thread, or FD counts",
    ),
    test(
        45,
        "time-to-first-model-request",
        "Time to first model request",
        RS,
        "cold launch to first completed model request body",
        "all cold launch/request pairs present; p95 <=2000ms; max <=10000ms and below turn timeout",
    ),
    test(
        46,
        "memory-time-integral",
        "Memory time integral",
        RS,
        "trapezoidal effective-memory integral and N=1 CPU per turn",
        "coverage >=0.99; every turn bracketed; max sample gap <=2x cadence; median <=1024 MiB*s/turn; CPU p95 <=250ms",
    ),
    test(
        47,
        "disk-io-per-turn",
        "Disk I/O per turn",
        RS,
        "owned-tree bytes written plus journal and declared-log growth",
        "complete live/retired counters; disk p95 <=64 MiB/turn; bounded journal/log medians and growth slope",
    ),
    test(
        48,
        "model-wait-cpu",
        "Model wait CPU",
        RS,
        "owned-tree CPU while a paced provider response is pending",
        "all paced frames arrive once; exactly one success; owned-tree CPU <=5% of one core",
    ),
    test(
        49,
        "latency-vs-turn-index",
        "Latency versus turn index",
        RS,
        "Theil-Sen wall-latency slope and first/last-decile medians",
        "every growing-session turn terminalizes; slope, decile ratio, and last-decile latency remain bounded",
    ),
    test(
        50,
        "session-residue-sweep",
        "Session residue sweep",
        RS,
        "process and identity-safe declared-store residue after repeated public close-delete",
        "all sessions are really deleted and process, memory, FD, thread, and store slopes remain bounded",
    ),
    test(
        51,
        "context-limit-recovery",
        "Context limit recovery",
        AR,
        "fake-provider request stream across an exact context-length fault and compacted retry",
        "one bounded deterministic recovery preserves session identity, instructions, markers, effects, and every tool pair",
    ),
    test(
        52,
        "resume-latency-vs-length",
        "Resume latency versus session length",
        AR,
        "external resume-to-first-request latency over persisted session lengths",
        "session identity and cursor are exact while p95, Theil-Sen slope, and long/short ratio remain bounded",
    ),
    test(
        53,
        "journal-torn-tail-sweep",
        "Journal torn-tail sweep",
        AR,
        "repeated kill-after-growth journal copies truncated at exact record-relative cuts",
        "every committed prefix recovers cleanly without corruption, loss, fabrication, or duplicate effects",
    ),
    test(
        54,
        "fanout-cliff",
        "Fanout cliff",
        RS,
        "median-aggregated RSS and wall scaling across the required fanout widths",
        "all widths terminalize without an RSS/wall cliff, super-linear global RSS, or excessive N8 peak",
    ),
    test(
        55,
        "fairness-under-fanout",
        "Fairness under fanout",
        RS,
        "barrier-release-to-terminal latency distribution for every scheduled actor",
        "no starvation and bounded population CV, spread, and non-null max/min ratio",
    ),
    test(
        56,
        "child-failure-propagation",
        "Child failure propagation",
        FN,
        "parent terminal latency after a native child crash or hang",
        "parent fails within the declared bound, cancels the child, and leaves no owned residue",
    ),
    test(
        57,
        "signal-matrix",
        "Signal matrix",
        AR,
        "structured terminal and owned-tree cleanup for signals and stdin EOF",
        "each applicable operation emits one clean terminal within grace and leaves zero residue",
    ),
    test(
        58,
        "retry-budget",
        "Retry budget",
        TC,
        "physical attempts, backoff intervals, terminal count, and committed effects",
        "sustained 429/500 exhausts the documented bounded jittered policy with one failure terminal",
    ),
    test(
        59,
        "slow-stream-vs-stall",
        "Slow stream versus stall",
        TC,
        "reset-on-byte idle handling and true-stall terminalization",
        "paced bytes prevent idle timeout while zero-byte stall self-aborts within the declared idle bound",
    ),
    test(
        60,
        "large-tool-output",
        "Large tool output",
        TC,
        "bounded correlated capture of a deterministic ten-MiB tool result",
        "capture is bounded with an exact truncation marker, digest, correlation, and memory envelope",
    ),
    test(
        61,
        "workspace-fault",
        "Workspace fault",
        TC,
        "ordinary fixture write failure in an externally verified read-only workspace",
        "one structured errno-bearing failure, no escape or crash, and no process residue",
    ),
    test(
        62,
        "offline-mode",
        "Offline mode",
        AR,
        "successful provider-only run under a reviewed same-confinement egress guard",
        "provider remains reachable, every other connection is blocked, and the independent control probe is denied",
    ),
    test(
        63,
        "nondeterministic-field-report",
        "Nondeterministic field report",
        FN,
        "cross-execution canonical request leaf stability",
        "score >=0.99 and no varying critical request field",
    ),
    test(
        64,
        "cross-run-reproducibility",
        "Cross-run reproducibility",
        FN,
        "normalized canonical request stream reproducibility",
        "equal semantic request count and attempt multiplicity with byte-identical normalized streams",
    ),
    test(
        65,
        "injection-surface",
        "Injection surface",
        AR,
        "verified provider, base-URL, and credential carrier scores",
        "all three isolated trap trials verify with nonzero component scores and aggregate score >=0.50",
    ),
    test(
        66,
        "budget-enforcement",
        "Budget enforcement",
        AR,
        "token, cost, and time stop boundaries with post-boundary overruns",
        "every budget case stops at its exact boundary with one typed budget terminal and zero overruns",
    ),
    test(
        67,
        "usage-reporting",
        "Usage reporting",
        AR,
        "machine-readable per-repetition token, cost, and turn totals",
        "all five declared usage fields exactly match every fake-provider repetition with zero crosscheck errors",
    ),
    test(
        68,
        "session-ops-cli",
        "Session operations CLI",
        AR,
        "create, list, resume, fork, and delete CLI lifecycle operations",
        "every operation preserves a committed nonempty seed, identity, prefix, divergence, and delete semantics",
    ),
    test(
        69,
        "event-stream-completeness",
        "Event stream completeness",
        AR,
        "tool IDs, result correlation, timestamps, usage, terminal typing, and schema version",
        "at least four of six components pass including tool-call ID, correlated result, and terminal typing",
    ),
    test(
        70,
        "headless-permission-model",
        "Headless permission model",
        AR,
        "permission granularity, prompt count, and allowed or denied effects",
        "score >=0.50 with every allowed and denied trial effective, zero TTY prompts, and zero scope violations",
    ),
    test(
        71,
        "secrets-hygiene-on-disk",
        "Secrets hygiene on disk",
        AR,
        "credential matches across captured streams and profile artifacts",
        "nonempty exact carrier declarations and zero credential occurrences outside allowed private carrier files or argv",
    ),
    test(
        72,
        "tool-result-role-fidelity",
        "Tool-result role fidelity",
        TC,
        "protocol-native successful and failed tool-result roles in subsequent requests",
        "exactly two checks per repetition with one matching typed result and zero violations",
    ),
];

const fn test(
    row: u8,
    id: &'static str,
    name: &'static str,
    pillar: Pillar,
    metric: &'static str,
    pass_criteria: &'static str,
) -> TestDefinition {
    TestDefinition {
        row,
        id,
        name,
        pillar,
        metric,
        pass_criteria,
    }
}

#[cfg(test)]
mod wave4_tests {
    use super::*;

    #[test]
    fn wave4_rows_have_exact_ids_and_roles() {
        let expected = [
            (65, "injection-surface", RequirementKind::Informational),
            (
                66,
                "budget-enforcement",
                RequirementKind::OptionalFacet {
                    capability: "budget_enforcement",
                },
            ),
            (
                67,
                "usage-reporting",
                RequirementKind::OptionalFacet {
                    capability: "usage_reporting",
                },
            ),
            (
                68,
                "session-ops-cli",
                RequirementKind::OptionalFacet {
                    capability: "session_ops_cli",
                },
            ),
            (
                69,
                "event-stream-completeness",
                RequirementKind::Informational,
            ),
            (
                70,
                "headless-permission-model",
                RequirementKind::OptionalFacet {
                    capability: "headless_permission_model",
                },
            ),
            (71, "secrets-hygiene-on-disk", RequirementKind::Core),
            (72, "tool-result-role-fidelity", RequirementKind::Core),
        ];
        for (row, id, requirement) in expected {
            let definition = all()
                .iter()
                .find(|definition| definition.row == row)
                .expect("Wave-4 definition");
            assert_eq!(definition.id, id);
            assert_eq!(definition.requirement(), requirement);
        }
    }

    #[test]
    fn wave4_badge_facets_follow_all_v1_facets_in_normative_order() {
        let suffix = BADGE_FACETS
            .iter()
            .filter(|facet| facet.row >= 65)
            .map(|facet| (facet.row, facet.label, facet.order))
            .collect::<Vec<_>>();
        assert_eq!(
            suffix,
            vec![
                (66, "budgets", 80),
                (67, "usage", 90),
                (68, "session-cli", 100),
                (70, "permissions", 110),
            ]
        );
    }
}
