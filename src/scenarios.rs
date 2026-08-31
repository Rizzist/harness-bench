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
        if matches!(self.row, 42 | 43 | 45 | 46 | 63) {
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

const OPTIONAL_FACETS: [OptionalFacet; 6] = [
    OptionalFacet {
        row: 4,
        capability: "parallel_tool_execution",
    },
    OptionalFacet {
        row: 18,
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

const TESTS: [TestDefinition; 48] = [
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
