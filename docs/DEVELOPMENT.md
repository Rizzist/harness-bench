# Development lanes

## Workflow and model routing

Fable 5.1 orchestrates the current development run. Each lane has a scope, acceptance criteria, dependencies, a branch/worktree, a checklist and an evidence record.

1. **Clean code pass.** GPT6-Astra reads the relevant specification, establishes the baseline and cleans the code in scope, including stale helpers/imports and platform gates. Preserve unrelated work.
2. **Implement.** GPT6-Astra handles non-UI work and 3D modelling. Fable 5.1 or Opus 5 handles UI work. Split mixed changes by responsibility.
3. **Verify and repair.** GPT6-Astra performs code verification and computer-use verification. Review the actual diff; run relevant compilation, lint, tests and platform checks; exercise the affected behavior in a real terminal, application or browser and inspect produced artifacts. Return defects to the appropriate implementation model and repeat on the new candidate until GPT6-Astra issues **SHIP**. A build alone is not behavioral verification. Use a separate verification worker/context from implementation.
4. **Complete.** Only after both verification modes pass, land a focused lane commit and push it to the authorized GitHub remote. Record the pushed commit and verification evidence, mark the lane complete and proceed. If integration changes the verified tree, verify the new candidate before landing. Never force-push or overwrite unrelated work.

Record actual model/runtime identity and worker session IDs. A model name in a prompt or checklist does not establish which provider ran the task. If required models, credentials, tools or quota are unavailable, leave a precise blocker and resume point; never silently substitute or fabricate SHIP.

## Candidate and evidence

For each lane, record its stage, acceptance criteria, implementation/verifier identities, worktree, base/candidate commit, source tree or working-file digest, checks and exit codes, behavioral observations, artifact hashes when relevant, findings and final verdict. Bind SHIP to the exact source candidate; subsequent source changes require verification again. Keep mutable status and raw traces outside the source candidate to avoid self-referential evidence hashes. The published lane ledger can reference the verified source tree and final landing commit.

Use `docs/LANES.md` for the human-readable checklist and concise lane summaries. Keep private worker logs and bulky benchmark profiles in an external evidence directory. Publish only reviewed, sanitized aggregate benchmark results and durable specifications/documentation.

## AHRB checks

Start with `cargo build --locked` and `target/debug/ahrb list-tests`. Follow existing project test conventions and the relevant specification. Run affected checks plus necessary regression coverage; do not run concurrent memory-heavy matrix suites on a 16 GiB machine. A baseline failure must remain visible.

Before six-harness reference measurements, record binary versions and hashes, run each adapter's doctor, and self-certify both mock transports. Use only hermetic fake-model profiles; never copy the developer's real provider credentials into benchmark roots. Pause development workloads during reference measurements. Reference outputs need absolute, fresh timestamped paths and serial execution. Never remove a temporary directory owned by a live run or delete earlier evidence to make room silently.

The historical handoff's mock-cert script prints failure information without reliably failing its exit status; the six-harness script also contains broad `/tmp/ahrb-*` cleanup and reuses output paths. Address these in the first lane before trusting those scripts for unattended certification/reference runs. Add focused regression coverage for failure propagation and ownership-scoped cleanup.

## Storage scope

The normative storage contract is [SPEC-v4-storage.md](SPEC-v4-storage.md).

Start from `docs/PROPOSAL-v4-storage.md`: write the normative storage spec first, then implement W1 (S1, S3, S5, S7, S8) and W2 (S2, S6, S9, S10), each in reviewable lanes. Include S4 compaction-versus-disk explicitly; the proposal's wave list omits it, but its ten-row scope includes it. Document thresholds, units, accounting and unsupported conditions before implementing claims.

Keep adapters declarative and measurements harness-neutral. Preserve the proposal's honesty rails: separate physical writes from allocated footprint; distinguish measurement from inference; report unsupported OS instrumentation honestly; never infer request-body retention solely from total disk growth; never treat estimated durability cost as measured latency. A new row needs positive and negative mock evidence plus actual CLI/report verification. Six-harness tables are measurements, not expected values.

Independent lane worktrees may run in parallel. Serialize heavy builds, benchmarks and VM/emulator use. Reconcile upstream changes before each landing; historical references to another machine's active pass are not a mandate to duplicate that work.
