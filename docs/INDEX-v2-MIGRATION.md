# Results index v2 migration

AHRB keeps the existing append-only results/index.jsonl filename. New runs append
schema 2 lines containing only the 14 fields defined by SPEC-v2 G1. The
completed_at value is captured after the report bundle has been written, rather
than reusing the run-start timestamp.

Readers accept the historical unversioned line shape as implicit schema 1.
They synthesize a stable legacy run key from the original line number and bytes,
and hydrate report schema, specification, OS, topology, profile, and hashes from
the referenced report when it is still available. Existing lines are never
rewritten. New and legacy lines may therefore coexist in one index and resolve
through the same hbench diff commands.

Resource deltas are emitted only when OS, topology, and quick/cert profile all
match. Explicit comparisons across any of those boundaries retain the row-state
diff but label every resource field not-comparable.
