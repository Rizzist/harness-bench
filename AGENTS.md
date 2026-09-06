# AHRB development

Read `docs/DEVELOPMENT.md`, the lane checklist in `docs/LANES.md` when present, and the relevant specification before editing. The Mac mini's build/reference handoff is `docs/HANDOFF-macmini.md`; planned storage work is `docs/PROPOSAL-v4-storage.md`.

Every lane follows **clean code pass -> implement -> verify/repair until SHIP -> complete**. Non-UI cleanup/implementation and 3D use GPT6-Astra. UI implementation uses Fable 5.1 or Opus 5. Code verification, computer-use verification and SHIP are **GPT6-Astra only**. Never silently substitute a model or infer a pass from a missing test.

Keep secrets, signing material, real account profiles, raw benchmark results and private agent transcripts outside Git. Preserve unrelated work and use isolated worktrees for independent lanes.
