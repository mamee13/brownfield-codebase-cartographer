# Brownfield Cartographer: Personal Operator Plan

## Purpose
Use this as your daily control sheet while the AI IDE executes `plan.md`.

## Your Responsibilities (Human-in-the-Loop)
- Lock exact target repos and commit hashes for reproducibility.
- Provide API keys/model access needed for Semanticist tasks.
- Run and verify the critical test commands at the end of each day.
- Review evidence quality in generated outputs (citations must be real and navigable).
- Validate demo flow and timing.

## Daily Checklist

### Day 1 (March 9)
- Confirm repo scaffolding and dependency lock are committed.
- Confirm `module_graph.json` is generated.
- Run:
  - `uv run pytest -q`
  - `uv run ruff check .`
  - `uv run mypy src`
  - `uv run python -m src.cli --help`

### Day 2 (March 10)
- Confirm lineage graph is generated and non-empty.
- Spot-check 10 lineage edges against source files.
- Run:
  - `uv run pytest -q tests/analyzers tests/agents`
  - `uv run python -m src.cli analyze --repo <target_repo>`

### Day 3 (March 11)
- Verify `CODEBASE.md` and `onboarding_brief.md` include citation format (`file:line`, method).
- Check at least 2 doc drift flags manually.
- Run:
  - `uv run pytest -q tests/semantic tests/archivist`

### Day 4 (March 12)
- Verify `query` mode returns evidence-backed responses for all 4 required tools.
- Validate incremental mode speedup on second run.
- Run:
  - `uv run pytest -q`
  - `uv run python -m src.cli query`
  - `uv run python -m src.cli analyze --repo <target_repo> --incremental`

## Quality Gate (Must be true before submission)
- 2+ target codebases have complete required artifacts.
- All 5 Day-One answers are specific and cited.
- Required demo steps run without manual patching.
- No critical failing tests.

## Demo Rehearsal Script (6 min max)
- Minute 1-3:
  - Cold start analyze on unfamiliar codebase.
  - Lineage query with citations.
  - Blast radius query.
- Minute 4-6:
  - Day-One brief + verify 2 citations live.
  - Context injection comparison.
  - Self-audit discrepancy on Week 1 repo.

## Nice-to-Have Queue (Only if core is complete)
- Column-level lineage.
- Graph UI.
- Architecture diff mode.
- Confidence score per answer.
