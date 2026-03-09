# Brownfield Cartographer: AI IDE Execution Plan (4 Days)

## Objective
Deliver a rubric-level-5 Brownfield Cartographer in 4 days with test-backed outputs, evidence citations, and demo-ready flows.

## Dates
- Current date: March 9, 2026
- Build window: March 9-12, 2026
- Final deadline from brief: March 15, 2026, 03:00 UTC

## Hard Acceptance Gates
- Multi-language static analysis (Python + SQL + YAML minimum).
- Mixed lineage graph from Python + SQL (sqlglot) + config signals.
- `CODEBASE.md` and `onboarding_brief.md` include `file:line` + method citations.
- Navigator query mode supports 4 required tools.
- Incremental mode re-analyzes only changed files.
- Full required artifacts generated for 2+ codebases.
- Parser failures degrade gracefully (log + skip, no full crash).

## Locked Targets
- `dbt-labs/jaffle_shop`
- Apache Airflow `examples/`
- Week 1 repo (self-audit target)

---

## Day 1 (March 9): Foundation + Models + Surveyor Core
Branch: `feat/foundation-graph-contracts`

### Step 1. Bootstrap project structure
Tasks:
- Initialize `src/` package structure.
- Create pinned `pyproject.toml` and lock dependencies with `uv`.
- Configure lint/type/test tooling.
- Define `.cartography/` artifact layout and version fields.
- Add pre-commit framework with hooks:
  - `ruff check` (lint)
  - `ruff format --check` (format enforcement)
  - `mypy` (type-check)
  - `pytest` (tests must pass before commit)
- Add local helper target/script to run the same gate manually before each commit.

Tests:
- `uv run pytest -q`
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy src`
- `uv run python -m src.cli --help`

Critical checks:
- Fail if dependency versions are not pinned.
- Fail if CLI fails before analysis starts.
- Fail if any pre-commit hook can be bypassed without explicit override.
- Fail if a commit is allowed when lint/type/tests are red.

### Step 2. Implement graph schemas and core wrapper
Tasks:
- Create Pydantic schemas for all node/edge/graph types.
- Implement `src/graph/knowledge_graph.py`.
- Add serialization/deserialization and schema guards.

Tests:
- Unit: schema validation for each node type.
- Unit: graph JSON round-trip.
- Negative: invalid enums, missing required fields.

Critical checks:
- Fail if invalid edge-node combinations pass validation.

### Step 3. Build Surveyor MVP
Tasks:
- Implement `tree_sitter_analyzer.py` and language router.
- Parse imports, public functions/classes, inheritance.
- Implement git velocity extraction.
- Build import graph, run PageRank + SCC.
- Write `.cartography/module_graph.json`.

Tests:
- Unit: parser extraction fixtures.
- Unit: relative import resolution.
- Unit: git velocity parsing.
- Integration: mixed-language fixture repo.
- Contract: module graph output schema validation.

Critical checks:
- Fail if parser exceptions crash full run.
- Fail if empty graph edge-cases break ranking/SCC.

---

## Day 2 (March 10): Hydrologist and Lineage Quality
Branch: `feat/lineage-extraction-engine`

### Step 4. SQL lineage analyzer
Tasks:
- Implement `src/analyzers/sql_lineage.py` with sqlglot.
- Extract dependencies from SELECT/FROM/JOIN/WITH.
- Support PostgreSQL, BigQuery, Snowflake, DuckDB.

Tests:
- Unit: per-dialect fixtures.
- Unit: CTE dependency resolution.
- Unit: parse failure fallback behavior.

Critical checks:
- Fail if alias names are mistaken for physical tables.
- Fail if unresolved SQL crashes run.

### Step 5. Python data-flow analyzer
Tasks:
- Detect pandas read/write, SQLAlchemy execute, PySpark read/write.
- Resolve static refs; log dynamic refs as unresolved.

Tests:
- Unit: extraction for each supported API pattern.
- Unit: dynamic ref handling (`f-string`, variable interpolation).
- Negative: avoid same-name false positives.

Critical checks:
- Fail if unresolved dynamic refs are silently dropped.

### Step 6. DAG/config analyzer + merge
Tasks:
- Implement `dag_config_parser.py` for Airflow/dbt topology extraction.
- Merge SQL + Python + config outputs into one lineage graph.
- Implement `blast_radius`, `find_sources`, `find_sinks`.
- Write `.cartography/lineage_graph.json`.

Tests:
- Unit: Airflow DAG fixture parsing.
- Unit: dbt YAML/source parsing.
- Unit: graph traversal functions.
- Integration: jaffle_shop lineage sanity check.

Critical checks:
- Fail if dataset/module namespaces collide.
- Fail if edge direction errors break blast radius.

---

## Day 3 (March 11): Semanticist + Archivist
Branch: `feat/semantic-archival-intelligence`

### Step 7. Semanticist implementation
Tasks:
- Implement `ContextWindowBudget` and model routing.
- Generate code-grounded purpose statements.
- Detect doc drift.
- Embed + cluster domains (k=5-8).
- Generate Day-One answers with evidence citations.

Tests:
- Unit: budget enforcement and tier routing.
- Unit: doc drift detection fixture.
- Unit: deterministic clustering (fixed seed).
- Integration: citation presence in Day-One outputs.

Critical checks:
- Fail if prompts rely on docstrings as primary evidence.
- Fail if uncited claims appear in outputs.

### Step 8. Archivist outputs
Tasks:
- Implement `archivist.py` to generate:
  - `.cartography/CODEBASE.md`
  - `.cartography/onboarding_brief.md`
  - `.cartography/cartography_trace.jsonl`
- Enforce confidence + method tagging where applicable.

Tests:
- Unit: required sections in `CODEBASE.md`.
- Unit: trace JSONL schema validation.
- Integration: full artifact generation after pipeline run.

Critical checks:
- Fail if Known Debt omits circular deps or doc drift.

---

## Day 4 (March 12): Navigator + Incremental + Hardening
Branch: `feat/navigator-incremental-runtime`

### Step 9. Orchestrator + CLI completion
Tasks:
- Implement full pipeline orchestration.
- Add `analyze` and `query` subcommands.
- Support local path and GitHub URL input handling.

Tests:
- Integration: `analyze` on fixture and real targets.
- Integration: `query` mode smoke tests.
- Error-path: invalid repo, missing git metadata, empty repo.

Critical checks:
- Fail if command exits success with missing mandatory artifacts.

### Step 10. Navigator tools
Tasks:
- Implement:
  - `find_implementation(concept)`
  - `trace_lineage(dataset, direction)`
  - `blast_radius(module_path)`
  - `explain_module(path)`
- Ensure each tool returns evidence with method labels.

Tests:
- Unit: tool output contracts.
- Integration: jaffle_shop + airflow queries.
- Regression: all answers include citation + method.

Critical checks:
- Fail if any tool returns uncited natural-language answers.

### Step 11. Incremental mode and resilience
Tasks:
- Re-analyze changed files only using git diff.
- Add file hash/mtime cache.
- Harden parser failures to warning + skip.

Tests:
- Integration: full vs incremental run comparison.
- Performance: incremental runtime improvement.
- Fault-injection: malformed files do not crash pipeline.

Critical checks:
- Fail if incremental updates produce stale graph links.

### Step 12. Final package readiness
Tasks:
- Complete README usage for analyze/query.
- Generate artifacts for 2+ targets.
- Finalize report inputs and demo script sequence.

Tests:
- Required-file checklist validation.
- Demo dry-run timing <= 6 minutes.
- Repro test from clean checkout + lockfile.

Critical checks:
- Fail if any demo claim lacks verifiable artifact evidence.

---

## Global Test Matrix
- Unit: schemas, analyzers, graph algorithms, budgeting, tool contracts.
- Integration: pipeline on fixtures and real repos.
- E2E: `cli analyze` then `cli query` required scenarios.
- Reliability: malformed SQL/YAML/Python and empty inputs.
- Performance: baseline full run and incremental speedup.
- Regression: output schema + citation format stability.

## Risk Controls
- Tree-sitter setup risk: isolate parser init and fallback per language.
- SQL edge-case risk: dialect fixtures and unresolved-node warnings.
- LLM cost risk: strict budget + cheaper bulk model + caching.
- Citation risk: schema-level enforcement for evidence fields.
- Trust risk: mark outputs as observed vs inferred.

## Optional Nice-to-Haves (only after hard gates pass)
- Column-level lineage.
- Interactive graph UI.
- Architecture diff across commits.
- Confidence scoring combining static + LLM evidence.
- Plugin adapters for Dagster/Prefect/Luigi.
- Impact-notification command (`if X changes, who is affected`).

## Execution Rule
- No optional work before all hard gates are green.
- Each implementation step must add or update tests in the same change.
- Every LLM claim must be cited or marked as inference.
- Work only on the branch assigned to that day section.
- Merge to `main` only via PR with green checks.
- Rebase branch before merge to keep history clean.
