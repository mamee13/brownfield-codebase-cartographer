"""Tests for the Archivist agent."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any


from src.agents.archivist import Archivist
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import (
    AnswerWithCitation,
    Citation,
    DatasetNode,
    Edge,
    EdgeType,
    ModuleNode,
    StorageType,
    TraceEntry,
)


def _build_kg_with_modules(n: int = 3) -> KnowledgeGraph:
    """Helper: KG with n module nodes, some with purpose statements."""
    kg = KnowledgeGraph()
    for i in range(n):
        mod = ModuleNode(
            id=f"module:src/mod{i}.py",
            path=f"src/mod{i}.py",
            language="python",
            purpose_statement=f"Module {i} handles ETL.",
            change_velocity_30d=i * 5,
            domain_cluster="ingestion" if i % 2 == 0 else "serving",
        )
        kg.add_node(mod)

    # Add a dataset node as source (no in-edges)
    ds = DatasetNode(
        id="dataset:raw.orders",
        name="raw.orders",
        storage_type=StorageType.TABLE,
    )
    kg.add_node(ds)

    return kg


# ── CODEBASE.md ───────────────────────────────────────────────────────────────


def test_codebase_md_has_all_six_sections(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_codebase_md(kg, tmp_path / "CODEBASE.md")

    required = [
        "## Architecture Overview",
        "## Critical Path",
        "## Data Sources & Sinks",
        "## Domain Map",
        "## Known Debt",
        "## High-Velocity Files",
    ]
    for section in required:
        assert section in content, f"Missing section: {section}"


def test_codebase_md_critical_path_at_most_five(tmp_path: Any) -> None:
    kg = _build_kg_with_modules(n=2)
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_codebase_md(kg, tmp_path / "CODEBASE.md")
    cp_section = content.split("## Critical Path")[1].split("##")[0]
    entries = [line for line in cp_section.splitlines() if line.strip().startswith("-")]
    assert len(entries) <= 5


def test_codebase_md_known_debt_present_with_circular_deps(tmp_path: Any) -> None:
    kg = KnowledgeGraph()
    m1 = ModuleNode(id="module:a.py", path="a.py", language="python")
    m2 = ModuleNode(id="module:b.py", path="b.py", language="python")
    kg.add_node(m1)
    kg.add_node(m2)
    # Circular: a→b, b→a
    kg.add_edge(Edge(source="module:a.py", target="module:b.py", type=EdgeType.IMPORTS))
    kg.add_edge(Edge(source="module:b.py", target="module:a.py", type=EdgeType.IMPORTS))

    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_codebase_md(kg, tmp_path / "CODEBASE.md")
    debt_section = content.split("## Known Debt")[1].split("##")[0]
    assert "Circular dependency" in debt_section


def test_codebase_md_source_citation_on_fact_lines(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_codebase_md(kg, tmp_path / "CODEBASE.md")
    # Every bullet line in Critical Path must contain "(source:"
    cp_section = content.split("## Critical Path")[1].split("##")[0]
    bullet_lines = [
        line for line in cp_section.splitlines() if line.strip().startswith("-")
    ]
    for line in bullet_lines:
        assert "(source:" in line, f"Missing citation in: {line}"


# ── onboarding_brief.md ───────────────────────────────────────────────────────


def test_onboarding_brief_has_all_five_questions(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_onboarding_brief(kg, tmp_path / "onboarding_brief.md")
    for i in range(1, 6):
        assert f"## Q{i}:" in content, f"Missing Q{i} section"


def test_onboarding_brief_header_has_timestamp(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_onboarding_brief(kg, tmp_path / "onboarding_brief.md")
    # ISO timestamp pattern
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", content), (
        "No ISO timestamp found in onboarding_brief header"
    )


def test_onboarding_brief_with_answers_includes_evidence(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    kg.set_day_one_answers(
        {
            "Q1": AnswerWithCitation(
                answer="Ingestion via kafka_consumer.py.",
                citations=[
                    Citation(
                        file="src/kafka.py",
                        line_range="L1-50",
                        method="static_analysis",
                    )
                ],
                confidence="observed",
            )
        }
    )
    arch = Archivist(output_dir=tmp_path)
    content = arch.generate_onboarding_brief(kg, tmp_path / "onboarding_brief.md")
    # Check evidence line format: file:path:LN-M
    assert re.search(r"file:src/kafka\.py:L1-50", content), (
        "Expected evidence citation line not found"
    )


# ── cartography_trace.jsonl ───────────────────────────────────────────────────


def test_write_trace_log_produces_valid_jsonl(tmp_path: Any) -> None:
    kg = KnowledgeGraph()
    kg.add_trace_entry(
        TraceEntry(
            timestamp=datetime.now(tz=timezone.utc),
            agent="Semanticist",
            action="generate_purpose_statement",
            evidence_source="llm_inference",
            confidence="inferred",
            file="src/etl.py",
        )
    )
    kg.add_trace_entry(
        TraceEntry(
            timestamp=datetime.now(tz=timezone.utc),
            agent="Archivist",
            action="write_codebase_md",
            evidence_source="static_analysis",
            confidence="observed",
        )
    )
    arch = Archivist(output_dir=tmp_path)
    out_path = arch.write_trace_log(kg, tmp_path / "trace.jsonl")

    lines = out_path.read_text().splitlines()
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)  # must not raise
        assert "agent" in parsed
        assert "action" in parsed


def test_trace_log_lines_in_order(tmp_path: Any) -> None:
    kg = KnowledgeGraph()
    for i in range(3):
        kg.add_trace_entry(
            TraceEntry(
                timestamp=datetime.now(tz=timezone.utc),
                agent=f"Agent{i}",
                action=f"action{i}",
                evidence_source="static_analysis",
                confidence="observed",
            )
        )
    arch = Archivist(output_dir=tmp_path)
    out_path = arch.write_trace_log(kg, tmp_path / "trace.jsonl")
    lines = out_path.read_text().splitlines()
    agents = [json.loads(line)["agent"] for line in lines]
    assert agents == ["Agent0", "Agent1", "Agent2"]


# ── Full Archivist.run() integration ─────────────────────────────────────────


def test_archivist_run_produces_all_three_artifacts(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    arch.run(kg)

    assert (tmp_path / "CODEBASE.md").exists()
    assert (tmp_path / "onboarding_brief.md").exists()
    assert (tmp_path / "cartography_trace.jsonl").exists()


def test_archivist_run_logs_own_actions_to_trace(tmp_path: Any) -> None:
    kg = _build_kg_with_modules()
    arch = Archivist(output_dir=tmp_path)
    arch.run(kg)
    # Archivist must have logged ≥3 entries (one per artifact write)
    archivist_entries = [e for e in kg.trace_entries if e.agent == "Archivist"]
    assert len(archivist_entries) >= 3
