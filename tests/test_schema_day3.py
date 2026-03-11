"""Tests for Day 3 schema additions: Citation, AnswerWithCitation, TraceEntry."""

import pytest
from datetime import datetime, timezone

from pydantic import ValidationError

from src.models.schema import (
    AnswerWithCitation,
    Citation,
    GraphSchema,
    ModuleNode,
    TraceEntry,
)


# ── Citation ──────────────────────────────────────────────────────────────────


def test_citation_valid() -> None:
    c = Citation(file="src/foo.py", line_range="L10-20", method="static_analysis")
    assert c.file == "src/foo.py"
    assert c.method == "static_analysis"


def test_citation_rejects_invalid_method() -> None:
    with pytest.raises(ValidationError):
        Citation(file="src/foo.py", line_range="L1-1", method="guesswork")  # type: ignore[arg-type]


def test_citation_missing_method_raises() -> None:
    with pytest.raises(ValidationError):
        Citation(file="src/foo.py", line_range="L1-1")  # type: ignore[call-arg]


# ── AnswerWithCitation ────────────────────────────────────────────────────────


def test_answer_with_citation_valid() -> None:
    a = AnswerWithCitation(
        answer="The ingestion path starts at kafka_consumer.py.",
        citations=[
            Citation(
                file="src/kafka_consumer.py",
                line_range="L1-50",
                method="static_analysis",
            )
        ],
        confidence="observed",
    )
    assert len(a.citations) == 1
    assert a.confidence == "observed"


def test_answer_with_empty_citations_raises() -> None:
    """AnswerWithCitation with citations=[] must raise ValidationError."""
    with pytest.raises(ValidationError):
        AnswerWithCitation(
            answer="Some answer.",
            citations=[],
            confidence="inferred",
        )


def test_answer_round_trips_json() -> None:
    a = AnswerWithCitation(
        answer="Business logic is in src/transforms/.",
        citations=[
            Citation(
                file="src/transforms/revenue.py",
                line_range="L42-55",
                method="llm_inference",
            )
        ],
        confidence="inferred",
    )
    restored = AnswerWithCitation.model_validate_json(a.model_dump_json())
    assert restored.answer == a.answer
    assert restored.citations[0].file == "src/transforms/revenue.py"


# ── TraceEntry ────────────────────────────────────────────────────────────────


def test_trace_entry_valid() -> None:
    e = TraceEntry(
        timestamp=datetime.now(tz=timezone.utc),
        agent="Semanticist",
        action="generate_purpose_statement",
        evidence_source="llm_inference",
        confidence="inferred",
        file="src/etl.py",
        detail="model=gemini-flash",
    )
    assert e.agent == "Semanticist"
    assert e.evidence_source == "llm_inference"


def test_trace_entry_round_trips() -> None:
    e = TraceEntry(
        timestamp=datetime.now(tz=timezone.utc),
        agent="Archivist",
        action="write_codebase_md",
        evidence_source="static_analysis",
        confidence="observed",
    )
    restored = TraceEntry.model_validate_json(e.model_dump_json())
    assert restored.agent == "Archivist"


def test_trace_entry_rejects_invalid_evidence_source() -> None:
    with pytest.raises(ValidationError):
        TraceEntry(
            timestamp=datetime.now(tz=timezone.utc),
            agent="X",
            action="y",
            evidence_source="telepathy",  # type: ignore[arg-type]
            confidence="observed",
        )


# ── ModuleNode Day 3 additions ────────────────────────────────────────────────


def test_module_node_doc_drift_defaults_false() -> None:
    m = ModuleNode(id="module:src/foo.py", path="src/foo.py", language="python")
    assert m.doc_drift is False


def test_module_node_symbol_line_map_defaults_empty() -> None:
    m = ModuleNode(id="module:src/foo.py", path="src/foo.py", language="python")
    assert m.symbol_line_map == {}


def test_module_node_domain_cluster_is_present() -> None:
    """Confirms domain_cluster field exists and was not removed."""
    m = ModuleNode(
        id="module:src/foo.py",
        path="src/foo.py",
        language="python",
        domain_cluster="ingestion",
    )
    assert m.domain_cluster == "ingestion"


# ── GraphSchema Day 3 additions ───────────────────────────────────────────────


def test_graph_schema_has_day_one_answers_field() -> None:
    gs = GraphSchema(nodes={}, edges=[])
    assert gs.day_one_answers == {}


def test_graph_schema_has_trace_entries_field() -> None:
    gs = GraphSchema(nodes={}, edges=[])
    assert gs.trace_entries == []
