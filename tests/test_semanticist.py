"""Tests for the Semanticist agent — all via FakeLLMClient, zero real API calls."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List


from src.agents.semanticist import (
    EMBED_DIM,
    EMBED_MODEL,
    ContextWindowBudget,
    FakeLLMClient,
    LLMResponse,
    Semanticist,
    TraceLogger,
    build_symbol_line_map,
    estimate_tokens,
    extract_module_docstring,
    route_model,
    truncate_source,
    MAX_SOURCE_BYTES,
    MODEL_BULK,
    MODEL_SYNTHESIS,
)
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import (
    AnswerWithCitation,
    Citation,
    ModuleNode,
    WarningRecord,
)


# ── FakeLLMClient ─────────────────────────────────────────────────────────────


def test_fake_client_returns_canned_response() -> None:
    client = FakeLLMClient(responses=["Hello from fake."])
    resp = client.complete("some prompt", model="gemini/fake")
    assert resp.text == "Hello from fake."
    assert resp.tokens_in > 0
    assert resp.tokens_out > 0


def test_fake_client_records_calls() -> None:
    client = FakeLLMClient(responses=["A", "B"])
    client.complete("p1", model="m")
    client.complete("p2", model="m")
    assert len(client.calls) == 2


def test_fake_client_embed_returns_correct_dim() -> None:
    client = FakeLLMClient()
    vecs = client.embed(["foo", "bar"])
    assert len(vecs) == 2
    assert len(vecs[0]) == EMBED_DIM


def test_fake_client_embed_is_deterministic() -> None:
    client = FakeLLMClient()
    v1 = client.embed(["determinism test"])
    v2 = client.embed(["determinism test"])
    assert v1 == v2


# ── estimate_tokens ───────────────────────────────────────────────────────────


def test_estimate_tokens_nonzero_for_nonempty() -> None:
    assert estimate_tokens("hello world") > 0


def test_estimate_tokens_gemini_uses_char4() -> None:
    text = "a" * 400
    result = estimate_tokens(text, model="google/gemini-2.0-flash")
    assert result == 100  # 400 // 4


def test_estimate_tokens_zero_input_returns_zero() -> None:
    assert estimate_tokens("") == 0


# ── ContextWindowBudget ───────────────────────────────────────────────────────


def test_budget_accumulates_spend() -> None:
    budget = ContextWindowBudget(max_tokens=1000)
    r = LLMResponse(text="hi", tokens_in=100, tokens_out=50, model="m")
    budget.charge(r)
    assert budget.used == 150


def test_budget_cap_sets_exhausted() -> None:
    budget = ContextWindowBudget(max_tokens=100)
    r = LLMResponse(text="x", tokens_in=60, tokens_out=60, model="m")
    budget.charge(r)
    assert budget.exhausted is True


def test_budget_check_returns_false_when_exhausted() -> None:
    budget = ContextWindowBudget(max_tokens=1)
    r = LLMResponse(text="x", tokens_in=1, tokens_out=1, model="m")
    budget.charge(r)
    assert budget.check() is False


def test_budget_exceeded_emits_warning_not_exception() -> None:
    """Budget exhaustion must emit BUDGET_EXCEEDED warning and NOT crash."""
    client = FakeLLMClient(responses=["Purpose: does something."])
    budget = ContextWindowBudget(max_tokens=1)  # immediately exhausted
    budget.exhausted = True  # force exhaustion

    kg = KnowledgeGraph()
    mod = ModuleNode(id="module:src/tiny.py", path="src/tiny.py", language="python")
    kg.add_node(mod)

    sem = Semanticist(client=client, budget=budget)
    tracer = TraceLogger(kg)
    # Must not raise — returns None and adds warning
    result = sem.generate_purpose_statement(mod, "x = 1", kg, tracer)
    assert result is None
    assert any(w.code == "BUDGET_EXCEEDED" for w in kg.warnings)


# ── route_model ───────────────────────────────────────────────────────────────


def test_route_model_bulk() -> None:
    assert route_model("bulk") == MODEL_BULK


def test_route_model_synthesis() -> None:
    assert route_model("synthesis") == MODEL_SYNTHESIS


def test_route_model_unknown_defaults_bulk() -> None:
    assert route_model("other") == MODEL_BULK


# ── truncate_source ───────────────────────────────────────────────────────────


def test_truncate_source_no_truncation_needed() -> None:
    short = "x = 1\n"
    warnings: List[WarningRecord] = []
    result, was_truncated = truncate_source(short, "f.py", warnings)
    assert result == short
    assert was_truncated is False
    assert warnings == []


def test_truncate_source_large_input_emits_warning() -> None:
    big = "x = 1\n" * 10000  # ~70k bytes
    warnings: List[WarningRecord] = []
    result, was_truncated = truncate_source(big, "big.py", warnings)
    assert was_truncated is True
    assert len(result.encode("utf-8")) <= MAX_SOURCE_BYTES
    assert any(w.code == "CODE_TRUNCATED" for w in warnings)


def test_truncate_source_preserves_whole_lines() -> None:
    big = "abcdefgh\n" * 5000  # clean line boundaries
    warnings: List[WarningRecord] = []
    result, _ = truncate_source(big, "f.py", warnings)
    # Every line must end with \n or be empty (no mid-line cut)
    for line in result.splitlines(keepends=True):
        assert line.endswith("\n"), f"Partial line found: {line!r}"


# ── build_symbol_line_map ─────────────────────────────────────────────────────


def test_symbol_line_map_finds_functions() -> None:
    src = "def foo():\n    pass\ndef bar():\n    pass\n"
    m = build_symbol_line_map(src)
    assert "foo" in m
    assert "bar" in m
    assert m["foo"] == 1
    assert m["bar"] == 3


def test_symbol_line_map_finds_classes() -> None:
    src = "class MyClass:\n    pass\n"
    m = build_symbol_line_map(src)
    assert "MyClass" in m


def test_symbol_line_map_returns_empty_on_syntax_error() -> None:
    m = build_symbol_line_map("def invalid syntax ???")
    assert m == {}


# ── extract_module_docstring ──────────────────────────────────────────────────


def test_extract_docstring_finds_module_docstring() -> None:
    src = '"""This is the module docstring."""\nx = 1\n'
    doc = extract_module_docstring(src)
    assert doc == "This is the module docstring."


def test_extract_docstring_returns_none_when_absent() -> None:
    src = "x = 1\n"
    assert extract_module_docstring(src) is None


# ── Purpose statement generation ──────────────────────────────────────────────


def test_generate_purpose_statement_returns_nonempty(tmp_path: Any) -> None:
    client = FakeLLMClient(
        responses=["This module reads data from S3 and writes to BigQuery."]
    )
    kg = KnowledgeGraph()
    mod = ModuleNode(id="module:src/etl.py", path="src/etl.py", language="python")
    kg.add_node(mod)
    sem = Semanticist(client=client)
    tracer = TraceLogger(kg)
    result = sem.generate_purpose_statement(mod, "import pandas as pd\n", kg, tracer)
    assert result is not None
    assert len(result) > 5


def test_generate_purpose_statement_no_docstring_no_drift_warning() -> None:
    client = FakeLLMClient(responses=["Ingest data.", "MATCH"])
    kg = KnowledgeGraph()
    mod = ModuleNode(id="module:src/etl.py", path="src/etl.py", language="python")
    kg.add_node(mod)
    sem = Semanticist(client=client)
    tracer = TraceLogger(kg)
    sem.generate_purpose_statement(mod, "x = 1\n", kg, tracer)
    assert not any(w.code == "DOC_DRIFT" for w in kg.warnings)


def test_generate_purpose_statement_drift_detected() -> None:
    """When LLM says DRIFT, a DOC_DRIFT warning must be emitted."""
    client = FakeLLMClient(responses=["This does X.", "DRIFT"])
    kg = KnowledgeGraph()
    src = '"""This module does Y."""\nx = 1\n'
    mod = ModuleNode(id="module:src/etl.py", path="src/etl.py", language="python")
    kg.add_node(mod)
    sem = Semanticist(client=client)
    tracer = TraceLogger(kg)
    sem.generate_purpose_statement(mod, src, kg, tracer)
    assert any(w.code == "DOC_DRIFT" for w in kg.warnings)


# ── LLM timeout / failure path ────────────────────────────────────────────────


class _FailingClient:
    """Simulates LLM timeout — always raises."""

    def complete(self, prompt: str, model: str, max_tokens: int = 1024) -> LLMResponse:
        raise TimeoutError("LLM request timed out")

    def embed(self, texts: List[str], model: str = EMBED_MODEL) -> List[List[float]]:
        raise TimeoutError("embed timed out")


def test_llm_timeout_emits_warning_no_crash() -> None:
    """Simulate LLM timeout — pipeline must not crash, LLM_ERROR trace entry logged."""
    client = _FailingClient()
    kg = KnowledgeGraph()
    mod = ModuleNode(id="module:src/etl.py", path="src/etl.py", language="python")
    kg.add_node(mod)

    sem = Semanticist(client=client)
    tracer = TraceLogger(kg)
    result = sem.generate_purpose_statement(mod, "x = 1\n", kg, tracer)
    # Must not raise — returns None
    assert result is None
    # Must log an error trace entry
    assert any(e.action == "llm_error" for e in kg.trace_entries)


# ── Domain clustering ─────────────────────────────────────────────────────────


def test_cluster_into_domains_deterministic() -> None:
    """Same input + same seed must produce identical cluster assignments."""
    client = FakeLLMClient(responses=["ingestion"] * 10)
    kg = KnowledgeGraph()
    tracer = TraceLogger(kg)

    nodes = [
        ModuleNode(
            id=f"module:src/mod{i}.py",
            path=f"src/mod{i}.py",
            language="python",
            purpose_statement=f"Module {i} does data processing.",
        )
        for i in range(10)
    ]
    for n in nodes:
        kg.add_node(n)

    sem = Semanticist(client=client)
    d1 = sem.cluster_into_domains(nodes, tracer)

    # Re-run with same client (same deterministic embeddings)
    client2 = FakeLLMClient(responses=["ingestion"] * 10)
    tracer2 = TraceLogger(kg)
    sem2 = Semanticist(client=client2)
    d2 = sem2.cluster_into_domains(nodes, tracer2)

    # Same cluster assignments (same set of module lists per domain)
    assert sorted(d1.values()) == sorted(d2.values())


def test_cluster_constants_defined() -> None:
    assert EMBED_MODEL and len(EMBED_MODEL) > 0
    assert EMBED_DIM == 1536


# ── TraceLogger ───────────────────────────────────────────────────────────────


def test_trace_logger_appends_entries() -> None:
    kg = KnowledgeGraph()
    tracer = TraceLogger(kg)
    tracer.log("Semanticist", "test_action", "static_analysis", "observed", detail="hi")
    assert len(kg.trace_entries) == 1
    assert kg.trace_entries[0].action == "test_action"


def test_trace_entries_in_chronological_order() -> None:
    kg = KnowledgeGraph()
    tracer = TraceLogger(kg)
    tracer.log("A", "first", "static_analysis", "observed")
    tracer.log("B", "second", "llm_inference", "inferred")
    ts0 = kg.trace_entries[0].timestamp
    ts1 = kg.trace_entries[1].timestamp
    assert ts0 <= ts1


# ── KnowledgeGraph Day 3 extensions ──────────────────────────────────────────


def test_kg_set_day_one_answers_and_round_trip(tmp_path: Any) -> None:
    kg = KnowledgeGraph()
    answers = {
        "Q1": AnswerWithCitation(
            answer="Ingestion via kafka.",
            citations=[
                Citation(
                    file="src/kafka.py", line_range="L1-10", method="static_analysis"
                )
            ],
            confidence="observed",
        )
    }
    kg.set_day_one_answers(answers)
    assert "Q1" in kg.day_one_answers

    # Save and reload
    p = tmp_path / "kg.json"
    kg.save(p)
    kg2 = KnowledgeGraph.load(p)
    assert "Q1" in kg2.day_one_answers
    assert kg2.day_one_answers["Q1"].answer == "Ingestion via kafka."


def test_kg_trace_entries_round_trip(tmp_path: Any) -> None:
    from src.models.schema import TraceEntry as TE

    kg = KnowledgeGraph()
    kg.add_trace_entry(
        TE(
            timestamp=datetime.now(tz=timezone.utc),
            agent="Test",
            action="check",
            evidence_source="static_analysis",
            confidence="observed",
        )
    )
    p = tmp_path / "kg.json"
    kg.save(p)
    kg2 = KnowledgeGraph.load(p)
    assert len(kg2.trace_entries) == 1
    assert kg2.trace_entries[0].agent == "Test"


def test_parse_day_one_answers_handles_markdown() -> None:
    sem = Semanticist(client=FakeLLMClient())
    kg = KnowledgeGraph()
    raw = """
**Q1:** The primary ingestion path is x.
file:src/etl.py:L1-10

### Q2. 
The critical outputs are y.
file:src/out.py:L20-30

_Q3_
The blast radius is z.
file:src/core.py:L5-8

Q4:
Logic is here.
file:src/logic.py:L1-5

**Q5:**
Velocity is high.
file:src/high.py:L100-200
"""
    answers = sem._parse_day_one_answers(raw, kg, [])
    assert len(answers) == 5
    assert "Q1" in answers
    assert "Q2" in answers
    assert "Q3" in answers
    assert "Q4" in answers
    assert "Q5" in answers
    assert answers["Q1"].citations[0].file == "src/etl.py"
    assert answers["Q2"].citations[0].file == "src/out.py"


def test_parse_day_one_answers_handles_json() -> None:
    sem = Semanticist(client=FakeLLMClient())
    kg = KnowledgeGraph()
    raw = """```json
{
  "answers": [
    {
      "id": "Q1",
      "answer": "The primary data ingestion path starts here.",
      "method": "static_analysis",
      "citation": "src/main.py::process_corpus_refined:L17"
    },
    {
      "id": "Q2",
      "answer": "The most critical output datasets.",
      "method": "static_analysis",
      "citation": "src/data/fact_table.py::FactTable:L15"
    },
    {
      "id": "Q3",
      "answer": "Blast radius.",
      "method": "static_analysis",
      "citation": "src/models/core.py::DocumentProfile:L13"
    },
    {
      "id": "Q4",
      "answer": "Business logic.",
      "method": "static_analysis",
      "citation": "rubric/extraction_rules.yaml:L1-1"
    },
    {
      "id": "Q5",
      "answer": "High velocity.",
      "method": "static_analysis",
      "citation": "src/models/core.py::DocumentProfile:L13"
    }
  ]
}
```"""
    answers = sem._parse_day_one_answers(raw, kg, [])
    assert len(answers) == 5
    assert "Q1" in answers
    assert answers["Q1"].citations[0].file == "src/main.py"
    assert answers["Q2"].citations[0].file == "src/data/fact_table.py"
    assert answers["Q4"].citations[0].file == "rubric/extraction_rules.yaml"
