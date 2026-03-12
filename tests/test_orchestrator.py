"""Integration tests for the Orchestrator (full Day 3 pipeline)."""

from typing import Any

from src.orchestrator import Orchestrator
from src.agents.semanticist import FakeLLMClient


def test_orchestrator_generates_all_artifacts(tmp_path: Any, monkeypatch: Any) -> None:
    """The full pipeline must produce all 5 artifact files."""
    # Create a dummy repo structure
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text("def hello(): pass", encoding="utf-8")

    # Mock OpenRouterLLMClient to use FakeLLMClient to avoid network calls
    monkeypatch.setattr(
        "src.agents.semanticist.OpenRouterLLMClient",
        lambda *args, **kwargs: FakeLLMClient(
            responses=[
                "Module purpose.",
                "MATCH",
                "domain_label",
                "Q1: answer file:src/main.py:L1-1",
            ]
        ),
    )

    orchestrator = Orchestrator(str(repo))
    orchestrator.analyze()

    cartography_dir = repo / ".cartography"
    assert cartography_dir.exists()

    # Check all 5 artifacts exist
    assert (cartography_dir / "module_graph.json").exists()
    assert (cartography_dir / "lineage_graph.json").exists()
    assert (cartography_dir / "CODEBASE.md").exists()
    assert (cartography_dir / "onboarding_brief.md").exists()
    assert (cartography_dir / "cartography_trace.jsonl").exists()

    # Check CODEBASE.md has content
    codebase = (cartography_dir / "CODEBASE.md").read_text(encoding="utf-8")
    assert "## Architecture Overview" in codebase
    assert "## Critical Path" in codebase

    # Check trace log has entries
    trace = (cartography_dir / "cartography_trace.jsonl").read_text(encoding="utf-8")
    assert len(trace.splitlines()) > 0


def test_orchestrator_graceful_degradation_on_llm_error(
    tmp_path: Any, monkeypatch: Any
) -> None:
    """If the LLM raises an error, the pipeline must not crash and still produce artifacts."""
    repo = tmp_path / "repo2"
    repo.mkdir()
    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text("x = 1", encoding="utf-8")

    class FailingClient:
        def complete(self, prompt: str, model: str, max_tokens: int = 1024) -> Any:
            raise TimeoutError("API timeout")

        def embed(self, texts: list[str], model: str = "") -> list[list[float]]:
            raise TimeoutError("API timeout")

    monkeypatch.setattr(
        "src.agents.semanticist.OpenRouterLLMClient",
        lambda *args, **kwargs: FailingClient(),
    )

    orchestrator = Orchestrator(str(repo))
    # Must NOT raise an exception
    orchestrator.analyze()

    cartography_dir = repo / ".cartography"

    # Artifacts should still be generated (populated with fallback/static data)
    assert (cartography_dir / "CODEBASE.md").exists()

    # Trace log should contain an llm_error action
    trace = (cartography_dir / "cartography_trace.jsonl").read_text(encoding="utf-8")
    assert "llm_error" in trace
