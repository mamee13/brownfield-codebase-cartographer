"""
End-to-End Integration Test for Brownfield Cartographer.
Verifies the full pipeline from Surveyor to Archivist on a multi-dialect repo.
"""

from typing import Any

from src.orchestrator import Orchestrator
from src.agents.semanticist import FakeLLMClient


def test_e2e_full_pipeline(tmp_path: Any, monkeypatch: Any) -> None:
    """
    Test 1: Full pipeline on a simulated repo with:
    - Python files (data ingestion)
    - SQL files (transformations)
    - dbt models
    """
    repo = tmp_path / "e2e_repo"
    repo.mkdir()

    # 1. Python ingestion
    (repo / "ingest.py").write_text(
        """
import sqlite3
def run_ingest():
    conn = sqlite3.connect("raw.db")
    conn.execute("INSERT INTO raw_events SELECT * FROM external_source")
""",
        encoding="utf-8",
    )

    # 2. SQL transformation
    (repo / "transform.sql").write_text(
        """
CREATE TABLE staging_events AS
SELECT * FROM raw_events WHERE event_type = 'click'
""",
        encoding="utf-8",
    )

    # 3. dbt model
    (repo / "models").mkdir()
    (repo / "models" / "schema.yml").write_text(
        """
version: 2
models:
  - name: final_metrics
    description: "Final rollup"
    tests:
      - unique
""",
        encoding="utf-8",
    )
    (repo / "models" / "final_metrics.sql").write_text(
        """
SELECT count(*) FROM {{ ref('staging_events') }}
""",
        encoding="utf-8",
    )

    # Mock LLM
    monkeypatch.setattr(
        "src.agents.semanticist.OpenRouterLLMClient",
        lambda *args, **kwargs: FakeLLMClient(
            responses=[
                "Simulated purpose for ingest.py",
                "Simulated purpose for transform.sql",
                "Simulated purpose for final_metrics.sql",
                '{"domains": ["Ingestion", "Transformation"]}',  # Domain clustering
                """Q1: Architecture: Multi-stage pipeline from raw to metrics.
                   Q2: Ingestion starts at ingest.py.
                   Q3: Blast radius covers staging_events.
                   Q4: Logic in SQL files.
                   Q5: High change in transform.sql.""",  # All in one call
            ]
        ),
    )

    # Mock Git change velocity to avoid external git dependencies in tests
    monkeypatch.setattr(
        "src.agents.surveyor.Surveyor.extract_git_velocity",
        lambda self, days=30: {"ingest.py": 5, "transform.sql": 10},
    )

    # Run pipeline
    orchestrator = Orchestrator(str(repo))
    orchestrator.analyze()

    cart_dir = repo / ".cartography"
    assert cart_dir.exists()

    # Verify CODEBASE.md
    codebase = (cart_dir / "CODEBASE.md").read_text(encoding="utf-8")
    assert "Architecture Overview" in codebase
    assert "Critical Path" in codebase
    assert "Data Sources & Sinks" in codebase
    assert "Module Purpose Index" in codebase
    # Verify the table format for Module Purpose Index
    assert "| Module | Purpose |" in codebase
    assert "ingest.py" in codebase

    # Verify onboarding_brief.md
    brief = (cart_dir / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "Q1: What is the primary data ingestion path?" in brief
    # Verify citations exist
    assert "Evidence:" in brief

    # Verify trace log
    trace = (cart_dir / "cartography_trace.jsonl").read_text(encoding="utf-8")
    assert "Archivist" in trace
    assert "generate_codebase_md" in trace

    # Verify graphs
    assert (cart_dir / "module_graph.json").exists()
    assert (cart_dir / "lineage_graph.json").exists()


def test_e2e_empty_repo(tmp_path: Any, monkeypatch: Any) -> None:
    """Test pipeline robustness on an empty repo."""
    repo = tmp_path / "empty_repo"
    repo.mkdir()

    orchestrator = Orchestrator(str(repo))
    # Should not crash
    orchestrator.analyze()

    assert (repo / ".cartography" / "CODEBASE.md").exists()
    content = (repo / ".cartography" / "CODEBASE.md").read_text()
    assert "No modules found" in content or "Architecture Overview" in content
