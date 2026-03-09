"""Integration tests for the Hydrologist agent: namespace collision, graph traversal, e2e artifact."""

import json
import os
from typing import Any


from src.agents.hydrologist import Hydrologist, _dataset_id


# ── Namespace collision prevention ────────────────────────────────────────────


def test_idempotent_dataset_node_no_duplicates(tmp_path: Any) -> None:
    """The same dataset referenced by multiple SQL files must produce ONE node."""
    sql_dir = tmp_path / "models"
    sql_dir.mkdir()

    # Both SQL files reference the same source table
    (sql_dir / "orders_agg.sql").write_text(
        "SELECT user_id, SUM(amount) FROM raw.orders GROUP BY 1", encoding="utf-8"
    )
    (sql_dir / "orders_count.sql").write_text(
        "SELECT COUNT(*) FROM raw.orders", encoding="utf-8"
    )

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    # raw.orders should exist exactly once
    dataset_node_id = _dataset_id("raw.orders")
    assert dataset_node_id in kg.graph.nodes
    # Count occurrences of that node (networkx set-like, so should be 1)
    matching = [n for n in kg.graph.nodes if n == dataset_node_id]
    assert len(matching) == 1


def test_deterministic_node_ids(tmp_path: Any) -> None:
    """Nodes must use deterministic ID prefixes to prevent cross-type collisions."""
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    (sql_dir / "users.sql").write_text("SELECT * FROM source.users", encoding="utf-8")

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    # All node IDs must start with a namespace prefix
    for node_id in kg.graph.nodes:
        assert any(
            node_id.startswith(prefix)
            for prefix in ("dataset:", "transformation:", "module:")
        ), f"Node ID lacks namespace prefix: {node_id}"


# ── Graph traversal ───────────────────────────────────────────────────────────


def test_find_sources_and_sinks(tmp_path: Any) -> None:
    """Source datasets (no upstream) and sink datasets (no downstream) are identified correctly."""
    sql_dir = tmp_path / "models"
    sql_dir.mkdir()
    (sql_dir / "final_report.sql").write_text(
        "SELECT * FROM staging.cleaned_events", encoding="utf-8"
    )

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    sources = h.find_sources(kg)
    sinks = h.find_sinks(kg)

    # staging.cleaned_events is a source (no upstream edge pointing to it)
    assert _dataset_id("staging.cleaned_events") in sources
    # final_report dataset is a sink (no downstream edge from it)
    assert _dataset_id("final_report") in sinks


def test_blast_radius(tmp_path: Any) -> None:
    """blast_radius must return all transitive downstream nodes."""
    sql_dir = tmp_path / "models"
    sql_dir.mkdir()
    (sql_dir / "mart.sql").write_text("SELECT * FROM staging.orders", encoding="utf-8")

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    # Source dataset should have downstream nodes
    source_id = _dataset_id("staging.orders")
    radius = h.blast_radius(source_id, kg)
    # At minimum the transformation and the mart dataset are downstream
    assert len(radius) >= 1


# ── End-to-end artifact creation ─────────────────────────────────────────────


def test_analyze_writes_lineage_graph_json(tmp_path: Any) -> None:
    """Running the Hydrologist then saving must produce a valid lineage_graph.json."""
    sql_dir = tmp_path / "queries"
    sql_dir.mkdir()
    (sql_dir / "dashboard.sql").write_text(
        "SELECT * FROM dw.dim_users JOIN dw.fact_sales ON dim_users.id = fact_sales.user_id",
        encoding="utf-8",
    )

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    out_path = tmp_path / ".cartography" / "lineage_graph.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    kg.save(out_path)

    assert os.path.exists(out_path)

    with open(out_path) as f:
        data = json.load(f)

    assert "nodes" in data
    assert "edges" in data
    # Both source tables must appear as datasets
    assert _dataset_id("dw.dim_users") in data["nodes"]
    assert _dataset_id("dw.fact_sales") in data["nodes"]
