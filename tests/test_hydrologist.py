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


# ── Airflow e2e fixture integration test (audit finding #6) ──────────────────

AIRFLOW_DAG_SOURCE = """\
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime

with DAG('etl_pipeline', start_date=datetime.datetime(2024,1,1)) as dag:
    extract = PythonOperator(task_id='extract', python_callable=run_extract)
    transform = PostgresOperator(
        task_id='transform',
        sql='SELECT * FROM raw.events WHERE date > current_date - 7',
    )
    load = PythonOperator(task_id='load', python_callable=run_load)
    extract >> transform >> load
"""


def test_hydrologist_airflow_dag_produces_transformation_nodes(tmp_path: Any) -> None:
    """
    End-to-end test: Hydrologist runs over a fake repo containing an Airflow DAG file.
    Verifies:
      - Transformation nodes are created for each task
      - Task dependency edges (CALLS) exist between transformation nodes
      - The artifact (lineage_graph.json) is valid JSON with nodes and edges
    """
    dag_dir = tmp_path / "dags"
    dag_dir.mkdir()
    (dag_dir / "etl_pipeline.py").write_text(AIRFLOW_DAG_SOURCE, encoding="utf-8")

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    out_path = tmp_path / ".cartography" / "lineage_graph.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    kg.save(out_path)

    with open(out_path) as f:
        data = json.load(f)

    assert "nodes" in data
    assert "edges" in data

    # Check transformation nodes exist for the three tasks
    node_ids = list(data["nodes"].keys())
    assert any("extract" in n for n in node_ids), "Expected 'extract' task node"
    assert any("transform" in n for n in node_ids), "Expected 'transform' task node"
    assert any("load" in n for n in node_ids), "Expected 'load' task node"

    # Check CALLS edges (extract→transform, transform→load)
    edges = data["edges"]
    calls_edges = [e for e in edges if e.get("type") == "calls"]
    assert len(calls_edges) >= 2, f"Expected ≥2 CALLS edges, got: {calls_edges}"


def test_hydrologist_sql_parse_failures_appear_in_warnings(tmp_path: Any) -> None:
    """
    Hydrologist must surface SQL parse errors as structured warnings in the
    lineage_graph.json, not silently swallow them.
    """
    sql_dir = tmp_path / "queries"
    sql_dir.mkdir()
    (sql_dir / "broken.sql").write_text(
        "THIS IS COMPLETELY INVALID SQL @@@@", encoding="utf-8"
    )

    h = Hydrologist(str(tmp_path))
    kg = h.run()

    out_path = tmp_path / ".cartography" / "lineage_graph.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    kg.save(out_path)

    with open(out_path) as f:
        data = json.load(f)

    warnings = data.get("warnings", [])
    assert any(w.get("code") == "SQL_PARSE_ERROR" for w in warnings), (
        f"Expected SQL_PARSE_ERROR warning, got: {warnings}"
    )
