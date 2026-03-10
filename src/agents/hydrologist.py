"""
Hydrologist Agent — Step 6 (Day 2)

Orchestrates lineage extraction across:
  - SQL files   (SQLAnalyzer)
  - Python files (PythonDataFlowAnalyzer)
  - Airflow DAG files (AirflowDagAnalyzer)
  - dbt schema.yml files (DbtSchemaAnalyzer)

Merges all results into a unified DataLineageGraph (KnowledgeGraph) using
deterministic node IDs:
  - dataset:<normalized_name>
  - module:<relative_path>
  - transformation:<relative_path>:<line_range>

Exposes blast_radius, find_sources, find_sinks for graph traversal.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Set

import networkx as nx

from src.analyzers.dag_config_parser import AirflowDagAnalyzer, DbtSchemaAnalyzer
from src.analyzers.python_dataflow import DataRef, PythonDataFlowAnalyzer
from src.analyzers.sql_lineage import SQLAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import (
    DatasetNode,
    Edge,
    EdgeType,
    StorageType,
    TransformationNode,
    WarningRecord,
    WarningSeverity,
)


def _dataset_id(name: str) -> str:
    return f"dataset:{name}"


def _transformation_id(filepath: str, line_range: str) -> str:
    return f"transformation:{filepath}:{line_range}"


class Hydrologist:
    """
    Builds the merged DataLineageGraph from all analyzer outputs.

    Usage::

        h = Hydrologist(repo_root)
        kg = h.run()
        kg.save(".cartography/lineage_graph.json")
    """

    def __init__(self, repo_root: str) -> None:
        self.root = Path(repo_root)
        self._sql = SQLAnalyzer()
        self._py_flow = PythonDataFlowAnalyzer()
        self._airflow = AirflowDagAnalyzer()
        self._dbt = DbtSchemaAnalyzer()

    def run(self) -> KnowledgeGraph:
        kg = KnowledgeGraph()

        self._ingest_sql_files(kg)
        self._ingest_python_files(kg)
        self._ingest_airflow_dags(kg)
        self._ingest_dbt_schemas(kg)

        return kg

    # ── SQL ────────────────────────────────────────────────────────────────────

    def _ingest_sql_files(self, kg: KnowledgeGraph) -> None:
        for sql_path in self.root.rglob("*.sql"):
            rel = str(sql_path.relative_to(self.root))
            try:
                sql = sql_path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                kg.add_warning(
                    WarningRecord(
                        code="READ_ERROR",
                        message=str(exc),
                        file=rel,
                        analyzer="Hydrologist/SQL",
                        severity=WarningSeverity.ERROR,
                    )
                )
                continue

            deps, parse_warnings = self._sql.extract_dependencies(sql, filepath=rel)
            for w in parse_warnings:
                kg.add_warning(w)
            # Determine target table from the file name (snake_case convention)
            target_name = sql_path.stem.lower()
            target_id = _dataset_id(target_name)
            tx_id = _transformation_id(rel, "1-end")

            # Ensure target dataset node exists
            _ensure_dataset(kg, target_name)

            # Transformation node
            source_datasets = list(deps)
            tx_node = TransformationNode(
                id=tx_id,
                source_datasets=source_datasets,
                target_datasets=[target_name],
                transformation_type="sql",
                source_file=rel,
                line_range="1-end",
            )
            kg.add_node(tx_node)

            # Source datasets → Transformation
            for dep in deps:
                dep_id = _dataset_id(dep)
                _ensure_dataset(kg, dep)
                kg.add_edge(Edge(source=dep_id, target=tx_id, type=EdgeType.CONSUMES))

            # Transformation → target dataset
            kg.add_edge(Edge(source=tx_id, target=target_id, type=EdgeType.PRODUCES))

    # ── Python ─────────────────────────────────────────────────────────────────

    def _ingest_python_files(self, kg: KnowledgeGraph) -> None:
        for py_path in self.root.rglob("*.py"):
            rel = str(py_path.relative_to(self.root))
            # Skip test/config files
            if any(p in rel for p in ["test_", "__pycache__", ".venv", "setup.py"]):
                continue

            result = self._py_flow.analyze_file(py_path)
            for w in result.warnings:
                kg.add_warning(w)

            self._add_python_refs(kg, rel, result.reads, "read")
            self._add_python_refs(kg, rel, result.writes, "write")

    def _add_python_refs(
        self,
        kg: KnowledgeGraph,
        rel: str,
        refs: List[DataRef],
        direction: str,
    ) -> None:
        for ref in refs:
            ds_id = _dataset_id(ref.name)
            tx_id = _transformation_id(rel, f"{ref.line}-{ref.line}")
            _ensure_dataset(kg, ref.name)

            tx_node = TransformationNode(
                id=tx_id,
                source_datasets=[ref.name] if direction == "read" else [],
                target_datasets=[ref.name] if direction == "write" else [],
                transformation_type=ref.api,
                source_file=rel,
                line_range=f"{ref.line}-{ref.line}",
            )
            kg.add_node(tx_node)

            if direction == "read":
                kg.add_edge(Edge(source=ds_id, target=tx_id, type=EdgeType.CONSUMES))
            else:
                kg.add_edge(Edge(source=tx_id, target=ds_id, type=EdgeType.PRODUCES))

    # ── Airflow ────────────────────────────────────────────────────────────────

    def _ingest_airflow_dags(self, kg: KnowledgeGraph) -> None:
        for dag_path in self.root.rglob("*.py"):
            # Quick heuristic: airflow dags import DAG
            try:
                content = dag_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if "from airflow" not in content and "import airflow" not in content:
                continue

            rel = str(dag_path.relative_to(self.root))
            dag_result = self._airflow.analyze(content, rel)
            for w in dag_result.warnings:
                kg.add_warning(w)

            # Add task dependency edges as CALLS edges between transformation nodes
            task_id_to_node: Dict[str, str] = {}
            for task_id, task in dag_result.tasks.items():
                tx_id = _transformation_id(rel, task_id)
                tx_node = TransformationNode(
                    id=tx_id,
                    source_datasets=[],
                    target_datasets=[],
                    transformation_type=task.operator,
                    source_file=rel,
                    line_range=task_id,
                )
                kg.add_node(tx_node)
                task_id_to_node[task_id] = tx_id

            for task_id, task in dag_result.tasks.items():
                for upstream_id in task.dependencies:
                    up_tx = task_id_to_node.get(upstream_id)
                    dn_tx = task_id_to_node.get(task_id)
                    if up_tx and dn_tx:
                        kg.add_edge(
                            Edge(source=up_tx, target=dn_tx, type=EdgeType.CALLS)
                        )

    # ── dbt ────────────────────────────────────────────────────────────────────

    def _ingest_dbt_schemas(self, kg: KnowledgeGraph) -> None:
        for schema_path in self.root.rglob("schema.yml"):
            rel = str(schema_path.relative_to(self.root))
            try:
                content = schema_path.read_text(encoding="utf-8")
            except OSError as exc:
                kg.add_warning(
                    WarningRecord(
                        code="READ_ERROR",
                        message=str(exc),
                        file=rel,
                        analyzer="Hydrologist/dbt",
                        severity=WarningSeverity.ERROR,
                    )
                )
                continue

            dbt_result = self._dbt.analyze(content, rel)
            for w in dbt_result.warnings:
                kg.add_warning(w)

            for model in dbt_result.models:
                _ensure_dataset(kg, model.name, owner=model.owner)

            for source in dbt_result.sources:
                for table in source.tables:
                    full_name = f"{source.name}.{table}"
                    _ensure_dataset(
                        kg,
                        full_name,
                        storage_type=StorageType.TABLE,
                        owner=source.owner,
                    )

    # ── Graph traversal ────────────────────────────────────────────────────────

    def blast_radius(self, node_id: str, kg: KnowledgeGraph) -> Set[str]:
        """Return all downstream nodes reachable from node_id."""
        try:
            return set(nx.descendants(kg.graph, node_id))
        except nx.NetworkXError:
            return set()

    def find_sources(self, kg: KnowledgeGraph) -> List[str]:
        """Return nodes with no incoming edges (graph sources)."""
        return [n for n in kg.graph.nodes if kg.graph.in_degree(n) == 0]

    def find_sinks(self, kg: KnowledgeGraph) -> List[str]:
        """Return nodes with no outgoing edges (graph sinks / final outputs)."""
        return [n for n in kg.graph.nodes if kg.graph.out_degree(n) == 0]


# ── helpers ───────────────────────────────────────────────────────────────────


def _ensure_dataset(
    kg: KnowledgeGraph,
    name: str,
    storage_type: StorageType = StorageType.UNKNOWN,
    owner: str | None = None,
) -> None:
    """Add a DatasetNode if it doesn't already exist (idempotent)."""
    node_id = _dataset_id(name)
    if node_id not in kg.graph.nodes:
        node = DatasetNode(
            id=node_id,
            name=name,
            storage_type=storage_type,
            owner=owner,
        )
        kg.add_node(node)
