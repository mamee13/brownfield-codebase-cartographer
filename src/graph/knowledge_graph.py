import json
from pathlib import Path
from typing import Any, Dict, List

import networkx as nx
from pydantic import TypeAdapter

from src.models.schema import (
    AnswerWithCitation,
    Edge,
    GraphSchema,
    Node,
    TraceEntry,
    WarningRecord,
)

NodeAdapter: TypeAdapter[Any] = TypeAdapter(Node)


class KnowledgeGraph:
    def __init__(self) -> None:
        self.graph: nx.DiGraph[Any] = nx.DiGraph()
        self.warnings: List[WarningRecord] = []
        # Day 3 additions — in-memory stores, serialized via to_schema()
        self.day_one_answers: Dict[str, AnswerWithCitation] = {}
        self.trace_entries: List[TraceEntry] = []

    # ── Warnings ──────────────────────────────────────────────────────────────

    def add_warning(self, warning: WarningRecord) -> None:
        """Add a warning, avoiding duplicates based on (code, file, line)."""
        # Simple deduplication
        for existing in self.warnings:
            if (
                existing.code == warning.code
                and existing.file == warning.file
                and existing.line == warning.line
            ):
                return
        self.warnings.append(warning)

    # ── Nodes / Edges ─────────────────────────────────────────────────────────

    def add_node(self, node: Node) -> None:
        """Add a Node to the graph, using its id as the NetworkX node identifier."""
        data = node.model_dump(exclude_none=True)
        self.graph.add_node(node.id, **data)

    def add_edge(self, edge: Edge) -> None:
        """Add an Edge to the graph."""
        data = edge.model_dump(exclude_none=True)
        src = data.pop("source")
        dst = data.pop("target")
        self.graph.add_edge(src, dst, **data)

    # ── Day 3 storage ─────────────────────────────────────────────────────────

    def set_day_one_answers(self, answers: Dict[str, AnswerWithCitation]) -> None:
        self.day_one_answers = answers

    def add_trace_entry(self, entry: TraceEntry) -> None:
        self.trace_entries.append(entry)

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_schema(self) -> GraphSchema:
        """Export the graph as a GraphSchema Pydantic model."""
        nodes: Dict[str, Node] = {}
        for n_id, data in self.graph.nodes(data=True):
            nodes[str(n_id)] = NodeAdapter.validate_python(data)

        edges = []
        for src, dst, data in self.graph.edges(data=True):
            edge_data = {"source": str(src), "target": str(dst)}
            edge_data.update(data)
            edges.append(Edge.model_validate(edge_data))

        return GraphSchema(
            nodes=nodes,
            edges=edges,
            warnings=self.warnings,
            day_one_answers=self.day_one_answers,
            trace_entries=self.trace_entries,
        )

    def save(self, filepath: str | Path) -> None:
        """Serialize the graph to a JSON file."""
        schema = self.to_schema()
        with open(filepath, "w") as f:
            f.write(schema.model_dump_json(indent=2, exclude_none=True))

    @classmethod
    def load(cls, filepath: str | Path) -> "KnowledgeGraph":
        """Load the graph from a JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        schema = GraphSchema.model_validate(data)

        kg = cls()
        for node in schema.nodes.values():
            kg.add_node(node)
        for edge in schema.edges:
            kg.add_edge(edge)
        kg.warnings = list(schema.warnings)
        # Day 3: restore answers and trace from saved JSON
        kg.day_one_answers = dict(schema.day_one_answers)
        kg.trace_entries = list(schema.trace_entries)
        return kg
