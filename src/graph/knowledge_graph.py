import json
from pathlib import Path
from typing import Dict

import networkx as nx
from pydantic import TypeAdapter
from typing import Any

from src.models.schema import Edge, GraphSchema, Node, WarningRecord

NodeAdapter: TypeAdapter[Any] = TypeAdapter(Node)


class KnowledgeGraph:
    def __init__(self) -> None:
        self.graph: nx.DiGraph[Any] = nx.DiGraph()
        self.warnings: list[WarningRecord] = []

    def add_warning(self, warning: WarningRecord) -> None:
        self.warnings.append(warning)

    def add_node(self, node: Node) -> None:
        """Add a Node to the graph, using its id as the NetworkX node identifier."""
        data = node.model_dump(exclude_none=True)
        self.graph.add_node(node.id, **data)

    def add_edge(self, edge: Edge) -> None:
        """Add an Edge to the graph."""
        data = edge.model_dump(exclude_none=True)
        # source and target are explicit in edge, we can separate them
        src = data.pop("source")
        dst = data.pop("target")
        self.graph.add_edge(src, dst, **data)

    def to_schema(self) -> GraphSchema:
        """Export the graph as a GraphSchema Pydantic model."""
        nodes: Dict[str, Node] = {}
        for n_id, data in self.graph.nodes(data=True):
            # Parse dict back to appropriate Node model
            nodes[str(n_id)] = NodeAdapter.validate_python(data)

        edges = []
        for src, dst, data in self.graph.edges(data=True):
            edge_data = {"source": str(src), "target": str(dst)}
            edge_data.update(data)
            edges.append(Edge.model_validate(edge_data))

        return GraphSchema(nodes=nodes, edges=edges, warnings=self.warnings)

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
        # Restore warnings from the serialized schema
        kg.warnings = list(schema.warnings)
        return kg
