import os
from src.models.schema import (
    DatasetNode,
    TransformationNode,
    Edge,
    EdgeType,
    WarningRecord,
)
from typing import Any
from src.graph.knowledge_graph import KnowledgeGraph


def test_lineage_graph_serialization(tmp_path: Any) -> None:
    kg = KnowledgeGraph()

    ds1 = DatasetNode(id="dataset:source_db.users", name="source_db.users")
    ds2 = DatasetNode(
        id="dataset:warehouse.active_users", name="warehouse.active_users"
    )

    tx1 = TransformationNode(
        id="transformation:models/active_users.sql:1-10",
        source_datasets=["source_db.users"],
        target_datasets=["warehouse.active_users"],
        transformation_type="sql",
        source_file="models/active_users.sql",
        line_range="1-10",
    )

    kg.add_node(ds1)
    kg.add_node(ds2)
    kg.add_node(tx1)

    edge1 = Edge(
        source="dataset:source_db.users",
        target="transformation:models/active_users.sql:1-10",
        type=EdgeType.CONSUMES,
    )
    edge2 = Edge(
        source="transformation:models/active_users.sql:1-10",
        target="dataset:warehouse.active_users",
        type=EdgeType.PRODUCES,
    )

    kg.add_edge(edge1)
    kg.add_edge(edge2)

    kg.add_warning(
        WarningRecord(
            code="UNRESOLVED_REF",
            message="Could not resolve dynamic reference",
            file="models/active_users.sql",
            analyzer="SQLAnalyzer",
        )
    )

    file_path = tmp_path / "lineage_graph.json"
    kg.save(file_path)

    assert os.path.exists(file_path)

    kg2 = KnowledgeGraph.load(file_path)
    schema = kg2.to_schema()

    assert "dataset:source_db.users" in schema.nodes
    assert len(schema.edges) == 2
    assert len(kg2.warnings) == 1
    assert kg2.warnings[0].code == "UNRESOLVED_REF"
