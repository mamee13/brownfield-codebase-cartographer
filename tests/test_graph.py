import os
from src.models.schema import ModuleNode, Edge, EdgeType
from src.graph.knowledge_graph import KnowledgeGraph


def test_knowledge_graph_serialization(tmp_path) -> None:
    kg = KnowledgeGraph()

    mod1 = ModuleNode(id="src/main.py", path="src/main.py", language="python")
    mod2 = ModuleNode(id="src/utils.py", path="src/utils.py", language="python")

    kg.add_node(mod1)
    kg.add_node(mod2)

    edge = Edge(
        source="src/main.py", target="src/utils.py", type=EdgeType.IMPORTS, weight=1
    )
    kg.add_edge(edge)

    file_path = tmp_path / "graph.json"
    kg.save(file_path)

    assert os.path.exists(file_path)

    kg2 = KnowledgeGraph.load(file_path)

    schema = kg2.to_schema()
    assert "src/main.py" in schema.nodes
    assert isinstance(schema.nodes["src/main.py"], ModuleNode)
    assert len(schema.edges) == 1
    assert schema.edges[0].source == "src/main.py"
    assert schema.edges[0].target == "src/utils.py"
    assert schema.edges[0].type == EdgeType.IMPORTS
