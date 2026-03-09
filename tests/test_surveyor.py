from pathlib import Path

from src.agents.surveyor import Surveyor
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer


def test_tree_sitter_excludes_private_and_dunder_symbols() -> None:
    analyzer = TreeSitterAnalyzer()
    content = b"""
def __dunder__():
    return 1

def _private():
    return 2

def public():
    return 3

class _Hidden:
    pass

class Visible:
    pass
"""
    imports, symbols = analyzer.analyze_python_file("module.py", content)
    assert imports == []
    assert symbols == ["public", "Visible"]


def test_surveyor_builds_import_edges_and_metrics(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    src = repo / "src"
    src.mkdir(parents=True)

    (src / "__init__.py").write_text("", encoding="utf-8")
    (src / "a.py").write_text(
        "import src.b\nfrom src.c import useful\n\ndef public_api():\n    return 1\n",
        encoding="utf-8",
    )
    (src / "b.py").write_text(
        "from src.c import useful\n\ndef helper():\n    return useful()\n",
        encoding="utf-8",
    )
    (src / "c.py").write_text("def useful():\n    return 3\n", encoding="utf-8")
    (src / "d.py").write_text("def orphan_public():\n    return 4\n", encoding="utf-8")

    surveyor = Surveyor(str(repo))
    kg = surveyor.run()
    schema = kg.to_schema()

    edges = {(edge.source, edge.target) for edge in schema.edges}
    assert ("src/a.py", "src/b.py") in edges
    assert ("src/a.py", "src/c.py") in edges
    assert ("src/b.py", "src/c.py") in edges

    assert schema.nodes["src/d.py"].is_dead_code_candidate is True
    assert schema.nodes["src/c.py"].is_dead_code_candidate is False

    for node_id in schema.nodes:
        node_data = kg.graph.nodes[node_id]
        assert "pagerank" in node_data
