import pytest
from src.agents.navigator import Navigator
from src.graph.knowledge_graph import KnowledgeGraph


def test_navigator_initialization(tmp_path):
    # Mock repo structure
    repo = tmp_path / "repo"
    repo.mkdir()
    carto = repo / ".cartography"
    carto.mkdir()

    # Save a fake module graph
    kg = KnowledgeGraph()
    # Pydantic requires id, type, path, language
    kg.graph.add_node(
        "module:test.py",
        id="module:test.py",
        type="module",
        path="test.py",
        language="python",
    )
    kg.save(carto / "module_graph.json")

    # Set fake API key for testing
    import os

    os.environ["OPENROUTER_API_KEY"] = "fake_key"

    nav = Navigator(str(repo))
    assert nav.repo_path == repo.resolve()
    assert "module:test.py" in nav.kg.graph.nodes
    assert len(nav.tools) == 4


@pytest.mark.asyncio
async def test_navigator_tools_binding(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".cartography").mkdir()

    kg = KnowledgeGraph()
    kg.graph.add_node(
        "module:test.py",
        id="module:test.py",
        type="module",
        path="test.py",
        language="python",
    )
    kg.save(repo / ".cartography" / "module_graph.json")

    import os

    os.environ["OPENROUTER_API_KEY"] = "fake_key"

    nav = Navigator(str(repo))
    # Check if tools are bound
    # In newer langchain-openai, it might be in additional_kwargs or bound_tools
    assert len(nav.tools) == 4
