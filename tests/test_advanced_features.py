from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import ModuleNode, Edge, EdgeType
from src.agents.semanticist import LLMResponse
from src.agents.navigator import blast_radius_logic
from src.utils.cost import calculate_cost
from src.agents.semanticist import ContextWindowBudget
from src.state_tracker import FileStateTracker


def test_transitive_blast_radius():
    kg = KnowledgeGraph()
    # A -> B -> C (A depends on B, B depends on C)
    # Blast radius of C should be {B, A}
    kg.add_node(ModuleNode(id="module:A.py", path="A.py", language="python"))
    kg.add_node(ModuleNode(id="module:B.py", path="B.py", language="python"))
    kg.add_node(ModuleNode(id="module:C.py", path="C.py", language="python"))

    kg.add_edge(Edge(source="module:A.py", target="module:B.py", type=EdgeType.IMPORTS))
    kg.add_edge(Edge(source="module:B.py", target="module:C.py", type=EdgeType.IMPORTS))

    output = blast_radius_logic("C.py", kg)
    assert "Transitive dependents (blast radius) for 'C.py':" in output
    assert "A.py" in output
    assert "B.py" in output


def test_cost_calculation():
    # gpt-4o: $5/1M in, $15/1M out
    cost = calculate_cost("openai/gpt-4o", 1_000_000, 1_000_000)
    assert cost == 20.0

    # free model
    cost = calculate_cost("qwen/qwen-2.5-7b-instruct:free", 1000, 1000)
    assert cost == 0.0


def test_budget_usd_tracking():
    budget = ContextWindowBudget()
    resp = LLMResponse(
        text="hi", tokens_in=1_000_000, tokens_out=1_000_000, model="openai/gpt-4o"
    )
    budget.charge(resp)
    assert budget.used_usd == 20.0


def test_incremental_deletion_detection(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    f1 = repo / "f1.py"
    f1.write_text("print(1)")

    tracker = FileStateTracker(tmp_path)
    # First run
    changed, deleted = tracker.get_changed_files(repo)
    assert len(changed) == 1
    assert len(deleted) == 0
    tracker.save_state()

    # Delete file
    f1.unlink()

    # Second run
    tracker = FileStateTracker(tmp_path)
    changed, deleted = tracker.get_changed_files(repo)
    assert len(changed) == 0
    assert "f1.py" in deleted
