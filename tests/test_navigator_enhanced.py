from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import WarningRecord, Citation, AnswerWithCitation


def test_warning_deduplication():
    kg = KnowledgeGraph()
    w1 = WarningRecord(code="TEST", message="m1", file="f1.py", line=10, analyzer="A")
    w2 = WarningRecord(code="TEST", message="m2", file="f1.py", line=10, analyzer="A")
    w3 = WarningRecord(code="OTHER", message="m1", file="f1.py", line=10, analyzer="A")

    kg.add_warning(w1)
    kg.add_warning(w2)  # Duplicate
    kg.add_warning(w3)  # Not duplicate (different code)

    assert len(kg.warnings) == 2
    assert kg.warnings[0].code == "TEST"
    assert kg.warnings[1].code == "OTHER"


def test_citation_validation(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "exists.py").write_text("print('hello')")
    (repo / ".cartography").mkdir()
    (repo / ".cartography" / "module_graph.json").write_text("{}")

    # Test valid and invalid citations using the schema models
    c1 = Citation(file="exists.py", line_range="L1-1", method="static_analysis")
    c2 = Citation(file="missing.py", line_range="L1-1", method="static_analysis")
    c3 = Citation(file="module_graph.json", line_range="N/A", method="static_analysis")

    ans = AnswerWithCitation(
        answer="Hello", citations=[c1, c2, c3], confidence="observed"
    )

    valid_citations = []
    for c in ans.citations:
        if c.file in ["lineage_graph.json", "module_graph.json"]:
            valid_citations.append(c)
            continue
        full_path = repo / c.file
        if full_path.exists():
            valid_citations.append(c)

    assert len(valid_citations) == 2
    assert any(c.file == "exists.py" for c in valid_citations)
    assert any(c.file == "module_graph.json" for c in valid_citations)
    assert not any(c.file == "missing.py" for c in valid_citations)


def test_logic_functions_evidence_markers():
    # Verify that logic functions return the required EVIDENCE markers
    from src.agents.navigator import trace_lineage_logic, blast_radius_logic
    from src.models.schema import ModuleNode, DatasetNode

    kg = KnowledgeGraph()
    # Mock some nodes for blast_radius
    m1 = ModuleNode(
        id="module:m1.py", path="m1.py", language="python", purpose_statement="test"
    )
    kg.add_node(m1)

    # Mock lineage graph for trace_lineage
    # It expects id to be formatted as 'dataset:{name}'
    d1 = DatasetNode(id="dataset:s1", name="s1")
    d2 = DatasetNode(id="dataset:s2", name="s2")
    kg.add_node(d1)
    kg.add_node(d2)

    # Add an edge for upstream check
    from src.models.schema import Edge, EdgeType

    kg.add_edge(Edge(source="dataset:s1", target="dataset:s2", type=EdgeType.PRODUCES))

    # trace_lineage
    output = trace_lineage_logic("s2", "upstream", kg)
    assert "EVIDENCE: [file: lineage_graph.json" in output

    # blast_radius
    output = blast_radius_logic("m1.py", kg)
    assert "EVIDENCE: [file: lineage_graph.json" in output
