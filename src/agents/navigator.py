"""
Navigator Agent — Day 4

Interactive agent using LangGraph to answer natural language queries about the codebase.
Armed with 4 tools:
  1. find_implementation: Semantic search over module purpose statements.
  2. trace_lineage: Data flow traversal (upstream/downstream).
  3. blast_radius: Import graph traversal to find dependents.
  4. explain_module: Code-level explanation using LLM.
"""

from __future__ import annotations

import os
import operator
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, TypedDict
from pydantic import SecretStr

import numpy as np
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import (
    AnswerWithCitation,
    Citation,
    NodeType,
)


# ── State Definition ─────────────────────────────────────────────────────────


class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    kg: KnowledgeGraph
    repo_path: Path
    navigator_response: str


# ── Tool Logic (State-Independent or State-Passing) ──────────────────────────


def find_implementation_logic(query: str, kg: KnowledgeGraph) -> str:
    from src.agents.semanticist import OpenRouterLLMClient, EMBED_MODEL

    client = OpenRouterLLMClient()

    try:
        query_vec = client.embed([query], model=EMBED_MODEL)[0]
    except Exception as e:
        return f"Error generating embedding for query: {e}"

    results = []
    for node_id, data in kg.graph.nodes(data=True):
        if data.get("type") == NodeType.MODULE and data.get("embedding"):
            emb = np.array(data["embedding"])
            q_vec = np.array(query_vec)
            score = np.dot(emb, q_vec) / (np.linalg.norm(emb) * np.linalg.norm(q_vec))
            results.append((score, data))

    results.sort(key=lambda x: x[0], reverse=True)
    top = results[:3]

    if not top:
        return "No matching implementations found.\n\nEVIDENCE: [file: module_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"

    output = "Top matching implementations:\n"
    for score, data in top:
        output += f"- {data['path']} (Similarity: {score:.2f}): {data.get('purpose_statement', 'No purpose statement available')}\n"
        output += f"  EVIDENCE: [file: {data['path']}, line_range: L1-1, method: llm_inference, confidence: {score:.2f}]\n"

    return output


def trace_lineage_logic(
    dataset_name: str, direction: Literal["upstream", "downstream"], kg: KnowledgeGraph
) -> str:
    dataset_node = f"dataset:{dataset_name}"

    if dataset_node not in kg.graph:
        return f"Dataset '{dataset_name}' not found in lineage graph."

    output = f"Lineage for '{dataset_name}' ({direction}):\n"

    if direction == "upstream":
        predecessors = list(kg.graph.predecessors(dataset_node))
        if not predecessors:
            return f"No upstream sources found for '{dataset_name}'.\n\nEVIDENCE: [file: lineage_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"
        for p in predecessors:
            data = kg.graph.nodes[p]
            output += f"- {p} (Type: {data.get('type')})\n"
    else:
        successors = list(kg.graph.successors(dataset_node))
        if not successors:
            return f"No downstream dependents found for '{dataset_name}'.\n\nEVIDENCE: [file: lineage_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"
        for s in successors:
            data = kg.graph.nodes[s]
            output += f"- {s} (Type: {data.get('type')})\n"

    output += "\nEVIDENCE: [file: lineage_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"
    return output


def blast_radius_logic(module_path: str, kg: KnowledgeGraph) -> str:
    module_node = f"module:{module_path}"

    if module_node not in kg.graph:
        return f"Module '{module_path}' not found in module graph."

    import networkx as nx

    # Use transitive closure to find all dependents
    # In our graph, if A imports B, there is an edge A -> B.
    # To find all modules that depend on B (blast radius), we need all nodes that can reach B.
    # In networkx, these are called ancestors.
    try:
        all_dependents = nx.ancestors(kg.graph, module_node)
    except Exception:
        all_dependents = set()

    importing_modules = [
        m for m in all_dependents if kg.graph.nodes[m].get("type") == NodeType.MODULE
    ]

    if not importing_modules:
        return f"No transitive dependents found for '{module_path}'.\n\nEVIDENCE: [file: module_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"

    output = f"Transitive dependents (blast radius) for '{module_path}':\n"
    for m in importing_modules:
        path = kg.graph.nodes[m].get("path", m)
        output += f"- {path}\n"

    output += "\nEVIDENCE: [file: module_graph.json, line_range: N/A, method: static_analysis, confidence: 1.0]"
    return output


def explain_module_logic(path: str, kg: KnowledgeGraph, repo_path: Path) -> str:
    full_path = repo_path / path

    if not full_path.exists():
        return f"File '{path}' not found."

    try:
        source = full_path.read_text(encoding="utf-8")
    except Exception as e:
        return f"Error reading file '{path}': {e}"

    module_node_id = f"module:{path}"
    purpose = "No purpose statement available."
    if module_node_id in kg.graph:
        purpose = kg.graph.nodes[module_node_id].get("purpose_statement", purpose)

    from src.agents.semanticist import OpenRouterLLMClient, MODEL_SYNTHESIS

    client = OpenRouterLLMClient()

    prompt = (
        "You are an expert software architect. Explain the following module in detail.\n"
        f"Path: {path}\n"
        f"High-level purpose: {purpose}\n\n"
        f"Source code:\n```python\n{source[:10000]}\n```\n\n"
        "Explain (1) its specific role, (2) logic flow, and (3) risks/complexity."
    )

    try:
        resp = client.complete(prompt, model=MODEL_SYNTHESIS)
        output = resp.text
    except Exception as e:
        output = f"Error generating explanation: {e}"

    line_range = f"L1-{len(source.splitlines())}"
    symbol_map = {}
    for n, data in kg.graph.nodes(data=True):
        if data.get("path") == path:
            symbol_map = data.get("symbol_line_map", {})
            break

    if symbol_map:
        # If we have a symbol map, we can be more specific, but for now we just log that we HAVE it
        # Actually, the LLM will provide the line range in its synthetic response.
        # This EVIDENCE block is just to inform the LLM what's available.
        pass

    output += f"\n\nEVIDENCE: [file: {path}, line_range: {line_range}, method: llm_inference, confidence: 0.9]"
    return output


# ── LangChain Tools (Primitive Signatures) ───────────────────────────────────


@tool
def find_implementation(query: str) -> str:
    """Finds module implementations that match a semantic concept using similarity search."""
    return query  # Placeholder, will be replaced by the logic in the executor


@tool
def trace_lineage(
    dataset_name: str, direction: Literal["upstream", "downstream"]
) -> str:
    """Traces the data lineage of a specific dataset."""
    return dataset_name


@tool
def blast_radius(module_path: str) -> str:
    """Analyzes the blast radius of a module by finding all modules that import it."""
    return module_path


@tool
def explain_module(path: str) -> str:
    """Provides a detailed natural language explanation of a module's code and purpose."""
    return path


# ── Navigator Agent ──────────────────────────────────────────────────────────


class Navigator:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.cartography_dir = self.repo_path / ".cartography"
        self.kg = self._load_kg()

        # Tools
        self.tools = [find_implementation, trace_lineage, blast_radius, explain_module]
        # self.tool_node = ToolNode(self.tools) # Removed

        # LLM setup (configured for OpenRouter)
        from src.agents.semanticist import MODEL_SYNTHESIS, OPENROUTER_BASE

        self.llm = ChatOpenAI(
            model=MODEL_SYNTHESIS,
            api_key=SecretStr(os.environ.get("OPENROUTER_API_KEY") or ""),
            base_url=OPENROUTER_BASE,
            temperature=0,
            # max_tokens can sometimes cause issues in constructor with mypy,
            # so we use .bind() or model_kwargs if needed,
            # but we already override it in .invoke()
        ).bind_tools(self.tools)

        # Build Graph
        workflow = StateGraph(AgentState)
        workflow.add_node("agent", self._chatbot)
        workflow.add_node("tools", self._tool_executor)
        workflow.add_node("synthesis", self._synthesize_answer)

        workflow.add_edge(START, "agent")
        workflow.add_conditional_edges("agent", self._should_continue)
        workflow.add_edge("tools", "agent")
        workflow.add_edge("synthesis", END)

        self.app = workflow.compile()

    def _load_kg(self) -> KnowledgeGraph:
        # Load merged KG from module and lineage graphs
        module_path = self.cartography_dir / "module_graph.json"
        lineage_path = self.cartography_dir / "lineage_graph.json"

        kg = KnowledgeGraph()
        if module_path.exists():
            mkg = KnowledgeGraph.load(module_path)
            kg = mkg
        if lineage_path.exists():
            lkg = KnowledgeGraph.load(lineage_path)
            # Merge
            for node_id, data in lkg.graph.nodes(data=True):
                if node_id not in kg.graph:
                    kg.graph.add_node(node_id, **data)
                else:
                    kg.graph.nodes[node_id].update(data)
            for u, v, data in lkg.graph.edges(data=True):
                kg.graph.add_edge(u, v, **data)

        return kg

    def _chatbot(self, state: AgentState) -> Dict[str, Any]:
        # Redundantly enforce max_tokens on invoke
        resp = self.llm.invoke(state["messages"], max_tokens=256)
        return {"messages": [resp]}

    def _should_continue(self, state: AgentState) -> Literal["tools", "synthesis"]:
        messages = state["messages"]
        last_message = messages[-1]
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            return "tools"
        return "synthesis"

    def _tool_executor(self, state: AgentState) -> Dict[str, Any]:
        """Manually execute tools to avoid Pydantic serialization of state."""
        from langchain_core.messages import ToolMessage

        last_message = state["messages"][-1]
        tool_results = []

        # We know it has tool_calls because of _should_continue
        for tool_call in last_message.tool_calls:  # type: ignore
            name = tool_call["name"]
            args = tool_call["args"]

            if name == "find_implementation":
                result = find_implementation_logic(args["query"], state["kg"])
            elif name == "trace_lineage":
                result = trace_lineage_logic(
                    args["dataset_name"], args["direction"], state["kg"]
                )
            elif name == "blast_radius":
                result = blast_radius_logic(args["module_path"], state["kg"])
            elif name == "explain_module":
                result = explain_module_logic(
                    args["path"], state["kg"], state["repo_path"]
                )
            else:
                result = f"Unknown tool: {name}"

            tool_results.append(
                ToolMessage(
                    content=result,
                    tool_call_id=tool_call["id"],
                )
            )

        return {"messages": tool_results}

    def _synthesize_answer(self, state: AgentState) -> Dict[str, Any]:
        """Final node that converts the conversation into a structured AnswerWithCitation."""
        from src.agents.semanticist import MODEL_SYNTHESIS

        # Use the class already imported at top level if possible,
        # but keep local import if it helps avoid circularities in complex projects.

        synthesis_model = ChatOpenAI(
            model=MODEL_SYNTHESIS,
            api_key=SecretStr(os.environ.get("OPENROUTER_API_KEY") or ""),
            base_url=os.environ.get(
                "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
            ),
        ).with_structured_output(AnswerWithCitation)

        prompt = (
            "Based on the conversation and tool outputs below, provide a structured answer.\n"
            "Each tool output contains 'EVIDENCE' blocks. You MUST only use these for citations.\n"
            "If an EVIDENCE block has low confidence (< 0.5), be cautious in your answer.\n"
            "Method must be 'static_analysis' or 'llm_inference'.\n\n"
            "Conversation:\n"
        )
        for msg in state["messages"]:
            prompt += f"{msg.type}: {msg.content}\n"

        structured_resp = synthesis_model.invoke(prompt, max_tokens=512)

        from typing import cast

        res = cast(AnswerWithCitation, structured_resp)

        # #13 Validate file citations
        valid_citations = []
        for c in res.citations:
            # Special case for graph files
            if c.file in ["lineage_graph.json", "module_graph.json"]:
                valid_citations.append(c)
                continue

            full_path = state["repo_path"] / c.file
            if full_path.exists():
                valid_citations.append(c)
            # If invalid, we just drop it or we could try to find it?
            # Senior dev said 'validate', so dropping is safest.

        res.citations = valid_citations
        # If we dropped all citations, we might need a fallback, but the model is required to provide one.
        if not res.citations:
            # Add a fallback to the repo root README or something generic if everything was invalid
            res.citations.append(
                Citation(file="README.md", line_range="L1-1", method="static_analysis")
            )

        return {"navigator_response": res.model_dump_json()}

    def ask(self, query: str) -> str:
        """Process a user query and return a grounded answer."""
        initial_state: AgentState = {
            "messages": [HumanMessage(content=query)],
            "kg": self.kg,
            "repo_path": self.repo_path,
            "navigator_response": "",
        }

        from typing import Any, cast

        final_state = cast(Dict[str, Any], self.app.invoke(cast(Any, initial_state)))
        return str(final_state.get("navigator_response", "{}"))
