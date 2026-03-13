"""
Archivist Agent — Day 3

Produces all living artifact outputs from the enriched KnowledgeGraph:
  - .cartography/CODEBASE.md          (6 required sections)
  - .cartography/onboarding_brief.md  (5 FDE Day-One Q&A sections)
  - .cartography/cartography_trace.jsonl  (JSONL audit log)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import ModuleNode, TraceEntry

_CARTOGRAPHY_DIR = ".cartography"

_FDE_QUESTIONS = [
    "What is the primary data ingestion path?",
    "What are the 3-5 most critical output datasets or endpoints?",
    "What is the blast radius if the most critical module fails?",
    "Where is the business logic concentrated versus distributed?",
    "What has changed most frequently in the last 90 days?",
]


class Archivist:
    """Produces artifact files from an enriched KnowledgeGraph."""

    def __init__(self, output_dir: Optional[Path] = None) -> None:
        self._out = output_dir or Path(_CARTOGRAPHY_DIR)

    # ── CODEBASE.md ───────────────────────────────────────────────────────────

    def generate_codebase_md(
        self, kg: KnowledgeGraph, output_path: Optional[Path] = None
    ) -> str:
        """
        Generate CODEBASE.md with 6 required H2 sections.
        Every fact line includes an inline (source: ...) citation tag.
        Returns the full markdown string.
        """
        out_path = output_path or self._out / "CODEBASE.md"
        sections: List[str] = []

        # 1. Architecture Overview
        q1_answer = ""
        if kg.day_one_answers.get("Q1"):
            q1_answer = kg.day_one_answers["Q1"].answer

        if not q1_answer:
            module_count = sum(
                1
                for _, data in kg.graph.nodes(data=True)
                if data.get("type") == "module"
            )
            top_modules = sorted(
                [
                    (data.get("path", "?"), data.get("change_velocity_30d", 0))
                    for _, data in kg.graph.nodes(data=True)
                    if data.get("type") == "module"
                ],
                key=lambda x: x[1],
                reverse=True,
            )[:3]
            q1_answer = (
                f"Static analysis identified {module_count} modules. "
                "The core architecture spans these high-velocity files: "
                f"{', '.join(p for p, _ in top_modules)}. "
                "Run with LLM enabled for a synthesized structural overview."
            )
        sections.append(
            "## Architecture Overview\n\n"
            + (q1_answer)
            + "\n\n"
            + "(source: llm_inference)"
        )

        # 2. Critical Path — top 5 module nodes by PageRank
        module_nodes = self._get_module_nodes(kg)
        try:
            import networkx as nx

            pr: Dict[Any, float] = nx.pagerank(kg.graph, weight=None)
            top5 = sorted(
                [
                    (n, pr.get(n, 0.0))
                    for n in kg.graph.nodes
                    if kg.graph.nodes[n].get("type") == "module"
                ],
                key=lambda x: x[1],
                reverse=True,
            )[:5]
        except Exception:
            top5 = [(m.id, 0.0) for m in module_nodes[:5]]

        cp_lines: List[str] = []
        for node_id, score in top5:
            data = kg.graph.nodes[node_id]
            path = data.get("path", node_id)
            purpose = data.get("purpose_statement", "_no purpose statement_")
            vel = data.get("change_velocity_30d", "?")
            cp_lines.append(
                f"- **{path}** (score={score:.4f}, velocity={vel}): {purpose}  "
                f"(source: static_analysis, file: {path}, line: L1-1)"
            )
        sections.append(
            "## Critical Path\n\n"
            + ("\n".join(cp_lines) or "_No module data available._")
        )

        # 3. Data Sources & Sinks
        source_ids = [n for n in kg.graph.nodes if kg.graph.in_degree(n) == 0]
        sink_ids = [n for n in kg.graph.nodes if kg.graph.out_degree(n) == 0]
        src_lines = [
            f"- {kg.graph.nodes[n].get('name', n)} (source: static_analysis, file: {kg.graph.nodes[n].get('source_file', 'unknown')}, line: L1-1)"
            for n in source_ids
            if kg.graph.nodes[n].get("type") == "dataset"
        ]
        sink_lines = [
            f"- {kg.graph.nodes[n].get('name', n)} (source: static_analysis, file: {kg.graph.nodes[n].get('source_file', 'unknown')}, line: L1-1)"
            for n in sink_ids
            if kg.graph.nodes[n].get("type") == "dataset"
        ]
        sections.append(
            "## Data Sources & Sinks\n\n"
            "### Sources\n" + ("\n".join(src_lines) or "_None identified._") + "\n\n"
            "### Sinks\n" + ("\n".join(sink_lines) or "_None identified._")
        )

        # 4. Domain Map
        domain_map: Dict[str, List[str]] = kg.graph.graph.get("domain_map", {})
        if domain_map:
            rows = ["| Domain | Modules |", "|---|---|"]
            for domain, paths in domain_map.items():
                rows.append(
                    f"| {domain} | {', '.join(paths[:3])}{'...' if len(paths) > 3 else ''} |"
                )
            sections.append(
                "## Domain Map\n\n" + "\n".join(rows) + "\n(source: llm_inference)"
            )
        else:
            sections.append("## Domain Map\n\n_Domain clustering not yet run._")

        # 5. Known Debt
        debt_lines: List[str] = []
        # Circular deps from SCC
        try:
            import networkx as nx

            sccs = list(nx.strongly_connected_components(kg.graph))
            circular = [scc for scc in sccs if len(scc) > 1]
            for scc in circular:
                debt_lines.append(
                    f"- **Circular dependency**: {', '.join(sorted(scc))} (source: static_analysis)"
                )
        except Exception:
            pass
        # Doc drift warnings
        for w in kg.warnings:
            if w.code == "DOC_DRIFT":
                debt_lines.append(
                    f"- **Doc Drift** in `{w.file}`: {w.message[:120]}... (source: llm_inference)"
                )
        sections.append(
            "## Known Debt\n\n"
            + (
                "\n".join(debt_lines)
                or "_No circular dependencies or doc drift detected._"
            )
        )

        # 6. High-Velocity Files
        velocity_nodes = sorted(
            [
                (data.get("path", nid), data.get("change_velocity_30d", 0))
                for nid, data in kg.graph.nodes(data=True)
                if data.get("type") == "module"
            ],
            key=lambda x: x[1] or 0,
            reverse=True,
        )[:10]
        vel_lines = [
            f"- **{path}** — {vel} changes/30d (source: static_analysis, file: {path}, line: L1-1)"
            for path, vel in velocity_nodes
        ]
        sections.append(
            "## High-Velocity Files\n\n"
            + ("\n".join(vel_lines) or "_No git velocity data available._")
        )

        # 7. Module Purpose Index
        mpi_lines: List[str] = []
        for path, data in sorted(
            [
                (data.get("path", nid), data)
                for nid, data in kg.graph.nodes(data=True)
                if data.get("type") == "module"
            ],
            key=lambda x: x[0],
        ):
            purpose = data.get("purpose_statement", "No purpose statement generated.")
            mpi_lines.append(f"- **`{path}`**: {purpose} (source: llm_inference)")

        sections.append(
            "## Module Purpose Index\n\n"
            + ("\n".join(mpi_lines) or "_No modules found._")
        )

        content = (
            f"# CODEBASE.md\n\n"
            f"_Generated: {datetime.now(tz=timezone.utc).isoformat()}_\n\n"
            + "\n\n".join(sections)
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        return content

    # ── onboarding_brief.md ───────────────────────────────────────────────────

    def generate_onboarding_brief(
        self, kg: KnowledgeGraph, output_path: Optional[Path] = None
    ) -> str:
        """
        Generate onboarding_brief.md with 5 FDE Day-One Q&A sections.
        Each section includes question, answer, and file:path:LN-M evidence lines.
        """
        out_path = output_path or self._out / "onboarding_brief.md"

        # Count analyzed assets
        n_modules = sum(
            1 for _, d in kg.graph.nodes(data=True) if d.get("type") == "module"
        )
        n_datasets = sum(
            1 for _, d in kg.graph.nodes(data=True) if d.get("type") == "dataset"
        )
        timestamp = datetime.now(tz=timezone.utc).isoformat()

        header = (
            "# FDE Onboarding Brief\n\n"
            f"| Field | Value |\n|---|---|\n"
            f"| Generated | {timestamp} |\n"
            f"| Modules analyzed | {n_modules} |\n"
            f"| Datasets identified | {n_datasets} |\n\n"
            "---\n"
        )

        sections: List[str] = [header]
        for i, question in enumerate(_FDE_QUESTIONS, 1):
            q_key = f"Q{i}"
            answer_obj = kg.day_one_answers.get(q_key)
            if answer_obj:
                answer_text = answer_obj.answer
                evidence_lines = "\n".join(
                    f"- file:{c.file}:{c.line_range} (method: {c.method})"
                    for c in answer_obj.citations
                )
                confidence = answer_obj.confidence
            else:
                answer_text = "_Not yet answered._"
                evidence_lines = ""
                confidence = "inferred"

            sections.append(
                f"## Q{i}: {question}\n\n"
                f"**Answer** _(confidence: {confidence})_:\n{answer_text}\n\n"
                + (f"**Evidence:**\n{evidence_lines}\n" if evidence_lines else "")
            )

        content = "\n---\n".join(sections)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        return content

    # ── cartography_trace.jsonl ───────────────────────────────────────────────

    def write_trace_log(
        self, kg: KnowledgeGraph, output_path: Optional[Path] = None
    ) -> Path:
        """
        Write trace_entries from KnowledgeGraph to cartography_trace.jsonl.
        Each line is a valid JSON object (JSONL format).
        """
        out_path = output_path or self._out / "cartography_trace.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with out_path.open("w", encoding="utf-8") as f:
            for entry in kg.trace_entries:
                f.write(entry.model_dump_json() + "\n")

        return out_path

    # ── Full run ──────────────────────────────────────────────────────────────

    def run(self, kg: KnowledgeGraph) -> None:
        """
        Produce all 3 artifacts: CODEBASE.md, onboarding_brief.md, cartography_trace.jsonl.
        Also logs each write to the trace.
        """
        # Log archivist actions to trace
        kg.add_trace_entry(
            TraceEntry(
                timestamp=datetime.now(tz=timezone.utc),
                agent="Archivist",
                action="generate_codebase_md",
                evidence_source="static_analysis",
                confidence="observed",
                detail=str(self._out / "CODEBASE.md"),
            )
        )
        self.generate_codebase_md(kg)

        kg.add_trace_entry(
            TraceEntry(
                timestamp=datetime.now(tz=timezone.utc),
                agent="Archivist",
                action="generate_onboarding_brief",
                evidence_source="static_analysis",
                confidence="observed",
                detail=str(self._out / "onboarding_brief.md"),
            )
        )
        self.generate_onboarding_brief(kg)

        kg.add_trace_entry(
            TraceEntry(
                timestamp=datetime.now(tz=timezone.utc),
                agent="Archivist",
                action="write_trace_log",
                evidence_source="static_analysis",
                confidence="observed",
                detail=str(self._out / "cartography_trace.jsonl"),
            )
        )
        self.write_trace_log(kg)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_module_nodes(self, kg: KnowledgeGraph) -> List[ModuleNode]:
        nodes: List[ModuleNode] = []
        for _, data in kg.graph.nodes(data=True):
            if data.get("type") == "module":
                try:
                    nodes.append(ModuleNode.model_validate(data))
                except Exception:
                    pass
        return nodes
