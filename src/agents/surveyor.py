from __future__ import annotations

import os
import subprocess
from collections import Counter
from pathlib import Path
from typing import Dict, List

import networkx as nx

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import Edge, EdgeType, ModuleNode, WarningRecord, WarningSeverity


class Surveyor:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.analyzer = TreeSitterAnalyzer()
        self.kg = KnowledgeGraph()

    def extract_git_velocity(self, days: int = 30) -> Dict[str, int]:
        """Runs git log to compute change frequency per file."""
        cmd = [
            "git",
            "-C",
            str(self.repo_path),
            "log",
            f"--since={days}.days.ago",
            "--name-only",
            "--pretty=format:",
        ]
        try:
            output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            return {}

        velocity: Dict[str, int] = {}
        for line in output.split("\n"):
            line = line.strip()
            if line:
                velocity[line] = velocity.get(line, 0) + 1
        return velocity

    def run(self, files_to_process: set[Path] | None = None) -> KnowledgeGraph:
        """Scan the repo, build module nodes, and establish import edges.
        If files_to_process is given, only generate nodes/edges for those files.
        """
        velocity = self.extract_git_velocity()
        high_velocity_files = self._identify_high_velocity_files(velocity)

        py_files: List[Path] = []
        sql_files: List[Path] = []
        yaml_files: List[Path] = []
        for root, _, files in os.walk(self.repo_path):
            if ".venv" in root or ".git" in root or "tests" in root:
                continue
            for file in files:
                full_path = Path(root) / file
                if files_to_process is not None and full_path not in files_to_process:
                    continue

                if file.endswith(".py"):
                    py_files.append(full_path)
                elif file.endswith(".sql"):
                    sql_files.append(full_path)
                elif file.endswith(".yml") or file.endswith(".yaml"):
                    yaml_files.append(full_path)

        module_index = self._build_module_index(py_files)
        imports_by_module: Dict[str, List[str]] = {}
        public_symbol_counts: Dict[str, int] = {}

        for filepath in py_files:
            rel_path = str(filepath.relative_to(self.repo_path))
            try:
                with open(filepath, "rb") as f:
                    content = f.read()
            except OSError as exc:
                self.kg.add_warning(
                    WarningRecord(
                        code="READ_ERROR",
                        message=str(exc),
                        file=rel_path,
                        analyzer="Surveyor",
                        severity=WarningSeverity.ERROR,
                    )
                )
                continue
            line_range = _line_range_from_bytes(content)

            analysis = self.analyzer.analyze_python_file(rel_path, content)
            for warning in analysis.warnings:
                self.kg.add_warning(
                    WarningRecord(
                        code="PARSE_WARNING",
                        message=warning,
                        file=rel_path,
                        analyzer="TreeSitterAnalyzer",
                        severity=WarningSeverity.WARNING,
                    )
                )

            resolved_imports = self.analyzer.resolve_imports(
                rel_path, analysis.imports, module_index
            )
            public_symbols = [
                f.name for f in analysis.functions if not f.name.startswith("_")
            ] + [c.name for c in analysis.classes if not c.name.startswith("_")]
            imports_by_module[rel_path] = resolved_imports
            public_symbol_counts[f"module:{rel_path}"] = len(public_symbols)

            from src.models.schema import FunctionNode

            for func in analysis.functions:
                fn_node = FunctionNode(
                    id=f"function:{rel_path}::{func.name}",
                    qualified_name=f"{rel_path}::{func.name}",
                    parent_module=f"module:{rel_path}",
                    signature=func.signature,
                    is_public_api=not func.name.startswith("_"),
                )
                self.kg.add_node(fn_node)
                self.kg.add_edge(
                    Edge(
                        source=f"module:{rel_path}",
                        target=fn_node.id,
                        type=EdgeType.CALLS,
                        weight=1,
                    )
                )

            node = ModuleNode(
                id=f"module:{rel_path}",
                path=rel_path,
                language="python",
                change_velocity_30d=velocity.get(rel_path, 0),
                complexity_score=self._compute_cyclomatic_complexity(content),
                is_dead_code_candidate=False,
                line_range=line_range,
            )
            self.kg.add_node(node)
            if node.id in self.kg.graph.nodes:
                if rel_path in high_velocity_files:
                    self.kg.graph.nodes[node.id]["high_velocity"] = True

        # Add SQL and YAML files as lightweight module nodes
        for filepath in sql_files:
            rel_path = str(filepath.relative_to(self.repo_path))
            line_range = _line_range_from_text_file(filepath)
            node = ModuleNode(
                id=f"module:{rel_path}",
                path=rel_path,
                language="sql",
                change_velocity_30d=velocity.get(rel_path, 0),
                complexity_score=0,
                is_dead_code_candidate=False,
                line_range=line_range,
            )
            self.kg.add_node(node)
            if node.id in self.kg.graph.nodes:
                if rel_path in high_velocity_files:
                    self.kg.graph.nodes[node.id]["high_velocity"] = True

        for filepath in yaml_files:
            rel_path = str(filepath.relative_to(self.repo_path))
            line_range = _line_range_from_text_file(filepath)
            node = ModuleNode(
                id=f"module:{rel_path}",
                path=rel_path,
                language="yaml",
                change_velocity_30d=velocity.get(rel_path, 0),
                complexity_score=0,
                is_dead_code_candidate=False,
                line_range=line_range,
            )
            self.kg.add_node(node)
            if node.id in self.kg.graph.nodes:
                if rel_path in high_velocity_files:
                    self.kg.graph.nodes[node.id]["high_velocity"] = True

        edge_counts: Counter[tuple[str, str]] = Counter()
        for source_path, targets in imports_by_module.items():
            for target_path in targets:
                if source_path == target_path:
                    continue
                edge_counts[(source_path, target_path)] += 1

        for (source_path, target_path), count in edge_counts.items():
            self.kg.add_edge(
                Edge(
                    source=f"module:{source_path}",
                    target=f"module:{target_path}",
                    type=EdgeType.IMPORTS,
                    weight=count,
                )
            )

        self.compute_graph_metrics(public_symbol_counts)

        return self.kg

    def compute_graph_metrics(self, public_symbol_counts: Dict[str, int]) -> None:
        """Computes PageRank and strongly connected components."""
        if self.kg.graph.number_of_nodes() == 0:
            return

        try:
            pagerank = nx.pagerank(self.kg.graph, weight="weight")
        except ModuleNotFoundError:
            pagerank = self._pagerank_fallback(self.kg.graph)
        nx.set_node_attributes(self.kg.graph, pagerank, "pagerank")

        scc_components = list(nx.strongly_connected_components(self.kg.graph))
        scc_id_map: Dict[str, int] = {}
        for index, component in enumerate(scc_components):
            if len(component) <= 1:
                continue
            for node_id in component:
                scc_id_map[str(node_id)] = index
        nx.set_node_attributes(self.kg.graph, scc_id_map, "scc_id")

        for node_id in self.kg.graph.nodes:
            node_key = str(node_id)
            in_degree = self.kg.graph.in_degree(node_id)
            has_public_symbols = public_symbol_counts.get(node_key, 0) > 0
            self.kg.graph.nodes[node_id]["is_dead_code_candidate"] = (
                has_public_symbols and in_degree == 0
            )

    def _build_module_index(self, py_files: List[Path]) -> Dict[str, str]:
        module_index: Dict[str, str] = {}
        for path in py_files:
            rel = str(path.relative_to(self.repo_path))
            module_name = Surveyor._module_name_from_path(rel)
            module_index[module_name] = rel
        return module_index

    @staticmethod
    def _module_name_from_path(path: str) -> str:
        normalized = path.replace(os.sep, "/")
        if normalized.endswith(".py"):
            normalized = normalized[:-3]
        dotted = normalized.replace("/", ".")
        if dotted.endswith(".__init__"):
            return dotted[: -len(".__init__")]
        return dotted

    @staticmethod
    def _pagerank_fallback(
        graph: nx.DiGraph[str],
        damping: float = 0.85,
        max_iter: int = 100,
        tol: float = 1e-6,
    ) -> Dict[str, float]:
        nodes = list(graph.nodes())
        n_nodes = len(nodes)
        if n_nodes == 0:
            return {}

        ranks = {str(node): 1.0 / n_nodes for node in nodes}
        base = (1.0 - damping) / n_nodes

        for _ in range(max_iter):
            previous = ranks.copy()
            max_delta = 0.0

            for node in nodes:
                node_key = str(node)
                rank_sum = 0.0
                for predecessor in graph.predecessors(node):
                    predecessor_key = str(predecessor)
                    out_degree = graph.out_degree(predecessor)
                    if out_degree == 0:
                        rank_sum += previous[predecessor_key] / n_nodes
                    else:
                        rank_sum += previous[predecessor_key] / out_degree

                ranks[node_key] = base + damping * rank_sum
                max_delta = max(max_delta, abs(ranks[node_key] - previous[node_key]))

            if max_delta < tol:
                break

        return ranks

    @staticmethod
    def _compute_cyclomatic_complexity(content: bytes) -> int:
        """McCabe complexity: 1 + number of branch points."""
        import ast

        try:
            tree = ast.parse(content)
        except SyntaxError:
            return 1
        count = 1
        for node in ast.walk(tree):
            if isinstance(
                node,
                (
                    ast.If,
                    ast.While,
                    ast.For,
                    ast.ExceptHandler,
                    ast.With,
                    ast.Assert,
                    ast.comprehension,
                ),
            ):
                count += 1
            elif isinstance(node, ast.BoolOp):
                count += len(node.values) - 1
        return count

    @staticmethod
    def _identify_high_velocity_files(velocity: Dict[str, int]) -> set[str]:
        """Return the top 20% highest-velocity files (80/20 rule)."""
        if not velocity:
            return set()
        ordered = sorted(velocity.items(), key=lambda kv: kv[1], reverse=True)
        cutoff = max(1, int(len(ordered) * 0.2))
        return {path for path, _ in ordered[:cutoff]}


def _line_range_from_bytes(content: bytes) -> str:
    """Return a line range like L1-LN for file content."""
    if not content:
        return "L1-1"
    lines = content.count(b"\n") + 1
    return f"L1-L{lines}"


def _line_range_from_text_file(path: Path) -> str:
    """Return a line range like L1-LN for a text file path."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "L1-1"
    lines = text.count("\n") + 1 if text else 1
    return f"L1-L{lines}"
