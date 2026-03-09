from __future__ import annotations

import ast
import os
import subprocess
from collections import Counter
from pathlib import Path
from typing import Dict, List

import networkx as nx

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import Edge, EdgeType, ModuleNode


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

    def run(self) -> KnowledgeGraph:
        """Scan the repo, build module nodes, and establish import edges."""
        velocity = self.extract_git_velocity()

        py_files: List[Path] = []
        for root, _, files in os.walk(self.repo_path):
            if ".venv" in root or ".git" in root or "tests" in root:
                continue
            for file in files:
                if file.endswith(".py"):
                    py_files.append(Path(root) / file)

        module_index = self._build_module_index(py_files)
        imports_by_module: Dict[str, List[str]] = {}
        public_symbol_counts: Dict[str, int] = {}

        for filepath in py_files:
            rel_path = str(filepath.relative_to(self.repo_path))
            with open(filepath, "rb") as f:
                content = f.read()

            imports, public_symbols = self.analyzer.analyze_python_file(
                rel_path, content
            )
            resolved_imports = self._resolve_imports(rel_path, imports, module_index)
            imports_by_module[rel_path] = resolved_imports
            public_symbol_counts[rel_path] = len(public_symbols)

            node = ModuleNode(
                id=rel_path,
                path=rel_path,
                language="python",
                change_velocity_30d=velocity.get(rel_path, 0),
                complexity_score=len(public_symbols),  # basic heuristic for now
                is_dead_code_candidate=False,
            )
            self.kg.add_node(node)

        edge_counts: Counter[tuple[str, str]] = Counter()
        for source_path, targets in imports_by_module.items():
            for target_path in targets:
                if source_path == target_path:
                    continue
                edge_counts[(source_path, target_path)] += 1

        for (source_path, target_path), count in edge_counts.items():
            self.kg.add_edge(
                Edge(
                    source=source_path,
                    target=target_path,
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

    def _resolve_imports(
        self, rel_path: str, imports: List[str], module_index: Dict[str, str]
    ) -> List[str]:
        module_name = self._module_name_from_path(rel_path)
        if rel_path.endswith("__init__.py"):
            current_package = module_name
        else:
            current_package = (
                module_name.rsplit(".", 1)[0] if "." in module_name else ""
            )

        resolved: set[str] = set()
        for import_stmt in imports:
            candidates = self._candidate_modules(import_stmt, current_package)
            for module_candidate in candidates:
                target = self._resolve_module_to_path(module_candidate, module_index)
                if target is not None:
                    resolved.add(target)

        return sorted(resolved)

    @staticmethod
    def _candidate_modules(import_stmt: str, current_package: str) -> List[str]:
        try:
            tree = ast.parse(import_stmt)
        except SyntaxError:
            return []

        if len(tree.body) != 1:
            return []
        stmt = tree.body[0]
        candidates: List[str] = []

        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                candidates.append(alias.name)
            return candidates

        if isinstance(stmt, ast.ImportFrom):
            base_module = Surveyor._resolve_relative_module(
                current_package=current_package, level=stmt.level, module=stmt.module
            )
            if base_module:
                candidates.append(base_module)
                for alias in stmt.names:
                    if alias.name == "*":
                        continue
                    candidates.append(f"{base_module}.{alias.name}")
            return candidates

        return candidates

    @staticmethod
    def _resolve_relative_module(
        current_package: str, level: int, module: str | None
    ) -> str | None:
        if level == 0:
            return module

        if not current_package:
            return module

        package_parts = current_package.split(".")
        go_up = level - 1
        if go_up > len(package_parts):
            return module

        anchor = package_parts[: len(package_parts) - go_up]
        if module:
            return ".".join(anchor + [module])
        return ".".join(anchor)

    @staticmethod
    def _resolve_module_to_path(
        module_name: str, module_index: Dict[str, str]
    ) -> str | None:
        if module_name in module_index:
            return module_index[module_name]

        parts = module_name.split(".")
        while len(parts) > 1:
            parts = parts[:-1]
            candidate = ".".join(parts)
            if candidate in module_index:
                return module_index[candidate]

        return None

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
