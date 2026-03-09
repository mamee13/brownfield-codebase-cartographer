import os
import subprocess
from pathlib import Path
from typing import Dict, List


from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import ModuleNode


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

        # Build initial nodes and module map
        modules: Dict[str, ModuleNode] = {}
        for filepath in py_files:
            rel_path = str(filepath.relative_to(self.repo_path))
            with open(filepath, "rb") as f:
                content = f.read()

            imports, public_symbols = self.analyzer.analyze_python_file(
                rel_path, content
            )

            node = ModuleNode(
                id=rel_path,
                path=rel_path,
                language="python",
                change_velocity_30d=velocity.get(rel_path, 0),
                complexity_score=len(public_symbols),  # basic heuristic for now
            )
            modules[rel_path] = node
            self.kg.add_node(node)

        # Build edges based on simple heuristics in MVP
        # ... logic for edges ...

        self.compute_graph_metrics()

        return self.kg

    def compute_graph_metrics(self) -> None:
        """Computes PageRank and strongly connected components."""
        # For a full implementation, we'd assign PageRank to node complexity_score
        # and identify circular deps.
        pass
