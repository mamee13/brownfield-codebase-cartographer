from pathlib import Path

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor


class Orchestrator:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.cartography_dir = self.repo_path / ".cartography"
        self.cartography_dir.mkdir(parents=True, exist_ok=True)

    def analyze(self) -> None:
        # Run Surveyor → module graph
        surveyor = Surveyor(str(self.repo_path))
        module_kg = surveyor.run()
        module_kg.save(self.cartography_dir / "module_graph.json")

        # Run Hydrologist → lineage graph
        hydrologist = Hydrologist(str(self.repo_path))
        lineage_kg = hydrologist.run()
        lineage_kg.save(self.cartography_dir / "lineage_graph.json")
