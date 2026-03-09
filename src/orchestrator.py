from pathlib import Path
from src.agents.surveyor import Surveyor


class Orchestrator:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.cartography_dir = self.repo_path / ".cartography"
        self.cartography_dir.mkdir(parents=True, exist_ok=True)

    def analyze(self) -> None:
        surveyor = Surveyor(str(self.repo_path))
        kg = surveyor.run()
        kg.save(self.cartography_dir / "module_graph.json")
