from pathlib import Path

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor
from src.models.schema import WarningRecord, WarningSeverity


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

        # Combine nodes for Semanticist
        # We'll enrich the module_kg in place, but we need Hydrologist lineage
        # data to answer Day One Qs.
        from src.agents.semanticist import OpenRouterLLMClient, Semanticist
        from src.agents.archivist import Archivist
        import networkx as nx

        try:
            client = OpenRouterLLMClient()
            semanticist = Semanticist(client=client)
        except Exception as exc:
            module_kg.add_warning(
                WarningRecord(
                    code="LLM_ERROR",
                    message=str(exc),
                    analyzer="Orchestrator",
                    severity=WarningSeverity.ERROR,
                )
            )
            semanticist = None

        # Build source map for Semanticist: module_path -> source code
        source_map = {}
        for _, data in module_kg.graph.nodes(data=True):
            if data.get("type") == "module":
                path = data.get("path")
                if path:
                    full_path = self.repo_path / path
                    if full_path.exists():
                        try:
                            source_map[path] = full_path.read_text(encoding="utf-8")
                        except Exception:
                            pass

        # Calculate PR for top 5 (optional, Semanticist can take it)
        try:
            pr = nx.pagerank(module_kg.graph, weight=None)
            top5 = sorted(
                [
                    n
                    for n in module_kg.graph.nodes
                    if module_kg.graph.nodes[n].get("type") == "module"
                ],
                key=lambda x: pr.get(x, 0.0),
                reverse=True,
            )[:5]
        except Exception:
            top5 = []

        if semanticist is not None:
            semanticist.run(
                module_kg,
                source_map=source_map,
                find_sources_fn=hydrologist.find_sources,
                find_sinks_fn=hydrologist.find_sinks,
                pagerank_top5=top5,
            )

        # Run Archivist to generate artifacts from enriched KG
        archivist = Archivist(output_dir=self.cartography_dir)
        archivist.run(module_kg)

        print(f"Analysis complete. Artifacts written to {self.cartography_dir}")
