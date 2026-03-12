from pathlib import Path

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor
from src.graph.knowledge_graph import KnowledgeGraph


class Orchestrator:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = Path(repo_path).resolve()
        self.cartography_dir = self.repo_path / ".cartography"
        self.cartography_dir.mkdir(parents=True, exist_ok=True)

    def analyze(self, incremental: bool = False) -> None:
        from src.state_tracker import FileStateTracker

        tracker = FileStateTracker(self.cartography_dir)

        files_to_process = None
        if incremental:
            files_to_process = tracker.get_changed_files(self.repo_path)
            print(f"Incremental mode: {len(files_to_process)} files changed.")
            if not files_to_process:
                print("No changes detected. Analysis skipped.")
                return

        # Run Surveyor → module graph
        print("Running Surveyor (module graph)...")
        surveyor = Surveyor(str(self.repo_path))
        module_kg = surveyor.run(files_to_process=files_to_process)

        if incremental and (self.cartography_dir / "module_graph.json").exists():
            base_module_kg = KnowledgeGraph.load(
                self.cartography_dir / "module_graph.json"
            )
            module_kg = self._merge_knowledge_graphs(base_module_kg, module_kg)

        module_kg.save(self.cartography_dir / "module_graph.json")

        # Run Hydrologist → lineage graph
        print("Running Hydrologist (lineage graph)...")
        hydrologist = Hydrologist(str(self.repo_path))
        lineage_kg = hydrologist.run(files_to_process=files_to_process)

        if incremental and (self.cartography_dir / "lineage_graph.json").exists():
            base_lineage_kg = KnowledgeGraph.load(
                self.cartography_dir / "lineage_graph.json"
            )
            lineage_kg = self._merge_knowledge_graphs(base_lineage_kg, lineage_kg)

        lineage_kg.save(self.cartography_dir / "lineage_graph.json")

        # Merge graphs so Semanticist/Archivist can see both modules + lineage
        merged_kg = self._merge_knowledge_graphs(module_kg, lineage_kg)

        # Combine nodes for Semanticist
        # We'll enrich the module_kg in place, but we need Hydrologist lineage
        # data to answer Day One Qs.
        from src.agents.semanticist import OpenRouterLLMClient, Semanticist
        from src.agents.archivist import Archivist
        import networkx as nx

        client = OpenRouterLLMClient()
        semanticist = Semanticist(client=client)

        # Build source map for Semanticist: module_path -> source code
        source_map = {}
        for _, data in merged_kg.graph.nodes(data=True):
            if data.get("type") == "module":
                path = data.get("path")
                if path:
                    full_path = self.repo_path / path
                    # In incremental mode, skip generating source maps for unchanged files
                    # unless they are totally missing a purpose statement for some reason
                    has_purpose = bool(data.get("purpose_statement"))
                    if incremental and files_to_process is not None:
                        if full_path not in files_to_process and has_purpose:
                            continue

                    if full_path.exists():
                        try:
                            source_map[path] = full_path.read_text(encoding="utf-8")
                        except Exception:
                            pass

        # Calculate PR for top 5 (optional, Semanticist can take it)
        try:
            pr = nx.pagerank(merged_kg.graph, weight=None)
            top5 = sorted(
                [
                    n
                    for n in merged_kg.graph.nodes
                    if merged_kg.graph.nodes[n].get("type") == "module"
                ],
                key=lambda x: pr.get(x, 0.0),
                reverse=True,
            )[:5]
        except Exception:
            top5 = []

        semanticist.run(
            merged_kg,
            source_map=source_map,
            find_sources_fn=hydrologist.find_sources,
            find_sinks_fn=hydrologist.find_sinks,
            pagerank_top5=top5,
        )

        # Run Archivist to generate artifacts from enriched KG
        archivist = Archivist(output_dir=self.cartography_dir)
        archivist.run(merged_kg)

        # Save the ENRICHED graphs back to disk
        merged_kg.save(self.cartography_dir / "module_graph.json")
        merged_kg.save(self.cartography_dir / "lineage_graph.json")

        # Save the new file state tracker so the next run knows what we processed
        if incremental or (files_to_process is None):
            # If it's a completely fresh full run, we should probably save state too
            if not incremental:
                tracker.get_changed_files(self.repo_path)
            tracker.save_state()

        print(f"Analysis complete. Artifacts written to {self.cartography_dir}")

    def _merge_knowledge_graphs(
        self, base: KnowledgeGraph, other: KnowledgeGraph
    ) -> KnowledgeGraph:
        """
        Merge nodes/edges/warnings from `other` into `base` (in-place) and return base.
        Existing node attributes are preserved; missing attributes are filled from `other`.
        """
        for node_id, data in other.graph.nodes(data=True):
            if node_id not in base.graph.nodes:
                base.graph.add_node(node_id, **data)
            else:
                base.graph.nodes[node_id].update(
                    {
                        k: v
                        for k, v in data.items()
                        if k not in base.graph.nodes[node_id]
                    }
                )

        for src, dst, data in other.graph.edges(data=True):
            if base.graph.has_edge(src, dst):
                base.graph.edges[src, dst].update(data)
            else:
                base.graph.add_edge(src, dst, **data)

        base.warnings.extend(other.warnings)
        return base
