"""Tracks file modification times for incremental analysis."""

import json
from pathlib import Path
from typing import Dict, Set


class FileStateTracker:
    def __init__(self, cartography_dir: Path) -> None:
        self.state_file = cartography_dir / "file_state.json"
        self._previous_state: Dict[str, float] = self._load_state()
        self._current_state: Dict[str, float] = {}

    def _load_state(self) -> Dict[str, float]:
        if self.state_file.exists():
            try:
                from typing import cast

                data = json.loads(self.state_file.read_text(encoding="utf-8"))
                return cast(Dict[str, float], data)
            except Exception:
                return {}
        return {}

    def save_state(self) -> None:
        """Saves the observed state of the current run."""
        self.state_file.write_text(
            json.dumps(self._current_state, indent=2), encoding="utf-8"
        )

    def get_changed_files(self, repo_path: Path) -> Set[Path]:
        """
        Scans all supported files in the repo.
        Returns the set of paths that are new or modified since the last save.
        Populates _current_state with the latest mtimes.
        """
        changed: Set[Path] = set()

        # Scan Python, SQL, YAML
        for ext in ["*.py", "*.sql", "*.yml", "*.yaml"]:
            for file_path in repo_path.rglob(ext):
                # Hardcoded simple ignores to match the agents
                if any(
                    p in str(file_path)
                    for p in [".venv", ".git", "__pycache__", "tests"]
                ):
                    continue

                try:
                    mtime = file_path.stat().st_mtime
                    rel_path = str(file_path.relative_to(repo_path))
                    self._current_state[rel_path] = mtime

                    if (
                        rel_path not in self._previous_state
                        or self._previous_state[rel_path] < mtime
                    ):
                        changed.add(file_path)
                except OSError:
                    continue

        return changed
