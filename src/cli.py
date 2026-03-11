from pathlib import Path
import subprocess
from typing import Optional

import typer

from src.orchestrator import Orchestrator

app = typer.Typer(help="The Brownfield Cartographer CLI")


@app.command()
def analyze(
    path: Optional[str] = typer.Argument(None, help="Local path to a repo to analyze."),
    repo: Optional[str] = typer.Option(
        None,
        "--repo",
        help="GitHub repo URL to clone and analyze (e.g., https://github.com/dbt-labs/jaffle_shop).",
    ),
) -> None:
    """Run full Cartography analysis on a codebase."""
    if not path and not repo:
        raise typer.BadParameter("Provide either a local PATH or --repo URL.")
    if path and repo:
        raise typer.BadParameter("Provide only one of PATH or --repo.")

    target_path = path
    if repo:
        target_path = _clone_repo(repo)
    assert target_path is not None

    typer.echo(f"Analyzing {target_path}...")
    orchestrator = Orchestrator(target_path)
    orchestrator.analyze()
    typer.echo("Analysis complete. Artifacts saved to .cartography/")


@app.command()
def query() -> None:
    """Start the interactive query agent."""
    typer.echo("Query mode not implemented yet.")


def _clone_repo(repo_url: str) -> str:
    """Clone a GitHub repo URL into a local cache dir and return the path."""
    base_dir = Path(".cartography_repos")
    base_dir.mkdir(parents=True, exist_ok=True)

    # Derive a folder name from the repo URL.
    name = repo_url.rstrip("/").split("/")[-1]
    if name.endswith(".git"):
        name = name[: -len(".git")]
    dest = base_dir / name

    if dest.exists():
        typer.echo(f"Repo already present at {dest}; reusing.")
        return str(dest)

    typer.echo(f"Cloning {repo_url} -> {dest} ...")
    subprocess.run(["git", "clone", repo_url, str(dest)], check=True)
    return str(dest)


if __name__ == "__main__":
    app()
