from pathlib import Path
import json
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
    incremental: bool = typer.Option(
        False,
        "--incremental",
        help="Re-analyze only files that have changed since the last run.",
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

    typer.echo(f"Analyzing {target_path} (incremental={incremental})...")
    orchestrator = Orchestrator(target_path)
    orchestrator.analyze(incremental=incremental)
    typer.echo("Analysis complete. Artifacts saved to .cartography/")


@app.command()
def query(
    path: str = typer.Argument(
        ..., help="Local path to the repo (must be already analyzed)."
    ),
) -> None:
    """Start the interactive query agent."""
    target_path = Path(path).resolve()
    if not (target_path / ".cartography").exists():
        typer.echo(
            f"Error: {target_path} has not been analyzed yet. Run 'analyze' first."
        )
        raise typer.Exit(1)

    typer.echo(f"Starting Navigator for {target_path}...")
    typer.echo("Type your question below, or 'exit'/'quit' to stop.")

    from src.agents.navigator import Navigator

    navigator = Navigator(str(target_path))

    while True:
        try:
            user_input = typer.prompt("\ncartographer> ")
            if user_input.strip().lower() in ["exit", "quit", "q"]:
                break
            if not user_input.strip():
                continue

            response_json = navigator.ask(user_input)
            try:
                data = json.loads(response_json)
                typer.secho("\nAnswer:", fg=typer.colors.GREEN, bold=True)
                typer.echo(data.get("answer", "No answer provided."))

                citations = data.get("citations", [])
                if citations:
                    typer.secho("\nCitations:", fg=typer.colors.CYAN, bold=True)
                    for c in citations:
                        typer.echo(
                            f"- {c['file']}:{c['line_range']} (method: {c['method']})"
                        )

                confidence = data.get("confidence_score")
                if confidence is not None:
                    typer.echo(f"\nConfidence: {confidence:.2f}")
            except Exception:
                # Fallback if not JSON or parsing fails
                typer.echo(f"\n{response_json}")

        except typer.Abort:
            typer.echo("\nExiting Navigator.")
            break
        except Exception as e:
            typer.echo(f"Error: {e}")


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
