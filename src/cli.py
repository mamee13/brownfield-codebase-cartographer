import typer

from src.orchestrator import Orchestrator

app = typer.Typer(help="The Brownfield Cartographer CLI")


@app.command()
def analyze(path: str) -> None:
    """Run full Cartography analysis on a codebase."""
    typer.echo(f"Analyzing {path}...")
    orchestrator = Orchestrator(path)
    orchestrator.analyze()
    typer.echo("Analysis complete. Artifacts saved to .cartography/")


@app.command()
def query() -> None:
    """Start the interactive query agent."""
    typer.echo("Query mode not implemented yet.")


if __name__ == "__main__":
    app()
