from pathlib import Path
from typer.testing import CliRunner
from src.cli import app

runner = CliRunner()


def test_analyze_command() -> None:
    result = runner.invoke(app, ["analyze", "test-repo"])
    assert result.exit_code == 0
    assert "Analyzing test-repo" in result.stdout


def test_query_command(tmp_path: Path) -> None:
    # Test that it errors if the path hasn't been analyzed
    repo = tmp_path / "never-analyzed"
    repo.mkdir()
    result = runner.invoke(app, ["query", str(repo)])
    assert result.exit_code == 1
    assert "Error: " in result.stdout
    assert "has not been analyzed yet" in result.stdout
