from typer.testing import CliRunner
from src.cli import app

runner = CliRunner()


def test_analyze_command() -> None:
    result = runner.invoke(app, ["analyze", "test-repo"])
    assert result.exit_code == 0
    assert "Analyzing test-repo" in result.stdout


def test_query_command() -> None:
    result = runner.invoke(app, ["query"])
    assert result.exit_code == 0
    assert "Query mode not implemented yet." in result.stdout
