"""Extended tests for PythonDataFlowAnalyzer."""

import pytest
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer


@pytest.fixture
def analyzer() -> PythonDataFlowAnalyzer:
    return PythonDataFlowAnalyzer()


def test_json_load(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "import json\nwith open('config.json') as f:\n    data = json.load(f)\n"
    result = analyzer.analyze(src, "test.py")
    assert any(r.api == "json.load" for r in result.reads)


def test_open_read(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "with open('data.txt', 'r') as f:\n    content = f.read()\n"
    result = analyzer.analyze(src, "test.py")
    assert any(r.name == "data.txt" for r in result.reads)


def test_pathlib_read_text(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "from pathlib import Path\ncontent = Path('README.md').read_text()\n"
    result = analyzer.analyze(src, "test.py")
    assert any(r.name == "README.md" and r.direction == "read" for r in result.reads)


def test_pathlib_write_text(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "from pathlib import Path\nPath('output.txt').write_text('hello')\n"
    result = analyzer.analyze(src, "test.py")
    assert any(r.name == "output.txt" and r.direction == "write" for r in result.writes)


def test_httpx_get(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "import httpx\nresp = httpx.get('https://api.example.com')\n"
    result = analyzer.analyze(src, "test.py")
    assert any(
        r.name == "https://api.example.com" and r.direction == "read"
        for r in result.reads
    )


def test_requests_post(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "import requests\nrequests.post(url='https://api.test.com/v1', data={})\n"
    result = analyzer.analyze(src, "test.py")
    assert any(
        r.name == "https://api.test.com/v1" and r.direction == "write"
        for r in result.writes
    )
