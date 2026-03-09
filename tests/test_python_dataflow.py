"""Tests for PythonDataFlowAnalyzer — Step 5 (Day 2)."""

import pytest
from src.analyzers.python_dataflow import PythonDataFlowAnalyzer


@pytest.fixture
def analyzer() -> PythonDataFlowAnalyzer:
    return PythonDataFlowAnalyzer()


# ── 1. pandas reads ──────────────────────────────────────────────────────────


def test_pandas_read_csv(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "import pandas as pd\ndf = pd.read_csv('data/users.csv')\n"
    result = analyzer.analyze(src, "etl.py")
    assert len(result.reads) == 1
    assert result.reads[0].name == "data/users.csv"
    assert result.reads[0].api == "pandas.read_csv"
    assert result.warnings == []


def test_pandas_read_parquet(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df = pd.read_parquet('s3://bucket/events.parquet')\n"
    result = analyzer.analyze(src, "load.py")
    assert any(r.name == "s3://bucket/events.parquet" for r in result.reads)


def test_pandas_read_sql(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df = pd.read_sql('SELECT * FROM orders', con=engine)\n"
    result = analyzer.analyze(src, "query.py")
    assert any(r.name == "SELECT * FROM orders" for r in result.reads)


# ── 2. pandas writes ──────────────────────────────────────────────────────────


def test_pandas_to_csv(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df.to_csv('output/result.csv')\n"
    result = analyzer.analyze(src, "sink.py")
    assert len(result.writes) == 1
    assert result.writes[0].name == "output/result.csv"
    assert result.writes[0].api == "pandas.to_csv"


def test_pandas_to_parquet(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df.to_parquet('gs://bucket/output.parquet')\n"
    result = analyzer.analyze(src, "save.py")
    assert any(w.name == "gs://bucket/output.parquet" for w in result.writes)


# ── 3. PySpark reads and writes ───────────────────────────────────────────────


def test_pyspark_read_parquet(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df = spark.read.parquet('/data/raw/events')\n"
    result = analyzer.analyze(src, "spark_job.py")
    assert any(r.name == "/data/raw/events" for r in result.reads)


def test_pyspark_read_table(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df = spark.read.table('warehouse.orders')\n"
    result = analyzer.analyze(src, "spark_job.py")
    assert any(r.name == "warehouse.orders" for r in result.reads)


def test_pyspark_write_parquet(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "df.write.parquet('/data/processed/events')\n"
    result = analyzer.analyze(src, "spark_job.py")
    assert any(w.name == "/data/processed/events" for w in result.writes)


# ── 4. Dynamic refs — must produce DYNAMIC_REF warnings, never silent drop ────


def test_dynamic_ref_fstring_emits_warning(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "path = 'x'\ndf = pd.read_csv(f'data/{path}.csv')\n"
    result = analyzer.analyze(src, "dynamic.py")
    # Should not appear in reads
    assert result.reads == []
    # Must emit a warning
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "DYNAMIC_REF"
    assert result.warnings[0].file == "dynamic.py"


def test_dynamic_ref_variable_emits_warning(analyzer: PythonDataFlowAnalyzer) -> None:
    src = "table = get_table_name()\ndf = spark.read.table(table)\n"
    result = analyzer.analyze(src, "dynamic2.py")
    assert result.reads == []
    assert any(w.code == "DYNAMIC_REF" for w in result.warnings)


# ── 5. Negative — avoid false positives ──────────────────────────────────────


def test_no_false_positive_on_same_name(analyzer: PythonDataFlowAnalyzer) -> None:
    # A method called 'to_csv' on a custom class should still be detected
    # but unrelated 'to_csv' string literals must not pollute reads list
    src = "result = some_object.execute('SELECT 1')\n"
    result = analyzer.analyze(src, "misc.py")
    # execute maps to sqlalchemy.execute (write), not a read
    assert result.reads == []
