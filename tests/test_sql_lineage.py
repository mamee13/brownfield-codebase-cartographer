"""Tests for SQLAnalyzer — all audit findings addressed."""

import pytest
from src.analyzers.sql_lineage import SQLAnalyzer


@pytest.fixture
def analyzer() -> SQLAnalyzer:
    return SQLAnalyzer()


# ── 1. Basic extraction ───────────────────────────────────────────────────────


def test_sqlglot_extracts_dependencies(analyzer: SQLAnalyzer) -> None:
    # Basic SELECT
    sql = "SELECT * FROM my_schema.users"
    deps, warnings = analyzer.extract_dependencies(sql)
    assert deps == {"my_schema.users"}
    assert warnings == []

    # JOIN — aliases (u, o) must NOT appear in output
    sql2 = "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
    deps2, _ = analyzer.extract_dependencies(sql2)
    assert deps2 == {"users", "orders"}


def test_sqlglot_cte_resolution(analyzer: SQLAnalyzer) -> None:
    # CTE names must not be returned as external dependencies
    sql = """
    WITH recent_users AS (
        SELECT * FROM production.users WHERE created_at > '2023-01-01'
    )
    SELECT * FROM recent_users JOIN raw.events e ON e.user_id = recent_users.id
    """
    deps, warnings = analyzer.extract_dependencies(sql)
    assert deps == {"production.users", "raw.events"}
    assert warnings == []


def test_dbt_ref_and_source_extraction(analyzer: SQLAnalyzer) -> None:
    sql = """
    WITH models AS (
        SELECT * FROM {{ ref('stg_models') }}
    ),
    sources AS (
        SELECT * FROM {{ source('raw_data', 'clickstream') }}
    )
    SELECT * FROM models
    """
    deps, _ = analyzer.extract_dependencies(sql)
    assert deps == {"stg_models", "raw_data.clickstream"}


# ── 2. Identifier normalization ───────────────────────────────────────────────


def test_normalization_double_quoted_postgres(analyzer: SQLAnalyzer) -> None:
    sql = 'SELECT * FROM "public"."MyTable"'
    deps, warnings = analyzer.extract_dependencies(sql, dialect="postgres")
    assert deps == {"public.mytable"}
    assert warnings == []


def test_normalization_backtick_bigquery() -> None:
    bq_analyzer = SQLAnalyzer(default_dialect="bigquery")
    sql = "SELECT * FROM `project`.`dataset`.`my_view`"
    deps, warnings = bq_analyzer.extract_dependencies(sql)
    assert deps == {"project.dataset.my_view"}
    assert warnings == []


# ── 3. Dialect coverage (Snowflake + DuckDB) — audit finding #2 ──────────────


def test_snowflake_dialect_extracts_qualified_tables() -> None:
    sf_analyzer = SQLAnalyzer(default_dialect="snowflake")
    sql = 'SELECT * FROM "PROD_DB"."PUBLIC"."ORDERS" o JOIN raw.events e ON o.id = e.order_id'
    deps, warnings = sf_analyzer.extract_dependencies(sql)
    assert "prod_db.public.orders" in deps
    assert "raw.events" in deps
    # Aliases must not appear
    assert not any("as o" in d or "as e" in d for d in deps)


def test_duckdb_dialect_extracts_tables() -> None:
    duck_analyzer = SQLAnalyzer(default_dialect="duckdb")
    sql = "SELECT * FROM read_parquet('data/events.parquet') AS events_raw"
    deps, warnings = duck_analyzer.extract_dependencies(sql)
    # read_parquet is a table function, not a Table node — should not crash
    # No physical tables → empty deps or table function name, no error
    assert isinstance(deps, set)
    assert isinstance(warnings, list)


def test_duckdb_regular_table_extraction() -> None:
    duck_analyzer = SQLAnalyzer(default_dialect="duckdb")
    sql = "SELECT a.id, b.amount FROM analytics.users a JOIN analytics.orders b ON a.id = b.user_id"
    deps, warnings = duck_analyzer.extract_dependencies(sql)
    assert "analytics.users" in deps
    assert "analytics.orders" in deps
    assert warnings == []


# ── 4. Parse failure → warning channel (audit finding #1) ────────────────────


def test_parse_failure_emits_warning_not_exception(analyzer: SQLAnalyzer) -> None:
    """A completely invalid SQL must NOT raise an exception — it must emit a warning."""
    bad_sql = "THIS IS NOT SQL AT ALL !!!"
    deps, warnings = analyzer.extract_dependencies(bad_sql, filepath="etl/bad.sql")
    # Must not crash; returns empty deps
    assert isinstance(deps, set)
    # Must log a structured warning
    assert len(warnings) == 1
    assert warnings[0].code == "SQL_PARSE_ERROR"
    assert warnings[0].file == "etl/bad.sql"
    assert warnings[0].analyzer == "SQLAnalyzer"


def test_parse_failure_fallback_regex_still_returns_macros(
    analyzer: SQLAnalyzer,
) -> None:
    # Even with invalid SQL around the macro, regex extraction still works
    sql = "SELECT garbage SYNTAX ERROR {{ ref('my_model') }}"
    deps, warnings = analyzer.extract_dependencies(sql)
    assert "my_model" in deps
