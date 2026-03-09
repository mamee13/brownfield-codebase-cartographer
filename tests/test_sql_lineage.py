import pytest
from src.analyzers.sql_lineage import SQLAnalyzer


@pytest.fixture
def analyzer() -> SQLAnalyzer:
    return SQLAnalyzer()


def test_sqlglot_extracts_dependencies(analyzer: SQLAnalyzer) -> None:
    # Basic SELECT
    sql = "SELECT * FROM my_schema.users"
    deps = analyzer.extract_dependencies(sql)
    assert deps == {"my_schema.users"}

    # JOIN — aliases (u, o) must not appear in output, just base table names
    sql2 = "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
    deps2 = analyzer.extract_dependencies(sql2)
    assert deps2 == {"users", "orders"}


def test_sqlglot_cte_resolution(analyzer: SQLAnalyzer) -> None:
    # CTE names must not be returned as external dependencies
    sql = """
    WITH recent_users AS (
        SELECT * FROM production.users WHERE created_at > '2023-01-01'
    )
    SELECT * FROM recent_users JOIN raw.events e ON e.user_id = recent_users.id
    """
    deps = analyzer.extract_dependencies(sql)
    assert deps == {"production.users", "raw.events"}


def test_dbt_ref_and_source_extraction(analyzer: SQLAnalyzer) -> None:
    # Macros should be caught via regex even if parsing fails or before parsing
    sql = """
    WITH models AS (
        SELECT * FROM {{ ref('stg_models') }}
    ),
    sources AS (
        SELECT * FROM {{ source('raw_data', 'clickstream') }}
    )
    SELECT * FROM models
    """
    deps = analyzer.extract_dependencies(sql)
    assert deps == {"stg_models", "raw_data.clickstream"}


def test_normalization_double_quoted_postgres(analyzer: SQLAnalyzer) -> None:
    # Double-quoted identifiers (postgres / ANSI SQL)
    sql = 'SELECT * FROM "public"."MyTable"'
    deps = analyzer.extract_dependencies(sql, dialect="postgres")
    assert deps == {"public.mytable"}


def test_normalization_backtick_bigquery() -> None:
    # Backtick-quoted 3-part identifiers (BigQuery)
    bq_analyzer = SQLAnalyzer(default_dialect="bigquery")
    sql = "SELECT * FROM `project`.`dataset`.`my_view`"
    deps = bq_analyzer.extract_dependencies(sql)
    assert deps == {"project.dataset.my_view"}


def test_parse_failure_fallback_returns_regex_matches(analyzer: SQLAnalyzer) -> None:
    # This SQL is structurally broken — but regex should still catch the dbt macro
    sql = "SELECT garbage SYNTAX ERROR {{ ref('my_model') }}"
    deps = analyzer.extract_dependencies(sql)
    assert deps == {"my_model"}
