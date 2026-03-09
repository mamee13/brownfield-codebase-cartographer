"""Tests for AirflowDagAnalyzer and DbtSchemaAnalyzer."""

import pytest
from src.analyzers.dag_config_parser import AirflowDagAnalyzer, DbtSchemaAnalyzer

# ── Airflow ──────────────────────────────────────────────────────────────────


@pytest.fixture
def airflow() -> AirflowDagAnalyzer:
    return AirflowDagAnalyzer()


SIMPLE_DAG = """\
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
import datetime

with DAG('my_dag', start_date=datetime.datetime(2024,1,1)) as dag:
    extract = PythonOperator(task_id='extract', python_callable=run_extract)
    transform = BigQueryOperator(
        task_id='transform',
        sql='SELECT * FROM raw.events',
    )
    load = PythonOperator(task_id='load', python_callable=run_load)
    extract >> transform >> load
"""


def test_airflow_task_extraction(airflow: AirflowDagAnalyzer) -> None:
    result = airflow.analyze(SIMPLE_DAG, "dags/my_dag.py")
    assert "extract" in result.tasks
    assert "transform" in result.tasks
    assert "load" in result.tasks


def test_airflow_sql_arg_extraction(airflow: AirflowDagAnalyzer) -> None:
    result = airflow.analyze(SIMPLE_DAG, "dags/my_dag.py")
    assert result.tasks["transform"].sql_arg == "SELECT * FROM raw.events"


def test_airflow_rshift_dependencies(airflow: AirflowDagAnalyzer) -> None:
    result = airflow.analyze(SIMPLE_DAG, "dags/my_dag.py")
    assert "extract" in result.tasks["transform"].dependencies
    assert "transform" in result.tasks["load"].dependencies


SET_UPSTREAM_DAG = """\
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG('upstream_dag') as dag:
    a = BashOperator(task_id='a', bash_command='echo a')
    b = BashOperator(task_id='b', bash_command='echo b')
    b.set_upstream(a)
"""


def test_airflow_set_upstream_dependency(airflow: AirflowDagAnalyzer) -> None:
    result = airflow.analyze(SET_UPSTREAM_DAG, "dags/upstream_dag.py")
    assert "a" in result.tasks["b"].dependencies


def test_airflow_parse_failure_emits_warning(airflow: AirflowDagAnalyzer) -> None:
    result = airflow.analyze("def invalid syntax !!!!", "dags/broken.py")
    assert any(w.code == "PARSE_ERROR" for w in result.warnings)


# ── dbt ───────────────────────────────────────────────────────────────────────


@pytest.fixture
def dbt() -> DbtSchemaAnalyzer:
    return DbtSchemaAnalyzer()


DBT_SCHEMA = """\
version: 2

models:
  - name: stg_orders
    description: Staged orders from raw
    meta:
      owner: data-team
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: amount

sources:
  - name: raw
    database: production
    schema: raw_data
    meta:
      owner: platform-team
    tables:
      - name: orders
      - name: customers
"""


def test_dbt_model_extraction(dbt: DbtSchemaAnalyzer) -> None:
    result = dbt.analyze(DBT_SCHEMA, "models/schema.yml")
    assert len(result.models) == 1
    model = result.models[0]
    assert model.name == "stg_orders"
    assert model.description == "Staged orders from raw"
    assert model.owner == "data-team"


def test_dbt_model_columns(dbt: DbtSchemaAnalyzer) -> None:
    result = dbt.analyze(DBT_SCHEMA, "models/schema.yml")
    model = result.models[0]
    assert "order_id" in model.columns
    assert "amount" in model.columns


def test_dbt_model_tests(dbt: DbtSchemaAnalyzer) -> None:
    result = dbt.analyze(DBT_SCHEMA, "models/schema.yml")
    model = result.models[0]
    assert "unique" in model.tests
    assert "not_null" in model.tests


def test_dbt_source_extraction(dbt: DbtSchemaAnalyzer) -> None:
    result = dbt.analyze(DBT_SCHEMA, "models/schema.yml")
    assert len(result.sources) == 1
    src = result.sources[0]
    assert src.name == "raw"
    assert src.database == "production"
    assert src.schema_name == "raw_data"
    assert src.owner == "platform-team"
    assert "orders" in src.tables
    assert "customers" in src.tables


def test_dbt_parse_failure_emits_warning(dbt: DbtSchemaAnalyzer) -> None:
    result = dbt.analyze(": bad: yaml: [", "models/broken.yml")
    assert any(w.code == "PARSE_ERROR" for w in result.warnings)
