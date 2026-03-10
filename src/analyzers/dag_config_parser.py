"""
DAG/Config Parser — Step 6 (Day 2)

Parses:
  - Airflow DAG Python files: extracts task dependencies (>>, set_upstream/downstream)
    and operator SQL/file args where present.
  - dbt schema.yml files: extracts models, sources, tests and ownership metadata.

Emits WarningRecord on parse failures; never crashes the whole pipeline.
"""

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.models.schema import WarningRecord, WarningSeverity


@dataclass
class TaskNode:
    task_id: str
    operator: str
    sql_arg: Optional[str] = (
        None  # SQL or filepath from BashOperator/SQLExecuteQueryOperator
    )
    dependencies: List[str] = field(default_factory=list)  # upstream task_ids


@dataclass
class DagResult:
    dag_id: str
    tasks: Dict[str, TaskNode] = field(default_factory=dict)
    warnings: List[WarningRecord] = field(default_factory=list)


@dataclass
class DbtModel:
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    tests: List[str] = field(default_factory=list)


@dataclass
class DbtSource:
    name: str
    database: Optional[str] = None
    schema_name: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[str] = None
    tables: List[str] = field(default_factory=list)


@dataclass
class DbtResult:
    models: List[DbtModel] = field(default_factory=list)
    sources: List[DbtSource] = field(default_factory=list)
    warnings: List[WarningRecord] = field(default_factory=list)


# ──────────────────────────── Airflow parser ──────────────────────────────────


class AirflowDagAnalyzer:
    """
    Statically parses an Airflow DAG Python file.

    Extracts:
      - Task IDs and operator types
      - >> and set_upstream / set_downstream dependencies
      - SQL / bash_command args from supported operators
    """

    SQL_OPERATORS = {
        "SQLExecuteQueryOperator",
        "PostgresOperator",
        "MySqlOperator",
        "BigQueryOperator",
        "SnowflakeOperator",
        "SparkSqlOperator",
    }

    def analyze(self, source: str, filepath: str) -> DagResult:
        dag_id = Path(filepath).stem
        result = DagResult(dag_id=dag_id)

        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            result.warnings.append(
                WarningRecord(
                    code="PARSE_ERROR",
                    message=f"Airflow DAG parse error in {filepath}: {exc}",
                    file=filepath,
                    analyzer="AirflowDagAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result

        # 1. Extract task objects and their operators
        task_var_to_id: Dict[str, str] = {}  # variable_name -> task_id

        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            # Look for: task_var = SomeOperator(task_id='...', ...)
            if not isinstance(node.value, ast.Call):
                continue
            call = node.value
            operator_name = _get_call_name(call)
            task_id = _get_kwarg_str(call, "task_id")
            if task_id is None:
                continue

            sql_ref: Optional[str] = None
            if operator_name in self.SQL_OPERATORS:
                sql_ref = _get_kwarg_str(call, "sql")
            if operator_name == "BashOperator":
                sql_ref = _get_kwarg_str(call, "bash_command")

            t = TaskNode(
                task_id=task_id, operator=operator_name or "Unknown", sql_arg=sql_ref
            )
            result.tasks[task_id] = t

            # Map variable names to task IDs for dependency resolution
            for target in node.targets:
                if isinstance(target, ast.Name):
                    task_var_to_id[target.id] = task_id

        # 2. Extract >> dependencies and set_upstream / set_downstream
        for node in ast.walk(tree):
            self._extract_deps(node, task_var_to_id, result)

        return result

    def analyze_file(self, path: Path) -> DagResult:
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as exc:
            result = DagResult(dag_id=path.stem)
            result.warnings.append(
                WarningRecord(
                    code="READ_ERROR",
                    message=str(exc),
                    file=str(path),
                    analyzer="AirflowDagAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result
        return self.analyze(source, str(path))

    def _extract_deps(
        self,
        node: ast.AST,
        var_map: Dict[str, str],
        result: DagResult,
    ) -> None:
        # Handle: a >> b >> c  (BinOp chain)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
            names = _collect_rshift_names(node.value)
            for i in range(1, len(names)):
                upstream_var = names[i - 1]
                downstream_var = names[i]
                up_id = var_map.get(upstream_var)
                dn_id = var_map.get(downstream_var)
                if up_id and dn_id and dn_id in result.tasks:
                    if up_id not in result.tasks[dn_id].dependencies:
                        result.tasks[dn_id].dependencies.append(up_id)

        # Handle: task_b.set_upstream(task_a)  /  task_a.set_downstream(task_b)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute):
                method = call.func.attr
                obj_var = _get_name(call.func.value)
                if call.args:
                    arg_var = _get_name(call.args[0])
                    obj_id = var_map.get(obj_var or "")
                    arg_id = var_map.get(arg_var or "")
                    if method == "set_upstream" and obj_id and arg_id:
                        if (
                            arg_id
                            not in result.tasks.get(
                                obj_id, TaskNode("", "")
                            ).dependencies
                        ):
                            if obj_id in result.tasks:
                                result.tasks[obj_id].dependencies.append(arg_id)
                    elif method == "set_downstream" and obj_id and arg_id:
                        if arg_id in result.tasks:
                            if obj_id not in result.tasks[arg_id].dependencies:
                                result.tasks[arg_id].dependencies.append(obj_id)


# ──────────────────────────── dbt parser ──────────────────────────────────────


class DbtSchemaAnalyzer:
    """
    Parses dbt schema.yml / sources.yml files.

    Extracts:
      - models: name, description, owner (from meta), columns, tests
      - sources: name, database, schema, description, owner, tables
    """

    def analyze(self, content: str, filepath: str) -> DbtResult:
        result = DbtResult()
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as exc:
            result.warnings.append(
                WarningRecord(
                    code="PARSE_ERROR",
                    message=f"dbt YAML parse error in {filepath}: {exc}",
                    file=filepath,
                    analyzer="DbtSchemaAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result

        if not isinstance(data, dict):
            return result

        # models section
        for model_def in data.get("models", []) or []:
            model = DbtModel(
                name=model_def.get("name", ""),
                description=model_def.get("description"),
                owner=_deep_get(model_def, "meta", "owner"),
            )
            for col in model_def.get("columns", []) or []:
                if col.get("name"):
                    model.columns.append(col["name"])
                for test in col.get("tests", []) or []:
                    tname = test if isinstance(test, str) else list(test.keys())[0]
                    model.tests.append(tname)
            for test in model_def.get("tests", []) or []:
                tname = test if isinstance(test, str) else list(test.keys())[0]
                model.tests.append(tname)
            result.models.append(model)

        # sources section
        for src_def in data.get("sources", []) or []:
            src = DbtSource(
                name=src_def.get("name", ""),
                database=src_def.get("database"),
                schema_name=src_def.get("schema"),
                description=src_def.get("description"),
                owner=_deep_get(src_def, "meta", "owner"),
            )
            for tbl in src_def.get("tables", []) or []:
                if tbl.get("name"):
                    src.tables.append(tbl["name"])
            result.sources.append(src)

        return result

    def analyze_file(self, path: Path) -> DbtResult:
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            result = DbtResult()
            result.warnings.append(
                WarningRecord(
                    code="READ_ERROR",
                    message=str(exc),
                    file=str(path),
                    analyzer="DbtSchemaAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result
        return self.analyze(content, str(path))


# ──────────────────────────── helpers ────────────────────────────────────────


def _get_call_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _get_kwarg_str(call: ast.Call, arg: str) -> Optional[str]:
    for kw in call.keywords:
        if kw.arg == arg and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def _get_name(node: ast.expr) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    return None


def _collect_rshift_names(node: ast.expr) -> List[str]:
    """Recursively collect all Name nodes from a chain of >> ops in order."""
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift):
        return _collect_rshift_names(node.left) + _collect_rshift_names(node.right)
    if isinstance(node, ast.Name):
        return [node.id]
    return []


def _flatten_rshift(node: ast.BinOp) -> List[Tuple[str, str]]:
    """Return consecutive (upstream, downstream) pairs from a >> chain."""
    names = _collect_rshift_names(node)
    return [(names[i], names[i + 1]) for i in range(len(names) - 1)]


def _deep_get(d: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k, {})
    return str(d) if d else None
