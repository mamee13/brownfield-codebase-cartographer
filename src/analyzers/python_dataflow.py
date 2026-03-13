"""
Python Data-Flow Analyzer — Step 5 (Day 2)

Detects dataset reads and writes from common Python data APIs:
  - pandas: read_csv, read_parquet, read_sql, read_json, read_excel, to_csv, to_parquet, to_sql
  - SQLAlchemy: engine.execute, connection.execute, session.execute
  - PySpark: spark.read.csv/parquet/json/table, DataFrame.write.csv/parquet/mode().csv etc.

Static refs are resolved to dataset names.
Dynamic refs (f-strings, variable interpolation) are logged as WarningRecords with
code=DYNAMIC_REF and must never be silently dropped.
"""

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.models.schema import WarningRecord, WarningSeverity


@dataclass
class DataRef:
    """A resolved reference to a dataset read or write."""

    name: str
    direction: str  # 'read' | 'write'
    source_file: str
    line: int
    api: str  # e.g. 'pandas.read_csv'


@dataclass
class DataFlowResult:
    reads: List[DataRef] = field(default_factory=list)
    writes: List[DataRef] = field(default_factory=list)
    warnings: List[WarningRecord] = field(default_factory=list)


# Pattern registry: (module_attr_chain, direction, api_label)
# module_attr_chain is matched against the dotted call like `pd.read_csv`
_PATTERNS: List[Tuple[List[str], str, str]] = [
    # pandas reads
    (["read_csv"], "read", "pandas.read_csv"),
    (["read_parquet"], "read", "pandas.read_parquet"),
    (["read_sql"], "read", "pandas.read_sql"),
    (["read_json"], "read", "pandas.read_json"),
    (["read_excel"], "read", "pandas.read_excel"),
    # pandas writes
    (["to_csv"], "write", "pandas.to_csv"),
    (["to_parquet"], "write", "pandas.to_parquet"),
    (["to_sql"], "write", "pandas.to_sql"),
    # SQLAlchemy execute (guarded by chain provenance in _is_sqlalchemy_execute_chain)
    (["execute"], "write", "sqlalchemy.execute"),
    # PySpark reads
    (["read", "csv"], "read", "pyspark.read.csv"),
    (["read", "parquet"], "read", "pyspark.read.parquet"),
    (["read", "json"], "read", "pyspark.read.json"),
    (["read", "table"], "read", "pyspark.read.table"),
    # PySpark writes (via .write.csv etc or .mode().csv)
    (["write", "csv"], "write", "pyspark.write.csv"),
    (["write", "parquet"], "write", "pyspark.write.parquet"),
    (["write", "saveAsTable"], "write", "pyspark.write.saveAsTable"),
    # Standard library: json
    (["json", "load"], "read", "json.load"),
    (["json", "loads"], "read", "json.loads"),
    (["json", "dump"], "write", "json.dump"),
    (["json", "dumps"], "write", "json.dumps"),
    # Standard library: open
    (["open"], "read", "open"),
    # HTTP: httpx
    (["httpx", "get"], "read", "httpx.get"),
    (["httpx", "post"], "write", "httpx.post"),
    (["httpx", "put"], "write", "httpx.put"),
    (["httpx", "patch"], "write", "httpx.patch"),
    (["httpx", "delete"], "write", "httpx.delete"),
    (["get"], "read", "httpx.get"),
    (["post"], "write", "httpx.post"),
    # HTTP: requests
    (["requests", "get"], "read", "requests.get"),
    (["requests", "post"], "write", "requests.post"),
    (["requests", "put"], "write", "requests.put"),
    (["requests", "patch"], "write", "requests.patch"),
    (["requests", "delete"], "write", "requests.delete"),
]

_SQLALCHEMY_EXECUTE_ROOTS = {"engine", "conn", "connection", "session", "cursor"}


def _call_chain(node: ast.expr) -> List[str]:
    """Flatten an AST attr chain into a list of names (e.g. df.to_csv -> ['df','to_csv'])."""
    parts: List[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return list(reversed(parts))


def _try_resolve_static(arg: ast.expr) -> Optional[str]:
    """
    Try to extract a static string value from an AST node.
    Returns None for dynamic expressions (f-strings, variables, etc.).
    """
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    # Joined strings (f-strings) are dynamic
    if isinstance(arg, ast.JoinedStr):
        return None
    # Name references (variables) are dynamic
    if isinstance(arg, ast.Name):
        return None
    return None


class PythonDataFlowAnalyzer:
    """Walks a Python AST and extracts read/write dataset references."""

    def analyze(self, source: str, filepath: str) -> DataFlowResult:
        result = DataFlowResult()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            result.warnings.append(
                WarningRecord(
                    code="PARSE_ERROR",
                    message=f"Could not parse Python file: {filepath}",
                    file=filepath,
                    analyzer="PythonDataFlowAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result

        # Track assignments to help resolve Path objects and Open handles
        # path_vars: var_name -> path_string
        # file_vars: var_name -> path_string
        path_vars: Dict[str, str] = {}
        file_vars: Dict[str, str] = {}

        for node in ast.walk(tree):
            # Track Path assignments: p = Path('data.csv')
            if isinstance(node, ast.Assign):
                if isinstance(node.value, ast.Call) and _call_chain(
                    node.value.func
                ) == ["Path"]:
                    if node.value.args:
                        resolved = _try_resolve_static(node.value.args[0])
                        if resolved:
                            for target in node.targets:
                                if isinstance(target, ast.Name):
                                    path_vars[target.id] = resolved

                # Track open() assignments: f = open('data.csv')
                elif isinstance(node.value, ast.Call) and _call_chain(
                    node.value.func
                ) == ["open"]:
                    if node.value.args:
                        resolved = _try_resolve_static(node.value.args[0])
                        if resolved:
                            for target in node.targets:
                                if isinstance(target, ast.Name):
                                    file_vars[target.id] = resolved

            # Track with open(...) as f:
            if isinstance(node, ast.With):
                for item in node.items:
                    if (
                        isinstance(item.context_expr, ast.Call)
                        and _call_chain(item.context_expr.func) == ["open"]
                        and item.context_expr.args
                    ):
                        resolved = _try_resolve_static(item.context_expr.args[0])
                        if resolved and isinstance(item.optional_vars, ast.Name):
                            file_vars[item.optional_vars.id] = resolved

            if not isinstance(node, ast.Call):
                continue

            try:
                self._check_call_refined(node, filepath, result, path_vars, file_vars)
            except Exception as e:
                result.warnings.append(
                    WarningRecord(
                        code="ANALYZE_ERROR",
                        message=f"Error checking call node at L{node.lineno}: {e}",
                        file=filepath,
                        line=node.lineno,
                        analyzer="PythonDataFlowAnalyzer",
                        severity=WarningSeverity.WARNING,
                    )
                )

        return result

    def analyze_file(self, path: Path) -> DataFlowResult:
        try:
            source = path.read_text(encoding="utf-8")
        except OSError as exc:
            result = DataFlowResult()
            result.warnings.append(
                WarningRecord(
                    code="READ_ERROR",
                    message=f"Could not read file: {exc}",
                    file=str(path),
                    analyzer="PythonDataFlowAnalyzer",
                    severity=WarningSeverity.ERROR,
                )
            )
            return result
        return self.analyze(source, str(path))

    def _check_call_refined(
        self,
        node: ast.Call,
        filepath: str,
        result: DataFlowResult,
        path_vars: Dict[str, str],
        file_vars: Dict[str, str],
    ) -> None:
        chain = _call_chain(node.func)
        suffix = chain

        # 1. Standard pattern matching (including json.load(f) where f is tracked)
        for pattern, direction, api in _PATTERNS:
            plen = len(pattern)
            if len(suffix) >= plen and suffix[-plen:] == pattern:
                if pattern == ["execute"] and not self._is_sqlalchemy_execute_chain(
                    chain
                ):
                    return

                # Special handle for json.load(f) where f is a tracked file handle
                if (
                    api == "json.load"
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                ):
                    handle_name = node.args[0].id
                    if handle_name in file_vars:
                        ref = DataRef(
                            name=file_vars[handle_name],
                            direction="read",
                            source_file=filepath,
                            line=node.lineno,
                            api=api,
                        )
                        result.reads.append(ref)
                        return

                self._extract_arg(node, filepath, direction, api, result)
                return

        # 2. Method calls on Path objects or direct Path calls
        if isinstance(node.func, ast.Attribute) and node.func.attr in [
            "read_text",
            "read_bytes",
            "write_text",
            "write_bytes",
        ]:
            resolved = None
            # case: p.read_text() where p is Path object
            if (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id in path_vars
            ):
                resolved = path_vars[node.func.value.id]
            # case: Path('...').read_text()
            elif (
                isinstance(node.func.value, ast.Call)
                and isinstance(node.func.value.func, ast.Name)
                and node.func.value.func.id == "Path"
                and node.func.value.args
            ):
                resolved = _try_resolve_static(node.func.value.args[0])

            if resolved:
                direction = "read" if "read" in node.func.attr else "write"
                api = f"pathlib.{node.func.attr}"
                ref = DataRef(
                    name=resolved,
                    direction=direction,
                    source_file=filepath,
                    line=node.lineno,
                    api=api,
                )
                if direction == "read":
                    result.reads.append(ref)
                else:
                    result.writes.append(ref)

    @staticmethod
    def _is_sqlalchemy_execute_chain(chain: List[str]) -> bool:
        """
        Best-effort provenance check for execute() to reduce false positives.
        Accept patterns that look like SQLAlchemy handles, e.g.:
        - conn.execute(...)
        - session.execute(...)
        - self.engine.execute(...)
        - sqlalchemy.engine.execute(...)
        """
        if len(chain) < 2 or chain[-1] != "execute":
            return False

        root_tokens = set(chain[:-1])
        if "sqlalchemy" in root_tokens:
            return True

        return any(token in _SQLALCHEMY_EXECUTE_ROOTS for token in root_tokens)

    def _extract_arg(
        self,
        node: ast.Call,
        filepath: str,
        direction: str,
        api: str,
        result: DataFlowResult,
    ) -> None:
        """Extract the first positional arg as the dataset name, or emit a warning."""
        arg: Optional[ast.expr] = None
        if node.args:
            arg = node.args[0]
        elif node.keywords:
            # Some APIs use keyword args like path=, name=, con=, url=
            for kw in node.keywords:
                if kw.arg in ("path", "name", "table_name", "con", "sql", "url"):
                    arg = kw.value
                    break

        if arg is None:
            return

        lineno = node.lineno
        resolved = _try_resolve_static(arg)
        if resolved is not None:
            ref = DataRef(
                name=resolved,
                direction=direction,
                source_file=filepath,
                line=lineno,
                api=api,
            )
            if direction == "read":
                result.reads.append(ref)
            else:
                result.writes.append(ref)
        else:
            # Dynamic reference — MUST be logged, never silently dropped
            result.warnings.append(
                WarningRecord(
                    code="DYNAMIC_REF",
                    message=(
                        f"Dynamic reference in {api} call cannot be statically resolved"
                    ),
                    file=filepath,
                    line=lineno,
                    analyzer="PythonDataFlowAnalyzer",
                    severity=WarningSeverity.WARNING,
                )
            )
