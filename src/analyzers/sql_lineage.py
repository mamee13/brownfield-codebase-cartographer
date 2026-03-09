"""
SQL Lineage Analyzer (improved)

Parse failures are now logged to a structured WarningRecord list instead of
being silently swallowed. The caller receives both deps and warnings.
"""

import re
from typing import List, Optional, Set, Tuple

from sqlglot import parse, exp

from src.models.schema import WarningRecord, WarningSeverity


class SQLAnalyzer:
    """
    Analyzes SQL for dependencies (tables, views) and dbt macros/sources.

    Returns both a set of resolved dependencies and a list of WarningRecords.
    Parse failures are ALWAYS logged — never silently dropped.
    """

    def __init__(self, default_dialect: str = "postgres") -> None:
        self.default_dialect = default_dialect

    def extract_dependencies(
        self,
        sql: str,
        dialect: Optional[str] = None,
        filepath: str = "<unknown>",
    ) -> Tuple[Set[str], List[WarningRecord]]:
        """
        Returns (deps, warnings).
        deps: normalized set of table names (schema.table, lowercase, unquoted).
        warnings: any WarningRecords generated (parse failures, etc.).
        """
        deps: Set[str] = set()
        warnings: List[WarningRecord] = []

        # 1. dbt macro handling via regex (before sqlglot, works on broken SQL too)
        ref_pattern = r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(ref_pattern, sql):
            deps.add(self._normalize_str(match.group(1)))

        source_pattern = r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(source_pattern, sql):
            schema, table = match.groups()
            deps.add(self._normalize_str(f"{schema}.{table}"))

        # 2. Parse actual SQL using sqlglot
        try:
            statements = parse(sql, read=dialect or self.default_dialect)
            for statement in statements:
                if not statement:
                    continue

                # Collect CTE names to exclude them as external dependencies
                ctes: Set[str] = set()
                for cte in statement.find_all(exp.CTE):
                    if cte.alias:
                        ctes.add(cte.alias.lower())

                # Walk table references — use name parts, NOT .sql() which includes alias
                for table in statement.find_all(exp.Table):
                    parts = []
                    catalog = table.args.get("catalog")
                    db = table.args.get("db")
                    name = table.name

                    if not name:
                        continue

                    if catalog and hasattr(catalog, "name"):
                        parts.append(self._normalize_str(catalog.name))
                    if db and hasattr(db, "name"):
                        parts.append(self._normalize_str(db.name))
                    parts.append(self._normalize_str(name))

                    fullname = ".".join(parts)

                    # Skip CTE self-references
                    if self._normalize_str(name) in ctes:
                        continue

                    deps.add(fullname)

        except Exception as exc:
            # Log parse failure — never swallow silently
            warnings.append(
                WarningRecord(
                    code="SQL_PARSE_ERROR",
                    message=f"sqlglot could not parse SQL in {filepath}: {exc}",
                    file=filepath,
                    analyzer="SQLAnalyzer",
                    severity=WarningSeverity.WARNING,
                )
            )

        return deps, warnings

    def _normalize_str(self, s: str) -> str:
        """Strip SQL dialect quotes and lowercase."""
        clean = re.sub(r'["`\']', "", s)
        return clean.lower().strip()
