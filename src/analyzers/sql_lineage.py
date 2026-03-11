"""
SQL Lineage Analyzer (enhanced)

- Parses SQL with sqlglot across multiple dialects.
- Extracts per-statement source/target tables with line ranges.
- Handles dbt ref()/source() macros before parsing.
- Logs parse failures as WarningRecords (never silent).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple

from sqlglot import exp, parse
from sqlglot.errors import ParseError
from sqlglot.tokens import Tokenizer

from src.models.schema import WarningRecord, WarningSeverity


@dataclass
class QueryLineage:
    sources: Set[str] = field(default_factory=set)
    targets: Set[str] = field(default_factory=set)
    line_range: str = "L1-1"
    dialect: str = ""


class SQLAnalyzer:
    """
    Analyzes SQL for dependencies (tables, views) and dbt macros/sources.

    Returns both per-statement lineage and warnings.
    Parse failures are ALWAYS logged — never silently dropped.
    """

    def __init__(self, default_dialect: str = "postgres") -> None:
        self.default_dialect = default_dialect
        self.supported_dialects = ["postgres", "bigquery", "snowflake", "duckdb"]

    def extract_lineage(
        self,
        sql: str,
        dialect: Optional[str] = None,
        filepath: str = "<unknown>",
    ) -> Tuple[List[QueryLineage], List[WarningRecord]]:
        warnings: List[WarningRecord] = []
        queries: List[QueryLineage] = []

        # 1) dbt macro handling via regex (before sqlglot, works on broken SQL too)
        macro_deps: Set[str] = set()
        ref_pattern = r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(ref_pattern, sql):
            macro_deps.add(self._normalize_str(match.group(1)))

        source_pattern = r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(source_pattern, sql):
            schema, table = match.groups()
            macro_deps.add(self._normalize_str(f"{schema}.{table}"))

        # 2) Parse SQL using sqlglot (best-effort across multiple dialects)
        try:
            cleaned_sql = self._strip_jinja(sql)
            statements, used_dialect = self._parse_with_dialects(cleaned_sql, dialect)
            line_ranges = self._statement_line_ranges(cleaned_sql)

            for idx, statement in enumerate(statements):
                if not statement:
                    continue
                line_range = line_ranges[idx] if idx < len(line_ranges) else "L1-1"

                # Collect CTE names to exclude them as external dependencies
                ctes: Set[str] = set()
                for cte in statement.find_all(exp.CTE):
                    name = cte.alias_or_name
                    if name:
                        ctes.add(self._normalize_str(name))

                targets = self._extract_targets(statement)
                sources = self._extract_sources(statement, ctes, targets)

                # Include dbt macro dependencies (if any)
                sources.update(macro_deps)

                queries.append(
                    QueryLineage(
                        sources=sources,
                        targets=targets,
                        line_range=line_range,
                        dialect=used_dialect,
                    )
                )

        except Exception as exc:  # pragma: no cover - safety net
            warnings.append(
                WarningRecord(
                    code="SQL_PARSE_ERROR",
                    message=f"sqlglot could not parse SQL in {filepath}: {exc}",
                    file=filepath,
                    analyzer="SQLAnalyzer",
                    severity=WarningSeverity.WARNING,
                )
            )
            if macro_deps:
                queries.append(
                    QueryLineage(
                        sources=set(macro_deps),
                        targets=set(),
                        line_range="L1-1",
                        dialect="unknown",
                    )
                )

        return queries, warnings

    def extract_dependencies(
        self,
        sql: str,
        dialect: Optional[str] = None,
        filepath: str = "<unknown>",
    ) -> Tuple[Set[str], List[WarningRecord]]:
        """
        Backwards-compatible API: return a flat set of referenced tables.
        """
        queries, warnings = self.extract_lineage(
            sql, dialect=dialect, filepath=filepath
        )
        deps: Set[str] = set()
        for q in queries:
            deps.update(q.sources)
            deps.update(q.targets)
        return deps, warnings

    # ── Parsing helpers ──────────────────────────────────────────────────────

    def _parse_with_dialects(
        self, sql: str, dialect: Optional[str]
    ) -> Tuple[List[exp.Expression], str]:
        dialects = [dialect] if dialect else self.supported_dialects
        last_error: Optional[Exception] = None

        for d in dialects:
            if not d:
                continue
            try:
                statements = [s for s in parse(sql, read=d) if s is not None]
                return statements, d
            except ParseError as exc:
                last_error = exc
                continue

        # Fallback to default dialect
        try:
            statements = [
                s for s in parse(sql, read=self.default_dialect) if s is not None
            ]
            return statements, self.default_dialect
        except ParseError as exc:
            raise exc from last_error

    def _statement_line_ranges(self, sql: str) -> List[str]:
        """Compute line ranges per statement using sqlglot tokenizer."""
        tokens = Tokenizer().tokenize(sql)
        ranges: List[str] = []
        current_start: Optional[int] = None
        current_end: Optional[int] = None

        for token in tokens:
            if current_start is None:
                current_start = token.line
            current_end = token.line

            if token.text == ";":
                if current_start is not None and current_end is not None:
                    ranges.append(f"L{current_start}-L{current_end}")
                current_start = None
                current_end = None

        if current_start is not None and current_end is not None:
            ranges.append(f"L{current_start}-L{current_end}")

        return ranges

    # ── Table extraction ─────────────────────────────────────────────────────

    def _extract_targets(self, statement: exp.Expression) -> Set[str]:
        targets: Set[str] = set()

        if isinstance(statement, exp.Insert):
            name = self._table_name(statement.this)
            if name:
                targets.add(name)

        if isinstance(statement, exp.Create):
            name = self._table_name(statement.this)
            if name:
                targets.add(name)

        if isinstance(statement, exp.Merge):
            name = self._table_name(statement.this)
            if name:
                targets.add(name)

        if isinstance(statement, exp.Update):
            name = self._table_name(statement.this)
            if name:
                targets.add(name)

        return targets

    def _extract_sources(
        self,
        statement: exp.Expression,
        ctes: Set[str],
        targets: Set[str],
    ) -> Set[str]:
        sources: Set[str] = set()
        for table in statement.find_all(exp.Table):
            name = self._table_name(table)
            if not name:
                continue
            if self._normalize_str(name) in ctes:
                continue
            if self._normalize_str(name) in {self._normalize_str(t) for t in targets}:
                continue
            sources.add(name)
        return sources

    def _table_name(self, table: exp.Expression | None) -> Optional[str]:
        if table is None:
            return None

        if isinstance(table, exp.Table):
            parts: List[str] = []
            catalog = table.args.get("catalog")
            db = table.args.get("db")
            name = table.name

            if not name:
                return None

            if catalog and hasattr(catalog, "name"):
                parts.append(self._normalize_str(catalog.name))
            if db and hasattr(db, "name"):
                parts.append(self._normalize_str(db.name))
            parts.append(self._normalize_str(name))
            return ".".join(parts)

        if isinstance(table, exp.Identifier):
            return self._normalize_str(table.name)

        return None

    # ── Jinja cleanup ────────────────────────────────────────────────────────

    def _strip_jinja(self, sql: str) -> str:
        """
        Remove dbt/Jinja templating blocks and expressions to improve sqlglot parsing.
        Preserves line counts to keep line ranges stable.
        """

        def _preserve_newlines(match: re.Match[str]) -> str:
            text = match.group(0)
            newlines = text.count("\n")
            return "\n" * newlines

        def _ref_repl(match: re.Match[str]) -> str:
            return match.group(1)

        def _source_repl(match: re.Match[str]) -> str:
            schema, table = match.groups()
            return f"{schema}.{table}"

        sql = re.sub(
            r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
            _ref_repl,
            sql,
        )
        sql = re.sub(
            r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
            _source_repl,
            sql,
        )

        sql = re.sub(r"\{#\s*.*?\s*#\}", _preserve_newlines, sql, flags=re.DOTALL)
        sql = re.sub(r"\{%\s*.*?\s*%\}", _preserve_newlines, sql, flags=re.DOTALL)
        sql = re.sub(r"\{\{\s*.*?\s*\}\}", _preserve_newlines, sql, flags=re.DOTALL)
        return sql

    def _normalize_str(self, s: str) -> str:
        clean = re.sub(r"[\"`']", "", s)
        return clean.lower().strip()
