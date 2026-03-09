import re
from typing import Set, Optional
from sqlglot import parse, exp


class SQLAnalyzer:
    """Analyzes SQL for dependencies (tables, views) and dbt macros/sources."""

    def __init__(self, default_dialect: str = "postgres") -> None:
        self.default_dialect = default_dialect

    def extract_dependencies(self, sql: str, dialect: Optional[str] = None) -> Set[str]:
        """
        Parses SQL using sqlglot and extracts physical table dependencies.
        Returns a normalized set of table names (schema.table, lowercase, unquoted).
        Also extracts dbt macros: {{ ref('x') }} and {{ source('a', 'b') }}.
        """
        deps: Set[str] = set()

        # 1. dbt macro handling via regex (before sqlglot, so works even on broken SQL)
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

                # Find all table references — use name parts, NOT .sql() which includes alias
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

                    # Skip CTE references
                    if self._normalize_str(name) in ctes:
                        continue

                    deps.add(fullname)

        except Exception:
            # Gracefully swallow parse errors; regex already caught macros above
            pass

        return deps

    def _normalize_str(self, s: str) -> str:
        """Strip sql quotes and lowercase."""
        clean = re.sub(r'["`\']', "", s)
        return clean.lower().strip()
