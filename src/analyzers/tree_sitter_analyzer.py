from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_sql
import tree_sitter_yaml
from tree_sitter import Language, Node, Parser


@dataclass
class ImportRef:
    module: Optional[str]
    name: Optional[str]
    level: int
    is_from: bool
    raw: str


@dataclass
class FunctionDef:
    name: str
    signature: str
    decorators: List[str] = field(default_factory=list)
    is_method: bool = False


@dataclass
class ClassDef:
    name: str
    bases: List[str] = field(default_factory=list)
    decorators: List[str] = field(default_factory=list)


@dataclass
class PythonAnalysisResult:
    imports: List[ImportRef] = field(default_factory=list)
    functions: List[FunctionDef] = field(default_factory=list)
    classes: List[ClassDef] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    resolved_imports: List[str] = field(default_factory=list)

    def __iter__(self) -> Iterator[object]:
        imports_raw = [imp.raw for imp in self.imports]
        public_symbols = [
            f.name for f in self.functions if not f.name.startswith("_")
        ] + [c.name for c in self.classes if not c.name.startswith("_")]
        return iter((imports_raw, public_symbols))


@dataclass
class SqlAnalysisResult:
    table_refs: List[str] = field(default_factory=list)
    cte_names: List[str] = field(default_factory=list)
    join_tables: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class YamlAnalysisResult:
    key_paths: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class LanguageRouter:
    @staticmethod
    def get_language(ext: str) -> Optional[Language]:
        ext = ext.lower()
        if ext == ".py":
            return Language(tree_sitter_python.language())
        if ext == ".sql":
            return Language(tree_sitter_sql.language())
        if ext in (".yaml", ".yml"):
            return Language(tree_sitter_yaml.language())
        if ext in (".js", ".jsx", ".ts", ".tsx"):
            return Language(tree_sitter_javascript.language())
        return None


class TreeSitterAnalyzer:
    def __init__(self) -> None:
        self.parser = Parser()

    # ── Python ──────────────────────────────────────────────────────────────

    def analyze_python_file(self, path: str, content: bytes) -> PythonAnalysisResult:
        _ = path
        lang = LanguageRouter.get_language(".py")
        if not lang:
            return PythonAnalysisResult(warnings=["python grammar not available"])

        self.parser.language = lang
        tree = self.parser.parse(content)
        result = PythonAnalysisResult()

        if tree.root_node.has_error:
            result.warnings.append("tree-sitter parse errors detected in python file")

        def node_text(node: Optional[Node]) -> str:
            if node is None or node.text is None:
                return ""
            return node.text.decode("utf-8")

        def extract_decorators(node: Node) -> List[str]:
            decorators: List[str] = []
            for child in node.children:
                if child.type == "decorator":
                    text = node_text(child).strip()
                    if text.startswith("@"):  # normalize
                        text = text[1:]
                    if text:
                        decorators.append(text)
            return decorators

        def extract_import(node: Node) -> None:
            raw = node_text(node).strip()
            if not raw:
                return
            if node.type == "import_statement":
                result.imports.append(
                    ImportRef(
                        module=None,
                        name=None,
                        level=0,
                        is_from=False,
                        raw=raw,
                    )
                )
                return

            if node.type == "import_from_statement":
                module = None
                module_node = node.child_by_field_name("module")
                if module_node is not None:
                    module = node_text(module_node).strip() or None

                level = 0
                # best-effort: count leading dots after "from"
                # e.g. "from ..subpkg import x"
                if raw.startswith("from "):
                    after_from = raw[len("from ") :]
                    dots = 0
                    for ch in after_from:
                        if ch == ".":
                            dots += 1
                        else:
                            break
                    level = dots

                result.imports.append(
                    ImportRef(
                        module=module,
                        name=None,
                        level=level,
                        is_from=True,
                        raw=raw,
                    )
                )

        def extract_function(
            node: Node, decorators: List[str], current_class: str | None
        ) -> None:
            name_node = node.child_by_field_name("name")
            params_node = node.child_by_field_name("parameters")
            name = node_text(name_node)
            signature = node_text(params_node)
            if name:
                result.functions.append(
                    FunctionDef(
                        name=name,
                        signature=signature,
                        decorators=decorators,
                        is_method=current_class is not None,
                    )
                )

        def extract_class(node: Node, decorators: List[str]) -> None:
            name_node = node.child_by_field_name("name")
            bases_node = node.child_by_field_name("superclasses")
            name = node_text(name_node)
            bases: List[str] = []
            if bases_node is not None:
                raw = node_text(bases_node)
                raw = raw.strip().lstrip("(").rstrip(")")
                bases = [b.strip() for b in raw.split(",") if b.strip()]
            if name:
                result.classes.append(
                    ClassDef(name=name, bases=bases, decorators=decorators)
                )

        def walk(node: Node, current_class: str | None = None) -> None:
            if node.type in ("import_statement", "import_from_statement"):
                extract_import(node)

            if node.type == "decorated_definition":
                decorators = extract_decorators(node)
                for child in node.children:
                    if child.type == "function_definition":
                        extract_function(child, decorators, current_class)
                        return
                    if child.type == "class_definition":
                        extract_class(child, decorators)
                        # still descend into class body for methods
                        class_name_node = child.child_by_field_name("name")
                        class_name = node_text(class_name_node) or None
                        for grandchild in child.children:
                            walk(grandchild, current_class=class_name)
                        return

            if node.type == "function_definition":
                extract_function(node, [], current_class)

            if node.type == "class_definition":
                extract_class(node, [])
                class_name_node = node.child_by_field_name("name")
                class_name = node_text(class_name_node) or None
                for child in node.children:
                    walk(child, current_class=class_name)
                return

            for child in node.children:
                walk(child, current_class)

        walk(tree.root_node)
        return result

    def resolve_imports(
        self,
        rel_path: str,
        imports: List[ImportRef],
        module_index: dict[str, str],
    ) -> List[str]:
        module_name = self._module_name_from_path(rel_path)
        if rel_path.endswith("__init__.py"):
            current_package = module_name
        else:
            current_package = (
                module_name.rsplit(".", 1)[0] if "." in module_name else ""
            )

        resolved: set[str] = set()
        for imp in imports:
            candidates = self._candidate_modules(imp.raw, current_package)
            for module_candidate in candidates:
                target = self._resolve_module_to_path(module_candidate, module_index)
                if target is not None:
                    resolved.add(target)

        return sorted(resolved)

    # ── SQL ─────────────────────────────────────────────────────────────────

    def analyze_sql(self, content: bytes) -> SqlAnalysisResult:
        lang = LanguageRouter.get_language(".sql")
        if not lang:
            return SqlAnalysisResult(warnings=["sql grammar not available"])

        self.parser.language = lang
        tree = self.parser.parse(content)
        result = SqlAnalysisResult()

        if tree.root_node.has_error:
            result.warnings.append("tree-sitter parse errors detected in sql file")

        def node_text(node: Optional[Node]) -> str:
            if node is None or node.text is None:
                return ""
            return node.text.decode("utf-8")

        def collect_table_names(node: Node) -> List[str]:
            names: List[str] = []
            for child in node.children:
                if child.type in (
                    "table_name",
                    "qualified_name",
                    "object_name",
                    "identifier",
                ):
                    text = node_text(child).strip()
                    if text:
                        names.append(text)
                names.extend(collect_table_names(child))
            return names

        def walk(node: Node) -> None:
            if node.type in ("common_table_expression", "cte"):
                name_node = node.child_by_field_name("name")
                text = node_text(name_node).strip()
                if text:
                    result.cte_names.append(text)

            if node.type in ("from_clause", "join_clause", "join"):
                tables = collect_table_names(node)
                for t in tables:
                    result.table_refs.append(t)
                    if node.type in ("join_clause", "join"):
                        result.join_tables.append(t)

            for child in node.children:
                walk(child)

        walk(tree.root_node)
        return result

    # ── YAML ────────────────────────────────────────────────────────────────

    def analyze_yaml(self, content: bytes) -> YamlAnalysisResult:
        lang = LanguageRouter.get_language(".yaml")
        if not lang:
            return YamlAnalysisResult(warnings=["yaml grammar not available"])

        self.parser.language = lang
        tree = self.parser.parse(content)
        result = YamlAnalysisResult()

        if tree.root_node.has_error:
            result.warnings.append("tree-sitter parse errors detected in yaml file")

        def node_text(node: Optional[Node]) -> str:
            if node is None or node.text is None:
                return ""
            return node.text.decode("utf-8").strip().strip('"')

        def walk(node: Node, path: List[str]) -> None:
            if node.type in ("block_mapping_pair", "flow_mapping_pair"):
                key_node = node.child_by_field_name("key")
                value_node = node.child_by_field_name("value")
                key = node_text(key_node)
                if key:
                    new_path = path + [key]
                    result.key_paths.append(".".join(new_path))
                    if value_node is not None:
                        walk(value_node, new_path)
                    return

            for child in node.children:
                walk(child, path)

        walk(tree.root_node, [])
        return result

    # ── Python import resolution helpers ─────────────────────────────────────

    @staticmethod
    def _module_name_from_path(path: str) -> str:
        normalized = path.replace("/", ".")
        if normalized.endswith(".py"):
            normalized = normalized[:-3]
        if normalized.endswith(".__init__"):
            return normalized[: -len(".__init__")]
        return normalized

    @staticmethod
    def _candidate_modules(import_stmt: str, current_package: str) -> List[str]:
        try:
            tree = ast.parse(import_stmt)
        except SyntaxError:
            return []

        if len(tree.body) != 1:
            return []
        stmt = tree.body[0]
        candidates: List[str] = []

        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                candidates.append(alias.name)
            return candidates

        if isinstance(stmt, ast.ImportFrom):
            base_module = TreeSitterAnalyzer._resolve_relative_module(
                current_package=current_package, level=stmt.level, module=stmt.module
            )
            if base_module:
                candidates.append(base_module)
                for alias in stmt.names:
                    if alias.name == "*":
                        continue
                    candidates.append(f"{base_module}.{alias.name}")
            return candidates

        return candidates

    @staticmethod
    def _resolve_relative_module(
        current_package: str, level: int, module: str | None
    ) -> str | None:
        if level == 0:
            return module

        if not current_package:
            return module

        package_parts = current_package.split(".")
        go_up = level - 1
        if go_up > len(package_parts):
            return module

        anchor = package_parts[: len(package_parts) - go_up]
        if module:
            return ".".join(anchor + [module])
        return ".".join(anchor)

    @staticmethod
    def _resolve_module_to_path(
        module_name: str, module_index: dict[str, str]
    ) -> str | None:
        if module_name in module_index:
            return module_index[module_name]

        parts = module_name.split(".")
        while len(parts) > 1:
            parts = parts[:-1]
            candidate = ".".join(parts)
            if candidate in module_index:
                return module_index[candidate]

        return None
