from typing import List, Optional

import tree_sitter_python
import tree_sitter_sql
import tree_sitter_yaml
import tree_sitter_javascript
from tree_sitter import Language, Parser, Node


class LanguageRouter:
    @staticmethod
    def get_language(ext: str) -> Optional[Language]:
        ext = ext.lower()
        if ext == ".py":
            return Language(tree_sitter_python.language())
        elif ext == ".sql":
            return Language(tree_sitter_sql.language())
        elif ext in (".yaml", ".yml"):
            return Language(tree_sitter_yaml.language())
        elif ext in (".js", ".jsx", ".ts", ".tsx"):
            return Language(tree_sitter_javascript.language())
        return None


class TreeSitterAnalyzer:
    def __init__(self) -> None:
        self.parser = Parser()

    def analyze_python_file(
        self, path: str, content: bytes
    ) -> tuple[List[str], List[str]]:
        """Parse python file and extract imports and public functions/classes."""
        lang = LanguageRouter.get_language(".py")
        if not lang:
            return [], []

        self.parser.language = lang
        tree = self.parser.parse(content)

        imports = []
        public_symbols = []

        # Simple tree walking for MVP
        def walk(node: Node) -> None:
            if node.type in ("import_statement", "import_from_statement"):
                # We can use queries properly later, but for MVP we just extract the text
                text = node.text
                if text is not None:
                    imports.append(text.decode("utf-8"))

            if node.type in ("function_definition", "class_definition"):
                name_node = node.child_by_field_name("name")
                if name_node is not None:
                    name_text = name_node.text
                    if name_text is not None:
                        name = name_text.decode("utf-8")
                        if not name.startswith("_") or name.startswith("__"):
                            public_symbols.append(name)

            for child in node.children:
                walk(child)

        walk(tree.root_node)
        return imports, public_symbols
