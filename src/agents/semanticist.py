"""
Semanticist Agent — Day 3

Adds semantic understanding to the KnowledgeGraph using LLMs:
  1. LLM client abstraction (SemanticistLLMClient + FakeLLMClient for tests)
  2. ContextWindowBudget with concrete tokenization (char/4, tiktoken optional)
  3. Source-code truncation policy with CODE_TRUNCATED warnings
  4. Purpose statement generation (code-grounded, never docstring)
  5. Documentation drift detection (DOC_DRIFT warnings)
  6. Domain clustering (text-embedding-3-small + k-means, seed=42)
  7. Day-One question answering with mandatory evidence citations
"""

from __future__ import annotations

import ast
import os
import re
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

import httpx
from dotenv import load_dotenv

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schema import (
    AnswerWithCitation,
    Citation,
    ModuleNode,
    TraceEntry,
    WarningRecord,
    WarningSeverity,
)

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────

EMBED_MODEL = os.getenv("CARTOGRAPHER_EMBED_MODEL", "openai/text-embedding-3-small")
EMBED_DIM = 1536

MAX_SOURCE_BYTES = 32_000  # truncate modules beyond this size
KMEANS_SEED = 42  # fixed seed for deterministic clustering
KMEANS_K_MIN = 5
KMEANS_K_MAX = 8

MODEL_BULK = os.getenv("CARTOGRAPHER_MODEL_BULK", "qwen/qwen-2.5-7b-instruct:free")
MODEL_SYNTHESIS = os.getenv(
    "CARTOGRAPHER_MODEL_SYNTHESIS", "mistralai/mistral-small-24b-instruct-2501:free"
)

OPENROUTER_BASE = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
_OPENROUTER_SITE_URL = os.getenv("OPENROUTER_SITE_URL")
_OPENROUTER_APP_TITLE = os.getenv("OPENROUTER_APP_TITLE")

_FDE_QUESTIONS = [
    "What is the primary data ingestion path?",
    "What are the 3-5 most critical output datasets or endpoints?",
    "What is the blast radius if the most critical module fails?",
    "Where is the business logic concentrated versus distributed?",
    "What has changed most frequently in the last 90 days?",
]


# ── LLM client abstraction ────────────────────────────────────────────────────


@dataclass
class LLMResponse:
    text: str
    tokens_in: int
    tokens_out: int
    model: str


class SemanticistLLMClient(Protocol):
    """Protocol — all Semanticist methods accept this; swap real/fake for tests."""

    def complete(
        self, prompt: str, model: str, max_tokens: int = 1024
    ) -> LLMResponse: ...

    def embed(
        self, texts: List[str], model: str = EMBED_MODEL
    ) -> List[List[float]]: ...


class OpenRouterLLMClient:
    """Real implementation backed by OpenRouter."""

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self._key:
            raise ValueError(
                "OPENROUTER_API_KEY not set. Add it to your .env file or environment."
            )
        self._headers = {"Authorization": f"Bearer {self._key}"}
        if _OPENROUTER_SITE_URL:
            self._headers["HTTP-Referer"] = _OPENROUTER_SITE_URL
        if _OPENROUTER_APP_TITLE:
            self._headers["X-Title"] = _OPENROUTER_APP_TITLE
        self._min_interval = (
            float(os.getenv("CARTOGRAPHER_LLM_MIN_INTERVAL_MS", "0")) / 1000.0
        )
        self._retry_backoff_s = float(
            os.getenv("CARTOGRAPHER_LLM_RETRY_BACKOFF_S", "5")
        )
        self._last_call_ts = 0.0
        self._http = httpx.Client(timeout=60.0)

    def complete(self, prompt: str, model: str, max_tokens: int = 1024) -> LLMResponse:
        if self._min_interval > 0:
            now = time.monotonic()
            wait_s = self._min_interval - (now - self._last_call_ts)
            if wait_s > 0:
                time.sleep(wait_s)
        for attempt in range(5):
            resp = self._http.post(
                f"{OPENROUTER_BASE}/chat/completions",
                headers=self._headers,
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                },
            )
            if resp.status_code == 429 and attempt < 4:
                time.sleep(self._retry_backoff_s * (attempt + 1))
                continue
            resp.raise_for_status()
            break
        self._last_call_ts = time.monotonic()
        body = resp.json()
        text = body["choices"][0]["message"]["content"]
        usage = body.get("usage", {})
        return LLMResponse(
            text=text,
            tokens_in=usage.get("prompt_tokens", 0),
            tokens_out=usage.get("completion_tokens", 0),
            model=model,
        )

    def embed(self, texts: List[str], model: str = EMBED_MODEL) -> List[List[float]]:
        if self._min_interval > 0:
            now = time.monotonic()
            wait_s = self._min_interval - (now - self._last_call_ts)
            if wait_s > 0:
                time.sleep(wait_s)
        resp = self._http.post(
            f"{OPENROUTER_BASE}/embeddings",
            headers=self._headers,
            json={"model": model, "input": texts},
        )
        resp.raise_for_status()
        self._last_call_ts = time.monotonic()
        data = resp.json()["data"]
        return [item["embedding"] for item in data]


@dataclass
class FakeLLMClient:
    """
    Test double — zero network calls.
    Configure canned responses via `responses` (list, consumed in order, last repeated).
    Configure canned embeddings via `embeddings_response`.
    Calls are recorded in `calls`.
    """

    responses: List[str] = field(default_factory=lambda: ["Fake purpose statement."])
    embeddings_response: Optional[List[List[float]]] = None
    calls: List[Dict[str, Any]] = field(default_factory=list)
    _call_idx: int = field(default=0, init=False, repr=False)

    def complete(self, prompt: str, model: str, max_tokens: int = 1024) -> LLMResponse:
        idx = min(self._call_idx, len(self.responses) - 1)
        text = self.responses[idx]
        self._call_idx += 1
        self.calls.append({"type": "complete", "model": model, "prompt": prompt[:80]})
        # Estimate tokens the same way ContextWindowBudget does
        tokens_in = estimate_tokens(prompt, model)
        tokens_out = estimate_tokens(text, model)
        return LLMResponse(
            text=text, tokens_in=tokens_in, tokens_out=tokens_out, model=model
        )

    def embed(self, texts: List[str], model: str = EMBED_MODEL) -> List[List[float]]:
        self.calls.append({"type": "embed", "model": model, "n": len(texts)})
        if self.embeddings_response is not None:
            return self.embeddings_response
        # Deterministic fake: hash-based unit vectors per text
        import hashlib

        result = []
        for t in texts:
            seed = int(hashlib.md5(t.encode()).hexdigest(), 16) % (2**31)
            import random

            rng = random.Random(seed)
            vec = [rng.gauss(0, 1) for _ in range(EMBED_DIM)]
            norm = sum(x**2 for x in vec) ** 0.5 or 1.0
            result.append([x / norm for x in vec])
        return result


# ── Tokenization ──────────────────────────────────────────────────────────────


def estimate_tokens(text: str, model: str = "") -> int:
    """
    Estimate token count.
    - Gemini models: always use char/4 (tiktoken doesn't support Gemini).
    - OpenAI models (gpt-*, cl100k-based): use tiktoken if installed.
    - Default fallback: char/4.
    """
    if not text:
        return 0
    is_openai = any(prefix in model for prefix in ("gpt-", "cl100k", "text-embedding"))
    if is_openai:
        try:
            import tiktoken  # type: ignore[import-not-found]

            enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
            return len(enc.encode(text))
        except Exception:
            pass
    return max(1, len(text) // 4)


# ── Budget ────────────────────────────────────────────────────────────────────


class ContextWindowBudget:
    """Tracks cumulative token spend and enforces a hard cap."""

    def __init__(self, max_tokens: int = 500_000) -> None:
        self.max_tokens = max_tokens
        self.used: int = 0
        self.exhausted: bool = False

    def charge(self, resp: LLMResponse) -> None:
        self.used += resp.tokens_in + resp.tokens_out
        if self.used >= self.max_tokens:
            self.exhausted = True

    def remaining(self) -> int:
        return max(0, self.max_tokens - self.used)

    def check(self) -> bool:
        """Return True if budget is still available."""
        return not self.exhausted


# ── Source-code truncation ────────────────────────────────────────────────────


def truncate_source(
    source: str, filepath: str, warnings: List[WarningRecord]
) -> Tuple[str, bool]:
    """
    Truncate source code to MAX_SOURCE_BYTES, preserving whole lines.
    Returns (possibly-truncated source, was_truncated).
    Emits CODE_TRUNCATED warning if truncated.
    """
    encoded = source.encode("utf-8")
    if len(encoded) <= MAX_SOURCE_BYTES:
        return source, False

    truncated_bytes = encoded[:MAX_SOURCE_BYTES]
    # Find last newline to preserve whole lines
    last_newline = truncated_bytes.rfind(b"\n")
    if last_newline > 0:
        truncated_bytes = truncated_bytes[: last_newline + 1]
    truncated = truncated_bytes.decode("utf-8", errors="replace")
    warnings.append(
        WarningRecord(
            code="CODE_TRUNCATED",
            message=(
                f"Source too large ({len(encoded)} bytes > {MAX_SOURCE_BYTES}). "
                "Truncated to first lines — outputs marked as confidence=inferred."
            ),
            file=filepath,
            analyzer="Semanticist",
            severity=WarningSeverity.WARNING,
        )
    )
    return truncated, True


# ── Symbol line map ───────────────────────────────────────────────────────────


def build_symbol_line_map(source: str) -> Dict[str, int]:
    """
    Parse source and map function/class names to their start line numbers.
    This is the source of line_range in citations.
    Returns {} on parse failure (never raises).
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return {}
    result: Dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            result[node.name] = node.lineno
    return result


# ── Docstring extraction ──────────────────────────────────────────────────────


def extract_module_docstring(source: str) -> Optional[str]:
    """Extract the module-level docstring using ast.get_docstring."""
    try:
        tree = ast.parse(source)
        return ast.get_docstring(tree)
    except SyntaxError:
        return None


# ── Model routing ─────────────────────────────────────────────────────────────


def route_model(task_type: str) -> str:
    """Return the correct model for a given task_type: 'bulk' or 'synthesis'."""
    if task_type == "synthesis":
        return MODEL_SYNTHESIS
    return MODEL_BULK


# ── TraceLogger ───────────────────────────────────────────────────────────────


class TraceLogger:
    """Appends structured entries to the KnowledgeGraph's trace_entries list."""

    def __init__(self, kg: KnowledgeGraph) -> None:
        self._kg = kg

    def log(
        self,
        agent: str,
        action: str,
        evidence_source: str,
        confidence: str,
        file: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        entry = TraceEntry(
            timestamp=datetime.now(tz=timezone.utc),
            agent=agent,
            action=action,
            evidence_source=evidence_source,  # type: ignore[arg-type]
            confidence=confidence,  # type: ignore[arg-type]
            file=file,
            detail=detail,
        )
        self._kg.add_trace_entry(entry)


# ── Answer cleanup ────────────────────────────────────────────────────────────


def _clean_answer_text(answer_text: str, question: str | None = None) -> str:
    """Remove echoed question lines and inline file citations from answers."""
    text = answer_text.strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if (
        question
        and lines
        and lines[0].rstrip("?").lower() == question.rstrip("?").lower()
    ):
        lines = lines[1:]
    # Remove inline file citations (we render evidence separately)
    cleaned = []
    for ln in lines:
        ln = re.sub(r"file:[\\w./\\-_]+:L\\d+-\\d+", "", ln)
        # Remove any bracketed method/file annotations
        ln = re.sub(r"\[[^\]]*method:[^\]]*\]", "", ln)
        ln = re.sub(r"\[[^\]]*file:[^\]]*\]", "", ln)
        # Remove parenthetical method annotations
        ln = re.sub(r"\(method:[^)]+\)", "", ln)
        ln = re.sub(r"method:(static_analysis|llm_inference)", "", ln)
        cleaned.append(ln.strip())
    return " ".join([ln for ln in cleaned if ln]).strip()


# ── Semanticist ───────────────────────────────────────────────────────────────


class Semanticist:
    """
    Enriches a KnowledgeGraph with LLM-derived semantic understanding.
    All methods accept a client: SemanticistLLMClient for DI/testing.
    """

    def __init__(
        self,
        client: SemanticistLLMClient,
        budget: Optional[ContextWindowBudget] = None,
        on_budget_exceeded: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._client = client
        self._budget = budget or ContextWindowBudget()
        self._on_budget_exceeded = on_budget_exceeded

    # ── Internal: LLM call with budget tracking + trace ───────────────────────

    def _call(
        self,
        prompt: str,
        task_type: str,
        tracer: TraceLogger,
        filepath: Optional[str] = None,
    ) -> Optional[LLMResponse]:
        if not self._budget.check():
            return None
        model = route_model(task_type)
        try:
            resp = self._client.complete(prompt, model=model)
        except Exception as exc:
            tracer.log(
                agent="Semanticist",
                action="llm_error",
                evidence_source="llm_inference",
                confidence="inferred",
                file=filepath,
                detail=str(exc),
            )
            return None
        self._budget.charge(resp)
        tracer.log(
            agent="Semanticist",
            action=f"llm_{task_type}",
            evidence_source="llm_inference",
            confidence="inferred",
            file=filepath,
            detail=f"model={resp.model} in={resp.tokens_in} out={resp.tokens_out}",
        )
        return resp

    # ── Purpose statement + doc drift ─────────────────────────────────────────

    def generate_purpose_statement(
        self,
        module_node: ModuleNode,
        source_code: str,
        kg: KnowledgeGraph,
        tracer: TraceLogger,
    ) -> Optional[str]:
        """
        Generate a code-grounded purpose statement for a module.
        Source code is always the primary evidence — docstring is never used as input.
        """
        local_warnings: List[WarningRecord] = []
        source, was_truncated = truncate_source(
            source_code, module_node.path, local_warnings
        )
        for w in local_warnings:
            kg.add_warning(w)
        confidence = "inferred" if was_truncated else "observed"

        # Update symbol line map on the node (stored in-graph via add_node)
        line_map = build_symbol_line_map(source)
        # Store directly on the node data in the networkx graph
        if module_node.id in kg.graph.nodes:
            kg.graph.nodes[module_node.id]["symbol_line_map"] = line_map

        if not self._budget.check():
            kg.add_warning(
                WarningRecord(
                    code="BUDGET_EXCEEDED",
                    message=f"Token budget exhausted — skipping purpose statement for {module_node.path}",
                    file=module_node.path,
                    analyzer="Semanticist",
                    severity=WarningSeverity.WARNING,
                )
            )
            return None

        lang = (module_node.language or "python").lower()
        if lang == "python":
            prompt = (
                "You are a senior data engineering analyst. "
                "Analyse the Python source code below and write a 2-3 sentence purpose statement.\n"
                "Rules:\n"
                "- Base your answer ONLY on the code, not on any docstring.\n"
                "- Cover: (1) business function, (2) key inputs/outputs, (3) what dependencies this module bridges.\n"
                "- Do NOT quote or mirror docstrings.\n\n"
                f"File: {module_node.path}\n\n"
                f"```python\n{source}\n```\n\n"
                "Purpose statement:"
            )
        elif lang == "sql":
            prompt = (
                "You are a senior data engineering analyst. "
                "Analyse the SQL/dbt model below and write a 2-3 sentence purpose statement.\n"
                "Rules:\n"
                "- Base your answer ONLY on the SQL.\n"
                "- Cover: (1) business function, (2) key inputs/outputs, (3) key transformations.\n"
                "- Keep it concrete and avoid generic phrasing.\n\n"
                f"File: {module_node.path}\n\n"
                f"```sql\n{source}\n```\n\n"
                "Purpose statement:"
            )
        else:
            prompt = (
                "You are a senior data engineering analyst. "
                "Analyse the configuration below and write a 2-3 sentence purpose statement.\n"
                "Rules:\n"
                "- Base your answer ONLY on the configuration contents.\n"
                "- Cover: (1) what this config defines, (2) key entities, (3) how it fits into the pipeline.\n\n"
                f"File: {module_node.path}\n\n"
                f"```yaml\n{source}\n```\n\n"
                "Purpose statement:"
            )
        resp = self._call(prompt, "bulk", tracer, filepath=module_node.path)
        if resp is None:
            return None

        purpose = resp.text.strip()

        # Doc drift detection
        if lang == "python":
            existing_doc = extract_module_docstring(source_code)
            if existing_doc:
                drift_prompt = (
                    "Compare these two descriptions of the same Python module.\n"
                    "Reply with exactly one word: MATCH if they describe the same behaviour, "
                    "DRIFT if they contradict each other.\n\n"
                    f"Generated description:\n{purpose}\n\n"
                    f"Existing docstring:\n{existing_doc}\n\n"
                    "Verdict:"
                )
                drift_resp = self._call(
                    drift_prompt, "bulk", tracer, filepath=module_node.path
                )
                if drift_resp and "DRIFT" in drift_resp.text.upper():
                    kg.add_warning(
                        WarningRecord(
                            code="DOC_DRIFT",
                            message=(
                                f"Documentation drift detected in {module_node.path}.\n"
                                f"Generated: {purpose}\n"
                                f"Existing docstring: {existing_doc}"
                            ),
                            file=module_node.path,
                            analyzer="Semanticist",
                            severity=WarningSeverity.WARNING,
                        )
                    )
                    # Update node data
                    if module_node.id in kg.graph.nodes:
                        kg.graph.nodes[module_node.id]["doc_drift"] = True
                    return purpose  # still return the good statement

        _ = confidence  # used to inform callers if needed
        return purpose

    def _fallback_purpose_statement(
        self, module_node: ModuleNode, source_code: str
    ) -> str:
        """Heuristic purpose statement when LLM is unavailable."""
        lang = (module_node.language or "python").lower()
        path = module_node.path or "unknown"
        if lang == "sql":
            refs = re.findall(
                r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", source_code
            )
            sources = re.findall(
                r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
                source_code,
            )
            deps = refs + [f"{s}.{t}" for s, t in sources]
            target = Path(path).stem
            if deps:
                return (
                    f"SQL model that builds `{target}` from upstream sources "
                    f"{', '.join(sorted(set(deps)))}."
                )
            return f"SQL model that defines `{target}`; upstream sources not resolved."
        if lang in ("yaml", "yml"):
            if "sources:" in source_code:
                return "YAML config defining dbt sources and dataset metadata."
            if "models:" in source_code:
                return "YAML config defining dbt model metadata and tests."
            return "YAML configuration file supporting the data pipeline."
        return f"Module `{path}`; purpose not inferred (LLM unavailable)."

    # ── Domain clustering ─────────────────────────────────────────────────────

    def cluster_into_domains(
        self, module_nodes: List[ModuleNode], tracer: TraceLogger
    ) -> Dict[str, List[str]]:
        """
        Embed purpose statements and run k-means to group modules into domain clusters.
        Returns domain_map: {domain_label: [module_path, ...]}
        """
        if os.getenv("CARTOGRAPHER_DISABLE_EMBEDDINGS", "").lower() in {
            "1",
            "true",
            "yes",
        }:
            tracer._kg.add_warning(
                WarningRecord(
                    code="EMBED_DISABLED",
                    message="Embeddings disabled via CARTOGRAPHER_DISABLE_EMBEDDINGS.",
                    analyzer="Semanticist",
                    severity=WarningSeverity.WARNING,
                )
            )
            return {}
        from sklearn.cluster import KMeans  # type: ignore[import-untyped]

        eligible = [m for m in module_nodes if m.purpose_statement]
        if not eligible:
            return {}

        # Embed all purpose statements
        texts = [m.purpose_statement for m in eligible if m.purpose_statement]
        tracer.log(
            agent="Semanticist",
            action="embed_purpose_statements",
            evidence_source="llm_inference",
            confidence="inferred",
            detail=f"model={EMBED_MODEL} n={len(texts)}",
        )
        try:
            embeddings = self._client.embed(texts, model=EMBED_MODEL)
        except Exception as exc:
            tracer._kg.add_warning(
                WarningRecord(
                    code="EMBED_ERROR",
                    message=str(exc),
                    analyzer="Semanticist",
                    severity=WarningSeverity.WARNING,
                )
            )
            return {}

        k = min(max(KMEANS_K_MIN, len(eligible) // 2), KMEANS_K_MAX, len(eligible))
        kmeans = KMeans(n_clusters=k, random_state=KMEANS_SEED, n_init=10)
        try:
            labels = kmeans.fit_predict(embeddings)
        except Exception as exc:
            tracer._kg.add_warning(
                WarningRecord(
                    code="CLUSTER_ERROR",
                    message=str(exc),
                    analyzer="Semanticist",
                    severity=WarningSeverity.WARNING,
                )
            )
            return {}

        # Group purpose statements by cluster for label generation
        cluster_groups: Dict[int, List[Tuple[ModuleNode, str]]] = {}
        for idx, (mod, label) in enumerate(zip(eligible, labels)):
            cluster_groups.setdefault(int(label), []).append((mod, texts[idx]))

        domain_map: Dict[str, List[str]] = {}
        for cluster_id, members in cluster_groups.items():
            statements = "\n".join(f"- {stmt}" for _, stmt in members)
            label_prompt = (
                "Given these module descriptions, produce a 2-3 word domain label "
                "(e.g. 'data ingestion', 'schema validation', 'reporting').\n"
                f"{statements}\n\nDomain label:"
            )
            resp = self._call(label_prompt, "bulk", tracer)
            label = resp.text.strip().lower() if resp else f"domain_{cluster_id}"
            domain_map[label] = [mod.path for mod, _ in members]

            # Store label on each node in the graph
            for mod, _ in members:
                if mod.id in tracer._kg.graph.nodes:
                    tracer._kg.graph.nodes[mod.id]["domain_cluster"] = label

        tracer.log(
            agent="Semanticist",
            action="domain_clustering_complete",
            evidence_source="llm_inference",
            confidence="inferred",
            detail=f"k={k} domains={list(domain_map.keys())}",
        )
        return domain_map

    # ── Day-One answers ───────────────────────────────────────────────────────

    def answer_day_one_questions(
        self,
        kg: KnowledgeGraph,
        tracer: TraceLogger,
        find_sources_fn: Optional[Callable[..., List[str]]] = None,
        find_sinks_fn: Optional[Callable[..., List[str]]] = None,
        pagerank_top5: Optional[List[str]] = None,
    ) -> Dict[str, AnswerWithCitation]:
        """
        Generate answers to the Five FDE Day-One Questions using synthesis-tier model.
        blast_radius/find_sources/find_sinks are called from Hydrologist — passed in as callables.
        Each answer must contain ≥1 Citation grounded in static evidence.
        """
        # Build context from static evidence
        sources: List[str] = find_sources_fn(kg) if find_sources_fn else []
        sinks: List[str] = find_sinks_fn(kg) if find_sinks_fn else []
        top5: List[str] = pagerank_top5 or []

        # Collect purpose statements + domain clusters from graph nodes
        purpose_lines: List[str] = []
        domain_lines: List[str] = []
        for _, data in kg.graph.nodes(data=True):
            if data.get("type") == "module" and data.get("purpose_statement"):
                vel = data.get("change_velocity_30d", "?")
                purpose_lines.append(
                    f"- {data['path']} (velocity={vel}): {data['purpose_statement']}"
                )
            if data.get("domain_cluster"):
                domain_lines.append(f"  {data['path']} → {data['domain_cluster']}")

        context = "\n".join(
            [
                "=== TOP 5 MODULES BY PAGERANK (Surveyor static analysis) ===",
                "\n".join(f"- {m}" for m in top5) or "(none)",
                "",
                "=== DATA SOURCES (Hydrologist static analysis) ===",
                "\n".join(f"- {s}" for s in sources) or "(none)",
                "",
                "=== DATA SINKS (Hydrologist static analysis) ===",
                "\n".join(f"- {s}" for s in sinks) or "(none)",
                "",
                "=== MODULE PURPOSE STATEMENTS ===",
                "\n".join(purpose_lines) or "(none generated yet)",
                "",
                "=== DOMAIN CLUSTERS ===",
                "\n".join(domain_lines) or "(none)",
            ]
        )

        questions_block = "\n".join(
            f"{i + 1}. {q}" for i, q in enumerate(_FDE_QUESTIONS)
        )

        prompt = (
            "You are an expert data engineer performing a codebase onboarding analysis.\n"
            "Using ONLY the static evidence below, answer all five Day-One FDE questions.\n"
            "For each answer:\n"
            "  - Write 2-4 sentences.\n"
            "  - Cite at least one specific file from the evidence (format: file:path/to/file.py:L1-1).\n"
            "  - Label each citation as method:static_analysis or method:llm_inference.\n"
            "  - Do NOT repeat the question in your answer text.\n"
            "Format: number each answer exactly as Q1: ... Q2: ... Q3: ... Q4: ... Q5: ...\n\n"
            f"=== STATIC EVIDENCE ===\n{context}\n\n"
            f"=== QUESTIONS ===\n{questions_block}\n\n"
            "Answers (cite file:path:LN-M for each fact):"
        )

        use_bulk = os.getenv("CARTOGRAPHER_USE_BULK_FOR_DAY_ONE", "").lower() in {
            "1",
            "true",
            "yes",
        }
        resp = self._call(prompt, "bulk" if use_bulk else "synthesis", tracer)
        if resp is None:
            # Fallback to bulk model to avoid rate limits on synthesis tier
            resp = self._call(prompt, "bulk", tracer)
        if resp is None:
            kg.add_warning(
                WarningRecord(
                    code="LLM_ERROR",
                    message="Day-One answer generation failed — LLM call returned no response.",
                    analyzer="Semanticist",
                    severity=WarningSeverity.ERROR,
                )
            )
            return self._fallback_day_one_answers(kg, top5, sources, sinks)

        raw = resp.text.strip()
        answers = self._parse_day_one_answers(raw, kg, top5)
        if not answers or len(answers) < 5:
            kg.add_warning(
                WarningRecord(
                    code="DAY_ONE_PARSE_ERROR",
                    message="LLM output did not match expected Q1..Q5 format; using heuristic fallback.",
                    analyzer="Semanticist",
                    severity=WarningSeverity.WARNING,
                )
            )
            return self._fallback_day_one_answers(kg, top5, sources, sinks)
        tracer.log(
            agent="Semanticist",
            action="day_one_questions_answered",
            evidence_source="llm_inference",
            confidence="inferred",
            detail=f"n_answers={len(answers)}",
        )
        return answers

    def _fallback_day_one_answers(
        self, kg: KnowledgeGraph, top5: List[str], sources: List[str], sinks: List[str]
    ) -> Dict[str, AnswerWithCitation]:
        """Heuristic Day-One answers when LLM is unavailable."""

        def dataset_names(node_ids: List[str]) -> List[str]:
            names: List[str] = []
            for nid in node_ids:
                data = kg.graph.nodes.get(nid, {})
                if data.get("type") == "dataset":
                    names.append(data.get("name", nid))
            return names

        src_names = dataset_names(sources)
        sink_names = dataset_names(sinks)

        def cite_default() -> List[Citation]:
            if top5:
                return [
                    Citation(
                        file=top5[0],
                        line_range="L1-1",
                        method="static_analysis",
                    )
                ]
            return [
                Citation(
                    file="unknown",
                    line_range="L1-1",
                    method="static_analysis",
                )
            ]

        answers: Dict[str, AnswerWithCitation] = {}
        answers["Q1"] = AnswerWithCitation(
            answer=(
                "Primary ingestion appears to start from sources: "
                + (", ".join(src_names) if src_names else "no sources identified yet.")
            ),
            citations=cite_default(),
            confidence="inferred",
        )
        answers["Q2"] = AnswerWithCitation(
            answer=(
                "Most critical outputs appear to be the final sinks: "
                + (
                    ", ".join(sink_names[:5])
                    if sink_names
                    else "no sinks identified yet."
                )
            ),
            citations=cite_default(),
            confidence="inferred",
        )
        answers["Q3"] = AnswerWithCitation(
            answer=(
                "Blast radius is largest for top-ranked modules; downstream datasets likely impacted include "
                + (
                    ", ".join(sink_names[:5])
                    if sink_names
                    else "the downstream graph is currently unclear."
                )
            ),
            citations=cite_default(),
            confidence="inferred",
        )
        answers["Q4"] = AnswerWithCitation(
            answer=(
                "Business logic appears concentrated in transformation models rather than application code, "
                "based on the prevalence of SQL/dbt models and limited Python modules."
            ),
            citations=cite_default(),
            confidence="inferred",
        )
        answers["Q5"] = AnswerWithCitation(
            answer=(
                "High-velocity files are not available from LLM analysis; see static velocity metrics in CODEBASE."
            ),
            citations=cite_default(),
            confidence="inferred",
        )
        return answers

    def _parse_day_one_answers(
        self, raw: str, kg: KnowledgeGraph, top5: List[str]
    ) -> Dict[str, AnswerWithCitation]:
        """
        Parse the LLM's raw answer block into AnswerWithCitation objects.
        Extracts 'file:path:LN-M' patterns for citations.
        Falls back to citing a top-5 module if no explicit citation found.
        """
        import re

        answers: Dict[str, AnswerWithCitation] = {}
        # Split on Q1:, Q2: ... Q5:
        parts = re.split(r"Q(\d):", raw)
        # parts = ['', '1', 'answer1', '2', 'answer2', ...]
        for i in range(1, len(parts), 2):
            q_num = int(parts[i])
            answer_text = parts[i + 1].strip() if i + 1 < len(parts) else ""
            q_key = f"Q{q_num}"

            # Extract file citations: file:path/to/file.py:L1-42
            cite_pattern = re.findall(
                r"file:([\w./\-_]+\.(?:py|sql|yaml|yml)):L(\d+)-(\d+)", answer_text
            )
            citations: List[Citation] = [
                Citation(
                    file=path,
                    line_range=f"L{start}-{end}",
                    method="llm_inference",
                )
                for path, start, end in cite_pattern
            ]

            # If LLM only provided L1-1, upgrade to module line range when available
            upgraded: List[Citation] = []
            for c in citations:
                if c.line_range == "L1-1":
                    node = kg.graph.nodes.get(c.file)
                    if node and node.get("line_range"):
                        upgraded.append(
                            Citation(
                                file=c.file,
                                line_range=node["line_range"],
                                method=c.method,
                            )
                        )
                        continue
                upgraded.append(c)
            citations = upgraded

            # Ensure at least one static citation from top5 modules
            if not citations and top5:
                line_range = "L1-1"
                node = kg.graph.nodes.get(top5[0])
                if node and node.get("line_range"):
                    line_range = node["line_range"]
                citations = [
                    Citation(
                        file=top5[0],
                        line_range=line_range,
                        method="static_analysis",
                    )
                ]
            elif not citations:
                # Last resort — mark as inferred with no file
                citations = [
                    Citation(
                        file="unknown",
                        line_range="L1-1",
                        method="llm_inference",
                    )
                ]
                kg.add_warning(
                    WarningRecord(
                        code="UNCITED_ANSWER",
                        message=f"No file citation found for {q_key} — inserted placeholder.",
                        analyzer="Semanticist",
                        severity=WarningSeverity.WARNING,
                    )
                )

            answers[q_key] = AnswerWithCitation(
                answer=_clean_answer_text(
                    answer_text,
                    question=_FDE_QUESTIONS[q_num - 1]
                    if 0 < q_num <= len(_FDE_QUESTIONS)
                    else None,
                ),
                citations=citations,
                confidence="inferred",
            )

        return answers

    # ── Main run ──────────────────────────────────────────────────────────────

    def run(
        self,
        kg: KnowledgeGraph,
        source_map: Optional[Dict[str, str]] = None,
        find_sources_fn: Optional[Callable[..., List[str]]] = None,
        find_sinks_fn: Optional[Callable[..., List[str]]] = None,
        pagerank_top5: Optional[List[str]] = None,
    ) -> None:
        """
        Full Semanticist pipeline:
          1. Purpose statements + doc drift for all module nodes
          2. Domain clustering
          3. Day-One answers
        Writes everything back into kg (in-memory).
        source_map: {module_path: source_code} — caller supplies this.
        """
        tracer = TraceLogger(kg)
        source_map = source_map or {}

        module_nodes: List[ModuleNode] = []
        for _, data in kg.graph.nodes(data=True):
            if data.get("type") == "module":
                try:
                    node = ModuleNode.model_validate(data)
                    if os.getenv("CARTOGRAPHER_SKIP_YAML", "").lower() in {
                        "1",
                        "true",
                        "yes",
                    } and (node.language or "").lower() in {"yaml", "yml"}:
                        continue
                    module_nodes.append(node)
                except Exception:
                    continue

        # Step 1: purpose statements
        for mod in module_nodes:
            if not self._budget.check():
                kg.add_warning(
                    WarningRecord(
                        code="BUDGET_EXCEEDED",
                        message="Token budget exhausted — stopping purpose statement generation.",
                        analyzer="Semanticist",
                        severity=WarningSeverity.WARNING,
                    )
                )
                break
            src = source_map.get(mod.path, "")
            purpose = self.generate_purpose_statement(mod, src, kg, tracer)
            if purpose and mod.id in kg.graph.nodes:
                kg.graph.nodes[mod.id]["purpose_statement"] = purpose

        # Step 2: domain clustering
        # Refresh module_nodes with updated purpose statements
        updated_nodes: List[ModuleNode] = []
        for _, data in kg.graph.nodes(data=True):
            if data.get("type") == "module":
                try:
                    updated_nodes.append(ModuleNode.model_validate(data))
                except Exception:
                    continue
        domain_map = self.cluster_into_domains(updated_nodes, tracer)
        # Store domain_map as a graph attribute
        kg.graph.graph["domain_map"] = domain_map

        # Step 3: Day-One answers
        answers = self.answer_day_one_questions(
            kg,
            tracer,
            find_sources_fn=find_sources_fn,
            find_sinks_fn=find_sinks_fn,
            pagerank_top5=pagerank_top5,
        )
        kg.set_day_one_answers(answers)
