from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator


class NodeType(str, Enum):
    MODULE = "module"
    DATASET = "dataset"
    FUNCTION = "function"
    TRANSFORMATION = "transformation"
    CONFIG = "config"


class EdgeType(str, Enum):
    IMPORTS = "imports"
    PRODUCES = "produces"
    CONSUMES = "consumes"
    CALLS = "calls"
    CONFIGURES = "configures"


class StorageType(str, Enum):
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    UNKNOWN = "unknown"


class ModuleNode(BaseModel):
    id: str
    type: NodeType = NodeType.MODULE
    path: str
    language: str
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None  # confirmed existing field
    complexity_score: Optional[float] = None
    change_velocity_30d: Optional[int] = None
    is_dead_code_candidate: Optional[bool] = None
    last_modified: Optional[datetime] = None
    line_range: Optional[str] = None
    # Day 3 additions
    doc_drift: bool = False
    symbol_line_map: Dict[str, int] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None


class DatasetNode(BaseModel):
    id: str
    type: NodeType = NodeType.DATASET
    name: str
    storage_type: StorageType = StorageType.UNKNOWN
    schema_snapshot: Optional[Dict[str, str]] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: Optional[bool] = None


class FunctionNode(BaseModel):
    id: str
    type: NodeType = NodeType.FUNCTION
    qualified_name: str
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: Optional[int] = None
    is_public_api: Optional[bool] = None


class WarningSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class WarningRecord(BaseModel):
    code: str
    message: str
    file: Optional[str] = None
    line: Optional[int] = None
    analyzer: str
    severity: WarningSeverity = WarningSeverity.WARNING


class TransformationNode(BaseModel):
    id: str
    type: NodeType = NodeType.TRANSFORMATION
    source_datasets: List[str]
    target_datasets: List[str]
    transformation_type: str
    source_file: str
    line_range: str
    sql_query_if_applicable: Optional[str] = None


class ConfigNode(BaseModel):
    id: str
    type: NodeType = NodeType.CONFIG
    path: str


Node = Union[ModuleNode, DatasetNode, FunctionNode, TransformationNode, ConfigNode]


class Edge(BaseModel):
    source: str
    target: str
    type: EdgeType
    weight: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


# ── Day 3: Citation + Answer schemas ─────────────────────────────────────────


class Citation(BaseModel):
    """A grounded evidence reference. method distinguishes static analysis from LLM inference."""

    file: str
    line_range: str
    method: Literal["static_analysis", "llm_inference"]


class AnswerWithCitation(BaseModel):
    """A Day-One answer with mandatory evidence citations. Empty citations list is rejected."""

    answer: str
    citations: List[Citation] = Field(min_length=1)
    confidence: Literal["observed", "inferred"]

    @field_validator("citations")
    @classmethod
    def citations_must_not_be_empty(cls, v: List[Citation]) -> List[Citation]:
        if not v:
            raise ValueError(
                "citations must contain at least one Citation — uncited answers are not allowed"
            )
        return v


# ── Day 3: Audit trace schema ─────────────────────────────────────────────────


class TraceEntry(BaseModel):
    """One logged action from any agent. Written to cartography_trace.jsonl."""

    timestamp: datetime
    agent: str
    action: str
    evidence_source: Literal["static_analysis", "llm_inference", "config_parse"]
    confidence: Literal["observed", "inferred"]
    file: Optional[str] = None
    detail: Optional[str] = None


# ── Graph container ───────────────────────────────────────────────────────────


class GraphSchema(BaseModel):
    nodes: Dict[str, Node]
    edges: List[Edge]
    warnings: List[WarningRecord] = Field(default_factory=list)
    # Day 3 additions — serialized alongside graph
    day_one_answers: Dict[str, AnswerWithCitation] = Field(default_factory=dict)
    trace_entries: List[TraceEntry] = Field(default_factory=list)
