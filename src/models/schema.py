from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel


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
    domain_cluster: Optional[str] = None
    complexity_score: Optional[float] = None
    change_velocity_30d: Optional[int] = None
    is_dead_code_candidate: Optional[bool] = None
    last_modified: Optional[datetime] = None


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


class GraphSchema(BaseModel):
    nodes: Dict[str, Node]
    edges: List[Edge]
