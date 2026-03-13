from typing import List, Optional
from pydantic import BaseModel


class AnalysisRequest(BaseModel):
    repo_url: str
    incremental: bool = False


class QueryRequest(BaseModel):
    cartography_dir: str
    query: str


class CitationModel(BaseModel):
    file: str
    line_range: str
    method: str


class QueryResponse(BaseModel):
    answer: str
    citations: List[CitationModel]


class AgentProgress(BaseModel):
    timestamp: str
    agent: str
    action: str
    evidence_source: str
    confidence: str
    file: Optional[str] = None
    detail: Optional[str] = None
