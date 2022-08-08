from typing import Optional, List

from pydantic import BaseModel, EmailStr, Field

# Cluster
class Edge(BaseModel):
    from_: int = Field(..., alias='from')
    to: int = Field(...)


class DARGraph(BaseModel):
    id: int = Field(..., alias='_id')
    nodes: List[dict]
    edges: List[Edge]


class DARAddressMapping(BaseModel):
    id: str = Field(..., alias='_id')
    cluster_id: int

# Blacklist
class Poison(BaseModel):
    id: str = Field(..., alias='_id')
    flagged: bool
