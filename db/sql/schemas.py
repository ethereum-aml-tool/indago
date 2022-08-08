from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class HaircutBase(BaseModel):
    address: str
    balance: Optional[float] = None
    taint: Optional[float] = None


class Haircut(HaircutBase):
    class Config:
        orm_mode = True


class PoisonBase(BaseModel):
    address: str
    flagged: Optional[bool] = None


class Poison(PoisonBase):
    class Config:
        orm_mode = True


class FifoBase(BaseModel):
    address: str
    tainted: Optional[float] = None
    untainted: Optional[float] = None


class Fifo(FifoBase):
    class Config:
        orm_mode = True


class SeniorityBase(BaseModel):
    address: str
    tainted_balance: Optional[float] = None


class Seniority(SeniorityBase):
    class Config:
        orm_mode = True


class BlacklistSummary(BaseModel):
    address: str
    haircut_taint: Optional[float] = None
    poison_taint: Optional[bool] = None
    fifo_taint: Optional[float] = None
    seniority_taint: Optional[float] = None
    risk_level: Optional[int] = None