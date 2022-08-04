from unicodedata import numeric
from numpy import block
from sqlalchemy import Boolean, Column, ForeignKey, Integer, Numeric, String, Float, BigInteger, DateTime
from sqlalchemy.orm import relationship

from .database import Base


class Account(Base):
    __tablename__ = "accounts"

    address = Column(String, primary_key=True, index=True)
    balance = Column(Float)
    risk_level = Column(Integer)


class Transaction(Base):
    __tablename__ = "transactions"

    hash = Column(String, primary_key=True)
    value = Column(Float)
    block_number = Column(Integer)
    block_timestamp = Column(DateTime)
    from_address = Column(String, index=True)
    to_address = Column(String, index=True)


class Haircut(Base):
    __tablename__ = "haircut"

    address = Column(String, primary_key=True, index=True)
    balance = Column(Float)
    taint = Column(Float)


class Poison(Base):
    __tablename__ = "poison"

    address = Column(String, primary_key=True, index=True)
    flagged = Column(Boolean)


class Fifo(Base):
    __tablename__ = "fifo"

    address = Column(String, primary_key=True, index=True)
    tainted = Column(Float)
    untainted = Column(Float)


class Seniority(Base):
    __tablename__ = "seniority"

    address = Column(String, primary_key=True, index=True)
    tainted_balance = Column(Float)