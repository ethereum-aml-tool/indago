from sqlalchemy.orm import Session
from . import models, schemas


def get_account(db: Session, address: str):
    # TODO Force lower in some way?
    return db.query(models.Account).filter(models.Account.address == address.lower()).first()


def get_accounts(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Account).offset(skip).limit(limit).all()


def create_account(db: Session, account: schemas.AccountCreate):
    db_account = models.Account(**account.dict())
    db.add(db_account)
    db.commit()
    db.refresh(db_account)
    return db_account


def get_transaction(db: Session, hash: str):
    return db.query(models.Transaction).filter(models.Transaction.hash == hash).first()


def get_transactions(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Transaction).offset(skip).limit(limit).all()


def get_transactions_from_account(db: Session, address: str, skip: int = 0, limit: int = 100):
    return db.query(models.Transaction).filter(models.Transaction.from_address == address.lower()).offset(skip).limit(limit).all()


def get_transactions_to_account(db: Session, address: str, skip: int = 0, limit: int = 100):
    return db.query(models.Transaction).filter(models.Transaction.to_address == address.lower()).offset(skip).limit(limit).all()


def create_transaction(db: Session, transaction: schemas.TransactionCreate):
    db_transaction = models.Transaction(**transaction.dict())
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    return db_transaction


def get_haircut(db: Session, address: str) -> schemas.Haircut:
    return db.query(models.Haircut).filter(models.Haircut.address == address.lower()).first()

def get_fifo(db: Session, address: str) -> schemas.Fifo:
    return db.query(models.Fifo).filter(models.Fifo.address == address.lower()).first()

def get_poison(db: Session, address: str) -> schemas.Poison:
    return db.query(models.Poison).filter(models.Poison.address == address.lower()).first()

def get_seniority(db: Session, address: str) -> schemas.Seniority:
    return db.query(models.Seniority).filter(models.Seniority.address == address.lower()).first()