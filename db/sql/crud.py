from sqlalchemy.orm import Session
from . import models, schemas


def get_haircut(db: Session, address: str) -> schemas.Haircut:
    return db.query(models.Haircut).filter(models.Haircut.address == address.lower()).first()


def get_fifo(db: Session, address: str) -> schemas.Fifo:
    return db.query(models.Fifo).filter(models.Fifo.address == address.lower()).first()


def get_poison(db: Session, address: str) -> schemas.Poison:
    return db.query(models.Poison).filter(models.Poison.address == address.lower()).first()


def get_seniority(db: Session, address: str) -> schemas.Seniority:
    return db.query(models.Seniority).filter(models.Seniority.address == address.lower()).first()
