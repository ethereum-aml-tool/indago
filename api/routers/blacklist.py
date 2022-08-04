from typing import Optional
from requests import get

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from db.sql import crud, schemas
from db.sql.database import SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(
    prefix="/blacklist",
    tags=["blacklist"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)


@router.get("/poison/{address}", response_model=schemas.Poison, tags=["blacklisting"])
def read_poison(address: str, db: Session = Depends(get_db)):
    poison = crud.get_poison(db, address=address)
    if poison is None:
        raise HTTPException(status_code=404, detail=f"Poison for [{address}] not found")
    return poison


@router.get("/haircut/{address}", response_model=schemas.Haircut, tags=["blacklisting"])
def read_haircut(address: str, db: Session = Depends(get_db)):
    haircut = crud.get_haircut(db, address=address)
    if haircut is None:
        raise HTTPException(status_code=404, detail=f"Haircut for [{address}] not found")
    haircut.taint = haircut.taint / 10 ** 18 # TODO Should be converted earlier...
    haircut.balance = haircut.balance / 10 ** 18 # TODO Should be converted earlier...
    return haircut


# @app.get("/taint/fifo/{address}", response_model=schemas.Fifo, tags=["blacklisting"])
# def read_fifo(address: str, db: Session = Depends(get_db)):
#     fifo = crud.get_fifo(db, address=address)
#     if fifo is None:
#         raise HTTPException(status_code=404, detail=f"Fifo for [{address}] not found")
#     return fifo


@router.get("/seniority/{address}", response_model=schemas.Seniority, tags=["blacklisting"])
def read_seniority(address: str, db: Session = Depends(get_db)):
    seniority = crud.get_seniority(db, address=address)
    if seniority is None:
        raise HTTPException(status_code=404, detail=f"Seniority for [{address}] not found")
    seniority.tainted_balance = seniority.tainted_balance / 10 ** 18 # TODO Should be converted earlier...
    return seniority
