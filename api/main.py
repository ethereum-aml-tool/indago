import imp
import os
import re
import time
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import true
from sqlalchemy.orm import Session
from api.routers import etherscan, cluster, blacklist

from db.sql import models, crud, schemas
from db.sql.database import SessionLocal, engine

# PSQL
models.Base.metadata.create_all(bind=engine)

# TODO Extract dependency?
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

# Router used for getting data from Etherscan
app.include_router(etherscan.router)
app.include_router(cluster.router)
app.include_router(blacklist.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Indago": True, "Authors": ["Max", "Pontus"], "Website": "https://indago.ponbac.xyz"}


@app.get("/accounts/", response_model=list[schemas.Account])
def read_accounts(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    accounts = crud.get_accounts(db, skip=skip, limit=limit)
    return accounts


@app.get("/accounts/{address}", response_model=schemas.Account)
def read_account(address: str, db: Session = Depends(get_db)):
    db_account = crud.get_account(db, address=address)
    if db_account is None:
        db_account = crud.create_account(
            db, account=schemas.AccountCreate(address=address.lower()))
        # raise HTTPException(status_code=404, detail=f'Account for [{address}] not found')

    time.sleep(3)  # TODO REMOVE, FOR DEMO ONLY!
    return db_account


@app.get("/transactions/to/{address}", response_model=list[schemas.Transaction])
def read_transactions_to_account(address: str, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    transactions = crud.get_transactions_to_account(db, address=address, skip=skip, limit=limit)
    return transactions


@app.get("/transactions/from/{address}", response_model=list[schemas.Transaction])
def read_transactions_from_account(address: str, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    transactions = crud.get_transactions_from_account(db, address=address, skip=skip, limit=limit)
    return transactions
