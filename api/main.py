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


@app.get("/health")
def health():
    return {"status": "ok"}
