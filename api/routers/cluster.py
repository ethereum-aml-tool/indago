from pprint import pprint
from typing import Optional
from requests import get

from fastapi import APIRouter, HTTPException
from bson.objectid import ObjectId
from db.mongo.database import IndagoSession
from db.mongo.schemas import DARAddressMapping, DARGraph

from db.mongo.config import Settings


# MONGO
db = IndagoSession
dar_collection = db['dar']
dar_map_collection = db['dar_map']


router = APIRouter(
    prefix="/cluster",
    tags=["cluster"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)


@router.get("/get-dar/", response_description="Graph data retrieved", response_model=DARGraph)
async def get_graph(address: Optional[str] = None, id: Optional[int] = None):
    if address is not None:
        address = address.lower()
        cluster_id: int = None
        if (id := await dar_map_collection.find_one({"_id": address})) is not None:
            cluster_id = id["cluster_id"]
        else:
            raise HTTPException(
                status_code=404, detail=f"Graph for {address} not found")
        if (graph := await dar_collection.find_one({"_id": cluster_id})) is not None:
            # Converts the edge-objects to a dict instead of a simple array
            parsed_edges = []
            for edge in graph["edges"]:
                parsed_edges.append({"from": edge[0], "to": edge[1]})
            graph["edges"] = parsed_edges
            return graph
        raise HTTPException(status_code=404, detail=f"Graph {id} not found")
    elif id is not None:
        if (graph := await dar_collection.find_one({"_id": id})) is not None:
            return graph
        raise HTTPException(status_code=404, detail=f"Graph {id} not found")


@router.get("/dar-mapping/{address}", response_description="Address graph ID retrieved", response_model=DARAddressMapping)
async def get_graph(address: str):
    address = address.lower()
    if (id := await dar_map_collection.find_one({"_id": address})) is not None:
        return id
    raise HTTPException(status_code=404, detail=f"Address {address} not found")
