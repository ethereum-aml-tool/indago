from pprint import pprint
from typing import Optional
from requests import get

from fastapi import APIRouter, HTTPException
from bson.objectid import ObjectId
from db.mongo.database import IndagoSession
from db.mongo.schemas import DARAddressMapping, DARGraph

from db.mongo.config import Settings
from db.sql import crud
from db.sql.database import SessionLocal


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


def is_flagged(address: str) -> bool:
    """
    Check if an address is flagged in the blacklist.

    TODO: Should not only check seniority, but some kind of combined blacklisting mechanism.
    """
    db = SessionLocal()
    tainted = crud.get_seniority(db, address=address)
    db.close()

    return True if tainted is not None else False


@router.get("/get-dar/", response_description="Graph data retrieved", response_model=DARGraph)
async def get_graph(address: Optional[str] = None, id: Optional[int] = None):
    '''
    Get the graph data from mongo database, accepts either raw graph ID
    or an ethereum address. If given an address, a lookup is first performed
    in the dar_map collection to find the graph ID.

    returns: DARGraph
    '''
    if address is not None:
        address = address.lower()
        cluster_id: int = None
        if (id := await dar_map_collection.find_one({"_id": address})) is not None:
            cluster_id = id["cluster_id"]
        else:
            raise HTTPException(
                status_code=404, detail=f"Graph for {address} not found")
        if (graph := await dar_collection.find_one({"_id": cluster_id})) is not None:
            # TODO: Should be done somewhere else, this is not a good place for it

            # Flags the non-exchange addresses if they are in the blacklist
            non_exchange_nodes = []
            exchange_nodes = []
            for edge in graph['edges']:
                non_exchange_nodes.append(graph['nodes'][edge[0]])
                exchange_nodes.append(graph['nodes'][edge[1]])
            non_exchange_nodes = set(non_exchange_nodes)
            exchange_nodes = set(exchange_nodes)
            
            checked_nodes = []
            for node in non_exchange_nodes:
                if is_flagged(node):
                    checked_nodes.append({"address": node, "flagged": True})
                else:
                    checked_nodes.append({"address": node, "flagged": False})
            for node in exchange_nodes:
                checked_nodes.append({"address": node, "flagged": False})

            graph['nodes'] = checked_nodes
            # pprint(graph['nodes'])

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
