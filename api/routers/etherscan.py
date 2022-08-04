from requests import get

from fastapi import APIRouter, HTTPException
from db.sql.config import Settings

ETHERSCAN_API_URL = "https://api.etherscan.io/api"
s = Settings()


router = APIRouter(
    prefix="/etherscan",
    tags=["etherscan"],
    dependencies=[],
    responses={404: {"description": "Not found"}},
)


@router.get("/balance/{address}")
def get_balance(address: str):
    url = f"{ETHERSCAN_API_URL}?module=account&action=balance&address={address}&tag=latest&apikey={s.ETHERSCAN_API_KEY}"
    response = get(url).json()
    if response.get("status") != "1":
        raise HTTPException(status_code=404, detail=f"Account for [{address}] not found on Etherscan")
    response["result"] = int(response["result"]) / 10 ** 18 # Wei to ETH
    
    return response
