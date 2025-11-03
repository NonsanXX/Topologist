import requests
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from database import SCHEDULER_URL

router = APIRouter(prefix="/api", tags=["orchestrator"])


@router.post("/discover")
def api_discover_route(payload: dict = Body(...)):
    resp = requests.post(f"{SCHEDULER_URL}/discover", json=payload, timeout=30)
    return JSONResponse(resp.json(), status_code=resp.status_code)


@router.post("/discover_all")
def discover_all_route():
    resp = requests.post(f"{SCHEDULER_URL}/discover_all", timeout=60)
    return JSONResponse(resp.json(), status_code=resp.status_code)
