import requests
from fastapi import Body, HTTPException
from fastapi.responses import JSONResponse

from database import SCHEDULER_URL
from models import (
    list_identities, get_identity, add_identity, set_default_identity,
    unset_default_identity, update_identity, delete_identity,
    list_devices, add_device, delete_device, save_device_creds, update_device,
    latest_topology, topology_graph, clear_topology
)


def get_identities_handler():
    """Handler for GET /api/identities"""
    return list_identities()


def get_identity_handler(identity_id: str):
    """Handler for GET /api/identities/{identity_id}"""
    return get_identity(identity_id)


def add_identity_handler(payload: dict = Body(...)):
    """Handler for POST /api/identities"""
    return add_identity(payload)


def set_default_identity_handler(identity_id: str):
    """Handler for POST /api/identities/{identity_id}/set_default"""
    return set_default_identity(identity_id)


def unset_default_identity_handler():
    """Handler for POST /api/identities/unset_default"""
    return unset_default_identity()


def update_identity_handler(identity_id: str, payload: dict = Body(...)):
    """Handler for PATCH /api/identities/{identity_id}"""
    return update_identity(identity_id, payload)


def delete_identity_handler(identity_id: str):
    """Handler for DELETE /api/identities/{identity_id}"""
    return delete_identity(identity_id)


def get_devices_handler():
    """Handler for GET /api/devices"""
    return list_devices()


def add_device_handler(payload: dict = Body(...)):
    """Handler for POST /api/devices"""
    return add_device(payload)


def delete_device_handler(device_id: str):
    """Handler for DELETE /api/devices/{device_id}"""
    return delete_device(device_id)


def save_device_creds_handler(device_id: str, payload: dict = Body(...)):
    """Handler for POST /api/devices/{device_id}/creds"""
    return save_device_creds(device_id, payload)


def update_device_handler(device_id: str, payload: dict = Body(...)):
    """Handler for PATCH /api/devices/{device_id}"""
    return update_device(device_id, payload)


def latest_topology_handler():
    """Handler for GET /api/topology/latest"""
    return latest_topology()


def topology_graph_handler():
    """Handler for GET /api/topology/graph"""
    return topology_graph()


def clear_topology_handler():
    """Handler for POST /api/topology/clear"""
    return clear_topology()


def api_discover_handler(payload: dict = Body(...)):
    """Handler for POST /api/discover"""
    r = requests.post(f"{SCHEDULER_URL}/discover", json=payload, timeout=30)
    return JSONResponse(r.json(), status_code=r.status_code)


def discover_all_handler():
    """Handler for POST /api/discover_all"""
    r = requests.post(f"{SCHEDULER_URL}/discover_all", timeout=60)
    return JSONResponse(r.json(), status_code=r.status_code)
