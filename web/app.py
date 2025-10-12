from fastapi import FastAPI, Body
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from routes import (
    get_identities_handler, get_identity_handler, add_identity_handler,
    set_default_identity_handler, unset_default_identity_handler,
    update_identity_handler, delete_identity_handler,
    get_devices_handler, add_device_handler, delete_device_handler,
    save_device_creds_handler, update_device_handler,
    latest_topology_handler, topology_graph_handler, clear_topology_handler,
    api_discover_handler, discover_all_handler
)

app = FastAPI(title="Topologist Web")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


# ---------- Identities (Credentials) ----------
@app.get("/api/identities")
def list_identities():
    return get_identities_handler()


@app.get("/api/identities/{identity_id}")
def get_identity(identity_id: str):
    return get_identity_handler(identity_id)


@app.post("/api/identities")
def add_identity(payload: dict = Body(...)):
    return add_identity_handler(payload)


@app.post("/api/identities/{identity_id}/set_default")
def set_default_identity(identity_id: str):
    return set_default_identity_handler(identity_id)


@app.post("/api/identities/unset_default")
def unset_default_identity():
    return unset_default_identity_handler()


@app.patch("/api/identities/{identity_id}")
def update_identity(identity_id: str, payload: dict = Body(...)):
    return update_identity_handler(identity_id, payload)


@app.delete("/api/identities/{identity_id}")
def delete_identity(identity_id: str):
    return delete_identity_handler(identity_id)


# ---------- Inventory ----------
@app.get("/api/devices")
def list_devices():
    return get_devices_handler()


@app.post("/api/devices")
def add_device(payload: dict = Body(...)):
    return add_device_handler(payload)


@app.delete("/api/devices/{device_id}")
def delete_device(device_id: str):
    return delete_device_handler(device_id)


@app.post("/api/devices/{device_id}/creds")
def save_creds(device_id: str, payload: dict = Body(...)):
    return save_device_creds_handler(device_id, payload)


@app.patch("/api/devices/{device_id}")
def update_device(device_id: str, payload: dict = Body(...)):
    return update_device_handler(device_id, payload)


# ---------- Topology ----------
@app.get("/api/topology/latest")
def latest_topology():
    return latest_topology_handler()


@app.get("/api/topology/graph")
def topology_graph():
    return topology_graph_handler()


@app.post("/api/topology/clear")
def clear_topology():
    return clear_topology_handler()


# ---------- Orchestration (call scheduler) ----------
@app.post("/api/discover")
def api_discover(payload: dict = Body(...)):
    return api_discover_handler(payload)


@app.post("/api/discover_all")
def discover_all():
    return discover_all_handler()
