import os, time, requests
from fastapi import FastAPI, Body, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pymongo import MongoClient
from bson.objectid import ObjectId

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")
DB_NAME   = os.getenv("DB_NAME","topologist")
SCHEDULER_URL = os.getenv("SCHEDULER_URL","http://scheduler:5001")

db = MongoClient(MONGO_URI)[DB_NAME]

app = FastAPI(title="Topologist Web")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

# ---------- Identities (Credentials) ----------
@app.get("/api/identities")
def list_identities():
    items = []
    for d in db.identities.find().sort("created_at", -1):
        d["_id"] = str(d["_id"])
        # Don't send password in list view for security
        d.pop("password", None)
        items.append(d)
    return items

@app.get("/api/identities/{identity_id}")
def get_identity(identity_id: str):
    try:
        oid = ObjectId(identity_id)
    except:
        raise HTTPException(400, "invalid id")
    doc = db.identities.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "identity not found")
    doc["_id"] = str(doc["_id"])
    return doc

@app.post("/api/identities")
def add_identity(payload: dict = Body(...)):
    name = (payload.get("name") or "").strip()
    username = (payload.get("username") or "").strip()
    password = (payload.get("password") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    if not username or not password:
        raise HTTPException(400, "username and password required")
    
    doc = {
        "name": name,
        "username": username,
        "password": password,
        "is_default": False,
        "created_at": time.time()
    }
    oid = db.identities.insert_one(doc).inserted_id
    return {"_id": str(oid), "message": "identity created"}

@app.post("/api/identities/{identity_id}/set_default")
def set_default_identity(identity_id: str):
    try:
        oid = ObjectId(identity_id)
    except:
        raise HTTPException(400, "invalid id")
    
    # Check if identity exists
    identity = db.identities.find_one({"_id": oid})
    if not identity:
        raise HTTPException(404, "identity not found")
    
    # Unset all other defaults
    db.identities.update_many({}, {"$set": {"is_default": False}})
    
    # Set this one as default
    db.identities.update_one({"_id": oid}, {"$set": {"is_default": True}})
    
    return {"ok": True, "message": f"'{identity['name']}' is now the default identity"}

@app.post("/api/identities/unset_default")
def unset_default_identity():
    # Unset all defaults
    db.identities.update_many({}, {"$set": {"is_default": False}})
    return {"ok": True, "message": "Default identity unset"}

@app.patch("/api/identities/{identity_id}")
def update_identity(identity_id: str, payload: dict = Body(...)):
    try:
        oid = ObjectId(identity_id)
    except:
        raise HTTPException(400, "invalid id")
    
    allowed = {"name", "username", "password"}
    data = {k: v for k, v in payload.items() if k in allowed and v}
    if not data:
        raise HTTPException(400, "no valid field")
    
    db.identities.update_one({"_id": oid}, {"$set": data})
    return {"ok": True, "set": data}

@app.delete("/api/identities/{identity_id}")
def delete_identity(identity_id: str):
    try:
        oid = ObjectId(identity_id)
    except:
        raise HTTPException(400, "invalid id")
    
    # Check if any device is using this identity
    count = db.devices.count_documents({"identity_id": identity_id})
    if count > 0:
        raise HTTPException(400, f"Cannot delete: {count} device(s) are using this identity")
    
    db.identities.delete_one({"_id": oid})
    return {"deleted": True}

# ---------- Inventory ----------
@app.get("/api/devices")
def list_devices():
    items=[]
    for d in db.devices.find().sort("created_at",-1):
        d["_id"]=str(d["_id"])
        # Include identity name if identity_id is set
        if d.get("identity_id"):
            identity = db.identities.find_one({"_id": ObjectId(d["identity_id"])})
            d["identity_name"] = identity["name"] if identity else "(deleted)"
        items.append(d)
    return items

@app.post("/api/devices")
def add_device(payload: dict = Body(...)):
    host=(payload.get("host") or "").strip()
    platform=(payload.get("platform") or "cisco_ios").strip()
    identity_id=(payload.get("identity_id") or "").strip() or None
    device_type=(payload.get("device_type") or "").strip() or None
    
    if not host: raise HTTPException(400,"host required")
    
    # Determine status based on identity_id or legacy username/password
    username = None
    password = None
    if identity_id:
        # Fetch credentials from identity
        try:
            identity = db.identities.find_one({"_id": ObjectId(identity_id)})
            if identity:
                username = identity["username"]
                password = identity["password"]
                status = "ready"
            else:
                raise HTTPException(400, "identity not found")
        except:
            raise HTTPException(400, "invalid identity_id")
    else:
        # Legacy: direct username/password (for backward compatibility)
        username=(payload.get("username") or "").strip() or None
        password=(payload.get("password") or "").strip() or None
        status="ready" if (username and password) else "unknown"
    
    doc={"host":host,"platform":platform,
         "identity_id":identity_id,"username":username,"password":password,
         "status":status,"depth":int(payload.get("depth",0)),"parent":payload.get("parent"),
         "device_type":device_type,
         "created_at":time.time(),"last_seen":None}
    old=db.devices.find_one({"host":host})
    if old:
        db.devices.update_one({"_id":old["_id"]},{"$set":doc})
        oid=old["_id"]
    else:
        oid=db.devices.insert_one(doc).inserted_id
    return {"_id":str(oid),"message":"saved"}

@app.delete("/api/devices/{device_id}")
def delete_device(device_id:str):
    try: oid=ObjectId(device_id)
    except: raise HTTPException(400,"invalid id")
    db.devices.delete_one({"_id":oid})
    return {"deleted":True}

@app.post("/api/devices/{device_id}/creds")
def save_creds(device_id:str, payload:dict=Body(...)):
    try: oid=ObjectId(device_id)
    except: raise HTTPException(400,"invalid id")
    u=(payload.get("username") or "").strip()
    p=(payload.get("password") or "").strip()
    if not u or not p: raise HTTPException(400,"username/password required")
    db.devices.update_one({"_id":oid},{"$set":{"username":u,"password":p,"status":"ready","last_seen":time.time()}})
    return {"ok":True}

@app.patch("/api/devices/{device_id}")
def update_device(device_id: str, payload: dict = Body(...)):
    """แก้ไขค่า inline เช่น identity_id, display_name, device_type"""
    try:
        oid = ObjectId(device_id)
    except:
        raise HTTPException(400, "invalid id")

    allowed = {"identity_id", "display_name", "device_type"}
    data = {k: v for k, v in payload.items() if k in allowed}
    if not data:
        raise HTTPException(400, "no valid field")

    # If identity_id changed, update username/password from the new identity
    if "identity_id" in data:
        identity_id = data["identity_id"]
        if identity_id:
            try:
                identity = db.identities.find_one({"_id": ObjectId(identity_id)})
                if identity:
                    data["username"] = identity["username"]
                    data["password"] = identity["password"]
                    data["status"] = "ready"
                else:
                    raise HTTPException(400, "identity not found")
            except:
                raise HTTPException(400, "invalid identity_id")
        else:
            # Clear identity - set to unknown status
            data["username"] = None
            data["password"] = None
            data["status"] = "unknown"
    
    db.devices.update_one({"_id": oid}, {"$set": data})
    return {"ok": True, "set": data}


# ---------- Topology ----------
@app.get("/api/topology/latest")
def latest_topology():
    doc = db.topology.find_one(sort=[("created_at",-1)], projection={"_id":0})
    return doc or {"nodes":[],"links":[],"meta":{"generated_at":None}}

# ---------- Orchestration (call scheduler) ----------
@app.post("/api/discover")
def api_discover(payload:dict=Body(...)):
    # payload: {"device_id": "..."} or {"host": "..."} + optional {"auto_recursive": false, "max_depth":2}
    r=requests.post(f"{SCHEDULER_URL}/discover", json=payload, timeout=30)
    return JSONResponse(r.json(), status_code=r.status_code)

# ----- Global Graph -----
@app.get("/api/topology/graph")
def topology_graph():
    # nodes keyed by IP (graph_nodes._id). Attach display label (display_name > host > id)
    nodes = []
    for n in db.graph_nodes.find({}, {"_id": 1}):
        ip = n["_id"]
        dev = db.devices.find_one({"host": ip}, {"display_name": 1, "host": 1, "device_type": 1})
        if dev:
            label = dev.get("display_name") or dev.get("host") or ip
        else:
            label = ip
        nodes.append({"id": ip, "label": label, "device_type": (dev.get("device_type") if dev else None)})
    edges = []
    for e in db.graph_links.find({}, {"_id": 0, "a": 1, "b": 1, "ifA": 1, "ifB": 1}):
        edges.append({
            "source": e["a"], "target": e["b"],
            "ifSrc": e.get("ifA", ""), "ifDst": e.get("ifB", "")
        })
    return {"nodes": nodes, "links": edges}

# ----- Discover All (enqueue ทุก device ที่ ready) -----
@app.post("/api/discover_all")
def discover_all():
    r = requests.post(f"{SCHEDULER_URL}/discover_all", timeout=60)
    return JSONResponse(r.json(), status_code=r.status_code)

# ----- Maintenance: Clear topology (graph + historical topology docs) -----
@app.post("/api/topology/clear")
def clear_topology():
    gn = db.graph_nodes.delete_many({}).deleted_count
    gl = db.graph_links.delete_many({}).deleted_count
    tp = db.topology.delete_many({}).deleted_count
    return {"cleared": True, "graph_nodes": gn, "graph_links": gl, "topology_docs": tp}
