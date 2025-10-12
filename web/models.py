import time
from fastapi import HTTPException, Body
from bson.objectid import ObjectId

from database import db


def list_identities():
    """Get list of all identities."""
    items = []
    for d in db.identities.find().sort("created_at", -1):
        d["_id"] = str(d["_id"])
        # Don't send password in list view for security
        d.pop("password", None)
        items.append(d)
    return items


def get_identity(identity_id: str):
    """Get a specific identity by ID."""
    try:
        oid = ObjectId(identity_id)
    except:
        raise HTTPException(400, "invalid id")
    doc = db.identities.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "identity not found")
    doc["_id"] = str(doc["_id"])
    return doc


def add_identity(payload: dict):
    """Add a new identity."""
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


def set_default_identity(identity_id: str):
    """Set an identity as default."""
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


def unset_default_identity():
    """Unset default identity."""
    # Unset all defaults
    db.identities.update_many({}, {"$set": {"is_default": False}})
    return {"ok": True, "message": "Default identity unset"}


def update_identity(identity_id: str, payload: dict):
    """Update an identity."""
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


def delete_identity(identity_id: str):
    """Delete an identity."""
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


def list_devices():
    """Get list of all devices."""
    items = []
    for d in db.devices.find().sort("created_at", -1):
        d["_id"] = str(d["_id"])
        # Include identity name if identity_id is set
        if d.get("identity_id"):
            identity = db.identities.find_one({"_id": ObjectId(d["identity_id"])})
            d["identity_name"] = identity["name"] if identity else "(deleted)"
        items.append(d)
    return items


def add_device(payload: dict):
    """Add a new device."""
    host = (payload.get("host") or "").strip()
    platform = (payload.get("platform") or "cisco_ios").strip()
    identity_id = (payload.get("identity_id") or "").strip() or None
    device_type = (payload.get("device_type") or "").strip() or None
    
    if not host:
        raise HTTPException(400, "host required")
    
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
        username = (payload.get("username") or "").strip() or None
        password = (payload.get("password") or "").strip() or None
        status = "ready" if (username and password) else "unknown"
    
    doc = {
        "host": host,
        "platform": platform,
        "identity_id": identity_id,
        "username": username,
        "password": password,
        "status": status,
        "depth": int(payload.get("depth", 0)),
        "parent": payload.get("parent"),
        "device_type": device_type,
        "created_at": time.time(),
        "last_seen": None
    }
    old = db.devices.find_one({"host": host})
    if old:
        db.devices.update_one({"_id": old["_id"]}, {"$set": doc})
        oid = old["_id"]
    else:
        oid = db.devices.insert_one(doc).inserted_id
    return {"_id": str(oid), "message": "saved"}


def delete_device(device_id: str):
    """Delete a device."""
    try:
        oid = ObjectId(device_id)
    except:
        raise HTTPException(400, "invalid id")
    db.devices.delete_one({"_id": oid})
    return {"deleted": True}


def save_device_creds(device_id: str, payload: dict):
    """Save credentials for a device."""
    try:
        oid = ObjectId(device_id)
    except:
        raise HTTPException(400, "invalid id")
    u = (payload.get("username") or "").strip()
    p = (payload.get("password") or "").strip()
    if not u or not p:
        raise HTTPException(400, "username/password required")
    db.devices.update_one(
        {"_id": oid},
        {"$set": {"username": u, "password": p, "status": "ready", "last_seen": time.time()}}
    )
    return {"ok": True}


def update_device(device_id: str, payload: dict):
    """Update device fields."""
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


def latest_topology():
    """Get the latest topology snapshot."""
    doc = db.topology.find_one(sort=[("created_at", -1)], projection={"_id": 0})
    return doc or {"nodes": [], "links": [], "meta": {"generated_at": None}}


def topology_graph():
    """Get global topology graph with nodes and links."""
    nodes = []
    for n in db.graph_nodes.find({}, {"_id": 1}):
        node_id = n["_id"]
        
        # Determine if this is an IP-based or name-based node
        if node_id.startswith("name:"):
            # Name-based node: extract the device name
            device_name = node_id[5:]  # Remove "name:" prefix
            # Look up by display_name or host field
            dev = db.devices.find_one(
                {"$or": [{"display_name": device_name}, {"host": ""}]},
                {"display_name": 1, "host": 1, "device_type": 1}
            )
            if dev:
                label = dev.get("display_name") or device_name
                device_type = dev.get("device_type")
            else:
                label = device_name
                device_type = None
        else:
            # IP-based node: look up by host (IP)
            dev = db.devices.find_one({"host": node_id}, {"display_name": 1, "host": 1, "device_type": 1})
            if dev:
                label = dev.get("display_name") or dev.get("host") or node_id
                device_type = dev.get("device_type")
            else:
                label = node_id
                device_type = None
        
        nodes.append({"id": node_id, "label": label, "device_type": device_type})
    
    edges = []
    for e in db.graph_links.find({}, {"_id": 0, "a": 1, "b": 1, "ifA": 1, "ifB": 1}):
        edges.append({
            "source": e["a"], "target": e["b"],
            "ifSrc": e.get("ifA", ""), "ifDst": e.get("ifB", "")
        })
    return {"nodes": nodes, "links": edges}


def clear_topology():
    """Clear all topology data."""
    gn = db.graph_nodes.delete_many({}).deleted_count
    gl = db.graph_links.delete_many({}).deleted_count
    tp = db.topology.delete_many({}).deleted_count
    return {"cleared": True, "graph_nodes": gn, "graph_links": gl, "topology_docs": tp}
