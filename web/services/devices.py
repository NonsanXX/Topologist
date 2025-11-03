import time
from bson.objectid import ObjectId
from fastapi import HTTPException

from database import db


def resolve_device_credentials(oid: ObjectId):
    dev = db.devices.find_one({"_id": oid})
    if not dev:
        raise HTTPException(404, "device not found")

    host = dev.get("host")
    username = dev.get("username")
    password = dev.get("password")

    if not username or not password:
        identity_id = dev.get("identity_id")
        if identity_id:
            try:
                identity = db.identities.find_one({"_id": ObjectId(identity_id)})
                if identity:
                    username = identity.get("username")
                    password = identity.get("password")
                else:
                    raise HTTPException(
                        400, "Device identity not found, but was linked"
                    )
            except Exception:
                raise HTTPException(400, "Invalid identity_id format")

    if not host or not username or not password:
        label = host or str(oid)
        raise HTTPException(400, f"Device {label} is missing host or credentials")

    return dev, host, username, password


def list_devices():
    items = []
    for d in db.devices.find().sort("created_at", -1):
        d["_id"] = str(d["_id"])
        if d.get("identity_id"):
            identity = db.identities.find_one({"_id": ObjectId(d["identity_id"])})
            d["identity_name"] = identity["name"] if identity else "(deleted)"
        items.append(d)
    return items


def add_device(payload: dict):
    host = (payload.get("host") or "").strip()
    platform = (payload.get("platform") or "cisco_ios").strip()
    identity_id = (payload.get("identity_id") or "").strip() or None
    device_type = (payload.get("device_type") or "").strip() or None

    if not host:
        raise HTTPException(400, "host required")

    username = None
    password = None
    if identity_id:
        try:
            identity = db.identities.find_one({"_id": ObjectId(identity_id)})
            if identity:
                username = identity["username"]
                password = identity["password"]
                status = "ready"
            else:
                raise HTTPException(400, "identity not found")
        except Exception:
            raise HTTPException(400, "invalid identity_id")
    else:
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
        "last_seen": None,
    }
    old = db.devices.find_one({"host": host})
    if old:
        db.devices.update_one({"_id": old["_id"]}, {"$set": doc})
        oid = old["_id"]
    else:
        oid = db.devices.insert_one(doc).inserted_id
    return {"_id": str(oid), "message": "saved"}


def delete_device(device_id: str):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid id")
    db.devices.delete_one({"_id": oid})
    return {"deleted": True}


def save_device_creds(device_id: str, payload: dict):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid id")
    u = (payload.get("username") or "").strip()
    p = (payload.get("password") or "").strip()
    if not u or not p:
        raise HTTPException(400, "username/password required")
    db.devices.update_one(
        {"_id": oid},
        {
            "$set": {
                "username": u,
                "password": p,
                "status": "ready",
                "last_seen": time.time(),
            }
        },
    )
    return {"ok": True}


def update_device(device_id: str, payload: dict):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid id")

    allowed = {"identity_id", "display_name", "device_type"}
    data = {k: v for k, v in payload.items() if k in allowed}
    if not data:
        raise HTTPException(400, "no valid field")

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
            except Exception:
                raise HTTPException(400, "invalid identity_id")
        else:
            data["username"] = None
            data["password"] = None
            data["status"] = "unknown"

    db.devices.update_one({"_id": oid}, {"$set": data})
    return {"ok": True, "set": data}


def _collect_connected_interfaces(dev: dict):
    connected = set()
    host = dev.get("host")
    if not host:
        return connected
    for edge in db.graph_links.find({"$or": [{"a": host}, {"b": host}]}):
        if edge.get("a") == host and edge.get("ifA"):
            connected.add(edge.get("ifA"))
        if edge.get("b") == host and edge.get("ifB"):
            connected.add(edge.get("ifB"))
    return connected


def get_device_interfaces(device_id: str):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid device id")

    dev = db.devices.find_one({"_id": oid})
    if not dev:
        raise HTTPException(404, "device not found")

    interface_map = dev.get("interface_map") or {}
    disabled = set(dev.get("disabled_interfaces") or [])
    connected = _collect_connected_interfaces(dev)

    iface_to_ips = {}
    for ip, iface in interface_map.items():
        if not iface:
            continue
        iface_to_ips.setdefault(iface, []).append(ip)

    interfaces = []
    for iface, ips in sorted(iface_to_ips.items()):
        interfaces.append(
            {
                "name": iface,
                "ips": ips,
                "disabled": iface in disabled,
                "connected": iface in connected,
                "can_disable": iface not in connected,
            }
        )

    return {
        "device_id": str(dev["_id"]),
        "host": dev.get("host") or "",
        "display_name": dev.get("display_name") or "",
        "interfaces": interfaces,
        "disabled_interfaces": list(disabled),
    }


def set_device_interface_state(device_id: str, payload: dict):
    interface_name = (payload.get("interface") or "").strip()
    if not interface_name:
        raise HTTPException(400, "interface required")
    disable = bool(payload.get("disabled", False))

    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid device id")

    dev = db.devices.find_one({"_id": oid})
    if not dev:
        raise HTTPException(404, "device not found")

    interface_map = dev.get("interface_map") or {}
    available_ifaces = {iface for iface in interface_map.values() if iface}
    if interface_name not in available_ifaces:
        raise HTTPException(404, "interface not found on device")

    connected = _collect_connected_interfaces(dev)
    if disable and interface_name in connected:
        raise HTTPException(
            400, "cannot disable an interface that is currently connected"
        )

    if disable:
        db.devices.update_one(
            {"_id": oid}, {"$addToSet": {"disabled_interfaces": interface_name}}
        )
    else:
        db.devices.update_one(
            {"_id": oid}, {"$pull": {"disabled_interfaces": interface_name}}
        )

    return get_device_interfaces(device_id)
