import time
from bson.objectid import ObjectId
from fastapi import HTTPException

from database import db


def list_identities():
    """Get list of all identities."""
    items = []
    for d in db.identities.find().sort("created_at", -1):
        d["_id"] = str(d["_id"])
        d.pop("password", None)
        items.append(d)
    return items


def get_identity(identity_id: str):
    try:
        oid = ObjectId(identity_id)
    except Exception:
        raise HTTPException(400, "invalid id")
    doc = db.identities.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "identity not found")
    doc["_id"] = str(doc["_id"])
    return doc


def add_identity(payload: dict):
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
    try:
        oid = ObjectId(identity_id)
    except Exception:
        raise HTTPException(400, "invalid id")

    identity = db.identities.find_one({"_id": oid})
    if not identity:
        raise HTTPException(404, "identity not found")

    db.identities.update_many({}, {"$set": {"is_default": False}})
    db.identities.update_one({"_id": oid}, {"$set": {"is_default": True}})

    return {"ok": True, "message": f"'{identity['name']}' is now the default identity"}


def unset_default_identity():
    db.identities.update_many({}, {"$set": {"is_default": False}})
    return {"ok": True, "message": "Default identity unset"}


def update_identity(identity_id: str, payload: dict):
    try:
        oid = ObjectId(identity_id)
    except Exception:
        raise HTTPException(400, "invalid id")

    allowed = {"name", "username", "password"}
    data = {k: v for k, v in payload.items() if k in allowed and v}
    if not data:
        raise HTTPException(400, "no valid field")

    db.identities.update_one({"_id": oid}, {"$set": data})
    return {"ok": True, "set": data}


def delete_identity(identity_id: str):
    try:
        oid = ObjectId(identity_id)
    except Exception:
        raise HTTPException(400, "invalid id")

    count = db.devices.count_documents({"identity_id": identity_id})
    if count > 0:
        raise HTTPException(400, f"Cannot delete: {count} device(s) are using this identity")

    db.identities.delete_one({"_id": oid})
    return {"deleted": True}
