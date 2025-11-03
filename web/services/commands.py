import re
import time
from bson.objectid import ObjectId
from fastapi import HTTPException

from database import db
from .devices import resolve_device_credentials
from .queueing import queue_command_payload


def run_device_command(device_id: str, payload: dict):
    command = (payload.get("command") or "").strip()
    if not command:
        raise HTTPException(400, "command required")

    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid device id")

    resolve_device_credentials(oid)

    refresh_inventory = bool(re.search(r"\bhostname\b", command, re.IGNORECASE))
    now = time.time()
    job_doc = {
        "device_id": str(oid),
        "command": command,
        "status": "queued",
        "output": None,
        "error": None,
        "refresh_inventory": refresh_inventory,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
    }
    job_id = db.device_commands.insert_one(job_doc).inserted_id

    payload = {
        "type": "cli_command",
        "job_id": str(job_id),
        "device_id": str(oid),
        "command": command,
    }
    queue_command_payload(payload, db.device_commands, job_id)

    return {"job_id": str(job_id), "refresh_inventory": refresh_inventory}


def get_command_job(job_id: str):
    try:
        oid = ObjectId(job_id)
    except Exception:
        raise HTTPException(400, "invalid job id")

    doc = db.device_commands.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "command job not found")

    return {
        "job_id": str(doc["_id"]),
        "status": doc.get("status"),
        "output": doc.get("output"),
        "error": doc.get("error"),
        "refresh_inventory": bool(doc.get("refresh_inventory", False)),
        "updated_at": doc.get("updated_at"),
    }
