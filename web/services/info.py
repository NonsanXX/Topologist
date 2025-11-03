import time
from bson.objectid import ObjectId
from fastapi import HTTPException

from database import db
from .devices import resolve_device_credentials
from .queueing import queue_command_payload

INFO_COMMANDS = ["show ip interface brief", "show ip route"]


def run_device_info(device_id: str, force: bool = False):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid device id")

    resolve_device_credentials(oid)

    existing = db.device_info_jobs.find_one(
        {"device_id": str(oid)}, sort=[("updated_at", -1)]
    )
    if (
        not force
        and existing
        and existing.get("status") == "succeeded"
        and existing.get("results")
    ):
        return {
            "job_id": str(existing["_id"]),
            "cached": True,
            "updated_at": existing.get("updated_at"),
        }

    now = time.time()
    job_doc = {
        "device_id": str(oid),
        "commands": INFO_COMMANDS.copy(),
        "status": "queued",
        "results": [],
        "error": None,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
    }
    job_id = db.device_info_jobs.insert_one(job_doc).inserted_id

    payload = {
        "type": "device_info",
        "job_id": str(job_id),
        "device_id": str(oid),
    }
    queue_command_payload(payload, db.device_info_jobs, job_id)

    return {"job_id": str(job_id), "cached": False}


def get_device_info_job(job_id: str):
    try:
        oid = ObjectId(job_id)
    except Exception:
        raise HTTPException(400, "invalid job id")

    doc = db.device_info_jobs.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "device info job not found")

    normalized_results = []
    for item in doc.get("results", []) or []:
        normalized_results.append(
            {"command": item.get("command"), "output": item.get("output")}
        )

    return {
        "job_id": str(doc["_id"]),
        "status": doc.get("status"),
        "results": normalized_results,
        "error": doc.get("error"),
        "updated_at": doc.get("updated_at"),
    }


def get_latest_device_info(device_id: str):
    try:
        oid = ObjectId(device_id)
    except Exception:
        raise HTTPException(400, "invalid device id")

    latest = db.device_info_jobs.find_one(
        {"device_id": str(oid)}, sort=[("updated_at", -1)]
    )
    if not latest:
        raise HTTPException(404, "no interface summary available yet")

    doc = latest
    fallback_used = False

    if latest.get("status") != "succeeded":
        fallback = db.device_info_jobs.find_one(
            {
                "device_id": str(oid),
                "status": "succeeded",
                "results": {"$exists": True, "$ne": []},
            },
            sort=[("updated_at", -1)],
        )
        if fallback:
            doc = fallback
            fallback_used = True

    normalized_results = []
    for item in doc.get("results", []) or []:
        normalized_results.append(
            {"command": item.get("command"), "output": item.get("output")}
        )

    return {
        "job_id": str(doc["_id"]),
        "status": doc.get("status"),
        "results": normalized_results,
        "error": doc.get("error"),
        "updated_at": doc.get("updated_at"),
        "cached": doc.get("status") == "succeeded",
        "fallback": fallback_used,
        "latest_status": latest.get("status"),
        "latest_updated_at": latest.get("updated_at"),
    }
