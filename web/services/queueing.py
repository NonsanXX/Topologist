import time
import requests
from bson.objectid import ObjectId
from fastapi import HTTPException

from database import SCHEDULER_URL


def queue_command_payload(payload: dict, collection, job_oid: ObjectId):
    try:
        resp = requests.post(f"{SCHEDULER_URL}/queue/commands", json=payload, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        collection.update_one(
            {"_id": job_oid},
            {"$set": {"status": "failed", "error": str(exc), "updated_at": time.time(), "completed_at": time.time()}}
        )
        raise HTTPException(502, "Failed to queue job with scheduler") from exc
