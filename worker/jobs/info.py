import time
from bson.objectid import ObjectId

from .utils import establish_connection, run_command_list

DEFAULT_INFO_COMMANDS = ["show ip interface brief", "show ip route"]


def do_info_job(job, db):
    job_id = job.get("job_id")
    if not job_id:
        print("[INFO] Received job without job_id")
        return

    try:
        job_oid = ObjectId(job_id)
    except Exception:
        print(f"[INFO] Invalid job_id {job_id}")
        return

    doc = db.device_info_jobs.find_one({"_id": job_oid})
    if not doc:
        print(f"[INFO] Job document {job_id} not found")
        return

    device_id = doc.get("device_id") or job.get("device_id")
    if not device_id:
        db.device_info_jobs.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "failed",
                    "error": "device id missing",
                    "updated_at": time.time(),
                    "completed_at": time.time(),
                }
            },
        )
        print(f"[INFO] Job {job_id} missing device id")
        return

    commands = DEFAULT_INFO_COMMANDS

    try:
        device_oid = ObjectId(device_id)
    except Exception:
        db.device_info_jobs.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "failed",
                    "error": "invalid device id",
                    "updated_at": time.time(),
                    "completed_at": time.time(),
                }
            },
        )
        print(f"[INFO] Job {job_id} invalid device id {device_id}")
        return

    dev = db.devices.find_one({"_id": device_oid})
    if not dev:
        db.device_info_jobs.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "failed",
                    "error": "device not found",
                    "updated_at": time.time(),
                    "completed_at": time.time(),
                }
            },
        )
        print(f"[INFO] Device {device_id} not found for job {job_id}")
        return

    start_time = time.time()
    db.device_info_jobs.update_one(
        {"_id": job_oid},
        {
            "$set": {
                "status": "running",
                "started_at": start_time,
                "updated_at": start_time,
                "error": None,
            }
        },
    )

    conn = None
    results = []
    try:
        conn, used_proxy = establish_connection(dev, db)
        for cmd, output in run_command_list(conn, commands, use_timing=used_proxy):
            results.append({"command": cmd, "output": output})
            db.device_info_jobs.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "results": results,
                        "updated_at": time.time(),
                        "status": "running",
                    }
                },
            )

        finish_time = time.time()
        db.device_info_jobs.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "succeeded",
                    "results": results,
                    "updated_at": finish_time,
                    "completed_at": finish_time,
                    "error": None,
                }
            },
        )
        print(f"[INFO] Job {job_id} completed ({len(results)} commands)")
    except Exception as e:
        finish_time = time.time()
        db.device_info_jobs.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "updated_at": finish_time,
                    "completed_at": finish_time,
                }
            },
        )
        print(f"[INFO] Job {job_id} failed: {e}")
    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass
