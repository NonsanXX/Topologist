import re
import time
from bson.objectid import ObjectId

from .utils import establish_connection, run_command_list


def do_cli_job(job, db):
    job_id = job.get("job_id")
    if not job_id:
        print("[CLI] Received job without job_id")
        return

    try:
        job_oid = ObjectId(job_id)
    except Exception:
        print(f"[CLI] Invalid job_id {job_id}")
        return

    doc = db.device_commands.find_one({"_id": job_oid})
    if not doc:
        print(f"[CLI] Job document {job_id} not found")
        return

    device_id = doc.get("device_id") or job.get("device_id")
    command = doc.get("command") or job.get("command")
    if not command or not command.strip():
        db.device_commands.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "failed",
                    "error": "command missing",
                    "updated_at": time.time(),
                    "completed_at": time.time(),
                }
            },
        )
        print(f"[CLI] Job {job_id} missing command")
        return

    try:
        device_oid = ObjectId(device_id)
    except Exception:
        db.device_commands.update_one(
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
        print(f"[CLI] Job {job_id} invalid device id {device_id}")
        return

    dev = db.devices.find_one({"_id": device_oid})
    if not dev:
        db.device_commands.update_one(
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
        print(f"[CLI] Device {device_id} not found for job {job_id}")
        return

    start_time = time.time()
    db.device_commands.update_one(
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
    try:
        conn, used_proxy = establish_connection(dev, db)

        is_config_cmd = bool(
            re.search(
                r"^(conf|hostname|int(erface)?|ip route|enable)",
                command.strip(),
                re.IGNORECASE,
            )
        )
        if is_config_cmd:
            commands = [line for line in command.splitlines() if line.strip()]
            commands = commands or [command.strip()]
            output = conn.send_config_set(commands)
        else:
            outputs = run_command_list(conn, [command], use_timing=used_proxy)
            output = outputs[0][1]

        finish_time = time.time()
        db.device_commands.update_one(
            {"_id": job_oid},
            {
                "$set": {
                    "status": "succeeded",
                    "output": output,
                    "updated_at": finish_time,
                    "completed_at": finish_time,
                    "error": None,
                }
            },
        )
        print(f"[CLI] Job {job_id} completed")
    except Exception as e:
        finish_time = time.time()
        db.device_commands.update_one(
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
        print(f"[CLI] Job {job_id} failed: {e}")
    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass
