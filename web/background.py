import asyncio
import os
import time
from contextlib import suppress

from database import db
from services import run_device_info

REFRESH_INTERVAL = float(os.getenv("INTERFACE_REFRESH_INTERVAL", "30"))


async def device_info_refresh_loop():
    """Periodically queue interface summary jobs for ready devices."""
    try:
        while True:
            now = time.time()
            try:
                devices = list(db.devices.find({"status": "ready"}, {"_id": 1}))
                for dev in devices:
                    device_id = str(dev["_id"])
                    latest_job = db.device_info_jobs.find_one(
                        {"device_id": device_id}, sort=[("updated_at", -1)]
                    )
                    if latest_job:
                        status = latest_job.get("status")
                        updated_at = latest_job.get("updated_at")

                        # Skip if a job is already in progress
                        if status in ("queued", "running"):
                            continue

                        # Throttle refresh if the most recent result is still fresh
                        if (
                            status == "succeeded"
                            and updated_at
                            and (now - updated_at) < REFRESH_INTERVAL
                        ):
                            continue
                    try:
                        run_device_info(device_id, force=True)
                    except Exception as exc:  # pragma: no cover - log only
                        print(f"[AUTO INFO] Failed to queue job for {device_id}: {exc}")
            except Exception as loop_err:  # pragma: no cover - log only
                print(f"[AUTO INFO] Loop error: {loop_err}")
            await asyncio.sleep(REFRESH_INTERVAL)
    except asyncio.CancelledError:
        raise


def start_background_tasks():
    loop = asyncio.get_event_loop()
    task = loop.create_task(device_info_refresh_loop())
    return task


async def stop_background_tasks(task):
    if not task:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
