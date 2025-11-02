from fastapi import APIRouter

from services import get_device_info_job

router = APIRouter(prefix="/api/device_infos", tags=["device-info"])


@router.get("/{job_id}")
def get_info_job_route(job_id: str):
    return get_device_info_job(job_id)
