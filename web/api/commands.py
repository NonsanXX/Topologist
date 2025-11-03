from fastapi import APIRouter

from services import get_command_job

router = APIRouter(prefix="/api/device_commands", tags=["commands"])


@router.get("/{job_id}")
def get_command_job_route(job_id: str):
    return get_command_job(job_id)
