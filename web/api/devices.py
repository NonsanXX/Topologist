from fastapi import APIRouter, Body

from services import (
    list_devices,
    add_device,
    delete_device,
    save_device_creds,
    update_device,
    run_device_command,
    run_device_info,
    get_latest_device_info,
    get_device_interfaces,
    set_device_interface_state,
)

router = APIRouter(prefix="/api/devices", tags=["devices"])


@router.get("")
def list_devices_route():
    return list_devices()


@router.post("")
def add_device_route(payload: dict = Body(...)):
    return add_device(payload)


@router.delete("/{device_id}")
def delete_device_route(device_id: str):
    return delete_device(device_id)


@router.post("/{device_id}/creds")
def save_device_creds_route(device_id: str, payload: dict = Body(...)):
    return save_device_creds(device_id, payload)


@router.patch("/{device_id}")
def update_device_route(device_id: str, payload: dict = Body(...)):
    return update_device(device_id, payload)


@router.post("/{device_id}/run_command")
def run_command_route(device_id: str, payload: dict = Body(...)):
    return run_device_command(device_id, payload)


@router.post("/{device_id}/info")
def run_device_info_route(device_id: str, force: bool = False):
    return run_device_info(device_id, force=force)


@router.get("/{device_id}/info/latest")
def get_latest_device_info_route(device_id: str):
    return get_latest_device_info(device_id)


@router.get("/{device_id}/interfaces")
def get_device_interfaces_route(device_id: str):
    return get_device_interfaces(device_id)


@router.post("/{device_id}/interfaces")
def set_device_interface_state_route(device_id: str, payload: dict = Body(...)):
    return set_device_interface_state(device_id, payload)
