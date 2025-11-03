from fastapi import APIRouter, Body

from services import (
    list_identities,
    get_identity,
    add_identity,
    set_default_identity,
    unset_default_identity,
    update_identity,
    delete_identity,
)

router = APIRouter(prefix="/api/identities", tags=["identities"])


@router.get("")
def list_identities_route():
    return list_identities()


@router.get("/{identity_id}")
def get_identity_route(identity_id: str):
    return get_identity(identity_id)


@router.post("")
def add_identity_route(payload: dict = Body(...)):
    return add_identity(payload)


@router.post("/{identity_id}/set_default")
def set_default_identity_route(identity_id: str):
    return set_default_identity(identity_id)


@router.post("/unset_default")
def unset_default_identity_route():
    return unset_default_identity()


@router.patch("/{identity_id}")
def update_identity_route(identity_id: str, payload: dict = Body(...)):
    return update_identity(identity_id, payload)


@router.delete("/{identity_id}")
def delete_identity_route(identity_id: str):
    return delete_identity(identity_id)
