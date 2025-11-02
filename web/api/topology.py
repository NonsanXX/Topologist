from fastapi import APIRouter

from services import latest_topology, topology_graph, clear_topology

router = APIRouter(prefix="/api/topology", tags=["topology"])


@router.get("/latest")
def latest_topology_route():
    return latest_topology()


@router.get("/graph")
def topology_graph_route():
    return topology_graph()


@router.post("/clear")
def clear_topology_route():
    return clear_topology()
