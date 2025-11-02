from .identities import router as identities_router
from .devices import router as devices_router
from .commands import router as commands_router
from .topology import router as topology_router
from .orchestrator import router as orchestrator_router
from .info import router as info_router

routers = [
    identities_router,
    devices_router,
    commands_router,
    info_router,
    topology_router,
    orchestrator_router,
]

__all__ = ["routers"]
