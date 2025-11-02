from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api import routers as api_routers
from background import start_background_tasks, stop_background_tasks


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = start_background_tasks()
    app.state.interface_task = task
    try:
        yield
    finally:
        await stop_background_tasks(task)


app = FastAPI(title="Topologist Web", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")

@app.get("/info")
def info_page():
    return FileResponse("static/info.html")
for router in api_routers:
    app.include_router(router)
