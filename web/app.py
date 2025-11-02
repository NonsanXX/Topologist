from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from api import routers as api_routers
from background import start_background_tasks, stop_background_tasks

app = FastAPI(title="Topologist Web")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")

@app.get("/info")
def info_page():
    return FileResponse("static/info.html")


@app.on_event("startup")
async def startup_event():
    app.state.interface_task = start_background_tasks()


@app.on_event("shutdown")
async def shutdown_event():
    task = getattr(app.state, "interface_task", None)
    if task:
        await stop_background_tasks(task)


for router in api_routers:
    app.include_router(router)
