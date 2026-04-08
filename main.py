from fastapi.security import HTTPBearer
from fastapi.openapi.models import APIKey, APIKeyIn, SecuritySchemeType
from fastapi.openapi.utils import get_openapi
from fastapi import Depends, HTTPException
from fastapi.security.http import HTTPAuthorizationCredentials
from contextlib import asynccontextmanager

bearer_scheme = HTTPBearer()
from fastapi import FastAPI
from auth import router as auth_router


# ─── Background Scheduler (APScheduler) ───────────────────────────────────────
from apscheduler.schedulers.background import BackgroundScheduler
from scheduler import run_scheduled_jobs, run_single_task, Session
from db import ScheduledTask

_scheduler = BackgroundScheduler()
_scheduler.add_job(run_scheduled_jobs, "interval", minutes=1, id="scheduled_tasks_job")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _scheduler.start()
    print("[Scheduler] APScheduler started — checking tasks every minute.")
    yield
    _scheduler.shutdown(wait=False)
    print("[Scheduler] APScheduler stopped.")


app = FastAPI(lifespan=lifespan)

# Mount routes
app.include_router(auth_router)


@app.get("/")
def home():
    return {"message": "Hotel Labor API running"}


# ─── Send Now endpoint ─────────────────────────────────────────────────────────
@app.post("/run-task/{task_id}")
def run_task_now(task_id: int):
    """Immediately execute a single scheduled task and send its report email."""
    session = Session()
    try:
        task = session.query(ScheduledTask).filter(ScheduledTask.id == task_id).first()
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
        ok, message = run_single_task(task, session=session)
        if ok:
            return {"status": "success", "message": message}
        else:
            raise HTTPException(status_code=500, detail=message)
    finally:
        session.close()


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="LaborLens API",
        version="1.0.0",
        description="API for multi-hotel labor tool with JWT login",
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT"
        }
    }
    for path in openapi_schema["paths"].values():
        for method in path.values():
            method["security"] = [{"BearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

if __name__ == "__main__":
    from db import Base, ENGINE
    import db

    Base.metadata.create_all(bind=ENGINE)
    print("✅ All missing tables created")
