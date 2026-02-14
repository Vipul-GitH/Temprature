from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.routers import temperature
from app.scheduler import start_alert_scheduler, start_dummy_scheduler

app = FastAPI(title="Room Temperature Dashboard")

# Serve static assets
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Routes
app.include_router(temperature.router)
start_alert_scheduler()
start_dummy_scheduler()


@app.get("/")
def root():
    return RedirectResponse(url="/temperature", status_code=302)


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=3003,
        reload=False,
    )
