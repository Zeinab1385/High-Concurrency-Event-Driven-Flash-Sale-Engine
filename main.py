from fastapi import FastAPI
from api import router as api_router
from metrics import metrics_app
app = FastAPI(
    title="Flash Sale",
    description="This API handles high-volume requests and queues them into Kafka.",
    version="1.0",
)

app.include_router(api_router)

app.mount("/metrics", metrics_app)
@app.get("/health")
async def health_check():
    return {"status": "ok"}