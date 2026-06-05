from fastapi import APIRouter , HTTPException , status
from pydantic import BaseModel
from contextlib import asynccontextmanager
from kafka_client import KAFKA_MANAGER
from redis_client import redis_manager
from metrics import REQUEST_COUNT, REQUEST_LATENCY

class orederRequest(BaseModel):
    user_id: str
    order_id: str

@asynccontextmanager
async def lifespan_router(router:APIRouter):
    await KAFKA_MANAGER.start()
    await redis_manager.start()
    await redis_manager.initialize_stock(item_id="phone", count=5)
    yield
    await KAFKA_MANAGER.stop()
    await redis_manager.stop()
router = APIRouter(lifespan=lifespan_router)


@router.post("/buy", status_code=status.HTTP_202_ACCEPTED)
async def buy(request: orederRequest):
    with REQUEST_LATENCY.time():
        REQUEST_COUNT.inc()
        is_allowed = await redis_manager.acquire_lock(request.user_id, request.order_id, expire_seconds=15)
        if not is_allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Duplicate request detected! Please wait 15 seconds."
            )

        is_limited = await redis_manager.is_rate_limited(request.user_id, limit=2, window_seconds=10)
        if is_limited:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests! Please slow down."
            )

        await KAFKA_MANAGER.send_order(user_id=request.user_id, order_id=request.order_id)
        return {
            "status": "In Queue",
            "message": "Your request has been received and is being processed."
        }

