import asyncio
import json
from redis_client import redis_manager
from kafka_client import KAFKA_BOOTSTRAP_SERVERS,TOPIC_NAME
from aiokafka import AIOKafkaConsumer
from metrics import ORDER_PROCESSING_COUNT


async def start_worker():
    await redis_manager.start()
    # await redis_manager.initialize_stock("phone", 15)

    consumer = AIOKafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        group_id="flash_sale_worker_group_v3",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    await consumer.start()
    print("worker is running and waiting for messages...")

    try:
        async for message in consumer:
            order_data= message.value
            user_id=order_data["user_id"]
            item_id="phone"
            print(f"processing order for user:{user_id}, item:{item_id}")

            status= await redis_manager.reserve_item(user_id,item_id)
            if status == "SUCCESS":
                print(f"✅ Reservation Successful for User: {user_id}")
                ORDER_PROCESSING_COUNT.labels(status="SUCCESS").inc()  # ➕ افزایش کانتر موفقیت

            elif status == "SOLD_OUT":
                print(f"❌ Product Sold Out! Request failed for User: {user_id}")
                ORDER_PROCESSING_COUNT.labels(status="SOLD_OUT").inc()  # ➕ افزایش کانتر اتمام موجودی

            elif status == "ALREADY_RESERVED":
                print(f"⚠️ User {user_id} has already reserved this item!")
                ORDER_PROCESSING_COUNT.labels(status="ALREADY_RESERVED").inc()  # ➕ افزایش کانتر درخواست تکراری

            else:
                print(f"❓ Unknown status from Redis: {status}")
                ORDER_PROCESSING_COUNT.labels(status="UNKNOWN").inc()
    except Exception as e:
        print(f"💥 Worker encountered an error: {e}")
    finally:
        print("🛑 Stopping worker and closing connections...")
        await consumer.stop()
        await redis_manager.stop()

if __name__ == "__main__":
    asyncio.run(start_worker())

