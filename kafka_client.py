import asyncio
from aiokafka import AIOKafkaProducer
import json
import os

TOPIC_NAME="flash_sale_orders"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


class KafkaManager:
    def __init__(self):
        self.producer = None
    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        await self.producer.start()
        print(f"Producer started at {KAFKA_BOOTSTRAP_SERVERS}")
    async def send_order(self ,user_id:str,order_id:str):
        if not self.producer:
            raise RuntimeError("producer not initialized")
        payload={
            "user_id":user_id,
            "order_id":order_id
        }
        await self.producer.send_and_wait(TOPIC_NAME, payload)
        print(f"Sent order {order_id} at {KAFKA_BOOTSTRAP_SERVERS}")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            print("Producer stopped")

KAFKA_MANAGER = KafkaManager()