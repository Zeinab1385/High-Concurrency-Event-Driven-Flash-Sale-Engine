import asyncio
from redis.asyncio import Redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379


class RedisManager:
    def __init__(self):
        self.redis = None
        self.lua_reserve_script = """
                local stock_key = KEYS[1]
                local reservation_key = KEYS[2]
                local user_id = ARGV[1]
                local item_id = ARGV[2]
                
                if redis.call('EXISTS', reservation_key) == 1 then
                    return "ALREADY_RESERVED"
                end    

                -- ۱. بررسی موجودی فعلی کالا
                local current_stock = redis.call('GET', stock_key)

                if not current_stock or tonumber(current_stock) <= 0 then
                    return "SOLD_OUT"
                end

                -- ۲. کاهش اتمیک موجودی کالا
                redis.call('DECR', stock_key)

                -- ۳. ایجاد Hash برای ذخیره وضعیت رزرو کاربر
                redis.call('HSET', reservation_key, 'user_id', user_id, 'item_id', item_id, 'status', 'reserved')

                -- ۴. تنظیم TTL (مثلا ۱۲۰ ثانیه زمان برای نهایی کردن خرید)
                redis.call('EXPIRE', reservation_key, 120)

                return "SUCCESS"
                """

    async def start(self):
        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print("🚀 Redis Client connected successfully.")

    async def acquire_lock(self, user_id: str, order_id: str, expire_seconds: int = 15):
        if not self.redis:
            raise RuntimeError("Redis Client is not started. Please call start() first.")

        lock_key = f"lock:{user_id}:{order_id}"
        success = await self.redis.set(lock_key, "locked", nx=True, ex=expire_seconds)
        return success

    async def initialize_stock(self, item_id: str, count: int):
        if not self.redis:
            raise RuntimeError("Redis is not started.")
        await self.redis.set(f"item:{item_id}:stock", count, nx=True)
        print(f"📦 Stock for {item_id} initialized to {count}.")

    async def reserve_item(self, user_id: str, item_id: str) -> str:
        if not self.redis:
            raise RuntimeError("Redis is not started.")

        stock_key = f"item:{item_id}:stock"
        reservation_key = f"reservation:{user_id}:{item_id}"

        result = await self.redis.eval(
            self.lua_reserve_script,
            2,  # تعداد کلیدها (KEYS)
            stock_key, reservation_key,
            user_id, item_id
        )
        return result

    async def is_rate_limited(self, user_id: str, limit: int = 3, window_seconds: int = 10) -> bool:
        """
        بررسی محدودیت نرخ درخواست‌ها با الگوی مطمئن و اتمیک Counter
        """
        if not self.redis:
            raise RuntimeError("Redis Client is not started.")

        import time
        # تقسیم زمان بر اساس پنجره‌های ۱۰ ثانیه‌ای ثابت برای ساخت کلید منحصربه‌فرد
        current_window = int(time.time() / window_seconds)
        rate_limit_key = f"rate_limit:{user_id}:{current_window}"

        async with self.redis.pipeline(transaction=True) as pipe:
            # ۱. افزایش اتمیک مقدار شمارنده
            pipe.incr(rate_limit_key)
            # ۲. تنظیم زمان انقضا برای پاکسازی کلید
            pipe.expire(rate_limit_key, window_seconds + 2)

            results = await pipe.execute()

        # مقدار شمارنده بعد از افزایش (خروجی دستور اول در پایپ‌لاین)
        request_count = results[0]
        print(f"📊 User {user_id} requests in current window: {request_count}/{limit}")

        # اگر تعداد درخواست‌ها از حد مجاز فراتر رفت، محدودیت اعمال می‌شود
        if request_count > limit:
            return True

        return False

    async def stop(self):
        if self.redis:
            await self.redis.aclose()
            print("🛑 Redis Client stopped.")


redis_manager = RedisManager()


async def test_main():
    await redis_manager.start()

    await redis_manager.initialize_stock("phone", 5)

    # تست رزرو برای کاربران مختلف
    for i in range(1, 3):
        user = f"user_{i}"
        status = await redis_manager.reserve_item(user, "phone")
        print(f"User {user} purchase status: {status}")

    await redis_manager.stop()


if __name__ == "__main__":
    asyncio.run(test_main())