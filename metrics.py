from prometheus_client import Counter, Histogram, make_asgi_app

# ۱. تعریف شمارنده برای کل درخواست‌های ورودی به وب‌سایت
REQUEST_COUNT = Counter(
    'flash_sale_requests_total',
    'Total number of buy requests received'
)

# ۲. تعریف شمارنده برای وضعیت خروجی پردازش صف (Erased, Sold Out, Success)
ORDER_PROCESSING_COUNT = Counter(
    'flash_sale_orders_processed_total',
    'Total number of orders processed by workers',
    ['status']
)

REQUEST_LATENCY = Histogram(
    'flash_sale_request_latency_seconds',
    'Time spent processing buy requests'
)

metrics_app = make_asgi_app()