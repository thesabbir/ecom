import os
from celery import Celery
from kombu import Queue, Exchange
from datetime import timedelta

# Get Redis URL from environment or use default
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BROKER_URL = os.getenv("CELERY_BROKER_URL", f"{REDIS_URL}/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", f"{REDIS_URL}/1")

# Create Celery app
app = Celery(
    "ecommerce_crawler",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=[
        "src.workers.crawl_tasks",
        "src.workers.log_tasks"
    ],
)

# Configure Celery
app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Result backend settings
    result_expires=3600 * 24,  # Results expire after 24 hours
    result_backend_always_retry=True,
    result_backend_max_retries=10,
    # Worker settings
    worker_prefetch_multiplier=1,  # Only fetch one task at a time
    worker_max_tasks_per_child=50,  # Restart worker after 50 tasks to prevent memory leaks
    worker_disable_rate_limits=False,
    # Task execution settings
    task_soft_time_limit=1800,  # 30 minutes soft limit
    task_time_limit=2100,  # 35 minutes hard limit
    task_acks_late=True,  # Tasks are acknowledged after they complete
    task_reject_on_worker_lost=True,
    # Queue configuration
    task_default_queue="crawl_default",
    task_queues=(
        Queue("crawl_high", Exchange("crawl"), routing_key="crawl.high"),
        Queue("crawl_default", Exchange("crawl"), routing_key="crawl.default"),
        Queue("crawl_low", Exchange("crawl"), routing_key="crawl.low"),
        Queue("logs", Exchange("logs"), routing_key="logs.process"),
    ),
    task_routes={
        "src.workers.crawl_tasks.crawl_website": {"queue": "crawl_default"},
        "src.workers.crawl_tasks.crawl_category": {"queue": "crawl_default"},
        "src.workers.crawl_tasks.crawl_product": {"queue": "crawl_low"},
        "src.workers.log_tasks.process_log_batch": {"queue": "logs"},
        "src.workers.log_tasks.query_logs_async": {"queue": "logs"},
        "src.workers.log_tasks.generate_log_report": {"queue": "logs"},
    },
    # Rate limiting
    task_annotations={
        "src.workers.crawl_tasks.crawl_website": {
            "rate_limit": "10/m",  # 10 crawls per minute
            "max_retries": 3,
            "default_retry_delay": 60,
        },
        "src.workers.crawl_tasks.crawl_category": {
            "rate_limit": "20/m",  # 20 category crawls per minute
            "max_retries": 3,
            "default_retry_delay": 30,
        },
    },
    # Beat schedule (for periodic tasks)
    beat_schedule={
        "cleanup-old-jobs": {
            "task": "src.workers.crawl_tasks.cleanup_old_jobs",
            "schedule": timedelta(hours=1),
        },
        "health-check": {
            "task": "src.workers.crawl_tasks.worker_health_check",
            "schedule": timedelta(minutes=5),
        },
        "cleanup-old-logs": {
            "task": "src.workers.log_tasks.cleanup_old_logs",
            "schedule": timedelta(days=1),
        },
        "compress-logs": {
            "task": "src.workers.log_tasks.compress_logs",
            "schedule": timedelta(days=7),
        },
    },
)

if __name__ == "__main__":
    app.start()
