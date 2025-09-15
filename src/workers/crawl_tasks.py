import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any
from celery import Task
from celery.exceptions import SoftTimeLimitExceeded
import redis

from .celery_app import app
from ..crawlers.ryans import RyansCrawler
from ..crawlers.startech import StarTechCrawler
from ..crawlers.base import GenericCrawler
from ..models.product import CrawlRequest
from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)
redis_client = redis.from_url(app.conf.broker_url)


class CrawlTask(Task):
    """Base task with error handling and monitoring"""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails"""
        logger.error(f"Task {task_id} failed: {exc}")
        # Update job status in Redis
        job_id = kwargs.get("job_id")
        if job_id:
            job_data = {
                "status": "failed",
                "error": str(exc),
                "completed_at": datetime.now().isoformat(),
            }
            redis_client.hset(f"job:{job_id}", mapping=job_data)

    def on_success(self, retval, task_id, args, kwargs):
        """Called when task succeeds"""
        logger.info(f"Task {task_id} completed successfully")
        job_id = kwargs.get("job_id")
        if job_id:
            job_data = {
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
            }
            redis_client.hset(f"job:{job_id}", mapping=job_data)


@app.task(base=CrawlTask, bind=True, max_retries=3)
def crawl_website(self, job_id: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main crawl task that runs in a separate worker process
    """
    try:
        # Update job status
        redis_client.hset(
            f"job:{job_id}",
            mapping={
                "status": "running",
                "started_at": datetime.now().isoformat(),
                "worker_id": self.request.id,
            },
        )

        # Run the async crawl in sync context
        result = asyncio.run(_async_crawl(job_id, request_data))

        # Store result
        redis_client.hset(
            f"job:{job_id}",
            mapping={
                "status": "completed",
                "products_found": result["products_found"],
                "completed_at": datetime.now().isoformat(),
                "result": json.dumps(result),
            },
        )

        # Set expiration for job data (7 days)
        redis_client.expire(f"job:{job_id}", 604800)

        return result

    except SoftTimeLimitExceeded:
        logger.error(f"Task {job_id} exceeded soft time limit")
        redis_client.hset(
            f"job:{job_id}",
            mapping={
                "status": "timeout",
                "error": "Task exceeded time limit",
                "completed_at": datetime.now().isoformat(),
            },
        )
        raise

    except Exception as e:
        logger.error(f"Task {job_id} failed: {e}")
        # Retry with exponential backoff
        raise self.retry(exc=e, countdown=60 * (2**self.request.retries))


async def _async_crawl(job_id: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Async crawl implementation
    """

    request = CrawlRequest(**request_data)
    storage = ParquetStorage()
    products = []

    # Get appropriate crawler
    crawler = None
    try:
        url = str(request.url)

        if "ryans.com" in url:
            crawler = RyansCrawler()
        elif "startech.com" in url:
            crawler = StarTechCrawler()
        else:
            crawler = GenericCrawler(url)

        async with crawler:
            # Crawl based on URL pattern
            if "ryans.com/categories" in url and isinstance(crawler, RyansCrawler):
                products = await crawler.crawl_all_categories(
                    max_pages_per_category=request.max_pages,
                    skip_duplicates=request.skip_duplicates,
                    skip_if_scraped_within_hours=request.skip_if_scraped_within_hours,
                    overwrite=request.overwrite,
                )
            elif "ryans.com/category/" in url and isinstance(crawler, RyansCrawler):
                products = await crawler.crawl_category_parallel(
                    url,
                    max_pages=request.max_pages,
                    skip_duplicates=request.skip_duplicates,
                    skip_if_scraped_within_hours=request.skip_if_scraped_within_hours,
                )
            elif (
                "startech.com.bd/component/" in url
                or "startech.com.bd/category/" in url
            ) and isinstance(crawler, StarTechCrawler):
                products = await crawler.crawl_category_parallel(
                    url,
                    max_pages=request.max_pages,
                    skip_duplicates=request.skip_duplicates,
                    skip_if_scraped_within_hours=request.skip_if_scraped_within_hours,
                )
            else:
                # Generic crawling
                product_urls = await crawler.extract_product_urls(
                    url, max_pages=request.max_pages
                )
                for product_url in product_urls:
                    product = await crawler.extract_product_data(product_url)
                    if product:
                        products.append(product)

            # Apply filters
            if request.category_filter:
                products = [
                    p
                    for p in products
                    if p.category
                    and request.category_filter.lower() in p.category.lower()
                ]

            if request.price_min is not None:
                products = [p for p in products if p.price >= request.price_min]

            if request.price_max is not None:
                products = [p for p in products if p.price <= request.price_max]

            # Save products
            storage.save_products(products)

            return {
                "job_id": job_id,
                "products_found": len(products),
                "status": "completed",
                "url": url,
                "site": crawler.site_name
                if hasattr(crawler, "site_name")
                else "generic",
            }

    finally:
        if crawler:
            await crawler.close()


@app.task
def crawl_category(category_url: str, max_pages: int = 1) -> Dict[str, Any]:
    """
    Lightweight task for crawling a single category
    """
    return asyncio.run(_async_crawl_category(category_url, max_pages))


async def _async_crawl_category(category_url: str, max_pages: int) -> Dict[str, Any]:
    """Async implementation for category crawling"""
    # Implementation here
    pass


@app.task
def cleanup_old_jobs():
    """
    Periodic task to cleanup old job data from Redis
    """
    try:
        # Get all job keys
        job_keys = redis_client.keys("job:*")

        cleanup_count = 0
        cutoff_time = datetime.now() - timedelta(days=7)

        for key in job_keys:
            job_data = redis_client.hgetall(key)
            if job_data:
                # Decode bytes to string if needed
                job_data = {
                    k.decode() if isinstance(k, bytes) else k: v.decode()
                    if isinstance(v, bytes)
                    else v
                    for k, v in job_data.items()
                }

                completed_at = job_data.get("completed_at")
                if completed_at:
                    completed_time = datetime.fromisoformat(completed_at)
                    if completed_time < cutoff_time:
                        redis_client.delete(key)
                        cleanup_count += 1

        logger.info(f"Cleaned up {cleanup_count} old jobs")
        return {"cleaned": cleanup_count}

    except Exception as e:
        logger.error(f"Error cleaning up old jobs: {e}")
        return {"error": str(e)}


@app.task
def worker_health_check():
    """
    Periodic health check for workers
    """
    try:
        # Check Redis connection
        redis_client.ping()

        # Check storage
        storage = ParquetStorage()
        _ = storage.load_products()

        # Log worker stats
        stats = {
            "timestamp": datetime.now().isoformat(),
            "worker": "healthy",
            "redis": "connected",
            "storage": "accessible",
        }

        logger.info(f"Worker health check: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Worker health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


@app.task(bind=True)
def cancel_crawl_job(self, job_id: str) -> Dict[str, Any]:
    """
    Cancel a running crawl job
    """
    try:
        # Get job data from Redis
        job_data = redis_client.hgetall(f"job:{job_id}")
        if not job_data:
            return {"error": "Job not found"}

        # Decode bytes
        job_data = {
            k.decode() if isinstance(k, bytes) else k: v.decode()
            if isinstance(v, bytes)
            else v
            for k, v in job_data.items()
        }

        # If job has a worker_id, revoke it
        worker_id = job_data.get("worker_id")
        if worker_id:
            app.control.revoke(worker_id, terminate=True)

        # Update job status
        redis_client.hset(
            f"job:{job_id}",
            mapping={"status": "cancelled", "cancelled_at": datetime.now().isoformat()},
        )

        return {"status": "cancelled", "job_id": job_id}

    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        return {"error": str(e)}
