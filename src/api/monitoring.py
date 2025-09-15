"""
Health, metrics, and monitoring endpoints
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import Response
from datetime import datetime

from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger
from prometheus_client import generate_latest, Counter, Histogram, Gauge

logger = get_logger(__name__)
router = APIRouter(tags=["Monitoring"])
storage = ParquetStorage()

# Prometheus metrics
http_requests_total = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "HTTP request duration", ["method", "endpoint"]
)
crawl_duration_seconds = Histogram(
    "crawl_duration_seconds", "Crawl job duration in seconds"
)
crawl_jobs_total = Counter("crawl_jobs_total", "Total number of crawl jobs", ["status"])
products_scraped_total = Counter(
    "products_scraped_total", "Total number of products scraped"
)
errors_total = Counter("errors_total", "Total number of errors", ["error_type"])
storage_operations_total = Counter(
    "storage_operations_total", "Storage operations", ["operation", "status"]
)
active_crawl_jobs = Gauge("active_crawl_jobs", "Number of active crawl jobs")


@router.get(
    "/health",
    summary="Health check endpoint",
    description="Check API and storage health status",
)
async def health_check():
    """Health check endpoint for monitoring"""
    storage_health = "healthy"
    active_jobs = active_crawl_jobs._value.get() if hasattr(active_crawl_jobs, '_value') else 0

    try:
        # Test storage connection
        storage.get_stats()
    except Exception as e:
        storage_health = f"unhealthy: {str(e)}"
        logger.error(f"Storage health check failed: {e}")

    return {
        "status": "healthy" if storage_health == "healthy" else "degraded",
        "timestamp": datetime.now().isoformat(),
        "storage": storage_health,
        "active_crawl_jobs": active_jobs,
    }


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    description="Export metrics in Prometheus format",
)
async def get_metrics():
    """Return Prometheus metrics"""
    return Response(generate_latest(), media_type="text/plain")


@router.get(
    "/api/stats",
    summary="Get crawler statistics",
    description="Get detailed statistics about the crawler operations",
)
async def get_stats():
    """Get detailed crawler statistics"""
    try:
        stats = storage.get_stats()

        # Add metrics
        stats["metrics"] = {
            "total_crawl_jobs": sum(crawl_jobs_total._metrics.values()) if hasattr(crawl_jobs_total, '_metrics') else 0,
            "total_products_scraped": products_scraped_total._value.get() if hasattr(products_scraped_total, '_value') else 0,
            "total_errors": sum(errors_total._metrics.values()) if hasattr(errors_total, '_metrics') else 0,
            "active_jobs": active_crawl_jobs._value.get() if hasattr(active_crawl_jobs, '_value') else 0,
        }

        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/api/storage/stats",
    summary="Get storage statistics",
    description="Get information about data storage",
)
async def get_storage_stats():
    """Get storage statistics"""
    try:
        return storage.get_stats()
    except Exception as e:
        logger.error(f"Error getting storage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/api/storage/clear",
    summary="Clear all stored data",
    description="Delete all products from storage (use with caution)",
)
async def clear_storage(confirm: bool = Query(False)):
    """Clear all stored data"""
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Set confirm=true to clear all data",
        )

    try:
        # Clear parquet file
        import os

        if os.path.exists(storage.parquet_path):
            os.remove(storage.parquet_path)
            logger.info("Cleared parquet storage")

        return {"message": "Storage cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing storage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Export metrics for use in other modules
__all__ = [
    'http_requests_total',
    'http_request_duration_seconds',
    'crawl_duration_seconds',
    'crawl_jobs_total',
    'products_scraped_total',
    'errors_total',
    'storage_operations_total',
    'active_crawl_jobs'
]