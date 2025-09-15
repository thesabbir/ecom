from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
from datetime import datetime
import uuid
import redis
import json

from ..models.product import CrawlRequest, CrawlResponse
from ..workers.crawl_tasks import crawl_website, cancel_crawl_job
from ..utils.logging_config import get_logger

logger = get_logger(__name__)

# Initialize router
router = APIRouter(prefix="/api", tags=["Crawl Queue"])

# Redis client for job tracking
REDIS_URL = "redis://localhost:6379/1"
redis_client = redis.from_url(REDIS_URL)


@router.post(
    "/crawl",
    response_model=CrawlResponse,
    summary="Submit crawl job to queue",
    description="Submit a crawl job to the Celery task queue for processing by workers",
)
async def submit_crawl_job(request: CrawlRequest) -> CrawlResponse:
    """
    Submit a crawl job to the task queue
    """
    try:
        # Generate job ID
        job_id = str(uuid.uuid4())

        # Prepare job data
        job_data = {
            "job_id": job_id,
            "status": "queued",
            "url": str(request.url),
            "created_at": datetime.now().isoformat(),
            "request": request.model_dump_json(),
        }

        # Store job in Redis
        redis_client.hset(f"job:{job_id}", mapping=job_data)

        # Submit to Celery queue
        crawl_website.apply_async(
            args=[job_id, request.model_dump()],
            task_id=job_id,
            queue="crawl_default",
            priority=5,
        )

        logger.info(f"Submitted crawl job {job_id} to queue")

        return CrawlResponse(
            job_id=job_id,
            status="queued",
            products_found=0,
            message="Job submitted to queue",
            started_at=datetime.now(),
            completed_at=None,
        )

    except Exception as e:
        logger.error(f"Error submitting crawl job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/crawl/{job_id}",
    summary="Get crawl job status",
    description="Get the current status and results of a crawl job",
)
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get job status from Redis
    """
    try:
        job_data = redis_client.hgetall(f"job:{job_id}")

        if not job_data:
            raise HTTPException(status_code=404, detail="Job not found")

        # Decode bytes to string
        job_data = {
            k.decode() if isinstance(k, bytes) else k: v.decode()
            if isinstance(v, bytes)
            else v
            for k, v in job_data.items()
        }

        # Parse result if available
        if "result" in job_data:
            job_data["result"] = json.loads(job_data["result"])

        return job_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/crawl/{job_id}",
    summary="Cancel crawl job",
    description="Cancel a running or queued crawl job",
)
async def cancel_job(job_id: str) -> Dict[str, Any]:
    """
    Cancel a crawl job
    """
    try:
        cancel_crawl_job.apply_async(args=[job_id])
        return {"message": "Cancellation requested", "job_id": job_id}

    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/jobs",
    summary="List crawl jobs",
    description="List all crawl jobs with optional filtering",
)
async def list_jobs(
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """
    List crawl jobs with pagination
    """
    try:
        # Get all job keys
        job_keys = redis_client.keys("job:*")
        jobs = []

        for key in job_keys:
            job_data = redis_client.hgetall(key)
            if job_data:
                # Decode bytes
                job_data = {
                    k.decode() if isinstance(k, bytes) else k: v.decode()
                    if isinstance(v, bytes)
                    else v
                    for k, v in job_data.items()
                }

                # Filter by status if provided
                if status and job_data.get("status") != status:
                    continue

                jobs.append(job_data)

        # Sort by created_at (newest first)
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        # Apply pagination
        total = len(jobs)
        jobs = jobs[offset : offset + limit]

        return {"total": total, "limit": limit, "offset": offset, "jobs": jobs}

    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/workers",
    summary="Get worker status",
    description="Get information about active Celery workers",
)
async def get_worker_status() -> Dict[str, Any]:
    """
    Get worker status from Celery
    """
    try:
        from ..workers.celery_app import app

        # Get worker stats
        inspect = app.control.inspect()
        stats = inspect.stats()
        active = inspect.active()
        reserved = inspect.reserved()
        registered = inspect.registered()

        return {
            "stats": stats,
            "active_tasks": active,
            "reserved_tasks": reserved,
            "registered_tasks": registered,
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error getting worker status: {e}")
        return {
            "error": str(e),
            "message": "Could not get worker status. Ensure workers are running.",
            "timestamp": datetime.now().isoformat(),
        }


@router.get(
    "/queue/stats",
    summary="Get queue statistics",
    description="Get statistics about the task queue",
)
async def get_queue_stats() -> Dict[str, Any]:
    """
    Get queue statistics from Redis
    """
    try:
        # Get queue lengths
        queues = ["crawl_high", "crawl_default", "crawl_low"]
        queue_stats = {}

        for queue_name in queues:
            length = redis_client.llen(queue_name)
            queue_stats[queue_name] = length

        # Get job counts by status
        job_keys = redis_client.keys("job:*")
        status_counts = {
            "queued": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
            "timeout": 0,
        }

        for key in job_keys:
            status = redis_client.hget(key, "status")
            if status:
                status = status.decode() if isinstance(status, bytes) else status
                if status in status_counts:
                    status_counts[status] += 1

        return {
            "queues": queue_stats,
            "jobs_by_status": status_counts,
            "total_jobs": len(job_keys),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
