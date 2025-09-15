"""
Queue-based logging API endpoints for production use
"""
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import uuid
import redis
import json

from ..utils.logging_config import get_logger
from ..workers.log_tasks import (
    process_log_batch,
    query_logs_async,
    generate_log_report
)

logger = get_logger(__name__)

# Initialize router
router = APIRouter(prefix="/api/logs", tags=["Logging"])

# Redis client for job tracking
REDIS_URL = "redis://localhost:6379/1"
redis_client = redis.from_url(REDIS_URL)


@router.post(
    "/batch",
    summary="Submit log batch to queue",
    description="Submit a batch of log entries for asynchronous processing"
)
async def submit_log_batch(log_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Submit log entries to the processing queue

    This endpoint is designed for:
    - High-volume log ingestion
    - Non-blocking log writes
    - Batch processing efficiency
    """
    try:
        # Create batch job
        batch_id = str(uuid.uuid4())

        # Submit to Celery queue
        task = process_log_batch.apply_async(
            args=[log_entries],
            task_id=batch_id,
            queue='logs'
        )

        logger.info(f"Submitted log batch {batch_id} with {len(log_entries)} entries")

        return {
            "batch_id": batch_id,
            "entries": len(log_entries),
            "status": "queued",
            "message": "Log batch submitted for processing"
        }

    except Exception as e:
        logger.error(f"Error submitting log batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/query",
    summary="Submit async log query",
    description="Submit a log query job for asynchronous execution"
)
async def submit_log_query(
    level: Optional[str] = Query(None, enum=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    logger_name: Optional[str] = Query(None),
    search_text: Optional[str] = Query(None),
    hours_ago: Optional[int] = Query(24),
    limit: int = Query(1000, ge=1, le=10000)
) -> Dict[str, Any]:
    """
    Submit a log query to the queue for async processing

    Use this for:
    - Large log queries that might be slow
    - Complex searches across many logs
    - Reports that need heavy processing
    """
    try:
        job_id = str(uuid.uuid4())

        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_ago) if hours_ago else None

        # Submit query task
        task = query_logs_async.apply_async(
            args=[
                job_id,
                start_time.isoformat() if start_time else None,
                end_time.isoformat(),
                level,
                logger_name,
                search_text,
                limit
            ],
            task_id=job_id,
            queue='logs'
        )

        logger.info(f"Submitted log query {job_id}")

        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Query submitted for processing",
            "query_params": {
                "level": level,
                "logger_name": logger_name,
                "search_text": search_text,
                "hours_ago": hours_ago,
                "limit": limit
            }
        }

    except Exception as e:
        logger.error(f"Error submitting log query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/query/{job_id}",
    summary="Get query results",
    description="Retrieve results of an async log query"
)
async def get_query_results(job_id: str) -> Dict[str, Any]:
    """
    Get the results of a previously submitted log query
    """
    try:
        # Get job data from Redis
        job_data = redis_client.hgetall(f"log_query:{job_id}")

        if not job_data:
            raise HTTPException(status_code=404, detail="Query job not found")

        # Decode bytes to string
        job_data = {
            k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v
            for k, v in job_data.items()
        }

        # Parse result if available
        if job_data.get("status") == "completed" and "result" in job_data:
            job_data["result"] = json.loads(job_data["result"])

        return job_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting query results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/report",
    summary="Generate log report",
    description="Submit a request to generate various log reports asynchronously"
)
async def generate_report(
    report_type: str = Query(
        ...,
        enum=["error_summary", "performance", "crawler_activity", "system_health"],
        description="Type of report to generate"
    ),
    hours: int = Query(24, ge=1, le=168, description="Hours of data to analyze")
) -> Dict[str, Any]:
    """
    Generate comprehensive log reports asynchronously

    Report types:
    - error_summary: Detailed error analysis
    - performance: Performance metrics from logs
    - crawler_activity: Crawler operation summary
    - system_health: Overall system health score
    """
    try:
        job_id = str(uuid.uuid4())

        # Submit report generation task
        task = generate_log_report.apply_async(
            args=[job_id, report_type, hours],
            task_id=job_id,
            queue='logs'
        )

        logger.info(f"Submitted report generation {job_id} ({report_type})")

        return {
            "job_id": job_id,
            "status": "queued",
            "report_type": report_type,
            "message": "Report generation started",
            "estimated_time": "30-60 seconds"
        }

    except Exception as e:
        logger.error(f"Error submitting report generation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/report/{job_id}",
    summary="Get report results",
    description="Retrieve generated report results"
)
async def get_report_results(job_id: str) -> Dict[str, Any]:
    """
    Get the results of a previously generated report
    """
    try:
        # Get report data from Redis
        report_data = redis_client.hgetall(f"log_report:{job_id}")

        if not report_data:
            raise HTTPException(status_code=404, detail="Report not found")

        # Decode bytes to string
        report_data = {
            k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v
            for k, v in report_data.items()
        }

        # Parse result if available
        if report_data.get("status") == "completed" and "result" in report_data:
            report_data["result"] = json.loads(report_data["result"])

        return report_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting report results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/stats",
    summary="Get logging system stats",
    description="Get real-time statistics about the logging system"
)
async def get_logging_stats() -> Dict[str, Any]:
    """
    Get statistics about log processing from Redis
    """
    try:
        # Get stats from Redis
        stats = redis_client.hgetall("log_stats:latest")

        if stats:
            stats = {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in stats.items()
            }
        else:
            stats = {
                "total_processed": 0,
                "total_failed": 0,
                "last_batch_time": None
            }

        # Get queue length
        from ..workers.celery_app import app
        inspect = app.control.inspect()

        # Count pending log tasks
        reserved = inspect.reserved()
        active = inspect.active()

        log_queue_length = 0
        if reserved:
            for worker, tasks in reserved.items():
                log_queue_length += len([t for t in tasks if 'log' in t.get('name', '')])

        if active:
            for worker, tasks in active.items():
                log_queue_length += len([t for t in tasks if 'log' in t.get('name', '')])

        return {
            "processing_stats": stats,
            "queue_length": log_queue_length,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting logging stats: {e}")
        return {
            "error": str(e),
            "message": "Could not retrieve logging statistics"
        }


@router.post(
    "/stream",
    summary="Stream logs to queue",
    description="Stream log entries to the queue in real-time"
)
async def stream_logs(
    background_tasks: BackgroundTasks,
    log_entry: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Stream individual log entries to the queue

    This is useful for:
    - Real-time log forwarding
    - External log aggregation
    - Microservice log collection
    """
    try:
        # Add timestamp if not present
        if 'timestamp' not in log_entry:
            log_entry['timestamp'] = datetime.now().isoformat()

        # Queue single entry as a batch of 1
        background_tasks.add_task(
            process_log_batch.apply_async,
            args=[[log_entry]],
            queue='logs',
            priority=1  # Lower priority for individual entries
        )

        return {
            "status": "accepted",
            "message": "Log entry queued for processing"
        }

    except Exception as e:
        logger.error(f"Error streaming log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/jobs",
    summary="List log processing jobs",
    description="List all log query and report jobs"
)
async def list_log_jobs(
    job_type: Optional[str] = Query(None, enum=["query", "report"]),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    List recent log processing jobs
    """
    try:
        jobs = []

        # Get query jobs
        if not job_type or job_type == "query":
            query_keys = redis_client.keys("log_query:*")[:limit]
            for key in query_keys:
                job_data = redis_client.hgetall(key)
                if job_data:
                    job_data = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in job_data.items()
                    }
                    job_data["job_type"] = "query"
                    job_data["job_id"] = key.decode().split(":")[1] if isinstance(key, bytes) else key.split(":")[1]
                    jobs.append(job_data)

        # Get report jobs
        if not job_type or job_type == "report":
            report_keys = redis_client.keys("log_report:*")[:limit]
            for key in report_keys:
                job_data = redis_client.hgetall(key)
                if job_data:
                    job_data = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in job_data.items()
                    }
                    job_data["job_type"] = "report"
                    job_data["job_id"] = key.decode().split(":")[1] if isinstance(key, bytes) else key.split(":")[1]
                    jobs.append(job_data)

        # Sort by timestamp (newest first)
        jobs.sort(
            key=lambda x: x.get("started_at", "") or x.get("completed_at", ""),
            reverse=True
        )

        return {
            "jobs": jobs[:limit],
            "total": len(jobs),
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Error listing log jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))