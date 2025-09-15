from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from ..observability.logging import log_storage

router = APIRouter(prefix="/api/logs", tags=["Observability"])


@router.get("",
    summary="Query logs",
    description="Query application logs with various filters",
    responses={
        200: {
            "description": "Logs retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "logs": [
                            {
                                "timestamp": "2024-01-15T10:00:00",
                                "level": "INFO",
                                "message": "Crawl job started",
                                "job_id": "123e4567",
                                "operation": "crawl_start",
                                "trace_id": "abc123"
                            }
                        ],
                        "total": 100,
                        "offset": 0,
                        "limit": 50
                    }
                }
            }
        }
    }
)
async def get_logs(
    job_id: Optional[str] = Query(None, description="Filter by job ID"),
    trace_id: Optional[str] = Query(None, description="Filter by trace ID"),
    level: Optional[str] = Query(None, description="Filter by log level", enum=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    operation: Optional[str] = Query(None, description="Filter by operation type"),
    hours_ago: Optional[int] = Query(24, description="Logs from last N hours"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of logs"),
    offset: int = Query(0, ge=0, description="Number of logs to skip")
):
    """Query logs with filters"""
    try:
        # Calculate time range
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_ago) if hours_ago else None

        # Query logs
        logs = log_storage.query_logs(
            job_id=job_id,
            trace_id=trace_id,
            level=level,
            operation=operation,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset
        )

        # Clean up logs to ensure JSON serialization works
        cleaned_logs = []
        for log in logs:
            cleaned_log = {}
            for key, value in log.items():
                # Handle float values that might be NaN or Infinity
                if isinstance(value, float):
                    if value != value:  # NaN check
                        cleaned_log[key] = None
                    elif value == float('inf') or value == float('-inf'):
                        cleaned_log[key] = None
                    else:
                        cleaned_log[key] = value
                else:
                    cleaned_log[key] = value
            cleaned_logs.append(cleaned_log)

        return {
            "logs": cleaned_logs,
            "total": len(cleaned_logs),
            "offset": offset,
            "limit": limit,
            "time_range": {
                "start": start_time.isoformat() if start_time else None,
                "end": end_time.isoformat()
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query logs: {str(e)}")


@router.get("/stats",
    summary="Get log statistics",
    description="Get aggregated statistics from logs",
    responses={
        200: {
            "description": "Statistics retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "by_level": [
                            {"level": "INFO", "count": 1000},
                            {"level": "ERROR", "count": 50}
                        ],
                        "by_operation": [
                            {"operation": "crawl_start", "count": 100, "avg_duration": 5000}
                        ],
                        "errors": [
                            {"error": "Connection timeout", "count": 10}
                        ],
                        "total": 1050
                    }
                }
            }
        }
    }
)
async def get_log_stats(
    job_id: Optional[str] = Query(None, description="Get stats for specific job")
):
    """Get log statistics"""
    try:
        stats = log_storage.get_log_stats(job_id=job_id)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@router.get("/job/{job_id}",
    summary="Get logs for a specific job",
    description="Retrieve all logs related to a specific crawl job",
    responses={
        200: {"description": "Job logs retrieved successfully"},
        404: {"description": "Job not found"}
    }
)
async def get_job_logs(
    job_id: str,
    level: Optional[str] = Query(None, description="Filter by log level"),
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of logs")
):
    """Get logs for a specific job"""
    try:
        logs = log_storage.query_logs(
            job_id=job_id,
            level=level,
            limit=limit
        )

        if not logs:
            raise HTTPException(status_code=404, detail="No logs found for this job")

        # Group logs by operation
        operations = {}
        for log in logs:
            op = log.get('operation', 'general')
            if op not in operations:
                operations[op] = []
            operations[op].append(log)

        return {
            "job_id": job_id,
            "total_logs": len(logs),
            "logs": logs,
            "operations": operations,
            "summary": {
                "errors": sum(1 for log in logs if log.get('level') == 'ERROR'),
                "warnings": sum(1 for log in logs if log.get('level') == 'WARNING'),
                "info": sum(1 for log in logs if log.get('level') == 'INFO')
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job logs: {str(e)}")


@router.get("/trace/{trace_id}",
    summary="Get logs for a specific trace",
    description="Retrieve all logs related to a specific trace ID",
    responses={
        200: {"description": "Trace logs retrieved successfully"},
        404: {"description": "Trace not found"}
    }
)
async def get_trace_logs(
    trace_id: str,
    limit: int = Query(500, ge=1, le=5000, description="Maximum number of logs")
):
    """Get logs for a specific trace"""
    try:
        logs = log_storage.query_logs(
            trace_id=trace_id,
            limit=limit
        )

        if not logs:
            raise HTTPException(status_code=404, detail="No logs found for this trace")

        # Sort logs by timestamp
        logs.sort(key=lambda x: x.get('timestamp', ''))

        # Calculate trace duration
        if logs:
            first_log = logs[0]
            last_log = logs[-1]
            if first_log.get('timestamp') and last_log.get('timestamp'):
                duration = (last_log['timestamp'] - first_log['timestamp']).total_seconds()
            else:
                duration = None
        else:
            duration = None

        return {
            "trace_id": trace_id,
            "total_logs": len(logs),
            "duration_seconds": duration,
            "logs": logs,
            "operations": list(set(log.get('operation') for log in logs if log.get('operation')))
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get trace logs: {str(e)}")


@router.get("/errors",
    summary="Get error logs",
    description="Retrieve recent error logs",
    responses={
        200: {"description": "Error logs retrieved successfully"}
    }
)
async def get_error_logs(
    hours_ago: int = Query(24, description="Errors from last N hours"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of errors")
):
    """Get recent error logs"""
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_ago)

        errors = log_storage.query_logs(
            level="ERROR",
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        # Group errors by type
        error_types = {}
        for error in errors:
            error_msg = error.get('error', 'Unknown error')
            if error_msg not in error_types:
                error_types[error_msg] = {
                    "count": 0,
                    "first_seen": error.get('timestamp'),
                    "last_seen": error.get('timestamp'),
                    "examples": []
                }
            error_types[error_msg]["count"] += 1
            error_types[error_msg]["last_seen"] = error.get('timestamp')
            if len(error_types[error_msg]["examples"]) < 3:
                error_types[error_msg]["examples"].append(error)

        return {
            "total_errors": len(errors),
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "error_types": error_types,
            "recent_errors": errors[:20]  # Most recent 20 errors
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get error logs: {str(e)}")


@router.delete("/cleanup",
    summary="Clean up old logs",
    description="Delete logs older than specified days",
    responses={
        200: {"description": "Logs cleaned up successfully"}
    }
)
async def cleanup_logs(
    days_old: int = Query(30, ge=1, le=365, description="Delete logs older than N days")
):
    """Clean up old logs"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)

        with duckdb.connect(str(log_storage.db_path)) as conn:
            result = conn.execute(
                "DELETE FROM logs WHERE timestamp < ? RETURNING COUNT(*)",
                [cutoff_date]
            ).fetchone()

            deleted_count = result[0] if result else 0

        return {
            "message": "Logs cleaned up successfully",
            "deleted_count": deleted_count,
            "cutoff_date": cutoff_date.isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to cleanup logs: {str(e)}")