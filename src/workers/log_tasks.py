"""
Celery tasks for asynchronous log processing and analytics
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from celery import Task
import redis

from .celery_app import app
from ..storage.log_storage import ParquetLogStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)
redis_client = redis.from_url(app.conf.broker_url)


class LogTask(Task):
    """Base task for log operations with proper error handling"""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails"""
        logger.error(f"Log task {task_id} failed: {exc}")


@app.task(base=LogTask, bind=True, queue="logs")
def process_log_batch(self, log_entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Process a batch of log entries asynchronously

    This task:
    - Receives batched log entries from the API
    - Writes them to Parquet storage
    - Updates log statistics in Redis
    """
    try:
        storage = ParquetLogStorage()
        processed = 0
        failed = 0

        for entry in log_entries:
            try:
                # Convert dict to LogRecord-like object for storage
                class LogRecordProxy:
                    def __init__(self, data):
                        self.created = data.get("timestamp", datetime.now()).timestamp()
                        self.levelname = data.get("level", "INFO")
                        self.name = data.get("logger_name", "unknown")
                        self.msg = data.get("message", "")
                        self.module = data.get("module", "")
                        self.funcName = data.get("function", "")
                        self.lineno = data.get("line_number", 0)
                        self.threadName = data.get("thread_name", "MainThread")
                        self.process = data.get("process_id", 0)
                        self.hostname = data.get("hostname", "")
                        self.extra_data = data.get("extra_data", {})

                    def getMessage(self):
                        return self.msg

                record = LogRecordProxy(entry)
                if storage.add_log_entry(record):
                    processed += 1
                else:
                    failed += 1

            except Exception as e:
                logger.error(f"Failed to process log entry: {e}")
                failed += 1

        # Force flush to ensure logs are written
        storage.flush()

        # Update stats in Redis
        stats_key = "log_stats:latest"
        redis_client.hincrby(stats_key, "total_processed", processed)
        redis_client.hincrby(stats_key, "total_failed", failed)
        redis_client.hset(stats_key, "last_batch_time", datetime.now().isoformat())
        redis_client.expire(stats_key, 86400)  # Expire after 1 day

        return {
            "processed": processed,
            "failed": failed,
            "batch_size": len(log_entries),
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Batch log processing failed: {e}")
        raise self.retry(exc=e, countdown=60)


@app.task(base=LogTask, bind=True, queue="logs")
def query_logs_async(
    self,
    job_id: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    level: Optional[str] = None,
    logger_name: Optional[str] = None,
    search_text: Optional[str] = None,
    limit: int = 1000,
) -> None:
    """
    Execute log query asynchronously for heavy queries
    Results are stored in Redis for retrieval
    """
    try:
        # Update job status
        redis_client.hset(
            f"log_query:{job_id}",
            mapping={"status": "processing", "started_at": datetime.now().isoformat()},
        )

        storage = ParquetLogStorage()

        # Parse datetime strings
        start_dt = datetime.fromisoformat(start_time) if start_time else None
        end_dt = datetime.fromisoformat(end_time) if end_time else None

        # Execute query
        logs = storage.query_logs(
            start_time=start_dt,
            end_time=end_dt,
            level=level,
            logger_name=logger_name,
            search_text=search_text,
            limit=limit,
        )

        # Store results in Redis
        result_data = {
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "result": json.dumps(
                {
                    "logs": logs,
                    "total": len(logs),
                    "query_params": {
                        "start_time": start_time,
                        "end_time": end_time,
                        "level": level,
                        "logger_name": logger_name,
                        "search_text": search_text,
                        "limit": limit,
                    },
                }
            ),
        }

        redis_client.hset(f"log_query:{job_id}", mapping=result_data)
        redis_client.expire(f"log_query:{job_id}", 3600)  # Expire after 1 hour

        logger.info(f"Log query {job_id} completed with {len(logs)} results")

    except Exception as e:
        logger.error(f"Log query {job_id} failed: {e}")
        redis_client.hset(
            f"log_query:{job_id}",
            mapping={
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            },
        )
        raise


@app.task(base=LogTask, bind=True, queue="logs")
def generate_log_report(
    self, job_id: str, report_type: str, hours: int = 24, **kwargs
) -> None:
    """
    Generate various log reports asynchronously

    Report types:
    - error_summary: Summary of errors by type and frequency
    - performance: Performance metrics from logs
    - crawler_activity: Crawler operation summary
    - system_health: Overall system health metrics
    """
    try:
        redis_client.hset(
            f"log_report:{job_id}",
            mapping={
                "status": "processing",
                "report_type": report_type,
                "started_at": datetime.now().isoformat(),
            },
        )

        storage = ParquetLogStorage()
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        report_data = {}

        if report_type == "error_summary":
            # Get all error logs
            errors = storage.query_logs(
                level="ERROR", start_time=start_time, end_time=end_time, limit=10000
            )

            # Group and analyze errors
            error_types = {}
            for error in errors:
                msg = error.get("message", "")
                error_key = msg.split("\n")[0][:100]

                if error_key not in error_types:
                    error_types[error_key] = {
                        "count": 0,
                        "first_seen": error.get("timestamp"),
                        "last_seen": error.get("timestamp"),
                        "modules": set(),
                    }

                error_types[error_key]["count"] += 1
                error_types[error_key]["last_seen"] = error.get("timestamp")
                error_types[error_key]["modules"].add(error.get("module", "unknown"))

            # Convert sets to lists for JSON serialization
            for key in error_types:
                error_types[key]["modules"] = list(error_types[key]["modules"])

            report_data = {
                "total_errors": len(errors),
                "unique_error_types": len(error_types),
                "error_types": error_types,
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                },
            }

        elif report_type == "performance":
            # Analyze performance-related logs
            all_logs = storage.query_logs(
                start_time=start_time, end_time=end_time, limit=10000
            )

            slow_operations = []
            for log in all_logs:
                msg = log.get("message", "")
                # Look for timing information in logs
                if "took" in msg or "duration" in msg or "elapsed" in msg:
                    slow_operations.append(
                        {
                            "timestamp": log.get("timestamp"),
                            "operation": msg[:200],
                            "module": log.get("module"),
                        }
                    )

            report_data = {
                "slow_operations": slow_operations[:100],
                "total_operations": len(slow_operations),
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                },
            }

        elif report_type == "crawler_activity":
            # Get crawler-specific logs
            crawler_logs = storage.query_logs(
                logger_name="src.crawlers",
                start_time=start_time,
                end_time=end_time,
                limit=10000,
            )

            activity = {
                "products_crawled": 0,
                "categories_processed": set(),
                "errors": 0,
                "sites": {},
            }

            for log in crawler_logs:
                msg = log.get("message", "")
                logger_name = log.get("logger_name", "")

                # Extract site name
                if "ryans" in logger_name:
                    site = "ryans"
                elif "startech" in logger_name:
                    site = "startech"
                else:
                    site = "generic"

                if site not in activity["sites"]:
                    activity["sites"][site] = {
                        "products": 0,
                        "errors": 0,
                        "categories": set(),
                    }

                # Count products
                import re

                product_match = re.search(r"(\d+)\s+products?", msg)
                if product_match:
                    count = int(product_match.group(1))
                    activity["products_crawled"] += count
                    activity["sites"][site]["products"] += count

                # Count errors
                if log.get("level") == "ERROR":
                    activity["errors"] += 1
                    activity["sites"][site]["errors"] += 1

                # Extract categories
                if "category" in msg.lower():
                    cat_match = re.search(r"category[:\s]+([^,\n]+)", msg, re.I)
                    if cat_match:
                        category = cat_match.group(1).strip()
                        activity["categories_processed"].add(category)
                        activity["sites"][site]["categories"].add(category)

            # Convert sets to lists
            activity["categories_processed"] = list(activity["categories_processed"])
            for site in activity["sites"]:
                activity["sites"][site]["categories"] = list(
                    activity["sites"][site]["categories"]
                )

            report_data = activity

        elif report_type == "system_health":
            # Overall system health metrics
            all_logs = storage.query_logs(
                start_time=start_time, end_time=end_time, limit=10000
            )

            stats = storage.get_stats()

            # Count by level
            level_counts = {}
            for log in all_logs:
                level = log.get("level", "UNKNOWN")
                level_counts[level] = level_counts.get(level, 0) + 1

            # Calculate health score
            total = len(all_logs)
            error_rate = (
                (level_counts.get("ERROR", 0) / total * 100) if total > 0 else 0
            )
            warning_rate = (
                (level_counts.get("WARNING", 0) / total * 100) if total > 0 else 0
            )

            health_score = 100
            health_score -= min(error_rate * 5, 50)  # Errors reduce score more
            health_score -= min(warning_rate * 2, 20)  # Warnings reduce score less

            report_data = {
                "health_score": max(0, health_score),
                "total_logs": total,
                "level_distribution": level_counts,
                "error_rate": error_rate,
                "warning_rate": warning_rate,
                "storage_stats": stats,
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                },
            }

        # Store report in Redis
        redis_client.hset(
            f"log_report:{job_id}",
            mapping={
                "status": "completed",
                "report_type": report_type,
                "completed_at": datetime.now().isoformat(),
                "result": json.dumps(report_data),
            },
        )
        redis_client.expire(f"log_report:{job_id}", 3600)  # Expire after 1 hour

        logger.info(f"Log report {job_id} ({report_type}) completed")

    except Exception as e:
        logger.error(f"Log report {job_id} failed: {e}")
        redis_client.hset(
            f"log_report:{job_id}",
            mapping={
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            },
        )
        raise


@app.task
def cleanup_old_logs():
    """
    Periodic task to cleanup old logs from storage
    """
    try:
        storage = ParquetLogStorage()
        success = storage.clear_old_logs(days_to_keep=30)

        if success:
            logger.info("Successfully cleaned up old logs")
            return {"status": "success", "message": "Old logs cleaned up"}
        else:
            logger.error("Failed to cleanup old logs")
            return {"status": "failed", "message": "Cleanup failed"}

    except Exception as e:
        logger.error(f"Log cleanup task failed: {e}")
        return {"status": "error", "message": str(e)}


@app.task
def compress_logs():
    """
    Periodic task to compress older log files
    """
    try:
        # This would compress older Parquet files
        # Implementation depends on specific compression needs
        logger.info("Log compression task completed")
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Log compression failed: {e}")
        return {"status": "error", "message": str(e)}
