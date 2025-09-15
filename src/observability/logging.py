import structlog
import logging
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from pythonjsonlogger import jsonlogger
import duckdb
from contextlib import contextmanager


class LogStorage:
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.db_path = self.log_dir / "logs.duckdb"
        self.json_log_file = self.log_dir / "app.json"
        self._init_db()

    def _init_db(self):
        """Initialize DuckDB table for logs"""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id VARCHAR PRIMARY KEY,
                    timestamp TIMESTAMP,
                    level VARCHAR,
                    logger VARCHAR,
                    message VARCHAR,
                    trace_id VARCHAR,
                    span_id VARCHAR,
                    job_id VARCHAR,
                    url VARCHAR,
                    site_name VARCHAR,
                    operation VARCHAR,
                    duration_ms DOUBLE,
                    status VARCHAR,
                    error VARCHAR,
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_logs_job_id ON logs(job_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id)
            """)

    def store_log(self, log_entry: Dict[str, Any]):
        """Store a single log entry"""
        import uuid

        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                INSERT INTO logs (
                    id, timestamp, level, logger, message,
                    trace_id, span_id, job_id, url, site_name,
                    operation, duration_ms, status, error, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    str(uuid.uuid4()),
                    log_entry.get("timestamp", datetime.now()),
                    log_entry.get("level", "INFO"),
                    log_entry.get("logger", ""),
                    log_entry.get("message", ""),
                    log_entry.get("trace_id"),
                    log_entry.get("span_id"),
                    log_entry.get("job_id"),
                    log_entry.get("url"),
                    log_entry.get("site_name"),
                    log_entry.get("operation"),
                    log_entry.get("duration_ms"),
                    log_entry.get("status"),
                    log_entry.get("error"),
                    json.dumps(log_entry.get("metadata", {})),
                ],
            )

    def query_logs(
        self,
        job_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        level: Optional[str] = None,
        operation: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query logs with filters"""
        with duckdb.connect(str(self.db_path)) as conn:
            conditions = []
            params = []

            if job_id:
                conditions.append("job_id = ?")
                params.append(job_id)

            if trace_id:
                conditions.append("trace_id = ?")
                params.append(trace_id)

            if level:
                conditions.append("level = ?")
                params.append(level)

            if operation:
                conditions.append("operation = ?")
                params.append(operation)

            if start_time:
                conditions.append("timestamp >= ?")
                params.append(start_time)

            if end_time:
                conditions.append("timestamp <= ?")
                params.append(end_time)

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            query = f"""
                SELECT * FROM logs
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT {limit} OFFSET {offset}
            """

            result = conn.execute(query, params).fetchdf()

            if result.empty:
                return []

            # Clean up the results to handle NaN/Infinity values
            import numpy as np

            result = result.replace([np.inf, -np.inf], np.nan).fillna(value=np.nan)
            records = result.to_dict("records")

            # Convert NaN to None for JSON serialization
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    if isinstance(value, float) and (
                        np.isnan(value) or np.isinf(value)
                    ):
                        cleaned_record[key] = None
                    else:
                        cleaned_record[key] = value
                cleaned_records.append(cleaned_record)

            return cleaned_records

    def get_log_stats(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Get log statistics"""
        with duckdb.connect(str(self.db_path)) as conn:
            if job_id:
                base_query = f"FROM logs WHERE job_id = '{job_id}'"
            else:
                base_query = "FROM logs"

            stats = {}

            # Count by level
            stats["by_level"] = (
                conn.execute(f"""
                SELECT level, COUNT(*) as count
                {base_query}
                GROUP BY level
            """)
                .fetchdf()
                .to_dict("records")
            )

            # Count by operation
            stats["by_operation"] = (
                conn.execute(f"""
                SELECT operation, COUNT(*) as count, AVG(duration_ms) as avg_duration
                {base_query}
                WHERE operation IS NOT NULL
                GROUP BY operation
                ORDER BY count DESC
                LIMIT 10
            """)
                .fetchdf()
                .to_dict("records")
            )

            # Error summary
            stats["errors"] = (
                conn.execute(f"""
                SELECT error, COUNT(*) as count
                {base_query}
                WHERE error IS NOT NULL
                GROUP BY error
                ORDER BY count DESC
                LIMIT 10
            """)
                .fetchdf()
                .to_dict("records")
            )

            # Total logs
            stats["total"] = conn.execute(f"SELECT COUNT(*) {base_query}").fetchone()[0]

            return stats


log_storage = LogStorage()


class DatabaseLogHandler(logging.Handler):
    """Custom handler to store logs in database"""

    def emit(self, record):
        try:
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created),
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
                "trace_id": getattr(record, "trace_id", None),
                "span_id": getattr(record, "span_id", None),
                "job_id": getattr(record, "job_id", None),
                "url": getattr(record, "url", None),
                "site_name": getattr(record, "site_name", None),
                "operation": getattr(record, "operation", None),
                "duration_ms": getattr(record, "duration_ms", None),
                "status": getattr(record, "status", None),
                "error": getattr(record, "error", None),
                "metadata": getattr(record, "metadata", {}),
            }
            log_storage.store_log(log_entry)
        except Exception:
            pass  # Avoid infinite loop if logging fails


def setup_logging():
    """Configure structured logging with multiple outputs"""

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)

    # JSON file handler
    json_handler = logging.FileHandler(log_storage.json_log_file)
    json_formatter = jsonlogger.JsonFormatter(
        "%(timestamp)s %(level)s %(name)s %(message)s",
        rename_fields={"timestamp": "@timestamp", "level": "level"},
    )
    json_handler.setFormatter(json_formatter)

    # Database handler
    db_handler = DatabaseLogHandler()
    db_handler.setLevel(logging.INFO)

    # Add handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(json_handler)
    root_logger.addHandler(db_handler)

    return structlog.get_logger()


@contextmanager
def log_operation(operation: str, **context):
    """Context manager for logging operations with timing"""
    logger = structlog.get_logger()
    start_time = datetime.now()

    logger.info(f"Starting {operation}", operation=operation, **context)

    try:
        yield logger
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        logger.info(
            f"Completed {operation}",
            operation=operation,
            duration_ms=duration_ms,
            status="success",
            **context,
        )
    except Exception as e:
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        logger.error(
            f"Failed {operation}",
            operation=operation,
            duration_ms=duration_ms,
            status="failed",
            error=str(e),
            **context,
        )
        raise


class CrawlLogger:
    """Specialized logger for crawl operations"""

    def __init__(self, job_id: str, trace_id: str = None):
        self.job_id = job_id
        self.trace_id = trace_id or str(uuid.uuid4())
        self.logger = structlog.get_logger().bind(job_id=job_id, trace_id=trace_id)

    def log_crawl_start(self, url: str, site_name: str, max_pages: int):
        self.logger.info(
            "Crawl job started",
            operation="crawl_start",
            url=url,
            site_name=site_name,
            max_pages=max_pages,
        )

    def log_page_fetch(
        self, url: str, status_code: int = None, duration_ms: float = None
    ):
        self.logger.info(
            "Fetched page",
            operation="page_fetch",
            url=url,
            status_code=status_code,
            duration_ms=duration_ms,
        )

    def log_product_extracted(self, product_id: str, title: str, price: float):
        self.logger.info(
            "Product extracted",
            operation="product_extract",
            product_id=product_id,
            title=title[:100],
            price=price,
        )

    def log_parse_error(self, url: str, error: str):
        self.logger.error(
            "Failed to parse page", operation="parse_error", url=url, error=error
        )

    def log_crawl_complete(self, products_found: int, duration_ms: float):
        self.logger.info(
            "Crawl job completed",
            operation="crawl_complete",
            products_found=products_found,
            duration_ms=duration_ms,
            status="completed",
        )

    def log_crawl_failed(self, error: str, duration_ms: float = None):
        self.logger.error(
            "Crawl job failed",
            operation="crawl_failed",
            error=error,
            duration_ms=duration_ms,
            status="failed",
        )
