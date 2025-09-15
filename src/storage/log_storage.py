import os
import json
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import logging
from threading import Lock
import atexit

logger = logging.getLogger(__name__)


class ParquetLogStorage:
    """Storage for logs using Parquet format with DuckDB for efficient querying"""

    def __init__(self, base_path: str = "logs"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.parquet_path = os.path.join(base_path, "application_logs.parquet")
        self.db_path = os.path.join(base_path, "logs.duckdb")
        self.buffer = []
        self.buffer_size = 100  # Flush after 100 log entries
        self.lock = Lock()
        self._init_storage()
        # Register cleanup on exit
        atexit.register(self.flush)

    def _init_storage(self):
        """Initialize storage with Parquet file"""
        if not os.path.exists(self.parquet_path):
            # Create empty Parquet file with schema
            df = pd.DataFrame(
                columns=[
                    "timestamp",
                    "level",
                    "logger_name",
                    "message",
                    "module",
                    "function",
                    "line_number",
                    "thread_name",
                    "process_id",
                    "hostname",
                    "extra_data",
                ]
            )
            df.to_parquet(self.parquet_path, index=False)
            logger.info(f"Created new log Parquet file: {self.parquet_path}")

    def add_log_entry(self, record: logging.LogRecord) -> bool:
        """Add a log entry to the buffer and flush if needed"""
        try:
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created),
                "level": record.levelname,
                "logger_name": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line_number": record.lineno,
                "thread_name": record.threadName,
                "process_id": record.process,
                "hostname": getattr(record, "hostname", None),
                "extra_data": json.dumps(getattr(record, "extra_data", {})),
            }

            with self.lock:
                self.buffer.append(log_entry)
                if len(self.buffer) >= self.buffer_size:
                    self.flush()

            return True
        except Exception as e:
            print(f"Error adding log entry: {e}")
            return False

    def flush(self):
        """Flush buffered logs to Parquet file"""
        if not self.buffer:
            return

        try:
            with self.lock:
                if not self.buffer:
                    return

                # Convert buffer to DataFrame
                df_new = pd.DataFrame(self.buffer)

                # Read existing data and append
                if (
                    os.path.exists(self.parquet_path)
                    and os.path.getsize(self.parquet_path) > 0
                ):
                    df_existing = pd.read_parquet(self.parquet_path)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_combined = df_new

                # Write back to Parquet
                df_combined.to_parquet(self.parquet_path, index=False)
                self.buffer.clear()

        except Exception as e:
            print(f"Error flushing logs to Parquet: {e}")

    def query_logs(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        level: Optional[str] = None,
        logger_name: Optional[str] = None,
        search_text: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """Query logs with various filters"""
        try:
            # Flush any pending logs first
            self.flush()

            with duckdb.connect(self.db_path) as conn:
                query = f"""
                    SELECT * FROM read_parquet('{self.parquet_path}')
                    WHERE 1=1
                """

                if start_time:
                    query += f" AND timestamp >= '{start_time.isoformat()}'"
                if end_time:
                    query += f" AND timestamp <= '{end_time.isoformat()}'"
                if level:
                    query += f" AND level = '{level}'"
                if logger_name:
                    query += f" AND logger_name LIKE '%{logger_name}%'"
                if search_text:
                    query += f" AND message LIKE '%{search_text}%'"

                query += f" ORDER BY timestamp DESC LIMIT {limit}"

                result = conn.execute(query).fetchall()
                columns = [desc[0] for desc in conn.description]

                logs = []
                for row in result:
                    log_dict = dict(zip(columns, row))
                    # Parse extra_data back to dict
                    if log_dict.get("extra_data"):
                        try:
                            log_dict["extra_data"] = json.loads(log_dict["extra_data"])
                        except:
                            pass
                    logs.append(log_dict)

                return logs

        except Exception as e:
            logger.error(f"Error querying logs: {e}")
            return []

    def get_stats(self) -> Dict:
        """Get log statistics"""
        try:
            # Flush any pending logs first
            self.flush()

            with duckdb.connect(self.db_path) as conn:
                stats = conn.execute(f"""
                    SELECT
                        COUNT(*) as total_logs,
                        COUNT(DISTINCT level) as log_levels,
                        COUNT(DISTINCT logger_name) as unique_loggers,
                        MIN(timestamp) as oldest_log,
                        MAX(timestamp) as newest_log
                    FROM read_parquet('{self.parquet_path}')
                """).fetchone()

                level_counts = conn.execute(f"""
                    SELECT level, COUNT(*) as count
                    FROM read_parquet('{self.parquet_path}')
                    GROUP BY level
                    ORDER BY count DESC
                """).fetchall()

                return {
                    "total_logs": int(stats[0]) if stats[0] else 0,
                    "log_levels": int(stats[1]) if stats[1] else 0,
                    "unique_loggers": int(stats[2]) if stats[2] else 0,
                    "oldest_log": stats[3],
                    "newest_log": stats[4],
                    "level_distribution": {row[0]: row[1] for row in level_counts},
                }

        except Exception as e:
            logger.error(f"Error getting log stats: {e}")
            return {}

    def clear_old_logs(self, days_to_keep: int = 30):
        """Remove logs older than specified days"""
        try:
            cutoff = datetime.now() - timedelta(days=days_to_keep)

            with duckdb.connect(self.db_path) as conn:
                # Create new Parquet file with filtered data
                conn.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{self.parquet_path}')
                        WHERE timestamp >= '{cutoff.isoformat()}'
                    ) TO '{self.parquet_path}.tmp' (FORMAT PARQUET)
                """)

            # Replace old file with new
            os.replace(f"{self.parquet_path}.tmp", self.parquet_path)
            logger.info(f"Removed logs older than {cutoff}")
            return True

        except Exception as e:
            logger.error(f"Error clearing old logs: {e}")
            return False


class ParquetLogHandler(logging.Handler):
    """Custom logging handler that writes to Parquet storage"""

    def __init__(self, storage: ParquetLogStorage):
        super().__init__()
        self.storage = storage

    def emit(self, record):
        """Emit a log record to Parquet storage"""
        try:
            self.storage.add_log_entry(record)
        except Exception:
            self.handleError(record)
