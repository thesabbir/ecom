"""
Data export and query endpoints
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional, Dict
from datetime import datetime
import json
import io

from ..storage.parquet_storage import ParquetStorage
from ..utils.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api", tags=["Export"])
storage = ParquetStorage()


@router.get(
    "/export",
    summary="Export product data",
    description="Export all product data in CSV or JSON format",
)
async def export_data(
    format: str = Query("csv", description="Export format", enum=["csv", "json"]),
    site: Optional[str] = Query(None, description="Filter by site"),
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """Export data in various formats"""
    try:
        df = storage.load_products()

        if df.empty:
            raise HTTPException(status_code=404, detail="No data to export")

        # Apply filters
        if site:
            df = df[df["site_name"] == site]
        if category:
            df = df[df["category"].str.contains(category, case=False, na=False)]

        if format == "csv":
            # Create CSV in memory
            output = io.StringIO()
            df.to_csv(output, index=False)
            output.seek(0)

            return StreamingResponse(
                io.BytesIO(output.getvalue().encode()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                },
            )
        else:  # JSON
            return JSONResponse(
                content=json.loads(df.to_json(orient="records", date_format="iso"))
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/query",
    summary="Execute SQL query",
    description="Run custom SQL queries on the product database (SELECT only)",
)
async def execute_query(query: Dict[str, str]):
    """Execute SQL query on product data"""
    if "sql" not in query:
        raise HTTPException(status_code=400, detail="SQL query required")

    sql = query["sql"]

    # Basic SQL injection prevention
    if any(
        keyword in sql.upper()
        for keyword in ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"]
    ):
        raise HTTPException(status_code=403, detail="Only SELECT queries allowed")

    try:
        # Use DuckDB to query the parquet file directly
        import duckdb

        conn = duckdb.connect(":memory:")

        # Check if parquet file exists
        import os
        if not os.path.exists(storage.parquet_path):
            raise HTTPException(status_code=404, detail="No data available to query")

        conn.execute(f"CREATE VIEW products AS SELECT * FROM '{storage.parquet_path}'")
        result = conn.execute(sql).fetchall()
        columns = [desc[0] for desc in conn.description]

        # Convert to list of dicts
        result_dicts = [dict(zip(columns, row)) for row in result]
        return {"result": result_dicts, "count": len(result_dicts)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise HTTPException(status_code=400, detail=str(e))