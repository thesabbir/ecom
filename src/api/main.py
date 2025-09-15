from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import asyncio
from datetime import datetime, timedelta
import uuid
import logging
from pathlib import Path
import pandas as pd
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from starlette.responses import Response

from ..models.product import CrawlRequest, CrawlResponse, Product, ProductList
from ..crawlers.base import GenericCrawler
from ..crawlers.ryans import RyansCrawler
from ..storage.csv_storage import CSVStorage
from ..storage.incremental_storage import IncrementalCSVStorage
from ..observability.logging import setup_logging, CrawlLogger, log_operation
from ..observability.tracing import setup_tracing, instrument_app, CrawlTracer

# Setup observability
logger = setup_logging()
tracer = setup_tracing()

# Prometheus metrics
crawl_jobs_total = Counter('crawl_jobs_total', 'Total number of crawl jobs', ['status'])
crawl_duration_seconds = Histogram('crawl_duration_seconds', 'Crawl job duration in seconds')
products_crawled_total = Counter('products_crawled_total', 'Total number of products crawled', ['site'])
active_crawl_jobs = Gauge('active_crawl_jobs', 'Number of active crawl jobs')
http_requests_total = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])

app = FastAPI(
    title="E-commerce Crawler API",
    description="""
    ## 🛍️ Unified E-commerce Data Platform

    A powerful API for crawling, analyzing, and gaining insights from e-commerce websites.

    ### Key Features:
    - **Multi-Site Support**: Ryans and generic e-commerce sites
    - **Unified Data Model**: Standardized product schema across platforms
    - **Real-time Crawling**: Async background job processing
    - **Advanced Analytics**: Price trends, competitive analysis, demand indicators
    - **SQL Query Engine**: Custom queries with DuckDB
    - **CSV/JSON Export**: Flexible data export options

    ### Getting Started:
    1. Start a crawl job using `/api/crawl`
    2. Monitor job status with `/api/jobs/{job_id}`
    3. Search products using `/api/search`
    4. Get market insights with `/api/insights`
    5. Run analytics queries through `/api/analytics/*`

    ### Authentication:
    Currently open access - API keys coming soon
    """,
    version="1.0.0",
    contact={
        "name": "API Support",
        "email": "support@example.com"
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    },
    servers=[
        {"url": "http://localhost:8000", "description": "Local server"},
        {"url": "https://api.example.com", "description": "Production server"}
    ],
    tags_metadata=[
        {"name": "Crawling", "description": "Start and manage crawl jobs"},
        {"name": "Products", "description": "Access and search product data"},
        {"name": "Analytics", "description": "Market analysis and insights"},
        {"name": "Export", "description": "Export data in various formats"},
    ]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instrument app for tracing
instrument_app(app)

storage = CSVStorage()
crawl_jobs: Dict[str, Dict[str, Any]] = {}


@app.middleware("http")
async def track_requests(request: Request, call_next):
    """Middleware to track HTTP requests and add tracing"""
    start_time = time.time()
    trace_id = str(uuid.uuid4())

    # Add trace ID to request state
    request.state.trace_id = trace_id

    # Log request
    logger.info(
        "HTTP request received",
        method=request.method,
        url=str(request.url),
        trace_id=trace_id
    )

    try:
        response = await call_next(request)
        duration = time.time() - start_time

        # Track metrics
        http_requests_total.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()

        # Log response
        logger.info(
            "HTTP request completed",
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
            duration_ms=duration * 1000,
            trace_id=trace_id
        )

        # Add trace ID to response headers
        response.headers["X-Trace-Id"] = trace_id
        return response

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "HTTP request failed",
            method=request.method,
            url=str(request.url),
            error=str(e),
            duration_ms=duration * 1000,
            trace_id=trace_id
        )
        raise


def get_crawler(url: str, site_adapter: Optional[str] = None):
    if site_adapter == "ryans" or "ryans.com" in url:
        return RyansCrawler()
    else:
        return GenericCrawler(url)


async def run_crawl_job(job_id: str, request: CrawlRequest):
    """Run a crawl job with full observability"""
    start_time = datetime.now()
    crawl_logger = CrawlLogger(job_id)
    crawl_tracer = CrawlTracer(job_id)

    crawl_jobs[job_id]["status"] = "running"
    crawl_jobs[job_id]["started_at"] = start_time
    active_crawl_jobs.inc()

    site_name = request.site_adapter or "generic"

    try:
        with crawl_tracer.trace_crawl_job(str(request.url), site_name):
            # Log crawl start
            crawl_logger.log_crawl_start(
                url=str(request.url),
                site_name=site_name,
                max_pages=request.max_pages
            )

            async with get_crawler(str(request.url), request.site_adapter) as crawler:
                # Fetch products with detailed logging
                products = []

                # Special handling for Ryans categories page
                if "ryans.com/categories" in str(request.url) and isinstance(crawler, RyansCrawler):
                    with log_operation("crawl_all_categories", job_id=job_id):
                        products = await crawler.crawl_all_categories(
                            max_pages_per_category=request.max_pages,
                            skip_duplicates=request.skip_duplicates,
                            skip_if_scraped_within_hours=request.skip_if_scraped_within_hours,
                            overwrite=request.overwrite
                        )
                        logger.info(f"Crawled all categories, found {len(products)} products", job_id=job_id)
                # Use optimized crawl_category_parallel for Ryans category pages
                elif "ryans.com/category/" in str(request.url) and isinstance(crawler, RyansCrawler):
                    with log_operation("crawl_category", job_id=job_id, url=str(request.url)):
                        products = await crawler.crawl_category_parallel(
                            str(request.url),
                            max_pages=request.max_pages,
                            skip_duplicates=request.skip_duplicates,
                            skip_if_scraped_within_hours=request.skip_if_scraped_within_hours
                        )
                        logger.info(f"Crawled category, found {len(products)} products", job_id=job_id)
                else:
                    with log_operation("fetch_product_urls", job_id=job_id, url=str(request.url)):
                        product_urls = await crawler.extract_product_urls(
                            str(request.url),
                            max_pages=request.max_pages
                        )
                        logger.info(f"Found {len(product_urls)} product URLs", job_id=job_id)

                    for url in product_urls:
                        try:
                            with crawl_tracer.trace_product_extraction(url):
                                fetch_start = time.time()
                                product = await crawler.extract_product_data(url)
                                fetch_duration = (time.time() - fetch_start) * 1000

                                if product:
                                    products.append(product)
                                    crawl_logger.log_product_extracted(
                                        product_id=product.product_id,
                                        title=product.title,
                                        price=product.price
                                    )
                                    crawl_logger.log_page_fetch(
                                        url=url,
                                        status_code=200,
                                        duration_ms=fetch_duration
                                    )
                        except Exception as e:
                            crawl_logger.log_parse_error(url=url, error=str(e))
                            logger.error(f"Failed to extract product from {url}", error=str(e), job_id=job_id)

                # Apply filters
                if request.category_filter:
                    products = [
                        p for p in products
                        if p.category and request.category_filter.lower() in p.category.lower()
                    ]

                if request.price_min is not None:
                    products = [p for p in products if p.price >= request.price_min]

                if request.price_max is not None:
                    products = [p for p in products if p.price <= request.price_max]

                # Save products
                with log_operation("save_products", job_id=job_id, count=len(products)):
                    storage.save_products(products)

                # Update metrics
                products_crawled_total.labels(site=site_name).inc(len(products))

                # Calculate duration
                duration = (datetime.now() - start_time).total_seconds()
                crawl_duration_seconds.observe(duration)

                # Update job status
                crawl_jobs[job_id]["status"] = "completed"
                crawl_jobs[job_id]["products_found"] = len(products)
                crawl_jobs[job_id]["completed_at"] = datetime.now()
                crawl_jobs[job_id]["message"] = f"Successfully crawled {len(products)} products"

                # Log completion
                crawl_logger.log_crawl_complete(
                    products_found=len(products),
                    duration_ms=duration * 1000
                )

                crawl_jobs_total.labels(status="completed").inc()

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()

        # Log failure
        crawl_logger.log_crawl_failed(error=str(e), duration_ms=duration * 1000)
        logger.error(f"Crawl job {job_id} failed", error=str(e), job_id=job_id)

        # Update job status
        crawl_jobs[job_id]["status"] = "failed"
        crawl_jobs[job_id]["message"] = str(e)
        crawl_jobs[job_id]["completed_at"] = datetime.now()

        crawl_jobs_total.labels(status="failed").inc()

    finally:
        active_crawl_jobs.dec()


@app.get("/")
async def root():
    return {
        "message": "E-commerce Crawler API",
        "endpoints": {
            "crawl": "/api/crawl",
            "products": "/api/products",
            "search": "/api/search",
            "insights": "/api/insights",
            "jobs": "/api/jobs/{job_id}",
            "export": "/api/export",
            "logs": "/api/logs",
            "storage": "/api/storage/stats",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


@app.get("/metrics",
    tags=["Observability"],
    summary="Prometheus metrics",
    description="Get Prometheus metrics for monitoring"
)
async def get_metrics():
    """Return Prometheus metrics"""
    return Response(generate_latest(), media_type="text/plain")


@app.post("/api/crawl",
    response_model=CrawlResponse,
    tags=["Crawling"],
    summary="Start a new crawl job",
    description="Initiates an asynchronous crawl job for the specified URL with optional filters",
    responses={
        200: {
            "description": "Crawl job started successfully",
            "content": {
                "application/json": {
                    "example": {
                        "job_id": "123e4567-e89b-12d3-a456-426614174000",
                        "status": "pending",
                        "products_found": 0,
                        "message": "Crawl job created",
                        "started_at": "2024-01-15T10:00:00",
                        "completed_at": None
                    }
                }
            }
        }
    }
)
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())

    crawl_jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "products_found": 0,
        "message": "Crawl job created",
        "started_at": datetime.now(),
        "completed_at": None
    }

    background_tasks.add_task(run_crawl_job, job_id, request)

    return CrawlResponse(**crawl_jobs[job_id])


@app.get("/api/jobs",
    tags=["Crawling"],
    summary="List all crawl jobs",
    description="Get a list of all crawl jobs with optional status filtering",
    responses={
        200: {
            "description": "List of crawl jobs",
            "content": {
                "application/json": {
                    "example": {
                        "jobs": [
                            {
                                "job_id": "123e4567-e89b-12d3-a456-426614174000",
                                "status": "completed",
                                "products_found": 50,
                                "message": "Successfully crawled 50 products",
                                "started_at": "2024-01-15T10:00:00",
                                "completed_at": "2024-01-15T10:05:00"
                            }
                        ],
                        "total": 1,
                        "stats": {
                            "pending": 0,
                            "running": 0,
                            "completed": 1,
                            "failed": 0
                        }
                    }
                }
            }
        }
    }
)
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status", enum=["pending", "running", "completed", "failed"]),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of jobs to return"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip"),
    sort: str = Query("desc", description="Sort order by start time", enum=["asc", "desc"])
):
    # Filter jobs by status if provided
    filtered_jobs = crawl_jobs.values()
    if status:
        filtered_jobs = [job for job in filtered_jobs if job.get("status") == status]

    # Sort jobs by started_at
    sorted_jobs = sorted(
        filtered_jobs,
        key=lambda x: x.get("started_at", datetime.min),
        reverse=(sort == "desc")
    )

    # Apply pagination
    paginated_jobs = sorted_jobs[offset:offset + limit]

    # Calculate statistics
    stats = {
        "pending": sum(1 for job in crawl_jobs.values() if job.get("status") == "pending"),
        "running": sum(1 for job in crawl_jobs.values() if job.get("status") == "running"),
        "completed": sum(1 for job in crawl_jobs.values() if job.get("status") == "completed"),
        "failed": sum(1 for job in crawl_jobs.values() if job.get("status") == "failed"),
    }

    return {
        "jobs": paginated_jobs,
        "total": len(filtered_jobs),
        "offset": offset,
        "limit": limit,
        "stats": stats
    }


@app.get("/api/jobs/{job_id}",
    tags=["Crawling"],
    summary="Get crawl job status",
    description="Check the status and results of a specific crawl job",
    responses={
        200: {"description": "Job status retrieved"},
        404: {"description": "Job not found"}
    }
)
async def get_job_status(job_id: str):
    if job_id not in crawl_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return crawl_jobs[job_id]


@app.delete("/api/jobs/{job_id}",
    tags=["Crawling"],
    summary="Cancel a crawl job",
    description="Cancel a pending or running crawl job",
    responses={
        200: {"description": "Job cancelled successfully"},
        404: {"description": "Job not found"},
        400: {"description": "Job cannot be cancelled"}
    }
)
async def cancel_job(job_id: str):
    if job_id not in crawl_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = crawl_jobs[job_id]
    if job["status"] in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job with status: {job['status']}")

    # Mark job as cancelled
    job["status"] = "cancelled"
    job["completed_at"] = datetime.now()
    job["message"] = "Job cancelled by user"

    return {"message": "Job cancelled successfully", "job_id": job_id}


@app.get("/api/products",
    response_model=ProductList,
    tags=["Products"],
    summary="List all products",
    description="Retrieve paginated list of all crawled products"
)
async def get_products(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Items per page")
):
    df = storage.load_products()

    if df.empty:
        return ProductList(products=[], total_count=0, page=page, page_size=page_size)

    total_count = len(df)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    page_df = df.iloc[start_idx:end_idx]

    products = []
    for _, row in page_df.iterrows():
        product_dict = row.to_dict()
        product_dict = {k: v for k, v in product_dict.items() if v is not None and (not isinstance(v, float) or not pd.isna(v))}
        products.append(Product(**product_dict))

    return ProductList(
        products=products,
        total_count=total_count,
        page=page,
        page_size=page_size,
        has_next=end_idx < total_count
    )


@app.get("/api/search",
    tags=["Products"],
    summary="Search products",
    description="Search products with multiple filter options",
    responses={
        200: {
            "description": "Search results",
            "content": {
                "application/json": {
                    "example": {
                        "results": [
                            {
                                "product_id": "ryans_12345",
                                "title": "Laptop Computer",
                                "price": 899.99,
                                "rating": 4.5,
                                "site_name": "Ryans"
                            }
                        ],
                        "count": 1
                    }
                }
            }
        }
    }
)
async def search_products(
    q: Optional[str] = Query(None, description="Search query for title, description, or brand"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    category: Optional[str] = Query(None, description="Category filter"),
    site: Optional[str] = Query(None, description="Site name filter (e.g., 'Ryans')"),
    in_stock: bool = Query(False, description="Filter for in-stock items only"),
    min_rating: Optional[float] = Query(None, description="Minimum rating filter (0-5)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    df = storage.search_products(
        query=q,
        min_price=min_price,
        max_price=max_price,
        category=category,
        site_name=site,
        in_stock_only=in_stock,
        min_rating=min_rating,
        limit=limit
    )

    if df.empty:
        return {"results": [], "count": 0}

    results = df.to_dict('records')
    return {"results": results, "count": len(results)}


@app.get("/api/insights",
    tags=["Analytics"],
    summary="Get market insights",
    description="Comprehensive market analysis including price distribution, top products, and category performance"
)
async def get_insights():
    insights = storage.get_insights()
    return insights


@app.post("/api/query",
    tags=["Analytics"],
    summary="Execute SQL query",
    description="Run custom SQL queries on the product database (SELECT only)",
    responses={
        200: {"description": "Query executed successfully"},
        400: {"description": "Invalid query or query error"}
    }
)
async def execute_query(query: Dict[str, str]):
    if "sql" not in query:
        raise HTTPException(status_code=400, detail="SQL query required")

    sql = query["sql"]

    if any(keyword in sql.upper() for keyword in ["DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER"]):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")

    try:
        result = storage.query_products(sql)
        return {
            "results": result.to_dict('records'),
            "count": len(result),
            "columns": result.columns.tolist()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")


@app.get("/api/export",
    tags=["Export"],
    summary="Export product data",
    description="Export all product data in CSV or JSON format",
    responses={
        200: {"description": "Data exported successfully"},
        404: {"description": "No data to export"}
    }
)
async def export_data(format: str = Query("csv", description="Export format", enum=["csv", "json"])):
    df = storage.load_products()

    if df.empty:
        raise HTTPException(status_code=404, detail="No data to export")

    if format == "json":
        return JSONResponse(content=df.to_dict('records'))
    else:
        csv_data = df.to_csv(index=False)
        return JSONResponse(
            content={"csv": csv_data},
            headers={"Content-Disposition": "attachment; filename=products.csv"}
        )


@app.get("/api/stats",
    tags=["Analytics"],
    summary="Get statistics",
    description="Basic statistics about the crawled data"
)
async def get_statistics():
    df = storage.load_products()

    if df.empty:
        return {"message": "No data available"}

    stats = {
        "total_products": len(df),
        "unique_sites": df['site_name'].nunique() if 'site_name' in df else 0,
        "avg_price": df['price'].mean() if 'price' in df else 0,
        "price_range": {
            "min": df['price'].min() if 'price' in df else 0,
            "max": df['price'].max() if 'price' in df else 0
        },
        "last_updated": df['scraped_at'].max() if 'scraped_at' in df else None,
        "categories": df['category'].value_counts().head(10).to_dict() if 'category' in df else {},
        "brands": df['brand'].value_counts().head(10).to_dict() if 'brand' in df else {}
    }

    return stats


@app.get("/api/storage/stats",
    tags=["Storage"],
    summary="Get storage statistics",
    description="Get statistics about stored data including duplicates and recent scrapes"
)
async def get_storage_stats():
    """Get detailed storage statistics"""
    from ..storage.incremental_storage import IncrementalCSVStorage

    incremental_storage = IncrementalCSVStorage()
    stats = incremental_storage.get_stats()

    # Add duplicate count
    df = storage.load_products()
    if not df.empty:
        stats['duplicate_count'] = int(df.duplicated(subset=['product_id']).sum())
        stats['unique_products'] = int(df['product_id'].nunique())

        # Products scraped in last 24 hours
        if 'scraped_at' in df:
            df['scraped_at'] = pd.to_datetime(df['scraped_at'])
            recent = df[df['scraped_at'] > (datetime.now() - timedelta(hours=24))]
            stats['scraped_last_24h'] = len(recent)

    return stats


@app.delete("/api/storage/clear",
    tags=["Storage"],
    summary="Clear storage",
    description="Clear storage data with optional age filter"
)
async def clear_storage(
    days_to_keep: Optional[int] = Query(None, description="Keep data from last N days, clear older"),
    confirm: bool = Query(False, description="Confirm deletion")
):
    """Clear storage data"""
    if not confirm:
        return {"error": "Set confirm=true to proceed with deletion"}

    from ..storage.incremental_storage import IncrementalCSVStorage
    incremental_storage = IncrementalCSVStorage()

    if days_to_keep:
        removed = incremental_storage.clear_old_data(days_to_keep)
        return {"message": f"Removed {removed} records older than {days_to_keep} days"}
    else:
        # Clear all data
        import os
        if os.path.exists(incremental_storage.csv_path):
            os.remove(incremental_storage.csv_path)
        incremental_storage._init_storage()
        return {"message": "All data cleared"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)