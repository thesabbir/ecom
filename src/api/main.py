from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uuid
import time

from ..utils.logging_config import get_logger
from .analytics import router as analytics_router
from .crawl_queue import router as crawl_queue_router
from .logs import router as logs_router
from .products import router as products_router
from .export import router as export_router
from .monitoring import router as monitoring_router
from .monitoring import (
    http_requests_total,
    http_request_duration_seconds,
    errors_total
)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting E-commerce Crawler API...")
    # Startup code here
    yield
    # Shutdown code here
    logger.info("Shutting down E-commerce Crawler API...")


app = FastAPI(
    title="E-commerce Crawler API",
    description="Advanced web crawler for e-commerce sites with analytics and monitoring",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Crawling",
            "description": "Queue-based crawl job management with Celery workers",
        },
        {
            "name": "Products",
            "description": "Product data retrieval and search",
        },
        {
            "name": "Analytics",
            "description": "Market insights and price analysis",
        },
        {
            "name": "Export",
            "description": "Data export in various formats",
        },
        {
            "name": "Monitoring",
            "description": "Health checks and metrics",
        },
    ],
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(analytics_router)
app.include_router(crawl_queue_router)
app.include_router(logs_router)
app.include_router(products_router)
app.include_router(export_router)
app.include_router(monitoring_router)


@app.middleware("http")
async def track_metrics(request, call_next):
    """Track metrics for all HTTP requests with enhanced logging"""
    start_time = time.time()

    # Generate trace ID for request tracking
    trace_id = str(uuid.uuid4())[:8]

    # Log incoming request
    logger.info(
        f"HTTP request received - method: {request.method}, url: {request.url}, trace_id: {trace_id}"
    )

    try:
        response = await call_next(request)
    except Exception as e:
        logger.error(f"Request failed - trace_id: {trace_id}, error: {e}")
        errors_total.labels(error_type="http_request").inc()
        raise

    duration = time.time() - start_time

    # Track metrics
    http_request_duration_seconds.labels(
        method=request.method, endpoint=request.url.path
    ).observe(duration)

    http_requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()

    # Log response
    logger.info(
        f"HTTP request completed - method: {request.method}, url: {request.url}, "
        f"status: {response.status_code}, duration_ms: {duration * 1000:.2f}, trace_id: {trace_id}"
    )

    return response


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "E-commerce Crawler API",
        "version": "2.0.0",
        "status": "operational",
        "documentation": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }