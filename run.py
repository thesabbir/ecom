import uvicorn
from src.api.main import app as main_app
from src.api.analytics import router as analytics_router
from src.api.logs import router as logs_router
from src.api.crawl_queue import router as queue_router

# Include routers
main_app.include_router(analytics_router)
main_app.include_router(logs_router)
main_app.include_router(queue_router)

if __name__ == "__main__":
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        timeout_keep_alive=30,  # Timeout for keep-alive connections
        limit_concurrency=100,  # Limit concurrent connections
        limit_max_requests=1000,  # Restart workers after N requests to prevent memory leaks
        backlog=2048,  # Socket backlog
    )
