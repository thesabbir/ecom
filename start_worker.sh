#!/bin/bash

echo "🔧 Starting Celery Worker..."
echo "📝 Logs will be saved to logs/celery_worker.log"
echo ""

# Create logs directory if it doesn't exist
mkdir -p logs

# Start Celery worker with proper configuration
celery -A src.workers.celery_app worker \
    --loglevel=info \
    --concurrency=4 \
    --pool=prefork \
    --queues=crawl_high,crawl_default,crawl_low \
    --hostname=worker@%h \
    --max-tasks-per-child=50 \
    --time-limit=2100 \
    --soft-time-limit=1800 \
    --logfile=logs/celery_worker.log