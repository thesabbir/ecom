#!/bin/bash

echo "🌸 Starting Flower (Celery Monitoring)..."
echo "📊 Dashboard will be available at: http://localhost:5555"
echo ""

# Start Flower for monitoring
celery -A src.workers.celery_app flower \
    --port=5555 \
    --broker=redis://localhost:6379/0 \
    --basic_auth=admin:admin123