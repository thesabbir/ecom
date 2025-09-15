# Multi-stage Dockerfile for both development and production
FROM python:3.11-slim AS base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY pyproject.toml README.md ./

# Install Python dependencies
RUN pip install --no-cache-dir uv && \
    uv pip install --system .

# Install Playwright browsers
RUN playwright install chromium

# Development stage
FROM base AS development

# Copy application code
COPY src/ ./src/

# Create necessary directories
RUN mkdir -p data logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command for development
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Production stage
FROM base AS production

# Install production server
RUN pip install --no-cache-dir gunicorn

# Create non-root user
RUN groupadd -r crawler && useradd -r -g crawler crawler

# Copy application code with proper ownership
COPY --chown=crawler:crawler src/ ./src/

# Create necessary directories with proper permissions
RUN mkdir -p /app/data /app/logs && \
    chown -R crawler:crawler /app/data /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1

# Switch to non-root user
USER crawler

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; exit(0 if requests.get('http://localhost:8000/health').status_code == 200 else 1)"

# Default command for production
CMD ["gunicorn", "src.api.main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--timeout", "120"]