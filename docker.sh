#!/bin/bash

# Docker management script for E-commerce Crawler
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Default environment
ENV="${ENV:-dev}"

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Function to load environment file
load_env() {
    local env_file=".env.$1"
    if [ -f "$env_file" ]; then
        print_info "Loading environment from $env_file"
        export $(cat "$env_file" | grep -v '^#' | xargs)
    else
        print_warning "Environment file $env_file not found, using defaults"
    fi
}

# Function to show usage
usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
    dev         Start development environment
    prod        Start production environment
    up          Start services (default: dev)
    down        Stop all services
    restart     Restart services
    build       Build Docker images
    logs        Show logs
    status      Show service status
    shell       Open shell in container
    clean       Clean up volumes and images
    test        Run tests
    backup      Backup data volumes
    restore     Restore data volumes

Options:
    -e, --env ENV       Environment (dev|prod|test)
    -s, --service SVC   Specific service
    -f, --follow        Follow logs
    -h, --help          Show this help message

Examples:
    $0 dev              Start development environment
    $0 prod             Start production environment
    $0 logs api -f      Follow API logs
    $0 shell worker     Open shell in worker container
    $0 clean            Clean up everything

EOF
}

# Development environment
dev() {
    print_info "Starting development environment..."
    load_env "dev"
    docker-compose --env-file .env.dev --profile dev up -d
    print_success "Development environment started"
    print_info "Services:"
    echo "  - API: http://localhost:8000"
    echo "  - Flower: http://localhost:5555"
    echo "  - Redis: localhost:6379"
}

# Production environment
prod() {
    print_info "Starting production environment..."
    load_env "prod"

    # Check for production requirements
    if [ ! -f ".env.prod" ]; then
        print_error ".env.prod file not found!"
        print_info "Copy .env.prod.example and configure it first"
        exit 1
    fi

    # Build production images
    print_info "Building production images..."
    docker-compose --env-file .env.prod build --build-arg BUILD_TARGET=production

    # Start services
    docker-compose --env-file .env.prod --profile prod up -d
    print_success "Production environment started"
    print_info "Services:"
    echo "  - API: http://localhost (via Nginx)"
    echo "  - Flower: http://localhost:5555 (protected)"
}

# Start services
up() {
    local profile="${1:-dev}"
    print_info "Starting $profile environment..."
    load_env "$profile"
    docker-compose --env-file ".env.$profile" --profile "$profile" up -d "${@:2}"
    print_success "Services started"
}

# Stop services
down() {
    print_info "Stopping all services..."
    docker-compose down
    print_success "Services stopped"
}

# Restart services
restart() {
    local service="$1"
    if [ -z "$service" ]; then
        print_info "Restarting all services..."
        docker-compose restart
    else
        print_info "Restarting $service..."
        docker-compose restart "$service"
    fi
    print_success "Services restarted"
}

# Build images
build() {
    local env="${1:-dev}"
    print_info "Building images for $env environment..."
    load_env "$env"

    if [ "$env" = "prod" ]; then
        docker-compose --env-file ".env.$env" build --build-arg BUILD_TARGET=production
    else
        docker-compose --env-file ".env.$env" build --build-arg BUILD_TARGET=development
    fi
    print_success "Images built"
}

# Show logs
logs() {
    local service="$1"
    local follow="${2:-}"

    if [ "$follow" = "-f" ] || [ "$follow" = "--follow" ]; then
        if [ -z "$service" ]; then
            docker-compose logs -f --tail=100
        else
            docker-compose logs -f --tail=100 "$service"
        fi
    else
        if [ -z "$service" ]; then
            docker-compose logs --tail=100
        else
            docker-compose logs --tail=100 "$service"
        fi
    fi
}

# Show status
status() {
    print_info "Service status:"
    docker-compose ps
    echo ""
    print_info "Resource usage:"
    docker stats --no-stream $(docker-compose ps -q) 2>/dev/null || true
}

# Open shell in container
shell() {
    local service="${1:-api}"
    print_info "Opening shell in $service container..."
    docker-compose exec "$service" /bin/bash || docker-compose exec "$service" /bin/sh
}

# Clean up
clean() {
    print_warning "This will remove all containers, volumes, and images!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cleaning up..."
        docker-compose down -v --rmi all
        docker system prune -af
        print_success "Cleanup complete"
    else
        print_info "Cleanup cancelled"
    fi
}

# Run tests
test() {
    print_info "Running tests..."
    docker-compose --profile test run --rm api pytest tests/
    print_success "Tests complete"
}

# Backup volumes
backup() {
    local backup_dir="backups/$(date +%Y%m%d_%H%M%S)"
    print_info "Creating backup in $backup_dir..."
    mkdir -p "$backup_dir"

    # Backup Redis data
    docker run --rm -v ecommerce-crawler_redis_data:/data -v "$PWD/$backup_dir":/backup \
        alpine tar czf /backup/redis_data.tar.gz -C /data .

    # Backup application data
    if [ -d "data" ]; then
        tar czf "$backup_dir/app_data.tar.gz" data/
    fi

    # Backup logs
    if [ -d "logs" ]; then
        tar czf "$backup_dir/logs.tar.gz" logs/
    fi

    print_success "Backup created in $backup_dir"
}

# Restore volumes
restore() {
    local backup_dir="$1"
    if [ -z "$backup_dir" ]; then
        print_error "Please specify backup directory"
        echo "Usage: $0 restore backups/YYYYMMDD_HHMMSS"
        exit 1
    fi

    if [ ! -d "$backup_dir" ]; then
        print_error "Backup directory not found: $backup_dir"
        exit 1
    fi

    print_warning "This will restore data from $backup_dir"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Restoring backup..."

        # Stop services
        docker-compose down

        # Restore Redis data
        if [ -f "$backup_dir/redis_data.tar.gz" ]; then
            docker run --rm -v ecommerce-crawler_redis_data:/data -v "$PWD/$backup_dir":/backup \
                alpine tar xzf /backup/redis_data.tar.gz -C /data
        fi

        # Restore application data
        if [ -f "$backup_dir/app_data.tar.gz" ]; then
            tar xzf "$backup_dir/app_data.tar.gz"
        fi

        # Restore logs
        if [ -f "$backup_dir/logs.tar.gz" ]; then
            tar xzf "$backup_dir/logs.tar.gz"
        fi

        print_success "Restore complete"
    else
        print_info "Restore cancelled"
    fi
}

# Main script
check_docker

case "$1" in
    dev)
        dev
        ;;
    prod)
        prod
        ;;
    up)
        up "${@:2}"
        ;;
    down)
        down
        ;;
    restart)
        restart "$2"
        ;;
    build)
        build "$2"
        ;;
    logs)
        logs "${@:2}"
        ;;
    status)
        status
        ;;
    shell)
        shell "$2"
        ;;
    clean)
        clean
        ;;
    test)
        test
        ;;
    backup)
        backup
        ;;
    restore)
        restore "$2"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        if [ -z "$1" ]; then
            usage
        else
            print_error "Unknown command: $1"
            usage
            exit 1
        fi
        ;;
esac