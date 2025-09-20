#!/bin/bash
# run.sh - Convenience script for Docker operations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_usage() {
    echo "Usage: $0 {build|start|stop|logs|shell|jupyter|test|clean}"
    echo ""
    echo "Commands:"
    echo "  build     Build Docker images"
    echo "  start     Start all services"
    echo "  stop      Stop all services"
    echo "  logs      Follow application logs"
    echo "  shell     Open shell in ETL container"
    echo "  jupyter   Open Jupyter Lab"
    echo "  test      Run test pipeline"
    echo "  clean     Clean up containers and volumes"
}

check_env() {
    if [ ! -f .env ]; then
        echo -e "${YELLOW}Warning: .env file not found. Copy .env.example to .env and configure it.${NC}"
        return 1
    fi
    return 0
}

case $1 in
    build)
        echo -e "${GREEN}Building Docker images...${NC}"
        docker-compose build
        ;;
    start)
        echo -e "${GREEN}Starting MongoDB Analytics POC...${NC}"
        check_env && docker-compose up -d
        echo -e "${GREEN}Services started! Access:${NC}"
        echo "  - Spark UI: http://localhost:4040"
        echo "  - Jupyter Lab: http://localhost:8888"
        ;;
    stop)
        echo -e "${YELLOW}Stopping services...${NC}"
        docker-compose down
        ;;
    logs)
        echo -e "${GREEN}Following application logs...${NC}"
        docker-compose logs -f mongodb-etl
        ;;
    shell)
        echo -e "${GREEN}Opening shell in ETL container...${NC}"
        docker-compose exec mongodb-etl bash
        ;;
    jupyter)
        echo -e "${GREEN}Opening Jupyter Lab...${NC}"
        echo "Visit: http://localhost:8888"
        echo "Token: $(grep JUPYTER_TOKEN .env | cut -d'=' -f2 || echo 'analytics123')"
        ;;
    test)
        echo -e "${GREEN}Running test pipeline...${NC}"
        docker-compose exec mongodb-etl python3 scripts/test_pipeline.py
        ;;
    clean)
        echo -e "${YELLOW}Cleaning up containers and volumes...${NC}"
        docker-compose down -v
        docker system prune -f
        ;;
    *)
        print_usage
        exit 1
        ;;
esac