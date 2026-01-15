#!/bin/bash

# Build and run telecom distributed system

set -e

echo "=========================================="
echo "BUILDING DISTRIBUTED TELECOM SYSTEM"
echo "=========================================="

# Build Docker image
echo "Building Docker image..."
docker build -t telecom-system:latest .

echo ""
echo "=========================================="
echo "CHOOSE DEPLOYMENT OPTION:"
echo "=========================================="
echo "1. Single container demo (quick)"
echo "2. Full multi-node deployment (12 nodes)"
echo "3. Performance evaluation only"
echo "4. Clean up containers"
echo "=========================================="

read -p "Enter choice (1-4): " choice

case $choice in
    1)
        echo "Starting single container demo..."
        docker-compose -f docker-compose-simple.yml up --build
        ;;
    2)
        echo "Starting full multi-node deployment (12 nodes)..."
        docker-compose up --build -d
        echo "Containers started in background."
        echo "Check logs: docker-compose logs -f"
        echo "Run demo: docker-compose run --rm demo"
        ;;
    3)
        echo "Running performance evaluation..."
        docker-compose -f docker-compose-simple.yml run --rm telecom-system \
            python3 tests/performance/evaluate_system_fixed.py
        ;;
    4)
        echo "Cleaning up..."
        docker-compose down -v
        docker system prune -f
        echo "Cleanup complete."
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac
EOF

chmod +x docker-build.sh