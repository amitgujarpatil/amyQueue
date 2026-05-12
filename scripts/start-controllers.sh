#!/bin/bash
# Start a 3-node controller cluster locally

set -e

echo "Starting AmyQueue Controller Cluster..."
echo "========================================="
echo ""

# Check if binaries exist
if [ ! -f "bin/controller" ]; then
    echo "Error: Controller binary not found. Run 'make build' first."
    exit 1
fi

# Create data directories
mkdir -p data/controller-1
mkdir -p data/controller-2
mkdir -p data/controller-3
mkdir -p logs

# Start controller-1 (will become leader)
echo "Starting controller-1 on ports 8080 (HTTP), 8081 (Raft), 8082 (gRPC)..."
NODE_ID=controller-1 \
HTTP_PORT=8080 \
RAFT_PORT=8081 \
GRPC_PORT=8082 \
DATA_DIR=./data/controller-1 \
CONTROLLER_PEERS=controller-1:8081,controller-2:8083,controller-3:8085 \
./bin/controller > logs/controller-1.log 2>&1 &
echo $! > /tmp/amyqueue-controller-1.pid

# Start controller-2
echo "Starting controller-2 on ports 8180 (HTTP), 8083 (Raft), 8182 (gRPC)..."
NODE_ID=controller-2 \
HTTP_PORT=8180 \
RAFT_PORT=8083 \
GRPC_PORT=8182 \
DATA_DIR=./data/controller-2 \
CONTROLLER_PEERS=controller-1:8081,controller-2:8083,controller-3:8085 \
./bin/controller > logs/controller-2.log 2>&1 &
echo $! > /tmp/amyqueue-controller-2.pid

# Start controller-3
echo "Starting controller-3 on ports 8280 (HTTP), 8085 (Raft), 8282 (gRPC)..."
NODE_ID=controller-3 \
HTTP_PORT=8280 \
RAFT_PORT=8085 \
GRPC_PORT=8282 \
DATA_DIR=./data/controller-3 \
CONTROLLER_PEERS=controller-1:8081,controller-2:8083,controller-3:8085 \
./bin/controller > logs/controller-3.log 2>&1 &
echo $! > /tmp/amyqueue-controller-3.pid

sleep 2

echo ""
echo "Controller cluster started!"
echo ""
echo "Endpoints:"
echo "  Controller-1: http://localhost:8080 (gRPC: 8082)"
echo "  Controller-2: http://localhost:8180 (gRPC: 8182)"
echo "  Controller-3: http://localhost:8280 (gRPC: 8282)"
echo ""
echo "Logs: ./logs/controller-*.log"
echo ""
echo "To stop: ./scripts/stop-controllers.sh"
