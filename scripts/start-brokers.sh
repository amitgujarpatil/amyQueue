#!/bin/bash
# Start broker nodes locally

set -e

echo "Starting AmyQueue Brokers..."
echo "=============================="
echo ""

# Check if binaries exist
if [ ! -f "bin/broker" ]; then
    echo "Error: Broker binary not found. Run 'make build' first."
    exit 1
fi

# Create data directories
mkdir -p data/broker-1
mkdir -p data/broker-2
mkdir -p logs

# Start broker-1
echo "Starting broker-1 on port 9092..."
NODE_ID=broker-1 \
BROKER_ID=broker-1 \
BROKER_HOST=localhost \
BROKER_PORT=9092 \
GRPC_PORT=9093 \
DATA_DIR=./data/broker-1 \
CONTROLLER_PEERS=localhost:8082,localhost:8182,localhost:8282 \
./bin/broker > logs/broker-1.log 2>&1 &
echo $! > /tmp/amyqueue-broker-1.pid

# Start broker-2
echo "Starting broker-2 on port 9192..."
NODE_ID=broker-2 \
BROKER_ID=broker-2 \
BROKER_HOST=localhost \
BROKER_PORT=9192 \
GRPC_PORT=9193 \
DATA_DIR=./data/broker-2 \
CONTROLLER_PEERS=localhost:8082,localhost:8182,localhost:8282 \
./bin/broker > logs/broker-2.log 2>&1 &
echo $! > /tmp/amyqueue-broker-2.pid

sleep 2

echo ""
echo "Brokers started!"
echo ""
echo "Endpoints:"
echo "  Broker-1: localhost:9092 (gRPC: 9093)"
echo "  Broker-2: localhost:9192 (gRPC: 9193)"
echo ""
echo "Logs: ./logs/broker-*.log"
echo ""
echo "To stop: ./scripts/stop-brokers.sh"
