#!/bin/bash
# Stop broker nodes

echo "Stopping AmyQueue Brokers..."

# Stop brokers
for i in 1 2; do
    if [ -f "/tmp/amyqueue-broker-$i.pid" ]; then
        PID=$(cat /tmp/amyqueue-broker-$i.pid)
        echo "Stopping broker-$i (PID: $PID)..."
        kill $PID 2>/dev/null || true
        rm /tmp/amyqueue-broker-$i.pid
    fi
done

echo "Brokers stopped."
