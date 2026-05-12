#!/bin/bash
# Stop controller cluster

echo "Stopping AmyQueue Controller Cluster..."

# Stop controllers
for i in 1 2 3; do
    if [ -f "/tmp/amyqueue-controller-$i.pid" ]; then
        PID=$(cat /tmp/amyqueue-controller-$i.pid)
        echo "Stopping controller-$i (PID: $PID)..."
        kill $PID 2>/dev/null || true
        rm /tmp/amyqueue-controller-$i.pid
    fi
done

echo "Controller cluster stopped."
