#!/bin/bash
# check-follower-logs.sh - Check if followers are receiving and processing events

echo "Checking follower logs for replication events..."
echo "================================================"
echo ""

# Get all follower pods
FOLLOWERS=$(kubectl get pods -l role=follower --no-headers -o custom-columns=":metadata.name")

if [ -z "$FOLLOWERS" ]; then
  echo "No follower pods found!"
  exit 1
fi

echo "Found follower pods:"
echo "$FOLLOWERS"
echo ""

# Check logs for each follower
for POD in $FOLLOWERS; do
  echo "===================================="
  echo "Logs for: $POD"
  echo "===================================="
  kubectl logs "$POD" --tail=20 | grep -i "applied\|event\|replication\|sequence" || echo "No replication events found"
  echo ""
done

echo ""
echo "To tail logs in real-time, run:"
echo "  kubectl logs -f <pod-name>"
echo ""
echo "Example:"
FIRST_FOLLOWER=$(echo "$FOLLOWERS" | head -n1)
echo "  kubectl logs -f $FIRST_FOLLOWER"
