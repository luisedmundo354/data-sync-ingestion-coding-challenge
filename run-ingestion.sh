#!/bin/sh
set -e

if [ -f ./.env ]; then
  # Export variables from .env for docker compose.
  set -a
  . ./.env
  set +a
fi

if [ -z "${TARGET_API_KEY:-}" ]; then
  echo "ERROR: TARGET_API_KEY is not set."
  echo "Create .env from .env.example and set TARGET_API_KEY, then re-run."
  exit 1
fi

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

# Start the ingestion services
echo "Starting services..."
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 10

if ! docker inspect assignment-ingestion >/dev/null 2>&1; then
  echo "ERROR: ingestion container (assignment-ingestion) was not created."
  echo "Check docker compose output for errors."
  exit 1
fi

# Monitor progress
echo ""
echo "Monitoring ingestion progress..."
echo "(Press Ctrl+C to stop monitoring)"
echo "=============================================="

while true; do
    RUNNING=$(docker inspect -f '{{.State.Running}}' assignment-ingestion 2>/dev/null || echo "false")
    EXITCODE=$(docker inspect -f '{{.State.ExitCode}}' assignment-ingestion 2>/dev/null || echo "1")
    if [ "$RUNNING" != "true" ] && [ "$EXITCODE" -ne 0 ]; then
        echo ""
        echo "=============================================="
        echo "INGESTION FAILED (exit code: $EXITCODE)"
        echo "Last logs:"
        docker logs --tail=200 assignment-ingestion 2>&1 || true
        echo "=============================================="
        exit 1
    fi

    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null | tr -d ' ' || echo "0")

    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "=============================================="
        exit 0
    fi

    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done
