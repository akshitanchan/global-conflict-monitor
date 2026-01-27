#!/bin/bash

echo "=== Fixing Replication Slot Issue ==="
echo ""

# 1. Find active slot PID
echo "1. Checking active slot..."
ACTIVE_PID=$(docker exec gdelt-postgres psql -U flink_user -d gdelt -t -c "SELECT active_pid FROM pg_replication_slots WHERE slot_name='gdelt_flink_slot';" | tr -d ' ')

if [ -n "$ACTIVE_PID" ] && [ "$ACTIVE_PID" != "" ]; then
    echo "   Found active PID: $ACTIVE_PID"
    
    # Terminate it
    echo "2. Terminating PID $ACTIVE_PID..."
    docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT pg_terminate_backend($ACTIVE_PID);" >/dev/null
    sleep 2
fi

# 3. Reset CDC
echo "3. Resetting CDC (dropping slot and publication)..."
./scripts/reset-cdc.sh

# 4. Cancel Flink jobs
echo "4. Cancelling all Flink jobs..."
curl -s http://localhost:8081/jobs | grep -o '"id":"[^"]*"' | cut -d'"' -f4 | while read job_id; do
  curl -s -X PATCH "http://localhost:8081/jobs/${job_id}?mode=cancel" >/dev/null
  sleep 1
done

sleep 5

# 5. Restart Flink
echo "5. Restarting Flink containers..."
docker-compose restart jobmanager taskmanager >/dev/null 2>&1
echo "   Waiting 30 seconds for restart..."
sleep 30

# 6. Verify clean state
echo "6. Verifying clean state..."
SLOTS=$(docker exec gdelt-postgres psql -U flink_user -d gdelt -t -c "SELECT COUNT(*) FROM pg_replication_slots;" | tr -d ' ')
echo "   Replication slots: $SLOTS (should be 0)"

# 7. Start jobs
echo "7. Starting Flink aggregation pipeline..."
./scripts/start-flink-aggregations.sh

echo ""
echo "8. Waiting 10 seconds for jobs to start..."
sleep 10

# 9. Check status
RUNNING=$(curl -s http://localhost:8081/jobs | grep -o '"status":"RUNNING"' | wc -l)
echo ""
echo "=== Status Check ==="
echo "Running jobs: $RUNNING (should be 4)"

if [ "$RUNNING" -eq 4 ]; then
    echo "✅ SUCCESS! All 4 jobs running!"
    echo ""
    echo "Verify in Web UI: http://localhost:8081"
else
    echo "❌ Issue persists. Check Flink Web UI for errors."
    echo "   http://localhost:8081"
fi
