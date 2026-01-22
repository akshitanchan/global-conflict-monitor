#!/bin/bash
echo "=== System Health Check ==="
echo ""
echo "1. Colima Status:"
colima status | head -3
echo ""
echo "2. Containers:"
docker-compose ps
echo ""
echo "3. PostgreSQL Row Count:"
docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT COUNT(*) FROM gdelt_events;" -t
echo ""
echo "4. Flink Web UI:"
curl -s http://localhost:8081/overview | grep -o '"taskmanagers":[0-9]*' || echo "Flink not accessible"
echo ""
echo "=== End Check ==="
