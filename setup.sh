#!/bin/bash

echo "======================================"
echo "GLOBAL CONFLICT MONITOR - Quick Setup"
echo "======================================"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
command -v brew >/dev/null 2>&1 || { echo "Homebrew not found. Install from https://brew.sh"; exit 1; }
echo "✓ Homebrew installed"

# Install Colima if needed
if ! command -v colima &> /dev/null; then
    echo "Installing Colima..."
    brew install colima
fi
echo "✓ Colima installed"

# Install Docker if needed
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    brew install docker docker-compose
fi
echo "✓ Docker installed"

# Start Colima
echo ""
echo "Starting Colima..."
colima start --cpu 4 --memory 8 --disk 50

# Verify JARs
echo ""
echo "Verifying Flink connector JARs..."
if [ ! -f "flink/lib/flink-sql-connector-postgres-cdc-2.4.2.jar" ]; then
    echo "Downloading Flink CDC connector..."
    curl -L -o flink/lib/flink-sql-connector-postgres-cdc-2.4.2.jar https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.4.2/flink-sql-connector-postgres-cdc-2.4.2.jar
fi

if [ ! -f "flink/lib/postgresql-42.7.1.jar" ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    curl -L -o flink/lib/postgresql-42.7.1.jar https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
fi
echo "✓ JARs present"

# Start services
echo ""
echo "Starting Docker services..."
docker-compose up -d

echo ""
echo "Waiting for services to start (30 seconds)..."
sleep 30

# Verify
echo ""
echo "======================================"
echo "Setup Complete!"
echo "======================================"
echo ""
docker-compose ps
echo ""
echo "Access Flink UI: http://localhost:8081"
echo ""
echo "To test PostgreSQL:"
echo "  docker exec -it gdelt-postgres psql -U flink_user -d gdelt"
echo ""
echo "To test Flink CDC:"
echo "  docker exec -it flink-jobmanager ./bin/sql-client.sh"
echo ""