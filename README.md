# GLOBAL CONFLICT MONITOR

Real-time analytics pipeline for GDELT event data using PostgreSQL Change Data Capture, Apache Flink stream processing, incremental materialized view maintenance and Streamlit.

## Architecture
```
PostgreSQL (Source) 
    ↓ CDC (Change Data Capture)
Apache Flink (Incremental Processing)
    ↓ Aggregated Results
PostgreSQL (Results Tables)
    ↓ Query
Streamlit Dashboard
```

## Team Members

- **Akshit Pramod Anchan** 
- **Aniket Sandeep Dasurkar** 
- **Anirudh Parihar** 
- **Anushka Sen** 
- **Prita Suresh** 

---

## Prerequisites

**macOS:**
- Homebrew
- Colima (Docker engine)
- Docker CLI
- Docker Compose

**Linux:**
- Docker Engine
- Docker Compose plugin

**Windows:**
- WSL2 (Ubuntu)
- Docker Desktop with WSL2 integration

---

## Quick Start

### Installation

Run the automated setup script:
```bash
./setup.sh
```

This script will:
- Detect the operating system
- Install missing dependencies (macOS only)
- Download required JAR files (Flink CDC connector, PostgreSQL JDBC driver)
- Start Colima (macOS)
- Launch all Docker containers
- Wait for services to become healthy

**Services started:**
- PostgreSQL (port 5432) - Source and sink database
- PostgreSQL Baseline (port 5433) - Comparison baseline
- Flink JobManager (port 8081) - Cluster coordinator
- Flink TaskManager - Stream processing worker

### Verification

Check that all services are running:
```bash
docker compose ps
```

Access Flink Web UI:
```bash
open http://localhost:8081  # macOS
# or visit http://localhost:8081 in browser
```

Test PostgreSQL connection:
```bash
docker exec -it gdelt-postgres psql -U flink_user -d gdelt
```

---

## Data Loading and Pipeline Execution

### Step 1: Load GDELT Dataset

Place your GDELT data file in `./data/` directory, then load:
```bash
# Load full dataset
./scripts/load-gdelt-copy.sh data/GDELT.MASTERREDUCEDV2.TXT

# Or load limited rows for testing (faster)
SMALL_LOAD_LINES=20000 ./scripts/load-gdelt-copy.sh data/GDELT.MASTERREDUCEDV2.TXT
```

### Step 2: Start Incremental Aggregation Jobs

Launch the streaming pipeline (runs continuously):
```bash
./scripts/start-flink-aggregations.sh
```
Verify jobs are running:
```bash
# Via Web UI
open http://localhost:8081

# Via command line
curl -s http://localhost:8081/jobs/overview | grep -c '"state":"RUNNING"'
# Expected: 4
```

### Step 3: Verify Result Tables

Check that aggregates have been computed:
```bash
docker exec -it gdelt-postgres psql -U flink_user -d gdelt << EOF
SELECT 'daily_event_volume' as table_name, COUNT(*) FROM daily_event_volume_by_quadclass
UNION ALL
SELECT 'dyad_interactions', COUNT(*) FROM dyad_interactions
UNION ALL
SELECT 'top_actors', COUNT(*) FROM top_actors
UNION ALL
SELECT 'daily_cameo_metrics', COUNT(*) FROM daily_cameo_metrics;
EOF
```

All counts should be greater than zero. Check timestamps:
```bash
docker exec -it gdelt-postgres psql -U flink_user -d gdelt -c \
  "SELECT MAX(last_updated) FROM top_actors;"
```

Timestamp should be recent (within last 2 minutes).

---

## Testing Incremental Processing

### Insert New Events
```bash
# Insert 100 new events
./scripts/load-gdelt-copy.sh SMALL_LOAD_LINES=20000
```

### Update and Delete Operations
```bash
# Update existing events
python3 scripts/workload.py --update 50
# Delete events
python3 scripts/workload.py --delete 20
```

All operations should reflect in aggregate tables within 5-10 seconds.

---

### Dashboard

Launch Streamlit dashboard:
```bash
pip3 install -r requirements.txt
streamlit run app.py
```

Access at: `http://localhost:8501`

The dashboard provides:
- Real-time metrics (total events, conflict rate, avg Goldstein score)
- Geographic visualization (world map)
- Temporal trends (event volume over time)
- Interactive controls for benchmark testing

---
