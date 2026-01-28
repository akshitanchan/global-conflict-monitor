# GLOBAL CONFLICT MONITOR

Real-time analytics dashboard using PostgreSQL CDC, Apache Flink, and Streamlit.

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

### Required Software

1. **Colima** (Docker Engine for Mac)
2. **Docker CLI**
3. **Docker Compose**

### System Requirements

**Supported Platforms**
- **macOS:** Apple Silicon (M1–M4) or Intel
- **Windows:** Windows 10/11 (64-bit)
- **Linux:** Ubuntu 20.04+ (or equivalent), 64-bit

**Hardware**
- **RAM:** 8GB+ recommended
- **CPU:** 4 cores+ recommended
- **Disk:** 10GB+ free space recommended

**Platform Notes**
- **Windows:** WSL2 recommended

---

## Setup Instructions

### Step 1: Install Colima and Docker
```bash
# Install Colima (lightweight Docker engine)
brew install colima

# Install Docker CLI tools
brew install docker

# Verify installations
colima --version
docker --version
docker compose version
```

### Step 2: Start Colima
```bash
# Start Colima with appropriate resources
colima start --cpu 4 --memory 8 --disk 50

# Verify Colima is running
colima status
```

### Step 3: Clone the Repository
```bash
# Clone the project
git clone https://github.com/akshitanchan/global-conflict-monitor
cd global-conflict-monitor

# Optional: Download JARs (if missing)
curl -L -O https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.4.2/flink-sql-connector-postgres-cdc-2.4.2.jar
curl -L -O https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

### Step 4: Start all Services and Verify
```bash
# Start docker cluster
docker compose up -d

# Check container status
docker compose ps
```

## Phase 2 Quickstart (ingestion + incremental views)

### 0) start containers
```bash
./setup.sh
./scripts/tests/phase1-infra-test.sh
```

### 1) load data (tab-separated, header)
Put your big file in `./data/` (docker mounts it into Postgres as `/data`), then run:
```bash
./scripts/setup/load-gdelt-copy.sh data/GDELT.MASTERREDUCEDV2.TXT
```

### 2) start streaming incremental aggregations (keeps running)
Run in a separate terminal:
```bash
./scripts/setup/start-flink-aggregations.sh
```

### 3) sanity check results in Postgres
```bash
docker exec -it gdelt-postgres psql -U flink_user -d gdelt -c "select * from daily_event_volume_by_quadclass limit 5;"
docker exec -it gdelt-postgres psql -U flink_user -d gdelt -c "select * from top_actors order by total_events desc limit 10;"
docker exec -it gdelt-postgres psql -U flink_user -d gdelt -c "select * from daily_cameo_metrics order by total_events desc limit 10;"
```

### 4) simulate changes (inserts/updates/deletes + late arrivals)
Install python deps (locally):
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Then run:
```bash
python scripts/simulate-changes.py --insert 200
python scripts/simulate-changes.py --update 50
python scripts/simulate-changes.py --delete 20
python scripts/simulate-changes.py --insert 100 --late
```

You should see the aggregate tables update within seconds.

## Dashboard workflow (phase 1)

the streamlit app is a **viewer + batch trigger + benchmark viewer**.

- do the **initial bulk load** from cli (or the app's **admin** tab if you really want), then start the flink jobs.
- use the sidebar **run batch** button to apply a delta workload and measure:
  - end-to-end incremental catch-up time (batch boundary detected via a marker actor)
  - baseline full recompute time (COUNT(*) wrappers; no baseline tables required)
  - correctness checks for impacted dates (event totals, quad breakdown, top-k match rates)

### Top-K cameo per day
We compute Top-K by querying the per-day cameo aggregate table:
```sql
select cameo_code, total_events
from daily_cameo_metrics
where event_date = 19790101
order by total_events desc
limit 10;
```
This is "bounded" to a single `event_date` bucket.