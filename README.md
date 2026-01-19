# GDELT Incremental Processing Project

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

## Team Members & Responsibilities

- **Akshit Pramod Anchan:** 
- **Aniket Sandeep Dasurkar:** 
- **Anirudh Parihar:** 
- **Anushka Sen:** 
- **Prita Suresh:** 

---

## Prerequisites

### Required Software

1. **Homebrew** (Mac package manager)
2. **Colima** (Docker Engine for Mac)
3. **Docker CLI**
4. **Docker Compose**

### System Requirements

- **OS:** macOS (M1/M2/M3/M4 or Intel)
- **RAM:** 8GB minimum (16GB recommended)
- **Disk:** 10GB free space
- **CPU:** 4 cores minimum

---

## Setup Instructions

### Step 1: Install Colima and Docker
```bash
# Install Colima (lightweight Docker engine)
brew install colima

# Install Docker CLI tools
brew install docker

# Install Docker Compose
brew install docker-compose

# Verify installations
colima --version
docker --version
docker-compose --version
```

### Step 2: Start Colima
```bash
# Start Colima with appropriate resources
colima start --cpu 4 --memory 8 --disk 50

# This takes ~1-2 minutes on first run
# Wait until you see: "done"

# Verify Colima is running
colima status
```

**Expected output:**
```
INFO[0000] colima is running
INFO[0000] arch: aarch64 (or x86_64)
INFO[0000] runtime: docker
```

### Step 3: Clone the Repository
```bash
# Clone the project
git clone <your-repo-url>
cd global-conflict-monitor

# Verify project structure
ls -la
```

**Expected structure:**
```
global-conflict-monitor/
├── docker-compose.yml
├── flink/
│   └── lib/
│       ├── flink-sql-connector-postgres-cdc-2.4.2.jar
│       └── postgresql-42.7.1.jar
├── postgres/
│   └── init/
│       └── 01-init-schema.sql
├── scripts/
├── dashboard/
└── README.md
```

### Step 4: Download Flink CDC JARs (If Missing)

**Note:** These JARs should already be in the repo. If missing, download them:
```bash
cd flink/lib

# Download Flink CDC connector
curl -L -O https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.4.2/flink-sql-connector-postgres-cdc-2.4.2.jar

# Download PostgreSQL JDBC driver
curl -L -O https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Verify sizes
ls -lh
# CDC connector: ~25MB
# JDBC driver: ~1MB

cd ../..
```

### Step 5: Start All Services
```bash
# Start PostgreSQL + Flink cluster
docker-compose up -d

# This will:
# 1. Pull Docker images (first time: ~3-5 min)
# 2. Create containers
# 3. Initialize PostgreSQL database
# 4. Start Flink JobManager and TaskManager
```

**Wait 30-60 seconds for startup.**

### Step 6: Verify Everything is Running
```bash
# Check container status
docker-compose ps
```

**Expected output (all should show "Up"):**
```
NAME                STATUS              PORTS
gdelt-postgres      Up (healthy)        0.0.0.0:5432->5432/tcp
flink-jobmanager    Up                  0.0.0.0:8081->8081/tcp
flink-taskmanager   Up
```

### Step 7: Test PostgreSQL Connection
```bash
# Connect to PostgreSQL
docker exec -it gdelt-postgres psql -U flink_user -d gdelt
```

**Inside psql, run:**
```sql
-- List tables
\dt

-- Count sample data
SELECT COUNT(*) FROM gdelt_events;
-- Should return: 10

-- Exit
\q
```

### Step 8: Test Flink Web UI

Open your browser:
```
http://localhost:8081
```

You should see the Flink Dashboard with:
- Task Managers: 1 available
- Task Slots: 4 available

### Step 9: Test CDC Functionality
```bash
# Open Flink SQL Client
docker exec -it flink-jobmanager ./bin/sql-client.sh
```

**Inside Flink SQL, paste:**
```sql
CREATE TABLE test_cdc_source (
    globaleventid BIGINT,
    actor1_country_code STRING,
    actor2_country_code STRING,
    goldstein_scale DECIMAL(10,2),
    PRIMARY KEY (globaleventid) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'database-name' = 'gdelt',
    'schema-name' = 'public',
    'table-name' = 'gdelt_events',
    'slot.name' = 'flink_test_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- Query it
SELECT * FROM test_cdc_source LIMIT 5;
```

**If you see data, CDC is working!** Type `exit;` to quit.

---

## ✅ Setup Complete!

Your environment is ready. You should have:
- ✅ PostgreSQL running with CDC enabled
- ✅ Flink cluster operational
- ✅ 10 sample events in database
- ✅ CDC connector tested and working

---

## Common Commands Reference

See [COMMANDS.md](./COMMANDS.md) for detailed command reference.

---

## Troubleshooting

See [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) for common issues and solutions.

---

## Project Configuration

### Database Credentials

- **Host:** localhost
- **Port:** 5432
- **Database:** gdelt
- **Username:** flink_user
- **Password:** flink_pass

### Flink Web UI

- **URL:** http://localhost:8081

### Important Directories

- `flink/lib/` - Flink connector JARs
- `postgres/init/` - Database initialization scripts
- `scripts/` - Data loading and simulation scripts
- `dashboard/` - Streamlit dashboard code

---
