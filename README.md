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