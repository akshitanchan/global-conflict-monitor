# Project Structure

Detailed explanation of every file and directory in the project.
```
global_conflict-monitor/
├── README.md                    # Main setup guide (start here!)
├── COMMANDS.md                  # Command reference
├── TROUBLESHOOTING.md           # Common issues and solutions
├── PROJECT_STRUCTURE.md         # This file
│
├── docker-compose.yml           # Defines all services (Postgres, Flink)
│
├── flink/                       # Flink-related files
│   ├── lib/                     # Connector JAR files
│   │   ├── flink-sql-connector-postgres-cdc-2.4.2.jar  # CDC connector (DO NOT DELETE)
│   │   └── postgresql-42.7.1.jar                       # JDBC driver (DO NOT DELETE)
│   └── sql/                     # Flink SQL scripts (created by Person 2)
│       └── (your SQL files here)
│
├── postgres/                    # PostgreSQL-related files
│   └── init/                    # Database initialization scripts
│       └── 01-init-schema.sql   # Creates tables, adds sample data
│
├── scripts/                     # Python/bash scripts
│   ├── load_gdelt_data.py      # Load GDELT dataset 
│   ├── simulate_changes.py     # Simulate data changes 
│   └── benchmark.py            # Performance testing 
│
├── dashboard/                   # Streamlit dashboard 
│   ├── app.py                  # Main dashboard file
│   ├── requirements.txt        # Python dependencies
│   └── (other dashboard files)
│
├── docs/                        # Additional documentation
│   ├── FLINK_JOBS.md           # Flink job development guide
│   ├── DATA_LOADING.md         # GDELT data loading guide
│   ├── DASHBOARD.md            # Dashboard development guide
│   └── PERFORMANCE.md          # Performance testing guide
│
└── .gitignore                   # Files to ignore in git
```
---

## Data Flow Through Project Structure
```
1. GDELT CSV file
   ↓ (scripts/load_gdelt_data.py)
2. postgres → gdelt_events table
   ↓ (Flink CDC captures changes)
3. flink/sql/create_cdc_sources.sql reads changes
   ↓ (Flink processes incrementally)
4. flink/sql/create_aggregations.sql aggregates
   ↓ (Flink writes results)
5. postgres → country_event_counts, hourly_event_trends, etc.
   ↓ (Streamlit queries)
6. dashboard/app.py displays charts
```

---