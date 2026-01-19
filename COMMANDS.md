# Common Commands Reference

Quick reference for frequently used commands.

---

## Colima Commands

### Start/Stop Colima
```bash
# Start Colima
colima start

# Start with specific resources
colima start --cpu 4 --memory 8 --disk 50

# Stop Colima (when done for the day)
colima stop

# Restart Colima
colima restart

# Check status
colima status

# Delete Colima (complete reset)
colima delete
```

### When to Use
- **Start:** Every time you begin working (if you stopped it)
- **Stop:** When you're done for the day (saves battery/memory)
- **Restart:** If containers are acting weird
- **Delete:** If everything is broken and you need fresh start

---

## Docker Compose Commands

### Start/Stop Services
```bash
# Start all services in background
docker-compose up -d

# Start all services with logs visible
docker-compose up

# Stop all services (keeps data)
docker-compose down

# Stop all services and DELETE all data
docker-compose down -v

# Restart specific service
docker-compose restart jobmanager
docker-compose restart postgres

# Restart all services
docker-compose restart
```

### View Status and Logs
```bash
# Check which containers are running
docker-compose ps

# View logs for all services
docker-compose logs

# View logs for specific service
docker-compose logs postgres
docker-compose logs jobmanager
docker-compose logs taskmanager

# Follow logs in real-time (Ctrl+C to stop)
docker-compose logs -f

# View last 50 lines of logs
docker-compose logs --tail=50 jobmanager
```

### When to Use
- **up -d:** Start everything normally
- **down:** Stop everything at end of day
- **down -v:** Nuclear option - deletes ALL data, start fresh
- **logs:** Debug when something isn't working

---

## Docker Commands (Individual Containers)

### Execute Commands Inside Containers
```bash
# Connect to PostgreSQL
docker exec -it gdelt-postgres psql -U flink_user -d gdelt

# Open Flink SQL Client
docker exec -it flink-jobmanager ./bin/sql-client.sh

# Open bash shell in any container
docker exec -it gdelt-postgres bash
docker exec -it flink-jobmanager bash

# Run one-off command
docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT COUNT(*) FROM gdelt_events;"
```

### View Container Details
```bash
# List all running containers
docker ps

# List all containers (including stopped)
docker ps -a

# View container logs
docker logs gdelt-postgres
docker logs flink-jobmanager

# Follow logs in real-time
docker logs -f flink-jobmanager

# Inspect container configuration
docker inspect gdelt-postgres

# View container resource usage
docker stats
```

### When to Use
- **exec -it:** Interactive access (psql, bash, SQL client)
- **exec:** Run single command and exit
- **logs:** Debug specific container issues
- **stats:** Check if containers are using too much memory/CPU

---

## PostgreSQL Commands

### Connect to Database
```bash
# Method 1: Via docker exec
docker exec -it gdelt-postgres psql -U flink_user -d gdelt

# Method 2: From your Mac (if you have psql installed)
psql -h localhost -p 5432 -U flink_user -d gdelt
# Password: flink_pass
```

### Common psql Commands

Once inside psql:
```sql
-- List all databases
\l

-- List all tables
\dt

-- Describe table structure
\d gdelt_events
\d country_event_counts

-- Show table data
SELECT * FROM gdelt_events LIMIT 10;

-- Count rows
SELECT COUNT(*) FROM gdelt_events;

-- Check CDC replication slots
SELECT * FROM pg_replication_slots;

-- Exit psql
\q
```

### Run SQL from Command Line
```bash
# Run single query
docker exec gdelt-postgres psql -U flink_user -d gdelt -c "SELECT COUNT(*) FROM gdelt_events;"

# Run SQL file
docker exec -i gdelt-postgres psql -U flink_user -d gdelt < my_query.sql
```

### When to Use
- **psql interactive:** Exploring data, running queries
- **-c flag:** Quick checks, scripts
- **SQL file:** Running complex queries or DDL

---

## Flink Commands

### Flink SQL Client
```bash
# Open SQL Client
docker exec -it flink-jobmanager ./bin/sql-client.sh

# Run SQL file
docker exec -i flink-jobmanager ./bin/sql-client.sh -f /path/to/script.sql
```

### Inside Flink SQL Client
```sql
-- Show all tables
SHOW TABLES;

-- Describe table
DESCRIBE table_name;

-- Show jobs
SHOW JOBS;

-- Cancel job
STOP JOB 'job_id';

-- Exit
exit;
```

### Flink Web UI

Access in browser: http://localhost:8081

Features:
- View running jobs
- Check task manager status
- Monitor checkpoints
- View job execution graphs

### When to Use
- **SQL Client:** Creating tables, running queries, testing
- **Web UI:** Monitoring jobs, debugging performance
- **SHOW JOBS:** Check what's currently running

---

## Git Commands (For Team Collaboration)

### Basic Workflow
```bash
# Pull latest changes
git pull origin main

# Check what changed
git status

# Add your changes
git add .

# Commit with message
git commit -m "Added data loading script"

# Push to GitHub
git push origin main

# Create new branch for your feature
git checkout -b feature/flink-jobs
git push origin feature/flink-jobs
```

### Before Starting Work
```bash
# Always pull latest first!
git pull origin main

# Start docker if needed
docker-compose up -d
```

### When to Use
- **git pull:** Start of every work session
- **git push:** End of work session or after completing a feature
- **branches:** For experimental work (optional for 10-day project)

---

## Health Check Commands

### Quick System Check
```bash
# Check everything at once
cat > check-health.sh << 'EOF'
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
EOF

chmod +x check-health.sh
./check-health.sh
```

### Individual Checks
```bash
# Is Colima running?
colima status

# Are containers up?
docker-compose ps

# Can I connect to PostgreSQL?
docker exec gdelt-postgres pg_isready -U flink_user

# Is Flink responding?
curl http://localhost:8081

# Do my JAR files exist?
ls -lh flink/lib/
```

---

## Troubleshooting Commands

### Container Issues
```bash
# Container won't start - check logs
docker-compose logs <service-name>

# Container is stuck - force restart
docker-compose restart <service-name>

# Everything is broken - nuclear option
docker-compose down -v
docker-compose up -d

# Check container resource usage
docker stats
```

### Port Conflicts
```bash
# Check what's using port 5432
lsof -i :5432

# Check what's using port 8081
lsof -i :8081

# Kill process using port (if needed)
kill -9 <PID>
```

### Volume/Data Issues
```bash
# Delete all data and start fresh
docker-compose down -v

# View volume details
docker volume ls
docker volume inspect gdelt-incremental-processing_postgres_data
```

### Colima Issues
```bash
# Colima won't start - check logs
colima logs

# Complete Colima reset
colima delete
colima start --cpu 4 --memory 8 --disk 50
```

---

## Quick Command Cheatsheet

| Task | Command |
|------|---------|
| Start everything | `docker-compose up -d` |
| Stop everything | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Check status | `docker-compose ps` |
| Connect to PostgreSQL | `docker exec -it gdelt-postgres psql -U flink_user -d gdelt` |
| Open Flink SQL | `docker exec -it flink-jobmanager ./bin/sql-client.sh` |
| Restart service | `docker-compose restart <service>` |
| Nuclear reset | `docker-compose down -v && docker-compose up -d` |

---

## Daily Workflow

### Starting Your Day
```bash
# 1. Navigate to project
cd ~/path/to/global-conflict-monitor

# 2. Pull latest changes
git pull origin main

# 3. Start Colima (if stopped)
colima start

# 4. Start services
docker-compose up -d

# 5. Verify everything is running
docker-compose ps
```

### Ending Your Day
```bash
# 1. Commit your work
git add .
git commit -m "Description of what you did"
git push origin main

# 2. Stop services (optional - saves resources)
docker-compose down

# 3. Stop Colima (optional - saves battery)
colima stop
```
---