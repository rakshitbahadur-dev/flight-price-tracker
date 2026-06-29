# FlightPriceTracker

An end-to-end batch data pipeline built on Databricks that ingests real-time flight schedule data, processes it through a Medallion architecture, detects price drops using a 7-day moving average, and sends automated email alerts.

---

## Architecture

```
AeroDataBox API
      ↓
ingest_flight_prices     →  flight_prices_raw    (Bronze)
      ↓
transform_flight_prices  →  flight_prices_clean  (Silver)
      ↓
aggregate_flight_prices  →  flight_prices_avg    (Gold)
      ↓
send_price_alerts        →  flight_price_alerts  (Gold) + Gmail
```

---

## Tech Stack

| Component  | Technology                        |
|------------|-----------------------------------|
| Platform   | Databricks Free Edition           |
| Processing | PySpark                           |
| Storage    | Delta Lake                        |
| Scheduler  | Databricks Jobs (every 3 hours)   |
| Language   | Python 3.12                       |
| Alerting   | Gmail SMTP                        |
| Source API | AeroDataBox via RapidAPI          |

---

## Tables

| Layer  | Table                  | Description                              |
|--------|------------------------|------------------------------------------|
| Config | routes_config          | Source of truth for all tracked routes   |
| Bronze | flight_prices_raw      | Raw API response, append only            |
| Silver | flight_prices_clean    | DQ checked, deduplicated, typed          |
| Gold   | flight_prices_avg      | 7-day moving average, SCD2               |
| Gold   | flight_price_alerts    | Alert history with email delivery status |
| Audit  | pipeline_audit_log     | Every notebook run with status and counts|
| Audit  | api_error_log          | API failures with retry details          |

---

## Routes Tracked

| Route   | Origin  | Destination | Type          |
|---------|---------|-------------|---------------|
| DEL-BOM | Delhi   | Mumbai      | Domestic      |
| DEL-BLR | Delhi   | Bangalore   | Domestic      |
| BOM-MAA | Mumbai  | Chennai     | Domestic      |
| CCU-DEL | Kolkata | Delhi       | Domestic      |
| DEL-DXB | Delhi   | Dubai       | International |
| BOM-LHR | Mumbai  | London      | International |
| DEL-SIN | Delhi   | Singapore   | International |
| BOM-JFK | Mumbai  | New York    | International |

---

## Pipeline Logic

### Bronze — ingest_flight_prices
- Reads active routes from routes_config
- Calls AeroDataBox API per route (720-minute window)
- Assigns mock prices with route-specific base and random variation
- Computes MD5 checksum per record
- Appends to Bronze Delta table
- Logs API errors with 3 retries and 60-second wait

### Silver — transform_flight_prices
- Reads Bronze records for current run
- Applies type casting and string cleaning
- Runs 9 DQ rules in priority order
- Deduplicates using ROW_NUMBER on record_checksum
- MERGEs into Silver on record_checksum (idempotent)

### Gold — aggregate_flight_prices
- Reads valid, non-duplicate Silver records from last 7 days
- Computes avg, min, max, stddev, count using Spark window functions
- Determines price trend — FALLING / RISING / STABLE
- SCD2 MERGE into Gold — expires old record, inserts new

### Alert — send_price_alerts
- Reads Gold records where price_drop_pct >= 15%
- Determines severity — LOW (15-24%), MEDIUM (25-39%), HIGH (40%+)
- Sends HTML email via Gmail SMTP
- Logs every alert to flight_price_alerts

---

## Alert Severity

| Drop %      | Severity |
|-------------|----------|
| 15% — 24.9% | LOW      |
| 25% — 39.9% | MEDIUM   |
| 40% and above | HIGH   |

---

## API Usage

| Parameter           | Value                        |
|---------------------|------------------------------|
| API                 | AeroDataBox via RapidAPI     |
| Plan                | Basic (Free)                 |
| Schedule            | Every 3 hours                |
| Calls per month     | 1,920                        |
| Free tier limit     | 2,400 requests / month       |

---

## Project Structure

```
flight-price-tracker/
│
├── pipeline/
│   ├── ingest_flight_prices
│   ├── transform_flight_prices
│   ├── aggregate_flight_prices
│   └── send_price_alerts
│
├── ddl/
│   ├── 01_create_database.sql
│   ├── 02_routes_config.sql
│   ├── 03_flight_prices_raw.sql
│   ├── 04_flight_prices_clean.sql
│   ├── 05_flight_prices_avg.sql
│   ├── 06_flight_price_alerts.sql
│   ├── 07_pipeline_audit_log.sql
│   ├── 08_api_error_log.sql
│   └── 09_seed_routes_config.sql
│
├── tests/
│   ├── test_ingest.py
│   ├── test_transform.py
│   ├── test_aggregate.py
│   └── test_alert.py
│
├── docs/
│   └── FlightPriceTracker_Technical_Documentation.pdf
│
└── README.md
```

---

## Monitoring

```sql
-- Pipeline run history
SELECT notebook_name, status, records_read, records_written, duration_seconds
FROM workspace.flight_tracker.pipeline_audit_log
ORDER BY created_at DESC;

-- Current gold prices
SELECT route_id, airline, latest_price, avg_7day_price, price_drop_pct, price_trend
FROM workspace.flight_tracker.flight_prices_avg
WHERE is_current = true
ORDER BY price_drop_pct DESC;

-- Alert history
SELECT route_id, airline, drop_pct, alert_severity, email_status, alert_sent_at
FROM workspace.flight_tracker.flight_price_alerts
ORDER BY alert_created_at DESC;
```

---

## Author

Rakshit Bahadur — Senior Data Engineer  
GitHub: [rakshitbahadur-dev](https://github.com/rakshitbahadur-dev)
