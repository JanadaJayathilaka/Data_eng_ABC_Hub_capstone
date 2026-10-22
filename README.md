# PulseTrack — Data Engineering Capstone Project

A comprehensive Data Warehousing & Engineering capstone project implementing a multi-source Medallion Architecture (Raw -> Bronze -> Silver -> Gold) using PostgreSQL, MongoDB, Kafka, and Apache Airflow.

---

## 📌 Project Architecture & Sources

The project integrates data from **three distinct source systems**:
1. **Relational Database Export (PostgreSQL / CSV)**:
   * 11 relational tables covering the entire advertising and conversion funnel (`account_managers`, `advertisers`, `partners`, `offers`, `campaigns`, `creatives`, `impressions`, `clicks`, `conversions`, `conversion_touchpoints`, `revenue_adjustments`).
2. **CRM / PartnerHub (MongoDB)**:
   * Partner profiles (`mongo_partner_profiles`) and advertiser accounts (`mongo_advertiser_accounts`).
3. **Event Stream (Kafka)**:
   * Real-time conversion event stream (`kafka_pulsetrack_conversions`).

---

## 📂 Repository Structure

```
├── DE01Capstone/
│   ├── data/                 # Raw source CSV extracts (11 tables)
│   ├── ddl/                  # Raw schema definition (pulsetrack_source_schema.sql)
│   ├── mongo/                # MongoDB JSONL extracts
│   ├── stream/               # Kafka conversion event log dump
│   ├── PulseTrack_Data_Guide.docx
│   └── pulsetrack_er_diagram.png
├── dags/
│   └── sql/
│       └── bronze/
│           ├── 00_create_bronze_schema_and_tables.sql  # DDL for bronze layer with audit metadata
│           └── full_load/                             # Airflow extraction SQLs for all 14 tables
├── import_kafka_raw.py       # Kafka log to PostgreSQL raw ingestion script
├── import_mongodb_raw.py     # MongoDB JSONL to PostgreSQL raw ingestion script
├── check_kafka_line.py       # Helper inspection utility
├── .gitattributes            # Line ending preservation for binary/raw data
└── .gitignore                # Git ignore rules for Python, Airflow, and logs
```

---

## 🚀 Bronze Layer Loading

The Bronze layer ingests raw data as-is with ingestion metadata (`load_timestamp`).

* **DDL Definition**: `dags/sql/bronze/00_create_bronze_schema_and_tables.sql`
* **Full Load Queries**: `dags/sql/bronze/full_load/*.sql`
