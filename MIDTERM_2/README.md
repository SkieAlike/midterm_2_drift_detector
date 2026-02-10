# Real-Time Drift Detection System

## System Overview

This project implements an **end-to-end real-time drift detection system** for telemetry metrics.  
It simulates how production monitoring systems detect **behavioral changes (drift)** in incoming data streams using historical baselines, rolling statistics, and sustained decision logic.

The system ingests events via Kafka, processes them in real time, stores raw and derived data in ClickHouse, manages configuration and baselines in PostgreSQL, exposes results via an API, and periodically updates historical baselines using Airflow.

The goal is to demonstrate **architecture, data flow, and drift detection logic**, not to build a fully production-hardened monitoring platform.

---

## High-Level Architecture

Kafka Producers
│

▼

Kafka (telemetry.events)

│

▼

ClickHouse (raw_events)

│

├── Airflow (baseline aggregation → PostgreSQL)

│

▼

Drift Detector Service

│

├── PostgreSQL (thresholds, baselines, source metadata)

├── ClickHouse (drift_events, drift_status_current)

│

▼

FastAPI (read-only monitoring API)




---

## Core Components

### 1. Kafka Producers

Three types of producers simulate different real-world behaviors:

- **Stable Producer**
  - Emits values with stable mean and variance
- **Dynamic Producer**
  - Emits values with slowly shifting distributions
- **Drift Producer**
  - Intentionally injects sudden distribution changes

Each event contains:
- `source_id`
- `metric`
- `value`
- `event_time` (ISO-8601)

All events are written to a single Kafka topic: telemetry.events

### 2. ClickHouse Storage

ClickHouse is used for **high-throughput ingestion** and **analytical queries**.

#### Raw Events Table

```sql
raw_events

