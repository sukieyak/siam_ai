# Automated Data Ingestion & Preparation Pipeline for AI Training

## Overview

This project is an automated data ingestion and preparation pipeline designed for AI training workflows.

The pipeline supports:

* Dataset ingestion from MinIO
* JSONL cleaning and preprocessing
* Airflow orchestration
* Monitoring with Prometheus + Grafana
* AI-ready dataset preparation

The system was developed and tested in a local VM environment.

---

# Project Structure

```bash
project/
│
├── dags/
├── docker-compose.yaml
├── Dockerfile.airflow
├── prometheus.yml
├── requirements.txt
├── .env
├── airflow.env
└── README.md
```

---

# Important Notes

⚠️ This repository contains MY LOCAL VM configuration.

Before running on another VM, you MUST update:

* IP addresses
* volume mount paths
* MinIO endpoint
* credentials
* local directories

to match your own environment.

---

# Prerequisites

Please install:

* Docker
* Docker Compose
* Python 3.10+
* MinIO

---

# Environment Setup

## 1. Create `.env`

Example:

```env
MINIO_ENDPOINT=http://YOUR_VM_IP:9010
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
```

---

# Docker Setup

## 2. Update `docker-compose.yaml`

Please change:

* local volume mount paths
* ports if conflicts exist
* VM IP references

Example:

```yaml
volumes:
  - /your/local/path/dags:/opt/airflow/dags
```

---

# Start Services

## Build Containers

```bash
docker compose build
```

## Start Containers

```bash
docker compose up -d
```

## Check Running Containers

```bash
docker ps
```

---

# Service URLs

## Airflow

```bash
http://YOUR_VM_IP:8080
```

## MinIO

```bash
http://YOUR_VM_IP:9010
```

## Grafana

```bash
http://YOUR_VM_IP:3000
```

## Prometheus

```bash
http://YOUR_VM_IP:9090
```

---

# Python Dependencies

Install manually if needed:

```bash
pip install -r requirements.txt
```

Main packages:

* minio
* beautifulsoup4
* lxml
* pythainlp
* transformers
* torch
* psycopg

---

# Running Cleaning Script Manually

Example:

```bash
python minio_cleaner.py \
  --bucket ai-datasets \
  --src-key raw/nectec/sample.jsonl \
  --out-clean-key cleaned/sample.cleaned.jsonl \
  --out-logs-key logs/sample.logs.jsonl
```

---

# Cleaning Pipeline Features

The cleaning script includes:

* Unicode normalization
* HTML tag removal
* whitespace normalization
* Thai language ratio validation
* duplicate removal
* checksum generation

---

# Dataset Structure

Example datasets:

```bash
raw/nectec/chillwithmeblog_com/
raw/nectec/fineweb2/
raw/nectec/wongnai/
```

Output structure:

```bash
cleaned/
logs/
processed/
```

---

# Monitoring

The project includes monitoring for:

## Airflow Monitoring

* DAG success/failure
* task duration
* retries
* ETL latency

## MinIO Monitoring

* bucket size
* upload/download rate
* storage usage

## Data Quality Monitoring

* null ratio
* schema validation
* filtered records
* anomaly detection

---

# Common Issues

## Port Already In Use

Check ports:

* 8080
* 3000
* 9090
* 9010

Change ports in `docker-compose.yaml`.

---

## DAG Not Appearing

Restart Airflow services:

```bash
docker compose restart airflow-webserver
docker compose restart airflow-scheduler
```

---

## MinIO Connection Failed

Verify:

* endpoint IP
* credentials
* network access
* firewall rules

---

## Permission Denied

Fix permissions:

```bash
sudo chmod -R 777 ./logs
sudo chmod -R 777 ./dags
```

---

# Things You MUST Change For Your VM

Before deployment:

* VM IP address
* mounted local paths
* MinIO credentials
* Docker ports
* dataset storage paths

---

# Future Improvements

Possible future improvements:

* migrate standalone scripts into full Airflow DAGs
* add Kafka streaming ingestion
* add schema registry
* add Great Expectations validation
* improve OCR/audio pipeline

---

# Final Notes

This project was developed for the internship project:
"Data Ingestion & Preparation Pipeline for AI Training"

Please adapt configurations based on your own infrastructure setup before deployment.
