# 🚌 Dublin Bus Real-Time Delay Analytics Platform

**NCI MSc Cloud Computing — Scalable Cloud Programming CA**  
Lambda Architecture · AWS · PySpark · Kinesis · Real-Time Analytics

---

## Architecture Overview

```
TFI GTFS-Realtime API (polled every 30s)
              │
              ▼
     ┌─────────────────┐
     │  Kinesis Data   │  ← ingestion/producer.py
     │    Streams      │
     └────────┬────────┘
              │
     ┌────────┴────────────────────────────────┐
     │                                         │
     ▼                                         ▼
┌──────────────────────┐         ┌──────────────────────────┐
│   SPEED LAYER        │         │   BATCH LAYER            │
│                      │         │                          │
│  stream_processor.py │         │  s3_consumer.py          │
│  lambda_handler.py   │         │  → S3 raw JSON-lines     │
│                      │         │  → EMR + PySpark         │
│  Sliding window 5min │         │  → spark_batch_job.py    │
│  Top-N delayed routes│         │  → S3 Parquet (Athena)   │
│  Anomaly detection   │         │                          │
│       │              │         │       │                  │
│       ▼              │         │       ▼                  │
│  DynamoDB            │         │  DynamoDB                │
│  (speed-view)        │         │  (batch-view)            │
└──────────┬───────────┘         └───────────┬──────────────┘
           │                                 │
           └──────────────┬──────────────────┘
                          ▼
               ┌─────────────────────┐
               │   SERVING LAYER     │
               │   serving_api.py    │
               │   Flask REST API    │
               │   Lambda MERGE      │
               └──────────┬──────────┘
                          ▼
               ┌─────────────────────┐
               │   DASHBOARD         │
               │   dashboard/        │
               │   index.html        │
               └─────────────────────┘
```

---

## Project Structure

```
dublin-bus-analytics/
├── config/
│   └── config.py              # Loads runtime settings from environment/.env
├── .env.example               # Template for required local environment variables
├── .gitignore                 # Keeps .env and local secrets out of Git
├── ingestion/
│   ├── producer.py            # Polls TFI API → Kinesis
│   └── s3_consumer.py         # Kinesis → S3 raw store
├── batch_layer/
│   ├── spark_batch_job.py     # PySpark MapReduce job (runs on EMR)
│   └── submit_batch_job.py    # Submits Spark job as EMR Step
├── speed_layer/
│   ├── stream_processor.py    # Long-running sliding window processor
│   └── lambda_handler.py      # AWS Lambda version (Kinesis trigger)
├── serving_layer/
│   ├── serving_api.py         # Flask API — merges batch + speed views
│   └── benchmark.py           # Performance benchmarking + chart generation
├── dashboard/
│   └── index.html             # Real-time dashboard (static HTML/JS)
├── infrastructure/
│   └── setup_aws.py           # Provisions all AWS resources
├── utils/
│   └── aws_utils.py           # Boto3 client helpers
├── run_pipeline.py            # Convenience runner for all components
└── requirements.txt
```

---

## Quick Start

### 1. Prerequisites

```bash
pip install -r requirements.txt
```

Register for a free TFI API key:
→ https://developer.nationaltransport.ie

### 2. Configure

Create a local `.env` file in the project root. The file is ignored by Git, so it will not be committed.

The easiest way is to copy the provided template:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Then fill in the required values in `.env`:

```env
TFI_API_KEY=your_tfi_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_SESSION_TOKEN=your_aws_session_token
AWS_REGION=us-east-1
KINESIS_STREAM_NAME=dublin-bus-stream
S3_BUCKET_NAME=dublin-bus-analytics-bucket-x24101001
```

The application reads these values automatically from `config/config.py`.

Required values for the pipeline to run:
- `TFI_API_KEY` — your TFI API key
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` — from your AWS account or AWS Academy Learner Lab
- `S3_BUCKET_NAME` — must be globally unique (use a custom suffix if needed)

### 3. Provision AWS Infrastructure

```bash
python run_pipeline.py --setup
```

This creates:
- S3 bucket (with versioning)
- Kinesis Data Stream (2 shards)
- DynamoDB tables (speed, batch, serving views)
- Athena database + external table

### 4. Launch the Pipeline

```bash
# Start all components (4 processes)
     python run_pipeline.py --all  

# Or individually:
python run_pipeline.py --producer    # Start data ingestion
python run_pipeline.py --consumer    # Buffer raw data to S3
python run_pipeline.py --speed       # Start speed layer
python run_pipeline.py --api         # Start serving API on :5000
```

### 5. Open the Dashboard

Open `dashboard/index.html` in a browser (update `API_BASE` to your server URL).

### 6. Submit Batch Job to EMR

```bash
# First launch EMR cluster (from infrastructure/setup_aws.py)
python -c "from infrastructure.setup_aws import create_emr_cluster; print(create_emr_cluster())"

# Then submit the Spark job
python batch_layer/submit_batch_job.py --cluster-id j-XXXXXXX
```

### 7. Run Benchmarks

```bash
# After the pipeline has been running for a while
python serving_layer/benchmark.py --api-url http://localhost:5000

# Charts are saved to benchmark_results/
```

---

## AWS Services Used

| Service | Role |
|---|---|
| **Kinesis Data Streams** | Real-time event ingestion |
| **S3** | Raw data store + Parquet batch output |
| **EMR + PySpark** | Batch MapReduce processing |
| **Athena** | SQL queries over batch Parquet |
| **DynamoDB** | Speed layer + serving layer key-value store |
| **AWS Lambda** | Serverless speed layer (alternative to EC2) |
| **EC2 Auto Scaling** | Elastic compute for EMR worker nodes |
| **CloudWatch** | Scaling triggers + monitoring |

---

## Lambda Architecture Mapping

| Component | Lambda Role | Implementation |
|---|---|---|
| TFI GTFS-RT + Kinesis | **Ingestion** | `ingestion/producer.py` |
| S3 + EMR PySpark | **Batch Layer** | `batch_layer/spark_batch_job.py` |
| Sliding Window + DynamoDB | **Speed Layer** | `speed_layer/stream_processor.py` |
| Flask API (merge) | **Serving Layer** | `serving_layer/serving_api.py` |
| HTML Dashboard | **View** | `dashboard/index.html` |

---

## Real-Time Questions Answered

- **"Which Dublin Bus routes are most delayed right now?"** → Speed layer, 5-min window
- **"Is today's delay worse than the historical average?"** → Serving layer merge
- **"Which routes are chronically unreliable?"** → Batch layer (full history)
- **"Which routes are currently anomalous?"** → Anomaly detection (speed > 2× batch baseline)

---

## Performance Targets

| Metric | Target |
|---|---|
| Ingestion latency | < 5 seconds |
| Speed layer update interval | 30 seconds |
| Batch job speedup (8 partitions) | > 4× sequential |
| Serving API p95 latency | < 200ms |
| Kinesis throughput | > 200 records/sec |

---

## GitHub Repository

Upload this entire folder to GitHub and include the URL in your IEEE report.

```bash
git init
git add .
git commit -m "Dublin Bus Analytics — NCI MSc Cloud Computing CA"
git remote add origin https://github.com/YOUR_USERNAME/dublin-bus-analytics.git
git push -u origin main
```
