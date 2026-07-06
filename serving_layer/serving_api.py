"""
serving_layer/serving_api.py
─────────────────────────────
The Lambda Architecture SERVING LAYER.

Merges the batch view (historical accuracy) with the speed view (real-time
freshness) into a combined query response — answering:

  "Which routes are most delayed right now, and is that worse than usual?"

Exposes a lightweight HTTP API using Flask (can be run on EC2 or as a
Lambda + API Gateway).

Endpoints:
  GET /health                 — health check
  GET /routes/top-delayed     — top N delayed routes (merged view)
  GET /routes/<route_id>      — detail for a specific route
  GET /anomalies              — routes currently performing worse than history
  GET /benchmark              — latest batch job benchmark results

Usage:
    pip install flask boto3
    python serving_layer/serving_api.py
"""

import json
import logging
import datetime
import sys
import os

from flask import Flask, jsonify, request
from boto3.dynamodb.conditions import Key
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import (
    DYNAMO_SPEED_TABLE, DYNAMO_BATCH_TABLE, DYNAMO_SERVING_TABLE,
    S3_BUCKET_NAME, S3_BATCH_OUTPUT_PREFIX, TOP_N_ROUTES
)
from utils.aws_utils import get_dynamodb_resource, get_athena_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
dynamo = get_dynamodb_resource()
speed_table = dynamo.Table(DYNAMO_SPEED_TABLE)
batch_table = dynamo.Table(DYNAMO_BATCH_TABLE)
serving_table = dynamo.Table(DYNAMO_SERVING_TABLE)


@app.after_request
def add_cors_headers(response):
    """Allow the local dashboard page to call the API from a file:// origin."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return response


# ─── Helpers ───────────────────────────────────────────────────────────────────

def decimal_to_float(obj):
    """Recursively convert Decimal to float for JSON serialisation."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    return obj


def get_latest_speed_snapshot() -> list[dict]:
    """Fetch the most recent top-N snapshot from the speed layer."""
    try:
        response = speed_table.query(
            KeyConditionExpression=Key("route_id").eq("__TOP_N_SNAPSHOT__"),
            ScanIndexForward=False,
            Limit=1
        )
        items = response.get("Items", [])
        if items:
            top_routes = json.loads(items[0].get("top_routes", "[]"))
            return top_routes
    except Exception as e:
        logger.error(f"Speed snapshot fetch failed: {e}")
    return []


def get_speed_view_for_route(route_id: str) -> dict | None:
    """Get the most recent speed-layer record for a route."""
    try:
        response = speed_table.query(
            KeyConditionExpression=Key("route_id").eq(route_id),
            ScanIndexForward=False,
            Limit=1
        )
        items = response.get("Items", [])
        return decimal_to_float(items[0]) if items else None
    except Exception as e:
        logger.error(f"Speed view query failed for {route_id}: {e}")
        return None


def get_batch_view_for_route(route_id: str) -> dict | None:
    """Get batch-layer historical baseline for the current time bucket."""
    now = datetime.datetime.utcnow()
    time_bucket = f"{now.strftime('%A')}_{now.hour:02d}"
    try:
        response = batch_table.get_item(
            Key={"route_id": route_id, "time_bucket": time_bucket}
        )
        item = response.get("Item")
        return decimal_to_float(item) if item else None
    except Exception as e:
        logger.error(f"Batch view query failed for {route_id}: {e}")
        return None


def merge_views(route_id: str, speed: dict | None, batch: dict | None) -> dict:
    """
    Core Lambda merge logic:
      - Speed view gives freshness (what's happening RIGHT NOW)
      - Batch view gives correctness (what's HISTORICALLY NORMAL)
      - Merged view answers: is now better or worse than usual?
    """
    merged = {
        "route_id": route_id,
        "merged_at": datetime.datetime.utcnow().isoformat() + "Z",
        "speed_layer": speed,
        "batch_layer": batch,
        "verdict": None,
        "delta_vs_historical": None,
    }

    if speed and batch:
        live_delay = speed.get("avg_delay_seconds", 0)
        hist_delay = batch.get("avg_delay_seconds", 0)
        delta = live_delay - hist_delay
        merged["delta_vs_historical"] = round(delta, 1)

        if delta > 120:
            merged["verdict"] = "SIGNIFICANTLY_WORSE_THAN_USUAL"
        elif delta > 60:
            merged["verdict"] = "WORSE_THAN_USUAL"
        elif delta < -60:
            merged["verdict"] = "BETTER_THAN_USUAL"
        else:
            merged["verdict"] = "NORMAL"

    elif speed and not batch:
        merged["verdict"] = "LIVE_ONLY_NO_HISTORY"
    elif batch and not speed:
        merged["verdict"] = "HISTORICAL_ONLY_NO_LIVE_DATA"

    return merged


# ─── Endpoints ─────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": datetime.datetime.utcnow().isoformat()})


@app.route("/routes/top-delayed")
def top_delayed():
    """Top N most delayed routes right now (speed layer snapshot + batch context)."""
    n = request.args.get("n", TOP_N_ROUTES, type=int)
    top_routes = get_latest_speed_snapshot()[:n]

    enriched = []
    for route in top_routes:
        route_id = route["route_id"]
        batch = get_batch_view_for_route(route_id)
        if batch:
            route["historical_avg_delay"] = batch.get("avg_delay_seconds")
            delta = route["avg_delay_seconds"] - (batch.get("avg_delay_seconds") or 0)
            route["delta_vs_historical"] = round(delta, 1)
        enriched.append(route)

    return jsonify({
        "as_of": datetime.datetime.utcnow().isoformat() + "Z",
        "window_minutes": 5,
        "top_delayed_routes": enriched
    })


@app.route("/routes/<route_id>")
def route_detail(route_id: str):
    """Full merged view for a specific route."""
    speed = get_speed_view_for_route(route_id)
    batch = get_batch_view_for_route(route_id)
    merged = merge_views(route_id, speed, batch)
    return jsonify(merged)


@app.route("/anomalies")
def anomalies():
    """Routes currently performing significantly worse than their historical average."""
    try:
        # Scan speed table for anomaly flags (small table — TTL keeps it bounded)
        response = speed_table.scan(
            FilterExpression="is_anomaly = :true",
            ExpressionAttributeValues={":true": True}
        )
        anomaly_routes = sorted(
            decimal_to_float(response.get("Items", [])),
            key=lambda x: x.get("anomaly_ratio", 0),
            reverse=True
        )
        return jsonify({
            "as_of": datetime.datetime.utcnow().isoformat() + "Z",
            "anomaly_count": len(anomaly_routes),
            "anomalies": anomaly_routes
        })
    except Exception as e:
        logger.error(f"Anomaly scan failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/benchmark")
def benchmark():
    """Return the latest batch job speedup benchmark from S3/Athena."""
    athena = get_athena_client()
    query = f"""
    SELECT sequential_seconds, parallel_seconds, speedup_ratio, total_records
    FROM {S3_BUCKET_NAME}.benchmark
    ORDER BY 1 DESC LIMIT 1
    """
    # Note: in production, query Athena properly with polling.
    # For the demo, return a static placeholder.
    return jsonify({
        "note": "Run submit_batch_job.py to populate benchmark results",
        "benchmark_output_location": f"s3://{S3_BUCKET_NAME}/{S3_BATCH_OUTPUT_PREFIX}benchmark/"
    })


@app.route("/summary")
def summary():
    """High-level system summary for the dashboard."""
    top = get_latest_speed_snapshot()
    return jsonify({
        "as_of": datetime.datetime.utcnow().isoformat() + "Z",
        "live_routes_tracked": len(top),
        "top_route": top[0] if top else None,
        "system_status": "operational"
    })


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("🚀 Serving API starting on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
