"""
serving_layer/serving_api.py
─────────────────────────────
The Lambda Architecture SERVING LAYER.

Combines:
- Speed layer (real-time data)
- Batch layer (historical data)

Provides HTTP API endpoints for querying Dublin Bus analytics.

Endpoints:
  GET /                     — API homepage
  GET /health               — health check
  GET /routes/top-delayed   — top delayed routes
  GET /routes/<route_id>    — route details
  GET /anomalies            — current anomalies
  GET /benchmark            — benchmark results
  GET /summary              — system summary

Run:
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
    DYNAMO_SPEED_TABLE,
    DYNAMO_BATCH_TABLE,
    DYNAMO_SERVING_TABLE,
    S3_BUCKET_NAME,
    S3_BATCH_OUTPUT_PREFIX,
    TOP_N_ROUTES
)

from utils.aws_utils import (
    get_dynamodb_resource,
    get_athena_client
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


app = Flask(__name__)


# AWS resources

dynamo = get_dynamodb_resource()

speed_table = dynamo.Table(DYNAMO_SPEED_TABLE)

batch_table = dynamo.Table(DYNAMO_BATCH_TABLE)

serving_table = dynamo.Table(DYNAMO_SERVING_TABLE)



@app.after_request
def add_cors_headers(response):
    """
    Allow dashboard/frontend requests.
    """

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = (
        "Content-Type, Authorization"
    )
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"

    return response



def decimal_to_float(obj):
    """
    Convert DynamoDB Decimal values into JSON compatible floats.
    """

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, dict):
        return {
            key: decimal_to_float(value)
            for key, value in obj.items()
        }

    if isinstance(obj, list):
        return [
            decimal_to_float(item)
            for item in obj
        ]

    return obj



# ==========================
# Homepage
# ==========================

@app.route("/")
def home():

    return jsonify({

        "service": "Dublin Bus Analytics API",

        "status": "running",

        "available_endpoints": [

            "/health",

            "/summary",

            "/routes/top-delayed",

            "/routes/<route_id>",

            "/anomalies",

            "/benchmark"

        ]

    })



# ==========================
# DynamoDB Queries
# ==========================


def get_latest_speed_snapshot():

    try:

        response = speed_table.query(

            KeyConditionExpression=
            Key("route_id").eq("__TOP_N_SNAPSHOT__"),

            ScanIndexForward=False,

            Limit=1
        )


        items = response.get("Items", [])


        if items:

            return json.loads(
                items[0].get("top_routes", "[]")
            )


    except Exception as e:

        logger.error(
            f"Speed snapshot fetch failed: {e}"
        )


    return []



def get_speed_view_for_route(route_id):

    try:

        response = speed_table.query(

            KeyConditionExpression=
            Key("route_id").eq(route_id),

            ScanIndexForward=False,

            Limit=1

        )


        items = response.get("Items", [])


        return (
            decimal_to_float(items[0])
            if items
            else None
        )


    except Exception as e:

        logger.error(
            f"Speed query failed: {e}"
        )

        return None



def get_batch_view_for_route(route_id):

    now = datetime.datetime.utcnow()


    time_bucket = (
        f"{now.strftime('%A')}_{now.hour:02d}"
    )


    try:

        response = batch_table.get_item(

            Key={

                "route_id": route_id,

                "time_bucket": time_bucket

            }

        )


        item = response.get("Item")


        return (
            decimal_to_float(item)
            if item
            else None
        )


    except Exception as e:

        logger.error(
            f"Batch query failed: {e}"
        )

        return None



def merge_views(route_id, speed, batch):

    result = {

        "route_id": route_id,

        "merged_at":
        datetime.datetime.utcnow().isoformat()+"Z",

        "speed_layer": speed,

        "batch_layer": batch,

        "verdict": None,

        "delta_vs_historical": None

    }


    if speed and batch:


        live_delay = speed.get(
            "avg_delay_seconds",
            0
        )


        historical_delay = batch.get(
            "avg_delay_seconds",
            0
        )


        delta = (
            live_delay -
            historical_delay
        )


        result["delta_vs_historical"] = round(
            delta,
            1
        )


        if delta > 120:

            result["verdict"] = (
                "SIGNIFICANTLY_WORSE_THAN_USUAL"
            )


        elif delta > 60:

            result["verdict"] = (
                "WORSE_THAN_USUAL"
            )


        elif delta < -60:

            result["verdict"] = (
                "BETTER_THAN_USUAL"
            )


        else:

            result["verdict"] = "NORMAL"



    elif speed:

        result["verdict"] = (
            "LIVE_ONLY_NO_HISTORY"
        )


    elif batch:

        result["verdict"] = (
            "HISTORICAL_ONLY_NO_LIVE_DATA"
        )


    return result



# ==========================
# API Routes
# ==========================


@app.route("/health")
def health():

    return jsonify({

        "status": "ok",

        "timestamp":
        datetime.datetime.utcnow().isoformat()

    })



@app.route("/summary")
def summary():

    top = get_latest_speed_snapshot()


    return jsonify({

        "as_of":
        datetime.datetime.utcnow().isoformat()+"Z",

        "live_routes_tracked":
        len(top),

        "top_route":
        top[0] if top else None,

        "system_status":
        "operational"

    })



@app.route("/routes/top-delayed")
def top_delayed():

    n = request.args.get(
        "n",
        TOP_N_ROUTES,
        type=int
    )


    routes = get_latest_speed_snapshot()[:n]


    return jsonify({

        "as_of":
        datetime.datetime.utcnow().isoformat()+"Z",

        "top_delayed_routes":
        routes

    })



@app.route("/routes/<route_id>")
def route_detail(route_id):

    speed = get_speed_view_for_route(route_id)

    batch = get_batch_view_for_route(route_id)


    return jsonify(
        merge_views(
            route_id,
            speed,
            batch
        )
    )



@app.route("/anomalies")
def anomalies():

    try:

        response = speed_table.scan()

        return jsonify({

            "count":
            len(response.get("Items", [])),

            "items":
            decimal_to_float(
                response.get("Items", [])
            )

        })


    except Exception as e:

        return jsonify({

            "error": str(e)

        }), 500



@app.route("/benchmark")
def benchmark():

    return jsonify({

        "note":
        "Benchmark results are generated by batch processing",

        "location":
        f"s3://{S3_BUCKET_NAME}/{S3_BATCH_OUTPUT_PREFIX}benchmark/"

    })



# ==========================
# Start Server
# ==========================

if __name__ == "__main__":

    logger.info(
        "🚀 Serving API starting on http://0.0.0.0:8080"
    )

    app.run(
        host="0.0.0.0",
        port=8080,
        debug=False
    )