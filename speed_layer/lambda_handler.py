"""
speed_layer/lambda_handler.py
──────────────────────────────
AWS Lambda function — triggered by Kinesis Data Streams event source mapping.

Processes each batch of Kinesis records serverlessly:
  - Aggregates delay per route in this batch
  - Detects anomalies vs batch baseline in DynamoDB
  - Writes incremental results to DynamoDB speed view

Deploy via:
    zip lambda.zip speed_layer/lambda_handler.py config/config.py utils/aws_utils.py
    aws lambda create-function \
        --function-name dublin-bus-speed-processor \
        --runtime python3.11 \
        --handler speed_layer/lambda_handler.lambda_handler \
        --zip-file fileb://lambda.zip \
        --role arn:aws:iam::<ACCOUNT>:role/LambdaKinesisDynamoRole

    # Add Kinesis trigger:
    aws lambda create-event-source-mapping \
        --function-name dublin-bus-speed-processor \
        --event-source-arn arn:aws:kinesis:<REGION>:<ACCOUNT>:stream/dublin-bus-stream \
        --starting-position LATEST \
        --batch-size 100 \
        --tumbling-window-in-seconds 60
"""

import json
import base64
import logging
import os
import datetime
import time
from collections import defaultdict
from decimal import Decimal

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMO_SPEED_TABLE = os.environ.get("DYNAMO_SPEED_TABLE", "dublin-bus-speed-view")
DYNAMO_BATCH_TABLE = os.environ.get("DYNAMO_BATCH_TABLE", "dublin-bus-batch-view")
TOP_N = int(os.environ.get("TOP_N_ROUTES", "5"))
ANOMALY_THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD", "2.0"))

dynamo = boto3.resource("dynamodb")
speed_table = dynamo.Table(DYNAMO_SPEED_TABLE)
batch_table = dynamo.Table(DYNAMO_BATCH_TABLE)

def get_batch_baseline(route_id: str) -> float | None:
    now = datetime.datetime.utcnow()
    time_bucket = f"{now.strftime('%A')}_{now.hour:02d}"
    try:
        resp = batch_table.get_item(Key={"route_id": route_id, "time_bucket": time_bucket})
        item = resp.get("Item")
        if item:
            return float(item.get("avg_delay_seconds", 0))
    except Exception as e:
        logger.warning(f"Baseline lookup failed for {route_id}: {e}")
    return None


def lambda_handler(event, context):
    """
    Entry point for Kinesis-triggered Lambda.
    event["Records"] contains Kinesis records in this batch.
    """
    window_end = datetime.datetime.utcnow().isoformat() + "Z"

    # Aggregate delays by route in this Lambda batch
    route_delays: dict[str, list[int]] = defaultdict(list)

    for record in event.get("Records", []):
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode("utf-8"))
            route_id = payload.get("route_id")
            delay = payload.get("delay_seconds")
            source = payload.get("source", "")

            if route_id and delay is not None and source == "tfi_trip_updates":
                route_delays[route_id].append(int(delay))
        except Exception as e:
            logger.warning(f"Failed to parse record: {e}")

    if not route_delays:
        logger.info("No valid delay records in this batch")
        return {"statusCode": 200, "processed": 0}

    # Write per-route stats to DynamoDB
    with speed_table.batch_writer() as batch:
        for route_id, delays in route_delays.items():
            avg_delay = sum(delays) / len(delays)
            baseline = get_batch_baseline(route_id)
            is_anomaly = False
            ratio = None

            if baseline and baseline > 0:
                ratio = avg_delay / baseline
                is_anomaly = ratio > ANOMALY_THRESHOLD

            item = {
                "route_id": route_id,
                "window_end": window_end,
                "avg_delay_seconds": Decimal(str(round(avg_delay, 2))),
                "max_delay_seconds": Decimal(str(max(delays))),
                "event_count": len(delays),
                "late_count": sum(1 for d in delays if d > 60),
                "is_anomaly": is_anomaly,
                "ttl": int(time.time()) + 3600
            }
            if ratio is not None:
                item["anomaly_ratio"] = Decimal(str(round(ratio, 2)))
            if baseline is not None:
                item["batch_baseline_seconds"] = Decimal(str(round(baseline, 2)))

            batch.put_item(Item=item)

    logger.info(f"✅ Processed {sum(len(v) for v in route_delays.values())} records "
                f"across {len(route_delays)} routes")

    return {
        "statusCode": 200,
        "processed": len(event.get("Records", [])),
        "routes_updated": len(route_delays)
    }
