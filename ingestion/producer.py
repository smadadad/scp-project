"""
ingestion/producer.py
─────────────────────
Polls the TFI GTFS-Realtime API every POLL_INTERVAL_SECONDS,
parses protobuf TripUpdates into JSON records, and streams them
into Amazon Kinesis Data Streams.

Usage:
    python ingestion/producer.py

Dependencies:
    pip install requests gtfs-realtime-bindings boto3 protobuf
"""

import json
import time
import logging
import datetime
import sys
import os
import requests

from google.transit import gtfs_realtime_pb2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import (
    TFI_API_KEY, TFI_TRIP_UPDATES_URL, TFI_VEHICLE_POSITIONS_URL,
    KINESIS_STREAM_NAME, POLL_INTERVAL_SECONDS
)
from utils.aws_utils import get_kinesis_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

kinesis = get_kinesis_client()


def fetch_trip_updates() -> list[dict]:
    """Fetch TFI TripUpdates protobuf and parse into list of delay records."""
    headers = {
        "x-api-key": TFI_API_KEY,
        "Cache-Control": "no-cache"
    }
    try:
        response = requests.get(TFI_TRIP_UPDATES_URL, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch TripUpdates: {e}")
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    ingestion_time = datetime.datetime.utcnow().isoformat() + "Z"

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip = entity.trip_update.trip
        route_id = trip.route_id
        trip_id = trip.trip_id
        start_date = trip.start_date
        schedule_relationship = trip.schedule_relationship

        for stop_time_update in entity.trip_update.stop_time_update:
            delay_seconds = None

            if stop_time_update.HasField("departure"):
                delay_seconds = stop_time_update.departure.delay
            elif stop_time_update.HasField("arrival"):
                delay_seconds = stop_time_update.arrival.delay

            if delay_seconds is None:
                continue

            record = {
                "event_id": f"{entity.id}_{stop_time_update.stop_sequence}",
                "ingestion_time": ingestion_time,
                "route_id": route_id,
                "trip_id": trip_id,
                "stop_sequence": stop_time_update.stop_sequence,
                "stop_id": stop_time_update.stop_id,
                "delay_seconds": delay_seconds,
                "start_date": start_date,
                "schedule_relationship": schedule_relationship,
                "source": "tfi_trip_updates"
            }
            records.append(record)

    logger.info(f"Parsed {len(records)} stop delay records from TripUpdates")
    return records


def fetch_vehicle_positions() -> list[dict]:
    """Fetch live vehicle positions and merge into records."""
    headers = {"x-api-key": TFI_API_KEY}
    try:
        response = requests.get(TFI_VEHICLE_POSITIONS_URL, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Vehicle positions unavailable: {e}")
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    records = []
    ingestion_time = datetime.datetime.utcnow().isoformat() + "Z"

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        records.append({
            "event_id": f"vp_{entity.id}",
            "ingestion_time": ingestion_time,
            "route_id": v.trip.route_id,
            "trip_id": v.trip.trip_id,
            "vehicle_id": v.vehicle.id,
            "latitude": v.position.latitude,
            "longitude": v.position.longitude,
            "bearing": v.position.bearing,
            "speed_ms": v.position.speed,
            "occupancy_status": v.occupancy_status,
            "source": "tfi_vehicle_positions"
        })

    logger.info(f"Parsed {len(records)} vehicle position records")
    return records


def put_records_to_kinesis(records: list[dict]):
    """Batch-put records into Kinesis (max 500 per call)."""
    if not records:
        return

    batch_size = 500
    total_sent = 0
    total_failed = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        kinesis_records = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": r.get("route_id", "unknown") or "unknown"
            }
            for r in batch
        ]

        try:
            response = kinesis.put_records(
                Records=kinesis_records,
                StreamName=KINESIS_STREAM_NAME
            )
            failed = response.get("FailedRecordCount", 0)
            sent = len(batch) - failed
            total_sent += sent
            total_failed += failed

            if failed > 0:
                logger.warning(f"{failed} records failed to write to Kinesis")

        except Exception as e:
            logger.error(f"Kinesis put_records failed: {e}")

    logger.info(f"Kinesis: {total_sent} records written, {total_failed} failed")


def run_producer(max_iterations: int = None):
    """
    Main polling loop. Runs indefinitely unless max_iterations is set
    (useful for testing).
    """
    logger.info(f"🚀 Producer started — polling TFI every {POLL_INTERVAL_SECONDS}s")
    logger.info(f"   Stream: {KINESIS_STREAM_NAME}")

    iteration = 0
    while True:
        iteration += 1
        logger.info(f"── Poll #{iteration} at {datetime.datetime.utcnow().isoformat()}Z ──")

        trip_records = fetch_trip_updates()
        vehicle_records = fetch_vehicle_positions()
        all_records = trip_records + vehicle_records

        put_records_to_kinesis(all_records)

        if max_iterations and iteration >= max_iterations:
            logger.info("Max iterations reached — stopping producer")
            break

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_producer()
