"""
ingestion/s3_consumer.py
────────────────────────
Reads from Kinesis Data Streams and writes raw records to S3
in JSON-lines format, partitioned by date/hour.

This feeds the batch layer's historical store.

Usage:
    python ingestion/s3_consumer.py
"""

import json
import time
import logging
import datetime
import sys
import os
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import (
    KINESIS_STREAM_NAME, S3_BUCKET_NAME, S3_RAW_PREFIX
)
from utils.aws_utils import get_kinesis_client, get_s3_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

kinesis = get_kinesis_client()
s3 = get_s3_client()

FLUSH_INTERVAL_SECONDS = 60    # Write to S3 every 60 seconds
BUFFER: list[dict] = []


def get_shard_iterators() -> list[str]:
    """Get LATEST shard iterators for all shards in the stream."""
    response = kinesis.describe_stream(StreamName=KINESIS_STREAM_NAME)
    shards = response["StreamDescription"]["Shards"]
    iterators = []
    for shard in shards:
        resp = kinesis.get_shard_iterator(
            StreamName=KINESIS_STREAM_NAME,
            ShardId=shard["ShardId"],
            ShardIteratorType="LATEST"
        )
        iterators.append(resp["ShardIterator"])
    logger.info(f"Got iterators for {len(iterators)} shards")
    return iterators


def read_from_shard(shard_iterator: str) -> tuple[list[dict], str]:
    """Read up to 100 records from a shard. Returns (records, next_iterator)."""
    try:
        response = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
        records = []
        for record in response.get("Records", []):
            try:
                data = json.loads(record["Data"].decode("utf-8"))
                data["_kinesis_sequence"] = record["SequenceNumber"]
                data["_kinesis_arrival"] = record["ApproximateArrivalTimestamp"].isoformat()
                records.append(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to decode record: {e}")
        return records, response.get("NextShardIterator", "")
    except Exception as e:
        logger.error(f"Error reading from shard: {e}")
        return [], shard_iterator


def flush_buffer_to_s3(buffer: list[dict]):
    """Write buffered records to S3 as a JSON-lines file, partitioned by date/hour."""
    if not buffer:
        return

    now = datetime.datetime.utcnow()
    partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    filename = f"{now.strftime('%Y%m%d_%H%M%S')}_{len(buffer)}_records.jsonl"
    s3_key = f"{S3_RAW_PREFIX}{partition}/{filename}"

    content = "\n".join(json.dumps(r) for r in buffer)

    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=content.encode("utf-8"),
            ContentType="application/x-ndjson"
        )
        logger.info(f"✅ Flushed {len(buffer)} records to s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}")


def run_consumer():
    """Main consumer loop — reads from Kinesis and buffers to S3."""
    logger.info("🚀 S3 Consumer started")
    shard_iterators = get_shard_iterators()
    buffer = []
    last_flush = time.time()

    while True:
        new_iterators = []
        for iterator in shard_iterators:
            records, next_iterator = read_from_shard(iterator)
            buffer.extend(records)
            if next_iterator:
                new_iterators.append(next_iterator)

        shard_iterators = new_iterators or shard_iterators

        # Flush to S3 every FLUSH_INTERVAL_SECONDS
        if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
            flush_buffer_to_s3(buffer)
            buffer = []
            last_flush = time.time()

        time.sleep(1)


if __name__ == "__main__":
    run_consumer()
