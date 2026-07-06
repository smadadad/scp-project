"""
speed_layer/stream_processor.py
────────────────────────────────
Speed layer of the Lambda architecture.

Reads from Kinesis in real time, applies sliding-window aggregations,
detects anomalies, and writes incremental results to DynamoDB.

Windows:
  • 5-minute sliding window — top 5 most delayed routes RIGHT NOW
  • 1-minute tumbling window — per-route average delay
  • Anomaly flag — route delay > 2x its historical batch average

This runs as a long-lived process (or can be wrapped as a Lambda trigger).

Usage:
    python speed_layer/stream_processor.py
"""

import json
import time
import logging
import datetime
import sys
import os
import threading
from collections import defaultdict, deque
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import (
    KINESIS_STREAM_NAME, DYNAMO_SPEED_TABLE, DYNAMO_BATCH_TABLE,
    WINDOW_SIZE_MINUTES, TOP_N_ROUTES
)
from utils.aws_utils import get_kinesis_client, get_dynamodb_resource

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

kinesis = get_kinesis_client()
dynamo = get_dynamodb_resource()
speed_table = dynamo.Table(DYNAMO_SPEED_TABLE)
batch_table = dynamo.Table(DYNAMO_BATCH_TABLE)


# ─── In-Memory Sliding Window ──────────────────────────────────────────────────

class SlidingWindowAggregator:
    """
    Maintains a sliding window of delay events per route.
    Thread-safe ring-buffer approach: events older than WINDOW_SIZE_MINUTES
    are discarded on each aggregation call.
    """

    def __init__(self, window_minutes: int = WINDOW_SIZE_MINUTES):
        self.window_seconds = window_minutes * 60
        # route_id → deque of (timestamp, delay_seconds)
        self._events: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def add_event(self, route_id: str, delay_seconds: int, ts: datetime.datetime = None):
        if ts is None:
            ts = datetime.datetime.utcnow()
        with self._lock:
            self._events[route_id].append((ts, delay_seconds))

    def _prune_old(self, route_id: str, cutoff: datetime.datetime):
        dq = self._events[route_id]
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def get_window_stats(self) -> dict[str, dict]:
        """Return per-route stats for all routes with events in the current window."""
        now = datetime.datetime.utcnow()
        cutoff = now - datetime.timedelta(seconds=self.window_seconds)
        stats = {}

        with self._lock:
            for route_id in list(self._events.keys()):
                self._prune_old(route_id, cutoff)
                delays = [d for _, d in self._events[route_id]]
                if not delays:
                    continue
                stats[route_id] = {
                    "count": len(delays),
                    "avg_delay": sum(delays) / len(delays),
                    "max_delay": max(delays),
                    "late_count": sum(1 for d in delays if d > 60),
                }

        return stats

    def top_n_delayed(self, n: int = TOP_N_ROUTES) -> list[tuple[str, dict]]:
        """Return top-N routes sorted by average delay in the current window."""
        stats = self.get_window_stats()
        sorted_routes = sorted(stats.items(), key=lambda x: x[1]["avg_delay"], reverse=True)
        return sorted_routes[:n]


# Global aggregator instance
aggregator = SlidingWindowAggregator(window_minutes=WINDOW_SIZE_MINUTES)


# ─── Batch Baseline Lookup ─────────────────────────────────────────────────────

_batch_baseline_cache: dict[str, float] = {}
_cache_ts: datetime.datetime = None
CACHE_TTL_MINUTES = 10


def get_batch_baseline(route_id: str) -> float | None:
    """
    Look up the historical average delay for a route from DynamoDB (batch view).
    Results cached locally for CACHE_TTL_MINUTES to avoid excess reads.
    """
    global _cache_ts

    now = datetime.datetime.utcnow()
    if _cache_ts and (now - _cache_ts).seconds < CACHE_TTL_MINUTES * 60:
        return _batch_baseline_cache.get(route_id)

    try:
        # Query batch table for current day-of-week + hour
        time_bucket = f"{now.strftime('%A')}_{now.hour:02d}"
        response = batch_table.get_item(
            Key={"route_id": route_id, "time_bucket": time_bucket}
        )
        item = response.get("Item")
        if item:
            baseline = float(item.get("avg_delay_seconds", 0))
            _batch_baseline_cache[route_id] = baseline
            _cache_ts = now
            return baseline
    except Exception as e:
        logger.warning(f"Batch baseline lookup failed for {route_id}: {e}")

    return None


# ─── DynamoDB Write ────────────────────────────────────────────────────────────

def write_speed_view(window_stats: dict[str, dict], window_end: str):
    """Write current window aggregates to DynamoDB speed view table."""
    with speed_table.batch_writer() as batch:
        for route_id, stats in window_stats.items():
            baseline = get_batch_baseline(route_id)
            is_anomaly = False
            anomaly_ratio = None

            if baseline and baseline > 0:
                anomaly_ratio = stats["avg_delay"] / baseline
                is_anomaly = anomaly_ratio > 2.0   # 2x historical average = anomaly

            item = {
                "route_id": route_id,
                "window_end": window_end,
                "window_size_minutes": WINDOW_SIZE_MINUTES,
                "avg_delay_seconds": Decimal(str(round(stats["avg_delay"], 2))),
                "max_delay_seconds": Decimal(str(stats["max_delay"])),
                "event_count": stats["count"],
                "late_count": stats["late_count"],
                "is_anomaly": is_anomaly,
                "anomaly_ratio": Decimal(str(round(anomaly_ratio, 2))) if anomaly_ratio else None,
                "batch_baseline_seconds": Decimal(str(round(baseline, 2))) if baseline else None,
                "ttl": int(time.time()) + 3600   # DynamoDB TTL: keep for 1 hour
            }
            # Remove None values (DynamoDB doesn't accept them)
            item = {k: v for k, v in item.items() if v is not None}
            batch.put_item(Item=item)


def write_top_routes(top_routes: list[tuple[str, dict]], window_end: str):
    """Write the top-N snapshot to DynamoDB for the dashboard."""
    snapshot_item = {
        "route_id": "__TOP_N_SNAPSHOT__",
        "window_end": window_end,
        "top_routes": json.dumps([
            {
                "route_id": r,
                "avg_delay_seconds": round(s["avg_delay"], 1),
                "max_delay_seconds": s["max_delay"],
                "event_count": s["count"],
                "rank": i + 1
            }
            for i, (r, s) in enumerate(top_routes)
        ]),
        "ttl": int(time.time()) + 3600
    }
    speed_table.put_item(Item=snapshot_item)
    logger.info(f"Top {len(top_routes)} routes at {window_end}: "
                + ", ".join(f"{r}={s['avg_delay']:.0f}s" for r, s in top_routes))


# ─── Kinesis Reader ────────────────────────────────────────────────────────────

def process_kinesis_record(record: dict):
    """Process a single Kinesis record — add to sliding window."""
    route_id = record.get("route_id")
    delay_seconds = record.get("delay_seconds")
    source = record.get("source", "")

    if not route_id or delay_seconds is None:
        return
    if source != "tfi_trip_updates":
        return   # Skip vehicle position records

    try:
        ts_str = record.get("ingestion_time", "")
        ts = datetime.datetime.fromisoformat(ts_str.rstrip("Z")) if ts_str else datetime.datetime.utcnow()
    except Exception:
        ts = datetime.datetime.utcnow()

    aggregator.add_event(route_id, int(delay_seconds), ts)


def get_shard_iterators_trim_horizon() -> list[str]:
    """Get TRIM_HORIZON iterators — reads from the oldest available record."""
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
    return iterators


# ─── Main Loop ─────────────────────────────────────────────────────────────────

def run_speed_layer():
    logger.info(f"🚀 Speed layer started — {WINDOW_SIZE_MINUTES}min sliding window")
    shard_iterators = get_shard_iterators_trim_horizon()

    AGGREGATE_EVERY_SECONDS = 30  # Write window stats every 30s
    last_aggregate = time.time()
    total_records_processed = 0

    while True:
        new_iterators = []
        for iterator in shard_iterators:
            try:
                response = kinesis.get_records(ShardIterator=iterator, Limit=500)
                records = response.get("Records", [])

                for raw in records:
                    try:
                        data = json.loads(raw["Data"].decode("utf-8"))
                        process_kinesis_record(data)
                        total_records_processed += 1
                    except Exception as e:
                        logger.warning(f"Record parse error: {e}")

                if records:
                    logger.debug(f"Read {len(records)} records from shard")

                next_iter = response.get("NextShardIterator")
                if next_iter:
                    new_iterators.append(next_iter)

            except Exception as e:
                logger.error(f"Shard read error: {e}")
                new_iterators.append(iterator)

        shard_iterators = new_iterators or shard_iterators

        # Aggregate and write to DynamoDB on interval
        if time.time() - last_aggregate >= AGGREGATE_EVERY_SECONDS:
            window_end = datetime.datetime.utcnow().isoformat() + "Z"
            window_stats = aggregator.get_window_stats()
            top_routes = aggregator.top_n_delayed(n=TOP_N_ROUTES)

            if window_stats:
                write_speed_view(window_stats, window_end)
                write_top_routes(top_routes, window_end)
                logger.info(f"Window snapshot written — {len(window_stats)} routes, "
                            f"{total_records_processed} total records processed")

            last_aggregate = time.time()

        time.sleep(1)


if __name__ == "__main__":
    run_speed_layer()
