"""
batch_layer/spark_batch_job.py
──────────────────────────────
PySpark batch job — runs on EMR over all raw S3 data.

Computes:
  • Average and max delay per route per hour-of-day and day-of-week
  • Top 20 most chronically delayed routes (batch view)
  • Delay distribution per route
  • Per-stop-level delay averages

This is the BATCH LAYER of the Lambda architecture:
  - Gives accurate, complete results over the entire history
  - Output written to S3 as Parquet (queried by Athena / serving layer)

Submit to EMR:
    spark-submit --master yarn --deploy-mode cluster batch_layer/spark_batch_job.py \
        --input s3://<bucket>/raw/ \
        --output s3://<bucket>/batch-output/

Or from the EMR Step API (see infrastructure/submit_batch_job.py).
"""

import sys
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("stop_sequence", IntegerType(), True),
    StructField("stop_id", StringType(), True),
    StructField("delay_seconds", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("schedule_relationship", IntegerType(), True),
    StructField("source", StringType(), True),
    StructField("_kinesis_sequence", StringType(), True),
    StructField("_kinesis_arrival", StringType(), True),
])


def run_batch_job(input_path: str, output_path: str, num_partitions: int = 8):
    spark = SparkSession.builder \
        .appName("DublinBus-BatchLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Reading raw data from: {input_path}")

    df_raw = spark.read \
        .schema(RAW_SCHEMA) \
        .json(input_path) \
        .filter(F.col("source") == "tfi_trip_updates") \
        .filter(F.col("route_id").isNotNull()) \
        .filter(F.col("delay_seconds").isNotNull())

    # Parse timestamps and extract time dimensions
    df = df_raw.withColumn(
        "event_ts", F.to_timestamp(F.col("ingestion_time"))
    ).withColumn(
        "day_of_week", F.date_format(F.col("event_ts"), "EEEE")
    ).withColumn(
        "hour_of_day", F.hour(F.col("event_ts"))
    ).withColumn(
        "date_str", F.to_date(F.col("event_ts")).cast(StringType())
    ).withColumn(
        "time_bucket", F.concat_ws(
            "_",
            F.col("day_of_week"),
            F.lpad(F.col("hour_of_day").cast(StringType()), 2, "0")
        )
    ).repartition(num_partitions, "route_id")

    total_records = df.count()
    logger.info(f"Total records loaded: {total_records:,}")

    df_route_agg = df.groupBy("route_id", "time_bucket", "day_of_week", "hour_of_day") \
        .agg(
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.max("delay_seconds").alias("max_delay_seconds"),
            F.min("delay_seconds").alias("min_delay_seconds"),
            F.stddev("delay_seconds").alias("stddev_delay_seconds"),
            F.count("*").alias("trip_count"),
            F.sum(F.when(F.col("delay_seconds") > 60, 1).otherwise(0)).alias("late_count"),
            F.sum(F.when(F.col("delay_seconds") > 300, 1).otherwise(0)).alias("very_late_count"),
            F.sum(F.when(F.col("delay_seconds") < 0, 1).otherwise(0)).alias("early_count"),
        ) \
        .withColumn(
            "on_time_rate",
            F.round(
                (F.col("trip_count") - F.col("late_count")) / F.col("trip_count") * 100, 2
            )
        ) \
        .withColumn("batch_computed_at", F.lit(datetime.utcnow().isoformat()))

    df_route_agg.write \
        .mode("overwrite") \
        .partitionBy("day_of_week", "hour_of_day") \
        .parquet(f"{output_path}/route_time_aggregate/")
    logger.info("✅ Written: route_time_aggregate")

    df_top_delayed = df.groupBy("route_id") \
        .agg(
            F.avg("delay_seconds").alias("overall_avg_delay"),
            F.count("*").alias("total_observations"),
            F.sum(F.when(F.col("delay_seconds") > 300, 1).otherwise(0)).alias("severe_delay_count")
        ) \
        .orderBy(F.col("overall_avg_delay").desc()) \
        .limit(20) \
        .withColumn("rank", F.monotonically_increasing_id() + 1)

    df_top_delayed.write \
        .mode("overwrite") \
        .parquet(f"{output_path}/top_delayed_routes/")
    logger.info("✅ Written: top_delayed_routes")

    df_daily = df.groupBy("route_id", "date_str") \
        .agg(
            F.avg("delay_seconds").alias("daily_avg_delay"),
            F.count("*").alias("daily_observations")
        )

    df_daily.write \
        .mode("overwrite") \
        .partitionBy("date_str") \
        .parquet(f"{output_path}/daily_trend/")
    logger.info("✅ Written: daily_trend")

    df_stop = df.groupBy("route_id", "stop_id") \
        .agg(
            F.avg("delay_seconds").alias("avg_stop_delay"),
            F.count("*").alias("stop_observations")
        )

    df_stop.write \
        .mode("overwrite") \
        .parquet(f"{output_path}/stop_delay/")
    logger.info("✅ Written: stop_delay")

    # Re-run route agg with 1 partition (sequential) vs 8 (parallel) for benchmarking
    logger.info("Running sequential benchmark (1 partition)...")
    t0 = datetime.utcnow()
    df.repartition(1).groupBy("route_id").agg(F.avg("delay_seconds")).count()
    t1 = datetime.utcnow()
    sequential_secs = (t1 - t0).total_seconds()

    logger.info("Running parallel benchmark (8 partitions)...")
    t2 = datetime.utcnow()
    df.repartition(8).groupBy("route_id").agg(F.avg("delay_seconds")).count()
    t3 = datetime.utcnow()
    parallel_secs = (t3 - t2).total_seconds()

    speedup = sequential_secs / parallel_secs if parallel_secs > 0 else 0
    benchmark = {
        "sequential_seconds": sequential_secs,
        "parallel_seconds": parallel_secs,
        "speedup_ratio": speedup,
        "total_records": total_records
    }

    spark.createDataFrame([benchmark]).write \
        .mode("overwrite") \
        .json(f"{output_path}/benchmark/")
    logger.info(f"✅ Benchmark: sequential={sequential_secs:.1f}s  parallel={parallel_secs:.1f}s  speedup={speedup:.2f}x")

    spark.stop()
    logger.info("🏁 Batch job complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dublin Bus Batch Layer")
    parser.add_argument("--input", required=True, help="S3 path to raw JSON-lines data")
    parser.add_argument("--output", required=True, help="S3 path for Parquet output")
    parser.add_argument("--partitions", type=int, default=8, help="Number of Spark partitions")
    args = parser.parse_args()

    run_batch_job(args.input, args.output, args.partitions)
