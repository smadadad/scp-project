"""
batch_layer/spark_batch_job.py

PySpark Batch Layer for Dublin Bus Analytics.

Runs on AWS EMR.

Arguments:
    --input       S3 input JSON data location
    --output      S3 output location
    --partitions  Spark partition count

Example:

spark-submit \
spark_batch_job.py \
--input s3://bucket/raw/ \
--output s3://bucket/batch-output/ \
--partitions 8
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------
# Schema
# -----------------------------------------------------

RAW_SCHEMA = StructType([

    StructField(
        "event_id",
        StringType(),
        True
    ),

    StructField(
        "ingestion_time",
        StringType(),
        True
    ),

    StructField(
        "route_id",
        StringType(),
        True
    ),

    StructField(
        "trip_id",
        StringType(),
        True
    ),

    StructField(
        "stop_sequence",
        IntegerType(),
        True
    ),

    StructField(
        "stop_id",
        StringType(),
        True
    ),

    StructField(
        "delay_seconds",
        IntegerType(),
        True
    ),

    StructField(
        "source",
        StringType(),
        True
    )
])



# -----------------------------------------------------
# Main batch job
# -----------------------------------------------------

def run_batch_job(
        input_path,
        output_path,
        partitions
):

    spark = (
        SparkSession.builder
        .appName(
            "DublinBus-BatchLayer"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .getOrCreate()
    )


    spark.sparkContext.setLogLevel(
        "WARN"
    )


    logger.info(
        f"Spark version: {spark.version}"
    )

    logger.info(
        f"Input: {input_path}"
    )

    logger.info(
        f"Partitions: {partitions}"
    )


    # -------------------------------------------------
    # Read data
    # -------------------------------------------------

    df = (

        spark.read
        .schema(RAW_SCHEMA)
        .json(input_path)

        .filter(
            F.col("route_id").isNotNull()
        )

        .filter(
            F.col("delay_seconds").isNotNull()
        )

        .repartition(
            partitions,
            "route_id"
        )

    )


    total_records = df.count()


    logger.info(
        f"Loaded records: {total_records}"
    )


    if total_records == 0:

        logger.error(
            "No input data found"
        )

        spark.stop()

        return



    # -------------------------------------------------
    # Time columns
    # -------------------------------------------------

    df = (

        df

        .withColumn(
            "event_ts",
            F.to_timestamp(
                "ingestion_time"
            )
        )

        .withColumn(
            "day_of_week",
            F.date_format(
                "event_ts",
                "EEEE"
            )
        )

        .withColumn(
            "hour_of_day",
            F.hour(
                "event_ts"
            )
        )

    )


    # -------------------------------------------------
    # Route statistics
    # -------------------------------------------------

    route_stats = (

        df.groupBy(
            "route_id",
            "day_of_week",
            "hour_of_day"
        )

        .agg(

            F.avg(
                "delay_seconds"
            )
            .alias(
                "avg_delay"
            ),


            F.max(
                "delay_seconds"
            )
            .alias(
                "max_delay"
            ),


            F.count("*")
            .alias(
                "observations"
            )

        )

        .withColumn(
            "generated_at",
            F.lit(
                datetime.utcnow()
                .isoformat()
            )
        )

    )


    route_stats.write \
        .mode("overwrite") \
        .parquet(
            f"{output_path}/route_statistics/"
        )


    logger.info(
        "Route statistics written"
    )



    # -------------------------------------------------
    # Top delayed routes
    # -------------------------------------------------

    top_routes = (

        df.groupBy(
            "route_id"
        )

        .agg(

            F.avg(
                "delay_seconds"
            )
            .alias(
                "average_delay"
            ),

            F.count("*")
            .alias(
                "records"
            )

        )

        .orderBy(
            F.desc(
                "average_delay"
            )
        )

        .limit(20)

    )


    top_routes.write \
        .mode("overwrite") \
        .parquet(
            f"{output_path}/top_delayed_routes/"
        )


    logger.info(
        "Top routes written"
    )



    # -------------------------------------------------
    # Spark partition benchmark
    # -------------------------------------------------

    logger.info(
        f"Running aggregation benchmark with {partitions} partitions"
    )


    start = datetime.utcnow()


    (
        df.repartition(
            partitions
        )

        .groupBy(
            "route_id"
        )

        .agg(
            F.avg(
                "delay_seconds"
            )
        )

        .count()

    )


    end = datetime.utcnow()


    execution_time = (
        end - start
    ).total_seconds()



    benchmark = {

        "partitions":
            partitions,

        "execution_seconds":
            execution_time,

        "records":
            total_records

    }



    spark.createDataFrame(
        [benchmark]
    ).write \
    .mode("overwrite") \
    .json(
        f"{output_path}/benchmark/"
    )


    logger.info(
        f"Benchmark finished: {benchmark}"
    )


    spark.stop()



# -----------------------------------------------------
# Entry point
# -----------------------------------------------------

if __name__ == "__main__":


    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--input",
        required=True
    )


    parser.add_argument(
        "--output",
        required=True
    )


    parser.add_argument(
        "--partitions",
        type=int,
        default=8
    )


    args = parser.parse_args()



    run_batch_job(

        args.input,

        args.output,

        args.partitions

    )