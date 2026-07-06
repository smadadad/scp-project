"""
batch_layer/submit_batch_job.py
────────────────────────────────
Uploads the PySpark script to S3 and submits it as an EMR Step.
Run this whenever you want to recompute the batch view.

Usage:
    python batch_layer/submit_batch_job.py --cluster-id j-XXXXXXX
"""

import argparse
import logging
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import S3_BUCKET_NAME, S3_RAW_PREFIX, S3_BATCH_OUTPUT_PREFIX
from utils.aws_utils import get_emr_client, get_s3_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def upload_script_to_s3() -> str:
    """Upload the PySpark script to S3 so EMR can access it."""
    s3 = get_s3_client()
    script_path = os.path.join(os.path.dirname(__file__), "spark_batch_job.py")
    s3_key = "scripts/spark_batch_job.py"

    with open(script_path, "rb") as f:
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=f.read())

    s3_uri = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    logger.info(f"✅ Script uploaded to {s3_uri}")
    return s3_uri


def submit_step(cluster_id: str, script_s3_uri: str, num_partitions: int = 8) -> str:
    emr = get_emr_client()

    response = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": "DublinBus-BatchLayer-SparkJob",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--master", "yarn",
                        "--conf", "spark.yarn.submit.waitAppCompletion=true",
                        "--conf", f"spark.default.parallelism={num_partitions}",
                        script_s3_uri,
                        "--input", f"s3://{S3_BUCKET_NAME}/{S3_RAW_PREFIX}",
                        "--output", f"s3://{S3_BUCKET_NAME}/{S3_BATCH_OUTPUT_PREFIX}",
                        "--partitions", str(num_partitions)
                    ]
                }
            }
        ]
    )

    step_id = response["StepIds"][0]
    logger.info(f"✅ EMR step submitted: {step_id}")
    return step_id


def wait_for_step(cluster_id: str, step_id: str):
    emr = get_emr_client()
    logger.info(f"Waiting for step {step_id} to complete...")

    while True:
        response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response["Step"]["Status"]["State"]
        logger.info(f"  Step state: {state}")

        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            if state == "COMPLETED":
                logger.info("✅ Batch job completed successfully!")
            else:
                logger.error(f"❌ Batch job ended with state: {state}")
            break

        time.sleep(30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-id", required=True, help="EMR cluster ID (e.g. j-XXXXXXX)")
    parser.add_argument("--partitions", type=int, default=8)
    args = parser.parse_args()

    script_uri = upload_script_to_s3()
    step_id = submit_step(args.cluster_id, script_uri, args.partitions)
    wait_for_step(args.cluster_id, step_id)
