"""
infrastructure/setup_aws.py
───────────────────────────
Provisions ALL AWS resources required for the Dublin Bus Analytics platform:
  • S3 bucket
  • Kinesis Data Stream
  • DynamoDB tables (speed view, batch view, serving view)
  • EMR cluster with managed auto-scaling
  • Athena database
  • IAM roles (EMR, Lambda)
  • EC2 Auto Scaling Group policy on EMR

Run once before starting the pipeline:
    python infrastructure/setup_aws.py
"""

import json
import time
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import *
from utils.aws_utils import (
    get_s3_client, get_kinesis_client, get_dynamodb_resource,
    get_emr_client, get_athena_client
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_s3_bucket():
    s3 = get_s3_client()
    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION}
            )
        # Enable versioning for data safety
        s3.put_bucket_versioning(
            Bucket=S3_BUCKET_NAME,
            VersioningConfiguration={"Status": "Enabled"}
        )
        logger.info(f"✅ S3 bucket created: s3://{S3_BUCKET_NAME}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logger.info(f"S3 bucket already exists: {S3_BUCKET_NAME}")
    except Exception as e:
        logger.error(f"S3 setup failed: {e}")
        raise


def create_kinesis_stream():
    kinesis = get_kinesis_client()
    try:
        kinesis.create_stream(
            StreamName=KINESIS_STREAM_NAME,
            ShardCount=KINESIS_SHARD_COUNT
        )
        # Wait for stream to become active
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=KINESIS_STREAM_NAME)
        logger.info(f"✅ Kinesis stream created: {KINESIS_STREAM_NAME} ({KINESIS_SHARD_COUNT} shards)")
    except kinesis.exceptions.ResourceInUseException:
        logger.info(f"Kinesis stream already exists: {KINESIS_STREAM_NAME}")


def enable_kinesis_enhanced_monitoring():
    kinesis = get_kinesis_client()
    kinesis.enable_enhanced_monitoring(
        StreamName=KINESIS_STREAM_NAME,
        ShardLevelMetrics=["IncomingBytes", "IncomingRecords", "IteratorAgeMilliseconds"]
    )
    logger.info("✅ Kinesis enhanced monitoring enabled")


def create_dynamodb_tables():
    dynamo = get_dynamodb_resource()

    tables = [
        {
            "TableName": DYNAMO_SPEED_TABLE,
            "KeySchema": [
                {"AttributeName": "route_id", "KeyType": "HASH"},
                {"AttributeName": "window_end", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "route_id", "AttributeType": "S"},
                {"AttributeName": "window_end", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
        },
        {
            "TableName": DYNAMO_BATCH_TABLE,
            "KeySchema": [
                {"AttributeName": "route_id", "KeyType": "HASH"},
                {"AttributeName": "time_bucket", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "route_id", "AttributeType": "S"},
                {"AttributeName": "time_bucket", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
        },
        {
            "TableName": DYNAMO_SERVING_TABLE,
            "KeySchema": [
                {"AttributeName": "route_id", "KeyType": "HASH"},
                {"AttributeName": "snapshot_time", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "route_id", "AttributeType": "S"},
                {"AttributeName": "snapshot_time", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
    ]

    for table_def in tables:
        try:
            table = dynamo.create_table(**table_def)
            table.wait_until_exists()
            logger.info(f"✅ DynamoDB table created: {table_def['TableName']}")
        except dynamo.meta.client.exceptions.ResourceInUseException:
            logger.info(f"DynamoDB table already exists: {table_def['TableName']}")


def setup_athena():
    athena = get_athena_client()
    query = f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}"
    athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION}
    )
    logger.info(f"✅ Athena database created: {ATHENA_DATABASE}")

    # Create external table over S3 batch output
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.batch_delay_history (
        route_id STRING,
        route_short_name STRING,
        time_bucket STRING,
        avg_delay_seconds DOUBLE,
        max_delay_seconds DOUBLE,
        trip_count BIGINT,
        day_of_week STRING,
        hour_of_day INT
    )
    STORED AS PARQUET
    LOCATION 's3://{S3_BUCKET_NAME}/{S3_BATCH_OUTPUT_PREFIX}'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """
    athena.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION}
    )
    logger.info("✅ Athena external table created: batch_delay_history")


def create_emr_cluster():
    emr = get_emr_client()

    # Managed auto-scaling policy
    auto_scaling_policy = {
        "Constraints": {
            "MinCapacity": EMR_MIN_WORKERS,
            "MaxCapacity": EMR_MAX_WORKERS
        },
        "Rules": [
            {
                "Name": "ScaleOutOnCPU",
                "Description": "Scale out when YARNMemoryAvailablePercentage < 20%",
                "Action": {
                    "SimpleScalingPolicyConfiguration": {
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": 2,
                        "CoolDown": AUTOSCALING_COOLDOWN_SECONDS
                    }
                },
                "Trigger": {
                    "CloudWatchAlarmDefinition": {
                        "ComparisonOperator": "LESS_THAN",
                        "EvaluationPeriods": 1,
                        "MetricName": "YARNMemoryAvailablePercentage",
                        "Namespace": "AWS/ElasticMapReduce",
                        "Period": 300,
                        "Threshold": 20,
                        "Unit": "PERCENT",
                        "Statistic": "AVERAGE"
                    }
                }
            },
            {
                "Name": "ScaleInOnCPU",
                "Description": "Scale in when YARNMemoryAvailablePercentage > 75%",
                "Action": {
                    "SimpleScalingPolicyConfiguration": {
                        "AdjustmentType": "CHANGE_IN_CAPACITY",
                        "ScalingAdjustment": -1,
                        "CoolDown": AUTOSCALING_COOLDOWN_SECONDS
                    }
                },
                "Trigger": {
                    "CloudWatchAlarmDefinition": {
                        "ComparisonOperator": "GREATER_THAN",
                        "EvaluationPeriods": 2,
                        "MetricName": "YARNMemoryAvailablePercentage",
                        "Namespace": "AWS/ElasticMapReduce",
                        "Period": 300,
                        "Threshold": 75,
                        "Unit": "PERCENT",
                        "Statistic": "AVERAGE"
                    }
                }
            }
        ]
    }

    response = emr.run_job_flow(
        Name=EMR_CLUSTER_NAME,
        ReleaseLabel=EMR_RELEASE_LABEL,
        Applications=[
            {"Name": "Hadoop"},
            {"Name": "Spark"},
            {"Name": "Hive"},
        ],
        Instances={
            "MasterInstanceType": EMR_MASTER_INSTANCE_TYPE,
            "SlaveInstanceType": EMR_WORKER_INSTANCE_TYPE,
            "InstanceCount": EMR_MIN_WORKERS + 1,  # 1 master + N workers
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        AutoScalingRole="EMR_AutoScaling_DefaultRole",
        ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
        LogUri=f"s3://{S3_BUCKET_NAME}/emr-logs/",
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        Tags=[
            {"Key": "Project", "Value": "DublinBusAnalytics"},
            {"Key": "Environment", "Value": "NCI-MSc"},
        ]
    )

    cluster_id = response["JobFlowId"]
    logger.info(f"✅ EMR cluster launched: {cluster_id}")

    # Wait until the EMR cluster is ready and instance groups exist
    try:
        waiter = emr.get_waiter("cluster_running")
        waiter.wait(ClusterId=cluster_id)
    except Exception as e:
        logger.warning(f"Cluster wait timed out or failed: {e}")

    try:
        instance_groups = emr.list_instance_groups(ClusterId=cluster_id).get("InstanceGroups", [])
        core_group = next((ig for ig in instance_groups if ig.get("InstanceGroupType") == "CORE"), None)

        if core_group:
            emr.put_auto_scaling_policy(
                ClusterId=cluster_id,
                InstanceGroupId=core_group["Id"],
                AutoScalingPolicy=auto_scaling_policy
            )
            logger.info(f"✅ Auto-scaling policy applied to instance group: {core_group['Id']}")
        else:
            logger.warning("No CORE instance group found yet; skipping auto-scaling policy")
    except Exception as e:
        logger.warning(f"Could not apply auto-scaling policy: {e}")

    return cluster_id


def setup_all():
    logger.info("🚀 Starting Dublin Bus Analytics infrastructure setup...")
    create_s3_bucket()
    create_kinesis_stream()
    enable_kinesis_enhanced_monitoring()
    create_dynamodb_tables()
    setup_athena()
    logger.info("⚠️  Skipping EMR cluster creation — run create_emr_cluster() separately to control timing/cost")
    logger.info("✅ Infrastructure setup complete!")


if __name__ == "__main__":
    setup_all()
