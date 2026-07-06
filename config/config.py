"""Application configuration loaded from environment variables."""

from pathlib import Path
import os

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def get_env(name: str, default: str = "", required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


TFI_API_KEY = get_env("TFI_API_KEY")
TFI_TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
TFI_VEHICLE_POSITIONS_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
TFI_STATIC_GTFS_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
POLL_INTERVAL_SECONDS = 30

AWS_REGION = get_env("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = get_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = get_env("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = get_env("AWS_SESSION_TOKEN")

KINESIS_STREAM_NAME = get_env("KINESIS_STREAM_NAME", "dublin-bus-stream")
KINESIS_SHARD_COUNT = 2
KINESIS_PARTITION_KEY = "route_id"

S3_BUCKET_NAME = get_env("S3_BUCKET_NAME", "dublin-bus-analytics-bucket-x24101001")
S3_RAW_PREFIX = "raw/"
S3_BATCH_OUTPUT_PREFIX = "batch-output/"
S3_SPEED_OUTPUT_PREFIX = "speed-output/"
S3_STATIC_GTFS_PREFIX = "gtfs-static/"

EMR_CLUSTER_NAME = get_env("EMR_CLUSTER_NAME", "dublin-bus-batch-cluster")
EMR_RELEASE_LABEL = "emr-6.15.0"
EMR_MASTER_INSTANCE_TYPE = "m5.xlarge"
EMR_WORKER_INSTANCE_TYPE = "m5.xlarge"
EMR_MIN_WORKERS = 1
EMR_MAX_WORKERS = 2

DYNAMO_SPEED_TABLE = "dublin-bus-speed-view"
DYNAMO_SERVING_TABLE = "dublin-bus-serving-view"
DYNAMO_BATCH_TABLE = "dublin-bus-batch-view"

LAMBDA_FUNCTION_NAME = "dublin-bus-speed-processor"
WINDOW_SIZE_MINUTES = 5
TOP_N_ROUTES = 5

ATHENA_DATABASE = "dublin_bus_analytics"
ATHENA_OUTPUT_LOCATION = f"s3://{S3_BUCKET_NAME}/athena-results/"

AUTOSCALING_TARGET_CPU = 70
AUTOSCALING_COOLDOWN_SECONDS = 300
KINESIS_BACKLOG_SCALE_TRIGGER = 1000

DASHBOARD_REFRESH_SECONDS = 30
