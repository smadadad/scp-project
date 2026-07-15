"""
Application configuration.

All application settings are defined here.
AWS credentials are NOT stored here.

AWS Cloud9 provides AWS authentication automatically
through boto3/IAM credentials.
"""


# ==========================
# Transport for Ireland API
# ==========================

TFI_API_KEY = "YOUR_TFI_API_KEY"

TFI_TRIP_UPDATES_URL = (
    "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
)

TFI_VEHICLE_POSITIONS_URL = (
    "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
)

TFI_STATIC_GTFS_URL = (
    "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
)

POLL_INTERVAL_SECONDS = 30


# ==========================
# AWS Configuration
# ==========================

# Cloud9/boto3 handles credentials automatically
AWS_REGION = "us-east-1"


# ==========================
# Kinesis
# ==========================

KINESIS_STREAM_NAME = "dublin-bus-stream"

KINESIS_SHARD_COUNT = 2

KINESIS_PARTITION_KEY = "route_id"


# ==========================
# S3
# ==========================

# Must be globally unique in AWS
S3_BUCKET_NAME = "dublin-bus-analytics-shritiz-nci-20260708"

S3_RAW_PREFIX = "raw/"

S3_BATCH_OUTPUT_PREFIX = "batch-output/"

S3_SPEED_OUTPUT_PREFIX = "speed-output/"

S3_STATIC_GTFS_PREFIX = "gtfs-static/"


# ==========================
# EMR
# ==========================

EMR_CLUSTER_NAME = "dublin-bus-batch-cluster"

EMR_RELEASE_LABEL = "emr-6.15.0"

EMR_MASTER_INSTANCE_TYPE = "m5.xlarge"

EMR_WORKER_INSTANCE_TYPE = "m5.xlarge"

EMR_MIN_WORKERS = 1

EMR_MAX_WORKERS = 2


# ==========================
# DynamoDB
# ==========================

DYNAMO_SPEED_TABLE = "dublin-bus-speed-view"

DYNAMO_SERVING_TABLE = "dublin-bus-serving-view"

DYNAMO_BATCH_TABLE = "dublin-bus-batch-view"


# ==========================
# Lambda
# ==========================

LAMBDA_FUNCTION_NAME = "dublin-bus-speed-processor"


# ==========================
# Processing
# ==========================

WINDOW_SIZE_MINUTES = 5

TOP_N_ROUTES = 5


# ==========================
# Athena
# ==========================

ATHENA_DATABASE = "dublin_bus_analytics"

ATHENA_OUTPUT_LOCATION = (
    f"s3://{S3_BUCKET_NAME}/athena-results/"
)


# ==========================
# Auto Scaling
# ==========================

AUTOSCALING_TARGET_CPU = 70

AUTOSCALING_COOLDOWN_SECONDS = 300

KINESIS_BACKLOG_SCALE_TRIGGER = 1000


# ==========================
# Dashboard
# ==========================

DASHBOARD_REFRESH_SECONDS = 30


EMR_CLUSTER_ID =  "j-2W7OYR3PY8KI5"

EMR_BATCH_JOB_SCRIPT_S3_PATH = "scripts/spark_batch_job.py"