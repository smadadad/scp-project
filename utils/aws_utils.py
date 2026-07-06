"""
AWS utility helpers — creates boto3 clients using config credentials.
In AWS Academy, credentials rotate every session; update config.py accordingly.
"""

import boto3
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.config import (
    AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_boto3_session():
    """Return a boto3 Session using credentials from config."""
    return boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )


def get_client(service: str):
    return get_boto3_session().client(service)


def get_resource(service: str):
    return get_boto3_session().resource(service)


def get_kinesis_client():
    return get_client("kinesis")


def get_s3_client():
    return get_client("s3")


def get_s3_resource():
    return get_resource("s3")


def get_dynamodb_resource():
    return get_resource("dynamodb")


def get_emr_client():
    return get_client("emr")


def get_athena_client():
    return get_client("athena")


def get_lambda_client():
    return get_client("lambda")


def get_autoscaling_client():
    return get_client("autoscaling")
