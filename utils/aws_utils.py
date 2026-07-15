"""
AWS utility helpers.

AWS credentials are automatically provided by AWS Cloud9 IAM role.
No access keys or secret keys are required.
"""

import boto3
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.config import AWS_REGION


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


def get_boto3_session():
    """
    Create a boto3 session using AWS Cloud9 credentials.

    Cloud9 automatically provides temporary credentials
    through the IAM role attached to the environment.
    """

    return boto3.Session(
        region_name=AWS_REGION
    )


def get_client(service: str):
    """
    Create an AWS service client.
    Example: s3, kinesis, emr, athena
    """
    return get_boto3_session().client(service)


def get_resource(service: str):
    """
    Create an AWS resource.
    Example: DynamoDB resource.
    """
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