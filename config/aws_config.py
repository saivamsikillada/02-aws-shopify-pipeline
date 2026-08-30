import boto3
from config.config import AWS_REGION

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION
    )

def get_glue_client():
    return boto3.client(
        "glue",
        region_name=AWS_REGION
    )