from config.aws_config import get_s3_client

s3 = get_s3_client()


def list_objects(bucket_name, prefix):
    """
    List all objects under a given S3 prefix.
    """
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    if "Contents" not in response:
        return []

    return [obj["Key"] for obj in response["Contents"]]


def folder_exists(bucket_name, prefix):
    """
    Check whether an S3 folder/prefix exists.
    """
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        MaxKeys=1
    )

    return "Contents" in response