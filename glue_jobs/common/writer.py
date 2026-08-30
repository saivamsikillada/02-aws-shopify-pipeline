from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    GOLD_PREFIX
)


def write_to_processed(
    glue_context,
    dynamic_frame,
    folder_name,
    partition_keys=None
):
    """
    Writes DynamicFrame to the Processed (Silver) layer in S3.
    """

    if partition_keys is None:
        partition_keys = []

    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{folder_name}/",
            "partitionKeys": partition_keys
        },
        format_options={
            "compression": "snappy"
        }
    )


def write_to_gold(
    glue_context,
    dynamic_frame,
    folder_name,
    partition_keys=None
):
    """
    Writes DynamicFrame to the Gold layer in S3.
    """

    if partition_keys is None:
        partition_keys = []

    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": f"s3://{S3_BUCKET}/{GOLD_PREFIX}{folder_name}/",
            "partitionKeys": partition_keys
        },
        format_options={
            "compression": "snappy"
        }
    )