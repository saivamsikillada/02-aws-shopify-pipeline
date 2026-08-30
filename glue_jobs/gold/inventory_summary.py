

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    INVENTORY_INPUT,
    INVENTORY_SUMMARY_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.writer import write_to_gold
from common.logger import get_logger

glueContext, spark, job = initialize_glue_job()

logger = get_logger("inventory_summary")
logger.info("Inventory Summary Job Started")

# ---------------------------------------------------------
# Read Silver Inventory
# ---------------------------------------------------------

inventory_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{INVENTORY_INPUT}/"
        ]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Create Inventory Summary
# ---------------------------------------------------------

inventory_summary_df = inventory_df.select(
    "inventory_id",
    "product_id",
    "warehouse_id",
    "warehouse_name",
    "warehouse_city",
    "available_stock",
    "reserved_stock",
    "reorder_level",
    "last_updated",
    "status"
)

# Total Stock = Available + Reserved
inventory_summary_df = inventory_summary_df.withColumn(
    "total_stock",
    col("available_stock") + col("reserved_stock")
)

# ---------------------------------------------------------
# Convert to DynamicFrame
# ---------------------------------------------------------

inventory_summary_dyf = DynamicFrame.fromDF(
    inventory_summary_df,
    glueContext,
    "inventory_summary_dyf"
)

# ---------------------------------------------------------
# Write Gold Layer
# ---------------------------------------------------------

logger.info("Writing Inventory Summary to Gold Layer")

write_to_gold(
    glueContext,
    inventory_summary_dyf,
    INVENTORY_SUMMARY_OUTPUT
)
logger.info("Inventory Summary Job Completed Successfully")

job.commit()