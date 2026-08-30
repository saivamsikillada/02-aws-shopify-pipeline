from common.constants import (
    S3_BUCKET,
    DATABASE_NAME,
    TERADATA_TABLE,
    PROCESSED_PREFIX,
    INVENTORY_OUTPUT
)
import sys
from awsglue.transforms import *
from common.glue_context import initialize_glue_job
from common.quality import run_data_quality
from common.writer import write_to_processed
from common.logger import get_logger
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

# ---------------------------------------------------------
# Initialize Glue Job
# ---------------------------------------------------------
glueContext, spark, job = initialize_glue_job()

logger = get_logger("inventory_etl")
logger.info("Inventory ETL Job Started")



# ---------------------------------------------------------
# Read Inventory Data
# ---------------------------------------------------------
inventory_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TERADATA_TABLE
)

# ---------------------------------------------------------
# Apply Mapping
# ---------------------------------------------------------
inventory_dyf = ApplyMapping.apply(
    frame=inventory_dyf,
    mappings=[
        ("inventory_id","string","inventory_id","string"),
        ("product_id","string","product_id","string"),
        ("warehouse_id","string","warehouse_id","string"),
        ("warehouse_name","string","warehouse_name","string"),
        ("warehouse_city","string","warehouse_city","string"),
        ("available_stock","long","available_stock","long"),
        ("reserved_stock","long","reserved_stock","long"),
        ("reorder_level","long","reorder_level","long"),
        ("last_updated","string","last_updated","string"),
        ("status","string","status","string")
    ]
)

# ---------------------------------------------------------
# Convert to DataFrame
# ---------------------------------------------------------
inventory_df = inventory_dyf.toDF()

# ---------------------------------------------------------
# Remove Duplicate Inventory Records
# ---------------------------------------------------------
inventory_df = inventory_df.dropDuplicates(["inventory_id"])

# ---------------------------------------------------------
# Trim Spaces
# ---------------------------------------------------------
inventory_df = (
    inventory_df
    .withColumn("warehouse_name", trim(col("warehouse_name")))
    .withColumn("warehouse_city", trim(col("warehouse_city")))
)

# ---------------------------------------------------------
# Uppercase Status
# ---------------------------------------------------------
inventory_df = inventory_df.withColumn(
    "status",
    upper(col("status"))
)

# ---------------------------------------------------------
# Fill Missing Values
# ---------------------------------------------------------
inventory_df = inventory_df.fillna({
    "warehouse_name": "UNKNOWN",
    "warehouse_city": "UNKNOWN"
})

# ---------------------------------------------------------
# Remove Invalid Stock
# ---------------------------------------------------------
inventory_df = inventory_df.filter(
    (col("available_stock") >= 0) &
    (col("reserved_stock") >= 0) &
    (col("reorder_level") >= 0)
)

# ---------------------------------------------------------
# Standardize Last Updated Timestamp
# ---------------------------------------------------------
inventory_df = inventory_df.withColumn(
    "last_updated",
    coalesce(
        to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("last_updated"), "yyyy-MM-dd"),
        to_timestamp(col("last_updated"), "dd/MM/yyyy"),
        to_timestamp(col("last_updated"))
    )
)

# ---------------------------------------------------------
# Convert Back to DynamicFrame
# ---------------------------------------------------------
clean_inventory_dyf = DynamicFrame.fromDF(
    inventory_df,
    glueContext,
    "clean_inventory_dyf"
)

# ---------------------------------------------------------
# Data Quality Check
# ---------------------------------------------------------
logger.info("Running Data Quality Checks")

run_data_quality(
    clean_inventory_dyf,
    "inventory_dq"
)

# ---------------------------------------------------------
# Write to Silver Layer
# ---------------------------------------------------------
logger.info("Writing Inventory data to Processed Layer")

write_to_processed(
    glueContext,
    clean_inventory_dyf,
    INVENTORY_OUTPUT
)
logger.info("Inventory ETL Job Completed Successfully")
# ---------------------------------------------------------
# Commit Job
# ---------------------------------------------------------
job.commit()