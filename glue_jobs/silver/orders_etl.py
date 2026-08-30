from common.constants import (
    DATABASE_NAME,
    SALESFORCE_TABLE,
    ORDERS_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.quality import run_data_quality
from common.writer import write_to_processed
from common.logger import get_logger

# ---------------------------------------------------------
# Initialize Glue Job
# ---------------------------------------------------------
glueContext, spark, job = initialize_glue_job()

logger = get_logger("orders_etl")
logger.info("Orders ETL Job Started")


# ---------------------------------------------------------
# Read Orders
# ---------------------------------------------------------
orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=SALESFORCE_TABLE
)
# ---------------------------------------------------------
# Mapping
# ---------------------------------------------------------
orders_dyf = ApplyMapping.apply(
    frame=orders_dyf,
    mappings=[
        ("order_id","string","order_id","string"),
        ("customer_id","string","customer_id","string"),
        ("product_id","string","product_id","string"),
        ("order_date","string","order_date","string"),
        ("quantity","int","quantity","int"),
        ("order_amount","int","order_amount","int"),
        ("order_status","string","order_status","string")
    ]
)

# ---------------------------------------------------------
# Convert to DataFrame
# ---------------------------------------------------------
orders_df = orders_dyf.toDF()

# ---------------------------------------------------------
# Remove Duplicate Orders
# ---------------------------------------------------------
orders_df = orders_df.dropDuplicates(["order_id"])

# ---------------------------------------------------------
# Trim Spaces
# ---------------------------------------------------------
orders_df = (
    orders_df
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
)

# ---------------------------------------------------------
# Uppercase Status
# ---------------------------------------------------------
orders_df = orders_df.withColumn(
    "order_status",
    upper(col("order_status"))
)

# ---------------------------------------------------------
# Remove Invalid Quantity
# ---------------------------------------------------------
orders_df = orders_df.filter(
    col("quantity") > 0
)

# ---------------------------------------------------------
# Remove Invalid Amount
# ---------------------------------------------------------
orders_df = orders_df.filter(
    col("order_amount") > 0
)

# ---------------------------------------------------------
# Standardize Date
# ---------------------------------------------------------
orders_df = orders_df.withColumn(
    "order_date",
    coalesce(
        to_date(col("order_date"),"yyyy-MM-dd"),
        to_date(col("order_date"),"dd/MM/yyyy"),
        to_date(col("order_date"),"yyyy/MM/dd")
    )
)

# ---------------------------------------------------------
# Convert back to DynamicFrame
# ---------------------------------------------------------
clean_orders_dyf = DynamicFrame.fromDF(
    orders_df,
    glueContext,
    "clean_orders_dyf"
)

# ---------------------------------------------------------
# Data Quality Check
# ---------------------------------------------------------
logger.info("Running Data Quality Checks")

run_data_quality(
    clean_orders_dyf,
    "orders_dq"
)

# ---------------------------------------------------------
# Write Silver Layer
# ---------------------------------------------------------
logger.info("Writing Orders data to Processed Layer")

write_to_processed(
    glueContext,
    clean_orders_dyf,
    ORDERS_OUTPUT
)
logger.info("Orders ETL Job Completed Successfully")

# ---------------------------------------------------------
# Commit
# ---------------------------------------------------------
job.commit()