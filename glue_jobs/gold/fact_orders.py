
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    GOLD_PREFIX,
    ORDERS_INPUT,
    DIM_CUSTOMER_OUTPUT,
    DIM_PRODUCT_OUTPUT,
    FACT_ORDERS_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.writer import write_to_gold
from common.logger import get_logger

glueContext, spark, job = initialize_glue_job()

logger = get_logger("fact_orders")
logger.info("Fact Orders Job Started")
# ---------------------------------------------------------
# Read Silver Orders
# ---------------------------------------------------------
orders_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[ f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{ORDERS_INPUT}/"]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Read Gold Customer Dimension
# ---------------------------------------------------------
customer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[f"s3://{S3_BUCKET}/{GOLD_PREFIX}{DIM_CUSTOMER_OUTPUT}/"]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Read Gold Product Dimension
# ---------------------------------------------------------
product_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[f"s3://{S3_BUCKET}/{GOLD_PREFIX}{DIM_PRODUCT_OUTPUT}/"]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Join Orders + Customers
# ---------------------------------------------------------
fact_df = orders_df.join(
    customer_df,
    "customer_id",
    "left"
)

# ---------------------------------------------------------
# Join Products
# ---------------------------------------------------------
fact_df = fact_df.join(
    product_df,
    "product_id",
    "left"
)

# ---------------------------------------------------------
# Select Required Columns
# ---------------------------------------------------------
fact_df = fact_df.select(
    "order_id",
    "customer_id",
    "first_name",
    "last_name",
    "product_id",
    "product_name",
    "category",
    "brand",
    "quantity",
    "order_amount",
    "order_status",
    "order_date"
)

# ---------------------------------------------------------
# Convert to DynamicFrame
# ---------------------------------------------------------
fact_dyf = DynamicFrame.fromDF(
    fact_df,
    glueContext,
    "fact_dyf"
)

# ---------------------------------------------------------
# Write Gold Layer
# ---------------------------------------------------------
logger.info("Writing Fact Orders to Gold Layer")

write_to_gold(
    glueContext,
    fact_dyf,
    FACT_ORDERS_OUTPUT
)
logger.info("Fact Orders Job Completed Successfully")

job.commit()