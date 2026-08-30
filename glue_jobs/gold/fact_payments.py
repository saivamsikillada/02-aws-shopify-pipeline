
from awsglue.dynamicframe import DynamicFrame
from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    GOLD_PREFIX,
    ORDERS_INPUT,
    PAYMENTS_INPUT,
    FACT_PAYMENTS_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.writer import write_to_gold
from common.logger import get_logger

glueContext, spark, job = initialize_glue_job()

logger = get_logger("fact_payments")
logger.info("Fact Payments Job Started")

# ---------------------------------------------------------
# Read Silver Orders
# ---------------------------------------------------------
orders_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{ORDERS_INPUT}/"]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Read Silver Payments
# ---------------------------------------------------------
payments_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths":[f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{PAYMENTS_INPUT}/"]
    },
    format="parquet"
).toDF()

# ---------------------------------------------------------
# Join Orders + Payments
# ---------------------------------------------------------
fact_payment_df = orders_df.join(
    payments_df,
    "order_id",
    "left"
)

# ---------------------------------------------------------
# Select Columns
# ---------------------------------------------------------
fact_payment_df = fact_payment_df.select(
    "payment_id",
    "order_id",
    "customer_id",
    "product_id",
    "payment_method",
    "amount",
    "payment_status",
    "transaction_time",
    "store_location",
    "order_amount",
    "order_status",
    "order_date"
)

# ---------------------------------------------------------
# Convert to DynamicFrame
# ---------------------------------------------------------
fact_payment_dyf = DynamicFrame.fromDF(
    fact_payment_df,
    glueContext,
    "fact_payment_dyf"
)

# ---------------------------------------------------------
# Write Gold Layer
# ---------------------------------------------------------
logger.info("Writing Fact Payments to Gold Layer")

write_to_gold(
    glueContext,
    fact_payment_dyf,
    FACT_PAYMENTS_OUTPUT
)
logger.info("Fact Payments Job Completed Successfully")

job.commit()