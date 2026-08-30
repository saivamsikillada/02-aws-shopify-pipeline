
from awsglue.dynamicframe import DynamicFrame
from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    PRODUCTS_INPUT,
    DIM_PRODUCT_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.writer import write_to_gold
from common.logger import get_logger

glueContext, spark, job = initialize_glue_job()

logger = get_logger("dim_product")
logger.info("Dim Product Job Started")

# ---------------------------------------------------------
# Read Silver Product Data
# ---------------------------------------------------------

product_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{PRODUCTS_INPUT}/"
        ]
    },
    format="parquet"
)

product_df = product_dyf.toDF()

# ---------------------------------------------------------
# Create Product Dimension
# ---------------------------------------------------------

dim_product_df = product_df.select(
    "product_id",
    "product_name",
    "category",
    "brand",
    "supplier",
    "unit_price",
    "cost_price",
    "launch_date",
    "status"
)

dim_product_dyf = DynamicFrame.fromDF(
    dim_product_df,
    glueContext,
    "dim_product_dyf"
)

# ---------------------------------------------------------
# Write Gold Layer
# ---------------------------------------------------------

logger.info("Writing Dim Product to Gold Layer")

write_to_gold(
    glueContext,
    dim_product_dyf,
    DIM_PRODUCT_OUTPUT
)
logger.info("Dim Product Job Completed Successfully")

job.commit()