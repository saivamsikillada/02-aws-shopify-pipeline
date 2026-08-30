from awsglue.dynamicframe import DynamicFrame

from common.constants import (
    S3_BUCKET,
    PROCESSED_PREFIX,
    CUSTOMERS_INPUT,
    DIM_CUSTOMER_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.writer import write_to_gold
from common.logger import get_logger

glueContext, spark, job = initialize_glue_job()

logger = get_logger("dim_customer")
logger.info("Dim Customer Job Started")

# ---------------------------------------------------------
# Read Silver Customer Data
# ---------------------------------------------------------

customer_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}{CUSTOMERS_INPUT}/"
        ]
    },
    format="parquet"
)

customer_df = customer_dyf.toDF()

# ---------------------------------------------------------
# Create Customer Dimension
# ---------------------------------------------------------

dim_customer_df = customer_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "gender",
    "city",
    "state",
    "country",
    "customer_status",
    "registration_date"
)

dim_customer_dyf = DynamicFrame.fromDF(
    dim_customer_df,
    glueContext,
    "dim_customer_dyf"
)

# ---------------------------------------------------------
# Write Gold Layer
# ---------------------------------------------------------

logger.info("Writing Dim Customer to Gold Layer")

write_to_gold(
    glueContext,
    dim_customer_dyf,
    DIM_CUSTOMER_OUTPUT
)
logger.info("Dim Customer Job Completed Successfully")

job.commit()