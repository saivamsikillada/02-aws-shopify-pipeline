from common.constants import (
    DATABASE_NAME,
    POS_TABLE,
    PAYMENTS_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.quality import run_data_quality
from common.writer import write_to_processed
from common.logger import get_logger

# ---------------------------------------------------------
# Initialize Glue Job
# ---------------------------------------------------------
glueContext, spark, job = initialize_glue_job()

logger = get_logger("payments_etl")
logger.info("Payments ETL Job Started")



# ---------------------------------------------------------
# Read Data
# ---------------------------------------------------------
payment_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=POS_TABLE
)

# ---------------------------------------------------------
# Apply Mapping
# ---------------------------------------------------------
payment_dyf = ApplyMapping.apply(
    frame=payment_dyf,
    mappings=[
        ("payment_id","string","payment_id","string"),
        ("order_id","string","order_id","string"),
        ("payment_method","string","payment_method","string"),
        ("amount","long","amount","long"),
        ("payment_status","string","payment_status","string"),
        ("transaction_time","string","transaction_time","string"),
        ("store_location","string","store_location","string")
    ]
)

# ---------------------------------------------------------
# Convert to DataFrame
# ---------------------------------------------------------
payment_df = payment_dyf.toDF()

# ---------------------------------------------------------
# Remove Duplicates
# ---------------------------------------------------------
payment_df = payment_df.dropDuplicates(["payment_id"])

# ---------------------------------------------------------
# Trim Spaces
# ---------------------------------------------------------
payment_df = (
    payment_df
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("store_location", trim(col("store_location")))
)

# ---------------------------------------------------------
# Uppercase Status
# ---------------------------------------------------------
payment_df = payment_df.withColumn(
    "payment_status",
    upper(col("payment_status"))
)

# ---------------------------------------------------------
# Fill Missing Values
# ---------------------------------------------------------
payment_df = payment_df.fillna({
    "payment_method": "UNKNOWN",
    "store_location": "UNKNOWN"
})

# ---------------------------------------------------------
# Remove Invalid Amounts
# ---------------------------------------------------------
payment_df = payment_df.filter(
    col("amount") > 0
)

# ---------------------------------------------------------
# Standardize Timestamp
# ---------------------------------------------------------
payment_df = payment_df.withColumn(
    "transaction_time",
    coalesce(
        to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm:ss"),
        to_timestamp(col("transaction_time"), "yyyy/MM/dd HH:mm:ss"),
        to_timestamp(col("transaction_time"))
    )
)

# ---------------------------------------------------------
# Convert Back to DynamicFrame
# ---------------------------------------------------------
clean_payment_dyf = DynamicFrame.fromDF(
    payment_df,
    glueContext,
    "clean_payment_dyf"
)

# ---------------------------------------------------------
# Data Quality Check
# ---------------------------------------------------------
logger.info("Running Data Quality Checks")

run_data_quality(
    clean_payment_dyf,
    "payment_dq"
)

# ---------------------------------------------------------
# Write to Processed Layer
# ---------------------------------------------------------
logger.info("Writing Payments data to Processed Layer")

write_to_processed(
    glueContext,
    clean_payment_dyf,
    PAYMENTS_OUTPUT
)
logger.info("Payments ETL Job Completed Successfully")
# ---------------------------------------------------------
# Commit
# ---------------------------------------------------------
job.commit()