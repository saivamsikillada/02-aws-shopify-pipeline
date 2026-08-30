from common.constants import (
    S3_BUCKET,
    DATABASE_NAME,
    MYSQL_TABLE,
    PROCESSED_PREFIX,
    CUSTOMERS_OUTPUT
)
import sys
from awsglue.transforms import *
from common.glue_context import initialize_glue_job
from common.quality import run_data_quality
from common.writer import write_to_processed
from common.logger import get_logger
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import *

# ------------------------------------------------------------------
# Initialize Glue Job
# ------------------------------------------------------------------
glueContext, spark, job = initialize_glue_job()

logger = get_logger("customer_etl")
logger.info("Customer ETL Job Started")


# ------------------------------------------------------------------
# Read Data from Glue Catalog
# ------------------------------------------------------------------
customer_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=MYSQL_TABLE
)

# ------------------------------------------------------------------
# Apply Schema Mapping
# ------------------------------------------------------------------
customer_dyf = ApplyMapping.apply(
    frame=customer_dyf,
    mappings=[
        ("customer_id","string","customer_id","string"),
        ("first_name","string","first_name","string"),
        ("last_name","string","last_name","string"),
        ("email","string","email","string"),
        ("phone","long","phone","long"),
        ("gender","string","gender","string"),
        ("city","string","city","string"),
        ("state","string","state","string"),
        ("country","string","country","string"),
        ("registration_date","string","registration_date","string"),
        ("customer_status","string","customer_status","string")
    ]
)

# ------------------------------------------------------------------
# Convert DynamicFrame to DataFrame
# ------------------------------------------------------------------
customer_df = customer_dyf.toDF()

# ------------------------------------------------------------------
# Remove Duplicate Customers
# ------------------------------------------------------------------
customer_df = customer_df.dropDuplicates(["customer_id"])

# ------------------------------------------------------------------
# Trim Leading and Trailing Spaces
# ------------------------------------------------------------------
customer_df = (
    customer_df
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
)

# ------------------------------------------------------------------
# Convert Status to Uppercase
# ------------------------------------------------------------------
customer_df = customer_df.withColumn(
    "customer_status",
    upper(col("customer_status"))
)

# ------------------------------------------------------------------
# Fill Missing Phone Numbers
# ------------------------------------------------------------------
customer_df = customer_df.fillna({
    "phone": 0
})

# ------------------------------------------------------------------
# Keep Only Valid Emails
# ------------------------------------------------------------------
customer_df = customer_df.filter(
    col("email").contains("@")
)

# ------------------------------------------------------------------
# Standardize Registration Date
# ------------------------------------------------------------------
customer_df = customer_df.withColumn(
    "registration_date",
    coalesce(
        to_date(col("registration_date"), "yyyy-MM-dd"),
        to_date(col("registration_date"), "dd/MM/yyyy"),
        to_date(col("registration_date"), "yyyy/MM/dd")
    )
)

# ------------------------------------------------------------------
# Convert Back to DynamicFrame
# ------------------------------------------------------------------
clean_customer_dyf = DynamicFrame.fromDF(
    customer_df,
    glueContext,
    "clean_customer_dyf"
)

# ------------------------------------------------------------------
# Data Quality Check
# ------------------------------------------------------------------
logger.info("Running Data Quality Checks")

run_data_quality(
    clean_customer_dyf,
    "customer_dq"
)

# ------------------------------------------------------------------
# Write to S3 (Processed Layer)
# ------------------------------------------------------------------
logger.info("Writing Customer data to Processed Layer")

write_to_processed(
    glueContext,
    clean_customer_dyf,
    CUSTOMERS_OUTPUT
)

# ------------------------------------------------------------------
# Commit Job
# ------------------------------------------------------------------
logger.info("Customer ETL Job Completed Successfully")
job.commit()