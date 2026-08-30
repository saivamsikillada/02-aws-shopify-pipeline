from common.constants import (
    DATABASE_NAME,
    SAPHANA_TABLE,
    PRODUCTS_OUTPUT
)

from common.glue_context import initialize_glue_job
from common.quality import run_data_quality
from common.writer import write_to_processed
from common.logger import get_logger
# ------------------------------------------------------------------
# Initialize Glue Job
# ------------------------------------------------------------------
glueContext, spark, job = initialize_glue_job()

logger = get_logger("product_etl")
logger.info("Product ETL Job Started")




# ------------------------------------------------------------------
# Read Data from Glue Catalog
# ------------------------------------------------------------------
product_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=SAPHANA_TABLE
)

# ------------------------------------------------------------------
# Apply Schema Mapping
# ------------------------------------------------------------------
product_dyf = ApplyMapping.apply(
    frame=product_dyf,
    mappings=[
        ("product_id","string","product_id","string"),
        ("product_name","string","product_name","string"),
        ("category","string","category","string"),
        ("brand","string","brand","string"),
        ("supplier","string","supplier","string"),
        ("unit_price","long","unit_price","long"),
        ("cost_price","long","cost_price","long"),
        ("launch_date","string","launch_date","string"),
        ("status","string","status","string")
    ]
)

# ------------------------------------------------------------------
# Convert DynamicFrame to DataFrame
# ------------------------------------------------------------------
product_df = product_dyf.toDF()

# ------------------------------------------------------------------
# Remove Duplicate Products
# ------------------------------------------------------------------
product_df = product_df.dropDuplicates(["product_id"])

# ------------------------------------------------------------------
# Trim Spaces
# ------------------------------------------------------------------
product_df = (
    product_df
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("brand", trim(col("brand")))
    .withColumn("supplier", trim(col("supplier")))
)

# ------------------------------------------------------------------
# Convert Status to Uppercase
# ------------------------------------------------------------------
product_df = product_df.withColumn(
    "status",
    upper(col("status"))
)

# ------------------------------------------------------------------
# Fill Missing Supplier
# ------------------------------------------------------------------
product_df = product_df.fillna({
    "supplier": "UNKNOWN"
})

# ------------------------------------------------------------------
# Reject Invalid Prices
# ------------------------------------------------------------------
product_df = product_df.filter(
    (col("unit_price") > 0) &
    (col("cost_price") > 0)
)

# ------------------------------------------------------------------
# Standardize Launch Date
# ------------------------------------------------------------------
product_df = product_df.withColumn(
    "launch_date",
    coalesce(
        to_date(col("launch_date"), "yyyy-MM-dd"),
        to_date(col("launch_date"), "dd/MM/yyyy"),
        to_date(col("launch_date"), "yyyy/MM/dd")
    )
)

# ------------------------------------------------------------------
# Convert Back to DynamicFrame
# ------------------------------------------------------------------
clean_product_dyf = DynamicFrame.fromDF(
    product_df,
    glueContext,
    "clean_product_dyf"
)

# ------------------------------------------------------------------
# Data Quality Check
# ------------------------------------------------------------------
logger.info("Running Data Quality Checks")

run_data_quality(
    clean_product_dyf,
    "product_dq"
)

# ------------------------------------------------------------------
# Write to S3 (Silver Layer)
# ------------------------------------------------------------------
logger.info("Writing Product data to Processed Layer")

write_to_processed(
    glueContext,
    clean_product_dyf,
    PRODUCTS_OUTPUT
)
logger.info("Product ETL Job Completed Successfully")

# ------------------------------------------------------------------
# Commit Job
# ------------------------------------------------------------------
job.commit()