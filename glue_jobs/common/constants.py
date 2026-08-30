"""
Common constants used across AWS Glue ETL jobs.
"""

AWS_REGION = "eu-north-1"

S3_BUCKET = "shopify-data-pipeline-ashwini"

RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"
GOLD_PREFIX = "gold/"

DATABASE_NAME = "shopify_db"

MYSQL_TABLE = "mysql"
POS_TABLE = "pos"
SALESFORCE_TABLE = "salesforce"
SAPHANA_TABLE = "saphana"
TERADATA_TABLE = "teradata"

CUSTOMERS_OUTPUT = "customers"
PRODUCTS_OUTPUT = "products"
ORDERS_OUTPUT = "orders"
PAYMENTS_OUTPUT = "payments"
INVENTORY_OUTPUT = "inventory"

DIM_CUSTOMER_OUTPUT = "dim_customer"
DIM_PRODUCT_OUTPUT = "dim_product"
FACT_ORDERS_OUTPUT = "fact_orders"
FACT_PAYMENTS_OUTPUT = "fact_payments"
INVENTORY_SUMMARY_OUTPUT = "inventory_summary"

CUSTOMERS_INPUT = "customers"
PRODUCTS_INPUT = "products"
ORDERS_INPUT = "orders"
PAYMENTS_INPUT = "payments"
INVENTORY_INPUT = "inventory"