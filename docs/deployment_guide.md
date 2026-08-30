# Deployment Guide

## Prerequisites

Before deploying the project, ensure the following are available:

- AWS Account
- Amazon S3 Bucket
- AWS Glue
- AWS Glue Crawlers
- AWS Glue Data Catalog
- Amazon Athena
- Apache Airflow
- GitLab Repository
- Python 3.11

---

## Deployment Steps

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd shopify-data-pipeline
```

### Step 2: Upload Source Data

Upload the Shopify datasets to the Raw Layer in the configured Amazon S3 bucket.

---

### Step 3: Configure AWS Glue

- Create Glue Crawlers.
- Run the crawlers to populate the Glue Data Catalog.
- Verify that all source tables are available.

---

### Step 4: Execute Silver ETL Jobs

Run the following Glue jobs:

- customer_etl_job
- product_etl_job
- Orders_etl_job
- payments_etl_job
- Inventory_etl_job

Verify that the transformed datasets are written to the Processed Layer.

---

### Step 5: Execute Gold ETL Jobs

Run the following Glue jobs:

- gold_dim_customer
- gold_dim_product
- gold_fact_orders
- gold_fact_payments
- gold_inventory_summary

Verify that the analytical datasets are written to the Gold Layer.

---

### Step 6: Configure Apache Airflow

- Install project dependencies.
- Configure the AWS connection.
- Deploy the DAG.
- Trigger the `shopify_data_pipeline` workflow.

---

### Step 7: Validate the Pipeline

- Confirm that all Glue jobs complete successfully.
- Review Airflow task logs.
- Verify the output datasets in Amazon S3.

---

### Step 8: Query with Amazon Athena

Execute the SQL queries available in the `athena/queries` folder to analyze the Gold Layer datasets.