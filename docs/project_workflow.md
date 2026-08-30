# Project Workflow

## Overview

This project implements an end-to-end data engineering pipeline for Shopify sales data using AWS services and Medallion Architecture.

## Workflow

### Step 1: Data Ingestion

- Source datasets are uploaded to Amazon S3 Raw Layer.
- Files include:
  - Customers
  - Products
  - Orders
  - Payments
  - Inventory

---

### Step 2: Data Discovery

AWS Glue Crawlers scan the raw data and create metadata in the AWS Glue Data Catalog.

---

### Step 3: Silver Layer Processing

AWS Glue ETL jobs transform the raw datasets by:

- Removing duplicate records
- Handling missing values
- Applying data type conversions
- Performing data quality validations
- Writing cleaned data to the Processed Layer in Amazon S3

---

### Step 4: Gold Layer Processing

AWS Glue ETL jobs create analytical datasets including:

- Customer Dimension
- Product Dimension
- Orders Fact
- Payments Fact
- Inventory Summary

These datasets are stored in the Gold Layer in Amazon S3.

---

### Step 5: Workflow Orchestration

Apache Airflow orchestrates the execution of Glue ETL jobs in the required sequence using GlueJobOperator.

---

### Step 6: Data Validation

GitLab CI/CD validates the project by:

- Checking Python syntax
- Running automated tests
- Validating Airflow DAG imports
- Verifying project structure

---

### Step 7: Analytics

Amazon Athena queries the Gold Layer to generate business reports and analytical insights.