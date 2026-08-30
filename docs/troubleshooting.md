# Troubleshooting Guide

## Overview

This document lists common issues that may occur during the deployment and execution of the Shopify Data Pipeline, along with recommended solutions.

---

## 1. AWS Glue Job Failure

### Issue

Glue ETL job fails during execution.

### Possible Causes

- Incorrect IAM permissions
- Missing input files in Amazon S3
- Invalid Python script
- Schema mismatch

### Resolution

- Verify IAM role permissions.
- Confirm the source files exist in the configured S3 bucket.
- Review the Glue job logs in AWS CloudWatch.
- Validate the schema before rerunning the job.

---

## 2. Glue Crawler Not Creating Tables

### Issue

Crawler completes but no tables appear in the Glue Data Catalog.

### Resolution

- Verify the S3 path configured for the crawler.
- Ensure supported file formats are used.
- Check crawler permissions.
- Run the crawler again after uploading the data.

---

## 3. Airflow DAG Not Visible

### Issue

The DAG does not appear in the Airflow UI.

### Resolution

- Verify the DAG is located in the `airflow/dags` folder.
- Check for Python syntax errors.
- Review Airflow scheduler logs.
- Restart the Airflow scheduler if necessary.

---

## 4. Athena Query Failure

### Issue

Athena query execution fails.

### Resolution

- Verify the selected database.
- Confirm the table exists in the Glue Data Catalog.
- Ensure the S3 output location is configured.
- Validate SQL syntax.

---

## 5. GitLab CI/CD Pipeline Failure

### Issue

The pipeline fails during validation or testing.

### Resolution

- Review the pipeline logs.
- Verify all required dependencies are listed in `requirements.txt`.
- Ensure all test files are present.
- Resolve Python syntax or import errors before rerunning the pipeline.

---

## Best Practices

- Use descriptive commit messages.
- Validate ETL jobs before deployment.
- Monitor Glue job execution logs.
- Keep Airflow DAGs modular and maintainable.
- Version control all SQL scripts and ETL code.