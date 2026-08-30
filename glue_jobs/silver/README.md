# Silver Layer ETL Jobs

## Overview

The Silver layer is responsible for cleansing, validating, and transforming raw data into standardized datasets.

## ETL Jobs

| Job | Description |
|------|-------------|
| customer_etl.py | Cleans and transforms customer data |
| product_etl.py | Cleans and transforms product data |
| orders_etl.py | Cleans and transforms order data |
| payments_etl.py | Cleans and transforms payment data |
| inventory_etl.py | Cleans and transforms inventory data |

## Output Location

Amazon S3

```
processed/
├── customers/
├── products/
├── orders/
├── payments/
└── inventory/
```

## Technology

- AWS Glue
- PySpark
- Amazon S3
- AWS Glue Data Catalog

## Refactoring

The Silver ETL jobs use shared utilities from the `common` package:

- constants.py
- glue_context.py
- quality.py
- writer.py
- logger.py

This avoids duplicate code across Glue jobs and improves maintainability.