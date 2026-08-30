# System Architecture

## Overview

The Shopify Data Pipeline follows the Medallion Architecture (Bronze, Silver, and Gold) to ingest, transform, and analyze e-commerce data using AWS services.

## Architecture Components

- Amazon S3
- AWS Glue Crawlers
- AWS Glue Data Catalog
- AWS Glue ETL Jobs
- Amazon Athena
- Apache Airflow
- GitLab CI/CD

## Data Flow

```
Source Data
     │
     ▼
Amazon S3 (Raw Layer)
     │
     ▼
Glue Crawlers
     │
     ▼
Glue Data Catalog
     │
     ▼
Silver ETL Jobs
     │
     ▼
Processed Layer (S3)
     │
     ▼
Gold ETL Jobs
     │
     ▼
Gold Layer (S3)
     │
     ▼
Amazon Athena
     │
     ▼
Business Reports
```

## Layers

### Bronze Layer
- Stores raw Shopify datasets.
- No transformations are applied.

### Silver Layer
- Cleans and standardizes data.
- Removes duplicates.
- Handles missing values.
- Applies business rules.

### Gold Layer
- Builds analytical datasets.
- Creates dimension and fact tables.
- Optimized for reporting and analytics.