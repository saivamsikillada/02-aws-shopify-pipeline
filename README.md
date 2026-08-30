# 🚀 AWS Shopify End-to-End Data Engineering Pipeline

## 📌 Project Overview

This project demonstrates an end-to-end AWS Data Engineering Pipeline built using the Medallion Architecture (Bronze, Silver, and Gold).

The pipeline extracts data from multiple enterprise source systems, stores raw data in Amazon S3, processes and transforms the data using AWS Glue ETL jobs, stores transformed datasets in S3, registers metadata using the AWS Glue Data Catalog, and enables analytics using Amazon Athena.

Apache Airflow is used to orchestrate the complete workflow, while GitHub and GitHub Actions are used for version control and automated code validation.

---

## 🏗️ Architecture

```text
                    GitHub Repository
                           │
                    GitHub Actions
                           │
                           ▼
                    Apache Airflow
                 (Workflow Orchestration)
                           │
                           ▼
                    AWS Glue ETL Jobs
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
        Bronze S3     Silver S3       Gold S3
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                AWS Glue Data Catalog
                           │
                           ▼
                    Amazon Athena
```

---

## 📂 Source Systems

The pipeline is designed to process data from multiple enterprise source systems:

- MySQL
- Salesforce
- SAP HANA
- POS
- Teradata

---

## 🛠️ Technology Stack

- Python
- SQL
- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- Apache Airflow
- Docker
- Git
- GitHub
- GitHub Actions

---

## 📁 Project Structure

```text
02-aws-shopify-pipeline/
│
├── airflow/
│   ├── config/
│   ├── dags/
│   ├── plugins/
│   ├── requirements.txt
│   └── README.md
│
├── config/
│   ├── aws_config.py
│   └── config.py
│
├── docs/
│   ├── architecture.md
│   ├── deployment_guide.md
│   ├── project_workflow.md
│   ├── troubleshooting.md
│   └── ppt/
│
├── glue_jobs/
│   ├── common/
│   │   ├── constants.py
│   │   ├── glue_context.py
│   │   ├── logger.py
│   │   ├── quality.py
│   │   └── writer.py
│   │
│   ├── silver/
│   │   ├── customer_etl.py
│   │   ├── product_etl.py
│   │   ├── orders_etl.py
│   │   ├── payments_etl.py
│   │   └── inventory_etl.py
│   │
│   ├── gold/
│   │   ├── dim_customer.py
│   │   ├── dim_product.py
│   │   ├── fact_orders.py
│   │   ├── fact_payments.py
│   │   └── inventory_summary.py
│   │
│   ├── logs/
│   └── requirements.txt
│
├── queries/
├── scripts/
├── tests/
├── utils/
├── views/
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── s3_setup.md
└── README.md
```

> `.env` is intentionally excluded from version control because it may contain sensitive configuration or credentials.

---

## ⚙️ Project Workflow

```text
Source Systems
      │
      ▼
Amazon S3 - Bronze / RAW
      │
      ▼
AWS Glue Crawler
      │
      ▼
AWS Glue Data Catalog
      │
      ▼
AWS Glue Silver ETL
      │
      ├── Customer
      ├── Product
      ├── Orders
      ├── Payments
      └── Inventory
      │
      ▼
Amazon S3 - Silver / PROCESSED
      │
      ▼
AWS Glue Gold ETL
      │
      ├── Dimension Customer
      ├── Dimension Product
      ├── Fact Orders
      ├── Fact Payments
      └── Inventory Summary
      │
      ▼
Amazon S3 - Gold
      │
      ▼
AWS Glue Data Catalog
      │
      ▼
Amazon Athena
```

---

## 🔄 Airflow Orchestration Flow

```text
Start
   │
   ▼
Customer ETL
   │
   ▼
Product ETL
   │
   ▼
Orders ETL
   │
   ▼
Payments ETL
   │
   ▼
Inventory ETL
   │
   ▼
Gold Dimension Customer
   │
   ▼
Gold Dimension Product
   │
   ▼
Gold Fact Orders
   │
   ▼
Gold Fact Payments
   │
   ▼
Gold Inventory Summary
   │
   ▼
End
```

---

## 🔄 GitHub Development Workflow

```text
Developer
    │
    ▼
Local Project
    │
    ▼
Git
    │
    ▼
GitHub Repository
    │
    ▼
GitHub Actions
    │
    ├── Code Validation
    ├── Import Tests
    └── Glue/Airflow Tests
    │
    ▼
Deployment / Execution
```

---

## 🥉 Bronze Layer

The Bronze layer contains the raw source data.

Example:

```text
S3
└── RAW/
    ├── mysql/
    ├── pos/
    ├── salesforce/
    ├── saphana/
    └── teradata/
```

The data is stored in its original/raw form with minimal transformation.

---

## 🥈 Silver Layer

The Silver layer contains cleaned and standardized data.

Typical transformations include:

- Schema mapping
- Data type conversion
- Duplicate removal
- Trimming whitespace
- Standardizing values
- Null handling
- Email validation
- Date standardization
- Data quality checks

Example:

```text
S3
└── PROCESSED/
    ├── customers/
    ├── products/
    ├── orders/
    ├── payments/
    └── inventory/
```

---

## 🥇 Gold Layer

The Gold layer contains business-ready datasets designed for analytics.

Example:

```text
S3
└── GOLD/
    ├── dim_customer/
    ├── dim_product/
    ├── fact_orders/
    ├── fact_payments/
    └── inventory_summary/
```

---

## 🔍 AWS Glue Data Catalog

AWS Glue Crawler is used to discover schemas from data stored in Amazon S3.

The crawler creates and updates metadata in the AWS Glue Data Catalog.

Example:

```text
S3
   │
   ▼
Glue Crawler
   │
   ▼
Glue Data Catalog
   │
   ├── Database: shopify_db
   └── Tables
        ├── mysql
        ├── pos
        ├── salesforce
        ├── saphana
        └── teradata
```

Glue ETL jobs can then read data through the Data Catalog.

---

## 🧹 Data Quality

The pipeline includes data quality validation as part of the Silver transformation process.

Examples include:

- Duplicate detection
- Null checks
- Email validation
- Schema validation
- Data type validation
- Date validation

Invalid records can be identified before the data reaches downstream analytical layers.

---

## 🛡️ Data Deduplication

Customer records are deduplicated using the business key:

```text
customer_id
```

Example:

```python
df.dropDuplicates(["customer_id"])
```

For scenarios where the latest record must be retained, window functions can be used based on an appropriate timestamp.

---

## 📊 Analytics

Amazon Athena is used to query the processed datasets using SQL.

Example analytical areas:

- Customer analysis
- Product analysis
- Sales analysis
- Payment analysis
- Inventory analysis

---

## 🤖 Automation and Orchestration

Apache Airflow orchestrates the complete data pipeline.

Airflow is responsible for:

- Triggering ETL jobs
- Managing task dependencies
- Handling retries
- Monitoring pipeline execution
- Scheduling workflows

---

## 🔐 Security

AWS IAM is used to control access to:

- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena

Sensitive credentials are not committed to GitHub.

The `.env` file is excluded using `.gitignore`.

---

## 🧪 Testing

The project includes automated tests for:

- Airflow DAG validation
- Glue job validation
- Python imports
- Project configuration

Tests are maintained under:

```text
tests/
```

---

## 🚀 CI/CD

GitHub Actions is used for automated code validation.

The workflow is:

```text
Code Change
    │
    ▼
GitHub
    │
    ▼
GitHub Actions
    │
    ├── Run Tests
    ├── Validate Code
    └── Check Imports
```

---

## 📊 AWS Services Used

- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- AWS IAM

---

## 🎯 Project Outcome

The project demonstrates a production-style cloud data engineering pipeline using AWS services.

Key outcomes include:

- Built an end-to-end AWS Data Engineering pipeline.
- Implemented Bronze, Silver and Gold data layers.
- Used Amazon S3 as the cloud data lake.
- Implemented AWS Glue ETL transformations.
- Implemented AWS Glue Data Catalog for metadata management.
- Implemented data quality and deduplication.
- Created business-ready Gold datasets.
- Enabled SQL analytics using Amazon Athena.
- Orchestrated workflows using Apache Airflow.
- Implemented Git-based version control using GitHub.
- Implemented automated validation using GitHub Actions.
- Designed a modular and scalable ETL architecture.

---

## 👨‍💻 Author

**Vamsi**
