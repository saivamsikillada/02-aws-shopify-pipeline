# 🚀 AWS Shopify End-to-End Data Engineering Pipeline

## 📌 Project Overview

This project demonstrates an end-to-end AWS Data Engineering Pipeline built using the Medallion Architecture (Bronze, Silver, and Gold). The pipeline extracts data from multiple enterprise source systems, processes and transforms it using AWS Glue ETL jobs, stores data in Amazon S3, registers metadata in the AWS Glue Data Catalog, and enables analytics through Amazon Athena. Apache Airflow is used to orchestrate the complete workflow, while GitLab and GitLab CI provide version control and automated code validation.

---

## 🏗️ Architecture

```
                GitLab Repository
                       │
                GitLab CI Pipeline
                       │
                       ▼
                Apache Airflow
             (Workflow Orchestration)
                       │
                       ▼
                AWS Glue ETL Jobs
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   Bronze S3      Silver S3      Gold S3
                       │
                       ▼
             AWS Glue Data Catalog
                       │
                       ▼
                Amazon Athena
```

---

## 📂 Source Systems

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
- GitLab
- GitLab CI

---

## 📁 Project Structure

```text
02-aws-shopify-pipeline/
├── airflow/
├── config/
├── docs/
├── glue_jobs/
├── queries/
├── scripts/
├── tests/
├── utils/
├── views/
├── .env
├── .gitignore
├── .gitlab-ci.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── s3_setup.md
└── README.md

---

## ⚙️ Project Workflow

1. Extract data from multiple enterprise source systems.
2. Store raw data in the Bronze layer of Amazon S3.
3. Process and clean data using AWS Glue ETL jobs.
4. Store transformed datasets in the Silver layer.
5. Create business-ready datasets in the Gold layer.
6. Register tables in the AWS Glue Data Catalog.
7. Query processed data using Amazon Athena.
8. Orchestrate the complete ETL workflow using Apache Airflow.
9. Validate code changes automatically using GitLab CI.

---

## 🔄 Airflow Orchestration Flow

```
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

## ✅ Project Features

- End-to-End AWS Data Engineering Pipeline
- Medallion Architecture (Bronze → Silver → Gold)
- Automated ETL using AWS Glue
- Workflow Orchestration using Apache Airflow
- Metadata Management using AWS Glue Data Catalog
- SQL Analytics using Amazon Athena
- Dockerized Airflow Environment
- Version Control using Git & GitLab
- Automated Validation using GitLab CI Pipeline



## 📊 AWS Services Used

- Amazon S3
- AWS Glue
- AWS Glue Data Catalog
- Amazon Athena
- AWS IAM

---

## 🎯 Project Outcome

- Built a scalable cloud-native Data Engineering pipeline.
- Successfully orchestrated AWS Glue ETL jobs using Apache Airflow.
- Processed enterprise data from multiple source systems.
- Implemented GitLab CI for automated project validation.
- Enabled SQL-based analytics using Amazon Athena.
- Designed a modular and production-style ETL workflow following the Medallion Architecture.

---

## 👩‍💻 Author

Vamsi

End-to-End AWS Data Engineering Project using Apache Airflow, AWS Glue, Amazon S3, Amazon Athena, Docker, Git, and GitLab CI.


