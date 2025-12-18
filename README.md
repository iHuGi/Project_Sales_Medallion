# Retail Sales Medallion Pipeline (DLT & PySpark)

This repository contains a professional **Medallion Architecture** implementation using **Databricks Delta Live Tables (DLT)**. The pipeline automates the transformation of raw customer and retail datasets into analytics-ready Gold-layer tables.

### 🛠️ Key Engineering Features

* **Hardened Data Integrity**: Implements strict **DLT Expectations** (`@dlt.expect_or_drop`) to quarantine records with NULL identifiers or invalid business values (e.g., negative quantities) before they reach the Silver layer.
* **Advanced Time-Series ETL**: Utilizes **Spark Window Functions** to calculate complex business KPIs, including **Month-over-Month (MoM) revenue growth** and customer lifetime spend.
* **Automated Data Cleaning**: Employs robust regex and coalesce logic to normalize inconsistent date formats and standardize categorical values (e.g., gender and loyalty tiers).
* **Lakehouse Architecture**: Fully integrated with **Unity Catalog Volumes** for file management and leverages automated schema evolution to ensure pipeline resilience.

### 🏗️ Data Flow

1. **Bronze**: Raw ingestion from JSON/CSV volumes with automated schema capture.
2. **Silver**: Data filtering, standardization, and integrity enforcement via SQL-based expectations.
3. **Gold**: Final aggregation and time-series modeling optimized for Power BI and executive dashboards.