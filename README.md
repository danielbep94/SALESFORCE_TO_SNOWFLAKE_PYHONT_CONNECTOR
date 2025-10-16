# SALESFORCE_TO_SNOWFLAKE_PYHONT_CONNECTOR



# 🌊 Danone DataStream: Salesforce to Snowflake Pipeline

**Danone DataStream** is a high-performance, Python-based Extract-Transform-Load (ETL) pipeline designed to ingest large volumes of critical Account data from Salesforce into Snowflake, creating a single, reliable source for Business Intelligence (BI).

## 🚀 The Power BI Bridge

This pipeline is the foundational layer for your enterprise reporting. By strictly enforcing data types, converting files to **Parquet format**, and loading directly into Snowflake, it ensures **Power BI** consumes clean, performant, and correctly structured data, dramatically improving dashboard load times and data accuracy.

---

## ✨ Core Pipeline Features

* **Salesforce Bulk API:** Utilizes the Salesforce Bulk API 2.0 to efficiently extract high-volume datasets, ensuring speed and reliability for millions of records.
* **Optimized Transformation:** Employs **Pandas** and **PyArrow** to process data in memory-efficient chunks, converting streaming CSV results into highly compressed, column-oriented **Apache Parquet** files.
* **Strict Type Safety:** Uses an explicit `PyArrow Schema` to guarantee data integrity, preventing common BI issues like incorrect timestamp handling or string/numeric mix-ups. All timestamps are standardized to **UTC**.
* **Snowflake Native Load:** Leverages Snowflake's `PUT` and `COPY INTO` commands for parallel, cost-efficient loading of Parquet data, which bypasses traditional data warehousing bottlenecks.
* **Modular & Maintainable:** The code is cleanly separated into distinct phases (Auth, Extract, Transform, Load) for easy debugging and future enhancements.

---

## ⚙️ Setup and Execution

### 1. Prerequisites

* Python 3.8+
* Access to Salesforce Bulk API (via `CONSUMER_KEY`/`CONSUMER_SECRET`)
* Access to a Snowflake warehouse, database, and schema.

### 2. Installation

Install the required Python libraries:

```bash
pip install httpx pandas pyarrow snowflake-connector-python python-dotenv