# transportation-lakeflow-pipeline

An end-to-end modern data engineering project built on **Databricks Lakehouse Platform** using **PySpark**, **Delta Lake**, and **Lakeflow Declarative Pipelines (Delta Live Tables)**. This project demonstrates how to build a scalable, production-ready data pipeline following the Medallion Architecture.

---

## 🚀 Project Overview

This project ingests raw data, validates data quality, transforms datasets through Bronze, Silver, and Gold layers, and produces analytics-ready tables.

The project demonstrates real-world Databricks data engineering concepts including:

- Auto Loader for incremental ingestion
- Medallion Architecture (Bronze, Silver, Gold)
- Delta Lake
- Change Data Capture (CDC)
- Data Quality Expectations
- Incremental Processing
- Streaming Pipelines
- Data Optimization
- PySpark Transformations

---

## 🛠️ Technologies Used

- Databricks
- PySpark
- SQL
- Delta Lake
- Lakeflow Declarative Pipelines (DLT)
- Spark Structured Streaming
- Auto Loader
- Unity Catalog
- GitHub

---

## 📂 Repository Structure

```
transportation-lakeflow-pipeline/
│
├── bronze/
│   ├── ingestion.py
│   └── autoloader.py
│
├── silver/
│   ├── transformations.py
│   ├── cleansing.py
│   └── cdc.py
│
├── gold/
│   ├── business_metrics.py
│   └── reporting.py
│
├── utilities/
│   ├── helper.py
│   └── config.py
│
├── notebooks/
│
├── images/
│
└── README.md
```

---

## ✅ Data Quality

The pipeline implements multiple data quality validations including:

- Required field validation
- Null checks
- Duplicate detection
- Valid date validation
- Rating validation
- Business rule enforcement

Invalid records are automatically quarantined for further analysis.

---

## 📈 Features

- Incremental file processing
- Streaming architecture
- Delta Lake ACID transactions
- Schema evolution
- Change Data Capture
- Slowly Changing Data handling
- Data Quality Expectations
- Production-ready architecture
- Modular PySpark code

---

## 📊 Medallion Architecture

| Layer | Purpose |
|--------|----------|
| Bronze | Raw ingestion |
| Silver | Cleansed and validated data |
| Gold | Business-ready aggregated data |

---

## 📚 Learning Outcomes

Through this project I learned:

- Building scalable data pipelines
- Databricks Lakehouse architecture
- PySpark DataFrame APIs
- Delta Lake optimization
- Auto Loader
- Lakeflow Declarative Pipelines
- Data Quality Expectations
- Incremental ETL
- CDC implementation
- Production data engineering best practices

---

## 🙏 Acknowledgements

This project was built as part of my Databricks Data Engineering learning journey and inspired by hands-on industry tutorials. It has been customized with my own implementation, code organization.

---

## 👤 Author

**Nagarjuna**

If you found this project useful, feel free to ⭐ this repository.
