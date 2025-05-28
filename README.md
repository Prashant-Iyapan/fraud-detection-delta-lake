
# Fraud Detection Pipeline with Delta Lake (Bronze → Silver → Gold)

This repository showcases a modular, streaming-friendly data pipeline for fraud detection using Apache Spark and Delta Lake. It follows a lakehouse architecture that mimics real-world streaming ingestion, enrichment, and analytics workflows.

---

## 🚀 Architecture Overview

```
             ┌─────────────┐
             │ JSON Streams│
             └─────┬───────┘
                   ▼
         ┌──────────────────┐
         │  Bronze Layer    │
         │------------------│
         │ - Streaming ingest
         │ - PII hashing
         │ - DQ checks
         └──────────────────┘
                   ▼
         ┌──────────────────┐
         │  Silver Layer    │
         │------------------│
         │ - Enrichment with lookups
         │ - Flags for missing data
         │ - usage_cost_ratio
         └──────────────────┘
                   ▼
         ┌──────────────────┐
         │   Gold Layer     │
         │------------------│
         │ - Spend summary
         │ - Usage metrics
         │ - Risky txn flags
         └──────────────────┘
```

---

## 💡 Key Features

- ✅ **Layered Delta Lake Architecture** (Bronze, Silver, Gold)
- ✅ **Structured Streaming ingestion** from JSON sources
- ✅ **PII hashing** (`ip_address`, `card_number`)
- ✅ **Broadcast joins** to enrich transactions
- ✅ **Data quality checks** written to timestamped folders
- ✅ **Modular class-based design** for scalability
- ✅ **Supports both batch and streaming mode**
- ✅ **Ready for dashboarding in QuickSight or Athena**

---

## 📂 Project Structure

```
.
├── main.py                   # Orchestrates layer-wise execution
├── src/
│   ├── config.py             # S3 paths and global settings
│   ├── logger.py             # Centralized structured logger
│   ├── ingestion_bronze.py   # Streaming ingestion logic
│   ├── transformation_silver.py # Silver enrichment logic
│   ├── enrichment_gold.py    # Gold aggregations & risk scoring
│   ├── validation.py         # Schema validators
│   └── data_quality.py       # DQ rules and reporting
└── data/                     # (ignored) Sample ingestion data
```

---

## 📊 Output Tables

| Layer  | Output Path                          | Description                    |
|--------|--------------------------------------|--------------------------------|
| Bronze | `s3://.../delta/bronze/`             | Raw stream + hashed fields     |
| Silver | `s3://.../delta/silver/`             | Enriched, DQ-checked records   |
| Gold   | `s3://.../delta/gold/*`              | Aggregates + risk flags        |

---

## 📈 Next Steps

- 🔧 Enable streaming in Silver and Gold
- 📊 Connect Gold tables to QuickSight via Athena
- 🧪 Add unit tests and metadata logger
- 🚨 Add alerting on DQ violations or anomaly spikes

---

## 🧠 Author

**Prashant Iyapan**  
Master’s in Entertainment Technology  
Big Data Engineer | Spark | PySpark | Delta Lake | AWS  
📫 [LinkedIn](https://www.linkedin.com/in/your-link)

---

## 📝 License

This project is open-source under the [MIT License](LICENSE).
