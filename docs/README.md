
# Fraud Detection Streaming Pipeline 🚀

## 🧠 Overview

This project is a **Spark Structured Streaming + Delta Lake** pipeline designed to simulate and process streaming transaction data across three layers: **Bronze**, **Silver**, and **Gold**. It demonstrates real-world data engineering practices like enrichment, data quality checks, partitioning, masking, broadcasting, and Delta optimizations.

---

## 📐 Architecture

```
JSON Source (S3 / Local) 
    ↓
[ Bronze Layer ]
    - Schema validation
    - PII masking and hashing
    - Partition by event_date

    ↓

[ Silver Layer ]
    - Data enrichment using 4 broadcast lookup tables
    - Null/missing flagging
    - Derived metrics (usage_cost_ratio)
    - Custom Data Quality checks

    ↓

[ Gold Layer ]
    - User spend summary
    - Product usage metrics
    - High-risk transaction flagging
    - Z-Ordering & VACUUM optimization
```

---

## 📁 Project Structure

```
fraud_detection_project/
├── src/
│   ├── config.py
│   ├── logger.py
│   ├── ingestion_bronze.py
│   ├── transformation_silver.py
│   ├── enrichment_gold.py
│   ├── data_quality.py
│   ├── validation.py
├── main_bronze.py
├── main_silver.py
├── main_gold.py
├── docs/
│   ├── *.md (Module documentation)
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🔧 Key Features

- ✅ Spark Structured Streaming
- ✅ Delta Lake integration with partitioning
- ✅ PII masking, hashing, and secure handling
- ✅ Broadcast joins for dimension enrichment
- ✅ Custom Data Quality checks
- ✅ Event-date aware processing
- ✅ Delta VACUUM & OPTIMIZE with Z-Ordering

---

## 🧪 Running the Pipeline

1. **Start Bronze Ingestion**
   ```bash
   python main_bronze.py
   ```

2. **Run Silver Transformation**
   ```bash
   python main_silver.py
   ```

3. **Launch Gold Enrichment & Optimization**
   ```bash
   python main_gold.py
   ```

---

## 👨‍💻 Author

Developed by Prasanth Iyyappan Raviraman as part of data engineering portfolio preparation for real-time fraud detection systems.

---

## 📝 License

This project is licensed for educational and demonstration purposes.
