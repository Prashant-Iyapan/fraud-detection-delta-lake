
# main_silver.py

## 📌 Purpose

This script drives the Silver layer transformation pipeline. It waits for Bronze data availability, reads filtered streaming data by `event_date`, and processes it through the `Transformation` class to enrich and store in the Silver Delta table.

---

## ⚙️ Core Logic and Steps

1. **Initialize Spark and Logger** using `Transformation` and `create_logger()`.
2. **Wait for Bronze data** using `wait_for_bronze_data()` with retries and delay.
3. **Extract distinct event dates** from Bronze data.
4. For each `event_date`:
   - Read data from Bronze stream using `read_bronze_stream_data_per_date()`.
   - Validate DataFrame is not `None`.
   - Call `write_to_silver()` method from `Transformation` class to process and persist data.

---

## 📥 Inputs

- **Bronze Delta Path**: Provided via `silver_read_path` from config.
- **Methods and Classes Used**:
  - `Transformation` from `transformation_silver.py`
  - `readStream()` on filtered `event_date` values

---

## 📤 Outputs

- Writes enriched data into the **Silver Delta table**, partitioned by `event_date`.
- Logs steps and errors using a dedicated logger (`silver_main`).

---

## 🧩 Optional Parameters and Configs

- `wait_for_bronze_data()` retries and sleep time configurable (`retries`, `wait_secs`).
- Modular structure enables filtering and rerun for specific `event_date`s.
- Useful for **event-time aware processing** in historical or backfilled data.

---

## 📝 Summary & Highlights

- Demonstrates **event-time filtering** and **controlled ingestion** using retry loops.
- Supports **partition-aware streaming reads** for efficient downstream processing.
- Logs all data arrival and transformation checkpoints for **observability**.
- Well-structured orchestration code, separating control logic from transformation logic.
