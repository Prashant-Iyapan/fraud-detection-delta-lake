
# ingestion_bronze.py

## 📌 Purpose

This module ingests raw JSON streaming data into the Bronze layer of a Delta Lake architecture. It performs schema validation, PII hashing, masking, basic enrichment, and partitions the data by `event_date` to optimize downstream queries.

---

## ⚙️ Core Logic and Steps

1. **Initialize Spark Session** with a defined schema for the incoming JSON data.
2. **Read stream** from the configured source using `readStream`.
3. **Validate schema** using `validate_columns()` before proceeding.
4. **Process each batch** using `foreachBatch`:
   - Add SHA-256 hashes for `ip_address` and `card_number` fields.
   - Persist original PII (ip and card number) to a secure path as JSON.
   - Mask the card number (`**** **** **** 1234`) and remove original fields.
   - Add `event_date` column derived from `event_time`.
   - Perform data quality checks using `run_data_quality()`.
   - If passed, write to Bronze Delta Lake, partitioned by `event_date`.
   - If failed, write results to a separate DQ result folder for analysis.
5. **Set up structured streaming write** with checkpointing and bad records path.

---

## 📥 Inputs

- **Streaming Source**: JSON files located at `source_path` (provided at runtime).
- **Schema**: Predefined `StructType` with 15 fields like:
  - `transaction_id`, `event_time`, `user_id`, `ip_address`, `card_number`, `amount`, etc.

---

## 📤 Outputs

- **Masked Bronze Table** stored in Delta format at `bronze_write_path`, partitioned by `event_date`.
- **Original PII JSON** records written to `pii_path/<formatted_utc_time>`.
- **DQ Failed Rows** stored in JSON format at `bronze_dq_result_folder/<formatted_utc_time>`.

---

## 🧩 Optional Parameters and Configs

- `maxFilesPerTrigger=1`: Controls how many files are picked up per micro-batch.
- Timestamped output folders for PII and DQ help in traceability and isolation.
- `badRecordsPath` ensures corrupt or malformed JSONs don’t break the pipeline.
- Checkpointing at `bronze_check_point_path` ensures streaming resiliency and recovery.

---

## 📝 Notes for Interviewers and Reviewers

- Implements real-world **data privacy best practices** (PII hashing and masking).
- Uses **`foreachBatch`** to enable custom logic on streaming data.
- Built with **clear logging and modular configuration paths**.
- Data is **partitioned by event_date** to improve downstream processing and querying.
- DQ checks are isolated, allowing easy plug-and-play of additional validations.
- Can be extended to handle schema evolution or more complex anonymization logic.
