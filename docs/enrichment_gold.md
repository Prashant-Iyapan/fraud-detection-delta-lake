
# enrichment_gold.py

## 📌 Purpose

This module takes enriched Silver layer data and performs aggregations and risk analysis to produce three Gold layer outputs: user spend summaries, product usage metrics, and high-risk transaction flags. It also applies Delta Lake optimizations like vacuuming and Z-Ordering.

---

## ⚙️ Core Logic and Steps

1. **Read Silver Data** from the given source path.
2. **Transformations include**:
   - `user_spend_summary`: Group by user and event_date to compute total spending.
   - `product_usage_metrics`: Group by product and event_date to compute average usage cost ratio.
   - `flag_high_risk_txns`: Mark records as high-risk if risk level is high and usage cost ratio < 0.2.
3. **Write Results**:
   - Writes each DataFrame (spend, usage, risk) to a separate Gold path in Delta format partitioned by `event_date`.
4. **Optimize & Vacuum**:
   - Vacuum data older than 168 hours.
   - Z-Order by user_id or product_id for faster query performance.
5. **Supports streaming ingestion** via `write_to_gold()` using `foreachBatch()`.

---

## 📥 Inputs

- **Source**: Silver layer Delta table (`source_path`)
- **Expected Columns**: Includes fields like `user_id`, `amount`, `usage_cost_ratio`, `risk_level`, `event_date`, `product_id`, etc.

---

## 📤 Outputs

- **Gold Layer Tables**:
  - `gold_write_path_spend_summary`: Total amount spent per user per day.
  - `gold_write_path_product_usage_metrics`: Avg. usage cost ratio per product per day.
  - `gold_write_path_high_risk_txns`: Transactions flagged as high-risk.

---

## 🧩 Optional Parameters and Configs

- `optimize=True`: Triggers Delta vacuum and Z-Ordering.
- `ZORDER BY`: `user_id` or `product_id` based on the table for performance tuning.
- Checkpointing and bad records path provided for streaming resilience.

---

## 📝 Notes for Interviewers and Reviewers

- Implements **business-driven aggregations** for user and product analytics.
- Applies **risk-based logic** for flagging anomalies.
- Supports **streaming data processing with batch-style enrichments**.
- Showcases **Delta Lake best practices**: partitioning, vacuuming, and Z-Ordering.
- Modular design with clear method boundaries for reusability and testing.
- Logs schema and execution progress for better observability.
