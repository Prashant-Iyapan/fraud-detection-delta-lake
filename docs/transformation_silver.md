
# transformation_silver.py

## 📌 Purpose

This module reads data from the Bronze layer, performs enrichment through multiple broadcast joins using lookup datasets, calculates derived metrics, flags missing references, and writes the transformed output to the Silver layer in Delta format.

---

## ⚙️ Core Logic and Steps

1. **Read data** from the Bronze Delta table with predefined schema.
2. **Broadcast join** with the following lookup tables to enrich the dataset:
   - User list (adds country and user_status)
   - Product list (adds product_name, category, price)
   - Subscription plan (adds monthly_cost, churn_risk, plan_duration)
   - IP risk list (adds risk_level, risk_score, location)
3. **Flag missing references** using `when().isNull()` checks for users, products, subscriptions, and IPs.
4. **Calculate derived column** `usage_cost_ratio` = amount / monthly_cost.
5. **Run Data Quality Checks** using `run_data_quality()` and log the results.
6. **Write enriched data** to Silver Delta path partitioned by `event_date`.
7. **Support both batch and streaming writes** with `write_to_silver()` and `foreachBatch()`.

---

## 📥 Inputs

- **Source**: Bronze layer Delta table (`source_path`)
- **Lookups**:
  - `lkp_users.csv`
  - `lkp_prod_list.csv`
  - `lkp_subs_plan.csv`
  - `lkp_ip_risk.csv`

---

## 📤 Outputs

- **Silver Layer Delta Table** partitioned by `event_date`, stored at `silver_write_path`.
- **Data Quality Results**: JSON output written to `silver_dq_result_folder/<timestamp>`.

---

## 🧩 Optional Parameters and Configs

- Uses `broadcast()` joins for optimal performance on large datasets.
- Logs count of each lookup dataset for validation and debugging.
- `usage_cost_ratio` can be used in downstream fraud or churn detection logic.
- Timestamp-based folders for DQ results ensure traceability.

---

## 📝 Summary & Highlights

- Demonstrates real-world **data enrichment** through multiple broadcast joins.
- Handles **referential integrity** gracefully by flagging missing foreign key references.
- Implements **modular and testable transformation pipeline**.
- Supports both **streaming and batch-style ingestion** into the Silver layer.
- Logs schema and sample records before write for transparency and debugging.
- Easily extendable to support more lookup joins or enrichments.
