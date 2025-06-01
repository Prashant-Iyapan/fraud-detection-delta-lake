
# main_gold.py

## 📌 Purpose

This script orchestrates the Gold layer processing. It waits for enriched Silver data, filters by `event_date`, and executes Gold layer aggregation and risk-flagging using the `Enrichment` class. It also triggers optimization routines such as vacuuming and Z-Ordering.

---

## ⚙️ Core Logic and Steps

1. **Initialize logger and Enrichment class** with Gold read path.
2. **Wait for Silver data availability** using `wait_for_silver_data()` with retries.
3. **Read distinct event dates** from the Silver table.
4. For each `event_date`:
   - Read stream data for that date.
   - Call `write_to_gold()` from `Enrichment` to:
     - Generate user spend summary.
     - Generate product usage metrics.
     - Flag high-risk transactions.
   - Perform `OPTIMIZE` and `VACUUM` operations after write.

---

## 📥 Inputs

- **Source**: Silver Delta table located at `gold_read_path`.
- **Methods and Classes Used**:
  - `Enrichment` from `enrichment_gold.py`
  - `DeltaTable.isDeltaTable` for data availability checks.

---

## 📤 Outputs

- Writes three Gold Delta outputs:
  - `user_spend_summary`
  - `product_usage_metrics`
  - `high_risk_txns`
- All outputs partitioned by `event_date`.

---

## 🧩 Optional Parameters and Configs

- `retries`, `wait_secs` in `wait_for_silver_data()` configurable.
- `optimize=True` flag triggers vacuuming and Z-Ordering for Gold tables.
- Logging helps track each step of the enrichment process.

---

## 📝 Notes for Interviewers and Reviewers

- Shows **controlled event-driven orchestration** of downstream processing.
- Applies **business rules** to generate KPI and risk metrics.
- Follows **Delta Lake best practices** (partitioning, optimization).
- Modular structure makes this script suitable for production workflows.
- Logs enriched lifecycle events and optimizations for full observability.
