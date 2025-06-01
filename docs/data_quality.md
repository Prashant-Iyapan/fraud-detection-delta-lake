
# data_quality.py

## 📌 Purpose

This module performs structured data quality checks on incoming DataFrames across different pipeline layers (Bronze and Silver). It validates null values, range conditions, format patterns, and includes custom rule-based logic for Silver layer enrichment validations.

---

## ⚙️ Core Logic and Steps

1. **`run_data_quality()`** - Master function that calls the appropriate checks based on the layer (Bronze or Silver).
2. **`check_nulls()`** - Checks each expected column for null values. Logs and records failure if any nulls are found.
3. **`check_range()`** - Validates that the `amount` column has only positive values.
4. **`check_format()`** - Validates format compliance for fields like currency or timestamp using regex patterns from `format_patterns`.
5. **`custom_silver_dq_check()`** - Runs additional rules for Silver data, such as:
   - Valid `usage_cost_ratio`
   - High-risk users with low usage
   - Amount values without corresponding monthly_cost
   - Amount values with missing currency

Each check returns structured results containing column name, check type, counts, percentages, and pass/fail status.

---

## 📥 Inputs

- **DataFrame (`df`)**: The DataFrame being validated.
- **`expected_schema`**: List of expected column names (from `validation.py`).
- **`batch_id`**: Identifier used in logs and DQ result rows.
- **`layer`**: Either `'bronze'` or `'silver'`, determines whether custom checks run.

---

## 📤 Outputs

- Returns a `list of dicts`, each dict representing a single DQ rule result with keys like:
  - `batch_id`, `column`, `check`, `status`, `count`, `percent`, etc.
- These are later written to disk in JSON format by the respective main scripts.

---

## 🧩 Optional Parameters and Configs

- Uses `format_patterns` from config for regex-based format validation.
- Custom logic in Silver is extensible — rules can be easily added.
- Logging enabled via `DQ_logger` for traceability.

---

## 📝 Summary & Highlights

- Implements a **modular, rule-based DQ system** suited for both Bronze and Silver layers.
- Custom Silver checks show **business logic awareness**, e.g., ratio checks, missing reference fields.
- Design allows **plug-and-play extension** for new checks without breaking others.
- Highly transparent: logs % violations and check-specific statuses.
- Makes batch metadata available for lineage tracking or alerting systems.
