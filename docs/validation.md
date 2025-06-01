
# validation.py

## 📌 Purpose

This module validates schema integrity for incoming streaming data frames, particularly at the Bronze and Silver layers. It ensures that both column names and their data types conform to expected definitions.

---

## ⚙️ Core Logic and Steps

1. **Define expected schemas** as dictionaries:
   - `expected_schema_bronze`: Expected structure for Bronze layer input.
   - `expected_schema_silver`: Expected structure for Silver layer after enrichment.

2. **`validate_columns(val_dataframe)`**:
   - Iterates through the `expected_schema_bronze`.
   - Confirms that each required column exists in the DataFrame.
   - Checks if the datatype matches what is defined in the schema.
   - Logs mismatches or confirmations for transparency.

---

## 📥 Inputs

- **DataFrame (`val_dataframe`)**: The DataFrame being validated (typically Bronze layer input).
- **Schemas**: Defined as dictionaries mapping column names to Spark data type names (as strings).

---

## 📤 Outputs

- Returns `True` if the schema matches expectations.
- Returns `False` if any column is missing or the data type mismatches.
- Logs the schema match status for each column.

---

## 🧩 Optional Parameters and Configs

- Uses the `val_logger` from `logger.py` for structured logs.
- Can be extended to validate nested structures or evolving schemas.

---

## 📝 Notes for Interviewers and Reviewers

- Enforces **schema contracts** early in the pipeline, preventing downstream errors.
- Encourages **transparent and traceable schema validation** with clear logs.
- Centralizes expected schema definitions for **Bronze and Silver layers**, promoting reusability.
- Simple but critical function for robust data pipeline design.
