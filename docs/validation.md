
# validation.py

## 📌 Purpose

This module provides schema validation for incoming DataFrames at both the **Bronze (Ingestion)** and **Silver (Transformation)** layers. It enforces strict schema expectations by comparing column names and data types against predefined schemas.

---

## ⚙️ Core Logic and Steps

1. **Define expected schemas**:
   - `expected_schema_bronze`: For raw input validation before Bronze ingestion.
   - `expected_schema_silver`: For enriched output after Silver transformations.

2. **Function: `validate_columns(df, module='Ingestion')`**
   - Accepts a DataFrame and a `module` name (`Ingestion` for Bronze, anything else for Silver).
   - Iterates over the corresponding schema and checks:
     - If each column exists in the DataFrame
     - If the data type matches the expected type
   - Logs all validation status and mismatches.

3. **Returns**:
   - `True` if all validations pass.
   - `False` if any column is missing or mismatched.

---

## 📥 Inputs

- **DataFrame (`val_dataframe`)**: The DataFrame to be validated.
- **Module (`module`)**: Determines whether to validate against Bronze or Silver schema.

---

## 📤 Outputs

- Boolean `True` or `False` indicating whether schema validation succeeded.
- Logs all outcomes for each column using `val_logger`.

---

## 🧩 Optional Parameters and Configs

- Uses `create_logger('validation')` for standardized logging.
- Can be easily extended to support new layers (e.g., Gold) or nested schemas.
- Currently uses hardcoded schema definitions; could be replaced by schema inference from a StructType if needed.

---

## 📝 Summary & Highlights

- Implements **layer-specific validation logic** for stronger pipeline integrity.
- **Fails early and loudly** if schema mismatches occur.
- Ensures that newly added fields from lookup joins or enrichment are also validated.
- Can be extended to support runtime schema auto-generation for more robustness.
