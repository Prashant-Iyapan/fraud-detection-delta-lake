
# main_bronze.py

## 📌 Purpose

This script serves as the entry point for the Bronze layer ingestion process. It initializes the logger, starts the streaming ingestion using the `StreamIngestor` class, and logs the workflow status.

---

## ⚙️ Core Logic and Steps

1. Create a logger named `"Main"` using the `create_logger()` utility.
2. Instantiate the `StreamIngestor` class with the configured `stream_file` path.
3. Call the `data_ingest()` method to start structured streaming ingestion.
4. Log the start and completion of the ingestion process.

---

## 📥 Inputs

- **Configuration**:
  - `stream_file`: Path to the streaming source folder, defined in `src.config`.

- **Class Used**:
  - `StreamIngestor` from `ingestion_bronze.py`

---

## 📤 Outputs

- Triggers streaming write into the Bronze Delta Lake path (handled internally by `StreamIngestor`).
- Console logs (and optionally file logs if enabled) showing process flow.

---

## 🧩 Optional Parameters and Configs

- Logging behavior can be controlled via the `create_logger()` function.
- `StreamIngestor` uses `maxFilesPerTrigger=1`, `badRecordsPath`, and checkpointing internally.

---

## 📝 Summary & Highlights

- Clean and minimal **driver script** for initiating Bronze ingestion.
- Uses **object-oriented modular structure** by separating logic into `StreamIngestor`.
- Easily extendable to add pre- or post-processing hooks around ingestion.
