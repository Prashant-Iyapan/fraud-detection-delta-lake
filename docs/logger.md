
# logger.py

## 📌 Purpose

This module provides a reusable logging utility that creates and configures loggers for each module in the project. It ensures consistent formatting and allows for optional file-based logging.

---

## ⚙️ Core Logic and Steps

1. **`create_logger(module, file_needed=False)`**:
   - Initializes a logger for the given `module` name.
   - Adds a stream handler to print logs to console with a consistent format.
   - Optionally adds a file handler to write logs to a `.log` file in the configured path (`log_path`).

2. Avoids duplicate handlers by checking `new_logger.handlers` and verifying if a `FileHandler` already exists.

3. Uses `formatter_format` from config for consistent formatting.

---

## 📥 Inputs

- **`module`**: Name of the current module using the logger (used in logger name and log file naming).
- **`file_needed`**: Boolean flag to indicate whether to add a file handler.

---

## 📤 Outputs

- Returns a `logger` instance configured with console (and optionally file) output.

---

## 🧩 Optional Parameters and Configs

- `formatter_format`: Controls log message format (timestamp, level, message, etc.)
- `log_path`: Directory path where log files will be stored if file logging is enabled.

---

## 📝 Notes for Interviewers and Reviewers

- Promotes **modular logging strategy** with support for both development (console) and production (file) use cases.
- Prevents **handler duplication**, a common issue in Spark or multi-threaded apps.
- Encourages centralized configuration of logging behavior through `src.config`.
- Can be extended to support different log levels or external logging systems.
