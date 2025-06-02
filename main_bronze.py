# ------------------------------------------------------------
# Bronze Layer Entry Point
# ------------------------------------------------------------
# This script initializes the ingestion process for streaming data.
# It uses the StreamIngestor class to read raw input and store it
# to Bronze-level Delta tables for downstream Silver and Gold processing.
# ------------------------------------------------------------

from src.logger import create_logger
from src.ingestion_bronze import StreamIngestor
from src.config import stream_file, silver_read_path, gold_read_path
from src.transformation_silver import Transformation
from src.enrichment_gold import Enrichment

def main_bronze():
    main_logger = create_logger("Main")
    main_logger.info("Starting main function")
    stream_start = StreamIngestor(source_path=stream_file)
    stream_start.data_ingest()
    main_logger.info("Ingestion complete Preparing for Transformation")

if __name__ == "__main__":
    main_bronze()
    