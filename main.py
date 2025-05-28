from src.logger import create_logger
from src.ingestion_bronze import StreamIngestor
from src.config import stream_file, silver_read_path, gold_read_path
from src.transformation_silver import Transformation
from src.enrichment_gold import Enrichment

def main():
    main_logger = create_logger("Main")
    main_logger.info("Starting main function")
    #stream_start = StreamIngestor(source_path=stream_file)
    #stream_start.data_ingest()
    #main_logger.info("Ingestion complete Preparing for Transformation")
    #stream_silver = Transformation(silver_read_path)
    #silver_df = stream_silver.read_bronze_data()
    #enriched_silver_df = stream_silver.enrich_with_lookups(silver_df)
    #stream_silver.run_dq_silver(enriched_silver_df)
    #main_logger.info("Transformation complete writing to silver")
    #stream_silver.write_to_silver(enriched_silver_df)
    #main_logger.info("Writing to silver complete")
    main_logger.info("Preparing for Gold Enrichment")
    stream_gold = Enrichment(gold_read_path)
    stream_gold.gold_run()


if __name__ == "__main__":
    main()
    