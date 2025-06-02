# ------------------------------------------------------------
# Gold Layer Entry Point
# ------------------------------------------------------------
# This script waits for enriched Silver data, then processes it
# through final aggregations and risk flagging in the Gold layer.
# It outputs three Gold tables:
# - User Spend Summaries
# - Product Usage Metrics
# - High-Risk Transaction Flags
# ------------------------------------------------------------

from src.config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from src.logger import create_logger
from src.enrichment_gold import Enrichment
from delta.tables import DeltaTable
import time

gold_main_logger = create_logger('gold_main')

# Wait for Silver Delta table to exist and contain data
def wait_for_silver_data(spark, path, retries=50, wait_secs=5):
    gold_main_logger.info("Waiting for data from silver")
    for i in range(retries):
        gold_main_logger.info(f'Attempting Silver data fetch: {i+1}/{retries} retries.')
        if DeltaTable.isDeltaTable(spark, path):
            try:
                df = spark.read.format("delta").load(path)
                if df.count() > 0:
                    return True
            except Exception as e:
                pass
        time.sleep(wait_secs)
    return False

# Read Silver Delta stream filtered by a specific event_date   
def read_silver_stream_data_per_date(spark, path, event_date, logger):
    logger.info('Reading the Source file from Silver path')
    try:
        df = spark.readStream.format('delta').load(path).where(col('event_date') == lit(event_date))
        logger.info('Silver Source file loaded into DataFrame')
        return df
    except Exception as e:
        logger.exception(f'Got an issue {e} while reading the silver data at {path}')
        return None
        
def gold_main():
    gold_main_logger.info("Preparing for Gold Enrichment")

    # Initialize Gold enrichment pipeline
    stream_gold = Enrichment(gold_read_path)

    # Wait until Silver Delta table exists and has data
    if not wait_for_silver_data(stream_gold.spark, gold_read_path):
        gold_main_logger.error("No data found in Silver after waiting.")
        return
    silver_df = stream_gold.spark.read.format("delta").load(gold_read_path)

    # Load all distinct event_dates to process each one separately
    event_dates = silver_df.select("event_date").distinct().rdd.flatMap(lambda x: x).collect()
    event_dates = sorted(event_dates)
    for event_date in event_dates:
        gold_main_logger.info(f"Processing event_date: {event_date}")
        gold_df = read_silver_stream_data_per_date(stream_gold.spark, gold_read_path, event_date, gold_main_logger)
        if gold_df is None:
            gold_main_logger.error(f"No data found in Silver for event date: {event_date}")
            continue
        else:
            gold_main_logger.info(f"Data found in Silver for event date: {event_date}, proceeding with Gold Enrichment")
            stream_gold.write_to_gold(gold_df, optimize=True)
            gold_main_logger.info("Gold Enrichment completed, Starting Optimization")

if __name__ == "__main__":
    gold_main()