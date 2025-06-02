# ------------------------------------------------------------
# Silver Layer Entry Point
# ------------------------------------------------------------
# This script reads event-wise Bronze data and processes it
# into the Silver layer using enrichment lookups and DQ checks.
# ------------------------------------------------------------
import time
from delta.tables import DeltaTable
from src.logger import create_logger
from src.transformation_silver import Transformation
from src.config import *
from pyspark.sql.functions import col, lit

# Wait for Bronze Delta table to be available and non-empty
def wait_for_bronze_data(spark, path, retries=30, wait_secs=5):
    print("Waiting for data from Bronze")
    for i in range(retries):
        print(f'Done with {i+1}/{retries} retries.')
        if DeltaTable.isDeltaTable(spark, path):
            try:
                print(f'reading data from {path}')
                df = spark.read.format("delta").load(path)
                if df.count() > 0:
                    return True
            except:
                pass
        time.sleep(wait_secs)
    return False

# Read Bronze Delta stream filtered by a single event_date
def read_bronze_stream_data_per_date(spark, path, event_date, logger):
    logger.info(f'Reading data from {path}')
    try:
        df = spark.readStream.format('delta').load(path).where(col('event_date') == lit(event_date))        
        logger.info('Bronze data loaded into Dataframe Now')
        return df
    except Exception as e:
        logger.exception(f'Error occured while reading data from {path} and the exception: {e}')
        return None
    
def main():
    silver_main_logger = create_logger("silver_main")
    # Initialize Silver transformation Pipeline
    stream_silver = Transformation(silver_read_path)

    # Wait for Bronze table to be available and contain data
    if not wait_for_bronze_data(stream_silver.spark, silver_read_path):
        silver_main_logger.error("No data found in Bronze after waiting.")
        return    
    bronze_df = stream_silver.spark.read.format("delta").load(silver_read_path)
    event_dates = bronze_df.select("event_date").distinct().rdd.flatMap(lambda x: x).collect()
    
    # Load all distinct event_dates to process each one separately 
    event_dates = sorted(event_dates)
    silver_main_logger.info("Waiting for Bronze data to arrive...")

    for event_date in event_dates:
        print(f'Processing event_date: {event_date}')
        silver_df = read_bronze_stream_data_per_date(stream_silver.spark, silver_read_path, event_date, silver_main_logger)
        if silver_df is None:
            print("Silver Dataframe is None!")
            continue
        else:
            # Trigger Silver layer transformation and write
            silver_main_logger.info("Data found in Bronze, writing to Silver")
            silver_df.printSchema()
            stream_silver.write_to_silver(silver_df)
            silver_main_logger.info("Writing to silver complete")

if __name__ == "__main__":
    main()
