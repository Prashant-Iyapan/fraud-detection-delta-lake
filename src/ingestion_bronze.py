from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType, DoubleType,  BooleanType
from src.validation import validate_columns, expected_schema_bronze
from src.config import bronze_bad_records_path, bronze_check_point_path, bronze_dq_result_folder, pii_path, bronze_write_path_incoming_txns as bronze_write_path
from src.logger import create_logger
from src.data_quality import run_data_quality
from pyspark.sql.functions import col, sha2, to_date, regexp_replace
from datetime import datetime

class StreamIngestor:
    """
    Initializes the Spark Structured Streaming ingestion process for Bronze layer.

    Responsibilities:
    - Creates a SparkSession for stream ingestion.
    - Defines the expected input schema for incoming JSON data.
    - Sets up a module-specific logger.
    """

    def __init__(self, source_path):
        self.spark = SparkSession.builder.appName('Ingestion').getOrCreate()
        self.module = 'Ingestion'
        self.ingestion_logger = create_logger(self.module)
        self.source_file = source_path
        self.set_schema = StructType([
            StructField('transaction_id', StringType(), True),
            StructField('event_time', TimestampType(), True),
            StructField('user_id', StringType(), True),
            StructField('product_id', StringType(),True),
            StructField('subscription_type', StringType(), True),
            StructField('amount', DoubleType(), True),
            StructField('location', StringType(), True),
            StructField('ip_address', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('payment_method', StringType(), True),
            StructField('card_number', StringType(), True),
            StructField('currency',StringType(), True),
            StructField('is_new_user', BooleanType(), True),
            StructField('referral_code', StringType(), True),
            StructField('session_id', StringType(), True)
        ])
    
    def process_batch(self, df, batch_id):
        """
        Processes each micro-batch in the Spark Structured Streaming pipeline.

        Steps:
        1. Check if incoming batch is empty.
        2. Hash sensitive fields (IP address, card number) for compliance.
        3. Store raw PII in a timestamped folder (audit/troubleshooting).
        4. Drop IP and mask card number for downstream anonymization.
        5. Add event_date column for partitioning.
        6. Run data quality checks against expected schema.
        7. Write processed data to Bronze Delta Lake.
        8. Persist DQ results to JSON path if present.
        """
        # Step 1: Check if incoming batch is empty
        if batch_df.isEmpty():
            self.ingestion_logger.info(f"Batch {batch_id} is empty. Skipping.")
            return
        else: 
            # Step 2:Hashing IP addresses and card numbers ensures compliance with:
            # - PCI-DSS: Avoids storing raw PAN/card details in downstream layers
            # - GDPR: Protects personally identifiable information (PII)
            batch_df = df.withColumn("hash_ip", sha2(col("ip_address"), 256)) \
                            .withColumn("hash_card_number", sha2(col("card_number"), 256))

            # Step 3: Capture original PII values into SSE - S3 bucket for audit logging and not direct use                
            pii_df = batch_df.select(col('ip_address'), col('card_number'))
            formatted_time = datetime.utcnow().strftime("%Y-%m-%d/%H-%M")
            pii_write_path = f'{pii_path}/{formatted_time}'
            try:
                pii_df.write.mode('append').json(pii_write_path)
            except Exception as e:
                self.ingestion_logger.exception(f"Error in writing pii data for batch {batch_id}: {e}")
            
            # Step 4: Replace IP address and Card number with hash_ip and masked_card_number
            batch_df= batch_df.drop(col('ip_address'))
            batch_df = batch_df.withColumn('masked_card_number', regexp_replace(col("card_number"), r"\d{12}(\d{4})", "**** **** **** $1"))\
                            .drop(col('card_number'))\
                            .withColumnRenamed('masked_card_number', 'card_number')
            
             # Step 5: Event Date for partitioning
            batch_df= batch_df.withColumn('event_date', to_date(col('event_time')))

            # Step 6: Run Data Quality Checks
            run_dq = run_data_quality(batch_df, batch_id, expected_schema_bronze)
            bronze_dq_result_folder_w_time = f'{bronze_dq_result_folder}/{formatted_time}'
            self.ingestion_logger.info(f"Writing batch {batch_id} to Bronze Delta path")
            self.ingestion_logger.info('Final Bronze Data frame Schema before we commence write:')
            batch_df.printSchema()

            # Step 7: Write to Bronze Delta Lake
            batch_df.write.partitionBy('event_date').mode('append').format('delta').save(bronze_write_path)
            if run_dq:
                self.ingestion_logger.info(f"Data Quality check failed for batch {batch_id}. Writing to DQ result folder")
                self.spark.createDataFrame(run_dq).write.mode('append').format('json').save(bronze_dq_result_folder_w_time)

    def data_ingest(self):
         # Read from S3 Source Path
        try:
            self.ingestion_logger.info(f"Reading from source path: {self.source_file}")
            df = self.spark.readStream.schema(self.set_schema).option('maxFilesPerTrigger', 1).json(self.source_file)
            self.ingestion_logger.info(f"Streaming status: {str(df.isStreaming)}")

            #Validate the columns before Starting the ingestion
            if not validate_columns(df):
                self.ingestion_logger.error("Schema validation failed. Aborting streaming job.")
                raise ValueError("Invalid columns in the data")
            self.ingestion_logger.info("Data Ingestion started to Bronze Layer")

            # Starting the ingestion
            query = df.writeStream.outputMode('append')\
                    .option("badRecordsPath", bronze_bad_records_path)\
                    .option('checkpointLocation', bronze_check_point_path)\
                    .foreachBatch(self.process_batch)\
                    .queryName('Bronze Ingestion Stream')\
                    .start()
            query.awaitTermination(600)
            self.ingestion_logger.info("Data ingestion into Bronze Layer completed.")
        except Exception as e:
            self.ingestion_logger.exception(f"Error in data ingestion: {e}")

# ------------------------------------------------------------
#  Future Enhancements - Ingestion
# ------------------------------------------------------------
# 1. Soft Schema Validation:
#     - Instead of failing the pipeline on minor schema drift, we log discrepancies between actual and expected columns.
#     - This ensures the pipeline remains resilient to optional column additions or reordering, which are common in real-world ingestion.
# 2. mergeSchema for Delta Writes:
#     - Enabled `mergeSchema=True` during Delta writes to automatically accommodate new columns when schema evolves.
#     - Prevents job failures due to minor schema updates while maintaining ACID guarantees.
# 3. Integrate row-level audit metadata (ingested_at, batch_id, source_file)
#    - Improves traceability for root-cause analysis
# 4. Replace `.awaitTermination(600)` with config-driven timeout or indefinite streaming
#    - Allows flexible deployment environments
# 5. Modularize masking logic (e.g. PIIProcessor class)
#    - Improves testability and future compliance upgrades
# 6. Introduce advanced exception handling to improve diagnostics, isolate failures, and ensure graceful recovery without interrupting streaming continuity.
# ------------------------------------------------------------