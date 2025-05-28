from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType, DoubleType,  BooleanType
from src.validation import validate_columns, expected_schema_bronze
from src.config import bad_records_path, check_point_path, bronze_write_path,bronze_dq_result_folder, pii_path
from src.logger import create_logger
from src.data_quality import run_data_quality
from pyspark.sql.functions import col, sha2
from datetime import datetime
class StreamIngestor:

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
        batch_df = df.withColumn("hash_ip", sha2(col("ip_address"), 256)) \
                        .withColumn("hash_card_number", sha2(col("card_number"), 256))
        pii_df = batch_df.select(col('ip_address'), col('card_number'))
        formatted_time = datetime.utcnow().strftime("%Y-%m-%d/%H-%M")
        pii_write_path = f'{pii_path}/{formatted_time}'
        pii_df.write.mode('append').json(pii_write_path)
        run_dq = run_data_quality(batch_df, batch_id, expected_schema_bronze)
        bronze_dq_result_folder_w_time = f'{bronze_dq_result_folder}/{formatted_time}'
        self.ingestion_logger.info(f"Writing batch {batch_id} to Bronze Delta path")
        batch_df.write.mode('append').format('delta').save(bronze_write_path)
        if run_dq:
            self.ingestion_logger.info(f"Data Quality check failed for batch {batch_id}. Writing to DQ result folder")
            self.spark.createDataFrame(run_dq).write.mode('append').format('json').save(bronze_dq_result_folder_w_time)


    def data_ingest(self):
        try:
            self.ingestion_logger.info(f"Reading from source path: {self.source_file}")
            df = self.spark.readStream.schema(self.set_schema).json(self.source_file)
            self.ingestion_logger.info(f"Streaming status: {df.isStreaming}")
            #Validate the columns before Starting the ingestion
            if not validate_columns(df):
                self.ingestion_logger.error("Schema validation failed. Aborting streaming job.")
                raise ValueError("Invalid columns in the data")
            self.ingestion_logger.info("Data Ingestion started to Bronze Layer")
            query = df.writeStream.outputMode('append')\
                    .option("badRecordsPath", bad_records_path)\
                    .option('checkpointLocation', check_point_path)\
                    .foreachBatch(self.process_batch)\
                    .queryName('Bronze Ingestion Stream')\
                    .start()
            query.awaitTermination()
            self.ingestion_logger.info("Data ingestion into Bronze Layer completed.")
        except Exception as e:
            self.ingestion_logger.exception(f"Error in data ingestion: {e}")