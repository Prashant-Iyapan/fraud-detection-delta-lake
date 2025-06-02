from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, TimestampType, DateType
from src.logger import create_logger
from pyspark.sql import SparkSession
from src.config import *
from pyspark.sql.functions import col, when, broadcast, current_date, lit
from src.data_quality import run_data_quality, custom_silver_dq_check
from src.validation import expected_schema_silver, validate_columns
from datetime import datetime

class Transformation:
    """
    Silver layer transformation class responsible for:
    - Reading Bronze-level Delta data
    - Defining schema for incoming records
    - Loading lookup tables for enrichment
    - Logging lookup availability and record counts
    """
    def __init__(self, source_path):
        #logger + Spark + Source
        self.spark=SparkSession.builder.appName('Transformation').getOrCreate()
        self.module = 'Transformation'
        self.source_file = source_path
        self.transform_logger=create_logger(module=self.module)

        # Schema for expected incoming data
        self.set_schema = StructType([
            StructField('transaction_id', StringType(), True),
            StructField('event_time', TimestampType(), True),
            StructField('user_id', StringType(), True),
            StructField('product_id', StringType(), True),
            StructField('subscription_type', StringType(), True),
            StructField('amount', DoubleType(), True),
            StructField('location', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('payment_method', StringType(), True),
            StructField('currency',StringType(), True),
            StructField('is_new_user', BooleanType(), True),
            StructField('referral_code', StringType(), True),
            StructField('session_id', StringType(), True),
            StructField('hash_ip', StringType(), True),
            StructField('hash_card_number', StringType(), True),
            StructField('card_number', StringType(), True),
            StructField('event_date', DateType(), True)          
            ])
        
        # Load Look up from S3
        self.lkp_ip_risk_df = self.spark.read.csv(lkp_ip_risk, header=True,inferSchema=True)
        self.prod_list_df = self.spark.read.csv(lkp_prod_list, header=True,inferSchema=True)
        self.subs_plan_df = self.spark.read.csv(lkp_subs_plan, header=True, inferSchema=True)
        self.user_list_df = self.spark.read.csv(lkp_users, header=True, inferSchema=True)

        # Sanity check for lookup data availability
        self.transform_logger.info("Checking Look-up data Availability")
        self.transform_logger.info(f'User list count: {self.user_list_df.count()}')
        self.transform_logger.info(f'Product list count: {self.prod_list_df.count()}')
        self.transform_logger.info(f'Subscription plan count: {self.subs_plan_df.count()}')
        self.transform_logger.info(f'IP risk list count: {self.lkp_ip_risk_df.count()}')

    def process_silver_batch(self, silver_df, batch_id):
        # check if incoming batch is empty
        if silver_df.isEmpty():
            self.transform_logger.info(f"Batch {batch_id} is empty. Skipping.")
            return
        
        # Silver Transformations
        enriched_silver_df = self.enrich_with_lookups(silver_df)
        self.transform_logger.info('Completed Silver Transformations Proceeding for Schema Validation')

        # schema validation at silver level after enrichment
        if validate_columns(enriched_silver_df, self.module):
            self.run_dq_silver(enriched_silver_df)
        else: 
            raise ValueError (f'There is a schema mismatch after Transformation, Please check if enriched dataframe matches {expected_schema_silver}')
        enriched_silver_df.write.mode('append').format('delta').partitionBy('event_date').save(silver_write_path)
    
    def enrich_with_lookups(self, df):
        df.printSchema()
        df = df.alias('bronze')

        # User lookup Enrichment - User Status and Country - Flag: Missing User
        self.user_list_df = self.user_list_df.alias('user_list')
        risk_df = df.join(broadcast(self.user_list_df), col('bronze.user_id') == col('user_list.user_id'), how='left')
        risk_df = risk_df.withColumn('missing_user', when(col('user_list.user_id').isNull(), True).otherwise(False))
        risk_df = risk_df.select(col('bronze.*'),col('user_list.country'),col('user_list.user_status'), col('missing_user')).alias('risk')
        risk_df= risk_df.alias('risk')

        # Product lookup Enrichment - Product Name, Category and Price - Flag: Missing Product
        self.prod_list_df = self.prod_list_df.withColumnRenamed('product_id', 'product_id_look_up')
        self.prod_list_df = self.prod_list_df.alias('prod_list')
        risk_prod_df = risk_df.join(broadcast(self.prod_list_df), col('risk.product_id') == col('prod_list.product_id_look_up'), how='left')
        risk_prod_df = risk_prod_df.withColumn('missing_product', when(col('prod_list.product_id_look_up').isNull(), True).otherwise(False))
        risk_prod_df = risk_prod_df.select(col('risk.*'), col('prod_list.product_name'), col('prod_list.category'), col('prod_list.price'), col('missing_product')).alias('risk_prod')
        risk_prod_df = risk_prod_df.alias('risk_prod')

        # Subscription lookup Enrichment - Subscription Price, Churn Risk and Plan Duration - Flag: Missing Subscription
        self.subs_plan_df = self.subs_plan_df.withColumnRenamed('subscription_type', 'subscription_type_look_up')
        self.subs_plan_df = self.subs_plan_df.alias('subs_plan')
        risk_prod_sub_df = risk_prod_df.join(broadcast(self.subs_plan_df), col('risk_prod.subscription_type') == col('subs_plan.subscription_type_look_up'), how='left')
        risk_prod_sub_df = risk_prod_sub_df.withColumn('missing_subscription', when(col('subs_plan.subscription_type_look_up').isNull(), True).otherwise(False))
        risk_prod_sub_df = risk_prod_sub_df.select(col('risk_prod.*'), col('subs_plan.monthly_cost'),col('subs_plan.churn_risk'), col('subs_plan.plan_duration'),col('missing_subscription')).alias('risk_prod_sub')
        risk_prod_sub_df = risk_prod_sub_df.alias('risk_prod_sub')

        # IP Risk lookup Enrichment - Risk Level, Risk Score and Risk Location - Flag: Missing IP
        self.lkp_ip_risk_df = self.lkp_ip_risk_df.alias('ip_risk')
        ip_prod_risk_sub_df = risk_prod_sub_df.join(broadcast(self.lkp_ip_risk_df), col('risk_prod_sub.hash_ip') == col('ip_risk.ip_hash'), how = 'left')
        ip_prod_risk_sub_df = ip_prod_risk_sub_df.withColumn('missing_ip', when(col('ip_risk.ip_hash').isNull(), True).otherwise(False))
        ip_prod_risk_sub_df = ip_prod_risk_sub_df.select(col('risk_prod_sub.*'), col('ip_risk.risk_level'), col('ip_risk.risk_score').cast(DoubleType()), col('ip_risk.location').alias('ip_risk_location'), col('missing_ip'))

        # Derived Enrichment - Calculating Usage Cost Ratio
        final_silver_df = ip_prod_risk_sub_df.withColumn('usage_cost_ratio', col('amount')/col('monthly_cost'))
        final_silver_df.printSchema()
        final_silver_df.show(n=5)
        return final_silver_df
        
    def run_dq_silver(self, df):
        # Data Quality Check
        self.transform_logger.info("Prepping encriched Silver dataframe for Data Quality Check")
        run_dq = run_data_quality(df, 'silver', expected_schema_silver, layer = 'silver')
        self.transform_logger.info("Data Quality Check is completed")
        self.transform_logger.info("Writing DQ Results to silver DQ path")
        formatted_time = datetime.utcnow().strftime("%Y-%m-%d/%H-%M")
        silver_dq_result_folder_w_time = f'{silver_dq_result_folder}/{formatted_time}'
        self.spark.createDataFrame(run_dq).write.mode('append').json(silver_dq_result_folder_w_time)
        self.transform_logger.info(f"DQ Results written to: {silver_dq_result_folder_w_time}")
  
    def write_to_silver(self,df):
        self.transform_logger.info("We are starting to our write into Silver layer")
        # Streaming Query definition to perform batch-wise transformation 
        silver_query=df.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', silver_check_point_path)\
        .option('badRecordsPath', silver_bad_records_path)\
        .foreachBatch(self.process_silver_batch)\
        .start()
        silver_query.awaitTermination()
        self.transform_logger.info("We have successfully written into Silver layer")


# ------------------------------------------------------------
#  Future Enhancements - Transformations
# ------------------------------------------------------------
# 1. Derive `churn_risk_score` and `retention_score` from user activity trends
#    - Requires time-based behavior or usage frequency metrics
# 2. Add `velocity_score` → transactions per user within a time window
#    - Helps flag rapid actions indicating fraud (velocity attack)
# 3. Geo-location lookup using `hash_ip` → country/region/city
#    - Adds geo_risk or localization support
# 4. Flag `is_device_change` → track multiple new devices per user
#    - Derived from device_id timeline
# 5. Introduce advanced exception handling to improve diagnostics, isolate failures, and ensure 
# graceful recovery without interrupting streaming continuity.
# ------------------------------------------------------------
#  Future Enhancements -Architecture
# ------------------------------------------------------------
# 1. Add structured error handling with custom exceptions
#    - Wrap joins and DQ checks with clear try/except logic
#
# 2. Implement `empty_lookup_check()` before enrichments
#    - Abort or log warning if critical lookup tables are empty

# 3. Add metrics collection: row counts, null %, join success %
#    - For real-time pipeline health monitoring
# ------------------------------------------------------------
