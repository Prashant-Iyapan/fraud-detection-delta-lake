from src.config import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, DoubleType, StringType, TimestampType, DateType
from src.logger import create_logger
from pyspark.sql.functions import col, sum, avg, when, lit
from delta.tables import DeltaTable

class Enrichment:
    def __init__(self, source_path):
        self.spark = SparkSession.builder.appName('gold').getOrCreate()
        self.enrichment_logger = create_logger('Enrichment')
        self.set_schema = StructType([StructField('transaction_id', StringType()),
            StructField('event_time', TimestampType(),True),
            StructField('user_id', StringType(),True),
            StructField('product_id', StringType(),True),
            StructField('subscription_type', StringType(),True),
            StructField('amount', DoubleType(),True),
            StructField('location', StringType(),True),
            StructField('device_id', StringType(),True),
            StructField('payment_method', StringType(),True),
            StructField('currency', StringType(),True),
            StructField('is_new_user', BooleanType(),True),
            StructField('referral_code', StringType(),True),
            StructField('session_id', StringType(),True),
			StructField('hash_ip', StringType(),True),
			StructField('hash_card_number', StringType(),True),
			StructField('card_number', StringType(), True),
			StructField('event_date', DateType(),True),
			StructField('country', StringType(), True),
			StructField('user_status', StringType(), True),
			StructField('missing_user', BooleanType(), True),
			StructField('product_name', StringType(), True),
			StructField('category', StringType(), True),
			StructField('price', DoubleType(), True),
            StructField('missing_product', BooleanType(),True),
			StructField('monthly_cost', DoubleType(), True),
			StructField('churn_risk', StringType(), True),
			StructField('plan_duration', IntegerType(), True),
            StructField('missing_subscription', BooleanType(),True),
			StructField('risk_level', StringType(), True),
			StructField('risk_score', DoubleType(), True),
			StructField('ip_risk_location', StringType(), True),
            StructField('missing_ip', BooleanType(),True),
            StructField('usage_cost_ratio', DoubleType(),True)])
        self.source_path = source_path
        
    def user_spend_summary(self,df):
        self.enrichment_logger.info('Calculating the spend summary for each user')
        spend_summary_df = df.groupBy(col('user_id'), col('event_date')).agg(sum(col('amount')).alias('Total_spent_per_user'))
        spend_summary_df.printSchema()
        return spend_summary_df
    
    def product_usage_metrics(self, df):
        self.enrichment_logger.info('Calculating the usage metrics for each product')
        usage_metrics_df = df.groupBy(col('product_id'), col('event_date')).agg(avg(col('usage_cost_ratio')).alias('average_usage_cost_ratio'))
        usage_metrics_df.printSchema()
        return usage_metrics_df
    
    def flag_high_risk_txns(self, df):
        self.enrichment_logger.info('Flagging High Risk Transactions now')
        high_risk_txn_df = df.withColumn('is_high_risk_txn', when((col('risk_level') == 'high') & (col('usage_cost_ratio') < 0.2), True).otherwise(False))
        high_risk_txn_df.printSchema()
        return high_risk_txn_df
    
    def process_gold_batch(self, silver_data_df, batch_id):
        self.enrichment_logger.info(f'Processing silver to gold batch: {batch_id}')
        try:
            if silver_data_df is not None:
                spend_summary_df = self.user_spend_summary(silver_data_df)
                usage_metrics_df = self.product_usage_metrics(silver_data_df)
                high_risk_txn_df = self.flag_high_risk_txns(silver_data_df)
                self.enrichment_logger.info('Writing the Gold DataFrames to Delta')
                spend_summary_df.write.format('delta').mode('append').partitionBy("event_date").save(gold_write_path_spend_summary)
                usage_metrics_df.write.format('delta').mode('append').partitionBy("event_date").save(gold_write_path_product_usage_metrics)
                high_risk_txn_df.write.format('delta').mode('append').partitionBy("event_date").save(gold_write_path_high_risk_txns)
                self.enrichment_logger.info('Gold DataFrames written to Delta')
        except Exception as e:
            self.enrichment_logger.exception(f'Got an issue {e} while running the gold pipeline for batch {batch_id}')

    def optimize_and_vacuum(self):
        DeltaTable.forPath(self.spark, gold_write_path_high_risk_txns).vacuum(168)
        DeltaTable.forPath(self.spark, gold_write_path_spend_summary).vacuum(168)
        DeltaTable.forPath(self.spark, gold_write_path_product_usage_metrics).vacuum(168)
        self.spark.sql(f"OPTIMIZE delta.`{gold_write_path_high_risk_txns}` ZORDER BY (user_id)")
        self.spark.sql(f"OPTIMIZE delta.`{gold_write_path_spend_summary}` ZORDER BY (user_id)")
        self.spark.sql(f"OPTIMIZE delta.`{gold_write_path_product_usage_metrics}` ZORDER BY (product_id)")

    def write_to_gold(self, df, optimize= False):
        gold_query= df.writeStream.format('delta')\
            .option('checkpointLocation', gold_check_point_path)\
            .option('badRecordsPath', gold_bad_records_path)\
            .foreachBatch(self.process_gold_batch)\
            .start()
        gold_query.awaitTermination()
        if optimize:
            self.optimize_and_vacuum()

