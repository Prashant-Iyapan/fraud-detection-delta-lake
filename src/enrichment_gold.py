from src.config import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, DoubleType, StringType, TimestampType
from src.logger import create_logger
from pyspark.sql.functions import col, sum, avg, when

class Enrichment:
    def __init__(self, source_path):
        self.spark = SparkSession.builder.appName('gold').getOrCreate()
        self.enrichment_logger = create_logger('Enrichment')
        self.set_schema = StructType([StructField('transaction_id', StringType()),
            StructField('event_time', TimestampType()),
            StructField('user_id', StringType()),
            StructField('product_id', StringType()),
            StructField('subscription_type', StringType()),
            StructField('amount', DoubleType()),
            StructField('location', StringType()),
            StructField('hash_ip', StringType()),
            StructField('device_id', StringType()),
            StructField('payment_method', StringType()),
            StructField('hash_card_number', StringType()),
            StructField('currency', StringType()),
            StructField('is_new_user', BooleanType()),
            StructField('referral_code', StringType()),
            StructField('session_id', StringType()),
            StructField('missing_user', BooleanType()),
            StructField('missing_product', BooleanType()),
            StructField('missing_subscription', BooleanType()),
            StructField('missing_ip', BooleanType()),
            StructField('usage_cost_ratio', DoubleType())])
        self.source_path = source_path

    def read_silver_data(self):
        self.enrichment_logger.info('Reading the Source file from Silver path')
        try:
            df = self.spark.read.format('delta').load(self.source_path)
            self.enrichment_logger.info('Silver Source file loaded into DataFrame')
            return df
        except Exception as e:
            self.enrichment_logger.exception(f'Got an issue {e} while reading the silver data at {self.source_path}')
            return None
        
    def user_spend_summary(self,df):
        self.enrichment_logger.info('Calculating the spend summary for each user')
        spend_summary_df = df.groupBy(col('user_id')).agg(sum(col('amount')).alias('Total_spent_per_user'))
        return spend_summary_df
    
    def product_usage_metrics(self, df):
        self.enrichment_logger.info('Calculating the usage metrics for each product')
        usage_metrics_df = df.groupBy(col('product_id')).agg(avg(col('usage_cost_ratio')).alias('average_usage_cost_ratio'))
        return usage_metrics_df
    
    def flag_high_risk_txns(self, df):
        self.enrichment_logger.info('Flagging High Risk Transactions now')
        high_risk_txn_df = df.withColumn('is_high_risk_txn', when((col('risk_level') == 'high') & (col('usage_cost_ratio') < 0.2), True).otherwise(False))
        return high_risk_txn_df
    
    def gold_run(self):
        try:
            silver_data_df = self.read_silver_data()
            if silver_data_df is not None:
                spend_summary_df = self.user_spend_summary(silver_data_df)
                usage_metrics_df = self.product_usage_metrics(silver_data_df)
                high_risk_txn_df = self.flag_high_risk_txns(silver_data_df)
                self.enrichment_logger.info('Writing the Gold DataFrames to Delta')
                spend_summary_df.write.format('delta').mode('overwrite').save(gold_write_path_spend_summary)
                usage_metrics_df.write.format('delta').mode('overwrite').save(gold_write_path_product_usage_metrics)
                high_risk_txn_df.write.format('delta').mode('overwrite').save(gold_write_path_high_risk_txns)
                self.enrichment_logger.info('Gold DataFrames written to Delta')
        except Exception as e:
            self.enrichment_logger.exception(f'Got an issue {e} while running the gold pipeline')


