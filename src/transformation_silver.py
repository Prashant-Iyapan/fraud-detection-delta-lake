from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, TimestampType
from src.logger import create_logger
from pyspark.sql import SparkSession
from src.config import *
from pyspark.sql.functions import col, when, broadcast
from src.data_quality import run_data_quality, custom_silver_dq_check
from src.validation import expected_schema_silver
from datetime import datetime

class Transformation:
    def __init__(self, source_path):
        self.spark=SparkSession.builder.appName('Transformation').getOrCreate()
        self.module = 'Transformation'
        self.source_file = source_path
        self.transform_logger=create_logger(module=self.module)
        self.set_schema = StructType([
            StructField('transaction_id', StringType(), True),
            StructField('event_time', TimestampType(), True),
            StructField('user_id', StringType(), True),
            StructField('product_id', StringType(), True),
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
        self.lkp_ip_risk_df = self.spark.read.csv(lkp_ip_risk, header=True,inferSchema=True)
        self.prod_list_df = self.spark.read.csv(lkp_prod_list, header=True,inferSchema=True)
        self.subs_plan_df = self.spark.read.csv(lkp_subs_plan, header=True, inferSchema=True)
        self.user_list_df = self.spark.read.csv(lkp_users, header=True, inferSchema=True)
        
        
    def read_bronze_data(self):
        self.transform_logger.info(f'Reading data from {self.source_file}')
        try:
            df = self.spark.read.format('delta').load(self.source_file)         
            self.transform_logger.info('Bronze data loaded into Dataframe Now')
            return df
        except Exception as e:
            self.transform_logger.exception(f'Error occured while reading data from {self.source_file} and the exception: {e}')
            return None
        

    def enrich_with_lookups(self, df):
        df.printSchema()
        df = df.alias('bronze')
        self.user_list_df = self.user_list_df.alias('user_list')
        risk_df = df.join(broadcast(self.user_list_df), col('bronze.user_id') == col('user_list.user_id'), how='left')
        risk_df = risk_df.withColumn('missing_user', when(col('user_list.user_id').isNull(), True).otherwise(False))
        risk_df = risk_df.select(col('bronze.*'),col('user_list.country'),col('user_list.user_status'), col('missing_user')).alias('risk')
        #print(risk_df.columns)
        self.prod_list_df = self.prod_list_df.alias('prod_list')
        risk_prod_df = risk_df.join(broadcast(self.prod_list_df), col('risk.product_id') == col('prod_list.product_id'), how='left')
        risk_prod_df = risk_prod_df.withColumn('missing_product', when(col('prod_list.product_id').isNull(), True).otherwise(False))
        risk_prod_df = risk_prod_df.select(col('risk.*'), col('prod_list.product_name'), col('prod_list.category'), col('prod_list.price'), col('missing_product')).alias('risk_prod')
        #print(risk_prod_df.columns)
        self.subs_plan_df = self.subs_plan_df.alias('subs_plan')
        risk_prod_sub_df = risk_prod_df.join(broadcast(self.subs_plan_df), col('risk_prod.subscription_type') == col('subs_plan.subscription_type'), how='left')
        risk_prod_sub_df = risk_prod_sub_df.withColumn('missing_subscription', when(col('subs_plan.subscription_type').isNull(), True).otherwise(False))
        risk_prod_sub_df = risk_prod_sub_df.select(col('risk_prod.*'), col('subs_plan.monthly_cost'),col('subs_plan.churn_risk'), col('subs_plan.plan_duration'),col('missing_subscription')).alias('risk_prod_sub')
        #print(risk_prod_sub_df.columns)
        self.lkp_ip_risk_df = self.lkp_ip_risk_df.alias('ip_risk')
        ip_prod_risk_sub_df = risk_prod_sub_df.join(broadcast(self.lkp_ip_risk_df), col('risk_prod_sub.hash_ip') == col('ip_risk.ip_hash'), how = 'left')
        ip_prod_risk_sub_df = ip_prod_risk_sub_df.withColumn('missing_ip', when(col('ip_risk.ip_hash').isNull(), True).otherwise(False))
        ip_prod_risk_sub_df = ip_prod_risk_sub_df.select(col('risk_prod_sub.*'), col('ip_risk.risk_level'), col('ip_risk.risk_score'), col('missing_ip')).alias('risk_prod_sub_ip')
        final_silver_df = ip_prod_risk_sub_df.withColumn('usage_cost_ratio', col('amount')/col('monthly_cost'))
        final_silver_df.printSchema()
        return final_silver_df
    
    def run_dq_silver(self, df):
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
        df.write.format('delta').mode('append').save(silver_write_path)
        self.transform_logger.info("We have successfully written into Silver layer")
        

        


