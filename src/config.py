#logging
formatter_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log_path = '/Workspace/Users/prasanthiyyappan@gmail.com/fraud_detection_w_delta_lake/logs/'

#bronze
bronze_check_point_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/checkpoints/bronze'
bronze_write_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/bronze'
bronze_bad_records_path= 's3a://prasanth-s3bucket-ver1/fraud_detection_data/bad_records/bronze'
bronze_write_path_incoming_txns = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/bronze/incoming_txns'
stream_file = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/stream_ingestion/'
bronze_dq_result_folder = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/data_quality/bronze'

#silver
silver_check_point_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/checkpoints/silver'
silver_write_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/silver'
silver_read_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/bronze/incoming_txns'
silver_bad_records_path= 's3a://prasanth-s3bucket-ver1/fraud_detection_data/bad_records/silver'
silver_dq_result_folder = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/data_quality/silver'

#gold
gold_check_point_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/checkpoints/gold'
gold_read_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/silver'
gold_write_path_spend_summary = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/gold/user_spend_summary'
gold_write_path_product_usage_metrics = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/gold/product_usage_metrics'
gold_write_path_high_risk_txns = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/gold/high_risk_txns'
gold_bad_records_path= 's3a://prasanth-s3bucket-ver1/fraud_detection_data/bad_records/gold'

#Data Quality and Look ups
lkp_ip_risk = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/lookup_data/ip_risk_lookup_with_location_v2.csv'
lkp_prod_list = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/lookup_data/products_nvidia.csv'
lkp_subs_plan = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/lookup_data/subscription_plans_nvidia.csv'
lkp_users = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/lookup_data/users_nvidia.csv'
format_patterns = {"ip_address": r"^\d{1,3}(\.\d{1,3}){3}$",
    "card_number": r"^\d{12,19}$",
    "currency": r"^[A-Z]{3}$"}

#PII
pii_path = 's3a://prasanth-s3bucket-ver1/fraud_detection_data/delta/pii'