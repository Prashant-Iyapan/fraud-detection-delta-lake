from src.logger import create_logger

expected_schema_bronze = {
    'transaction_id': 'StringType',
    'event_time': 'TimestampType',
    'user_id': 'StringType',
    'product_id': 'StringType',
    'subscription_type': 'StringType',
    'amount': 'DoubleType',
    'location': 'StringType',
    'ip_address': 'StringType',
    'device_id': 'StringType',
    'payment_method': 'StringType',
    'card_number': 'StringType',
    'currency': 'StringType',
    'is_new_user': 'BooleanType',
    'referral_code': 'StringType',
    'session_id': 'StringType'
}

expected_schema_silver = {
    'transaction_id': 'StringType',
    'event_time': 'TimestampType',
    'user_id': 'StringType',
    'product_id': 'StringType',
    'subscription_type': 'StringType',
    'amount': 'DoubleType',
    'location': 'StringType',
    'device_id': 'StringType',
    'payment_method': 'StringType',
    'card_number': 'StringType',
    'currency': 'StringType',
    'is_new_user': 'BooleanType',
    'referral_code': 'StringType',
    'session_id': 'StringType',
    'hash_ip': 'StringType',
    'hash_card_number': 'StringType',
    'country': 'StringType',
    'user_status': 'StringType',
    'missing_user': 'BooleanType',
    'product_name': 'StringType',
    'category':  'StringType',
    'price': 'DoubleType',
    'missing_product': 'BooleanType',
    'monthly_cost': 'DoubleType',
    'churn_risk': 'StringType',
    'plan_duration': 'IntegerType',
    'missing_subscription': 'BooleanType',
    'risk_level': 'StringType',
    'risk_score': 'DoubleType',
    'ip_risk_location': 'StringType',
    'missing_ip': 'BooleanType',
    'usage_cost_ratio': 'DoubleType',
    'event_date': 'DateType'
}

val_logger = create_logger('validation')

def validate_columns(val_dataframe, module='Ingestion'):
    val_logger.info("Validating the incoming columns")
    if module =='Ingestion':
        for key, value in expected_schema_bronze.items():
            if key not in val_dataframe.columns:
                val_logger.warning(f'column {key} is missing, stream input is not as expected')
                return False
            elif value != type(val_dataframe.schema[key].dataType).__name__:
                val_logger.warning(f'the column {key} dataType expected is {value} but the value is {val_dataframe.schema[key]}')
                return False
            else:
                val_logger.info(f'column {key} is as expected with datatype {value}')
        return True
    else:
        for key, value in expected_schema_silver.items():
            if key not in val_dataframe.columns:
                val_logger.warning(f'column {key} is missing, stream input is not as expected')
                return False
            elif value != type(val_dataframe.schema[key].dataType).__name__:
                val_logger.warning(f'the column {key} dataType expected is {value} but the value is {val_dataframe.schema[key]}')
                return False
            else:
                val_logger.info(f'column {key} is as expected with datatype {value}')
        return True
