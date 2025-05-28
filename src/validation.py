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
    'hash_ip': 'StringType',
    'device_id': 'StringType',
    'payment_method': 'StringType',
    'hash_card_number': 'StringType',
    'currency': 'StringType',
    'is_new_user': 'BooleanType',
    'referral_code': 'StringType',
    'session_id': 'StringType',
    'missing_user': 'BooleanType',
    'missing_product': 'BooleanType',
    'missing_subscription': 'BooleanType',
    'missing_ip': 'BooleanType',
    'usage_cost_ratio': 'DoubleType'
}

val_logger = create_logger('validation')


def validate_columns(val_dataframe):
    val_logger.info("Validating the incoming columns")
    for key, value in expected_schema_bronze.items():
        if key not in val_dataframe.columns:
            val_logger.warning(f'column{key} is missing, stream input is not as expected')
            return False
        elif value != type(val_dataframe.schema[key].dataType).__name__:
            val_logger.warning(f'the column{key} dataType expected is {value} but the value is {val_dataframe.schema[key]}')
            return False
        else:
            val_logger.info(f'column{key} is as expected with datatype {value}')
    return True
