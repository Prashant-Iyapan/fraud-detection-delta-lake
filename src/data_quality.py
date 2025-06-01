from src.logger import create_logger
from pyspark.sql.functions import col
from src.config import format_patterns

DQ_logger = create_logger('Data_Quality')

def check_nulls(df, batch_id, total_rows, expected_schema):
    loc_result = []
    for col_name in expected_schema:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percent = (null_count/total_rows)*100
        #Assuming Zero Threshold
        status='fail' if null_percent >0 else 'pass'
        if status == 'fail':
            DQ_logger.warning(f'The column: {col_name} has {null_percent:.2f}% null values in batch: {batch_id}')
        loc_result.append({'batch_id':batch_id,
                            'column':col_name,
                            'check': 'null_check',
                            'null_count':null_count,
                            'null_percent':null_percent,
                            'status': status})
    return loc_result

def check_range(df, batch_id, total_rows):
    loc_result = []
    range_count = df.filter(col('amount') <= 0).count()
    range_percent = (range_count/total_rows)*100
    status= 'fail' if range_percent > 0 else 'pass'
    if status == 'fail':
        DQ_logger.warning(f'The column: price has {range_percent:.2f}% values that are less or equal to 0 in batch: {batch_id}')
    loc_result.append({'batch_id': batch_id,
                        'column': 'amount',
                        'check': 'range_check',
                        'range_count': range_count,
                        'range_percent': range_percent,
                        'status': status})
    return loc_result

def check_format(df, batch_id, total_rows):
    loc_result = []
    for col_name, pattern in format_patterns.items():
        if col_name in df.columns:
            format_count = df.filter(~(col(col_name).rlike(pattern))).count()
            format_percent = (format_count/total_rows)*100
            status='fail' if format_percent > 0 else 'pass'
            if status == 'fail':
                DQ_logger.warning(f'The column: {col_name} has {format_percent:.2f}% values that do not match the pattern {pattern} in batch: {batch_id}')
            loc_result.append({'batch_id': batch_id,
                                'column': col_name,
                                'check': f'format_check for {col_name}',
                                'format_count': format_count,
                                'format_percent': format_percent,
                                'status': status})
    return loc_result
def custom_silver_dq_check(df, batch_id, total_rows):
    loc_result = []
    DQ_logger.info('Running Custom Data Quality Checks')
    usage_cost_ratio_count = df.filter((col('usage_cost_ratio').isNull()) | (col('usage_cost_ratio') <=0)).count()
    usage_cost_ratio_percent = (usage_cost_ratio_count/total_rows)* 100
    status = 'fail' if usage_cost_ratio_percent > 0 else 'pass'
    if status == 'fail':
        DQ_logger.warning(f'The column: usage_cost_ratio has {usage_cost_ratio_percent:.2f}% values that are in violation of ratio calculation in batch: {batch_id}')
    loc_result.append({'batch_id': batch_id,
                        'column': 'usage_cost_ratio',
                        'check': 'usage_cost_ratio_invalid',
                        'violation_count': usage_cost_ratio_count,
                        'violation_percent': usage_cost_ratio_percent,
                        'status': status})
    high_risk_low_usage_count = df.filter((col('usage_cost_ratio') < 0.2) & (col('risk_score') > 80)).count()
    high_risk_low_usage_percent = (high_risk_low_usage_count/total_rows)*100
    status = 'fail' if high_risk_low_usage_percent > 0 else 'pass'
    if status == 'fail':
        DQ_logger.warning(f'found {high_risk_low_usage_percent:.2f}% high-risk users with unusually low usage cost ratio in batch: {batch_id}')
    loc_result.append({'batch_id': batch_id,
                        'column': 'usage_cost_ratio',
                        'check': 'high_risk_low_usage',
                        'violation_count': high_risk_low_usage_count,
                        'violation_percent': high_risk_low_usage_percent,
                        'status': status})
    amount_w_o_monthly_cost = df.filter((col('amount').isNotNull()) & (col('monthly_cost').isNull())).count()
    amount_w_o_monthly_cost_percent = (amount_w_o_monthly_cost/total_rows)*100
    status = 'fail' if amount_w_o_monthly_cost_percent > 0 else 'pass'
    if status == 'fail':
        DQ_logger.warning(f"{amount_w_o_monthly_cost_percent:.2f}% of records violated [amount_without_monthly_cost] in batch: {batch_id}")
    loc_result.append({'batch_id': batch_id,
                        'column': 'amount',
                        'check': 'amount_without_monthly_cost',
                        'violation_count': amount_w_o_monthly_cost,
                        'violation_percent': amount_w_o_monthly_cost_percent,
                        'status': status})
    missing_currency_with_amount = df.filter((col('amount') > 0) & (col('currency').isNull())).count()
    missing_currency_with_amount_percent = (missing_currency_with_amount/total_rows)*100
    status = 'fail' if missing_currency_with_amount_percent > 0 else 'pass'
    if status == 'fail':
        DQ_logger.warning(f'found {missing_currency_with_amount_percent:.2f}% records with amount but no currency in batch: {batch_id}')
    loc_result.append({'batch_id': batch_id,
                        'column': 'amount',
                        'check': 'missing_currency_with_amount',
                        'violation_count': missing_currency_with_amount,
                        'violation_percent': missing_currency_with_amount_percent,
                        'status': status})
    return loc_result
    
def run_data_quality(df, batch_id, expected_schema, layer='bronze'):
    total_rows = df.count()
    results = []
    results.extend(check_nulls(df, batch_id, total_rows, expected_schema))
    results.extend(check_range(df, batch_id, total_rows))
    results.extend(check_format(df, batch_id, total_rows))
    if layer == 'silver': 
        results.extend(custom_silver_dq_check(df, batch_id, total_rows))
    return results
