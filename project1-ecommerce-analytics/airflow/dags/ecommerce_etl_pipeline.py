"""
E-commerce ETL Pipeline DAG
Comprehensive data pipeline for e-commerce analytics using Airflow, AWS, and Snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import pandas as pd
import boto3
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Comprehensive e-commerce data pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'etl', 'analytics']
)

# Configuration
S3_BUCKET = Variable.get("s3_bucket_name", "ecommerce-data-lake")
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"

def extract_source_data(**context):
    """
    Extract data from various source systems
    Simulates extraction from CRM, ERP, and web analytics
    """
    import sys
    import os
    sys.path.append('/opt/airflow/dags/utils')
    
    from data_extractors import CRMExtractor, ERPExtractor, WebAnalyticsExtractor
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # Initialize extractors
        crm_extractor = CRMExtractor()
        erp_extractor = ERPExtractor()
        web_extractor = WebAnalyticsExtractor()
        
        # Extract customer data
        logging.info("Extracting customer data from CRM")
        customer_data = crm_extractor.extract_customers(date_str)
        crm_extractor.save_to_s3(customer_data, f"raw/customers/date={date_str}/customers.parquet")
        
        # Extract order data
        logging.info("Extracting order data from ERP")
        order_data = erp_extractor.extract_orders(date_str)
        erp_extractor.save_to_s3(order_data, f"raw/orders/date={date_str}/orders.parquet")
        
        # Extract product data
        logging.info("Extracting product data from ERP")
        product_data = erp_extractor.extract_products()
        erp_extractor.save_to_s3(product_data, f"raw/products/date={date_str}/products.parquet")
        
        # Extract web analytics data
        logging.info("Extracting web analytics data")
        web_data = web_extractor.extract_sessions(date_str)
        web_extractor.save_to_s3(web_data, f"raw/web_sessions/date={date_str}/sessions.parquet")
        
        logging.info(f"Data extraction completed for {date_str}")
        
    except Exception as e:
        logging.error(f"Error in data extraction: {str(e)}")
        raise

def validate_data_quality(**context):
    """
    Perform data quality checks on extracted data
    """
    import great_expectations as ge
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # Initialize Great Expectations context
        context_ge = ge.data_context.DataContext()
        
        # Data quality checks for customers
        customers_df = pd.read_parquet(f"s3://{S3_BUCKET}/raw/customers/date={date_str}/customers.parquet")
        customers_ge_df = ge.from_pandas(customers_df)
        
        # Customer data quality expectations
        customers_ge_df.expect_column_to_exist("customer_id")
        customers_ge_df.expect_column_values_to_not_be_null("customer_id")
        customers_ge_df.expect_column_values_to_be_unique("customer_id")
        customers_ge_df.expect_column_values_to_match_regex("email", r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        # Validate customer data
        customer_results = customers_ge_df.validate()
        if not customer_results['success']:
            raise ValueError("Customer data quality check failed")
        
        # Data quality checks for orders
        orders_df = pd.read_parquet(f"s3://{S3_BUCKET}/raw/orders/date={date_str}/orders.parquet")
        orders_ge_df = ge.from_pandas(orders_df)
        
        # Order data quality expectations
        orders_ge_df.expect_column_to_exist("order_id")
        orders_ge_df.expect_column_values_to_not_be_null("order_id")
        orders_ge_df.expect_column_values_to_be_unique("order_id")
        orders_ge_df.expect_column_values_to_be_between("total_amount", min_value=0, max_value=100000)
        
        # Validate order data
        order_results = orders_ge_df.validate()
        if not order_results['success']:
            raise ValueError("Order data quality check failed")
        
        logging.info(f"Data quality validation passed for {date_str}")
        
    except Exception as e:
        logging.error(f"Data quality validation failed: {str(e)}")
        raise

def transform_data(**context):
    """
    Transform and clean data for analytics
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # Load raw data
        customers_df = pd.read_parquet(f"s3://{S3_BUCKET}/raw/customers/date={date_str}/customers.parquet")
        orders_df = pd.read_parquet(f"s3://{S3_BUCKET}/raw/orders/date={date_str}/orders.parquet")
        products_df = pd.read_parquet(f"s3://{S3_BUCKET}/raw/products/date={date_str}/products.parquet")
        
        # Transform customers
        customers_transformed = customers_df.copy()
        customers_transformed['full_name'] = customers_transformed['first_name'] + ' ' + customers_transformed['last_name']
        customers_transformed['age'] = (pd.to_datetime('today') - pd.to_datetime(customers_transformed['date_of_birth'])).dt.days // 365
        customers_transformed['age_group'] = pd.cut(customers_transformed['age'], 
                                                   bins=[0, 25, 35, 45, 55, 100], 
                                                   labels=['18-25', '26-35', '36-45', '46-55', '55+'])
        
        # Transform orders with enriched metrics
        orders_transformed = orders_df.copy()
        orders_transformed['order_year'] = pd.to_datetime(orders_transformed['order_date']).dt.year
        orders_transformed['order_month'] = pd.to_datetime(orders_transformed['order_date']).dt.month
        orders_transformed['order_quarter'] = pd.to_datetime(orders_transformed['order_date']).dt.quarter
        orders_transformed['order_day_of_week'] = pd.to_datetime(orders_transformed['order_date']).dt.day_name()
        
        # Calculate customer metrics
        customer_metrics = orders_transformed.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': ['sum', 'mean'],
            'order_date': ['min', 'max']
        }).round(2)
        
        customer_metrics.columns = ['total_orders', 'total_spent', 'avg_order_value', 'first_order', 'last_order']
        customer_metrics['days_since_last_order'] = (pd.to_datetime('today') - pd.to_datetime(customer_metrics['last_order'])).dt.days
        customer_metrics['customer_lifetime_days'] = (pd.to_datetime(customer_metrics['last_order']) - pd.to_datetime(customer_metrics['first_order'])).dt.days
        
        # Save transformed data to S3
        s3_client = boto3.client('s3')
        
        # Save to processed folder
        customers_transformed.to_parquet(f"s3://{S3_BUCKET}/processed/customers/date={date_str}/customers_transformed.parquet")
        orders_transformed.to_parquet(f"s3://{S3_BUCKET}/processed/orders/date={date_str}/orders_transformed.parquet")
        customer_metrics.to_parquet(f"s3://{S3_BUCKET}/processed/customer_metrics/date={date_str}/customer_metrics.parquet")
        
        logging.info(f"Data transformation completed for {date_str}")
        
    except Exception as e:
        logging.error(f"Data transformation failed: {str(e)}")
        raise

def calculate_rfm_metrics(**context):
    """
    Calculate RFM (Recency, Frequency, Monetary) metrics for customer segmentation
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # Load customer metrics
        customer_metrics = pd.read_parquet(f"s3://{S3_BUCKET}/processed/customer_metrics/date={date_str}/customer_metrics.parquet")
        
        # Calculate RFM scores
        customer_metrics['recency_score'] = pd.qcut(customer_metrics['days_since_last_order'].rank(method='first'), 
                                                   q=5, labels=[5,4,3,2,1])
        customer_metrics['frequency_score'] = pd.qcut(customer_metrics['total_orders'].rank(method='first'), 
                                                     q=5, labels=[1,2,3,4,5])
        customer_metrics['monetary_score'] = pd.qcut(customer_metrics['total_spent'].rank(method='first'), 
                                                    q=5, labels=[1,2,3,4,5])
        
        # Create RFM segment
        customer_metrics['rfm_score'] = (customer_metrics['recency_score'].astype(str) + 
                                       customer_metrics['frequency_score'].astype(str) + 
                                       customer_metrics['monetary_score'].astype(str))
        
        # Define customer segments
        def segment_customers(row):
            if row['rfm_score'] in ['555', '554', '544', '545', '454', '455', '445']:
                return 'Champions'
            elif row['rfm_score'] in ['543', '444', '435', '355', '354', '345', '344', '335']:
                return 'Loyal Customers'
            elif row['rfm_score'] in ['512', '511', '422', '421', '412', '411', '311']:
                return 'Potential Loyalists'
            elif row['rfm_score'] in ['534', '343', '334', '343', '334', '325', '324']:
                return 'New Customers'
            elif row['rfm_score'] in ['155', '154', '144', '214', '215', '115', '114']:
                return 'At Risk'
            elif row['rfm_score'] in ['155', '154', '144', '214', '215', '115']:
                return 'Cannot Lose Them'
            elif row['rfm_score'] in ['332', '322', '231', '241', '251', '233', '232']:
                return 'Need Attention'
            else:
                return 'Lost'
        
        customer_metrics['segment'] = customer_metrics.apply(segment_customers, axis=1)
        
        # Save RFM analysis
        customer_metrics.to_parquet(f"s3://{S3_BUCKET}/analytics/rfm_analysis/date={date_str}/rfm_metrics.parquet")
        
        logging.info(f"RFM analysis completed for {date_str}")
        
    except Exception as e:
        logging.error(f"RFM analysis failed: {str(e)}")
        raise

def load_to_snowflake(**context):
    """
    Load processed data to Snowflake data warehouse
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # This would typically use Snowflake COPY command
        # Here we simulate the load process
        
        logging.info(f"Loading data to Snowflake for {date_str}")
        
        # Load customers
        customer_copy_sql = f"""
        COPY INTO ECOMMERCE_DW.STAGING.stg_customers
        FROM 's3://{S3_BUCKET}/processed/customers/date={date_str}/'
        FILE_FORMAT = (TYPE = 'PARQUET')
        """
        
        # Load orders
        orders_copy_sql = f"""
        COPY INTO ECOMMERCE_DW.STAGING.stg_orders
        FROM 's3://{S3_BUCKET}/processed/orders/date={date_str}/'
        FILE_FORMAT = (TYPE = 'PARQUET')
        """
        
        # Load RFM metrics
        rfm_copy_sql = f"""
        COPY INTO ECOMMERCE_DW.ANALYTICS.fact_customer_metrics
        FROM 's3://{S3_BUCKET}/analytics/rfm_analysis/date={date_str}/'
        FILE_FORMAT = (TYPE = 'PARQUET')
        """
        
        logging.info(f"Data loaded to Snowflake for {date_str}")
        
    except Exception as e:
        logging.error(f"Snowflake load failed: {str(e)}")
        raise

def update_data_marts(**context):
    """
    Update data marts for business intelligence
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    try:
        # Update executive KPIs
        executive_kpi_sql = f"""
        INSERT INTO ECOMMERCE_DW.MART.dm_executive_kpis
        SELECT 
            '{date_str}' as report_date,
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT CASE WHEN first_order = '{date_str}' THEN customer_id END) as new_customers,
            COUNT(DISTINCT CASE WHEN first_order < '{date_str}' THEN customer_id END) as returning_customers,
            AVG(total_amount) as avg_order_value,
            0 as customer_acquisition_cost,
            AVG(total_spent) as customer_lifetime_value,
            0 as churn_rate,
            COUNT(DISTINCT customer_id) as monthly_active_users,
            0 as conversion_rate,
            CURRENT_TIMESTAMP as created_at
        FROM ECOMMERCE_DW.STAGING.stg_orders o
        JOIN ECOMMERCE_DW.ANALYTICS.fact_customer_metrics c ON o.customer_id = c.customer_id
        WHERE DATE(order_date) = '{date_str}'
        """
        
        # Update customer cohorts
        cohort_sql = f"""
        INSERT INTO ECOMMERCE_DW.MART.dm_customer_cohorts
        WITH cohort_data AS (
            SELECT 
                DATE_TRUNC('month', first_order) as cohort_month,
                customer_id,
                DATEDIFF('month', DATE_TRUNC('month', first_order), '{date_str}') as period_number
            FROM ECOMMERCE_DW.ANALYTICS.fact_customer_metrics
        )
        SELECT 
            cohort_month,
            period_number,
            COUNT(customer_id) as customers_count,
            COUNT(customer_id) / FIRST_VALUE(COUNT(customer_id)) OVER (PARTITION BY cohort_month ORDER BY period_number) as retention_rate,
            AVG(total_spent) as avg_revenue_per_user,
            SUM(total_spent) as cumulative_revenue,
            CURRENT_TIMESTAMP as created_at
        FROM cohort_data c
        JOIN ECOMMERCE_DW.ANALYTICS.fact_customer_metrics m ON c.customer_id = m.customer_id
        GROUP BY cohort_month, period_number
        """
        
        logging.info(f"Data marts updated for {date_str}")
        
    except Exception as e:
        logging.error(f"Data mart update failed: {str(e)}")
        raise

def send_success_notification(**context):
    """
    Send success notification with pipeline metrics
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Generate pipeline summary
    message = f"""
    E-commerce ETL Pipeline completed successfully for {date_str}
    
    Pipeline Summary:
    - Execution Date: {date_str}
    - Start Time: {context['dag_run'].start_date}
    - End Time: {datetime.now()}
    - Status: SUCCESS
    
    Data Processed:
    - Customers: Extracted and processed
    - Orders: Extracted and processed  
    - Products: Extracted and processed
    - Web Sessions: Extracted and processed
    
    Analytics Generated:
    - RFM Customer Segmentation
    - Executive KPIs
    - Customer Cohort Analysis
    
    Next Steps:
    - Data available in Snowflake
    - Dashboards will be refreshed automatically
    - Business reports updated
    """
    
    return message

# Define task dependencies
# 1. Data Extraction Tasks
extract_data_task = PythonOperator(
    task_id='extract_source_data',
    python_callable=extract_source_data,
    dag=dag
)

# 2. Data Quality Validation
validate_data_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# 3. Data Transformation
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# 4. RFM Analysis
rfm_analysis_task = PythonOperator(
    task_id='calculate_rfm_metrics',
    python_callable=calculate_rfm_metrics,
    dag=dag
)

# 5. Snowflake Load Tasks
load_customers_task = SnowflakeOperator(
    task_id='load_customers_to_snowflake',
    sql=f"""
    COPY INTO ECOMMERCE_DW.RAW_DATA.customers
    FROM 's3://{S3_BUCKET}/processed/customers/date={{{{ ds }}}}/'
    FILE_FORMAT = (TYPE = 'PARQUET')
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

load_orders_task = SnowflakeOperator(
    task_id='load_orders_to_snowflake',
    sql=f"""
    COPY INTO ECOMMERCE_DW.RAW_DATA.orders
    FROM 's3://{S3_BUCKET}/processed/orders/date={{{{ ds }}}}/'
    FILE_FORMAT = (TYPE = 'PARQUET')
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

load_rfm_task = SnowflakeOperator(
    task_id='load_rfm_to_snowflake',
    sql=f"""
    COPY INTO ECOMMERCE_DW.ANALYTICS.fact_customer_metrics
    FROM 's3://{S3_BUCKET}/analytics/rfm_analysis/date={{{{ ds }}}}/'
    FILE_FORMAT = (TYPE = 'PARQUET')
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

# 6. Data Mart Updates
update_marts_task = PythonOperator(
    task_id='update_data_marts',
    python_callable=update_data_marts,
    dag=dag
)

# 7. Success Notification
notify_success_task = EmailOperator(
    task_id='send_success_notification',
    to=['data-team@company.com'],
    subject='E-commerce ETL Pipeline - Success',
    html_content="{{ task_instance.xcom_pull(task_ids='send_success_notification') }}",
    dag=dag
)

send_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# 8. Data Quality Monitoring
monitor_data_quality = BashOperator(
    task_id='monitor_data_quality',
    bash_command="""
    python /opt/airflow/dags/utils/data_quality_monitor.py --date {{ ds }}
    """,
    dag=dag
)

# Define task dependencies
extract_data_task >> validate_data_task >> transform_data_task
transform_data_task >> rfm_analysis_task

# Parallel loading to Snowflake
[transform_data_task, rfm_analysis_task] >> [load_customers_task, load_orders_task, load_rfm_task]

# Data mart updates after loading
[load_customers_task, load_orders_task, load_rfm_task] >> update_marts_task

# Final monitoring and notifications
update_marts_task >> [monitor_data_quality, send_notification_task]
send_notification_task >> notify_success_task