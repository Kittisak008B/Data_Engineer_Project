from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime , timedelta

import pandas as pd
import requests
import sqlalchemy
import pymysql
import os

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

def get_data_from_db():
    # Connect to the database
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            db=Config.MYSQL_DB, 
        ) 
    )
    customer = pd.read_sql("SELECT * FROM customer", engine)
    product = pd.read_sql("SELECT * FROM product", engine)
    transaction = pd.read_sql("SELECT * FROM transaction", engine)
    merged_transaction = transaction.merge(product, how="left", left_on="ProductNo", right_on="ProductNo").merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
    merged_transaction.to_csv("/home/airflow/data/transaction_from_db.csv", index = False)

def get_data_from_api():
    url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
    result_conversion_rate = requests.get(url).json()
  
    conversion_rate = pd.DataFrame(result_conversion_rate)
    conversion_rate = conversion_rate.drop(columns=['id'])
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
    
    conversion_rate.to_csv("/home/airflow/data/conversion_rate_from_api.csv", index = False)

def convert_to_thb():
    merged_transaction = pd.read_csv("/home/airflow/data/transaction_from_db.csv")
    conversion_rate = pd.read_csv("/home/airflow/data/conversion_rate_from_api.csv")

    # merge 2 DataFrame (merged_transaction and conversion_rate)
    final_df = merged_transaction.merge(conversion_rate, how="left", left_on="Date", right_on="date")
    # create total_amount and thb_amount columns
    final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
    final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]
    # drop unused columns and rename columns
    final_df = final_df.drop(["date", "gbp_thb"], axis=1)
    final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
                        'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']
    # save file
    final_df.to_csv("/home/airflow/data/final_result.csv", index=False)

# Default Arguments
default_args = {
    'owner': 'user007',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), }

# Create DAG
dag = DAG(
    'sales_transaction_pipeline',
    default_args=default_args,
    description='Pipeline for ETL sales transaction data',
    schedule_interval=timedelta(days=1), )

# Tasks
t1 = PythonOperator(
    task_id = 'transaction_data_ingestion_from_database',
    python_callable = get_data_from_db,
    dag=dag, )
t2 = PythonOperator(
    task_id = 'get_conversion_rate_from_api',
    python_callable = get_data_from_api,
    dag=dag, )
t3 = PythonOperator(
    task_id = 'merge_data',
    python_callable = convert_to_thb,
    dag=dag, )

# Dependencies
[t1, t2] >> t3