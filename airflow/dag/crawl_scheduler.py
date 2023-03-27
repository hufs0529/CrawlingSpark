from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import csv
import requests
import json
import datetime

now = datetime.datetime.now()

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG("forex_data_pipeline", start_date=datetime(2023, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    crawling = PythonOperator(
        task_id="crawling",
        python_callable="\airflow\Pyscript\crawling.py"
    )

    is_crawl_currencies_file_available = FileSensor(
        task_id="is_crawl_currencies_file_available",
        fs_conn_id="csv_path",
        filepath=now + '.csv',
        poke_interval=5,
        timeout=20
    )


    saving_csv = BashOperator(
        task_id="save_csv",
        bash_command="""
            hdfs dfs -mkdir -p /{now} && \
            hdfs dfs -put -f /airflow/csvpath/{now}.csv /{now}
        """
    )


    forex_processing = SparkSubmitOperator(
        task_id="crawling",
        application="\airflow\Pyscript\crawling.py",
        conn_id="spark_conn",
        verbose=False
    )


    
    crawling >> is_crawl_currencies_file_available >> saving_csv >> forex_processing