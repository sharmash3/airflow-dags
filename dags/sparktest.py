from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 6),
}

with DAG('run_transaction_filter',
         schedule_interval=None,
         default_args=default_args,
         catchup=False) as dag:

    spark_job = SparkSubmitOperator(
        task_id='transaction_filter',
        application='/opt/spark-apps/transaction-filter.jar',
        java_class='com.example.TransactionFilter',
        application_args=['/opt/spark-apps/transactions.csv'],
        conn_id='spark_default',  # make sure this connection exists in Airflow
        executor_memory='1g',
        driver_memory='1g',
        verbose=True,
    )
