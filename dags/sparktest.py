from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 6),
}

with DAG(
    dag_id='run_transaction_filter',
    schedule=None,
    default_args=default_args,
    catchup=False
) as dag:

   spark_job = SparkSubmitOperator(
    task_id='transaction_filter',
    application='/opt/spark-apps/transaction-filter.jar',
    main_class='com.example.TransactionFilter',  # <-- fixed here
    application_args=['/opt/spark-apps/transactions.csv'],
    conn_id='spark_default',
    verbose=True,
    conf={
        "spark.master": "k8s://https://kubernetes.default.svc",
        "spark.submit.deployMode": "cluster",
        "spark.kubernetes.container.image": "apache/spark:3.5.3",
        "spark.kubernetes.namespace": "default",
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.kubernetes.driver.pod.name": "spark-driver-transaction-filter",
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent"
    }
)

