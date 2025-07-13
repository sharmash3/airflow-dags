from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="transaction_filter_spark_k8s",
    start_date=datetime(2025, 7, 13),
    schedule=None,
    catchup=False,
    tags=["spark", "kubernetes", "transaction"],
) as dag:

    transaction_filter = SparkKubernetesOperator(
        task_id="submit_transaction_filter",
        namespace="default",
        application_file="sparkapps/transaction-filter.yaml",
        do_xcom_push=False,
    )

    transaction_filter
