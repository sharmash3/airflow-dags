from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from datetime import datetime

with DAG(
    dag_id="transaction_filter_spark_k8s",
    start_date=datetime(2025, 7, 13),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "kubernetes", "transaction"],
) as dag:

    transaction_filter = SparkKubernetesOperator(
        task_id="submit_transaction_filter",
        namespace="default",
        application_manifest={
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": "transaction-filter",
                "namespace": "default",
            },
            "spec": {
                "type": "Scala",
                "mode": "cluster",
                "image": "transaction-filter:2.0",
                "imagePullPolicy": "IfNotPresent",
                "mainClass": "com.example.TransactionFilter",
                "mainApplicationFile": "local:///opt/spark/jars/transaction-filter-1.0.jar",
                "arguments": [
                    "/opt/spark/data/transactions.csv"
                ],
                "sparkVersion": "3.5.1",
                "restartPolicy": {
                    "type": "Never"
                },
                "driver": {
                    "cores": 1,
                    "coreLimit": "1200m",
                    "memory": "512m",
                    "labels": {
                        "version": "3.5.1"
                    },
                    "serviceAccount": "spark"
                },
                "executor": {
                    "cores": 1,
                    "instances": 1,
                    "memory": "512m",
                    "labels": {
                        "version": "3.5.1"
                    }
                }
            }
        },
        do_xcom_push=False
    )

    transaction_filter
