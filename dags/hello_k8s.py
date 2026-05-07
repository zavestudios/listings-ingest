from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


NAMESPACE = os.getenv("AIRFLOW_RUN_NAMESPACE", "airflow")
ETL_IMAGE = os.getenv("ETL_IMAGE", "zavestudios/etl-runner:0.1.0")
SERVICE_ACCOUNT = os.getenv("AIRFLOW_RUN_SERVICE_ACCOUNT", NAMESPACE)


with DAG(
    dag_id="hello_k8s",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "kubernetes"],
) as dag:
    hello = KubernetesPodOperator(
        task_id="hello",
        name="hello-k8s",
        namespace=NAMESPACE,
        image=ETL_IMAGE,
        service_account_name=SERVICE_ACCOUNT,
        cmds=["python", "-c"],
        arguments=["print('hello from kubernetes pod operator')"],
        in_cluster=True,
        on_finish_action="delete_pod",
        get_logs=True,
        do_xcom_push=False,
    )
