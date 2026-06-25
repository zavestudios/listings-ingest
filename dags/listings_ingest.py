from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


NAMESPACE = os.getenv("AIRFLOW_RUN_NAMESPACE", "airflow")
ETL_IMAGE = os.getenv("ETL_IMAGE", "zavestudios/etl-runner:0.1.0")
INPUT_PATH = os.getenv("ETL_INPUT_PATH", "/data/listings.csv")
EXECUTION_BACKEND = os.getenv("ETL_EXECUTION_BACKEND", "kubernetes")

# Service account name for IRSA (if using AWS Secrets Manager fallback)
SERVICE_ACCOUNT = os.getenv("AIRFLOW_SERVICE_ACCOUNT", "listings-ingest")

# Security context for hardened pods (STIG-aligned)
SECURITY_CONTEXT = k8s.V1PodSecurityContext(
    run_as_user=1000,
    run_as_group=1000,
    fs_group=1000,
    run_as_non_root=True,
)

CONTAINER_SECURITY_CONTEXT = k8s.V1SecurityContext(
    run_as_user=1000,
    run_as_group=1000,
    run_as_non_root=True,
    allow_privilege_escalation=False,
    read_only_root_filesystem=True,
    capabilities=k8s.V1Capabilities(add=[], drop=["ALL"]),
)

# Writable volume mounts for read-only filesystem
VOLUME_MOUNTS = [
    k8s.V1VolumeMount(name="tmp", mount_path="/tmp"),
    k8s.V1VolumeMount(name="cache", mount_path="/app/.cache"),
]

VOLUMES = [
    k8s.V1Volume(name="tmp", empty_dir=k8s.V1EmptyDirVolumeSource()),
    k8s.V1Volume(name="cache", empty_dir=k8s.V1EmptyDirVolumeSource()),
]


def _secret_env_vars() -> list[k8s.V1EnvVar]:
    """Build env vars from K8s Secrets (ESO-sourced from Vault)"""
    db_keys = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SSLMODE"]
    storage_keys = ["MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_ENDPOINT"]
    env_vars = [
        k8s.V1EnvVar(
            name=key,
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(name="listings-ingest-db", key=key)
            ),
        )
        for key in db_keys
    ]
    env_vars += [
        k8s.V1EnvVar(
            name=key,
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(name="listings-ingest-storage", key=key)
            ),
        )
        for key in storage_keys
    ]
    return env_vars


def _job_args(stage: str) -> list[str]:
    return [
        "-m",
        "etl.jobs.ingest_csv",
        "--input",
        INPUT_PATH,
        "--run-date",
        "{{ ds }}",
        "--batch-id",
        "{{ dag_run.run_id if dag_run else ts_nodash }}",
        "--stage",
        stage,
    ]


def _local_command(stage: str) -> str:
    return (
        "python -m etl.jobs.ingest_csv "
        f"--input {INPUT_PATH} "
        "--run-date '{{ ds }}' "
        "--batch-id '{{ dag_run.run_id if dag_run else ts_nodash }}' "
        f"--stage {stage}"
    )


with DAG(
    dag_id="listings_ingest",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["etl", "kubernetes"],
) as dag:
    if EXECUTION_BACKEND == "local":
        extract_validate = BashOperator(
            task_id="extract_validate",
            bash_command=_local_command("extract_validate"),
        )
        load_postgres = BashOperator(
            task_id="load_postgres",
            bash_command=_local_command("load_postgres"),
        )
        dq_assertions = BashOperator(
            task_id="dq_assertions",
            bash_command=_local_command("dq_assertions"),
        )
    else:
        extract_validate = KubernetesPodOperator(
            task_id="extract_validate",
            name="extract-validate",
            namespace=NAMESPACE,
            image=ETL_IMAGE,
            cmds=["python"],
            arguments=_job_args("extract_validate"),
            service_account_name=SERVICE_ACCOUNT,
            security_context=SECURITY_CONTEXT,
            container_security_context=CONTAINER_SECURITY_CONTEXT,
            volume_mounts=VOLUME_MOUNTS,
            volumes=VOLUMES,
            env_vars=_secret_env_vars(),
            in_cluster=True,
            on_finish_action="keep_pod",
            get_logs=True,
            do_xcom_push=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"},
            ),
        )

        load_postgres = KubernetesPodOperator(
            task_id="load_postgres",
            name="load-postgres",
            namespace=NAMESPACE,
            image=ETL_IMAGE,
            cmds=["python"],
            arguments=_job_args("load_postgres"),
            service_account_name=SERVICE_ACCOUNT,
            security_context=SECURITY_CONTEXT,
            container_security_context=CONTAINER_SECURITY_CONTEXT,
            volume_mounts=VOLUME_MOUNTS,
            volumes=VOLUMES,
            env_vars=_secret_env_vars(),
            in_cluster=True,
            on_finish_action="keep_pod",
            get_logs=True,
            do_xcom_push=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "150m", "memory": "256Mi"},
                limits={"cpu": "1000m", "memory": "1Gi"},
            ),
        )

        dq_assertions = KubernetesPodOperator(
            task_id="dq_assertions",
            name="dq-assertions",
            namespace=NAMESPACE,
            image=ETL_IMAGE,
            cmds=["python"],
            arguments=_job_args("dq_assertions"),
            service_account_name=SERVICE_ACCOUNT,
            security_context=SECURITY_CONTEXT,
            container_security_context=CONTAINER_SECURITY_CONTEXT,
            volume_mounts=VOLUME_MOUNTS,
            volumes=VOLUMES,
            env_vars=_secret_env_vars(),
            in_cluster=True,
            on_finish_action="keep_pod",
            get_logs=True,
            do_xcom_push=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"},
            ),
        )

    extract_validate >> load_postgres >> dq_assertions
