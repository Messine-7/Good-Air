from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(
    dag_id='pipeline_elt_docker',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['elt']
) as dag:

    elt_task = DockerOperator(
        task_id='run_elt',
        image='elt:latest',  # Nom du service ou image Docker
        container_name='elt_task_container',
        command='python /app/script.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[Mount(source="D:/DATA/2025-06-01_MSPR_1/Good-Air/elt", target="/app", type="bind")],
        mount_tmp_dir=False 
    )

    dbt_task = DockerOperator(
        task_id='run_dbt',
        image='dbt',  # Nom du service ou image Docker
        container_name='dbt_task_container',
        command='dbt run',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline'
    )

    elt_task >> dbt_task
