from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(
    dag_id='pipeline_elt_docker',
    start_date=datetime(2025, 8, 31),
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['elt']
) as dag:
    
    weather_task = DockerOperator(
        task_id='run_open_weather_extract',
        image='elt:latest',  # même image que ton script.py
        container_name='weather_task_container',
        command='python /app/open_weather_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[Mount(
            source="D:/DATA/2025-06-01_MSPR_1/Good-Air/elt",
            target="/app",
            type="bind"
        )],
        mount_tmp_dir=False
    )

    aqicn_task = DockerOperator(
        task_id='run_aqicn_extract',
        image='elt:latest',  # même image que ton script.py
        container_name='aqicn_task_container',
        command='python /app/aqicn_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[Mount(
            source="D:/DATA/2025-06-01_MSPR_1/Good-Air/elt",
            target="/app",
            type="bind"
        )],
        mount_tmp_dir=False
    )    
    
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

    """dbt_task = DockerOperator(
        task_id='run_dbt',
        image='dbt',  # Nom du service ou image Docker
        container_name='dbt_task_container',
        command='dbt run',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline'
    )"""

    weather_task >> elt_task #>> dbt_task
