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
        task_id='open_weather_extract',
        image='elt:latest',  
        container_name='weather_task_container',
        command='python /app/open_weather_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=f"C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/elt", target="/app", type="bind"),
            Mount(source="C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/.env", target="/app/.env", type="bind"),
            ],
        mount_tmp_dir=False
    )

    aqicn_task = DockerOperator(
        task_id='aqicn_extract',
        image='elt:latest',  
        container_name='aqicn_task_container',
        command='python /app/aqicn_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source="C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/elt", target="/app", type="bind"),
            Mount(source="C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/.env", target="/app/.env", type="bind"),
            ],
        mount_tmp_dir=False
    )    
    
    dbt_task = DockerOperator(
        task_id='open_weather_dbt_transform',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',  # image officielle dbt-snowflake
        container_name='dbt_task_container',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(
                source="C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/dbt/dbt_project",
                target="/app/dbt_project",
                type="bind"
            ),
            Mount(
                source="C:/Users/guill/OneDrive/Documents/MSPR 2025/Good-Air/dbt",
                target="/root/.dbt",
                type="bind"
            ),
        ],
        mount_tmp_dir=False

    )

    weather_task >>  dbt_task
    
with DAG(
    dag_id='pipeline_etl_docker',
    start_date=datetime(2025, 8, 31),
    schedule_interval = "0 0 1,15 * *",  # 1er et 15 du mois à minuit
    catchup=False,
    tags=['etl']
) as dag:

    etl_task = DockerOperator(
        task_id='etl_scrap_load',
        image='etl:latest',  
        container_name='elt_task_container',
        command='python /app/scrap_load_hubeau.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=f"D:/DATA/2025-06-01_MSPR_1/Good-Air/etl", target="/app", type="bind"),
            Mount(source="D:/DATA/2025-06-01_MSPR_1/Good-Air/.env", target="/app/.env", type="bind"),
            ],
        mount_tmp_dir=False
    )