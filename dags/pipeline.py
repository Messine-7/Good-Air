from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os
from dotenv import load_dotenv

# ============================================================
# Chargement des variables d'environnement
# ============================================================
load_dotenv('/opt/airflow/.env')
FOLDER_PATH = os.getenv('FOLDER_PATH')

# ============================================================
# DAG 1 : ELT OpenWeather (Extraction + DBT Silver)
# ============================================================

with DAG(
    dag_id='elt_openweather',
    start_date=datetime(2025, 8, 31),
    schedule_interval='*/30 * * * *',  # toutes les 30 min
    catchup=False,
    tags=['elt', 'openweather']
) as dag_openweather:

    # --- Extraction des données météo ---
    open_weather_extract = DockerOperator(
        task_id='open_weather_extract',
        image='elt:latest',
        container_name='open_weather_extract_container',
        command='python /app/open_weather_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=os.path.join(FOLDER_PATH, "elt"), target="/app", type="bind"),
            Mount(source=os.path.join(FOLDER_PATH, ".env"), target="/app/.env", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    # --- DBT Silver : Weather Records + Dimensions ---
    open_weather_dbt_transform = DockerOperator(
        task_id='open_weather_dbt_silver',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='open_weather_dbt_silver_container',
        command=(
            "run --project-dir /app/dbt_project --profiles-dir /root/.dbt "
            "--select silver.fact_weather_records silver.dim_city silver.dim_weather"
        ),
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=os.path.join(FOLDER_PATH, "dbt/dbt_project"), target="/app/dbt_project", type="bind"),
            Mount(source=os.path.join(FOLDER_PATH, "dbt"), target="/root/.dbt", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    open_weather_extract >> open_weather_dbt_transform


# ============================================================
# DAG 2 : ELT AQICN (Extraction + DBT Silver)
# ============================================================

with DAG(
    dag_id='elt_aqicn',
    start_date=datetime(2025, 8, 31),
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['elt', 'aqicn']
) as dag_aqicn:

    aqicn_extract = DockerOperator(
        task_id='aqicn_extract',
        image='elt:latest',
        container_name='aqicn_extract_container',
        command='python /app/aqicn_extract.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=os.path.join(FOLDER_PATH, "elt"), target="/app", type="bind"),
            Mount(source=os.path.join(FOLDER_PATH, ".env"), target="/app/.env", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    aqicn_dbt_transform = DockerOperator(
        task_id='aqicn_dbt_silver',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='aqicn_dbt_silver_container',
        command=(
            "run --project-dir /app/dbt_project --profiles-dir /root/.dbt "
            "--select silver.fact_water_quality_records silver.dim_city"
        ),
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=os.path.join(FOLDER_PATH, "dbt/dbt_project"), target="/app/dbt_project", type="bind"),
            Mount(source=os.path.join(FOLDER_PATH, "dbt"), target="/root/.dbt", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    aqicn_extract >> aqicn_dbt_transform


# ============================================================
# DAG 3 : ETL Hubeau (inchangé)
# ============================================================

with DAG(
    dag_id='etl_hubeau',
    start_date=datetime(2025, 8, 31),
    schedule_interval="0 0 1,15 * *",
    catchup=False,
    tags=['etl', 'hubeau']
) as dag_hubeau:

    etl_scrap_load = DockerOperator(
        task_id='etl_scrap_load',
        image='etl:latest',
        container_name='etl_hubeau_container',
        command='python /app/scrap_load_hubeau.py',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            Mount(source=os.path.join(FOLDER_PATH, "etl"), target="/app", type="bind"),
            Mount(source=os.path.join(FOLDER_PATH, ".env"), target="/app/.env", type="bind"),
        ],
        mount_tmp_dir=False,
    )
