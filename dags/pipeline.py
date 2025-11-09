from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
# DAG 1 : ELT OpenWeather (extraction + dbt silver)
# ============================================================

with DAG(
    dag_id='elt_openweather',
    start_date=datetime(2025, 8, 31),
    schedule_interval='*/30 * * * *',  # toutes les 30 min
    catchup=False,
    tags=['elt', 'openweather']
) as dag_openweather:

    # --- Extraction des données OpenWeather ---
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

    # --- Transformation dbt Silver ---
    open_weather_dbt_transform = DockerOperator(
        task_id='open_weather_dbt_transform',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='open_weather_dbt_silver_container',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt --select silver.weather_clean',
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

    # --- Déclenchement du DAG gold une fois terminé ---
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_elt_gold",
        trigger_dag_id="elt_gold",
        wait_for_completion=False
    )

    open_weather_extract >> open_weather_dbt_transform >> trigger_gold


# ============================================================
# DAG 2 : ELT AQICN (extraction + dbt silver)
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
        task_id='aqicn_dbt_transform',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='aqicn_dbt_silver_container',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt --select silver.aqicn_clean',
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

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_elt_gold",
        trigger_dag_id="elt_gold",
        wait_for_completion=False
    )

    aqicn_extract >> aqicn_dbt_transform >> trigger_gold


# ============================================================
# DAG 3 : ETL Hubeau (tous les 1er et 15 du mois)
# ============================================================

with DAG(
    dag_id='etl_hubeau',
    start_date=datetime(2025, 8, 31),
    schedule_interval="0 0 1,15 * *",  # le 1er et 15 à minuit
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


# ============================================================
# DAG 4 : ELT Gold (DBT Gold Layer)
# ============================================================

with DAG(
    dag_id='elt_gold',
    start_date=datetime(2025, 8, 31),
    schedule_interval='@hourly',  # toutes les heures
    catchup=False,
    tags=['elt', 'gold']
) as dag_gold:

    dbt_gold_transform = DockerOperator(
        task_id='dbt_gold_transform',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='dbt_gold_container',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt --select gold',
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