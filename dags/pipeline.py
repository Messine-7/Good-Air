from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

with DAG(
    dag_id='pipeline_elt_ml_docker',
    start_date=datetime(2025, 1, 1),
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['elt', 'ml']
) as dag:
    
    # Extraction météo
    weather_task = DockerOperator(
        task_id='open_weather_extract',
        image='elt:latest',
        container_name='weather_task_container',
        command='python /app/open_weather_extract.py',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./elt", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
    )

    # Extraction AQI
    aqicn_task = DockerOperator(
        task_id='aqicn_extract',
        image='elt:latest',
        container_name='aqicn_task_container',
        command='python /app/aqicn_extract.py',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./elt", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
    )

    # Transformation DBT
    dbt_task = DockerOperator(
        task_id='dbt_transform',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='dbt_task_container',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./dbt/dbt_project", target="/app/dbt_project", type="bind"),
            Mount(source="./dbt", target="/root/.dbt", type="bind"),
        ],
    )

    # Prédictions ML
    ml_predict_task = DockerOperator(
        task_id='ml_predictions',
        image='ml-service:latest',
        container_name='ml_predict_container',
        command='python /app/inference/predict.py --mode batch',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./ml", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
    )

    # Entraînement périodique (daily)
    ml_train_task = DockerOperator(
        task_id='ml_training',
        image='ml-service:latest',
        container_name='ml_train_container',
        command='python /app/training/train_model.py --model all --days_back 60',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./ml", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
        # Exécuter seulement une fois par jour
        execution_timeout=timedelta(hours=2),
    )

    # Dépendances
    [weather_task, aqicn_task] >> dbt_task >> ml_predict_task

# DAG séparé pour l'entraînement (moins fréquent)
with DAG(
    dag_id='ml_training_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 2 * * *',  # Tous les jours à 2h
    catchup=False,
    tags=['ml', 'training']
) as training_dag:
    
    ml_train_task = DockerOperator(
        task_id='ml_training_daily',
        image='ml-service:latest',
        container_name='ml_train_daily_container',
        command='python /app/training/train_model.py --model all --days_back 30',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./ml", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
        execution_timeout=timedelta(hours=2),
    )