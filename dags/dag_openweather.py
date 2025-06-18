from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id='openweather_air_pollution',
    description='Extraction quotidienne de données de pollution via OpenWeather API',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['api', 'openweather'],
) as dag:

    extract_pollution = DockerOperator(
        task_id='extract_air_pollution',
        image='elt',  # identique à ton docker-compose
        container_name='extract_openweather_container',
        command='python /app/API_openweather.py',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline'
    )

    extract_pollution
