from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

def run_ml_predictions():
    """Lance les prédictions ML"""
    import subprocess
    import sys
    
    # Installer les dépendances si nécessaire
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn", "snowflake-connector-python"])
    except:
        pass
    
    # Lancer le script ML
    result = subprocess.run([sys.executable, "/sources/ml_simple.py"], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print("Prédictions ML réussies:")
        print(result.stdout)
    else:
        print("Erreur ML:")
        print(result.stderr)
        raise Exception("Échec des prédictions ML")

# Configuration du DAG
default_args = {
    'owner': 'good-air',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'good_air_simple_ml',
    default_args=default_args,
    description='Pipeline Good-Air simple avec ML',
    schedule_interval=timedelta(hours=1),  # Toutes les heures
    catchup=False,
    tags=['good-air', 'ml', 'simple']
) as dag:

    # Extraction météo
    extract_weather = DockerOperator(
        task_id='extract_weather',
        image='elt:latest',
        container_name='weather_extract_{{ ts_nodash }}',
        command='python /app/open_weather_extract.py',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./elt", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
    )

    # Extraction AQI
    extract_aqi = DockerOperator(
        task_id='extract_aqi',
        image='elt:latest',
        container_name='aqi_extract_{{ ts_nodash }}',
        command='python /app/aqicn_extract.py',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./elt", target="/app", type="bind"),
            Mount(source="./.env", target="/app/.env", type="bind"),
        ],
    )

    # Transformation DBT
    transform_dbt = DockerOperator(
        task_id='transform_dbt',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='dbt_transform_{{ ts_nodash }}',
        command='run --project-dir /app/dbt_project --profiles-dir /root/.dbt',
        auto_remove=True,
        network_mode='data-pipeline',
        mounts=[
            Mount(source="./dbt/dbt_project", target="/app/dbt_project", type="bind"),
            Mount(source="./dbt", target="/root/.dbt", type="bind"),
        ],
    )

    # ML simple
    ml_predictions = PythonOperator(
        task_id='ml_predictions',
        python_callable=run_ml_predictions,
    )

    # Pipeline
    [extract_weather, extract_aqi] >> transform_dbt >> ml_predictions