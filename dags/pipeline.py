from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
import smtplib
from docker.types import Mount
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# ============================================================
# Chargement des variables d'environnement
# ============================================================
load_dotenv('/opt/airflow/.env')
FOLDER_PATH = os.getenv('FOLDER_PATH')

print(FOLDER_PATH)

# ============================================================
# Configuration des alertes
# ============================================================

default_args = {
    'owner': 'data-nova',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    
    # 1. Active l'envoi en cas d'échec
    'email_on_failure': True,
    
    # 2. (Optionnel) Active l'envoi si Airflow tente de relancer la tâche
    'email_on_retry':False,
    
    # 3. La liste des destinataires (votre adresse perso ou celle de l'équipe)
    'email': ['lucasmessina83@hotmail.com', 'docker-comopse ', 'ndeyeyande@gmail.com' ]

}
# ============================================================
# DAG 1 : ELT OpenWeather (Extraction + DBT Silver)
# ============================================================

with DAG(
    dag_id='elt_openweather',
    default_args=default_args,
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
    open_weather_dbt_silver = DockerOperator(
        task_id='open_weather_dbt_silver',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='open_weather_dbt_silver_container',
        command=(
            "run --project-dir /app/dbt_project --profiles-dir /root/.dbt "
            "--select silver.dim_city silver.dim_weather silver.fact_weather_records "
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

    open_weather_extract >> open_weather_dbt_silver


# ============================================================
# DAG 2 : ELT AQICN (Extraction + DBT Silver)
# ============================================================

with DAG(
    dag_id='elt_aqicn',
    start_date=datetime(2025, 8, 31),
    default_args=default_args,
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

    aqicn_dbt_silver = DockerOperator(
        task_id='aqicn_dbt_silver',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='aqicn_dbt_silver_container',
        command=(
            "run --project-dir /app/dbt_project --profiles-dir /root/.dbt "
            "--select silver.dim_city silver.fact_air_quality_records"
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

    # 3. Machine Learning (Python)
    # Utilise la même image 'elt:latest' que l'extract, car elle contient pandas/connector
    # Assurez-vous que scikit-learn est bien installé dans cette image !
    aqicn_ml_detect = DockerOperator(
        task_id='aqicn_ml_detect',
        image='elt:latest',
        container_name='aqicn_ml_container',
        command='python /app/aqicn_ml_detect.py', # Nom du fichier ML créé précédemment
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='data-pipeline',
        mounts=[
            # On monte le même dossier ML
            Mount(source=os.path.join(FOLDER_PATH, "ml"), target="/app", type="bind"),
            # Le ML a aussi besoin du .env pour les credentials Snowflake
            Mount(source=os.path.join(FOLDER_PATH, ".env"), target="/app/.env", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    #Gold dbt
    aqicn_dbt_gold = DockerOperator(
        task_id='aqicn_dbt_gold',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        container_name='aqicn_dbt_gold_container',
        command=(
            "run --project-dir /app/dbt_project --profiles-dir /root/.dbt "
            "--select gold.fusion_aqicn_weather"
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

    aqicn_extract >> aqicn_dbt_silver >> aqicn_ml_detect >> aqicn_dbt_gold


# ============================================================
# DAG 3 : ETL Hubeau (inchangé)
# ============================================================

with DAG(
    dag_id='etl_hubeau',
    start_date=datetime(2025, 8, 31),
    default_args=default_args,
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


# ============================================================
# TEST ENVOIS MAILS
# ============================================================

"""
def test_smtp_connection():
    # Remplacer par vos valeurs EXACTES utilisées dans l'interface ou le .env
    host = "smtp.gmail.com"
    port = 587
    user = "ml.bim.3d@gmail.com"
    password = os.getenv('GMAIL_APP_PASSWORD')

    print(f"--- DIAGNOSTIC SMTP VERS {host}:{port} ---")
    
    # 1. Test Réseau simple
    try:
        print("1. Tentative de connexion TCP...")
        server = smtplib.SMTP(host, port, timeout=10)
        print("   ✅ Connexion TCP OK")
    except Exception as e:
        print(f"   ❌ ÉCHEC TCP : {e}")
        print("   CAUSE PROBABLE : Problème réseau Docker ou Pare-feu.")
        raise e

    # 2. Test Chiffrement
    try:
        print("2. Activation TLS...")
        server.starttls()
        print("   ✅ TLS OK")
    except Exception as e:
        print(f"   ❌ ÉCHEC TLS : {e}")
        raise e

    # 3. Test Authentification
    try:
        print(f"3. Login avec l'utilisateur : {user}...")
        server.login(user, password)
        print("   ✅ LOGIN OK")
    except Exception as e:
        print(f"   ❌ ÉCHEC LOGIN : {e}")
        print("   CAUSE PROBABLE : Mauvais mot de passe ou SMTP Auth désactivé.")
        raise e
        
    server.quit()

with DAG('debug_smtp_manuel', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    t1 = PythonOperator(
        task_id='test_connexion',
        python_callable=test_smtp_connection
    )"""