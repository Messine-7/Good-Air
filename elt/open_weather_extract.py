import time
import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime, timezone
import snowflake.connector
import os
from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Charger les variables d'environnement
load_dotenv('/app/.env')

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
ACOUNT_SNOWFLAKE = os.getenv('ACOUNT_SNOWFLAKE')
USER_SNOWFLAKE = os.getenv('USER_SNOWFLAKE')
PASSWORD_SNOWFLAKE = os.getenv('PASSWORD_SNOWFLAKE')

# --- Prometheus / Pushgateway ---
registry = CollectorRegistry()
job_status = Gauge('elt_weather_status', 'Status du job ETL météo', registry=registry)
job_duration = Gauge('elt_weather_duration_seconds', 'Durée du job ETL météo', registry=registry)
api_request_count = Gauge('elt_weather_api_requests', 'Nombre total d’appels API', registry=registry)

start_time = time.time()

try:
    # --- Connexion Snowflake ---
    conn = snowflake.connector.connect(
        user=USER_SNOWFLAKE,
        password=PASSWORD_SNOWFLAKE,
        account=ACOUNT_SNOWFLAKE,
        warehouse="COMPUTE_WH",
        database="GOOD_AIR",
        schema="BRONZE"
    )
    cur = conn.cursor()

    # --- Villes ---
    CITIES = [
        ("Paris", 48.8566, 2.3522),
        ("Marseille", 43.2965, 5.3698),
        ("Lyon", 45.7640, 4.8357),
        ("Toulouse", 43.6045, 1.4440),
        ("Nice", 43.7102, 7.2620),
        ("Nantes", 47.2184, -1.5536),
        ("Montpellier", 43.6109, 3.8772),
        ("Strasbourg", 48.5734, 7.7521),
        ("Bordeaux", 44.8378, -0.5792),
        ("Lille", 50.6292, 3.0573),
        ("Rennes", 48.1173, -1.6778),
        ("Reims", 49.2583, 4.0317),
        ("Saint-Étienne", 45.4397, 4.3872),
        ("Le Havre", 49.4944, 0.1079),
        ("Toulon", 43.1242, 5.9280),
        ("Grenoble", 45.1885, 5.7245),
        ("Dijon", 47.3220, 5.0415),
        ("Angers", 47.4784, -0.5632),
        ("Nîmes", 43.8367, 4.3601),
        ("Villeurbanne", 45.7719, 4.8902)
    ]

    api_success = 0
    data_cities = []   # <-- Un seul tableau comme avant
    count = 0

    # --- Boucle sur les villes ---
    for city in CITIES:
        name_city, lat, lon = city
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=fr"

        try:
            response = requests.get(url)
            response.raise_for_status()
            api_success += 1

            raw_json = response.json()

            # --- Ajouter le vrai nom de la ville dans chaque objet JSON ---
            raw_json["base_city_name"] = name_city.upper()

            # --- Ajouter dans un seul tableau globale (comme avant) ---
            data_cities.append(raw_json)


            count += 1

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API {name_city}: {e}")

    # --- Insertion unique dans Snowflake ---
    cur.execute("""
        INSERT INTO weather_api(raw_json)
        SELECT PARSE_JSON(%s)
    """, (json.dumps(data_cities),))

    conn.commit()
    print("✅ Insertion réussie")

    api_request_count.set(api_success)
    job_status.set(1)

except Exception as e:
    print("❌ Erreur générale :", e)
    job_status.set(0)

finally:
    try:
        cur.close()
        conn.close()
    except:
        pass
    job_duration.set(time.time() - start_time)

    try:
        push_to_gateway("pushgateway:9091", job="elt_weather_job", registry=registry)
    except Exception as e:
        print("❌ Impossible de pousser les métriques:", e)

# ===============================================================
# ✅ Envois log Snowflake
# ===============================================================
conn = snowflake.connector.connect(
    user=USER_SNOWFLAKE,
    password=PASSWORD_SNOWFLAKE,
    account=ACOUNT_SNOWFLAKE,
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="LOGS"
)
cur = conn.cursor()

try:
    # 2. Requête SQL corrigée (Ajout de VALUES et des placeholders %s)
    sql_query = """
        INSERT INTO PIPELINE_METRICS 
        (pipeline_stage, dataset_name, rows_affected, total_rows_in_table, status) 
        VALUES (%s, %s, %s, %s, %s)
    """
    
    # 3. Paramètres regroupés dans un TUPLE (parenthèses obligatoires)
    # Note: J'ai mis 'API_BRONZE' au lieu de 'dbt_BRONZE' car c'est du Python, pas dbt.
    # Pour total_rows, si c'est la première insertion, c'est égal à 'count'.
    params = ("API_BRONZE", "weather_api", count, 0, "SUCCESS")

    cur.execute(sql_query, params)

    conn.commit()
    print(f"✅ Insertion réussie : {count} lignes ajoutées.")

except Exception as e:
    print("❌ Erreur lors de l'insertion logs :", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()