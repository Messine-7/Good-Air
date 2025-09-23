try : 
    import requests
    import pandas as pd
    import json
    import numpy as np
    from datetime import datetime, timezone
    import snowflake.connector
    import os
    from dotenv import load_dotenv
except :
    print("Error import lib")

load_dotenv('/app/.env')

# Configuration des APIs
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

#SNOWFLAKE
ACOUNT_SNOWFLAKE = os.getenv('ACOUNT_SNOWFLAKE')
PASSWORD_SNOWFLAKE = os.getenv('PASSWORD_SNOWFLAKE')

conn = snowflake.connector.connect(
    user="LOUK",
    password=PASSWORD_SNOWFLAKE,
    account=ACOUNT_SNOWFLAKE,
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="RAW"
)
cur = conn.cursor()

# Villes françaises avec coordonnées
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

# Paramètres
units = "metric"
lang = "fr"

#Array pour recevoir les reponse, un element pour une ville
data_cities = []

#boucle sur les ville selectionnées
for city in CITIES :
    name_city = city[0] 
    lat = city[1]
    lon = city[2]

    # 🔗 URL One Call 2.5
    url = (
        f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric&lang=fr"
    )

    response = requests.get(url)

    try:
        response.raise_for_status()  
        data = response.json()
        data_cities.append(data)
    except requests.exceptions.HTTPError as http_err:
        print(f" Erreur HTTP : {response.status_code} - {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f" Erreur de requête : {req_err}")
    except ValueError:
        print(" Réponse reçue mais le JSON est invalide.")


#Envois vers sbnowflake
#convertion de la data en json
json_data = json.dumps(data_cities)

try:
    # Insertion dans la table
    cur.execute("""
        INSERT INTO weather_api(raw_json) 
        SELECT PARSE_JSON(%s)
    """, (json_data,))
    
    conn.commit()
    print("✅ Insertion réussie")

except Exception as e:
    print("❌ Erreur lors de l'insertion :", e)
    conn.rollback()

finally:
    cur.close()