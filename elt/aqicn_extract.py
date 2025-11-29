import socket
import ssl
import requests
import json
import os
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.poolmanager import PoolManager

# ===============================================================
# ✅ Chargement des variables d’environnement
# ===============================================================
load_dotenv('/app/.env')

AQICN_API_KEY = os.getenv('AQICN_API_KEY')
ACOUNT_SNOWFLAKE = os.getenv('ACOUNT_SNOWFLAKE')
USER_SNOWFLAKE = os.getenv('USER_SNOWFLAKE')
PASSWORD_SNOWFLAKE = os.getenv('PASSWORD_SNOWFLAKE')

# ===============================================================
# ✅ Forcer IPv4 uniquement (corrige l’erreur SSL/EOF)
# ===============================================================
def force_ipv4():
    orig_getaddrinfo = socket.getaddrinfo
    def getaddrinfo_ipv4(*args, **kwargs):
        return [info for info in orig_getaddrinfo(*args, **kwargs) if info[0] == socket.AF_INET]
    socket.getaddrinfo = getaddrinfo_ipv4

force_ipv4()

# ===============================================================
# ✅ Forcer TLS 1.2 (corrige handshake AQICN)
# ===============================================================
class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context'] = ctx
        return super(TLS12Adapter, self).init_poolmanager(*args, **kwargs)

# ===============================================================
# ✅ Configuration de la session Requests avec retry automatique
# ===============================================================
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = TLS12Adapter(max_retries=retries)
session.mount("https://", adapter)

# ===============================================================
# ✅ Connexion à Snowflake
# ===============================================================
conn = snowflake.connector.connect(
    user=USER_SNOWFLAKE,
    password=PASSWORD_SNOWFLAKE,
    account=ACOUNT_SNOWFLAKE,
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="BRONZE"
)
cur = conn.cursor()

# ===============================================================
# ✅ Liste des villes françaises
# ===============================================================
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

# ===============================================================
# ✅ Récupération des données AQICN
# ===============================================================
data_cities = []
timestamp = datetime.utcnow().isoformat()
count = 0

for city in CITIES:
    name_city = city[0]
    url = f"https://api.waqi.info/feed/{name_city}/?token={AQICN_API_KEY}"
    print(f"🔍 Récupération des données pour {name_city}...")

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        data_cities.append({
            "city": name_city,
            "timestamp_utc": timestamp,
            "status": data.get("status"),
            "raw_json": data
        })
        print(f"✅ Données reçues pour {name_city}")
        count += 1
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur réseau pour {name_city} : {e}")
    except ValueError:
        print(f"❌ Réponse JSON invalide pour {name_city}")

print(f"nombre de villes envoyées {count}")
# ===============================================================
# ✅ Insertion dans Snowflake
# ===============================================================
json_data = json.dumps(data_cities)

try:
    cur.execute("""
        INSERT INTO aqicn_api(raw_json)
        SELECT PARSE_JSON(%s)
    """, (json_data,))
    conn.commit()
    print("✅ Insertion réussie dans Snowflake")

except Exception as e:
    print("❌ Erreur lors de l'insertion :", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()

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
    params = ("API_BRONZE", "aqicn_api", count, 0, "SUCCESS")

    cur.execute(sql_query, params)

    conn.commit()
    print(f"✅ Insertion réussie : {count} lignes ajoutées.")

except Exception as e:
    print("❌ Erreur lors de l'insertion logs :", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()