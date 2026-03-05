import pandas as pd
import snowflake.connector
import joblib
import os
from dotenv import load_dotenv
from datetime import datetime

# ============================================================
# 1. CHARGEMENT DE LA CONFIGURATION ET CONNEXION
# ============================================================
load_dotenv('/app/.env')

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="GOLD",
)
print("✅ Connexion Snowflake OK")

# ============================================================
# 2. CHARGEMENT DU MODÈLE
# ============================================================
MODEL_PATH = "/app/models/best_model_aqi.joblib"
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Modèle introuvable à {MODEL_PATH}")

print("⏳ Chargement du modèle...")
model = joblib.load(MODEL_PATH)
print("✅ Modèle chargé")

# ============================================================
# 3. RÉCUPÉRATION DES DONNÉES RÉCENTES DEPUIS SNOWFLAKE (GOLD)
# ============================================================
# On récupère les lignes des dernières 24h ou sans prédiction
query = """
SELECT RECORD_ID, CITY_ID, DT_HOUR, IAQI_O3, IAQI_PM10, IAQI_PM25
FROM GOOD_AIR.GOLD.FUSION_AQICN_WEATHER
WHERE DT_HOUR >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
"""
print("⏳ Récupération des données depuis Snowflake...")
df = pd.read_sql(query, conn)

if df.empty:
    print("⚠️ Aucune donnée récente à prédire.")
else:
    print(f"✅ {len(df)} lignes récupérées.")
    
    # Nettoyage et préparation pour le modèle (qui attend des noms de colonnes en minuscules basés sur ml-train.py)
    df = df.dropna(subset=['IAQI_O3', 'IAQI_PM10', 'IAQI_PM25'])
    features = ['IAQI_O3', 'IAQI_PM10', 'IAQI_PM25']
    X = df[features].rename(columns=str.lower)
    
    # ============================================================
    # 4. PRÉDICTION
    # ============================================================
    print("⏳ Prédiction en cours...")
    df['PREDICTED_AQI'] = model.predict(X)
    df['PREDICTION_DATE'] = datetime.now()
    
    # ============================================================
    # 5. SAUVEGARDE DES PRÉDICTIONS DANS SNOWFLAKE
    # ============================================================
    print("⏳ Création de la table de prédictions si elle n'existe pas...")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS GOOD_AIR.GOLD.AQI_PREDICTIONS (
        RECORD_ID VARCHAR,
        CITY_ID VARCHAR,
        DT_HOUR TIMESTAMP_NTZ,
        PREDICTED_AQI FLOAT,
        PREDICTION_DATE TIMESTAMP_LTZ
    )
    """)
    
    print("⏳ Sauvegarde des prédictions dans GOLD.AQI_PREDICTIONS...")
    
    # Préparation des requêtes d'insertion
    df = df.fillna(0)
    values = []
    
    for _, row in df.iterrows():
        record_id = f"'{row['RECORD_ID']}'"
        city_id = f"'{row['CITY_ID']}'"
        dt_hour = f"'{row['DT_HOUR']}'"
        pred = row['PREDICTED_AQI']
        pred_date = f"'{row['PREDICTION_DATE']}'"
        values.append(f"({record_id}, {city_id}, {dt_hour}, {pred}, {pred_date})")
    
    if values:
        # Insertion des lignes 
        # Pour de grands volumes, une technique avec executemany ou COPY INTO est préférable,
        # mais on insère généralement quelques lignes à la fois pour la prédiction
        chunk_size = 500
        total_inserted = 0
        
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i+chunk_size]
            insert_query = f"""
            INSERT INTO GOOD_AIR.GOLD.AQI_PREDICTIONS 
            (RECORD_ID, CITY_ID, DT_HOUR, IAQI_O3, IAQI_PM10, IAQI_PM25, PREDICTED_AQI, PREDICTION_DATE)
            VALUES {','.join(chunk)}
            """
            cursor.execute(insert_query)
            total_inserted += len(chunk)
            
        print(f"✅ {total_inserted} lignes insérées dans GOLD.AQI_PREDICTIONS.")

conn.close()
print("🎉 Processus de prédiction terminé avec succès.")
