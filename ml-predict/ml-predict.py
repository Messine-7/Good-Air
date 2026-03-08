import pandas as pd
import snowflake.connector
import joblib
import os
import numpy as np
from dotenv import load_dotenv
from datetime import datetime

# ============================================================
# 1. CHARGEMENT DE LA CONFIGURATION ET CONNEXION
# ============================================================
try:
    load_dotenv('/app/.env')
except:
    load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("USER_SNOWFLAKE"),
    password=os.getenv("PASSWORD_SNOWFLAKE"),
    account=os.getenv("ACOUNT_SNOWFLAKE"),
    warehouse=os.getenv("COMPUTE_WH", "COMPUTE_WH"),
    database=os.getenv("GOOD_AIR", "GOOD_AIR"),
    schema="GOLD",
)
print("✅ Connexion Snowflake OK")

# ============================================================
# 2. DÉFINITION DES FEATURES ET CONFIGURATION DES MODÈLES
# ============================================================
# Listes des features identiques au script d'entraînement
FEATURES_M1 = [
    'aqi', 'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',
    'month', 'day_of_week', 'day_of_year', 'year',
    'latitude', 'longitude'
]

FEATURES_M2 = [
    'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24', 'pm10_lag_48',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24', 'pm25_lag_48',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',   'o3_lag_48',
    'month', 'day_of_week', 'hour',
    'latitude', 'longitude'
]

FEATURES_M3 = [
    'temperature', 'temp_lag_12', 'temp_lag_24', 'temp_lag_48',
    'humidity', 'hum_lag_12', 'hum_lag_24',
    'wind_speed', 'wind_lag_12', 'wind_lag_24',
    'visibility', 'visibility_lag_12', 'visibility_lag_24',
    'hour', 'month',
    'latitude', 'longitude'
]

# Mapping : Fichier modèle -> Features associées -> Colonne Snowflake
MODEL_DIR = "/app/models"
MODEL_CONFIG = {
    "modele1_aqi_aqi_j1.joblib":       {"features": FEATURES_M1, "col": "PREDICT_AQI_J1"},
    "modele1_aqi_aqi_j2.joblib":       {"features": FEATURES_M1, "col": "PREDICT_AQI_J2"},
    "modele2_aqi_temps_reel.joblib":   {"features": FEATURES_M2, "col": "PREDICT_AQI_TEMPS_REEL"},
    "modele3_temp_temp_12h.joblib":    {"features": FEATURES_M3, "col": "PREDICT_TEMP_12H"},
    "modele3_temp_temp_24h.joblib":    {"features": FEATURES_M3, "col": "PREDICT_TEMP_24H"},
}

# ============================================================
# 3. RÉCUPÉRATION DES DONNÉES DEPUIS SNOWFLAKE
# ============================================================
# On part du principe qu'on cible une table "ALL_PREDICTIONS"
query = """
SELECT 
    f.*, 
    d.latitude, 
    d.longitude
FROM GOOD_AIR.GOLD.FUSION_AQICN_WEATHER AS f
LEFT JOIN GOOD_AIR.SILVER.DIM_CITY AS d 
    ON f.CITY_ID = d.CITY_ID
LEFT JOIN GOOD_AIR.GOLD.ALL_PREDICTIONS AS p 
    ON f.RECORD_ID = p.RECORD_ID
WHERE p.RECORD_ID IS NULL
  AND f.DT_HOUR > DATEADD(month, -6, CURRENT_TIMESTAMP())
"""

print("⏳ Récupération des données depuis Snowflake...")
df = pd.read_sql(query, conn)

if df.empty:
    print("⚠️ Aucune donnée récente à prédire.")
    conn.close()
    exit()

print(f"✅ {len(df)} lignes récupérées.")

# ============================================================
# 4. TRANSFORMATION GLOBALE DES DONNÉES
# ============================================================
df.columns = [c.lower() for c in df.columns]

# Création de TOUTES les features temporelles nécessaires aux 3 modèles
df['dt_hour']     = pd.to_datetime(df['dt_hour'], utc=True)
df['month']       = df['dt_hour'].dt.month
df['day_of_week'] = df['dt_hour'].dt.dayofweek
df['hour']        = df['dt_hour'].dt.hour
df['year']        = df['dt_hour'].dt.year
df['day_of_year'] = df['dt_hour'].dt.dayofyear

# Nettoyage (Interpolation + Fillna médian)
df = df.replace(-999, np.nan)
df = df.groupby('city_id', group_keys=False).apply(lambda g: g.interpolate(method='linear'))
df = df.reset_index(drop=True)

# Application d'une médiane globale sur les colonnes numériques pour sécuriser les NaN restants
num_cols = df.select_dtypes(include='number').columns
df[num_cols] = df[num_cols].fillna(df[num_cols].median())

# ============================================================
# 5. BOUCLE D'INFÉRENCE SUR TOUS LES MODÈLES
# ============================================================
print("\n🚀 Début des prédictions en lot...")

for model_file, config in MODEL_CONFIG.items():
    model_path = os.path.join(MODEL_DIR, model_file)
    target_col = config["col"]
    features   = config["features"]
    
    if not os.path.exists(model_path):
        print(f"⚠️ Modèle ignoré (introuvable) : {model_file}")
        df[target_col] = np.nan # Remplir de NaN si le modèle est absent
        continue
        
    print(f"⏳ Inférence avec {model_file} -> {target_col}...")
    model = joblib.load(model_path)
    
    # On s'assure que les features requises sont bien présentes dans le DF
    missing_cols = [c for c in features if c not in df.columns]
    if missing_cols:
        print(f"❌ Erreur : Colonnes manquantes pour {model_file} : {missing_cols}")
        df[target_col] = np.nan
        continue
        
    # Prédiction
    df[target_col] = model.predict(df[features])

df['PREDICTION_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ============================================================
# 6. SAUVEGARDE DES PRÉDICTIONS DANS SNOWFLAKE
# ============================================================    
print("\n⏳ Sauvegarde globale dans GOOD_AIR.GOLD.ALL_PREDICTIONS...")

data_to_insert = [
    (
        str(row['record_id']), 
        str(row['city_id']), 
        str(row['dt_hour']), 
        float(row['PREDICT_AQI_J1']) if pd.notna(row.get('PREDICT_AQI_J1')) else None,
        float(row['PREDICT_AQI_J2']) if pd.notna(row.get('PREDICT_AQI_J2')) else None,
        float(row['PREDICT_AQI_TEMPS_REEL']) if pd.notna(row.get('PREDICT_AQI_TEMPS_REEL')) else None,
        float(row['PREDICT_TEMP_12H']) if pd.notna(row.get('PREDICT_TEMP_12H')) else None,
        float(row['PREDICT_TEMP_24H']) if pd.notna(row.get('PREDICT_TEMP_24H')) else None,
        str(row['PREDICTION_DATE'])
    )
    for _, row in df.iterrows()
]

cursor = conn.cursor()
try:
    insert_query = """
    INSERT INTO GOOD_AIR.GOLD.AQI_PREDICTIONS_2
    (RECORD_ID, CITY_ID, DT_HOUR, PREDICT_AQI_J1, PREDICT_AQI_J2, PREDICT_AQI_TEMPS_REEL, PREDICT_TEMP_12H, PREDICT_TEMP_24H, PREDICTION_DATE)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"✅ {len(data_to_insert)} lignes insérées avec succès.")
    
except Exception as e:
    print(f"❌ Erreur lors de l'insertion : {e}")
    conn.rollback()
finally:
    cursor.close()

conn.close()
print("🎉 Processus d'inférence complet terminé.")