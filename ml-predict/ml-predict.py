import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import joblib
import os
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# ============================================================
# 1. CONNEXION SNOWFLAKE
# ============================================================
load_dotenv('/app/.env')
conn = snowflake.connector.connect(
    user=os.getenv("USER_SNOWFLAKE"),
    password=os.getenv("PASSWORD_SNOWFLAKE"),
    account=os.getenv("ACOUNT_SNOWFLAKE"),
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="GOLD",
)

# ============================================================
# 2. RÉCUPÉRATION DES DONNÉES (Table GOLD)
# ============================================================
# Note : On récupère toutes les colonnes nécessaires aux différents modèles (Lags inclus)
query = """SELECT f.*, d.latitude, d.longitude 
    FROM GOLD.FUSION_AQICN_WEATHER f 
    LEFT JOIN SILVER.DIM_CITY d ON f.city_id = d.city_id
    WHERE DT_HOUR > DATEADD(month, -6, CURRENT_TIMESTAMP())"""

df = pd.read_sql(query, conn)


df['MONTH']       = df['DT_HOUR'].dt.month
df['DAY_OF_WEEK'] = df['DT_HOUR'].dt.dayofweek
df['DAY_OF_YEAR'] = df['DT_HOUR'].dt.dayofyear
df['YEAR']        = df['DT_HOUR'].dt.year

df = df.replace(-999, np.nan)

df_source = df.copy()

df_source['DT_HOUR'] = pd.to_datetime(df_source['DT_HOUR']).dt.strftime('%Y-%m-%d %H:%M:%S')

if df_source.empty:
    print("⚠️ Aucune donnée source trouvée.")
    conn.close()
    exit()

# ============================================================
# 3. BOUCLE DE PRÉDICTION SUR TOUS LES MODÈLES
# ============================================================
MODEL_DIR = Path("/app/models")
prediction_results = []

# On parcourt les fichiers joblib : aqi_12.joblib, temp_24.joblib, etc.
for model_file in MODEL_DIR.glob("*.joblib"):
    print(f"🔄 Traitement du modèle : {model_file.name}")

    
    package = joblib.load(model_file)
    
    # Sécurité : On vérifie si c'est bien notre nouveau format (dictionnaire)
    if isinstance(package, dict) and 'model' in package:
        model = package['model']
        scaler = package['scaler']
        imputer = package['imputer']
        features_required = package['features']
    else:
        print(f"⚠️ Le fichier {model_file.name} est dans l'ancien format. Sautez-le ou ré-entraînez.")
        continue

    # Chargement du package complet
    package = joblib.load(model_file)
    model = package['model']
    scaler = package['scaler']
    imputer = package['imputer']
    features_required = package['features'] # Les colonnes attendues par ce modèle
    
    # Préparation du nom de la colonne de sortie (ex: PRED_AQI_12H)
    # On extrait le nom du fichier (ex: aqi_12)
    name_parts = model_file.stem.split('_')
    col_name = f"PRED_{name_parts[0].upper()}_{name_parts[1]}H"

    try:
        # 1. Identifier les colonnes que l'on doit VRAIMENT extraire de Snowflake
        # On ignore les colonnes 'missingindicator' car c'est l'imputer qui va les créer
        raw_features_needed = [c for c in features_required if not c.startswith('missingindicator')]
        
        # Vérification des colonnes brutes uniquement
        missing_in_df = [c for c in raw_features_needed if c not in df_source.columns]
        if missing_in_df:
            print(f"❌ Colonnes brutes manquantes dans Snowflake : {missing_in_df}")
            continue

        # 2. Préparer X avec uniquement les colonnes brutes
        # L'ordre doit être celui attendu par l'imputer (souvent les noms sans indicateurs)
        X = df_source[raw_features_needed].copy()
        
        # 3. L'IMPUTATION : C'est ici que les colonnes 'missingindicator' réapparaissent !
        # L'imputer va transformer X (raw) en X_prep (avec indicateurs)
        X_prep = imputer.transform(X)
        
        # 4. SCALING
        # On s'assure de ne scaler que ce que le scaler connaît
        cols_to_scale = [c for c in X_prep.columns if c in scaler.feature_names_in_]
        X_prep[cols_to_scale] = scaler.transform(X_prep[cols_to_scale])

        # 5. ALIGNEMENT FINAL
        # On s'assure que X_prep a exactement les colonnes dans l'ordre de 'features_required'
        X_prep = X_prep[features_required]

        # 6. PRÉDICTION
        df_source[col_name] = model.predict(X_prep)
        print(f"✅ Prédictions réussies pour {col_name}")

    except Exception as e:
        print(f"❌ Erreur lors du calcul pour {model_file.name} : {e}")

# ============================================================
# 4. PRÉPARATION DU DATAFRAME FINAL POUR EXPORT
# ============================================================
# On ne garde que les colonnes de prédiction et les IDs
pred_cols = [c for c in df_source.columns if c.startswith('PRED_')]
df_final = df_source[['RECORD_ID', 'CITY_ID', 'DT_HOUR'] + pred_cols].copy()
df_final['PRED_DATE'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# On s'assure que les noms match la table Snowflake (Tout en majuscule)
df_final.columns = [c.upper() for c in df_final.columns]

# ============================================================
# 5. SAUVEGARDE VIA WRITE_PANDAS (Plus rapide)
# ============================================================
print(f"⏳ Sauvegarde de {len(df_final)} lignes dans Snowflake...")

success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df_final,
    table_name='AQI_PREDICTIONS', # Assure-toi que cette table a les colonnes PRED_AQI_12H, etc.
    schema='GOLD',
    database='GOOD_AIR',
    overwrite=False # On ajoute les nouvelles prédictions
)

if success:
    print(f"🎉 {nrows} lignes de prédictions insérées avec succès.")

conn.close()