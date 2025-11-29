# %%
import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime, timezone
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sklearn.ensemble import IsolationForest
import os
from dotenv import load_dotenv
import time
import locale

# %%
# Connexion à Snowflake
load_dotenv('/app/.env')

ACOUNT_SNOWFLAKE = os.getenv('ACOUNT_SNOWFLAKE')
USER_SNOWFLAKE = os.getenv('USER_SNOWFLAKE')
PASSWORD_SNOWFLAKE = os.getenv('PASSWORD_SNOWFLAKE')
conn = snowflake.connector.connect(
    user=USER_SNOWFLAKE,
    password=PASSWORD_SNOWFLAKE,
    account=ACOUNT_SNOWFLAKE,  
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="SILVER"
)

def run_anomaly_detection():
    try:
        # --- ETAPE 1 : RECUPERATION (READ) ---
        print("Récupération des données...")
        # On ne prend que les colonnes utiles pour le ML (ex: valeurs numériques)
        query = "SELECT RECORD_ID, AQI FROM FACT_AIR_QUALITY_RECORDS"
        df = pd.read_sql(query, conn)

        # --- ETAPE 2 : MACHINE LEARNING (PROCESS) ---
        print("Analyse des outliers en cours...")
        
        # Initialisation du modèle 
        # contamination=0.01 signifie qu'on s'attend à environ 1% d'anomalies
        model = IsolationForest(contamination=0.01, random_state=42)
        
        # Entraînement et prédiction sur la valeur à surveiller
        # Reshape est nécessaire si on a une seule feature
        df['anomaly_score'] = model.fit_predict(df[['AQI']])
        
        # Le modèle retourne -1 pour une anomalie et 1 pour normal.
        # On convertit cela en booléen ou texte pour Snowflake
        df['IS_OUTLIER'] = df['anomaly_score'].apply(lambda x: True if x == -1 else False)

        # On filtre pour ne garder que les outliers à renvoyer (ou tout le monde, selon votre choix)
        df_outliers = df[df['IS_OUTLIER'] == True][['RECORD_ID']]

        # Ajoute la date et l'heure actuelles à chaque ligne
        df_outliers['DETECTED_AT'] = pd.to_datetime('now')
        
        # --- ETAPE 3 : INJECTION (WRITE) ---
        if not df_outliers.empty:
            print(f"Injection de {len(df_outliers)} anomalies détectées dans Snowflake...")
            len_df = len(df_outliers)
            # Utilisation de write_pandas pour la performance (bien plus rapide que INSERT)
            # On écrit dans une table dédiée aux alertes
            success, n_chunks, n_rows, _ = write_pandas(
                conn, 
                df_outliers, 
                table_name='ANOMALY_AQICN_RECORDS',
            )
            print(f"Succès : {n_rows} lignes insérées.")
            return len_df
        else:
            print("Aucune anomalie détectée.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    len_df = run_anomaly_detection()

# ===============================================================
# ✅ Envois log Snowflake
# ===============================================================
if len_df > 0:
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
        params = ("API_SILVER", "ANOMALY_AQICN_RECORDS", len_df, 0, "SUCCESS")

        cur.execute(sql_query, params)
        conn.commit()

    except Exception as e:
        print("❌ Erreur lors de l'insertion logs :", e)
        conn.rollback()
                    
    finally:
        cur.close()
        conn.close()
