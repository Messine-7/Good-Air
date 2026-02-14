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
        # --- ETAPE 1 : RECUPERATION ---
        print("Récupération des données...")
        query = "SELECT RECORD_ID, AQI FROM FACT_AIR_QUALITY_RECORDS"
        df = pd.read_sql(query, conn)

        # --- ETAPE 2 : MACHINE LEARNING ---
        print("Analyse des outliers en cours...")
        model = IsolationForest(contamination=0.01, random_state=42)
        df['anomaly_score'] = model.fit_predict(df[['AQI']])
        df['IS_OUTLIER'] = df['anomaly_score'] == -1

        # On garde les colonnes nécessaires
        df_outliers = df[df['IS_OUTLIER'] == True][['RECORD_ID']].copy()
        df_outliers['DETECTED_AT'] = datetime.now(timezone.utc)

        # --- ETAPE 3 : FILTRAGE & INJECTION ---
        if not df_outliers.empty:
            # 1. Récupérer les IDs déjà existants dans Snowflake
            print("Vérification des doublons dans Snowflake...")
            existing_ids_query = "SELECT RECORD_ID FROM ANOMALY_AQICN_RECORDS"
            existing_ids_df = pd.read_sql(existing_ids_query, conn)
            
            # 2. Exclure les IDs déjà présents (Anti-Join)
            # On ne garde que les RECORD_ID qui ne sont PAS dans existing_ids_df
            df_to_insert = df_outliers[~df_outliers['RECORD_ID'].isin(existing_ids_df['RECORD_ID'])]

            if not df_to_insert.empty:
                print(f"Injection de {len(df_to_insert)} nouvelles anomalies...")
                success, n_chunks, n_rows, _ = write_pandas(
                    conn, 
                    df_to_insert, 
                    table_name='ANOMALY_AQICN_RECORDS',
                )
                print(f"Succès : {n_rows} lignes insérées.")
                return len(df_to_insert)
            else:
                print("Toutes les anomalies détectées sont déjà présentes en base.")
                return 0
        else:
            print("Aucune anomalie détectée.")
            return 0
        
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
