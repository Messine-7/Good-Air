#!/usr/bin/env python3
"""
ML simple pour Good-Air - Version corrigée pour les vraies tables
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import snowflake.connector
import os
from datetime import datetime
import pickle

def connect_snowflake():
    """Connexion simple à Snowflake"""
    return snowflake.connector.connect(
        user="LOUK",
        password="Snowflake_08230!",
        account="ZQQYYBI-EM82872",
        warehouse="COMPUTE_WH",
        database="GOOD_AIR",
        schema="TRANSFORMED"
    )

def get_training_data():
    """Récupère les données d'entraînement depuis Snowflake"""
    print("📊 Récupération des données depuis Snowflake...")
    
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    # Requête corrigée avec les vrais noms de colonnes
    query = """
    WITH weather_data AS (
        SELECT city_name, temperature, humidity, pressure, wind_speed, dt_utc_plus2
        FROM WEATHER_CLEAN 
        WHERE dt_utc_plus2 >= DATEADD(day, -30, CURRENT_DATE())
        AND temperature IS NOT NULL
    ),
    aqi_data AS (
        SELECT city_name, aqi, dt_paris
        FROM AQICN_CLEAN 
        WHERE dt_paris >= DATEADD(day, -30, CURRENT_DATE())
        AND aqi IS NOT NULL
        AND aqi > 0
    )
    SELECT w.city_name, w.temperature, w.humidity, w.pressure, w.wind_speed, a.aqi
    FROM weather_data w
    JOIN aqi_data a ON w.city_name = a.city_name 
    AND DATE(w.dt_utc_plus2) = DATE(a.dt_paris)
    LIMIT 1000
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    df = pd.DataFrame(results, columns=columns)
    conn.close()
    
    print(f"✅ {len(df)} lignes récupérées")
    if len(df) > 0:
        print("Aperçu des données:")
        print(df.head())
    
    return df

def train_simple_model():
    """Entraîne un modèle simple"""
    print("🤖 Entraînement du modèle...")
    
    # Récupérer les données
    df = get_training_data()
    
    if len(df) < 10:
        print("❌ Pas assez de données pour l'entraînement")
        print("Vérifiez que vos tables WEATHER_CLEAN et AQICN_CLEAN contiennent des données récentes")
        return None
    
    # Préparer les features (noms en majuscules comme retournés par Snowflake)
    features = ['TEMPERATURE', 'HUMIDITY', 'PRESSURE', 'WIND_SPEED']
    X = df[features].fillna(0)
    y = df['AQI']
    
    print(f"Features utilisées: {features}")
    print(f"Plage des valeurs AQI: {y.min():.1f} - {y.max():.1f}")
    
    # Entraîner un modèle simple
    model = RandomForestRegressor(n_estimators=50, random_state=42)
    model.fit(X, y)
    
    # Prédictions de test
    predictions = model.predict(X)
    mae = mean_absolute_error(y, predictions)
    
    print(f"✅ Modèle entraîné - MAE: {mae:.2f}")
    
    # Sauvegarder
    with open('model_simple.pkl', 'wb') as f:
        pickle.dump({'model': model, 'features': features}, f)
    
    return model

def predict_aqi():
    """Fait des prédictions basées sur les dernières données météo"""
    print("🔮 Génération des prédictions...")
    
    # Charger le modèle
    try:
        with open('model_simple.pkl', 'rb') as f:
            model_data = pickle.load(f)
            model = model_data['model']
            features = model_data['features']
    except:
        print("❌ Modèle non trouvé, entraînement en cours...")
        model = train_simple_model()
        if model is None:
            return
        features = ['TEMPERATURE', 'HUMIDITY', 'PRESSURE', 'WIND_SPEED']
    
    # Récupérer les dernières données météo
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    query = """
    SELECT city_name, temperature, humidity, pressure, wind_speed
    FROM WEATHER_CLEAN 
    WHERE dt_utc_plus2 >= DATEADD(hour, -6, CURRENT_TIMESTAMP())
    ORDER BY dt_utc_plus2 DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    weather_df = pd.DataFrame(results, columns=columns)
    
    conn.close()
    
    if len(weather_df) == 0:
        print("❌ Pas de données météo récentes")
        print("Vérifiez que votre table WEATHER_CLEAN contient des données des dernières 6 heures")
        return
    
    print(f"Données météo trouvées pour {len(weather_df)} mesures")
    
    # Regrouper par ville (prendre la mesure la plus récente par ville)
    latest_weather = weather_df.groupby('CITY_NAME').first().reset_index()
    
    # Prédictions
    X = latest_weather[features].fillna(0)
    predictions = model.predict(X)
    
    # Créer le DataFrame des prédictions
    pred_df = pd.DataFrame({
        'city_name': latest_weather['CITY_NAME'],
        'predicted_aqi': predictions,
        'prediction_time': datetime.now()
    })
    
    print(f"✅ Prédictions générées pour {len(pred_df)} villes")
    print("\nRésultats des prédictions:")
    print("-" * 40)
    
    for _, row in pred_df.iterrows():
        city = row['city_name']
        aqi_pred = row['predicted_aqi']
        
        # Classification simple du niveau de pollution
        if aqi_pred <= 50:
            level = "Bon 🟢"
        elif aqi_pred <= 100:
            level = "Modéré 🟡"
        elif aqi_pred <= 150:
            level = "Mauvais pour groupes sensibles 🟠"
        else:
            level = "Mauvais 🔴"
        
        print(f"{city:15} | AQI: {aqi_pred:6.1f} | {level}")
    
    return pred_df

def check_data_availability():
    """Vérifie quelles données sont disponibles dans Snowflake"""
    print("🔍 Vérification des données disponibles...")
    
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    # Vérifier WEATHER_CLEAN
    cursor.execute("SELECT COUNT(*), MAX(dt_utc_plus2) FROM WEATHER_CLEAN")
    weather_count, latest_weather = cursor.fetchone()
    print(f"WEATHER_CLEAN: {weather_count} lignes, dernière mesure: {latest_weather}")
    
    # Vérifier AQICN_CLEAN
    cursor.execute("SELECT COUNT(*), MAX(dt_paris) FROM AQICN_CLEAN WHERE aqi IS NOT NULL")
    aqi_count, latest_aqi = cursor.fetchone()
    print(f"AQICN_CLEAN: {aqi_count} lignes avec AQI, dernière mesure: {latest_aqi}")
    
    # Vérifier les villes communes
    cursor.execute("""
    SELECT DISTINCT w.city_name 
    FROM WEATHER_CLEAN w 
    JOIN AQICN_CLEAN a ON w.city_name = a.city_name 
    WHERE w.dt_utc_plus2 >= DATEADD(day, -7, CURRENT_DATE())
    AND a.dt_paris >= DATEADD(day, -7, CURRENT_DATE())
    """)
    common_cities = [row[0] for row in cursor.fetchall()]
    print(f"Villes avec données météo ET AQI (7 derniers jours): {common_cities}")
    
    conn.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "train":
            train_simple_model()
        elif sys.argv[1] == "check":
            check_data_availability()
        else:
            predict_aqi()
    else:
        predict_aqi()