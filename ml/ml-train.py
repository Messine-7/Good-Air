# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.base import clone
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from snowflake.connector.pandas_tools import write_pandas

# %%
load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv('USER_SNOWFLAKE'),
    password=os.getenv('PASSWORD_SNOWFLAKE'),
    account=os.getenv('ACOUNT_SNOWFLAKE'),
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema='GOLD',
)
print('Connexion Snowflake OK')

# %%
query = """
SELECT f.*, d.latitude, d.longitude
FROM GOLD.FUSION_AQICN_WEATHER f
LEFT JOIN SILVER.DIM_CITY d
    ON f.city_id = d.city_id
"""

df_all = pd.read_sql(query, conn)
df_all['DT_HOUR'] = pd.to_datetime(df_all['DT_HOUR'], errors='coerce', utc=True)
df_all = df_all[df_all['DT_HOUR'].notna()].copy()

print(f'Shape total  : {df_all.shape}')
print(f'Periode      : {df_all["DT_HOUR"].min()} -> {df_all["DT_HOUR"].max()}')
print(f'Villes       : {df_all["CITY_ID"].nunique()}')

# %%
#découpage date

df = df_all.copy()

df['MONTH']       = df['DT_HOUR'].dt.month
df['DAY_OF_WEEK'] = df['DT_HOUR'].dt.dayofweek
df['DAY_OF_YEAR'] = df['DT_HOUR'].dt.dayofyear
df['YEAR']        = df['DT_HOUR'].dt.year

df = df.replace(-999, np.nan)

# %%
# 1. Découpage pour l'AQI (Base de données complète)
df_aqi = df[[
    'CITY_ID',
    'IAQI_O3',   'O3_LAG_12',   'O3_LAG_24',   'O3_LAG_48',
    'IAQI_PM10', 'PM10_LAG_12', 'PM10_LAG_24', 'PM10_LAG_48',
    'IAQI_PM25', 'PM25_LAG_12', 'PM25_LAG_24', 'PM25_LAG_48', 
    'AQI',       'AQI_LAG_12',  'AQI_LAG_24',  'AQI_LAG_48', 
    'LATITUDE', 'LONGITUDE', 
    'MONTH', 'DAY_OF_WEEK', 'DAY_OF_YEAR', 'YEAR'
]]

# ==========================================
# 2. DATASET HORAIRE (Prédiction à +12h)
# Garde : Target (AQI) + TOUS les lags disponibles (12, 24, 48)
# ==========================================
colonnes_12 = [
    'AQI', # La Target !
    'O3_LAG_12', 'PM10_LAG_12', 'PM25_LAG_12', 'AQI_LAG_12',
    'O3_LAG_24', 'PM10_LAG_24', 'PM25_LAG_24', 'AQI_LAG_24',
    'O3_LAG_48', 'PM10_LAG_48', 'PM25_LAG_48', 'AQI_LAG_48',
    'LATITUDE', 'LONGITUDE', 'MONTH', 'DAY_OF_WEEK', 'DAY_OF_YEAR', 'YEAR'
]
df_aqi_12 = df_aqi[colonnes_12]

# On supprime les lignes où l'historique récent (lag 12) est totalement vide
df_aqi_12 = df_aqi_12.dropna(
    subset=['O3_LAG_12', 'PM10_LAG_12', 'AQI_LAG_12'], 
    how='all'
)

# ==========================================
# 3. AGGRÉGATION JOURNALIÈRE (Préparation pour +24h et +48h)
# ==========================================
# On groupe par ville et par jour en prenant le maximum
df_aqi_daily = df_aqi.groupby(['CITY_ID', 'YEAR', 'DAY_OF_YEAR'], as_index=False).max(numeric_only=True)

# On supprime les polluants actuels (sauf AQI global) et les lags 12h
colonnes_a_supprimer_daily = [
    'CITY_ID', 'IAQI_PM10', 'IAQI_O3', 'IAQI_PM25',
    'O3_LAG_12', 'PM10_LAG_12', 'PM25_LAG_12', 'AQI_LAG_12'
]

df_aqi_daily = df_aqi_daily.drop(columns=colonnes_a_supprimer_daily)

# ==========================================
# 4. SÉPARATION DES PRÉDICTIONS (+24h et +48h)
# ==========================================

# Dataset 24h : On garde les lags 24 ET les lags 48 !
df_aqi_24 = df_aqi_daily.copy()

# Dataset 48h : On garde UNIQUEMENT les lags 48 (On supprime donc les lags 24)
df_aqi_48 = df_aqi_daily.drop(columns=['O3_LAG_24', 'PM10_LAG_24', 'PM25_LAG_24', 'AQI_LAG_24'])

# %%
# 1. Découpage pour les variables météorologiques
df_meto = df.dropna(subset=['TEMPERATURE'])

df_meteo = df_meto[[
    'RECORD_ID', 'CITY_ID', 'DT_HOUR', 
    'TEMPERATURE', 'HUMIDITY', 'WIND_SPEED', 'VISIBILITY', 
    'TEMP_LAG_12', 'HUM_LAG_12', 'WIND_LAG_12', 'VISIBILITY_LAG_12', 'PRESSURE_LAG_12', 
    'TEMP_LAG_24', 'HUM_LAG_24', 'WIND_LAG_24', 'VISIBILITY_LAG_24', 'PRESSURE_LAG_24', 
    'TEMP_LAG_48', 'HUM_LAG_48', 'WIND_LAG_48', 'VISIBILITY_LAG_48', 'PRESSURE_LAG_48', 
    'LATITUDE', 'LONGITUDE', 
    'MONTH', 'DAY_OF_WEEK', 'DAY_OF_YEAR', 'YEAR'
]]

# ==========================================
# 2. DATASET HORAIRE (Prédiction à +12h)
# Garde : Target + Lags 12, 24 et 48
# ==========================================
colonnes_12 = [
    'TEMPERATURE', 
    'TEMP_LAG_12', 'HUM_LAG_12', 'WIND_LAG_12', 'VISIBILITY_LAG_12', 'PRESSURE_LAG_12',
    'TEMP_LAG_24', 'HUM_LAG_24', 'WIND_LAG_24', 'VISIBILITY_LAG_24', 'PRESSURE_LAG_24',
    'TEMP_LAG_48', 'HUM_LAG_48', 'WIND_LAG_48', 'VISIBILITY_LAG_48', 'PRESSURE_LAG_48',
    'LATITUDE', 'LONGITUDE', 'MONTH', 'DAY_OF_WEEK', 'DAY_OF_YEAR', 'YEAR'
]
df_meteo_12 = df_meteo[colonnes_12]

df_meteo_12 = df_meteo_12.dropna(
    subset=['TEMP_LAG_12', 'HUM_LAG_12', 'WIND_LAG_12', 'VISIBILITY_LAG_12', 'PRESSURE_LAG_12'], 
    how='all'
)

# On supprime la météo actuelle (sauf la TARGET) et les lags 12h (car on vise à 24h min)
colonnes_a_supprimer_daily = [
    'CITY_ID','HUMIDITY', 'WIND_SPEED', 'VISIBILITY',
    'TEMP_LAG_12', 'HUM_LAG_12', 'WIND_LAG_12', 'VISIBILITY_LAG_12', 'PRESSURE_LAG_12'
]

df_meteo_daily = df_meteo.drop(columns=colonnes_a_supprimer_daily)

# ==========================================
# 4. SÉPARATION DES PRÉDICTIONS (+24h et +48h)
# ==========================================

# Dataset 24h : On garde les lags 24 ET les lags 48 (On ne supprime rien de plus !)
df_meteo_24 = df_meteo_daily.copy()

# Dataset 48h : On garde UNIQUEMENT les lags 48 (On supprime donc les lags 24)
df_meteo_48 = df_meteo_daily.drop(columns=[
    'TEMP_LAG_24', 'HUM_LAG_24', 'WIND_LAG_24', 'VISIBILITY_LAG_24', 'PRESSURE_LAG_24'
])

# %%
# 1. Définition des modèles (fusionnés en un seul dictionnaire pour la boucle)
ALL_MODELS = {
    'LinearRegression': LinearRegression(),
    'Ridge':            Ridge(alpha=1.0),
    'DecisionTree':     DecisionTreeRegressor(random_state=42),
    'RandomForest':     RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1),
    'GradientBoosting': GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

# 1. Gestion du répertoire de stockage (Volume Airflow/Docker)
MODEL_DIR = Path("/app/models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# 2. Configuration des boucles
# J'ai structuré ça dans une liste de dictionnaires pour itérer facilement
configurations = [
    {
        'domaine': 'AQI',
        'target': 'AQI',
        'dfs': [df_aqi_12, df_aqi_24, df_aqi_48],
        'horizons': [12, 24, 48]
    },
    {
        'domaine': 'Meteo',
        'target': 'TEMPERATURE', # J'utilise bien TEMPERATURE comme demandé au début
        'dfs': [df_meteo_12, df_meteo_24, df_meteo_48],
        'horizons': [12, 24, 48]
    }
]

# Colonnes à exclure des features (identifiants, dates) pour éviter de faire planter les modèles
cols_to_drop = ['RECORD_ID', 'CITY_ID', 'DT_HOUR'] 

all_results = []

# 3. Boucle principale d'entraînement
for config in configurations:
    domaine = config['domaine']
    target = config['target']
    
    for df, horizon in zip(config['dfs'], config['horizons']):
        
        # Nettoyage rapide : on enlève les lignes où la target manque (sinon scikit-learn plante)
        df_clean = df.dropna(subset=[target]).copy()
        
        # Séparation Features (X) / Target (y)
        # On exclut la target ET les colonnes d'identifiants/dates
        X = df_clean.drop(columns=[target] + [col for col in cols_to_drop if col in df_clean.columns])
        
        # Si tu as encore des valeurs manquantes dans X, il faut les imputer ici
        # X = X.fillna(0) # Décommenter si besoin d'une imputation rapide
        
        y = df_clean[target]
        
        # Train / Test split (80% train, 20% test)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # ==========================================
        # 1. IMPUTATION (Traitement des NaN)
        # ==========================================
        imputer = SimpleImputer(strategy='median', add_indicator=True)

        # LA LIGNE MAGIQUE : On force Scikit-Learn à nous rendre un DataFrame Pandas
        imputer.set_output(transform="pandas")

        X_train = imputer.fit_transform(X_train)
        X_test = imputer.transform(X_test)


        # ==========================================
        # 2. MISE À L'ÉCHELLE (Standard Scaler)
        # ==========================================
        # Maintenant que X_train est toujours un DataFrame, select_dtypes va fonctionner !
        numeric_cols = X_train.select_dtypes(include=['int64', 'float64']).columns

        scaler = StandardScaler()
        # On applique aussi la ligne magique au scaler
        scaler.set_output(transform="pandas")

        # On scale uniquement les colonnes numériques
        X_train[numeric_cols] = scaler.fit_transform(X_train[numeric_cols])
        X_test[numeric_cols] = scaler.transform(X_test[numeric_cols])
        
        best_r2 = -float('inf')
        best_model_obj = None
        best_name = ""
        # Entraînement de chaque modèle
        for model_name, model in ALL_MODELS.items():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            
            # Enregistrement des résultats (pour df_results)
            all_results.append({
                'domaine': domaine,
                'horizon_h': horizon,
                'model': model_name,
                'r2': r2,
                'mae': mean_absolute_error(y_test, y_pred),
                'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
                'run_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'best_model': False # Sera mis à jour après la boucle
            })

            # Vérification : est-ce le meilleur modèle pour cet horizon précis ?
            if r2 > best_r2:
                best_r2 = r2
                best_model_obj = model
                best_name = model_name

        # ============================================================
        # 4. SAUVEGARDE PHYSIQUE DU MEILLEUR MODÈLE PAR HORIZON
        # ============================================================
        if best_model_obj is not None:
            # Construction du nom du fichier : ex aqi_12.joblib ou meteo_24.joblib
            # Si le domaine est 'Meteo', on peut aussi forcer le nom 'temp' si tu préfères
            prefix = "AQI" if domaine == "AQI" else "TEMP"
            file_name = f"{prefix}_{horizon}.joblib"
            model_path = MODEL_DIR / file_name
            
            # On crée un dictionnaire contenant le modèle ET le scaler/imputer 
            # (Optionnel mais recommandé pour que ton modèle soit autonome)
            model_package = {
                'model': best_model_obj,
                'scaler': scaler,
                'imputer': imputer,
                'features': X_train.columns.tolist()
            }
            
            joblib.dump(model_package, model_path)
            
            print(f"🏆 Meilleur modèle pour {domaine} ({horizon}h) : {best_name} (R²: {best_r2:.4f})")
            print(f"💾 Sauvegardé sous : {model_path}")

# 4. Création du DataFrame final
df_results = pd.DataFrame(all_results)

# 5. Ajout de la colonne 'best_model' (basé sur le meilleur R2 par domaine et horizon)
# On initialise tout à False
df_results['best_model'] = False

# On trouve l'index du meilleur modèle (max R2) pour chaque groupe et on passe le booléen à True
idx_best_models = df_results.groupby(['domaine', 'horizon_h'])['r2'].idxmax()
df_results.loc[idx_best_models, 'best_model'] = True

# 6. Ajout de la date d'exécution
df_results['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Affichage des résultats triés
df_results = df_results.sort_values(by=['domaine', 'horizon_h', 'r2'], ascending=[True, True, False]).reset_index(drop=True)

# %%
# ============================================================
# 6. SAUVEGARDE DES PERFORMANCES DANS SNOWFLAKE (Via write_pandas)
# ============================================================


print("⏳ Préparation du DataFrame pour Snowflake...")

if not df_results.empty:
    # 1. Création d'une copie pour ne pas modifier l'original
    df_to_snowflake = df_results.copy()

    # 2. On recrée la colonne MODEL_NAME propre (Domaine + Horizon + Nom)
    # Exemple : "AQI_12H_RandomForest"
    df_to_snowflake['DOMAINE'] = (
        df_to_snowflake['domaine'].astype(str) + "_" + 
        df_to_snowflake['horizon_h'].astype(str)
    )

    df_to_snowflake['MODEL_NAME'] = df_to_snowflake['model']

    # 3. On sélectionne et on renomme pour matcher EXACTEMENT la table Snowflake
    # On s'assure que l'ordre et les noms correspondent aux colonnes de ta table
    df_to_snowflake = df_to_snowflake[[
        'DOMAINE','MODEL_NAME', 'mae', 'rmse', 'r2', 'run_date', 'best_model'
    ]]
    
    df_to_snowflake.columns = ['DOMAINE','MODEL_NAME', 'MAE', 'RMSE', 'R2', 'TRAINING_DATE', 'IS_BEST']

    # 4. Conversion explicite des types pour éviter les erreurs de driver
    df_to_snowflake['MAE'] = df_to_snowflake['MAE'].astype(float)
    df_to_snowflake['RMSE'] = df_to_snowflake['RMSE'].astype(float)
    df_to_snowflake['R2'] = df_to_snowflake['R2'].astype(float)
    df_to_snowflake['TRAINING_DATE'] = df_to_snowflake['TRAINING_DATE'].astype(str)
    df_to_snowflake['IS_BEST'] = df_to_snowflake['IS_BEST'].astype(bool)

    print(f"DEBUG : Envoi de {len(df_to_snowflake)} lignes vers Snowflake...")

    try:
        # Envoi direct du DataFrame
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn, 
            df=df_to_snowflake, 
            table_name='ML_MODEL_PERFORMANCE', 
            schema='LOGS', 
            database='GOOD_AIR'
        )
        
        if success:
            print(f"✅ Succès ! {num_rows} lignes insérées dans Snowflake.")
        else:
            print("❌ L'insertion a échoué.")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'usage de write_pandas : {e}")

else:
    print("⚠️ df_results est vide, rien à envoyer.")

conn.close()


