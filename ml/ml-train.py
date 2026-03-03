# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import snowflake.connector
from sklearn.metrics import mean_absolute_error
import seaborn as sns
from dotenv import load_dotenv
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import numpy as np
from sklearn.impute import SimpleImputer
import joblib
from datetime import datetime

# %%
# Chargement des variables
load_dotenv('/app/.env')

# Connexion
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="SILVER",
)

print("✅ Connexion Snowflake via .env OK")

# %%
df = pd.read_sql("SELECT * FROM fact_air_quality_records", conn)
df.head()

# %%
df.columns = [c.lower() for c in df.columns]


# %%
df_corr = df[['aqi','iaqi_no2', 'iaqi_o3', 'iaqi_pm10',
       'iaqi_pm25', 'iaqi_temp', 'iaqi_humidity', 'iaqi_pressure',
       'iaqi_wind', 'iaqi_wind_gust']]

# %%


corr_matrix = df_corr.corr()

plt.figure(figsize=(8,6))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
plt.title("Matrice de corrélation AQI")
plt.show()


# %%
df_model =  df[['aqi','iaqi_o3', 'iaqi_pm10','iaqi_pm25']]

# %%
# ============================================================
# 1. PRÉPARATION DES DONNÉES
# ============================================================

TARGET = "aqi"  # En majuscule pour correspondre à Snowflake

# Nettoyage des valeurs manquantes sur la cible
df_model = df_model.dropna(subset=[TARGET])

X = df_model.drop(columns=[TARGET])
y = df_model[TARGET]

# Split Train/Test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ============================================================
# 2. CONFIGURATION DES PIPELINES
# ============================================================

# Définition des modèles à tester
model_factory = {
    "LinearRegression": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", LinearRegression())
    ]),
    "DecisionTree": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", DecisionTreeRegressor(random_state=42))
    ]),
    "RandomForest": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", RandomForestRegressor(random_state=42, n_estimators=300))
    ]),
    "GradientBoosting": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", GradientBoostingRegressor(random_state=42))
    ]),
}

# ============================================================
# 3. ENTRAÎNEMENT ET ÉVALUATION
# ============================================================

performance_log = {}
trained_pipelines = {}

print(f"🚀 Début de l'entraînement sur {len(X_train)} lignes...")

for name, pipeline in model_factory.items():
    # Entraînement
    pipeline.fit(X_train, y_train)
    
    # Prédiction
    y_pred = pipeline.predict(X_test)
    
    # Métriques
    performance_log[name] = {
        "MAE": mean_absolute_error(y_test, y_pred),
        "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
        "R2": r2_score(y_test, y_pred)
    }
    trained_pipelines[name] = pipeline
    print(f"✅ {name} entraîné (R²: {performance_log[name]['R2']:.4f})")

# ============================================================
# 4. SÉLECTION ET SAUVEGARDE DU MEILLEUR MODÈLE
# ============================================================

# Choix du meilleur modèle basé sur le R²
best_model_name = max(performance_log, key=lambda k: performance_log[k]["R2"])
best_pipeline = trained_pipelines[best_model_name]

# Gestion du répertoire de stockage (Volume Airflow/Docker)
MODEL_DIR = "/app/models"
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_PATH = os.path.join(MODEL_DIR, "best_model_aqi.joblib")

# Sauvegarde physique
joblib.dump(best_pipeline, MODEL_PATH)

print(f"\n🏆 MEILLEUR MODÈLE : {best_model_name}")
print(f"💾 Sauvegardé sous : {MODEL_PATH}")

# ============================================================
# 5. EXPORT DES RÉSULTATS POUR SNOWFLAKE
# ============================================================

# Transformation du dictionnaire de scores en DataFrame
df_results = pd.DataFrame.from_dict(performance_log, orient='index').reset_index()
df_results.columns = ['MODEL_NAME', 'MAE', 'RMSE', 'R2']

# Ajout des métadonnées de tracking
df_results['TRAINING_DATE'] = datetime.now()
df_results['IS_BEST'] = df_results['MODEL_NAME'] == best_model_name

# Mise en forme pour Snowflake (Majuscules)
df_results.columns = [col.upper() for col in df_results.columns]

print("\n📊 Tableau de performance prêt pour Snowflake :")
print(df_results.to_string(index=False))


