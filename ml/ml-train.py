
# ## 1. Imports

# %%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
import os

from dotenv import load_dotenv
import snowflake.connector

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

print('Imports OK')


# %% [markdown]
# ## 2. Connexion Snowflake

# %%
load_dotenv()
conn = snowflake.connector.connect(
    user=os.getenv("USER_SNOWFLAKE"),
    password=os.getenv("PASSWORD_SNOWFLAKE"),
    account=os.getenv("ACOUNT_SNOWFLAKE"),
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="SILVER",
)
print('Connexion Snowflake OK')


# %% [markdown]
# ## 3. Chargement des données

# %%
query = """
SELECT f.*, d.latitude, d.longitude
FROM GOLD.FUSION_AQICN_WEATHER f
LEFT JOIN SILVER.DIM_CITY d
    ON f.city_id = d.city_id
"""

df_all = pd.read_sql(query, conn)
df_all.columns = [c.lower() for c in df_all.columns]
df_all['dt_hour'] = pd.to_datetime(df_all['dt_hour'], errors='coerce', utc=True)
df_all = df_all[df_all['dt_hour'].notna()].copy()

print(f'Shape total  : {df_all.shape}')
print(f'Periode      : {df_all["dt_hour"].min()} -> {df_all["dt_hour"].max()}')
print(f'Villes       : {df_all["city_id"].nunique()}')


# %% [markdown]
# ## 4. Séparation données journalières / horaires
# 
# - **Avant le 12/12/2025** : données journalières AQICN (2013–2025) → Modèles 1
# - **À partir du 12/12/2025** : données horaires AQICN + OpenWeather → Modèles 2 et 3

# %%
CUTOFF = pd.Timestamp('2025-12-12', tz='UTC')

df_daily  = df_all[df_all['dt_hour'] <  CUTOFF].copy()
df_hourly = df_all[df_all['dt_hour'] >= CUTOFF].copy()

print(f'Journalier : {len(df_daily):,} lignes | {df_daily["dt_hour"].min().date()} -> {df_daily["dt_hour"].max().date()}')
print(f'Horaire    : {len(df_hourly):,} lignes | {df_hourly["dt_hour"].min().date()} -> {df_hourly["dt_hour"].max().date()}')


# %% [markdown]
# ## 5. Fonctions utilitaires

# %%
LINEAR_MODELS = {
    'LinearRegression': LinearRegression(),
    'Ridge':            Ridge(alpha=1.0),
}
TREE_MODELS = {
    'DecisionTree':     DecisionTreeRegressor(random_state=42),
    'RandomForest':     RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1),
    'GradientBoosting': GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
}

def build_pipeline(model, features, is_linear=True):
    steps = [('imputer', SimpleImputer(strategy='median'))]
    if is_linear:
        steps.append(('scaler', StandardScaler()))
    preprocess = ColumnTransformer([
        ('num', Pipeline(steps), features)
    ], remainder='drop')
    return Pipeline([('preprocess', preprocess), ('model', clone(model))])

def split_by_city(df, date_col, test_size=0.2):
    train_dfs, test_dfs = [], []
    for city, group in df.groupby('city_id'):
        group = group.sort_values(date_col).reset_index(drop=True)
        n = len(group)
        cutoff = int(n * (1 - test_size))
        train_dfs.append(group.iloc[:cutoff])
        test_dfs.append(group.iloc[cutoff:])
    return pd.concat(train_dfs).reset_index(drop=True), pd.concat(test_dfs).reset_index(drop=True)

def train_and_evaluate(train_df, test_df, features, target):
    results = {}
    best_r2, best_model = -np.inf, None

    print(f'\n{"="*52}')
    print(f'  TARGET : {target}')
    print(f'{"="*52}')

    for name, model in {**LINEAR_MODELS, **TREE_MODELS}.items():
        pipe = build_pipeline(model, features, is_linear=(name in LINEAR_MODELS))
        pipe.fit(train_df[features], train_df[target])
        pred = pipe.predict(test_df[features])

        r2   = r2_score(test_df[target], pred)
        mae  = mean_absolute_error(test_df[target], pred)
        rmse = np.sqrt(mean_squared_error(test_df[target], pred))

        results[name] = {'r2': r2, 'mae': mae, 'rmse': rmse, 'model': pipe}
        flag = 'OK' if r2 >= 0.5 else '!!'
        print(f'  [{flag}] {name:<22}  R2={r2:.3f}  MAE={mae:.2f}  RMSE={rmse:.2f}')

        if r2 > best_r2:
            best_r2, best_model = r2, pipe

    print(f'  => Meilleur modele : R2 = {best_r2:.3f}')
    return results, best_model

print('Fonctions utilitaires OK')


# %% [markdown]
# ---
# # MODÈLE 1 — Prédiction AQI futur (J+1 et J+2)
# *Données journalières AQICN | 2013–2025 | ~71 000 lignes | 21 villes*
# 
# **Cas d'usage :** anticiper les pics de pollution pour alerter la population et les chercheurs à l'avance.

# %% [markdown]
# ## 6. Préparation données journalières

# %%
df = df_daily.copy()

# Deduplication : une valeur par ville+jour
df['date'] = df['dt_hour'].dt.date
df = df.groupby(['city_id', 'date'], as_index=False).mean(numeric_only=True)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['city_id', 'date']).reset_index(drop=True)

# Features calendaires
df['month']       = df['date'].dt.month
df['day_of_week'] = df['date'].dt.dayofweek
df['day_of_year'] = df['date'].dt.dayofyear
df['year']        = df['date'].dt.year

# Nettoyage
df = df.replace(-999, np.nan)
df = df.groupby('city_id', group_keys=False).apply(lambda g: g.interpolate(method='linear'))
df = df.reset_index(drop=True)
cols_force = ['aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48']
df = df.loc[:, (df.isna().mean() < 0.7) | (df.columns.isin(cols_force))]
num_cols = df.select_dtypes(include='number').columns
df[num_cols] = df[num_cols].fillna(df[num_cols].median())

print(f'Shape : {df.shape}')
print(f'Doublons : {df.duplicated(["city_id","date"]).sum()}')
print(f'Missing  : {df.isna().sum().sum()}')


# %% [markdown]
# ## 7. Création des targets AQI futur

# %%
df = df.sort_values(['city_id', 'date']).reset_index(drop=True)

df['aqi_j1'] = df.groupby('city_id')['aqi'].shift(-1)
df['aqi_j2'] = df.groupby('city_id')['aqi'].shift(-2)

# Verification
city_test = df['city_id'].iloc[0]
s = df[df['city_id'] == city_test].sort_values('date').reset_index(drop=True)
print('Verification aqi_j1 :')
print(f'  aqi[0]    = {s.loc[0,"aqi"]:.1f}  ({s.loc[0,"date"].date()})')
print(f'  aqi[1]    = {s.loc[1,"aqi"]:.1f}  ({s.loc[1,"date"].date()})')
print(f'  aqi_j1[0] = {s.loc[0,"aqi_j1"]:.1f}  <- doit etre egal a aqi[1]')

df = df.dropna(subset=['aqi_j1', 'aqi_j2'])
print(f'\nShape final : {df.shape}')


# %% [markdown]
# ## 8. Features Modèle 1

# %%
FEATURES_M1 = [
    'aqi', 'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',
    'month', 'day_of_week', 'day_of_year', 'year',
    'latitude', 'longitude',
]
FEATURES_M1 = [c for c in FEATURES_M1 if c in df.columns]

print(f'Features retenues : {len(FEATURES_M1)}')
print(FEATURES_M1)

# Correlations
print('\nCorrelations cles :')
print(f'  corr(aqi, aqi_j1) = {df["aqi"].corr(df["aqi_j1"]):.3f}')
print(f'  corr(aqi, aqi_j2) = {df["aqi"].corr(df["aqi_j2"]):.3f}')


# %% [markdown]
# ## 9. Split et entraînement — Modèle 1

# %%
train_m1, test_m1 = split_by_city(df, 'date')
print(f'Train : {len(train_m1):,} | Test : {len(test_m1):,}')

results_m1 = {}
best_m1    = {}

for target in ['aqi_j1', 'aqi_j2']:
    res, best = train_and_evaluate(train_m1, test_m1, FEATURES_M1, target)
    results_m1[target] = res
    best_m1[target]    = best


# %% [markdown]
# ## 10. Récapitulatif Modèle 1

# %%
rows = []
for target, res in results_m1.items():
    for name, m in res.items():
        rows.append({'Target': target, 'Modele': name,
                     'R2': round(m['r2'],3), 'MAE': round(m['mae'],2), 'RMSE': round(m['rmse'],2)})
recap_m1 = pd.DataFrame(rows)
recap_m1['Valide'] = recap_m1['R2'].apply(lambda x: 'OUI' if x >= 0.5 else 'NON')
print(recap_m1.to_string(index=False))


# %% [markdown]
# ## 11. Visualisation Modèle 1

# %%
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

for col, target in enumerate(['aqi_j1', 'aqi_j2']):
    model  = best_m1[target]
    y_pred = model.predict(test_m1[FEATURES_M1])
    y_real = test_m1[target]
    r2     = r2_score(y_real, y_pred)

    ax = axes[0][col]
    ax.scatter(y_real, y_pred, alpha=0.2, color='steelblue', s=10)
    lim = [min(y_real.min(), y_pred.min()), max(y_real.max(), y_pred.max())]
    ax.plot(lim, lim, 'r--', linewidth=1.5)
    ax.set_xlabel('AQI reel')
    ax.set_ylabel('AQI predit')
    ax.set_title(f'{target} — Reel vs Predit (R2={r2:.3f})')

    ax = axes[1][col]
    n = min(365, len(y_real))
    ax.plot(y_real.values[:n], label='Reel', alpha=0.8)
    ax.plot(y_pred[:n], label='Predit', alpha=0.8, linestyle='--')
    ax.set_title(f'{target} — Serie temporelle')
    ax.set_xlabel('Jours')
    ax.set_ylabel('AQI')
    ax.legend()

plt.suptitle('Modele 1 — Prediction AQI futur', fontsize=14, y=1.01)
plt.tight_layout()
plt.show()



# %% [markdown]
# ## 13. Test prédiction réelle — Modèle 1
# 
# Prédiction AQI pour J+1 et J+2 à partir des dernières données connues.

# %%
latest = df.sort_values(['city_id', 'date']).groupby('city_id').last().reset_index()

print(f'Derniere date connue : {latest["date"].max().date()}')
print(f'Villes               : {len(latest)}\n')

for target, label in zip(['aqi_j1', 'aqi_j2'], ['J+1', 'J+2']):
    latest[f'pred_{label}'] = best_m1[target].predict(latest[FEATURES_M1])

print('=== Predictions AQI par ville ===')
print(latest[['city_id', 'aqi', 'pred_J+1', 'pred_J+2']].round(1).to_string(index=False))


# %% [markdown]
# ---
# # MODÈLE 2 — Estimation AQI temps réel
# *Données horaires AQICN + OpenWeather | déc. 2025 – fév. 2026*
# 
# **Cas d'usage :** estimer l'AQI des villes sans capteur AQI direct à partir des mesures de polluants.

# %% [markdown]
# ## 14. Préparation données horaires

# %%
df_h = df_hourly.copy()

# Deduplication
df_h = df_h.groupby(['city_id', 'dt_hour'], as_index=False).mean(numeric_only=True)
df_h = df_h.sort_values(['city_id', 'dt_hour']).reset_index(drop=True)

# Features calendaires
df_h['month']       = df_h['dt_hour'].dt.month
df_h['day_of_week'] = df_h['dt_hour'].dt.dayofweek
df_h['hour']        = df_h['dt_hour'].dt.hour

# Nettoyage
df_h = df_h.replace(-999, np.nan)
df_h = df_h.groupby('city_id', group_keys=False).apply(lambda g: g.interpolate(method='linear'))
df_h = df_h.reset_index(drop=True)
cols_force = ['aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48']
df_h = df_h.loc[:, (df_h.isna().mean() < 0.7) | (df_h.columns.isin(cols_force))]
num_cols = df_h.select_dtypes(include='number').columns
df_h[num_cols] = df_h[num_cols].fillna(df_h[num_cols].median())

print(f'Shape : {df_h.shape}')
print(f'Missing : {df_h.isna().sum().sum()}')


# %% [markdown]
# ## 15. Features et entraînement — Modèle 2

# %%
FEATURES_M2 = [
    'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24', 'pm10_lag_48',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24', 'pm25_lag_48',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',   'o3_lag_48',
    'month', 'day_of_week', 'hour',
    'latitude', 'longitude',
]
FEATURES_M2 = [c for c in FEATURES_M2 if c in df_h.columns]

train_m2, test_m2 = split_by_city(df_h, 'dt_hour')
print(f'Train : {len(train_m2):,} | Test : {len(test_m2):,}\n')

results_m2, best_m2 = train_and_evaluate(train_m2, test_m2, FEATURES_M2, 'aqi')



# %% [markdown]
# ---
# # MODÈLE 3 — Prédiction Température +12h et +24h
# *Données horaires OpenWeather | déc. 2025 – fév. 2026*
# 
# **Cas d'usage :** prévoir la température à court terme pour anticiper les conditions favorables aux pics de pollution (canicules, inversions thermiques).

# %% [markdown]
# ## 17. Création des targets température future

# %%
df_h = df_h.sort_values(['city_id', 'dt_hour']).reset_index(drop=True)

df_h['temp_12h'] = df_h.groupby('city_id')['temperature'].shift(-12)
df_h['temp_24h'] = df_h.groupby('city_id')['temperature'].shift(-24)

# Verification
city_test = df_h['city_id'].iloc[0]
s = df_h[df_h['city_id'] == city_test].sort_values('dt_hour').reset_index(drop=True)
print('Verification temp_24h :')
print(f'  temp[0]    = {s.loc[0,"temperature"]:.1f}  ({s.loc[0,"dt_hour"]})')
print(f'  temp[24]   = {s.loc[24,"temperature"]:.1f}  ({s.loc[24,"dt_hour"]})')
print(f'  temp_24h[0]= {s.loc[0,"temp_24h"]:.1f}  <- doit etre egal a temp[24]')

df_h = df_h.dropna(subset=['temp_12h', 'temp_24h'])
print(f'\nShape apres dropna : {df_h.shape}')

# Correlations
print('\nCorrelations cles :')
print(f'  corr(temperature, temp_12h) = {df_h["temperature"].corr(df_h["temp_12h"]):.3f}')
print(f'  corr(temperature, temp_24h) = {df_h["temperature"].corr(df_h["temp_24h"]):.3f}')


# %% [markdown]
# ## 18. Features Modèle 3

# %%
FEATURES_M3 = [
    # Temperature et lags
    'temperature', 'temp_lag_12', 'temp_lag_24', 'temp_lag_48',
    # Humidity et lags
    'humidity', 'hum_lag_12', 'hum_lag_24',
    # Wind et lags
    'wind_speed', 'wind_lag_12', 'wind_lag_24',
    # Visibility et lags
    'visibility', 'visibility_lag_12', 'visibility_lag_24',
    # Temporel
    'hour', 'month',
    # Geographique
    'latitude', 'longitude',
]
FEATURES_M3 = [c for c in FEATURES_M3 if c in df_h.columns]

print(f'Features retenues : {len(FEATURES_M3)}')
print(FEATURES_M3)


# %% [markdown]
# ## 19. Split et entraînement — Modèle 3

# %%
train_m3, test_m3 = split_by_city(df_h, 'dt_hour')
print(f'Train : {len(train_m3):,} | Test : {len(test_m3):,}\n')

results_m3 = {}
best_m3    = {}

for target in ['temp_12h', 'temp_24h']:
    res, best = train_and_evaluate(train_m3, test_m3, FEATURES_M3, target)
    results_m3[target] = res
    best_m3[target]    = best


# %% [markdown]
# ## 20. Récapitulatif Modèle 3

# %%
rows = []
for target, res in results_m3.items():
    for name, m in res.items():
        rows.append({'Target': target, 'Modele': name,
                     'R2': round(m['r2'],3), 'MAE': round(m['mae'],2), 'RMSE': round(m['rmse'],2)})
recap_m3 = pd.DataFrame(rows)
recap_m3['Valide'] = recap_m3['R2'].apply(lambda x: 'OUI' if x >= 0.5 else 'NON')
print(recap_m3.to_string(index=False))


# %% [markdown]
# ## 21. Visualisation — Modèle 3

# %%
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

for col, target in enumerate(['temp_12h', 'temp_24h']):
    model  = best_m3[target]
    y_pred = model.predict(test_m3[FEATURES_M3])
    y_real = test_m3[target]
    r2     = r2_score(y_real, y_pred)

    ax = axes[0][col]
    ax.scatter(y_real, y_pred, alpha=0.3, color='coral', s=10)
    lim = [min(y_real.min(), y_pred.min()), max(y_real.max(), y_pred.max())]
    ax.plot(lim, lim, 'r--', linewidth=1.5)
    ax.set_xlabel('Temperature reelle (C)')
    ax.set_ylabel('Temperature predite (C)')
    ax.set_title(f'{target} — Reel vs Predit (R2={r2:.3f})')

    ax = axes[1][col]
    n = min(300, len(y_real))
    ax.plot(y_real.values[:n], label='Reelle', alpha=0.8)
    ax.plot(y_pred[:n], label='Predite', alpha=0.8, linestyle='--')
    ax.set_title(f'{target} — Serie temporelle')
    ax.set_xlabel('Heures')
    ax.set_ylabel('Temperature (C)')
    ax.legend()

plt.suptitle('Modele 3 — Prediction Temperature future', fontsize=14, y=1.01)
plt.tight_layout()
plt.show()



# %% [markdown]
# ## 23. Test prédiction réelle — Modèle 3

# %%
latest_h = df_h.sort_values(['city_id', 'dt_hour']).groupby('city_id').last().reset_index()

print(f'Derniere heure connue : {latest_h["dt_hour"].max()}')
print(f'Villes                : {len(latest_h)}\n')

for target, label in zip(['temp_12h', 'temp_24h'], ['+12h', '+24h']):
    latest_h[f'pred_{label}'] = best_m3[target].predict(latest_h[FEATURES_M3])

print('=== Predictions Temperature par ville ===')
print(latest_h[['city_id', 'temperature', 'pred_+12h', 'pred_+24h']].round(1).to_string(index=False))


# %% [markdown]
# ## 24. Sauvegarde de tous les modèles

# %%
os.makedirs('models', exist_ok=True)

# Modele 1 — AQI futur
for target, model in best_m1.items():
    path = f'models/modele1_aqi_{target}.pkl'
    joblib.dump(model, path)
    print(f'Sauvegarde : {path}')

# Modele 2 — AQI temps reel
joblib.dump(best_m2, 'models/modele2_aqi_temps_reel.pkl')
print('Sauvegarde : models/modele2_aqi_temps_reel.pkl')

# Modele 3 — Temperature
for target, model in best_m3.items():
    path = f'models/modele3_temp_{target}.pkl'
    joblib.dump(model, path)
    print(f'Sauvegarde : {path}')


# ## 1. Imports

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

print('Imports OK')


# %% [markdown]
# ## 2. Connexion Snowflake

# %%
# Essai du chemin absolu (style Docker/Airflow), sinon fallback classique
try:
    load_dotenv('/app/.env')
except:
    load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('USER_SNOWFLAKE'),
    password=os.getenv('PASSWORD_SNOWFLAKE'),
    account=os.getenv('ACOUNT_SNOWFLAKE'),
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema='GOLD',
)
print('✅ Connexion Snowflake OK')


# %% [markdown]
# ## 3. Chargement des données

# %%
query = """
SELECT f.*, d.latitude, d.longitude
FROM GOLD.FUSION_AQICN_WEATHER f
LEFT JOIN SILVER.DIM_CITY d
    ON f.city_id = d.city_id
"""

df_all = pd.read_sql(query, conn)
df_all.columns = [c.lower() for c in df_all.columns]
df_all['dt_hour'] = pd.to_datetime(df_all['dt_hour'], errors='coerce', utc=True)
df_all = df_all[df_all['dt_hour'].notna()].copy()

print(f'Shape total  : {df_all.shape}')
print(f'Periode      : {df_all["dt_hour"].min()} -> {df_all["dt_hour"].max()}')
print(f'Villes       : {df_all["city_id"].nunique()}')


# %% [markdown]
# ## 4. Séparation données journalières / horaires
# 
# - **Avant le 12/12/2025** : données journalières AQICN (2013–2025) → Modèles 1
# - **À partir du 12/12/2025** : données horaires AQICN + OpenWeather → Modèles 2 et 3

# %%
CUTOFF = pd.Timestamp('2025-12-12', tz='UTC')

df_daily  = df_all[df_all['dt_hour'] <  CUTOFF].copy()
df_hourly = df_all[df_all['dt_hour'] >= CUTOFF].copy()

print(f'Journalier : {len(df_daily):,} lignes | {df_daily["dt_hour"].min().date()} -> {df_daily["dt_hour"].max().date()}')
print(f'Horaire    : {len(df_hourly):,} lignes | {df_hourly["dt_hour"].min().date()} -> {df_hourly["dt_hour"].max().date()}')


# %% [markdown]
# ## 5. Fonctions utilitaires

# %%
LINEAR_MODELS = {
    'LinearRegression': LinearRegression(),
    'Ridge':            Ridge(alpha=1.0),
}
TREE_MODELS = {
    'DecisionTree':     DecisionTreeRegressor(random_state=42),
    'RandomForest':     RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1),
    'GradientBoosting': GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42),
}

def build_pipeline(model, features, is_linear=True):
    steps = [('imputer', SimpleImputer(strategy='median'))]
    if is_linear:
        steps.append(('scaler', StandardScaler()))
    preprocess = ColumnTransformer([
        ('num', Pipeline(steps), features)
    ], remainder='drop')
    return Pipeline([('preprocess', preprocess), ('model', clone(model))])

def split_by_city(df, date_col, test_size=0.2):
    train_dfs, test_dfs = [], []
    for city, group in df.groupby('city_id'):
        group = group.sort_values(date_col).reset_index(drop=True)
        n = len(group)
        cutoff = int(n * (1 - test_size))
        train_dfs.append(group.iloc[:cutoff])
        test_dfs.append(group.iloc[cutoff:])
    return pd.concat(train_dfs).reset_index(drop=True), pd.concat(test_dfs).reset_index(drop=True)

def train_and_evaluate(train_df, test_df, features, target):
    results = {}
    best_r2, best_model = -np.inf, None

    print(f'\n{"="*52}')
    print(f'  TARGET : {target}')
    print(f'{"="*52}')

    for name, model in {**LINEAR_MODELS, **TREE_MODELS}.items():
        pipe = build_pipeline(model, features, is_linear=(name in LINEAR_MODELS))
        pipe.fit(train_df[features], train_df[target])
        pred = pipe.predict(test_df[features])

        r2   = r2_score(test_df[target], pred)
        mae  = mean_absolute_error(test_df[target], pred)
        rmse = np.sqrt(mean_squared_error(test_df[target], pred))

        results[name] = {'r2': r2, 'mae': mae, 'rmse': rmse, 'model': pipe}
        flag = 'OK' if r2 >= 0.5 else '!!'
        print(f'  [{flag}] {name:<22}  R2={r2:.3f}  MAE={mae:.2f}  RMSE={rmse:.2f}')

        if r2 > best_r2:
            best_r2, best_model = r2, pipe

    print(f'  => Meilleur modele : R2 = {best_r2:.3f}')
    return results, best_model

print('Fonctions utilitaires OK')


# %% [markdown]
# ---
# # MODÈLE 1 — Prédiction AQI futur (J+1 et J+2)

# %% [markdown]
# ## 6. Préparation données journalières

# %%
df = df_daily.copy()

df['date'] = df['dt_hour'].dt.date
df = df.groupby(['city_id', 'date'], as_index=False).mean(numeric_only=True)
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['city_id', 'date']).reset_index(drop=True)

df['month']       = df['date'].dt.month
df['day_of_week'] = df['date'].dt.dayofweek
df['day_of_year'] = df['date'].dt.dayofyear
df['year']        = df['date'].dt.year

df = df.replace(-999, np.nan)
df = df.groupby('city_id', group_keys=False).apply(lambda g: g.interpolate(method='linear'))
df = df.reset_index(drop=True)
cols_force = ['aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48']
df = df.loc[:, (df.isna().mean() < 0.7) | (df.columns.isin(cols_force))]
num_cols = df.select_dtypes(include='number').columns
df[num_cols] = df[num_cols].fillna(df[num_cols].median())

print(f'Shape : {df.shape}')


# %% [markdown]
# ## 7. Création des targets AQI futur

# %%
df = df.sort_values(['city_id', 'date']).reset_index(drop=True)

df['aqi_j1'] = df.groupby('city_id')['aqi'].shift(-1)
df['aqi_j2'] = df.groupby('city_id')['aqi'].shift(-2)

df = df.dropna(subset=['aqi_j1', 'aqi_j2'])
print(f'Shape final : {df.shape}')


# %% [markdown]
# ## 8. Features Modèle 1

# %%
FEATURES_M1 = [
    'aqi', 'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',
    'month', 'day_of_week', 'day_of_year', 'year',
    'latitude', 'longitude',
]
FEATURES_M1 = [c for c in FEATURES_M1 if c in df.columns]


# %% [markdown]
# ## 9. Split et entraînement — Modèle 1

# %%
train_m1, test_m1 = split_by_city(df, 'date')

results_m1 = {}
best_m1    = {}

for target in ['aqi_j1', 'aqi_j2']:
    res, best = train_and_evaluate(train_m1, test_m1, FEATURES_M1, target)
    results_m1[target] = res
    best_m1[target]    = best


# %% [markdown]
# ## 10. Test prédiction réelle — Modèle 1

# %%
latest = df.sort_values(['city_id', 'date']).groupby('city_id').last().reset_index()

for target, label in zip(['aqi_j1', 'aqi_j2'], ['J+1', 'J+2']):
    latest[f'pred_{label}'] = best_m1[target].predict(latest[FEATURES_M1])


# %% [markdown]
# ---
# # MODÈLE 2 — Estimation AQI temps réel

# %% [markdown]
# ## 11. Préparation données horaires

# %%
df_h = df_hourly.copy()

df_h = df_h.groupby(['city_id', 'dt_hour'], as_index=False).mean(numeric_only=True)
df_h = df_h.sort_values(['city_id', 'dt_hour']).reset_index(drop=True)

df_h['month']       = df_h['dt_hour'].dt.month
df_h['day_of_week'] = df_h['dt_hour'].dt.dayofweek
df_h['hour']        = df_h['dt_hour'].dt.hour

df_h = df_h.replace(-999, np.nan)
df_h = df_h.groupby('city_id', group_keys=False).apply(lambda g: g.interpolate(method='linear'))
df_h = df_h.reset_index(drop=True)
cols_force = ['aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48']
df_h = df_h.loc[:, (df_h.isna().mean() < 0.7) | (df_h.columns.isin(cols_force))]
num_cols = df_h.select_dtypes(include='number').columns
df_h[num_cols] = df_h[num_cols].fillna(df_h[num_cols].median())


# %% [markdown]
# ## 12. Features et entraînement — Modèle 2

# %%
FEATURES_M2 = [
    'aqi_lag_12', 'aqi_lag_24', 'aqi_lag_48',
    'iaqi_pm10', 'pm10_lag_12', 'pm10_lag_24', 'pm10_lag_48',
    'iaqi_pm25', 'pm25_lag_12', 'pm25_lag_24', 'pm25_lag_48',
    'iaqi_o3',   'o3_lag_12',   'o3_lag_24',   'o3_lag_48',
    'month', 'day_of_week', 'hour',
    'latitude', 'longitude',
]
FEATURES_M2 = [c for c in FEATURES_M2 if c in df_h.columns]

train_m2, test_m2 = split_by_city(df_h, 'dt_hour')
results_m2, best_m2 = train_and_evaluate(train_m2, test_m2, FEATURES_M2, 'aqi')


# %% [markdown]
# ---
# # MODÈLE 3 — Prédiction Température +12h et +24h

# %% [markdown]
# ## 13. Création des targets température future

# %%
df_h = df_h.sort_values(['city_id', 'dt_hour']).reset_index(drop=True)

df_h['temp_12h'] = df_h.groupby('city_id')['temperature'].shift(-12)
df_h['temp_24h'] = df_h.groupby('city_id')['temperature'].shift(-24)

df_h = df_h.dropna(subset=['temp_12h', 'temp_24h'])


# %% [markdown]
# ## 14. Features et entraînement — Modèle 3

# %%
FEATURES_M3 = [
    'temperature', 'temp_lag_12', 'temp_lag_24', 'temp_lag_48',
    'humidity', 'hum_lag_12', 'hum_lag_24',
    'wind_speed', 'wind_lag_12', 'wind_lag_24',
    'visibility', 'visibility_lag_12', 'visibility_lag_24',
    'hour', 'month',
    'latitude', 'longitude',
]
FEATURES_M3 = [c for c in FEATURES_M3 if c in df_h.columns]

train_m3, test_m3 = split_by_city(df_h, 'dt_hour')

results_m3 = {}
best_m3    = {}

for target in ['temp_12h', 'temp_24h']:
    res, best = train_and_evaluate(train_m3, test_m3, FEATURES_M3, target)
    results_m3[target] = res
    best_m3[target]    = best


# %% [markdown]
# ## 15. Sauvegarde des Modèles et Logs Snowflake (Joblib + DB)

# %%
# ============================================================
# A. SAUVEGARDE PHYSIQUE DES MODÈLES
# ============================================================

MODEL_DIR = "/app/models"
os.makedirs(MODEL_DIR, exist_ok=True)

# Modele 1 — AQI futur
for target, model in best_m1.items():
    path = os.path.join(MODEL_DIR, f"modele1_aqi_{target}.joblib")
    joblib.dump(model, path)
    print(f"💾 Sauvegarde : {path}")

# Modele 2 — AQI temps reel
path_m2 = os.path.join(MODEL_DIR, "modele2_aqi_temps_reel.joblib")
joblib.dump(best_m2, path_m2)
print(f"💾 Sauvegarde : {path_m2}")

# Modele 3 — Temperature
for target, model in best_m3.items():
    path = os.path.join(MODEL_DIR, f"modele3_temp_{target}.joblib")
    joblib.dump(model, path)
    print(f"💾 Sauvegarde : {path}")

# ============================================================
# B. EXPORT DES RÉSULTATS POUR SNOWFLAKE
# ============================================================

all_logs = []
training_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def collect_logs(target_name, res_dict):
    """Extrait les métriques et identifie le meilleur modèle pour un target donné."""
    best_model_name = max(res_dict, key=lambda k: res_dict[k]['r2'])
    for name, metrics in res_dict.items():
        all_logs.append({
            'MODEL_NAME': f"{name}_{target_name}",
            'MAE': metrics['mae'],
            'RMSE': metrics['rmse'],
            'R2': metrics['r2'],
            'TRAINING_DATE': training_date,
            'IS_BEST': (name == best_model_name)
        })

# Collecte pour tous les modèles et targets
for target, res in results_m1.items():
    collect_logs(target, res)

collect_logs('aqi_temps_reel', results_m2)

for target, res in results_m3.items():
    collect_logs(target, res)

# Transformation en DataFrame normé
df_results = pd.DataFrame(all_logs)
df_results.columns = [col.upper() for col in df_results.columns]

print("\n📊 Tableau de performance prêt pour Snowflake :")
print(df_results.to_string(index=False))

# ============================================================
# C. SAUVEGARDE DES PERFORMANCES DANS SNOWFLAKE
# ============================================================

print("\n⏳ Tentative de préparation des données pour Snowflake...")
print(f"DEBUG : Nombre de lignes dans df_results : {len(df_results)}")

if not df_results.empty:
    data_to_insert = [
        (
            row['MODEL_NAME'], 
            float(row['MAE']), 
            float(row['RMSE']), 
            float(row['R2']), 
            str(row['TRAINING_DATE']), 
            row['IS_BEST']
        ) 
        for _, row in df_results.iterrows()
    ]
    
    print(f"DEBUG : {len(data_to_insert)} tuples préparés pour l'insertion.")

    cursor = conn.cursor()
    try:
        insert_query = """
        INSERT INTO GOOD_AIR.LOGS.ML_MODEL_PERFORMANCE
        (MODEL_NAME, MAE, RMSE, R2, TRAINING_DATE, IS_BEST)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print("✅ Données insérées avec succès dans Snowflake.")
    except Exception as e:
        print(f"❌ CRITICAL ERROR lors de l'insertion : {e}")
        conn.rollback()
    finally:
        cursor.close()
else:
    print("⚠️ Attention : df_results est vide, rien à insérer.")

# Fermeture finale de la connexion
conn.close()
print("🔌 Déconnexion Snowflake réussie.")