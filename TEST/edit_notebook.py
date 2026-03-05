import json

notebook_path = r"d:\DATA\2025-11-28_MSPR-1_2\Good-Air\etl\trans_hisotrique_aqi.ipynb"

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find the cell that calls `df = interpoler_aqi(df, cols)`
target_idx = -1
for i, cell in enumerate(nb["cells"]):
    if cell["cell_type"] == "code" and "df = interpoler_aqi(df, cols)" in "".join(cell["source"]):
        target_idx = i
        break

if target_idx != -1:
    new_cell = {
        "cell_type": "code",
        "execution_count": None,
        "id": "add_lags_pandas",
        "metadata": {},
        "outputs": [],
        "source": [
            "# --- AJOUT DES LAGS -24H ET -48H ---\n",
            "print(\"Calcul des lags -24h et -48h...\")\n",
            "df_lags = df[['CITY_ID', 'DT_PARIS', 'IAQI_PM10', 'IAQI_PM25', 'IAQI_O3', 'AQI']].copy()\n",
            "df['DT_PARIS'] = pd.to_datetime(df['DT_PARIS'])\n",
            "df_lags['DT_PARIS'] = pd.to_datetime(df_lags['DT_PARIS'])\n",
            "\n",
            "# On décale les dates de +24h et +48h pour le merge (une ligne d'il y a 24h apparaitra avec la date d'aujourd'hui)\n",
            "df_24 = df_lags.copy()\n",
            "df_24['DT_PARIS'] = df_24['DT_PARIS'] + pd.Timedelta(hours=24)\n",
            "df_24 = df_24.rename(columns={'IAQI_PM10': 'PM10_LAG_24', 'IAQI_PM25': 'PM25_LAG_24', 'IAQI_O3': 'O3_LAG_24', 'AQI': 'AQI_LAG_24'})\n",
            "\n",
            "df_48 = df_lags.copy()\n",
            "df_48['DT_PARIS'] = df_48['DT_PARIS'] + pd.Timedelta(hours=48)\n",
            "df_48 = df_48.rename(columns={'IAQI_PM10': 'PM10_LAG_48', 'IAQI_PM25': 'PM25_LAG_48', 'IAQI_O3': 'O3_LAG_48', 'AQI': 'AQI_LAG_48'})\n",
            "\n",
            "# Jointure\n",
            "df = df.merge(df_24, on=['CITY_ID', 'DT_PARIS'], how='left')\n",
            "df = df.merge(df_48, on=['CITY_ID', 'DT_PARIS'], how='left')\n",
            "\n",
            "# Les valeurs sans correspondances passées auront NaN, on nettoie potentiellement (ici on met -999 ou on laisse NaN)\n",
            "df[['PM10_LAG_24', 'PM25_LAG_24', 'O3_LAG_24', 'AQI_LAG_24', \n",
            "    'PM10_LAG_48', 'PM25_LAG_48', 'O3_LAG_48', 'AQI_LAG_48']] = df[['PM10_LAG_24', 'PM25_LAG_24', 'O3_LAG_24', 'AQI_LAG_24', \n",
            "    'PM10_LAG_48', 'PM25_LAG_48', 'O3_LAG_48', 'AQI_LAG_48']].fillna(-999)\n"
        ]
    }
    
    # Insert new cell right after interpoler_aqi cell
    nb["cells"].insert(target_idx + 1, new_cell)
    
    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)
        
    print("Notebook updated successfully with lags.")
else:
    print("Could not find the target cell.")
