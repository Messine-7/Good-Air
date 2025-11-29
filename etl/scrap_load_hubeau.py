# %%
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import json
import numpy as np
from datetime import datetime, timezone
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
import zipfile
import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import os
from dotenv import load_dotenv
import time
import locale
import sys
import shutil


# Charger les variables d'environnement
load_dotenv('/app/.env')

ACOUNT_SNOWFLAKE = os.getenv('ACOUNT_SNOWFLAKE')
USER_SNOWFLAKE = os.getenv('USER_SNOWFLAKE')
PASSWORD_SNOWFLAKE = os.getenv('PASSWORD_SNOWFLAKE')

# %%
# Villes françaises avec coordonnées
cities = ['Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice', 'Nantes', 'Montpellier', 
 'Strasbourg', 'Bordeaux', 'Lille', 'Rennes', 'Reims', 'Saint-Étienne', 
 'Le Havre', 'Toulon', 'Grenoble', 'Dijon', 'Angers', 'Nîmes', 'Villeurbanne']

# %%
# Connexion à Snowflake
conn = snowflake.connector.connect(
    user=USER_SNOWFLAKE,
    password=PASSWORD_SNOWFLAKE,
    account=ACOUNT_SNOWFLAKE,  # ex: "abcd-xy12345.europe-west4.gcp"
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="BRONZE"
)

# %%
#récupération de la derniére date de mise à jours de la table hubeau_clean
cur = conn.cursor()

cur.execute("""
    SELECT LAST_ALTERED
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'HUBEAU_CLEAN' 
      AND TABLE_SCHEMA = 'TRANSFORMED'
""")

result = cur.fetchone()

last_date_maj = ""

try :
    last_date_maj = result[0]
    last_date_maj = last_date_maj.replace(tzinfo=None)
    print(f" ************************* date snowflake récupéré {last_date_maj}")
except :
     print('Aucune date de mise a jours récente')


#Pour première utilisation
#last_date_maj = datetime.strptime("2020-09-01 00:00:00", "%Y-%m-%d %H:%M:%S")

# %%
#récupérer la liste des documents a téléchager

# Lancer le navigateur 
user_data_dir = tempfile.mkdtemp()

# chemin vers le dossier download
# Dossier de téléchargement
download_path = "/app/downloads"
os.makedirs(download_path, exist_ok=True)

# Parcourir et supprimer le contenu
for filename in os.listdir(download_path):
    file_path = os.path.join(download_path, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path) # Supprime les fichiers et liens symboliques
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path) # Supprime les sous-dossiers
    except Exception as e:
        print(f"Erreur lors de la suppression de {file_path}. Raison: {e}")

# Options Chromium
chrome_options = Options()
chrome_options.add_argument("--lang=fr-FR")  # Force le navigateur en Français
chrome_options.add_argument("--headless=new")             # Headless moderne
chrome_options.add_argument("--no-sandbox")               # Docker requirement
chrome_options.add_argument("--disable-dev-shm-usage")     # /dev/shm trop petit
chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
chrome_options.add_argument(f"--window-size=1920,1080")
prefs = {"download.default_directory": download_path}
chrome_options.add_experimental_option("prefs", prefs)


# Service pour Chromedriver
service = Service("/usr/bin/chromedriver", log_path="/app/chromedriver.log")
driver = webdriver.Chrome(service=service, options=chrome_options)

# Ouvre l'URL de ta page
driver.get("https://www.data.gouv.fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/")

print(" ************************* connexion a dat.gouv réussi")

# Laisser un peu de temps pour charger la page
time.sleep(3)

# Récupérer tous les éléments avec la classe "space-y-2.5"
elements = driver.find_elements(By.CSS_SELECTOR, "div.border.border-gray-default.overflow-auto")

list_dis =[]

# Définir la locale en français
locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')

# Afficher leur texte
for  el in elements:
    
    dis = el.text
    if "dept" not in dis and ".pdf" not in dis:
        print(f"Texte brut: {dis}")
        
        # 1. Extraction des morceaux (Mois, Jour, Année)
        # Supposons que cela donne : ['November', '3,', '2025']
        parts = dis.split("\n")[1].split(" ")[4:7]
        
        # 2. Nettoyage des virgules sur chaque morceau
        parts = [p.replace(",", "") for p in parts]
        
        # 3. Dictionnaire de traduction (Anglais -> Chiffres)
        mois_anglais = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }

        # 4. Traduction du mois (Le mois semble être en 1ère position : index 0)
        nom_mois = parts[0] # ex: "November"
        
        if nom_mois in mois_anglais:
            parts[0] = mois_anglais[nom_mois] # On remplace "November" par "11"
            print(f"Mois traduit : {nom_mois} -> {parts[0]}")
        
        # 5. Reconstruction de la chaîne : "11/3/2025"
        date_clean = "/".join(parts)
        print(f"Date nettoyée : {date_clean}")

        # 6. Conversion finale
        # Attention : L'ordre est Mois/Jour/Année donc on utilise %m/%d/%Y
        try:
            date_maj = datetime.strptime(date_clean, "%m/%d/%Y")
            print(f"Succès datetime : {date_maj}")
        except ValueError:
            # Sécurité si le format change (ex: Jour en premier)
            print("Tentative inversée (Jour/Mois)...")
            date_maj = datetime.strptime(date_clean, "%d/%m/%Y")

        #vérifie si la date du document à scrapper et plus récente que celle en bdd
        if date_maj > last_date_maj :
            print(dis.split("\n")[0], " " ,date_maj)

            try:
                # trouver le bouton <a> à l'intérieur de la div
                button = el.find_element(By.CSS_SELECTOR, "a.matomo_download")
                
                # Option 1 : cliquer directement
                button.click()
                time.sleep(2)  # petit délai pour le téléchargement

                print(f" ************************* téléchargement réussi {dis}")

                
            except Exception as e:
                print(f"Élément : pas de bouton ou erreur - {e}")
        else :
            print("Dataset déjà à jour")
            driver.quit()

time.sleep(300)
driver.quit()

# %%

list_donwload = os.listdir(download_path)
list_df = []

print(f" ************************* list des documents télécharger {list_donwload }")
for doc in list_donwload :
    print(f" ************************* esaie de dezip de  {doc}")
    if "dis-" in doc and "crdownload" not in doc : 
        
        zip_path = f"{download_path}/{doc}"
        
        # ouvrir le ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # liste des fichiers contenus dans le ZIP
            for file_name in zip_ref.namelist():
                    if "PLV" in file_name :
                        with zip_ref.open(file_name) as f:
                            # adapte le séparateur selon ton fichier (',' ou '\t' ou ';')
                            df = pd.read_csv(f, sep=",")
                            list_df.append(df)
                            print(f" ************************* dezip {doc} sucess")

df_concat = pd.concat(list_df)

print(f" ************************* dezip et concat df sucess")

# Dictionnaire de renommage
rename_dict = {
    'dateprel' : 'date_plv',
    'nomcommuneprinc': 'city_name',
    'plvconformitereferencebact': 'conformite_ref_bact',
    'plvconformitereferencechim' : 'conformite_ref_chim',
    'plvconformitebacterio' : 'conformite_bact',
    'plvconformitechimique' : 'conformite_chim',
    'conclusionprel' :'conclusion_plv',
}

# Renommer les colonnes
df_concat = df_concat.rename(columns=rename_dict)

#reduire le dict au colonne désiré /renomé
df = df_concat[list(rename_dict.values())]

# %%
#clean city_name
def clean_cities(city) :
    if city == 'NIMES' :
         return 'Nîmes'
    if city == 'SAINT-ETIENNE' :
         return 'Saint-Étienne'
    if city == 'HAVRE (LE)' :
         return 'Le Havre'
    else :
         return city

def corel_city(row_city)   :
     for city in cities :
          if row_city.lower() in city.lower() :
               return city
          

df["city_name"] = df["city_name"].apply(clean_cities)
df["city_name"] = df["city_name"].apply(corel_city)

# Filtrer le DataFrame
df_reduce = df[df["city_name"].isin(cities)]
df_reduce["city_name"] = df_reduce["city_name"].str.upper()
df_reduce.reset_index(drop=True)

# %%
#transformer les nom de colonne en majusucle pour Snowflake 
df_reduce.columns = [c.upper() for c in df.columns]

#récupérer les id des city depuis snowflake
query  = """SELECT CITY_NAME, CITY_ID FROM GOOD_AIR.SILVER.DIM_CITY """

try:
    df_dim_city = pd.read_sql(query, conn)
    print(f"Données chargées : {df_dim_city.shape[0]} lignes.")
except :
    print("Echec de récupérationde dim_city")

df_merge = pd.merge(df_reduce, df_dim_city, how ="left", on="CITY_NAME") 
df_merge.drop(columns= ["CITY_NAME"], inplace = True)

print(f" ************************* clean data sucess")
# %%

# Charger le DataFrame vers une table (la table doit exister)
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df_merge,
    table_name="FACT_HUBEAU_RECORDS", # Juste le nom de la table
    database="GOOD_AIR",              # La base de données
    schema="SILVER",                  # Le schéma
    quote_identifiers=False           # Souvent utile pour éviter les erreurs de casse
)

print(f"Upload terminé : {success}, {nrows} lignes insérées.")

# ===============================================================
# ✅ Envois log Snowflake
# ===============================================================
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
    params = ("API_SILVER", "FACT_HUBEAU_RECORDS", len(df_merge), 0, "SUCCESS")

    cur.execute(sql_query, params)
    conn.commit()

except Exception as e:
    print("❌ Erreur lors de l'insertion logs :", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()