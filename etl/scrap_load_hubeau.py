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

import time
import locale

# %%
# Villes françaises avec coordonnées
cities = ['Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice', 'Nantes', 'Montpellier', 
 'Strasbourg', 'Bordeaux', 'Lille', 'Rennes', 'Reims', 'Saint-Étienne', 
 'Le Havre', 'Toulon', 'Grenoble', 'Dijon', 'Angers', 'Nîmes', 'Villeurbanne']

# %%
# Connexion à Snowflake
conn = snowflake.connector.connect(
    user="LouK",
    password="Snowflake_08230!",
    account="ZQQYYBI-EM82872",  # ex: "abcd-xy12345.europe-west4.gcp"
    warehouse="COMPUTE_WH",
    database="GOOD_AIR",
    schema="TRANSFORMED"
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

last_date_maj = result[0]
last_date_maj = last_date_maj.replace(tzinfo=None)
#A supp lors du déploiement
last_date_maj = datetime.strptime("2020-09-01 00:00:00", "%Y-%m-%d %H:%M:%S")


print(f" ************************* date snowflake récupéré {last_date_maj}")
# %%
#récupérer la liste des documents a téléchager

# Lancer le navigateur 
user_data_dir = tempfile.mkdtemp()

# chemin vers le dossier download
# Dossier de téléchargement
download_path = "/app/downloads"
os.makedirs(download_path, exist_ok=True)


# Options Chromium
chrome_options = Options()
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
    if "dept" not in dis and ".pdf" not in dis :
        #extrait les dates de mis a jour des balise et les convertis en datetime
        date_maj = dis.split("\n")[1].split(" ")[4:7]
        date_maj ="/".join(date_maj)
        
        """if "û" in date_maj :
            date_maj = date_maj.replace("û","Ã»")"""

        date_maj = datetime.strptime(date_maj, "%d/%B/%Y")

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

print(f" ************************* list des documents téléarchger {list_donwload }")
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
df_reduce.reset_index(drop=True)

# %%
#transformer les nom de colonne en majusucle pour Snowflake 
df_reduce.columns = [c.upper() for c in df.columns]

print(f" ************************* clean data sucess")
# %%

# Charger le DataFrame vers une table (la table doit exister)
success, nchunks, nrows, _ = write_pandas(conn, df_reduce, "HUBEAU_CLEAN")

print(f"Upload terminé : {success}, {nrows} lignes insérées.")
