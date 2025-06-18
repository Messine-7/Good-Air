# elt/script.py

import requests
import json

API_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
LAT = "48.17264750507201"
LON = "6.452813643643765"
APPID = "dd2d8c2c92a3ebe773b453884a7f08c0"  # Remplace par ta vraie clé API

def extract_air_pollution():
    params = {
        "lat": LAT,
        "lon": LON,
        "appid": APPID
    }

    print("🔄 Appel API OpenWeather en cours...")
    response = requests.get(API_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        # with open("/app/air_pollution.json", "w") as f:
        #     json.dump(data, f, indent=2)
        # print("✅ Données de pollution sauvegardées.")
        print(data)
    else:
        print(f"❌ Échec de l'appel API: {response.status_code} - {response.text}")

if __name__ == "__main__":
    extract_air_pollution()
