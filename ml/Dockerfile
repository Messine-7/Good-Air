FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système pour le connecteur Snowflake et sklearn
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du script
COPY ml-train.py .

# Création du dossier de sortie pour le modèle
RUN mkdir -p /app/models

# Lancement du script
CMD ["python", "ml-train.py"]