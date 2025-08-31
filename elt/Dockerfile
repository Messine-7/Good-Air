# Base Python
FROM python:3.11-slim

# Dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    gcc \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Création du dossier de travail
WORKDIR /app

# Copie des fichiers de dépendances et scripts
COPY requirements.txt /app/requirements.txt
COPY . /app

# Installation des packages Python
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Commande par défaut (peut être remplacée par Airflow DockerOperator)
CMD ["python", "script.py"]
