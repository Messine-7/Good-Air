FROM python:3.9-slim

# Ajout des dépendances système requises pour compiler et installer certains paquets
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    gcc \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*\
    pip install -r requirements.txt

# Création du dossier pour le script
WORKDIR /app

# Copie du script
COPY ./script.py /app/script.py

# Installation des packages Python
RUN pip install --upgrade pip \
    && pip install pandas snowflake-connector-python

# Commande par défaut
CMD ["python", "script.py"]