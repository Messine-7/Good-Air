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

# Définir directement le dossier de travail final
WORKDIR /app/dbt_project

# Copier seulement le projet dbt
COPY dbt_project /app/dbt_project

# Installer dbt Snowflake
RUN pip install --upgrade pip && pip install dbt-core dbt-snowflake==1.10.0
