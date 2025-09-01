FROM python:3.11-slim

# Dépendances système
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copier seulement le projet dbt
COPY dbt_project /app/dbt_project

# Installer dbt Snowflake
RUN pip install --upgrade pip && pip install dbt-snowflake==1.10.10

WORKDIR /app/dbt_project