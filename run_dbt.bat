@echo off
docker run --rm -v "d:\DATA\2025-11-28_MSPR-1_2\Good-Air\dbt\dbt_project:/app/dbt_project" -v "d:\DATA\2025-11-28_MSPR-1_2\Good-Air\dbt:/root/.dbt" --env-file "d:\DATA\2025-11-28_MSPR-1_2\Good-Air\.env" --network good-air_data-pipeline ghcr.io/dbt-labs/dbt-snowflake:latest run --project-dir /app/dbt_project --profiles-dir /root/.dbt --select fusion_aqicn_weather --full-refresh
