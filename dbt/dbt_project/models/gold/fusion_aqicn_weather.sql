{{ config(
    materialized='incremental',
    unique_key=['CITY_ID', 'DATE_H'] 
) }}

WITH 
-- 1. Calcul de la moyenne par ville et par heure (Table A)
AVG_DATA AS (
    SELECT 
        CITY_ID,
        DATE_TRUNC('HOUR', DT_UTC) as DATE_H, -- On tronque à l'heure (ex: 14:23 -> 14:00)
        AVG(TEMPERATURE) as TEMPERATURE
    FROM GOOD_AIR.SILVER.FACT_WEATHER_RECORDS
    GROUP BY CITY_ID, DATE_TRUNC('HOUR', DT_UTC)
),

-- 2. Récupération de la donnée la plus récente par ville et par heure (Table B)
LATEST_DATA AS (
    SELECT 
        CITY_ID,
        DATE_TRUNC('HOUR', DT_UTC) as DATE_H,
        AVG(AQI) as AQI
    FROM GOOD_AIR.SILVER.FACT_AIR_QUALITY_RECORDS
    GROUP BY CITY_ID, DATE_TRUNC('HOUR', DT_UTC)
)

-- 3. Jointure finale
SELECT 
    A.CITY_ID,
    A.DATE_H,
    A.TEMPERATURE,
    B.AQI,
    DC.CITY_NAME
FROM AVG_DATA A
JOIN LATEST_DATA B 
   ON A.CITY_ID = B.CITY_ID
   AND A.DATE_H = B.DATE_H
LEFT JOIN GOOD_AIR.SILVER.DIM_CITY DC ON DC.CITY_ID  = A.CITY_ID