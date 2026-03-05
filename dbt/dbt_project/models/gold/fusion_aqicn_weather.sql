{{
  config(
    materialized='incremental',
    unique_key=['CITY_ID', 'DT_HOUR'],
    incremental_strategy='merge'
  )
}}

WITH RAW_AIR_DATA AS (
    -- 1. Données temps réel (SILVER FACT)
    SELECT 
        CITY_ID,
        DATE_TRUNC('hour', dt_paris) as DT_HOUR,
        RECORD_ID,
        IAQI_PM10,
        IAQI_PM25,
        IAQI_PRESSURE,
        IAQI_NO2,
        IAQI_O3,
        AQI
    FROM SILVER.FACT_AIR_QUALITY_RECORDS

    UNION ALL

    -- 2. Données Historiques (AQI_HISTORIQUE)
    -- On aligne les colonnes en mettant NULL là où la donnée météo/gaz manque
    SELECT 
        CITY_ID,
        DATE_TRUNC('hour', DT_PARIS) as DT_HOUR,
        RECORD_ID,
        IAQI_PM10,
        IAQI_PM25,
        NULL as IAQI_PRESSURE, 
        NULL as IAQI_NO2,      
        IAQI_O3,
        AQI
    FROM GOOD_AIR.SILVER.AQI_HISTORIQUE
),

BASE_DATA AS (
    -- 3. Jointure avec la météo et calcul du score de risque
    SELECT 
        MD5(COALESCE(w.RECORD_ID, 'W') || '_' || COALESCE(a.RECORD_ID, 'A')) as RECORD_ID,
        a.CITY_ID,
        a.DT_HOUR,
        w.TEMPERATURE,
        w.HUMIDITY,
        w.WIND_SPEED,
        w.VISIBILITY,
        a.IAQI_PM10,
        a.IAQI_PM25,
        a.IAQI_PRESSURE,
        a.IAQI_NO2,
        a.IAQI_O3,
        a.AQI,
        -- Calcul du risque (COALESCE pour gérer l'absence de météo dans l'historique)
        (COALESCE(a.AQI, 0) * 0.7) + 
        (CASE WHEN w.TEMPERATURE < 5 THEN 20 WHEN w.TEMPERATURE > 32 THEN 15 ELSE 0 END) + 
        (CASE WHEN w.HUMIDITY > 80 THEN 10 ELSE 0 END) as CURRENT_HEALTH_RISK_SCORE
    FROM RAW_AIR_DATA a
    LEFT JOIN SILVER.FACT_WEATHER_RECORDS w 
        ON a.CITY_ID = w.CITY_ID 
        AND a.DT_HOUR = DATE_TRUNC('hour', w.dt_paris)

    {% if is_incremental() %}
    -- On ne traite que les 3 derniers jours pour l'incrémental (ajustable)
    WHERE a.DT_HOUR >= (
        SELECT COALESCE(DATEADD('hour', -72, MAX(DT_HOUR)), '2020-01-01') 
        FROM {{ this }}
    )
    {% endif %}
),

OFFSETS AS (
    -- Définition des fenêtres temporelles pour les lags
    SELECT column1 as h FROM (VALUES (-48), (-24), (-12))
),

POTENTIAL_MATCHES AS (
    -- 4. Recherche des correspondances pour les décalages temporels
    SELECT 
        curr.CITY_ID,
        curr.DT_HOUR,
        o.h as target_offset,
        other.CURRENT_HEALTH_RISK_SCORE as match_risk,
        other.TEMPERATURE as match_temp,
        other.IAQI_PM10 as match_PM10,
        other.IAQI_PM25 as match_PM25,
        other.IAQI_O3 as match_O3,
        other.AQI as match_aqi,
        ROW_NUMBER() OVER (
            PARTITION BY curr.CITY_ID, curr.DT_HOUR, o.h 
            ORDER BY ABS(DATEDIFF('minute', other.DT_HOUR, DATEADD('hour', o.h, curr.DT_HOUR))) ASC
        ) as proximity_rank
    FROM BASE_DATA curr
    CROSS JOIN OFFSETS o
    LEFT JOIN BASE_DATA other
        ON curr.CITY_ID = other.CITY_ID
        AND other.DT_HOUR BETWEEN DATEADD(hour, o.h - 6, curr.DT_HOUR) 
                             AND DATEADD(hour, o.h + 6, curr.DT_HOUR)
)

-- 5. Final : Pivot des résultats pour obtenir les colonnes LAG
SELECT 
    b.RECORD_ID,
    b.CITY_ID,
    b.DT_HOUR,
    b.TEMPERATURE,
    b.HUMIDITY,
    b.WIND_SPEED,
    b.VISIBILITY,
    b.IAQI_PM10,
    b.IAQI_PM25,
    b.IAQI_PRESSURE,
    b.IAQI_O3,
    b.IAQI_NO2,
    b.AQI,
    b.CURRENT_HEALTH_RISK_SCORE,

    -- LAG 12H
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_risk END) as RISK_SCORE_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_temp END) as TEMP_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_PM10 END) as PM10_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_PM25 END) as PM25_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_O3 END) as O3_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_aqi END) as AQI_LAG_12,

    -- LAG 24H
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_risk END) as RISK_SCORE_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_temp END) as TEMP_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_PM10 END) as PM10_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_PM25 END) as PM25_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_O3 END) as O3_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_aqi END) as AQI_LAG_24,

    -- LAG 48H
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_risk END) as RISK_SCORE_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_temp END) as TEMP_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_PM10 END) as PM10_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_PM25 END) as PM25_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_O3 END) as O3_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_aqi END) as AQI_LAG_48

FROM BASE_DATA b
LEFT JOIN POTENTIAL_MATCHES p 
    ON b.CITY_ID = p.CITY_ID 
    AND b.DT_HOUR = p.DT_HOUR 
    AND p.proximity_rank = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY CITY_ID, DT_HOUR