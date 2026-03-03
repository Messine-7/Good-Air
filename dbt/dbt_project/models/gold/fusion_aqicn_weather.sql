{{
  config(
    materialized='incremental',
    unique_key=['CITY_ID', 'DT_HOUR'],
    incremental_strategy='merge'
  )
}}

WITH BASE_DATA AS (
    SELECT 
        COALESCE(w.CITY_ID, a.CITY_ID) as CITY_ID,
        COALESCE(DATE_TRUNC('hour', w.dt_paris), a.dt_paris) as DT_HOUR,
        w.TEMPERATURE,
        w.HUMIDITY,
        w.WIND_SPEED,
        W.VISIBILITY,
        a.IAQI_PM10,
        a.IAQI_PM25,
        a.IAQI_PRESSURE,
        a.IAQI_O3,
        a.AQI,
        (a.AQI * 0.7) + 
        (CASE WHEN w.TEMPERATURE < 5 THEN 20 WHEN w.TEMPERATURE > 32 THEN 15 ELSE 0 END) + 
        (CASE WHEN w.HUMIDITY > 80 THEN 10 ELSE 0 END) as CURRENT_HEALTH_RISK_SCORE
    FROM SILVER.FACT_WEATHER_RECORDS w
    FULL OUTER JOIN SILVER.FACT_AIR_QUALITY_RECORDS a 
        ON w.city_id = a.city_id 
        AND DATE_TRUNC('hour', w.dt_paris) = a.dt_paris

    {% if is_incremental() %}
    WHERE w.dt_paris >= (
        SELECT COALESCE(DATEADD('hour', -72, MAX(DT_HOUR)), '2020-01-01') 
        FROM {{ this }}
    )
    {% endif %}
),

OFFSETS AS (
    -- Liste des décalages cibles en heures
    SELECT column1 as h FROM (VALUES (-48), (-24), (-12), (12), (24), (48))
),

POTENTIAL_MATCHES AS (
    SELECT 
        curr.CITY_ID,
        curr.DT_HOUR,
        o.h as target_offset,
        other.CURRENT_HEALTH_RISK_SCORE as match_risk,
        other.TEMPERATURE as match_temp,
        other.HUMIDITY as match_hum,
        other.WIND_SPEED as match_wind,
        other.VISIBILITY as match_visibility,
        other.IAQI_PM10 as match_PM10,
        other.IAQI_PM25 as match_PM25,
        other.IAQI_PRESSURE as match_pressure,
        other.IAQI_O3 as match_O3,
        other.AQI as match_aqi,
        ABS(DATEDIFF('minute', other.DT_HOUR, DATEADD('hour', o.h, curr.DT_HOUR))) as gap_minutes,
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

SELECT 
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
    b.AQI,
    b.CURRENT_HEALTH_RISK_SCORE,

    -- ==========================================
    -- LAGS (Le passé : Features)
    -- ==========================================
    -- 12H BACK
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_risk END) as RISK_SCORE_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_temp END) as TEMP_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_hum END) as HUM_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_wind END) as WIND_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_visibility END) as VISIBILITY_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_PM10 END) as PM10_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_PM25 END) as PM25_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_pressure END) as PRESSURE_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_O3 END) as O3_LAG_12,
    MAX(CASE WHEN p.target_offset = -12 THEN p.match_aqi END) as AQI_LAG_12,

    -- 24H BACK
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_risk END) as RISK_SCORE_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_temp END) as TEMP_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_hum END) as HUM_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_wind END) as WIND_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_visibility END) as VISIBILITY_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_PM10 END) as PM10_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_PM25 END) as PM25_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_pressure END) as PRESSURE_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_O3 END) as O3_LAG_24,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_aqi END) as AQI_LAG_24,

    -- 48H BACK
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_risk END) as RISK_SCORE_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_temp END) as TEMP_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_hum END) as HUM_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_wind END) as WIND_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_visibility END) as VISIBILITY_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_PM10 END) as PM10_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_PM25 END) as PM25_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_pressure END) as PRESSURE_LAG_48,
    MAX(CASE WHEN p.target_offset = -24 THEN p.match_O3 END) as O3_LAG_48,
    MAX(CASE WHEN p.target_offset = -48 THEN p.match_aqi END) as AQI_LAG_48,

FROM BASE_DATA b
LEFT JOIN POTENTIAL_MATCHES p 
    ON b.CITY_ID = p.CITY_ID 
    AND b.DT_HOUR = p.DT_HOUR 
    AND p.proximity_rank = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY CITY_ID, DT_HOUR