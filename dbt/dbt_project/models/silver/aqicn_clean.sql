{{ config(
    materialized='table'
) }}

WITH raw AS (
    -- Parse la colonne JSON et accède au tableau racine
    SELECT
        PARSE_JSON(raw_json) AS json_array
    FROM RAW.AQICN_API
),

-- Chaque ligne du tableau correspond à une ville
flattened_array AS (
    SELECT
        city_element.value AS city_data
    FROM raw,
    LATERAL FLATTEN(input => json_array) city_element
),

-- Accède au vrai contenu "raw_json" interne
flattened AS (
    SELECT
        -- Identifiant station
        TRY_TO_NUMBER(city_data:raw_json:data:idx::string) AS station_id,

        -- Index temps
        TRY_TO_NUMBER(city_data:raw_json:data:time:v::string) AS dt,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:raw_json:data:time:v::string)) AS dt_utc,
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:raw_json:data:time:v::string))) AS dt_paris,

        -- Métadonnées principales
        city_data:raw_json:status::string AS status,
        TRY_TO_NUMBER(city_data:raw_json:data:aqi::string) AS aqi,
        city_data:raw_json:data:dominentpol::string AS dominent_pol,

        -- Ville
        city_data:city::string AS city_name,
        city_data:raw_json:data:city:name::string AS station_name,
        city_data:raw_json:data:city:url::string AS city_url,
        TRY_TO_NUMBER(city_data:raw_json:data:city:geo[0]::string) AS lat,
        TRY_TO_NUMBER(city_data:raw_json:data:city:geo[1]::string) AS lon,

        -- Indicateurs IAQI
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:co:v::string) AS iaqi_co,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:no2:v::string) AS iaqi_no2,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:o3:v::string) AS iaqi_o3,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:pm10:v::string) AS iaqi_pm10,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:pm25:v::string) AS iaqi_pm25,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:so2:v::string) AS iaqi_so2,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:t:v::string) AS iaqi_temp,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:h:v::string) AS iaqi_humidity,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:p:v::string) AS iaqi_pressure,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:w:v::string) AS iaqi_wind,
        TRY_TO_NUMBER(city_data:raw_json:data:iaqi:wg:v::string) AS iaqi_wind_gust,

        -- Temps
        city_data:raw_json:data:time:s::string AS time_str,
        city_data:raw_json:data:time:tz::string AS tz_offset,
        city_data:raw_json:data:time:iso::string AS time_iso,
        city_data:timestamp_utc::string AS timestamp_utc

    FROM flattened_array
)

SELECT *
FROM flattened
QUALIFY ROW_NUMBER() OVER (PARTITION BY station_id, dt ORDER BY dt_utc DESC) = 1