{{ config(materialized='table') }}

WITH raw AS (
    SELECT raw_json AS data
    FROM RAW.AQICN_API
),

flattened_array AS (
    -- flatten chaque élément du tableau JSON
    SELECT
        f.value AS city_data
    FROM raw,
    LATERAL FLATTEN(input => data) f
),

flattened AS (
    SELECT
        -- identifiant unique de la station
        TRY_TO_NUMBER(city_data:data:idx::string) AS station_id,

        -- index / temps
        TRY_TO_NUMBER(city_data:data:time:v::string) AS dt,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:data:time:v::string)) AS dt_utc,
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:data:time:v::string))) AS dt_paris,

        -- métadonnées principales
        city_data:status::string AS status,
        TRY_TO_NUMBER(city_data:data:aqi::string) AS aqi,
        city_data:data:dominentpol::string AS dominent_pol,

        -- ville
        city_data:data:city:name::string AS city_name,
        city_data:data:city:url::string AS city_url,
        city_data:data:city:location::string AS city_location,
        TRY_TO_NUMBER(city_data:data:city:geo[0]::string) AS lat,
        TRY_TO_NUMBER(city_data:data:city:geo[1]::string) AS lon,

        -- indicateurs (IAQI)
        TRY_TO_NUMBER(city_data:data:iaqi:co:v::string) AS iaqi_co,
        TRY_TO_NUMBER(city_data:data:iaqi:no2:v::string) AS iaqi_no2,
        TRY_TO_NUMBER(city_data:data:iaqi:o3:v::string) AS iaqi_o3,
        TRY_TO_NUMBER(city_data:data:iaqi:pm10:v::string) AS iaqi_pm10,
        TRY_TO_NUMBER(city_data:data:iaqi:pm25:v::string) AS iaqi_pm25,
        TRY_TO_NUMBER(city_data:data:iaqi:so2:v::string) AS iaqi_so2,
        TRY_TO_NUMBER(city_data:data:iaqi:t:v::string) AS iaqi_temp,
        TRY_TO_NUMBER(city_data:data:iaqi:h:v::string) AS iaqi_humidity,
        TRY_TO_NUMBER(city_data:data:iaqi:p:v::string) AS iaqi_pressure,
        TRY_TO_NUMBER(city_data:data:iaqi:w:v::string) AS iaqi_wind,
        TRY_TO_NUMBER(city_data:data:iaqi:wg:v::string) AS iaqi_wind_gust,

        -- infos horaires
        city_data:data:time:s::string AS time_str,
        city_data:data:time:tz::string AS tz_offset,
        city_data:data:time:iso::string AS time_iso

    FROM flattened_array
)

SELECT *
FROM flattened
QUALIFY ROW_NUMBER() OVER (PARTITION BY station_id, dt ORDER BY dt_utc DESC) = 1

{% if is_incremental() %}
-- Ne charge que les nouvelles mesures
WHERE dt NOT IN (
    SELECT DISTINCT dt
    FROM {{ this }}
    WHERE station_id = flattened.station_id
)
{% endif %}
