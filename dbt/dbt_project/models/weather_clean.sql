{{ config(materialized='table') }}

WITH raw AS (
    SELECT raw_json AS data
    FROM RAW.WEATHER_API
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
        -- index / temps
        city_data:dt::bigint AS dt,
        TO_TIMESTAMP_NTZ(city_data:dt) AS dt_utc,
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', TO_TIMESTAMP_NTZ(city_data:dt)) AS dt_utc_plus2,

        -- coordonnées
        city_data:coord:lat::float AS latitude,
        city_data:coord:lon::float AS longitude,

        -- météo principale
        city_data:weather[0]:id::int AS weather_id,
        city_data:weather[0]:main::string AS weather_main,
        city_data:weather[0]:description::string AS weather_description,
        city_data:weather[0]:icon::string AS weather_icon,

        -- mesures atmosphériques
        city_data:main:temp::float AS temperature,
        city_data:main:feels_like::float AS feels_like,
        city_data:main:temp_min::float AS temp_min,
        city_data:main:temp_max::float AS temp_max,
        city_data:main:pressure::int AS pressure,
        city_data:main:humidity::int AS humidity,
        city_data:main:sea_level::int AS sea_level,
        city_data:main:grnd_level::int AS ground_level,

        -- visibilité
        city_data:visibility::int AS visibility,

        -- vent
        city_data:wind:speed::float AS wind_speed,
        city_data:wind:deg::int AS wind_deg,

        -- pluie (optionnel)
        city_data:rain:"1h"::float AS rain_1h,
        city_data:rain:"3h"::float AS rain_3h,

        -- nuages
        city_data:clouds:all::int AS cloudiness,

        -- infos système
        city_data:sys:type::int AS sys_type,
        city_data:sys:id::int AS sys_id,
        city_data:sys:country::string AS country,
        TO_TIMESTAMP_NTZ(city_data:sys:sunrise) AS sunrise,
        TO_TIMESTAMP_NTZ(city_data:sys:sunset) AS sunset,

        -- timezone
        city_data:timezone::int AS timezone_offset,

        -- identifiants
        city_data:id::int AS city_id,
        city_data:name::string AS city_name,
        city_data:cod::int AS status_code,

        -- métadonnées
        city_data:base::string AS base_source
    FROM flattened_array
)

SELECT * FROM flattened
