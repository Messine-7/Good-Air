{{ config(materialized='table') }}

WITH raw AS (
    SELECT raw_json AS data
    FROM RAW.WEATHER_API
),

flattened_array AS (
    SELECT
        f.value AS city_data
    FROM raw,
    LATERAL FLATTEN(input => data) f
),

flattened AS (
    SELECT
        city_data:dt::bigint AS dt,
        TO_TIMESTAMP_NTZ(city_data:dt) AS dt_utc,
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', TO_TIMESTAMP_NTZ(city_data:dt)) AS dt_utc_plus2,
        city_data:coord:lat::float AS latitude,
        city_data:coord:lon::float AS longitude,
        city_data:weather[0]:id::int AS weather_id,
        city_data:weather[0]:main::string AS weather_main,
        city_data:weather[0]:description::string AS weather_description,
        city_data:weather[0]:icon::string AS weather_icon,
        city_data:main:temp::float AS temperature,
        city_data:main:feels_like::float AS feels_like,
        city_data:main:temp_min::float AS temp_min,
        city_data:main:temp_max::float AS temp_max,
        city_data:main:pressure::int AS pressure,
        city_data:main:humidity::int AS humidity,
        city_data:main:sea_level::int AS sea_level,
        city_data:main:grnd_level::int AS ground_level,
        city_data:visibility::int AS visibility,
        city_data:wind:speed::float AS wind_speed,
        city_data:wind:deg::int AS wind_deg,
        city_data:rain:"1h"::float AS rain_1h,
        city_data:rain:"3h"::float AS rain_3h,
        city_data:clouds:all::int AS cloudiness,
        city_data:sys:type::int AS sys_type,
        city_data:sys:id::int AS sys_id,
        city_data:sys:country::string AS country,
        TO_TIMESTAMP_NTZ(city_data:sys:sunrise) AS sunrise,
        TO_TIMESTAMP_NTZ(city_data:sys:sunset) AS sunset,
        city_data:timezone::int AS timezone_offset,
        city_data:id::int AS city_id,
        city_data:name::string AS city_name,
        city_data:cod::int AS status_code,
        city_data:base::string AS base_source,
        ROW_NUMBER() OVER(PARTITION BY city_data:id, city_data:dt ORDER BY city_data:dt DESC) AS rn
    FROM flattened_array
)

SELECT
    dt,
    dt_utc,
    dt_utc_plus2,
    latitude,
    longitude,
    weather_id,
    weather_main,
    weather_description,
    weather_icon,
    temperature,
    feels_like,
    temp_min,
    temp_max,
    pressure,
    humidity,
    sea_level,
    ground_level,
    visibility,
    wind_speed,
    wind_deg,
    rain_1h,
    rain_3h,
    cloudiness,
    sys_type,
    sys_id,
    country,
    sunrise,
    sunset,
    timezone_offset,
    city_id,
    city_name,
    status_code,
    base_source
FROM flattened
WHERE rn = 1
