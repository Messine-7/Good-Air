{{ config(
    materialized="incremental",
    unique_key="record_id"
) }}

WITH src AS (
    SELECT
        f.value:name::string AS city_name,

        f.value:dt::bigint AS dt,
        TO_TIMESTAMP_NTZ(f.value:dt) AS dt_utc,
        CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(f.value:dt)) AS dt_paris,

        f.value:coord:lat::float AS latitude,
        f.value:coord:lon::float AS longitude,

        f.value:weather[0]:id::int AS weather_id,

        f.value:main:temp::float AS temperature,
        f.value:main:feels_like::float AS feels_like,
        f.value:main:temp_min::float AS temp_min,
        f.value:main:temp_max::float AS temp_max,
        f.value:main:pressure::int AS pressure,
        f.value:main:humidity::int AS humidity,
        f.value:main:sea_level::int AS sea_level,
        f.value:main:grnd_level::int AS ground_level,

        f.value:visibility::int AS visibility,

        f.value:wind:speed::float AS wind_speed,
        f.value:wind:deg::int AS wind_deg,

        f.value:rain:"1h"::float AS rain_1h,
        f.value:rain:"3h"::float AS rain_3h,

        f.value:clouds:all::int AS cloudiness,

        f.value:sys:country::string AS country,
        TO_TIMESTAMP_NTZ(f.value:sys:sunrise) AS sunrise,
        TO_TIMESTAMP_NTZ(f.value:sys:sunset) AS sunset
    FROM BRONZE.WEATHER_API,
         LATERAL FLATTEN(input => raw_json) f
)

SELECT
    /* Clé primaire stable */
    md5(src.city_name || src.dt) AS record_id,

    /* Time */
    src.dt,
    src.dt_utc,
    src.dt_paris,

    /* Dimensions */
    -- UPPER(TRIM(src.city_name)) AS city_id,
    dc.city_id,
    src.weather_id,

    /* Measures */
    src.latitude,
    src.longitude,
    src.temperature,
    src.feels_like,
    src.temp_min,
    src.temp_max,
    src.pressure,
    src.humidity,
    src.sea_level,
    src.ground_level,
    src.visibility,

    src.wind_speed,
    src.wind_deg,
    src.rain_1h,
    src.rain_3h,
    src.cloudiness,

    src.country,
    src.sunrise,
    src.sunset

FROM src
LEFT JOIN SILVER.DIM_CITY dc
    ON UPPER(TRIM(src.city_name)) = dc.city_name

{% if is_incremental() %}
WHERE md5(src.city_name || src.dt) NOT IN (SELECT record_id FROM {{ this }})
{% endif %}
