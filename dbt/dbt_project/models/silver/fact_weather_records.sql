{{ config(
    materialized = "incremental",
    unique_key   = "record_id"
) }}

WITH src AS (

    SELECT
        /* Ville normalisée (clé métier API) */
        UPPER(TRIM(f.value:base_city_name::string)) AS base_city_name,

        /* Time */
        f.value:dt::bigint AS dt,
        TO_TIMESTAMP_NTZ(f.value:dt) AS dt_utc,
        CONVERT_TIMEZONE(
            'UTC',
            'Europe/Paris',
            TO_TIMESTAMP_NTZ(f.value:dt)
        ) AS dt_paris,

        /* Weather */
        f.value:weather[0]:id::int AS weather_id,

        /* Main metrics */
        f.value:main:temp::float        AS temperature,
        f.value:main:feels_like::float  AS feels_like,
        f.value:main:temp_min::float    AS temp_min,
        f.value:main:temp_max::float    AS temp_max,
        f.value:main:pressure::int      AS pressure,
        f.value:main:humidity::int      AS humidity,
        f.value:main:sea_level::int     AS sea_level,

        /* Other metrics */
        f.value:visibility::int         AS visibility,
        f.value:wind:speed::float       AS wind_speed,
        f.value:wind:deg::int           AS wind_deg,
        f.value:rain:"1h"::float        AS rain_1h,
        f.value:rain:"3h"::float        AS rain_3h,
        f.value:clouds:all::int         AS cloudiness,

        /* Sun */
        TO_TIMESTAMP_NTZ(f.value:sys:sunrise) AS sunrise,
        TO_TIMESTAMP_NTZ(f.value:sys:sunset)  AS sunset

    FROM BRONZE.WEATHER_API,
         LATERAL FLATTEN(input => raw_json) f

    WHERE f.value:dt IS NOT NULL
      AND f.value:base_city_name IS NOT NULL
),

joined AS (

    SELECT
        /* Clé primaire FACT (ville + timestamp) */
        md5(src.base_city_name || src.dt) AS record_id,

        /* Time */
        src.dt,
        src.dt_utc,
        src.dt_paris,

        /* Dimensions */
        dc.city_id,
        src.weather_id,

        /* Measures */
        src.temperature,
        src.feels_like,
        src.temp_min,
        src.temp_max,
        src.pressure,
        src.humidity,
        src.sea_level,
        src.visibility,
        src.wind_speed,
        src.wind_deg,
        src.rain_1h,
        src.rain_3h,
        src.cloudiness,
        src.sunrise,
        src.sunset,

        /* 🔥 Déduplication métier */
        ROW_NUMBER() OVER (
            PARTITION BY src.base_city_name, src.dt
            ORDER BY dc.city_id
        ) AS rn

    FROM src
    LEFT JOIN SILVER.DIM_CITY dc
        ON src.base_city_name = dc.city_name
)

SELECT
    record_id,
    dt,
    dt_utc,
    dt_paris,
    city_id,
    weather_id,
    temperature,
    feels_like,
    temp_min,
    temp_max,
    pressure,
    humidity,
    sea_level,
    visibility,
    wind_speed,
    wind_deg,
    rain_1h,
    rain_3h,
    cloudiness,
    sunrise,
    sunset

FROM joined
WHERE rn = 1

{% if is_incremental() %}
  AND record_id NOT IN (
      SELECT record_id FROM {{ this }}
  )
{% endif %}