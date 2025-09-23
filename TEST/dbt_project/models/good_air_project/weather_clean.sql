{{ config(materialized='table') }}

WITH raw AS (
    SELECT raw_json AS data
    FROM RAW.WEATHER_API
),

-- 1. Infos principales
city AS (
    SELECT
        r.data:idx::int AS idx,
        r.data:city.name::string AS city_name,
        r.data:city.url::string AS city_url,
        r.data:city.geo[0]::float AS latitude,
        r.data:city.geo[1]::float AS longitude,
        r.data:aqi::int AS aqi,
        r.data:dominentpol::string AS dominent_pol,
        r.data:time.iso::timestamp_tz AS measurement_time
    FROM raw r
),

-- 2. Mesures instantanées (iaqi)
iaqi AS (
    SELECT
        r.data:idx::int AS idx,
        k.key::string AS pollutant,
        k.value::float AS value
    FROM raw r,
    LATERAL FLATTEN(input => r.data:iaqi) k
),

-- 3. Prévisions (forecast.daily)
forecast AS (
    SELECT
        r.data:idx::int AS idx,
        p.key::string AS pollutant,
        f.value:day::date AS jour,
        f.value:avg::int AS avg_value,
        f.value:max::int AS max_value,
        f.value:min::int AS min_value
    FROM raw r,
    LATERAL FLATTEN(input => r.data:forecast.daily) p,
    LATERAL FLATTEN(input => p.value) f
),

-- 4. Attributions (sources)
attributions AS (
    SELECT
        r.data:idx::int AS idx,
        a.value:name::string AS source_name,
        a.value:url::string AS source_url,
        a.value:logo::string AS source_logo
    FROM raw r,
    LATERAL FLATTEN(input => r.data:attributions) a
)

-- Final : une seule table à plat
SELECT
    c.idx,
    c.city_name,
    c.city_url,
    c.latitude,
    c.longitude,
    c.aqi,
    c.dominent_pol,
    c.measurement_time,
    i.pollutant AS iaqi_pollutant,
    i.value AS iaqi_value,
    f.pollutant AS forecast_pollutant,
    f.jour AS forecast_day,
    f.avg_value,
    f.max_value,
    f.min_value,
    a.source_name,
    a.source_url,
    a.source_logo
FROM city c
LEFT JOIN iaqi i ON c.idx = i.idx
LEFT JOIN forecast f ON c.idx = f.idx
LEFT JOIN attributions a ON c.idx = a.idx
