{{ config(materialized='table') }}

WITH raw AS (
    SELECT raw_json AS data
    FROM RAW.AQICN_API
),

-- 1. Infos principales
city as (
    select
        j:data.idx::int as idx,
        j:data.city.name::string as city_name,
        j:data.city.url::string as city_url,
        j:data.city.geo[0]::float as latitude,
        j:data.city.geo[1]::float as longitude,
        j:data.aqi::int as aqi,
        j:data.dominentpol::string as dominent_pol,
        j:data.time.iso::timestamp_tz as measurement_time
    from source
),

-- 2. Mesures instantanées (iaqi)
iaqi as (
    select
        j:data.idx::int as idx,
        k.key::string as pollutant,
        k.value:v::float as value
    from source,
    lateral flatten(input => j:data.iaqi) k
),

-- 3. Prévisions (forecast.daily)
forecast as (
    select
        j:data.idx::int as idx,
        p.key::string as pollutant,
        f.value:day::date as jour,
        f.value:avg::int as avg_value,
        f.value:max::int as max_value,
        f.value:min::int as min_value
    from source,
    lateral flatten(input => j:data.forecast.daily) p,
    lateral flatten(input => p.value) f
),

-- 4. Attributions (sources)
attributions as (
    select
        j:data.idx::int as idx,
        a.value:name::string as source_name,
        a.value:url::string as source_url,
        a.value:logo::string as source_logo
    from source,
    lateral flatten(input => j:data.attributions) a
)

-- Final : une seule table à plat
select
    c.idx,
    c.city_name,
    c.city_url,
    c.latitude,
    c.longitude,
    c.aqi,
    c.dominent_pol,
    c.measurement_time,
    i.pollutant as iaqi_pollutant,
    i.value as iaqi_value,
    f.pollutant as forecast_pollutant,
    f.jour as forecast_day,
    f.avg_value,
    f.max_value,
    f.min_value,
    a.source_name,
    a.source_url,
    a.source_logo
from city c
left join iaqi i on c.idx = i.idx
left join forecast f on c.idx = f.idx
left join attributions a on c.idx = a.idx
