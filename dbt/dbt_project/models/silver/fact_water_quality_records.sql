{{ config(
    materialized="incremental",
    unique_key="record_id"
) }}

-- ======================
-- EXTRACTION DU WRAPPER JSON AQICN
-- ======================
WITH raw AS (
    SELECT 
        UPPER(TRIM(f.value:city::string)) AS city_name_clean,
        f.value:raw_json AS data
    FROM BRONZE.AQICN_API,
         LATERAL FLATTEN(input => PARSE_JSON(raw_json)) f
),

flattened_array AS (
    SELECT
        r.city_name_clean,
        f.value AS city_data
    FROM raw r,
         LATERAL FLATTEN(input => PARSE_JSON(r.data)) f
),

src AS (
    SELECT
        city_name_clean,

        TRY_TO_NUMBER(city_data:raw_json:data:idx::string) AS station_id,

        TRY_TO_NUMBER(city_data:raw_json:data:time:v::string) AS dt,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:raw_json:data:time:v::string)) AS dt_utc,
        CONVERT_TIMEZONE(
            'UTC','Europe/Paris',
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(city_data:raw_json:data:time:v::string))
        ) AS dt_paris,

        TRY_TO_NUMBER(city_data:raw_json:data:aqi::string) AS aqi,
        city_data:raw_json:data:dominentpol::string AS dominent_pol,

        TRY_TO_NUMBER(city_data:raw_json:data:city:geo[0]::string) AS lat,
        TRY_TO_NUMBER(city_data:raw_json:data:city:geo[1]::string) AS lon,

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

        city_data:raw_json:data:time:iso::string AS time_iso,
        city_data:raw_json:data:time:s::string AS time_str,
        city_data:raw_json:data:time:tz::string AS tz_offset

    FROM flattened_array
    WHERE city_data:raw_json:data:idx IS NOT NULL
      AND city_data:raw_json:data:time:v IS NOT NULL
)

SELECT
    md5(station_id || '-' || dt) AS record_id,
    station_id,

    dc.city_id,  -- join propre

    dt,
    dt_utc,
    dt_paris,

    aqi,
    dominent_pol,
    lat,
    lon,

    iaqi_co,
    iaqi_no2,
    iaqi_o3,
    iaqi_pm10,
    iaqi_pm25,
    iaqi_so2,
    iaqi_temp,
    iaqi_humidity,
    iaqi_pressure,
    iaqi_wind,
    iaqi_wind_gust,

    time_iso,
    time_str,
    tz_offset

FROM src
LEFT JOIN SILVER.DIM_CITY dc
    ON UPPER(TRIM(src.city_name_clean)) = UPPER(TRIM(dc.city_name))

{% if is_incremental() %}
WHERE md5(station_id || '-' || dt) NOT IN (SELECT record_id FROM {{ this }})
{% endif %}
