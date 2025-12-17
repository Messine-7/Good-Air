{{ config(
    materialized="incremental",
    unique_key="record_id"
) }}

WITH src AS (
    SELECT 
        UPPER(TRIM(f.value:city::string)) AS city_name_clean,

        TRY_TO_NUMBER(f.value:raw_json:data:idx::string) AS station_id,

        TRY_TO_NUMBER(f.value:raw_json:data:time:v::string) AS dt,
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:raw_json:data:time:v::string)) AS dt_utc,
        CONVERT_TIMEZONE(
            'UTC','Europe/Paris',
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:raw_json:data:time:v::string))
        ) AS dt_paris,

        TRY_TO_NUMBER(f.value:raw_json:data:aqi::string) AS aqi,
        f.value:raw_json:data:dominentpol::string AS dominent_pol,

        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:co:v::string) AS iaqi_co,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:no2:v::string) AS iaqi_no2,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:o3:v::string) AS iaqi_o3,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:pm10:v::string) AS iaqi_pm10,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:pm25:v::string) AS iaqi_pm25,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:so2:v::string) AS iaqi_so2,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:t:v::string) AS iaqi_temp,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:h:v::string) AS iaqi_humidity,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:p:v::string) AS iaqi_pressure,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:w:v::string) AS iaqi_wind,
        TRY_TO_NUMBER(f.value:raw_json:data:iaqi:wg:v::string) AS iaqi_wind_gust,

        f.value:raw_json:data:time:iso::string AS time_iso,
        f.value:raw_json:data:time:s::string AS time_str,
        f.value:raw_json:data:time:tz::string AS tz_offset
    FROM BRONZE.AQICN_API,
         LATERAL FLATTEN(input => PARSE_JSON(raw_json)) f
    WHERE f.value:raw_json:data:idx IS NOT NULL
      AND f.value:raw_json:data:time:v IS NOT NULL
),

deduplicated AS (
    SELECT
        md5(station_id || '-' || dt) AS record_id,
        station_id,
        dc.city_id,

        dt,
        dt_utc,
        dt_paris,

        aqi,
        dominent_pol,

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
        tz_offset,

        ROW_NUMBER() OVER (
            PARTITION BY station_id, dt
            ORDER BY dc.city_id
        ) AS rn

    FROM src
    LEFT JOIN SILVER.DIM_CITY dc
        ON src.city_name_clean = UPPER(TRIM(dc.city_name))
)

SELECT *
FROM deduplicated
WHERE rn = 1
  AND record_id NOT IN (
      SELECT record_id
      FROM SILVER.FACT_AIR_QUALITY_RECORDS
  )

