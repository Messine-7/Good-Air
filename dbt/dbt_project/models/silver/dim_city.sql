{{ config(materialized="table") }}

WITH weather AS (
    SELECT DISTINCT
        UPPER(TRIM(f.value:base_city_name::string)) AS city_name
    FROM BRONZE.WEATHER_API,
         LATERAL FLATTEN(input => raw_json) f
    WHERE f.value:base_city_name::string IS NOT NULL
),

aqicn AS (
    SELECT DISTINCT
        UPPER(TRIM(f.value:city::string)) AS city_name,
        f.value:raw_json:data:city:url::string AS city_url
    FROM BRONZE.AQICN_API,
         LATERAL FLATTEN(input => PARSE_JSON(raw_json)) f
    WHERE f.value:city::string IS NOT NULL
),

combined AS (
    SELECT 
        COALESCE(w.city_name, a.city_name) AS city_name,
        MAX(a.city_url) AS city_url
    FROM weather w
    FULL OUTER JOIN aqicn a 
        ON w.city_name = a.city_name
    GROUP BY COALESCE(w.city_name, a.city_name)
)

SELECT
    CONCAT('CT', LPAD(ROW_NUMBER() OVER (ORDER BY city_name), 3, '0')) AS city_id,
    city_name,
    city_url
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY city_name) = 1