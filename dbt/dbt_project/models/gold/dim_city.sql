{{ config(
    materialized='table'
) }}

WITH weather AS (
    SELECT
        DISTINCT UPPER(TRIM(CITY_NAME)) AS CITY_NAME
    FROM {{ ref('weather_clean') }}
),

aqicn AS (
    SELECT
        DISTINCT UPPER(TRIM(CITY_NAME)) AS CITY_NAME,
        MAX(CITY_URL) AS CITY_URL
    FROM {{ ref('aqicn_clean') }}
    GROUP BY CITY_NAME
),

-- Jointure entre les deux sources
combined AS (
    SELECT
        w.CITY_NAME,
        a.CITY_URL
    FROM weather w
    LEFT JOIN aqicn a
        ON w.CITY_NAME = a.CITY_NAME
)

-- Ajout d'un ID propre à la dimension
SELECT
    CONCAT('CT', LPAD(ROW_NUMBER() OVER (ORDER BY CITY_NAME), 2, '0')) AS CITY_ID,
    CITY_NAME,
    CITY_URL
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY CITY_NAME ORDER BY CITY_NAME) = 1