WITH weather AS (
    SELECT DISTINCT
        UPPER(TRIM(f.value:base_city_name::string)) AS city_name,
        f.value:sys:country::string AS country,
        f.value:coord:lat::float AS latitude,
        f.value:coord:lon::float AS longitude
    FROM BRONZE.AQICN_API,
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
        MAX(a.city_url) AS city_url,
        MAX(w.country) AS country,
        MAX(w.latitude) AS latitude,
        MAX(w.longitude) AS longitude
    FROM weather w
    FULL OUTER JOIN aqicn a 
        ON w.city_name = a.city_name
    GROUP BY COALESCE(w.city_name, a.city_name)
)

SELECT
    -- C'est l'astuce : On envoie NULL. 
    -- Snowflake verra le NULL et remplira automatiquement avec le numéro suivant (Auto-incrément).
    CAST(NULL AS INTEGER) as city_id,
    city_name,
    city_url,
    country,
    latitude,
    longitude
FROM combined

{% if is_incremental() %}
-- On ne charge que ce qui n'existe pas déjà
WHERE city_name NOT IN (SELECT city_name FROM {{ this }})
{% endif %}