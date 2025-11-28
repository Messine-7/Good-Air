{{ config(
    materialized="incremental",
    unique_key="weather_id"
) }}

WITH src AS (
    SELECT DISTINCT
        f.value:weather[0]:id::int AS weather_id,
        f.value:weather[0]:main::string AS weather_main,
        f.value:weather[0]:description::string AS weather_description,
        f.value:weather[0]:icon::string AS weather_icon
    FROM BRONZE.WEATHER_API,
         LATERAL FLATTEN(input => raw_json) f
    WHERE f.value:weather IS NOT NULL
)

SELECT
    weather_id,
    weather_main,
    weather_description,
    weather_icon,
    CURRENT_TIMESTAMP() AS last_updated
FROM src

{% if is_incremental() %}
WHERE weather_id NOT IN (SELECT weather_id FROM {{ this }})
{% endif %}
