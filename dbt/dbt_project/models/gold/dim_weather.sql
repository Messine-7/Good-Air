{{ config(
    materialized='incremental',
    unique_key='weather_id'
) }}

WITH source_weather AS (
    SELECT DISTINCT
        weather_id,
        weather_main,
        weather_description,
        weather_icon
    FROM {{ ref('weather_clean') }}
    WHERE weather_id IS NOT NULL
)

SELECT
    s.weather_id,
    s.weather_main,
    s.weather_description,
    s.weather_icon,
    CURRENT_TIMESTAMP() AS last_updated
FROM source_weather s

-- Condition d'incrémental : ne charger que les nouvelles valeurs
{% if is_incremental() %}
    WHERE s.weather_id NOT IN (
        SELECT weather_id FROM {{ this }}
    )
{% endif %}