{{ config(
    materialized = 'table',
    on_table_exists = 'drop',
    schema = 'silver'
) }}

WITH cities_cte AS (
    SELECT LOWER(name) AS cities
    FROM {{ ref('seed_cities') }}
)
SELECT j.kafka_key, p.cities AS city
FROM iceberg.bronze.jobs j
LEFT JOIN cities_cte p
    ON LOWER(j.city) LIKE '%' || p.cities || '%'
    OR LOWER(j.location) LIKE '%' || p.cities || '%'