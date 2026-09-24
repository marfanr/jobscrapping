{{ config(
    materialized = 'table',
    on_table_exists = 'drop',
    schema = 'silver'
) }}

WITH province_cte AS (
    SELECT LOWER(name) AS province, LOWER(name_en) AS province_en
    FROM {{ ref('seed_provinces') }}
)
SELECT j.kafka_key, p.province
FROM iceberg.bronze.jobs j
LEFT JOIN province_cte p
    ON LOWER(j.province) LIKE '%' || p.province || '%'
    OR LOWER(j.province) LIKE '%' || p.province_en || '%'
    OR LOWER(j.location) LIKE '%' || p.province || '%'
    OR LOWER(j.location) LIKE '%' || p.province_en || '%'