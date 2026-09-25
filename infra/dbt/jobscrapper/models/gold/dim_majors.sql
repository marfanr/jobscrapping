{{ config(
    materialized = 'table',
    on_table_exists = 'drop',
    schema = 'gold'
) }}

SELECT DISTINCT 
    to_hex(md5(to_utf8(CAST(s.major AS VARCHAR)))) AS id,
    s.major
FROM {{ source('silver', 'job_majors_list') }} AS t
CROSS JOIN UNNEST(majors) AS s(major)