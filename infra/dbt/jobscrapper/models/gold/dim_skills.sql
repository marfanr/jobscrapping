{{ config(
    materialized = 'table',
    on_table_exists = 'drop',
    schema = 'gold'
) }}

SELECT DISTINCT 
    to_hex(md5(to_utf8(CAST(s.skill AS VARCHAR)))) AS id,
    s.skill
FROM iceberg.silver.job_skills_list AS t
CROSS JOIN UNNEST(skills) AS s(skill)