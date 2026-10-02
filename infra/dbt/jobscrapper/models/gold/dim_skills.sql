{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    schema = 'gold',
    on_schema_change = 'sync_all_columns'
) }}

{% set max_seq = 0 %}
{% if is_incremental() %}
    {% set query  %}
        SELECT coalesce(max(sequence_number), 0) FROM {{ this }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% set max_seq = results.columns[0][0] %}
    {% endif %}
{% endif %}

WITH raw AS (
    SELECT DISTINCT 
        to_hex(md5(to_utf8(CAST(s.skill AS VARCHAR)))) AS id,
        s.skill,
        t.updated_at,
        e.sequence_number
    FROM {{ source('silver', 'job_skills_list_entries') }} AS e
    JOIN {{ source('silver', 'job_skills_list') }} t
        ON t."$path" = e.data_file.file_path
    CROSS JOIN UNNEST(skills) AS s(skill)
    WHERE e.status = 1
        AND e.sequence_number > {{max_seq}}
),

deduped AS (
    SELECT 
        to_hex(md5(to_utf8(cast(skill AS VARCHAR)))) as id,
        skill,
        sequence_number,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY skill
            ORDER BY updated_at DESC, sequence_number DESC
        ) AS rn
    FROM raw
)

SELECT 
    id,
    skill,
    sequence_number,
    updated_at
FROM deduped
WHERE rn = 1