{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    schema = 'gold',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns'
) }}

{% set max_sequence = 0 %}
{% if is_incremental() %}
    {% set query %}
        SELECT coalesce(max(sequence_number), 0) FROM {{this}}
    {% endset %}
    {% set result = run_query(query) %}
    {% if execute %}
        {% set max_sequence = result.columns[0][0] %}
    {% endif %}
{% endif %}

WITH raw AS (
    SELECT 
        to_hex(md5(to_utf8(CAST(s.major AS VARCHAR)))) AS id,
        s.major,
        j.updated_at,
        e.sequence_number
    FROM {{ source('silver', 'job_majors_list_entries') }} e
    JOIN {{ source('silver', 'job_majors_list') }} j
        ON j."$path" = e.data_file.file_path
    CROSS JOIN UNNEST(majors) AS s(major)
    WHERE e.status = 1 
      AND e.sequence_number > {{max_sequence}}
),

dedup AS (
    SELECT
        id,
        major,
        sequence_number,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY major
            ORDER BY updated_at DESC
        ) AS rn
    FROM raw
)

SELECT 
    id,
    major,
    sequence_number,
    updated_at
FROM dedup
WHERE
    rn = 1