{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    schema = 'gold',
    on_schema_change = 'append_new_columns'
) }}

{% set max_seq = 0 %}
{% if is_incremental() %}
    {% set query %}
        SELECT COALESCE(MAX(sequence_number), 0) FROM {{ this }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% set max_seq = results.columns[0][0] %}
    {% endif %}
{% endif %}

WITH raw_entries AS (
    SELECT
        j.company,
        j.ingested_at,
        e.sequence_number
    FROM {{ source('bronze', 'jobs_entries') }} e
    JOIN {{ source('bronze', 'jobs') }} j
        ON j."$path" = e.data_file.file_path
    WHERE e.status = 1
      AND e.sequence_number > {{ max_seq }}
),

deduped AS (
    SELECT
        company,
        ingested_at,
        sequence_number,
        ROW_NUMBER() OVER (
            PARTITION BY company
            ORDER BY ingested_at DESC, sequence_number DESC
        ) AS rn
    FROM raw_entries
)

SELECT 
    to_hex(md5(to_utf8(CAST(company AS VARCHAR)))) AS id,
    company,
    ingested_at,
    sequence_number
FROM deduped
WHERE rn = 1