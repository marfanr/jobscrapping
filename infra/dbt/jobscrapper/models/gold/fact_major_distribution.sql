{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    schema = 'gold',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns'
) }}

{% set max_sequence = 0 %}
{% if is_incremental() and execute %}
    {% set result = run_query("SELECT coalesce(max(sequence_number), 0) FROM " ~ this) %}
    {% set max_sequence = result.columns[0].values()[0] %}
{% endif %}

WITH delta AS (
    SELECT
        lower(r.province) AS province,
        s.major,
        to_hex(md5(to_utf8(lower(r.province) || '|' || CAST(s.major AS VARCHAR)))) AS id,
        max(j.updated_at) AS updated_at,
        max(e.sequence_number) AS sequence_number,
        count(j.kafka_key) AS job_count
    FROM {{ source('silver', 'job_majors_list_entries') }} e
    JOIN {{ source('silver', 'job_majors_list') }} j
        ON j."$path" = e.data_file.file_path
    CROSS JOIN UNNEST(j.majors) AS s(major)
    JOIN {{ source('silver', 'job_region') }} r
        ON j.kafka_key = r.kafka_key
    WHERE e.status = 1
      AND e.sequence_number > {{ max_sequence }}
    GROUP BY
        lower(r.province), s.major
)

SELECT
    d.id,
    d.major,
    d.province,
    {% if is_incremental() %}
    d.job_count + coalesce(t.job_count, 0) AS job_count,
    greatest(d.updated_at, coalesce(t.updated_at, d.updated_at)) AS updated_at,
    {% else %}
    d.job_count,
    d.updated_at,
    {% endif %}
    d.sequence_number
FROM delta d
{% if is_incremental() %}
LEFT JOIN {{ this }} t
    ON t.id = d.id
{% endif %}