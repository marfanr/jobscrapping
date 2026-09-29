{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    schema = 'silver'
) }}

SELECT
    j.company,
    e.snapshot_id,
    e.sequence_number,
    j.kafka_key,
    j.ingested_at
FROM {{ source('bronze', 'jobs_entries') }} e
JOIN {{ source('bronze', 'jobs') }} j
    ON j."$path" = e.data_file.file_path
WHERE e.status = 1

{% if is_incremental() %}
    AND e.sequence_number > (SELECT COALESCE(MAX(sequence_number), 0) FROM {{ this }})
{% endif %}

GROUP BY
    j.company,
    e.snapshot_id,
    e.sequence_number,
    j.kafka_key,
    j.ingested_at