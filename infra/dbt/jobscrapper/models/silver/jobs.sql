{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    schema = 'silver'
) }}

WITH src AS (
    SELECT
        j.kafka_key,
        j.company,
        j.job_name,
        j.keyword,
        j.source,
        j.applicant,
        j.url,
        j.quota,
        j.publisher_name,
        j.listing_date,
        j.publisher_last_online,
        j.ingested_at,
        e.snapshot_id,
        e.sequence_number,
        ROW_NUMBER() OVER (
            PARTITION BY j.job_name, j.company
            ORDER BY j.ingested_at DESC
        ) AS rn
    FROM {{ source('bronze', 'jobs_entries') }} e
    JOIN {{ source('bronze', 'jobs') }} j
        ON j."$path" = e.data_file.file_path
    WHERE e.status = 1

    {% if is_incremental() %}
        AND e.sequence_number > (SELECT COALESCE(MAX(sequence_number), 0) FROM {{ this }})
    {% endif %}
)

SELECT
    kafka_key,
    company,
    job_name,
    keyword,
    source,
    applicant,
    quota,
    publisher_name,
    listing_date,
    publisher_last_online,
    ingested_at,
    snapshot_id,
    sequence_number
FROM src
WHERE rn = 1