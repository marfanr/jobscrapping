{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'job_id',
    schema = 'silver'
) }}

{% set max_seq = 0 %}
{% if is_incremental() %}
    {% set query %}
    SELECT coalesce(max(sequence_number), 0) FROM {{ this }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% set max_seq = results.columns[0][0] %}
    {% endif %}
{% endif %}

WITH raw_data AS (
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
        e.sequence_number
    FROM {{ source('bronze', 'jobs_entries') }} e
    JOIN {{ source('bronze', 'jobs') }} j
        ON j."$path" = e.data_file.file_path
    WHERE e.status = 1
    AND e.sequence_number > {{max_seq}}
),

deduped AS (
    SELECT
        *,
        to_hex(md5(to_utf8(CAST(job_name || company || url AS VARCHAR)))) AS job_id,
        ROW_NUMBER() OVER (
            PARTITION BY job_name, company, url
            ORDER BY ingested_at DESC, sequence_number DESC
        ) AS rn
    FROM raw_data
)

SELECT
    job_id,
    kafka_key,
    company,
    job_name,
    keyword,
    source,
    applicant,
    url,
    quota,
    publisher_name,
    listing_date,
    publisher_last_online,
    ingested_at,
    snapshot_id,
    sequence_number
FROM deduped
WHERE rn = 1