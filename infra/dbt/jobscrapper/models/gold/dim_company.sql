{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    schema = 'silver'
) }}

with src AS (
    SELECT
        j.company_name,
        j.ingested_at,
        e.sequence_number,
        ROW_NUMBER() OVER(
            PARTITION BY j.company_name
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
    to_hex(md5(to_utf8(CAST(company_name AS VARCHAR)))) AS id,
    company_name
FROM src
WHERE rn = 1