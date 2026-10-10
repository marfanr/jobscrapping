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

with raw AS (
    SELECT 
        s.kafka_key,
        j.company,
        j.job_name,
        j.source,
        j.keyword,
        jr.province,
        jr.city,
        s.lower_salary,
        coalesce(s.upper_salary, s.lower_salary) as upper_salary,
        s.updated_at,
        e.sequence_number

    FROM {{ source('silver', 'job_salary_entries') }} e
    JOIN {{ source('silver', 'job_salary') }} s
        ON s."$path" = e.data_file.file_path
    JOIN {{ source('silver', 'jobs')}} j
        ON j.kafka_key = s.kafka_key
    JOIN {{ source('silver', 'job_region')}} jr
        ON jr.kafka_key = s.kafka_key
    WHERE
        e.status = 1
        AND e.sequence_number > {{ max_sequence }}
        AND s.lower_salary IS NOT NULL
),

dedup AS (
    SELECT
        company,
        job_name,
        source,
        keyword,
        province,
        city,
        lower_salary,
        upper_salary,
        updated_at,
        sequence_number,
        row_number() OVER (
            PARTITION BY company
            order by
                updated_at DESC,
                sequence_number DESC,
                kafka_key DESC
        ) AS rn
    FROM raw
)

SELECT 
    d.company,
    d.job_name,
    d.source,
    d.keyword,
    d.province,
    d.city,
    {% if is_incremental() %}
        coalesce(d.lower_salary, t.lower_salary) as lower_salary,
    {% else %}
        d.lower_salary,
    {% endif %}
    {% if is_incremental() %}
        coalesce(d.upper_salary, t.upper_salary) as upper_salary,
    {% else %}
        d.upper_salary,
    {% endif %}
    d.updated_at,
    d.sequence_number
FROM dedup d
{% if is_incremental() %}
    left join {{ this }} t
        on t.company = d.company
    WHERE
        rn = 1 
        and 
        (
            t.updated_at is null
            or d.updated_at > t.updated_at
            or (d.updated_at = t.updated_at AND d.sequence_number > t.sequence_number)
        )
{% else %}
    WHERE rn = 1
{% endif %}