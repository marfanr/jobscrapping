{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    schema = 'gold',
    unique_key = 'iso_code'
) }}

with raw as (

    select
        p.iso_code,
        p.province_name as province,
        max(j.updated_at) as updated_at,
        max(e.sequence_number) as sequence_number,
        count(j.kafka_key) as job_count

    from {{ source('silver', 'job_region_entries') }} e

    join {{ source('silver', 'job_region') }} j
        on j."$path" = e.data_file.file_path

    join {{ ref('seed_iso3166_province') }} p
        on lower(j.province) = lower(p.province_name)

    where e.status = 1
    {% if is_incremental() %}
      and e.sequence_number > (
          select coalesce(max(sequence_number), 0) from {{ this }}
      )
    {% endif %}

    group by
        p.iso_code,
        p.province_name
)

select
    r.iso_code,
    lower(r.province) as province,
    r.sequence_number,
    r.job_count
    {% if is_incremental() %}
        + coalesce(t.job_count, 0)
    {% endif %}
    as job_count,
    r.updated_at
from raw r
{% if is_incremental() %}
left join {{ this }} t
    on t.iso_code = r.iso_code
{% endif %}