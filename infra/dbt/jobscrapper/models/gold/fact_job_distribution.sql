{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    schema = 'gold',
    unique_key = 'id',
    on_schema_change = 'sync_all_columns'
) }}

with raw as (

    select
        p.iso_code,
        lower(p.province_name) as province,
        lower(j.city) as city,
        jj.keyword,
        jj.source,
        max(j.updated_at) as updated_at,
        max(e.sequence_number) as sequence_number,
        count(distinct j.kafka_key) as job_count

    from {{ source('silver', 'job_region_entries') }} e

    join {{ source('silver', 'job_region') }} j
        on j."$path" = e.data_file.file_path

    join {{ ref('seed_iso3166_province') }} p
        on lower(j.province) = lower(p.province_name)

    join {{ source('silver', 'jobs') }} jj
        on j.kafka_key = jj.kafka_key

    where e.status = 1
    {% if is_incremental() %}
      and e.sequence_number > (
          select coalesce(max(sequence_number), 0) from {{ this }}
      )
    {% endif %}

    group by
        p.iso_code,
        lower(p.province_name),
        lower(j.city),
        jj.keyword,
        jj.source
)

select
    to_hex(md5(to_utf8(
        r.iso_code || '|' || coalesce(r.keyword, '') || '|' || coalesce(r.source, '')
    ))) as id,
    r.iso_code,
    r.province,
    r.city,
    r.keyword,
    r.source,
    r.sequence_number,
    {% if is_incremental() %}
    r.job_count + coalesce(t.job_count, 0) as job_count,
    greatest(r.updated_at, coalesce(t.updated_at, r.updated_at)) as updated_at
    {% else %}
    r.job_count as job_count,
    r.updated_at as updated_at
    {% endif %}
from raw r
{% if is_incremental() %}
left join {{ this }} t
    on t.iso_code = r.iso_code
   and t.keyword is not distinct from r.keyword
   and t.source is not distinct from r.source
{% endif %}