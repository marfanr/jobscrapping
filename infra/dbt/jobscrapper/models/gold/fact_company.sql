{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id',
    schema = 'gold',
    on_schema_change = 'append_new_columns'
) }}

with raw_entries as (

    select
        j.kafka_key,
        r.province,
        j.company,
        j.source,
        j.ingested_at,
        e.sequence_number

    from {{ source('silver', 'jobs_entries') }} e

    join {{ source('silver', 'jobs') }} j
        on j."$path" = e.data_file.file_path

    left join {{ source('silver', 'job_region') }} r
        on j.kafka_key = r.kafka_key

    where e.status = 1
      and j.company is not null
    {% if is_incremental() %}
      and e.sequence_number > (
          select coalesce(max(sequence_number), 0) from {{ this }}
      )
    {% endif %}
),

deduped as (

    select
        to_hex(md5(to_utf8(company))) as id,
        company,
        source,
        province,
        ingested_at,
        sequence_number,
        row_number() over (
            partition by company
            order by
                ingested_at desc,
                sequence_number desc,
                kafka_key desc
        ) as rn
    from raw_entries
)

select
    d.id,
    d.company,
    {% if is_incremental() %}
    coalesce(d.province, t.province) as province,
    {% else %}
    d.province,
    {% endif %}
    d.source,
    d.ingested_at,
    d.sequence_number
from deduped d
{% if is_incremental() %}
left join {{ this }} t
    on t.id = d.id
where d.rn = 1
  and (
        t.id is null
        or t.ingested_at is null
        or d.ingested_at > t.ingested_at
        or (d.ingested_at = t.ingested_at and d.sequence_number > t.sequence_number)
      )
{% else %}
where d.rn = 1
{% endif %}