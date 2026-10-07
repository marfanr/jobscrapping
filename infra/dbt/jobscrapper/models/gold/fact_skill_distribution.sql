{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['skill', 'province'],
    schema = 'gold',
    on_schema_change = 'sync_all_columns'
) }}

with raw as (

    select
        s.skill,
        lower(r.province) as province,
        count(distinct t.kafka_key) as job_count,
        max(t.updated_at) as updated_at,
        max(e.sequence_number) as sequence_number

    from {{ source('silver', 'job_skills_list_entries') }} as e

    join {{ source('silver', 'job_skills_list') }} t
        on t."$path" = e.data_file.file_path

    cross join unnest(t.skills) as s(skill)

    join {{ source('silver', 'job_region') }} r
        on t.kafka_key = r.kafka_key

    where e.status = 1
      and s.skill is not null
      and trim(s.skill) <> ''
      and r.province is not null
    {% if is_incremental() %}
      and e.sequence_number > (
          select coalesce(max(sequence_number), 0) from {{ this }}
      )
    {% endif %}

    group by s.skill, lower(r.province)
)

select
    r.skill,
    r.province,
    r.job_count
    {% if is_incremental() %}
        + coalesce(x.job_count, 0)
    {% endif %}
    as job_count,
    r.sequence_number,
    r.updated_at
from raw r
{% if is_incremental() %}
left join {{ this }} x
    on x.skill = r.skill
   and x.province = r.province
{% endif %}