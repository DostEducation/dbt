with ten_sec_duration as (
  select
    content_id,
    data_source,
    content_name,
    language_name,
    program_name,
    module_id,
    date_trunc(created_on, day) created_on,
    count(user_id) as ten_sec_user_id
  from {{ ref("int_call_records_content_version_lang") }}
  where cast(duration as int64) < 10
  and cast(created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
  and duration is not null
  and user_id is not null
  group by 1,2,3,4,5,6,7
),
full_duration as (

  select 
      content_id,
      data_source,
      content_name,
      language_name,
      program_name,
      module_id,
      date_trunc(created_on, day) created_on,
      count(user_id) as full_duration_user_id
    from {{ ref("int_call_records_content_version_lang") }}
    where duration is not null
      and cast(created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
      and user_id is not null
    group by 1,2,3,4,5,6,7
)

select 
  t.content_id,
  t.data_source,
  t.created_on,
  t.content_name,
  t.language_name,
  t.program_name,
  t.module_id,
  (t.ten_sec_user_id / f.full_duration_user_id) as percent_cutoff_ten_sec
from ten_sec_duration t
left join full_duration f on t.content_id = f.content_id
      and t.data_source = f.data_source
      and t.created_on = f.created_on
      and t.language_name = f.language_name
      and t.program_name = f.program_name
      and t.module_id = f.module_id
group by 1,2,3,4,5,6,7,8