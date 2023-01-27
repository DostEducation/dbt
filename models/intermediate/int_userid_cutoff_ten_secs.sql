with ten_sec_duration as (
  select
    content_id,
    data_source,
    date_trunc(created_on, day) created_on,
    count(user_id) as ten_sec_user_id
  from {{ ref("int_call_records_content_version_lang") }}
  where cast(duration as int64) < 10
  and cast(created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
  and duration is not null
  and user_id is not null
  group by 1,2,3
),
full_duration as (

  select 
      content_id,
      data_source,
      date_trunc(created_on, day) created_on,
      count(user_id) as full_duration_user_id
    from {{ ref("int_call_records_content_version_lang") }}
    where duration is not null
      and user_id is not null
    group by 1,2,3 
)

select 
  t.content_id,
  t.data_source,
  t.created_on,
  c.content_name,
  (t.ten_sec_user_id / f.full_duration_user_id) as percentage_ten_sec
from ten_sec_duration t
left join full_duration f on t.content_id = f.content_id
      and t.data_source = f.data_source
      and t.created_on = f.created_on
left join {{ ref("stg_content") }} c on c.content_id = t.content_id
group by 1,2,3,4,5
