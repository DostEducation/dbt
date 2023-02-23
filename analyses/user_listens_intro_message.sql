with identify_latest_flow as (
    select
        *,
        row_number() over (
            partition by user_id order by created_on desc
        ) as row_number
    from {{ ref("int_call_records_content_version_lang")}}
    where user_id is not null
),
filter_out_row_number_1 as(    
  select 
    * 
  from identify_latest_flow 
  where row_number = 1
),
bucket_listen_message as 
    (select
        *,
        case
            when cast(duration as int64) = 58 or  cast(duration as int64) > 58 then "1. user_heard_full_intro"
            when (cast(duration as int64) < 58) and (cast(duration as int64) >= 40) then "2. listen_bucket_40-58"
            when (cast(duration as int64) < 40) and (cast(duration as int64) >=25) then "3. listen_bucket_25-40"
            when (cast(duration as int64) < 25) and (cast(duration as int64) >= 10) then "4. listen_bucket_10-25"
            else "5. user_didnot_listen( <10secs)" 
            end as user_listen_intro_bucket
    from filter_out_row_number_1
    where true
        and content_name = "t-6_1-hello_1-day1"
        or content_name = "b-3_1-hello_1-day1"
        -- and data_source = "admindashboard"
    )
select
    user_listen_intro_bucket,
    count(user_listen_intro_bucket)
from bucket_listen_message
group by 1
order by user_listen_intro_bucket asc