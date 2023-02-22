with bucket_listen_message as 
    (select
        *,
        case
            when cast(duration as int64) = 58 or  cast(duration as int64) > 58 then "user_heard_full_intro"
            when (cast(duration as int64) < 58) and (cast(duration as int64) >= 40) then "listen_bucket_40-58"
            when (cast(duration as int64) < 40) and (cast(duration as int64) >=25) then "listen_bucket_25-40"
            when (cast(duration as int64) < 25) and (cast(duration as int64) >= 10) then "listen_bucket_10-25"
            else "user_didnot_listen( <10secs)" 
            end as user_listen_intro_bucket
    from {{ ref('int_call_records_content_version_lang') }}
    where true
        and content_name = "t-6_1-hello_1-day1"
        or content_name = "b-3_1-hello_1-day1"
        -- and data_source = "admindashboard"
    )
select
    user_listen_intro_bucket,
    count(user_listen_intro_bucket) as no_users
from bucket_listen_message
group by 1
order by no_users desc