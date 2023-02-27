with
    call_records as (
        select *
        from {{ ref("stg_all_call_records") }}
        where content_name = "t-6_1-hello_1-day1" or content_name = "b-3_1-hello_1-day1"
    ),
    identify_latest_flow as (
        select
            *,
            row_number() over (
                partition by user_id, data_source order by duration desc
            ) as row_number
        from call_records
    ),
    filter_first_row as (
        select * from identify_latest_flow where row_number = 1
    ),
    bucket_listen_message as (
        select
            *,
            case
                when cast(duration as int64) >= 10 then 1 else 0
            end as user_listen_intro_bucket
        from filter_first_row
    )

select *
from bucket_listen_message
-- where data_source = "admindashboard"


