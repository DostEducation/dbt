with
    identify_latest_flow as (
        select
            *,
            row_number() over (
                partition by user_id order by engagement_start_time asc
            ) as row_number
        from {{ ref("stg_engagement_campaign_mapping") }}
        where user_id is not null and engagement_start_time is not null
    ),

    filter_out_row_number_1 as (
        select * from identify_latest_flow where row_number = 1
    ),

    time_difference_inbound_outbound as (
        select
            *,
            (date_diff(attempted_timestamp, engagement_start_time, second))
            / 60 as delay_in_minutes,
        from filter_out_row_number_1
    )

select *
from time_difference_inbound_outbound