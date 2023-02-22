with userwise_engagement as (select * from {{ ref('int_userwise_engagement_level') }}),
    callback_delay as (select * from {{ ref('int_immediate_callback_milestone_signup') }}),
    
    callback_delay_user_engagement as (
        select 
            callback_delay.uuid,
            callback_delay.user_number,
            callback_delay.user_id,
            callback_delay.delay_in_minutes,
            callback_delay.call_back_delay_bucket,
            (userwise_engagement.total_listen_seconds / nullif(userwise_engagement.total_content_duration, 0)) as listen_rate,
            (total_content_answered / nullif(total_content_delivered, 0)) as response_rate,
            userwise_engagement.data_source,
            userwise_engagement.program_name,
            userwise_engagement.first_user_month_engagement_level,
        from callback_delay
        left join userwise_engagement using (user_id)
        where data_source = "admindashboard"
        -- and program_name in ("B3", "T6")
        and delay_in_minutes < 1000
)
select
    *
from callback_delay_user_engagement
where first_user_month_engagement_level is not null