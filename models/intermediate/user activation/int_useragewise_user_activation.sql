with
    user_activation_project as (
        select *
        from {{ ref("stg_daily_engagement_user_agewise") }}
        where
            user_signup_status = "complete"
            and data_source = "admindashboard"
            and user_created_on >= "2021-06-01"
    ),
    user_activation_project_month as (
        select *
        from {{ ref("stg_monthwise_engagement_level") }}
        where
            user_signup_status = "complete"
            and data_source = "admindashboard"
            and user_created_on >= "2021-06-01"
    ),
    preparing_user_aggregation_monthone as (
        select * from user_activation_project_month where user_month_at_time_of_call = 1
    ),
    aggregating_users_month_one as (
        with
            aggregating_monthone as (
                select
                    user_id,
                    data_source,
                    engagement_level as monthone_engagement_level,
                    response_rate as monthone_response_rate,
                    average_listen_rate as listen_rate,
                from preparing_user_aggregation_monthone
            )
        select
            * except (monthone_engagement_level),
            case
                when monthone_response_rate >= 60 and listen_rate >= 50
                then 3
                when monthone_response_rate >= 40 and listen_rate >= 25
                then 2
                when monthone_response_rate >= 4 and listen_rate >= 0
                then 1
                else 0
            end as month_one_engagement_level
        from aggregating_monthone
    ),
    preparing_user_aggregation_monthtwo as (
        select * from user_activation_project_month where user_month_at_time_of_call = 2
    ),
    aggregating_users_month_two as (
        with
            aggregating_monthtwo as (
                select
                    user_id,
                    data_source,
                    engagement_level as second_month_engagement_level,
                    response_rate as monthtwo_response_rate,
                    average_listen_rate as monthtwo_listen_rate,
                from preparing_user_aggregation_monthtwo
            )
        select
            * except (second_month_engagement_level),
            case
                when monthtwo_response_rate >= 60 and monthtwo_listen_rate >= 50
                then 3
                when monthtwo_response_rate >= 40 and monthtwo_listen_rate >= 25
                then 2
                when monthtwo_response_rate >= 4 and monthtwo_listen_rate >= 0
                then 1
                else 0
            end as month_two_engagement_level
        from aggregating_monthtwo
    ),
    preparing_user_aggregation_monththree as (
        select *
        from user_activation_project_month
        where true and user_month_at_time_of_call in (3)
    ),
    aggregating_users_month_three as (
        with
            aggregating_monththree as (
                select
                    user_id,
                    data_source,
                    engagement_level as third_month_engagement_level,
                    response_rate as monththree_response_rate,
                    average_listen_rate as monththree_listen_rate,
                from preparing_user_aggregation_monththree
            )
        select
            * except (third_month_engagement_level),
            case
                when monththree_response_rate > 60 and monththree_listen_rate > 50
                then 3
                when monththree_response_rate > 40 and monththree_listen_rate > 25
                then 2
                when monththree_response_rate > 4 and monththree_listen_rate > 0
                then 1
                else 0
            end as month_three_engagement_level
        from aggregating_monththree
    ),
    prompt_answered as (
        with
            coding_prompt_response_rate as (
                select
                    user_id,
                    data_source,
                    sum(prompts_delivered) as prompts_delivered,
                    sum(prompts_responded) as prompt_responded,  
                    avg(response_rate) as prompt_answered_response_rate,
                    sum(unique_days_call_delivered) as calls_delivered,
                from user_activation_project
                -- where prompts_delivered != 0 

                where user_age_at_time_of_call
                    in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                -- and data_source = "admindashboard"
                group by 1, 2
            )
        select *,
            (prompt_responded / prompts_delivered)*100 as prompt_answered_cont,
            case
                when ((prompt_responded / prompts_delivered)*100) >= 50 then 1 else 0
            end as prompt_answered_coded
        from coding_prompt_response_rate
        where true and prompt_answered_response_rate >= 80 and calls_delivered >= 7
        group by 1,2,3,4,5,6
    ),
    picked_up_call as (
        with
            coding_pickup_call as (
                select
                    user_id,
                    data_source,
                    avg(response_rate) as call_pickup_cont,
                    case when avg(response_rate) >= 80 then 1 else 0 end as call_pickup,
                    sum(unique_days_call_delivered) as calls_delivered
                from user_activation_project
                where  unique_days_call_delivered != 0
                    and 
                    user_age_at_time_of_call
                    in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                group by 1, 2
            )
        select *
        from coding_pickup_call
        where calls_delivered >= 7
    ),
    user_listens_entire_message as (
        with
            coding_listens_message as (
                select
                    user_id,
                    data_source,
                    avg(average_listen_rate) as entire_call_heard_cont,
                    avg(response_rate) as listens_entire_message_response_rate,
                    sum(unique_days_call_delivered) as calls_delivered,
                    avg(average_listen_rate)
                from user_activation_project
                where unique_days_call_delivered != 0
                    and    
                    user_age_at_time_of_call
                    in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                group by 1, 2
            )
        select *,
            case
                when entire_call_heard_cont >= 50 then 1 else 0
            end as entire_call_heard_coded,
        from coding_listens_message
        where
            true
            and listens_entire_message_response_rate >= 80
            and calls_delivered >= 7
    ),
    immediate_callback as (
        with
            identify_latest_flow as (
                select
                    *,
                    row_number() over (
                        partition by user_id order by engagement_start_time asc
                    ) as row_number
                from `goalkeep_dev_swapneel.stg_engagement_campaign_mapping`
                where
                    user_id is not null
                    and engagement_start_time is not null
                    and engagement_created_on >= "2021-06-01"
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
        -- coding_immediate_one_zero as (
        select
            *,
            case
                when delay_in_minutes > 1 or attempted_timestamp is null
                then 0
                when
                    campaign_status in (
                        "failed_system",
                        "failed_noresponse",
                        "failed_outofwindow",
                        "failed_noconnect",
                        "failed_toomanyrequests",
                        "failed_noschedule",
                        "failed_dnd"
                    )
                then 0
                when delay_in_minutes <= 1
                then 1
                else null
            end as immediate_call_back
        from time_difference_inbound_outbound
    )
-- bringing_everything_under_onetable as (
select
    aggregating_users_month_one.user_id,
    aggregating_users_month_one.data_source,
    prompt_answered.prompt_answered_cont,
    case when prompt_answered.prompt_answered_coded = 1 then 1 else 0 end as prompt_answered_coded,
    case when picked_up_call.call_pickup = 1 then 1 else 0 end as call_pickup,
    picked_up_call.call_pickup_cont,
    case
        when user_listens_entire_message.entire_call_heard_coded = 1 then 1 else 0
    end as entire_message_heard,
    user_listens_entire_message.entire_call_heard_cont,
    immediate_callback.delay_in_minutes,
    case
        when immediate_callback.immediate_call_back = 1 then 1 else 0
    end as immediate_call_back,
    aggregating_users_month_one.month_one_engagement_level,
    aggregating_users_month_one.monthone_response_rate as monthone_response_rate,
    aggregating_users_month_one.listen_rate as monthone_listen_rate,
    case when aggregating_users_month_two.month_two_engagement_level is null then 0 else aggregating_users_month_two.month_two_engagement_level end as month_two_engagement_level,
    aggregating_users_month_two.monthtwo_response_rate as monthtwo_response_rate,
    aggregating_users_month_two.monthtwo_listen_rate as monthtwo_listen_rate,
    -- aggregating_users_month_three.month_three_engagement_level,
    -- aggregating_users_month_three.monththree_response_rate as monththree_response_rate,
    -- aggregating_users_month_three.monththree_listen_rate as monththree_listen_rate
from aggregating_users_month_one
left join aggregating_users_month_two using (user_id, data_source)
-- left join aggregating_users_month_one using (user_id, data_source)
left join picked_up_call using (user_id, data_source)
left join user_listens_entire_message using (user_id, data_source)
left join prompt_answered using (user_id, data_source)
left join immediate_callback using (user_id)
-- left join aggregating_users_month_three using (user_id, data_source)
where user_id is not null
-- )
-- select
    -- *
-- count(distinct user_id)
-- prompt_answered_coded,
-- count(prompt_answered_coded)
-- call_pickup,
-- count(call_pickup)
-- entire_message_heard,
-- count(entire_message_heard)
-- immediate_call_back,
-- count(immediate_call_back)
-- month_one_engagement_level,
-- count(month_three_engagement_level)
-- from bringing_everything_under_onetable
-- group by 1
-- where user_id = 75160