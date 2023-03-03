with user_info as (
    select 
        user_id,
        data_source
    from {{ ref('stg_users') }}
),
intro_message as (
    select
        user_id,
        data_source,
        user_listen_intro_bucket
    from {{ ref('int_user_listens_intro_message') }}
),
call_back_immediate as(
    select
        user_id,
        case 
            when delay_in_minutes <= 1 then 1
        else null
        end as user_gets_immediate_callback
    from {{ ref('int_immediate_callback_milestone_signup') }}
),
user_month_one_engagement as (
    select
        user_id,
        data_source,
        case 
            when first_user_month_engagement_level = "high" then 3
            when first_user_month_engagement_level = "medium" then 2
            when first_user_month_engagement_level = "low" then 1
        else null
        end as month_one_user_engagement
    from {{ ref('int_userwise_engagement_level') }}
),
user_answer_prompt as (
    with unique_userid_responded as (

        select
            user_id,
            data_source,
            keypress
        from {{ ref('fct_prompt_responses') }}
        where keypress >= -1
            and data_source = "admindashboard"
    )
    select
        distinct user_id,
        data_source,
        case 
            when keypress != -2 or keypress is not null then 1
        else 0
        end as user_responded_prompt
    from unique_userid_responded
)

select
    user_info.user_id,
    user_info.data_source,
    case 
        when call_back_immediate.user_gets_immediate_callback is null then 0
    else call_back_immediate.user_gets_immediate_callback
    end as user_get_immediate_callback,
    case 
        when intro_message.user_listen_intro_bucket is null then 0
    else intro_message.user_listen_intro_bucket
    end as user_listens_intro_message,
    case 
        when user_answer_prompt.user_responded_prompt is null then 0
    else user_answer_prompt.user_responded_prompt
    end as user_answered_prompt,
    user_month_one_engagement.month_one_user_engagement
from user_info
left join intro_message using(user_id, data_source)
left join call_back_immediate using(user_id)
left join user_month_one_engagement using (user_id, data_source)
left join user_answer_prompt using (user_id, data_source)
where user_info.data_source = "admindashboard"