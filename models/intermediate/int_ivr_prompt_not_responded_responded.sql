with 
ivr_prompt_not_responded as (
    select
        prompt_response_id,
        response,
        data_source,
        date_trunc(created_on, day) created_on, 
        count(user_phone) as count_not_responded
    from {{ ref("int_parse_prompt_responses") }}
    where keypress is null
        -- and cast(created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
    group by 1,2,3,4
),
all_prompt_responses as (
    select 
        prompt_response_id,
        response,
        data_source,
        date_trunc(created_on, day) created_on,
        count(user_phone) as count_all_responded
        from {{ ref("int_parse_prompt_responses") }}
        group by 1,2,3,4
)
select
    a.prompt_response_id,
    a.response,
    a.data_source,
    a.created_on,
    (i.count_not_responded / a.count_all_responded) as percent_ivr_response
from ivr_prompt_not_responded i
left join all_prompt_responses a on a.prompt_response_id = i.prompt_response_id
                    and a.data_source = i.data_source
                    and a.created_on = i.created_on
group by 1,2,3,4,5

