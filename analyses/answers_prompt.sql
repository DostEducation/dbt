with answers_prompt as (select * from {{ ref('fct_prompt_responses') }}),
    binning_prompt_answer as(
        select
            *,
            case 
                when keypress = -2 then "2. Did not answer prompt"
                when keypress != -2 then "1. Prompt answered"
                else null
                end as users_answered_notanswered_prompt
        from answers_prompt
    )
select
    users_answered_notanswered_prompt,
    count(users_answered_notanswered_prompt)
from binning_prompt_answer
where data_source = "admindashboard"
group by 1