with

    parsed_prompt_responses as (select * from {{ ref("int_parse_prompt_responses") }}),
    panel_users as (select * from {{ ref("int_panel_users") }}),

    join_tables as (
        select
            *,
            if(count_of_eval_phases > 1, true, false) as is_panel_user_response
        from
            parsed_prompt_responses
            left join panel_users using (user_id, data_source, baseline_question_id)
    )

select *
from join_tables

-- resolve duplicates