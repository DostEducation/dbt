with

    parsed_prompt_responses as (select * from {{ ref("int_parse_prompt_responses") }}),
    panel_users as (select * from {{ ref("int_panel_users") }}),
    user_engagement_level as (select * from {{ ref("int_dedupe_user_engagement") }}),
    user_dimensions as (select * from {{ ref("int_user_dimensions") }}),

    join_tables as (
        select
            parsed_prompt_responses.*,
            panel_users.count_of_eval_phases,
            if(count_of_eval_phases > 1, true, false) as is_panel_user_response,
            user_engagement_level.first_user_week_engagement_level,
            user_engagement_level.first_user_month_engagement_level,
            user_engagement_level.overall_engagement_level
        from
            parsed_prompt_responses
            left join panel_users using (user_id, data_source, baseline_question_id)
            left join user_engagement_level using (user_id, data_source, program_name)
    )

select *
from join_tables