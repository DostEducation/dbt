{{
    config(
        materialized="incremental",
        unique_key=["data_source", "prompt_response_id"],
        on_schema_change="fail",

        partition_by={
        "field": "ivr_prompt_response_created_on",
        "data_type": "timestamp",
        "granularity": "day"
        },

        cluster_by="data_source"
    )
}}

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
            user_engagement_level.overall_engagement_level,
            user_dimensions.* except (user_id, data_source, user_phone)
        from parsed_prompt_responses
        left join panel_users using (user_id, data_source, baseline_question_id)
        left join user_engagement_level using (user_id, data_source, program_name)
        left join user_dimensions using (user_id, data_source, user_phone)

        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where
            ivr_prompt_response_updated_on
            > (select max(ivr_prompt_response_updated_on) from {{ this }})
        {% endif %}
    ),
    calculating_user_week_at_time_of_call as(
        select  
            *,
            CASE 
            WHEN CAST(FLOOR(DATE_DIFF(DATE(created_on), DATE(TIMESTAMP_ADD(user_created_on, INTERVAL 330 MINUTE)), DAY)/7) AS INTEGER) + 1 > 0
            THEN CAST(FLOOR(DATE_DIFF(DATE(created_on), DATE(TIMESTAMP_ADD(user_created_on, INTERVAL 330 MINUTE)), DAY)/7) AS INTEGER) + 1 
            ELSE 0
            END AS user_week_at_time_of_call
        from join_tables
    )

select *
from calculating_user_week_at_time_of_call
