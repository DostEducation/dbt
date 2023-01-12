with

    baseline_question_map as (select * from {{ ref('int_baseline_question_map') }}),

    parsed_prompt_responses as (select * from {{ ref('int_parse_prompt_responses') }}),

    -- find baseine questiosn that have at least 1 midline or endline qusetion mapped
    eval_phases_available as (
        select
            baseline_question_id,
            count(distinct mapped_evaluation_phase) as count_of_eval_phases
        from baseline_question_map
        where mapped_question_id is not null
        group by 1
        having count_of_eval_phases > 1
    ),

    -- find users who have valid responses to both evaluation phases
    panel_users_with_baseline_question_id as (
        select
            parsed_prompt_responses.data_source,
            parsed_prompt_responses.user_id,
            parsed_prompt_responses.baseline_question_id,
            count(distinct parsed_prompt_responses.evaluation_phase) as count_of_eval_phases
        from
        parsed_prompt_responses
        -- right join eval_phases_available using (baseline_question_id)
        where
            parsed_prompt_responses.desired_response is not null
            and parsed_prompt_responses.user_id is not null
        group by
            1, 2, 3
        having
            count_of_eval_phases > 1
    )

select * from panel_users_with_baseline_question_id