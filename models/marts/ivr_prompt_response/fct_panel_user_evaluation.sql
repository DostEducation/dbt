with
    prompt_response as (select * from {{ ref("fct_prompt_responses") }}),
    pulling_required_columns as (
        select
            user_phone,
            user_id,
            question_id,
            baseline_question_id,
            question_english,
            evaluation_phase,
            desired_response
        from prompt_response
        where is_panel_user_response
    ),
    counting_desired_response as (
        select
            * except (desired_response),
            case
                when desired_response = 'Yes'
                then 1
                when desired_response = 'No'
                then 0
                else null
            end as desired_response
        from pulling_required_columns
    ),
    case_when as (
        select
            * except (desired_response),
            case
                when sum(desired_response) >= 1 then 'Yes' else 'No'
            end as desired_response_string
        from counting_desired_response
        group by 1, 2, 3, 4, 5, 6

    ),
    pivoting_baseline as (
        select * except (baseline_question_id)
        from
            case_when 
            pivot (
                max(desired_response_string) for evaluation_phase
                in ('Baseline' as baseline_desired_response)
            )
    ),
    pivoting_endline as (
        select *
        from
            case_when 
            pivot (
                max(desired_response_string) for evaluation_phase
                in ('Endline' as endline_desired_response)
            )
    ),
    joining_evaluation_phase as (
        select pivoting_baseline.*, pivoting_endline.endline_desired_response
        from pivoting_baseline
        left join
            pivoting_endline
            on pivoting_baseline.user_phone = pivoting_endline.user_phone
            and pivoting_baseline.question_id = pivoting_endline.baseline_question_id
    )
select *
from joining_evaluation_phase
where endline_desired_response is not null
