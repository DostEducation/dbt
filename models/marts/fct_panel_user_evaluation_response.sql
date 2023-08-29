with
    prompt_response as (select * from {{ ref("fct_prompt_responses") }}),
    pulling_required_columns as (
        select
            user_phone,
            user_id,
            baseline_question_id,
            question_english,
            evaluation_phase,
            desired_response,
            response_english,
            ivr_prompt_response_created_on
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
            end as desired_response
        from counting_desired_response
        group by 1, 2, 3, 4, 5, 6,7

    ),
    putting_row_number as (
        select
            *,
            row_number() over (partition by user_id, baseline_question_id order by ivr_prompt_response_created_on desc) as row_number
        from case_when
        where evaluation_phase = 'Baseline'
    ),
    latest_response as (
        select
            *
        from putting_row_number
        where row_number = 1
    ),
    joining_latest_response_case_when as(
        select
            case_when.*,
            latest_response.response_english as latest_response_english
        from case_when
        left join latest_response using(user_id, baseline_question_id)
        where case_when.evaluation_phase = 'Baseline'
    ),
    baseline_columns as (
        select
            *except(response_english, ivr_prompt_response_created_on,desired_response,latest_response_english),
            case 
                when desired_response = 'Yes' then response_english
            else latest_response_english
            end as normalized_baseline_response_english
        from joining_latest_response_case_when
    ),
    putting_row_number_for_endline as (
        select
            *,
            row_number() over (partition by user_id, baseline_question_id order by ivr_prompt_response_created_on desc) as row_number
        from case_when
        where evaluation_phase = 'Endline'
    ),
    latest_response_for_endline as (
        select
            *
        from putting_row_number_for_endline
        where row_number = 1
    ),
    joining_latest_response_case_when_for_endline as(
        select
            case_when.*,
            latest_response_for_endline.response_english as latest_response_english_endline
        from case_when
        left join latest_response_for_endline using(user_id, baseline_question_id)
        where case_when.evaluation_phase = 'Endline'
    ),
    endline_columns as (
        select
            *except(response_english, ivr_prompt_response_created_on,desired_response,latest_response_english_endline),
            case 
                when desired_response = 'Yes' then response_english
            else latest_response_english_endline
            end as normalized_endline_response_english
        from joining_latest_response_case_when_for_endline
    )
select
    baseline_columns.*except(evaluation_phase),
    endline_columns.normalized_endline_response_english
from baseline_columns
left join endline_columns using (user_id, baseline_question_id)
where endline_columns.normalized_endline_response_english is not null