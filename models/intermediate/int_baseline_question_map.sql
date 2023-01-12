with

    questions as (select * from {{ ref("stg_questions") }}),

    baseline_questions as (
        select
            question_id as baseline_question_id,
            question_english as baseline_question_english,
        from questions
        where
            true and evaluation_phase = 'Baseline' and question_status <> 'NotDeployed'
    ),
    
    map_midline_and_endline_questions as (
        select
            baseline_questions.*,
            mapped_mid_and_endline_questions.evaluation_phase as mapped_evaluation_phase,
            mapped_mid_and_endline_questions.question_id as mapped_question_id,
            mapped_mid_and_endline_questions.question_english as mapped_question_english
        from baseline_questions
        left join
            questions as mapped_mid_and_endline_questions
            on mapped_mid_and_endline_questions.parent_question_id
            = baseline_questions.baseline_question_id
            or mapped_mid_and_endline_questions.question_id
            = baseline_questions.baseline_question_id
        group by 1, 2, 3, 4, 5
    )

select *
from map_midline_and_endline_questions
