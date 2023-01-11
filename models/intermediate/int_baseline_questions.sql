with
    questions as (
        select question_id, question_english, evaluation_phase, parent_question_id
        from {{ ref("stg_questions") }}
    ),

    baseline_questions as (
        select *
        from questions
        where
            questions.parent_question_id is null
            and questions.evaluation_phase = 'Baseline'
    ),

    midline_questions as (select * from questions where evaluation_phase = 'Midline'),
    endline_questions as (select * from questions where evaluation_phase = 'Endline'),

    count_eval_phases_available as (
        select
            baseline_questions.question_id as baseline_question_id,
            baseline_questions.question_english as baseline_question_english,
            baseline_questions.evaluation_phase as baseline_evaluation_phase,
            midline_questions.question_id as midline_question_id,
            midline_questions.question_english as midline_question_english,
            midline_questions.evaluation_phase as midline_evaluation_phase,
            endline_questions.question_id as endline_questions_id,
            endline_questions.question_english as endline_questions_english,
            endline_questions.evaluation_phase as endline_evaluation_phase,
            case when midline_questions.question_id is null then 0 else 1 end
            + case when endline_questions.question_id is null then 0 else 1 end
            + 1 as count_of_eval_phases_available  -- for eval phase = baseline
        from baseline_questions
        left join
            midline_questions
            on midline_questions.parent_question_id = baseline_questions.question_id
        left join
            endline_questions
            on endline_questions.parent_question_id = baseline_questions.question_id
            or endline_questions.parent_question_id = midline_questions.question_id
    )

select *
from count_eval_phases_available
