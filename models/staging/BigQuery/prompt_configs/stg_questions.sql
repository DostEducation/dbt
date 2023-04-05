with
    questions as (
        select
            safe_cast(id as integer) as question_id,
            question_english,
            safe_cast(indicator_id as integer) as indicator_id,
            status as question_status,
            safe_cast(activation_date as date) as question_activation_date,
            safe_cast(deactivation_date as date) as question_deactivation_date,
            safe_cast(related_module_id as integer) as related_module_id,
            evaluation_phase,
            safe_cast(parent_question_id as integer) as parent_question_id,
        from {{ source("prompt_configs", "src_questions") }}
        -- where question_english is not null
    )

select
    *,
    if(evaluation_phase = 'Baseline', question_id, parent_question_id) as baseline_question_id
from questions
