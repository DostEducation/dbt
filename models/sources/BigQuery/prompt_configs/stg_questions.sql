with questions as (
    select
        safe_cast(id as integer) as question_id,
        question_english,
        safe_cast(indicator_id as integer) as indicator_id,
        status as question_status,
        safe_cast(activation_date as date) as question_activation_date,
        safe_cast(deactivation_date as date) as question_deactivation_date,
        related_module_id,
        evaluation_phase,
        parent_question_id
    from {{ source ('prompt_configs', 'src_questions')}}
)

select * from questions