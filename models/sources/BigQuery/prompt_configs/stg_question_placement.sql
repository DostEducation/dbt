with
    question_placement as (
        select
            safe_cast(id as integer) as question_placement_id,
            question_id,
            data_source,
            program_sequence_id,
            content_id
            status,
            position_relative_to_content,
            safe_cast(activation_date as date) as activation_date,
            safe_cast(deactivation_date as date) as deactivation_date
        from {{ source("prompt_configs", "src_question_placement") }}
    )

select *
from question_placement
