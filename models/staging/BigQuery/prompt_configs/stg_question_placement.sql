with
    question_placement as (
        select
            safe_cast(id as integer) as question_placement_id,
            safe_cast(question_id as integer) as question_id,
            data_source,
            safe_cast(program_sequence_id as integer) as program_sequence_id,
            safe_cast(content_id as integer) as content_id,
            status as question_placement_status,
            position_relative_to_content,
            safe_cast(activation_date as date) as question_placement_activation_date,
            safe_cast(deactivation_date as date) as question_placement_deactivation_date,
            if(data_source = 'admindashboard', program_sequence_id, content_id) as unified_content_map_id,
        from {{ source("prompt_configs", "src_question_placement") }}
    )

select *
from question_placement
where
    unified_content_map_id is not null          -- ingore rows where neither program_seq_id nor content_id are populated
    and (
        question_placement_id <= 323            -- to filter out instances where more than 1 prompt has been placed
        or  question_placement_id >= 345        -- need to fix this using webhook response value
    )
