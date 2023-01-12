with
    program_wise_engagement as (
        select
            id,
            uuid,
            user_id,
            partner_id,
            status as user_status,
            created_on as user_created_on,
            parent_type,
            program_id as registration_program_id,
            district,
            state,
            is_child_between_0_3,
            is_child_between_3_6,
            is_child_above_6,
            has_no_child,
            has_smartphone,
            education_level,
            occupation,
            gender_of_child
        from {{ source("unified_data_source", "registration") }}
    )

select *
from program_wise_engagement
