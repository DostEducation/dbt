with
    registration as (
        select
            id as registration_id,
            uuid as registration_uuid,
            data_source,
            user_id,
            user_phone,
            partner_id,
            status as user_status,
            created_on as user_created_on,
            parent_type,
            program_id as registration_program_id,
            district,
            state,
            block,
            sector,
            is_child_between_0_3,
            is_child_between_3_6,
            is_child_above_6,
            has_no_child,
            has_smartphone,
            education_level,
            occupation,
            gender_of_child,
            signup_date
        from {{ source("unified_data_source", "registration") }}
        where migrated_on <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 MINUTE)
    )

select *
from registration

