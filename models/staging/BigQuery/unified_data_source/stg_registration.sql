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
            district as district_name,
            state,
            block as block_name,
            sector as sector_name,
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
    ),
    fixing_block_district_names as (
        select
            * except(district_name, block_name),
            case when district_name = 'Udham Singh Nagar' then 'USN'
            when district_name = 'Dehradun' then 'DDN'
            when district_name = 'Nanital' then 'Nainital'
            else district_name
            end as district_name,
            case
                when block_name = 'Vikasnagar' then 'Vikashnagar'
                when block_name = 'Sitargunj' then 'Sitarganj'
                when block_name = 'Dehradun City' then 'DDN City'
                when block_name = 'Baajpur' then 'Bazpur'
            else block_name
            end as block_name
        from registration

    )

select *
from fixing_block_district_names