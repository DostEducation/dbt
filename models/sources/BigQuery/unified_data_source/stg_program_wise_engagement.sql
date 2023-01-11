with
    program_wise_engagement as (
        select
            data_source,
            user_id,
            program_name,
            first_user_week_engagement_level,
            first_user_month_engagement_level,
            engagement_level as overall_engagement_level,
            program_start_date,
            program_end_date,
            user_age
        from {{ source("unified_data_source", "program_wise_engagement") }}
    )

select *
from program_wise_engagement
