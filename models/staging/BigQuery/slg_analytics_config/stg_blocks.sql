with
    source as (select * from {{ source("slg_analytics_config", "src_blocks") }}),

    setup_columns as (
        select
            id as block_id,
            block_name,
            district_id,
            no_of_awc,
            potential_users,
            projected_registrations,
            projected_signups,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on
        from source
    )

select *
from setup_columns
