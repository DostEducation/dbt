with block_geographies as (select * from {{ source('slg_analytics_config', 'src_blocks') }} ),
    structuring_block as (
        select 
            id as block_id,
            block_name,
            district_id,
            no_of_awc,
            potential_users,
            projected_registrations,
            projected_signups,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from block_geographies
    )
select 
    *
from structuring_block