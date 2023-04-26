with block_geographies as (select * from {{ source('slg_analytics_config', 'src_block_level_geographies') }} ),
    structuring_block as (
        select 
            block_id,
            block_name,
            district_id,
            district_name,
            state_id,
            state_name,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from block_geographies
    )
select 
    *
from structuring_block