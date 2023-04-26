with sector_geographies as (select * from {{ source('slg_analytics_config', 'src_geographies') }}),
    structuring_table as (
        select 
            id as sector_id,
            sector_name,
            block_id,
            block_name,
            district_id,
            district_name,
            state_id,
            state_name,
            notes,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from sector_geographies
    )
select 
    *
from structuring_table