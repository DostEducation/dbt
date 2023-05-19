with sector_geographies as (select * from {{ source('slg_analytics_config', 'src_sectors') }}),
    structuring_table as (
        select 
            id as sector_id,
            sector_name,
            block_id,
            sector_code,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from sector_geographies
    )
select 
    *
from structuring_table