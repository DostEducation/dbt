with block_geographies as (select * from {{ ref('int_block_geogrpahies') }}),
    sector as (select * from {{ ref('stg_sectors') }}),
    sector_geographies as (
        select 
            sector.* except(created_on, updated_on, block_id),
            sector.block_id,
            block_geographies.* except(block_id)
        from block_geographies
        left join sector using (block_id)

    )
select 
    * 
from sector_geographies