with centre_geographies as (select * from {{ ref('int_centre_geographies') }}),
    sector_geographies as (select * from {{ ref('int_sector_geographies') }}),
    block_geographies as (select * from {{ ref('int_block_geogrpahies') }}),
    activities as (select * from {{ ref('stg_activities') }}),
    block as (select * from {{ ref('stg_blocks') }}),
    centre_activity_blockwise as (
        select 
            centre_geographies.block_id,
            centre_geographies.block_name,
            count(activities.centre_id) as no_of_activities_centre
        from centre_geographies
        left join activities using (centre_id)
        where activities.activity_level = 'Centre'
        group by 1,2
    ),
    sector_activity_blockwise as (
        select 
            sector_geographies.block_id,
            sector_geographies.block_name,
            count(activities.sector_id) as no_of_activities_sector
        from sector_geographies
        left join activities using (sector_id)
        where activities.activity_level = 'Sector'
        group by 1,2
    ),
    block_activity_blockwise as (
        select 
            block_geographies.block_id,
            block_geographies.block_name,
            count(activities.block_id) as no_of_activities_block
        from block_geographies
        left join activities using (block_id)
        where activities.activity_level = 'Block'
        group by 1,2
    )
select
    block.block_id,
    block.block_name,
    centre_activity_blockwise.no_of_activities_centre,
    sector_activity_blockwise.no_of_activities_sector,
    block_activity_blockwise.no_of_activities_block
from block
left join centre_activity_blockwise using (block_id)
left join sector_activity_blockwise using (block_id)
left join block_activity_blockwise using (block_id)