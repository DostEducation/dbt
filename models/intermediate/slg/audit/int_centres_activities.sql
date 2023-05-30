with activity as (Select * from {{ ref('stg_activities') }}),
    centre as (select * from {{ ref('stg_centres') }}),
    sector as (select * from {{ ref('stg_sectors') }}),
    block as (select * from {{ ref('stg_blocks') }}),
    joining_activity_centre as (
        select 
            activity.* except(sector_id),
            centre.sector_id as sector_id
        from activity
        left join centre using (centre_id)
        where activity.activity_level = 'Centre'
    ),
    joining_activity_ as (
        select 
            activity.* except(block_id),
            sector.block_id
        from activity
        left join sector using (sector_id)
        where activity.activity_level = 'Sector'
    )
select 
    *
from joining_activity_centre
-- where sector_id is null