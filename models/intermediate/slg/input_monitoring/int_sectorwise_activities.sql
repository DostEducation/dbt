with centre_geographies as (select * from {{ ref('int_centre_geographies') }}),
    sector_geographies as (select * from {{ ref('int_sector_geographies') }}),
    activities as (select * from {{ ref('stg_activities') }}),
    sector as (select * from {{ ref('stg_sectors') }}),
    centre_activity_sectorwise as (
        select 
            centre_geographies.sector_id,
            centre_geographies.sector_name,
            count(activities.centre_id) as no_of_activities_centre
        from centre_geographies
        left join activities using (centre_id)
        where activities.activity_level = 'Centre'
        group by 1,2
    ),
    sector_activity_sectorwise as (
        select 
            sector_geographies.sector_id,
            sector_geographies.sector_name,
            count(activities.sector_id) as no_of_activities_sector
        from sector_geographies
        left join activities using (sector_id)
        where activities.activity_level = 'Sector'
        group by 1,2
    )
select
    sector.sector_id,
    sector.sector_name,
    centre_activity_sectorwise.no_of_activities_centre,
    sector_activity_sectorwise.no_of_activities_sector
from sector
left join centre_activity_sectorwise using (sector_id)
left join sector_activity_sectorwise using (sector_id)