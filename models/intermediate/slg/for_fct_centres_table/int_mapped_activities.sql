with activities as (select * from {{ ref('stg_activities') }}),
    statewise_geo as (select * from {{ ref('stg_states') }}),
    districtwise_geo as (select * from {{ ref('int_district_geographies') }}),
    blockwise_geo as (select * from {{ ref('int_block_geogrpahies') }}),
    sectorwise_geo as (select * from {{ ref('int_sector_geographies') }}),
    centrewise_geo as (select * from {{ ref('int_centre_geographies') }}),
    joining_centrewise as (
        select 
            activities.* except (state_id, district_id, block_id, sector_id, centre_id),
            activities.centre_id,
            centrewise_geo.* except (centre_id, centre_name, sector_name, block_name, district_name, state_name, total_beneficiaries,sector_code)
        from activities
        left join centrewise_geo using (centre_id)
        where activity_level = 'Centre'
    ),
    joining_sectorwise as (
        select 
            activities.* except (state_id, district_id, block_id, sector_id, centre_id),
            activities.centre_id,
            activities.sector_id,
            sectorwise_geo.* except (sector_id, sector_name, block_name, district_name, state_name, sector_code)
        from activities
        left join sectorwise_geo using (sector_id)
        where activity_level = 'Sector'
    ),
    joining_blockwise as (
        select 
            activities.* except (state_id, district_id, block_id, sector_id, centre_id),
            activities.centre_id,
            activities.sector_id,
            activities.block_id,
            blockwise_geo.* except (block_id, block_name, district_name, state_name)
        from activities
        left join blockwise_geo using (block_id)
        where activity_level = 'Block'
    ),
    joining_districtwise as (
        select 
            activities.* except (state_id, district_id, block_id, sector_id, centre_id),
            activities.centre_id,
            activities.sector_id,
            activities.block_id,
            activities.district_id,
            districtwise_geo.* except (district_id, district_name, state_name)
        from activities
        left join districtwise_geo using (district_id)
        where activity_level = 'District'
    ),
    joining_statewise as (
        select 
            activities.* except (state_id, district_id, block_id, sector_id, centre_id),
            activities.centre_id,
            activities.sector_id,
            activities.block_id,
            activities.district_id,
            statewise_geo.* except (state_name, created_on, updated_on)
        from activities
        left join statewise_geo using (state_id)
        where activity_level = 'State'
    )

select
    *
from joining_centrewise
UNION ALL
select 
    * 
from joining_sectorwise
UNION ALL
select 
    * 
from joining_blockwise
UNION ALL
select
    * 
from joining_districtwise
UNION ALL
select 
    *
from joining_statewise