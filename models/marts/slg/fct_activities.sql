with mapped_activities as (select * from {{ ref('int_mapped_activities') }}),
    centre_geographies as (select * from {{ ref('stg_centres') }}),
    sector_geographies as (select * from {{ ref('stg_sectors') }}),
    block_geographies as (select * from {{ ref('stg_blocks') }}),
    district_geographies as (select * from {{ ref('stg_districts') }}),
    state_geographies as (select * from {{ ref('stg_states') }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),
    joining_centre_name as (
        select 
            mapped_activities.* except(state_id,district_id, block_id, sector_id),
            centre_geographies.centre_name,
            centre_geographies.total_beneficiaries,
            mapped_activities.sector_id,
            mapped_activities.block_id,
            mapped_activities.district_id,
            mapped_activities.state_id
        from mapped_activities
        left join centre_geographies using (centre_id)       
    ),
    joining_sector_name as (
        select 
            joining_centre_name.* except(state_id,district_id, block_id),
            sector_geographies.sector_name,
            joining_centre_name.block_id,
            joining_centre_name.district_id,
            joining_centre_name.state_id
        from joining_centre_name
        left join sector_geographies using (sector_id)
    ),
    joining_block_name as (
        select 
            joining_sector_name.* except(state_id,district_id),
            block_geographies.block_name,
            joining_sector_name.district_id,
            joining_sector_name.state_id
        from joining_sector_name
        left join block_geographies using (block_id)
    ),
    joining_district_name as (
        select 
            joining_block_name.* except(state_id),
            district_geographies.district_name,
            joining_block_name.state_id
        from joining_block_name
        left join district_geographies using (district_id)
    ),
    joining_state_name as (
        select 
            joining_district_name.*,
            state_geographies.state_name
        from joining_district_name
        left join state_geographies using (state_id)
    ),
    dost_team_name as (
        select
            joining_state_name.* except(dost_team_id, activity_level,activity_type, centre_id,centre_name,total_beneficiaries, sector_id, sector_name, block_id,block_name, district_id, district_name, state_id, state_name),
            joining_state_name.dost_team_id,
            dost_team.dost_member_name,
            joining_state_name.activity_level,
            joining_state_name.activity_type,
            joining_state_name.centre_id,
            joining_state_name.centre_name,
            joining_state_name.total_beneficiaries,
            joining_state_name.sector_id,
            joining_state_name.sector_name,
            joining_state_name.block_id,
            joining_state_name.block_name,
            joining_state_name.district_id,
            joining_state_name.district_name,
            joining_state_name.state_id,
            joining_state_name.state_name
        from joining_state_name
        left join dost_team using (dost_team_id)
    )    

select 
    *
from dost_team_name
