with
    activities as (select * from {{ ref('stg_activities') }}),
    geographies as (select * from {{ ref('int_geographies') }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),
    
    joining_all_geographies as (
        select
            activities.*,
            state_name,
            district_name,
            block_name,
            sector_name,
            centre_name,
            sectors_assigned_to_id
        from
            activities
            left join geographies
                on case
                    when activities.activity_level = 'State' then activities.state_id = geographies.state_id and geographies.activity_level = 'State'
                    when activities.activity_level = 'District' then activities.district_id = geographies.district_id and geographies.activity_level = 'District'
                    when activities.activity_level = 'Block' then activities.block_id = geographies.block_id and geographies.activity_level = 'Block'
                    when activities.activity_level = 'Sector' then activities.sector_id = geographies.sector_id and geographies.activity_level = 'Sector'
                    when activities.activity_level = 'Centre' then activities.centre_id = geographies.centre_id and geographies.activity_level = 'Centre'
                end
    ),

    add_dost_team_info as (
    select
        joining_all_geographies.*,
        dost_member_name,
        role
    from joining_all_geographies
    left join dost_team using (dost_team_id)
    ),
    add_sectors_assigned_to as (
        select
            add_dost_team_info.*,
            dost_team.dost_member_name as sector_assigned_to
        from add_dost_team_info
        left join dost_team on dost_team.dost_team_id = add_dost_team_info.sectors_assigned_to_id
    )

select *
from add_sectors_assigned_to