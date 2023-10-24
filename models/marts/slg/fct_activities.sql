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
            centre_name
        from
            activities
            left join geographies using (activity_level, activity_level_id)
    ),

    add_dost_team_info as (
        select
            joining_all_geographies.*,
            dost_member_name,
            role
        from joining_all_geographies
        left join dost_team using (dost_team_id)
    )

select *
from add_dost_team_info