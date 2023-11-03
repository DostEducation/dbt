with
    activities as (select * from {{ ref('stg_activities') }}),
    geographies as (select * from {{ ref('int_geographies') }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),
    
    get_geography_labels as (
        select
            activities.*,
            state_name,
            district_name,
            block_name,
            sector_name,
            centre_name,
            sector_assigned_to_id,
            sector_assigned_to_name,
            sector_assigned_to_role
        from
            activities
            left join geographies using (activity_level, activity_level_id)
    ),

    add_dost_team_info as (
    select
        get_geography_labels.*,
        dost_member_name as activity_performed_by,
        role as activity_performed_by_role,
    from get_geography_labels
    left join dost_team using (dost_team_id)
    )

select *
from add_dost_team_info