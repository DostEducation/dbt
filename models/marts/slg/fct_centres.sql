with activities as (select * from {{ ref('fct_activities') }} where activity_level = 'Centre' and activity_type = 'Centre Visit Onboarding'),
    geography as (select * from {{ ref('int_geographies') }} where activity_level = 'Centre'),
    dost_team as (select * from {{ ref('stg_dost_team') }}),
    join_the_two as (
        select
            distinct activities.centre_name as centres_visited,
            geography.*,
            date_of_meeting
        from geography
        left join activities using (centre_id)
    ),
    add_dost_team_name as (
        select
            join_the_two.*,
            dost_member_name as sector_assigned_to_name
        from join_the_two
        left join dost_team on sector_assigned_to_id = dost_team_id
    ),
    select_latest_record as (
        select
            *,
            row_number() over (
                partition by centre_id order by date_of_meeting desc
            ) as row_number
        from add_dost_team_name
    )

select
    *
from select_latest_record
where row_number = 1 
    -- and centre_id = '7125ec64'
