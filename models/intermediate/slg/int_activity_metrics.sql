with
    activities as (select * from {{ ref("fct_activities") }}),
    date_geographies as (select * from {{ ref('int_date_geographies') }}),

    -- registrations on app
    calculate_registrations_on_app as (
        select
            activity_level,
            activity_level_id,
            cast(activities.date_of_meeting as date) as date,
            sum(
                registration_on_app
            ) as registration_on_app
        from activities
        group by 1, 2, 3
    ),
    join_registrations_on_app as(
        select
            *
        from date_geographies
        left join calculate_registrations_on_app using (activity_level, activity_level_id, date)
    ),
    rollup_registrations_on_app as(
        select
            * except (registration_on_app),
            case
                when activity_level = 'Centre' then sum(registration_on_app) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(registration_on_app) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(registration_on_app) over (partition by block_id, date)
                when activity_level = 'District' then sum(registration_on_app) over (partition by district_id, date)
                when activity_level = 'State' then sum(registration_on_app) over (partition by state_id, date)                
            end as registration_on_app
        from join_registrations_on_app
    ),

    -- centres visited
    calculate_centres_visited as (
        select 
            'Centre' as activity_level,
            centre_id as activity_level_id,
            cast(date_of_meeting as date) as date,
            1 as centres_visited
        from activities
        where activity_type = 'Centre Visit Onboarding' and activity_level = 'Centre'
        group by 1, 2, 3
    ),
    join_centres_visited as (
        select
            *
        from rollup_registrations_on_app
        left join calculate_centres_visited using (activity_level, activity_level_id, date)
    ),
    roll_up_centres_visited as (
        select
            * except (centres_visited),
            case
                when activity_level = 'Centre' then sum(centres_visited) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(centres_visited) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(centres_visited) over (partition by block_id, date)
                when activity_level = 'District' then sum(centres_visited) over (partition by district_id, date)
                when activity_level = 'State' then sum(centres_visited) over (partition by state_id, date)                
            end as centres_visited
        from join_centres_visited
    ),
    calculate_centres_available as (
        select
            *,
            case
                when activity_level = 'Centre' then 1 
                when activity_level = 'Sector' then count(distinct centre_id) over (partition by sector_id, date)
                when activity_level = 'Block' then count(distinct centre_id) over (partition by block_id, date)
                when activity_level = 'District' then count(distinct centre_id) over (partition by district_id, date)
                when activity_level = 'State' then count(distinct centre_id) over (partition by state_id, date)                
            end as centres_available
        from roll_up_centres_visited
    )

select *
from calculate_centres_available