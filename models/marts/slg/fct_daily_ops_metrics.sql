with
    database_registrations as (select * from {{ ref('int_registration_metrics') }}),
    app_registrations as (select * from {{ ref('int_activity_metrics') }}),
    date_geographies as (select * from {{ ref('int_date_geographies') }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),

    
    join_database_registration_with_geographies as (
        select
            date_geographies.*,
            registrations_on_database,
            signup
        from date_geographies
        left join database_registrations using (activity_level, activity_level_id,date)
    ),
    
    join_app_registration as (
        select
            join_database_registration_with_geographies.*,
            registration_on_app,
            centres_visited,
            centres_available
        from join_database_registration_with_geographies
        left join app_registrations using (activity_level, activity_level_id,date)
    ),
    calculate_registrations_over_reported as (
        select
            *,
            coalesce(registration_on_app, 0) - coalesce(registrations_on_database, 0) as registrations_over_reported
        from join_app_registration
    ),
    get_sector_assigned_to_info as (
        select
            calculate_registrations_over_reported.*,
            dost_member_name as sector_assigned_to_name,
            role as sector_assigned_to_role,
        from calculate_registrations_over_reported
        left join dost_team on sector_assigned_to_id = dost_team_id
    )
select
    *
from get_sector_assigned_to_info