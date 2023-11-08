with
    database_registrations as (select * from {{ ref('int_registration_metrics') }}),
    app_registrations as (select * from {{ ref('int_activity_metrics') }}),

    join_tables as (
        select
            database_registrations.*,
            registration_on_app,
            centres_visited,
            centres_available
        from database_registrations
        full outer join app_registrations using (activity_level, activity_level_id,date)
    ),
    calculate_registrations_over_reported as (
        select
            *,
            coalesce(registration_on_app, 0) - coalesce(registrations_on_database, 0) as registrations_over_reported
        from join_tables
    )

select *
from calculate_registrations_over_reported