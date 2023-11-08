with
    registration_metrics as (select * from {{ ref('int_registration_metrics') }}),
    activity_metrics as (select * from {{ ref('int_activity_metrics') }}),

    join_tables as (
        select
            registration_metrics.*,
            registration_on_app,
            centres_visited,
            centres_available
        from registration_metrics
        full outer join activity_metrics using (activity_level, activity_level_id,date)
    ),
    calculate_registrations_over_reported as (
        select
            *,
            coalesce(registration_on_app, 0) - coalesce(registrations_on_database, 0) as registrations_over_reported
        from join_tables
    )

select *
from calculate_registrations_over_reported