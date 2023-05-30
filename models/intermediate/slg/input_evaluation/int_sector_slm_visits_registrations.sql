with
    sector_engagement as (select * from {{ ref("stg_activities") }}),
    sector_geographies as (select * from {{ ref("stg_sectors") }}),
    stakeholder as (select * from {{ ref("stg_stakeholders") }}),
    centre as (select * from {{ ref('stg_centres') }}),
    joining_offline_online_meeting as (
        select
            sector_geographies.sector_id,
            sector_geographies.sector_name,
            case
                when
                    sector_engagement.activity_type = 'offline_meeting'
                    or sector_engagement.activity_type = 'online_meeting'
                then 'meeting'
                else sector_engagement.activity_type
            end as activity
        from sector_geographies
        left join sector_engagement using (sector_id)

    ),
    counting_by_activity as (
        select sector_id, sector_name, activity
        from joining_offline_online_meeting
        where activity is not null
    ),
    calculating_activity_engagement as (
        select sector_id, sector_name,count(activity) as no_of_slms
        from counting_by_activity
        where activity = 'meeting'
        group by 1, 2
    ),
    calculate_centre_visits as (
        select
            sector_geographies.sector_id,
            (sum(cast(sector_engagement.centre_onboarding as int)) + sum(cast(sector_engagement.home_onboarding as int))) as reported_registrations,
            count(sector_engagement.activity_type) as no_of_centre_visits
        from centre
        left join sector_engagement using (centre_id)
        right join sector_geographies on centre.sector_id = sector_geographies.sector_id
        where activity_type = 'centre_visit'
        group by 1
    )
select
    sector_geographies.sector_id,
    sector_geographies.sector_name,
    calculating_activity_engagement.no_of_slms,
    calculate_centre_visits.no_of_centre_visits,
    calculate_centre_visits.reported_registrations
from sector_geographies
left join calculating_activity_engagement using (sector_id)
left join calculate_centre_visits using (sector_id)
order by no_of_slms desc
-- select
--     *
-- from calculate_centre_visits
-- -- order by no_of_slms desc