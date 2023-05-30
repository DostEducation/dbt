with
    sector_engagement as (select * from {{ ref("stg_activities") }}),
    sector_geographies as (select * from {{ ref("stg_sectors") }}),
    stakeholder as (select * from {{ ref("stg_stakeholders") }}),
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
            end as activity,
            FORMAT_DATE( '%Y%m',sector_engagement.date_of_meeting) AS year_and_month
        from sector_geographies
        left join sector_engagement using (sector_id)

    ),
    counting_by_activity as (
        select sector_id, sector_name, activity, year_and_month
        from joining_offline_online_meeting
        where activity is not null
    ),
    calculating_activity_engagement as (
        select sector_id, sector_name, year_and_month, count(activity) as no_of_slms
        from counting_by_activity
        where activity = 'meeting'
        group by 1, 2, 3
    ),

    calculating_tracker_dist as (
        select
            sector_engagement.sector_id,
            FORMAT_DATE( '%Y%m',sector_engagement.date_of_meeting) as year_and_month,
            sum(
                cast(sector_engagement.tracker_distribution_attendees as int)
            ) as trackers_distributed,
            sum(
                cast(sector_engagement.tracker_filling_attendees as int)
            ) as trackers_filled
        from sector_engagement
        group by 1, 2
    ),
    calculating_no_aww as (
        select calculating_tracker_dist.*, count(stakeholder.designation) as no_of_aww
        from calculating_tracker_dist
        right join stakeholder using (sector_id)
        group by 1, 2, 3, 4
    ),
    calculating_percent_trackers as (
        select
            calculating_no_aww.sector_id,
            sector_geographies.sector_name,
            calculating_no_aww.year_and_month,
            (nullif(calculating_no_aww.trackers_distributed, 0))
            / (nullif(calculating_no_aww.no_of_aww, 0))
            * 100 as percent_trackers_distributed,
            (nullif(calculating_no_aww.trackers_filled, 0))
            / (nullif(calculating_no_aww.no_of_aww, 0))
            * 100 as percent_trackers_filled,
        from calculating_no_aww
        left join sector_geographies using (sector_id)
    ),
    joining_sector_geographies as (
        select
            sector_geographies.sector_id,
            sector_geographies.sector_name,
            calculating_activity_engagement.year_and_month,
            calculating_activity_engagement.no_of_slms
        from sector_geographies
        left join calculating_activity_engagement using (sector_id)
    ),
    joining_sector_trackers as (
        select
            joining_sector_geographies.*,
            calculating_percent_trackers.percent_trackers_distributed,
            calculating_percent_trackers.percent_trackers_filled
        from joining_sector_geographies
        left join
            calculating_percent_trackers
            on joining_sector_geographies.sector_id
            = calculating_percent_trackers.sector_id
            and joining_sector_geographies.year_and_month
            = calculating_percent_trackers.year_and_month
    )

select
    sector_id,
    sector_name,
    year_and_month,
    case
        when no_of_slms >= 2 and percent_trackers_distributed >= 70 and percent_trackers_filled >= 70 then 'high'
        when
            no_of_slms = 1
            and percent_trackers_distributed >= 40
            and percent_trackers_filled >= 40
        then 'medium'
        when
            no_of_slms is null
            and percent_trackers_distributed is null
            and percent_trackers_filled is null
        then null
        else 'low'
    end as engagement_level
from joining_sector_trackers
