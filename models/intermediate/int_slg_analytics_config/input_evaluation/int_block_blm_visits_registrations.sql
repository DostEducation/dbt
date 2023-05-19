with
    block_engagement as (select * from {{ ref("stg_activities") }}),
    blocks as (select * from {{ ref('stg_blocks') }}),
    joining_offline_online_meeting as (
        select
            blocks.block_id,
            blocks.block_name,
            case
                when
                    block_engagement.activity_type = 'offline_meeting'
                    or block_engagement.activity_type = 'online_meeting'
                then 'meeting'
                else block_engagement.activity_type
            end as activity
        from blocks
        left join block_engagement using (block_id)

    ),
    counting_by_activity as (
        select block_id, block_name, activity
        from joining_offline_online_meeting
        where activity is not null
    ),
    calculating_activity_engagement as (
        select block_id, block_name,count(activity) as no_of_blms
        from counting_by_activity
        where activity = 'meeting'
        group by 1, 2
    ),
    calculate_centre_visits as (
        select
            block_id,
            (if(sum(cast(centre_onboarding as int)) is null,0,sum(cast(centre_onboarding as int))) + (if(sum(cast(home_onboarding as int)) is null,0,sum(cast(home_onboarding as int))))) as reported_registrations,
            count(block_id) as no_of_centre_visits
        from block_engagement
        where activity_type = 'centre_visit'
        group by 1
    )
select
    blocks.block_id,
    blocks.block_name,
    calculating_activity_engagement.no_of_blms,
    calculate_centre_visits.no_of_centre_visits,
    calculate_centre_visits.reported_registrations
from blocks
left join calculating_activity_engagement using (block_id)
left join calculate_centre_visits using (block_id)
order by no_of_blms desc
-- select
--     *
-- from calculate_centre_visits
-- -- order by no_of_slms desc