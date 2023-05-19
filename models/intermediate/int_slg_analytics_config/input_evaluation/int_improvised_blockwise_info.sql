with
    block_engagement as (select * from {{ ref("stg_activities") }}),
    blocks as (select * from {{ ref('stg_blocks') }}),
    sectors as (select * from {{ ref('stg_sectors') }}),
    joining_offline_online_meeting as (
        select
            sectors.block_id,
            sectors.sector_id,
            sectors.sector_name,
            case
                when
                    block_engagement.activity_type = 'offline_meeting'
                    or block_engagement.activity_type = 'online_meeting'
                then 'meeting'
                else block_engagement.activity_type
            end as activity
        from sectors
        left join block_engagement using (sector_id)

    ),
    counting_by_activity as (
        select blocks.block_id, blocks.block_name, joining_offline_online_meeting.activity
        from blocks
        left join joining_offline_online_meeting using (block_id)
        where activity is not null
    ),
    calculating_activity_engagement as (
        select block_id, block_name,count(activity) as no_of_blms
        from counting_by_activity
        where activity = 'meeting'
        group by 1, 2
    ),
    blockwise_offline_online_meeting as (
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
    counting_by_activity_block as (
        select blocks.block_id, blocks.block_name, blockwise_offline_online_meeting.activity
        from blocks
        left join blockwise_offline_online_meeting using (block_id)
        where activity is not null
    ),
    calculating_activity_engagement_block as (
        select block_id, block_name,count(activity) as no_of_blms
        from counting_by_activity_block
        where activity = 'meeting'
        group by 1, 2
    ),
    joining_the_two as (
        select
            blocks.block_id,
            blocks.block_name,
            (if(calculating_activity_engagement_block.no_of_blms is null,0,calculating_activity_engagement_block.no_of_blms) + (if(calculating_activity_engagement.no_of_blms is null,0,calculating_activity_engagement.no_of_blms))) as no_of_blms
        from blocks
        left join calculating_activity_engagement_block using (block_id)
        left join calculating_activity_engagement using (block_id)
    ),
    calculate_centre_visits_sector as (
        select
            sectors.block_id,
            block_engagement.sector_id,
            (if(sum(cast(centre_onboarding as int)) is null,0,sum(cast(centre_onboarding as int))) + (if(sum(cast(home_onboarding as int)) is null,0,sum(cast(home_onboarding as int))))) as reported_registrations,
            count(block_engagement.sector_id) as no_of_centre_visits
        from block_engagement
        left join sectors using (sector_id)
        where activity_type = 'centre_visit'
        group by 1,2
    ),
    joining_calculate_centre_visits_sector as (
        select
            block_id,
            reported_registrations,
            no_of_centre_visits
        from blocks
        left join calculate_centre_visits_sector using (block_id)
    ),
    calculate_centre_visits_block as (
        select
            block_id,
            (if(sum(cast(centre_onboarding as int)) is null,0,sum(cast(centre_onboarding as int))) + (if(sum(cast(home_onboarding as int)) is null,0,sum(cast(home_onboarding as int))))) as reported_registrations,
            count(block_id) as no_of_centre_visits
        from block_engagement
        where activity_type = 'centre_visit'
        group by 1
    ),
    joining_centre_visits as(
        select
            blocks.block_id,
            (if(joining_calculate_centre_visits_sector.reported_registrations is null,0,joining_calculate_centre_visits_sector.reported_registrations) + (if(calculate_centre_visits_block.reported_registrations is null,0,calculate_centre_visits_block.reported_registrations))) as reported_registrations,
            (if(joining_calculate_centre_visits_sector.no_of_centre_visits is null,0,joining_calculate_centre_visits_sector.no_of_centre_visits) + (if(calculate_centre_visits_block.no_of_centre_visits is null,0,calculate_centre_visits_block.no_of_centre_visits))) as centre_visits
        from blocks
        left join joining_calculate_centre_visits_sector using (block_id)
        left join calculate_centre_visits_block using (block_id)
    )

select
    blocks.block_id,
    blocks.block_name,
    joining_the_two.no_of_blms,
    joining_centre_visits.centre_visits,
    joining_centre_visits.reported_registrations
from blocks
left join joining_the_two using (block_id)
left join joining_centre_visits using (block_id)
order by no_of_blms desc
-- select
--     *
-- from joining_centre_visits
-- order by no_of_slms desc