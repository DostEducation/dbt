with
    block_engagement as (select * from {{ ref("stg_activities") }}),
    blocks as (select * from {{ ref('stg_blocks') }}),
    sectors as (select * from {{ ref('stg_sectors') }}),
    centre as (select * from {{ ref('stg_centres') }}),
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
        select sectors.block_id,sectors.sector_id, joining_offline_online_meeting.activity
        from sectors
        left join joining_offline_online_meeting using (sector_id)
        where activity is not null
    ),
    calculating_activity_engagement as (
        select block_id, count(activity) as no_of_slms
        from counting_by_activity
        where activity = 'meeting'
        group by 1
    ),
    meetings_grouped_by_block as (
        select 
            blocks.block_id,
            blocks.block_name,
            calculating_activity_engagement.no_of_slms as no_of_meetings
        from blocks
        left join calculating_activity_engagement using (block_id)
    ),
    calculating_blms as (
        select
            block_id,
            block_name,
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
        select blocks.block_id, blocks.block_name, calculating_blms.activity
        from blocks
        left join calculating_blms using (block_id)
        where activity is not null
    ),
    calculating_activity__block_engagement as (
        select block_id, block_name,count(activity) as no_of_blms
        from counting_by_activity_block
        where activity = 'meeting'
        group by 1, 2
    ),
    block_blm_level as (
        select
            blocks.block_id,
            blocks.block_name,
            calculating_activity__block_engagement.no_of_blms as no_of_meetings
        from blocks
        left join calculating_activity__block_engagement using (block_id)
    ),
    joining_the_two_block_info as(
        select 
            blocks.block_id,
            blocks.block_name,
            (if(block_blm_level.no_of_meetings is null,0,block_blm_level.no_of_meetings) + if(meetings_grouped_by_block.no_of_meetings is null,0,meetings_grouped_by_block.no_of_meetings)) as no_of_meetings 
        from blocks        
        left join meetings_grouped_by_block on meetings_grouped_by_block.block_id = blocks.block_id
        left join block_blm_level on block_blm_level.block_id = blocks.block_id
    ),
    centre_activity_sector_block as (
        select 
            blocks.block_id,
            blocks.block_name,
            count(block_engagement.activity_type) as no_of_centre_visits,
            (sum(cast(block_engagement.centre_onboarding as int)) + sum(cast(block_engagement.home_onboarding as int))) as reported_registrations
        from centre
        left join block_engagement using (centre_id)
        right join sectors on centre.sector_id = sectors.sector_id
        right join blocks on sectors.block_id = blocks.block_id
        where activity_type = 'centre_visit'
        group by 1,2
    ),
    joining_centre_activity_sector_block_with_block as (
        select 
            blocks.block_id,
            blocks.block_name,
            centre_activity_sector_block.no_of_centre_visits,
            centre_activity_sector_block.reported_registrations
        from blocks
        left join centre_activity_sector_block using (block_id)
    )
select 
    blocks.block_id,
    blocks.block_name,
    joining_the_two_block_info.no_of_meetings,
    joining_centre_activity_sector_block_with_block.no_of_centre_visits,
    joining_centre_activity_sector_block_with_block.reported_registrations
from blocks
left join joining_the_two_block_info using (block_id)
left join joining_centre_activity_sector_block_with_block using (block_id)