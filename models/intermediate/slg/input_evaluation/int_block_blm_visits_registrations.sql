with block as (select * from {{ ref('stg_blocks') }}),
    activity as (select * from {{ ref('stg_activities') }}),
    sector as (select * from {{ ref('stg_sectors') }}),
    centre as (select * from {{ ref('stg_centres') }}),
    calculating_blms as (
        select
            block_id,
            block_name,
            case
                when
                    activity.activity_type = 'offline_meeting'
                    or activity.activity_type = 'online_meeting'
                then 'meeting'
                else activity.activity_type
            end as activity
        from block
        left join activity using (block_id)

    ),
    counting_by_activity as (
        select block.block_id, block.block_name, calculating_blms.activity
        from block
        left join calculating_blms using (block_id)
        where activity is not null
    ),
    calculating_activity_engagement as (
        select block_id, block_name,count(activity) as no_of_blms
        from counting_by_activity
        where activity = 'meeting'
        group by 1, 2
    ),
    block_blm_level as (
        select
            block.block_id,
            block.block_name,
            calculating_activity_engagement.no_of_blms
        from block
        left join calculating_activity_engagement using (block_id)
    ),
    centre_activity_sector_block as (
        select 
            block.block_id,
            block.block_name,
            count(activity.activity_type) as no_of_centre_visits,
            (sum(if(cast(activity.centre_onboarding as int) is null,0,cast(activity.centre_onboarding as int))) + sum(if(cast(activity.home_onboarding as int) is null,0,cast(activity.home_onboarding as int)))+ sum(if(cast(activity.community_engagement_onboarded as int) is null,0,cast(activity.community_engagement_onboarded as int)))) as reported_registrations
        from centre
        left join activity using (centre_id)
        right join sector on centre.sector_id = sector.sector_id
        right join block on sector.block_id = block.block_id
        where activity_type = 'Centre Visit Onboarding'
        group by 1,2
    ),
    joining_centre_activity_sector_block_with_block as (
        select 
            block.block_id,
            block.block_name,
            centre_activity_sector_block.no_of_centre_visits,
            centre_activity_sector_block.reported_registrations
        from block
        left join centre_activity_sector_block using (block_id)
    )
select
    block_blm_level.*,
    joining_centre_activity_sector_block_with_block.no_of_centre_visits,
    joining_centre_activity_sector_block_with_block.reported_registrations
from block_blm_level
left join joining_centre_activity_sector_block_with_block using (block_id)