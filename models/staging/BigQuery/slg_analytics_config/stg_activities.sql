with activities_table as (select * from {{ source('slg_analytics_config', 'src_activities') }}),
    structuring_activities as (
        select 
            id as activities_id,
            dost_team_id,
            cast(date_of_meeting as DATE) as date_of_meeting,
            activity_level,
            state_id,
            district_id,
            block_id,
            sector_id,
            centre_id,
            activity_type,
            engagement_type,
            other_engagement_type,
            follow_up_done_by,
            no_of_sdp_attended,
            no_of_cdpo_attended,	
            no_of_dpo_attended,
            no_of_beo_attended,
            no_of_ceo_attended,
            no_of_teachers_attended,
            no_of_crc_atteneded,
            no_of_brc_attended,	
            no_of_ssa_district_cord_atteneded,
            number_of_aws_attendees,
            number_of_aww_attendees,
            no_of_balvatika_workers,	
            home_onboarding,
            centre_onboarding,
            community_engagement_onboarded,
            no_of_centres_participated,
            stakeholder_type,
            notes,		
            cast(created_on as DATETIME) as created_on,	
            cast(updated_on as DATETIME) as updated_on,	
            updated_by
        from activities_table
    )
select
    *
from structuring_activities