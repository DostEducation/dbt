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
            attended_by_sdp,
            attended_by_cdpo,	
            attended_by_dpo,	
            attended_by_Chief_Education_Officer as attended_by_ceo,
            attended_by_aws,	
            attended_by_aww,	
            stakeholder,	
            onboarding_done,	
            number_of_aws_attendees,
            number_of_aww_attendees,	
            tracker_distribution_attendees,	
            tracker_filling_attendees,	
            home_onboarding,
            centre_onboarding,	
            time_taken,	
            approval_done_by,	
            approval_status,	
            objective_achieved,
            cast(created_on as DATETIME) as created_on,	
            cast(updated_on as DATETIME) as updated_on,	
            updated_by
        from activities_table
    )
select
    *
from structuring_activities