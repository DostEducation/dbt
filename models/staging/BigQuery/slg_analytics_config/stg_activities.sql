with
    source as (select * from {{ source("slg_analytics_config", "src_activities") }}),

    setup_columns as (
        select
            id as activities_id,
            dost_team_id,
            cast(date_of_meeting as date) as date_of_meeting,
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
            safe_cast(no_of_sdp_attended as int) no_of_sdp_attended,
            safe_cast(no_of_cdpo_attended as int) no_of_cdpo_attended,
            safe_cast(no_of_dpo_attended as int) no_of_dpo_attended,
            safe_cast(no_of_beo_attended as int) no_of_beo_attended,
            safe_cast(no_of_ceo_attended as int) no_of_ceo_attended,
            safe_cast(no_of_teachers_attended as int) no_of_teachers_attended,
            safe_cast(no_of_crc_atteneded as int) no_of_crc_atteneded,
            safe_cast(no_of_brc_attended as int) no_of_brc_attended,
            safe_cast(no_of_ssa_district_cord_atteneded as int) no_of_ssa_district_cord_atteneded,
            safe_cast(number_of_aws_attendees as int) number_of_aws_attendees,
            safe_cast(number_of_aww_attendees as int) number_of_aww_attendees,
            safe_cast(no_of_balvatika_workers as int) no_of_balvatika_workers,
            safe_cast(home_onboarding as int) home_onboarding,
            safe_cast(centre_onboarding as int) centre_onboarding,
            safe_cast(community_engagement_onboarded as int) community_engagement_onboarded,
            safe_cast(no_of_centres_participated as int) no_of_centres_participated,
            stakeholder_type,
            notes,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on,
            updated_by
        from source
    ),

    get_reported_registrations as (   
        select
            *,
            coalesce(centre_onboarding , 0)
            + coalesce(home_onboarding, 0)
            + coalesce(community_engagement_onboarded, 0)
            as registration_on_app
        from setup_columns
    ),

    add_activity_level_details as (
        select
            *,
            case
                when activity_level = 'Centre' then centre_id
                when activity_level = 'Sector' then sector_id
                when activity_level = 'Block' then block_id
                when activity_level = 'District' then district_id
                when activity_level = 'State' then state_id
            end as activity_level_id
        from
            get_reported_registrations
    )

select *
from add_activity_level_details
