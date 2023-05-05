with centre_engagement as (select * from {{ ref('stg_activities') }}),
    centre_info as (select * from {{ ref('stg_centres') }}),
    calculating_centre_home_onboarding as (

    select
        FORMAT_DATE('%y%m', date_of_meeting) as year_and_month,
        centre_id,
        (cast(centre_onboarding as int) + cast(home_onboarding as int)) as total_onbaording
    from centre_engagement
    where activity_level = 'Centre'
    ),
    joining_centre_home as (
        select
            centre_info.centre_id,
            centre_info.centre_name,
            calculating_centre_home_onboarding.year_and_month,
            cast(centre_info.total_beneficiaries as int) as total_beneficiaries,
            sum(calculating_centre_home_onboarding.total_onbaording) as total_onbaording
        from centre_info
        left join calculating_centre_home_onboarding using (centre_id)
        group by 1,2,3,4
    ),
    calculating_percent_onboarding as(
        select
            * except(total_beneficiaries, total_onbaording),
            (total_onbaording / nullif(total_beneficiaries, 0)) * 100 as percent_onboarded
        from joining_centre_home
    )
select
    * except(percent_onboarded),
    case when percent_onboarded >= 70 then 'high'
        when percent_onboarded >= 40 then 'medium'
        when percent_onboarded >= 0 then 'low'
    else null
    end as onboarding_level
from calculating_percent_onboarding