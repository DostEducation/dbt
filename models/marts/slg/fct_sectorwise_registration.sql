with centres as (select * from {{ ref('int_centre_geographies') }}),
    activities as (select * from {{ ref('fct_activities') }}),
    calculating_beneficiaries_centres as (
        select 
            district_name,
            block_name,
            sector_id,
            sector_name,
            sum(coalesce(cast(centres.total_beneficiaries as int),0)) as total_beneficiaries,
        from centres
        group by 1,2,3,4
        order by sum(coalesce(cast(centres.total_beneficiaries as int),0)) desc
    ),
    calculating_total_onboarded as (
        select
            sector_id,
            sector_name,
            sum(coalesce(cast(centre_onboarding as int),0) + coalesce(cast(home_onboarding as int),0) + coalesce(cast(community_engagement_onboarded as int),0)) as reported_registrations
        from activities
        group by 1,2
        order by  sum(coalesce(cast(centre_onboarding as int),0) + coalesce(cast(home_onboarding as int),0) + coalesce(cast(community_engagement_onboarded as int),0)) desc
    )
select 
    calculating_beneficiaries_centres.*,
    calculating_total_onboarded.reported_registrations
from calculating_beneficiaries_centres
left join calculating_total_onboarded using (sector_id)