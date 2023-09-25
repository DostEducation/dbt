with
    centres as (select * from {{ ref("int_geographies") }}),
    activities as (select * from {{ ref("fct_activities") }}),

    aggregate_total_beneficiaries as (
        select
            district_name,
            block_name,
            sector_id,
            sector_name,
            sum(total_beneficiaries) as total_beneficiaries,
        from centres
        group by 1, 2, 3, 4
    ),

    aggregate_reported_registrations as (
        select
            sector_id,
            sector_name,
            sum(
                coalesce(centre_onboarding, 0 ) +
                coalesce(home_onboarding, 0 ) +
                coalesce(community_engagement_onboarded, 0 )
            ) as reported_registrations
        from activities
        group by 1, 2
    ),

    join_metrics as (
        select aggregate_total_beneficiaries.*, reported_registrations
        from aggregate_total_beneficiaries
        left join aggregate_reported_registrations using (sector_id)
    )

select *
from join_metrics
