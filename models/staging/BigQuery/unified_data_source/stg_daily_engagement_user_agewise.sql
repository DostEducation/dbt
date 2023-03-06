with daily_engagement as (select * from {{ source('unified_data_source', 'daily_engagement_user_age_wise') }}),
    engagement_user_agewise as (
        select 
            *
        from daily_engagement
    )
select 
    *
from engagement_user_agewise