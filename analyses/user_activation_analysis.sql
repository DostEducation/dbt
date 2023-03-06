select
    count(distinct user_id)
from {{ ref('stg_daily_engagement_user_agewise') }}
where true
    and user_signup_status = "complete"
    and data_source = "admindashboard"
    and user_created_on >= "2022-03-01"
    and user_created_on <= "2022-03-31"
    and user_age_at_time_of_call in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)