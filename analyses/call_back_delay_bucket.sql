select
  call_back_delay_bucket,
  count(distinct user_id) as number_of_users
from {{ ref('int_immediate_callback_milestone_signup') }}
group by 1
order by call_back_delay_bucket asc
