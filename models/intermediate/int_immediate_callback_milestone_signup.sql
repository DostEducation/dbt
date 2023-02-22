with identify_latest_flow as (
    select
        *,
        row_number() over (
            partition by user_id order by engagement_start_time asc
        ) as row_number
    from {{ ref("stg_engagement_campaign_mapping")}}
    where user_id is not null
      and engagement_start_time is not null
),

filter_out_row_number_1 as(    
  select 
    * 
  from identify_latest_flow 
  where row_number = 1
),

time_difference_inbound_outbound as (
  select
    *,
    (date_diff(attempted_timestamp, engagement_start_time, second)) / 60 as delay_in_minutes,
  from filter_out_row_number_1
),
binning_into_groups as (
  select
    *,
    case when delay_in_minutes <= 1 then "1. Immediate ( < 1 mins)"
         when delay_in_minutes between 1 and 31 then "2. Delay (between 1 to 31 mins)"
         when delay_in_minutes >= 31 then "3. Huge delay ( >=31 mins)"
         when delay_in_minutes is null then "4. Call never attempted"
    else null
    end as call_back_delay_bucket
  from time_difference_inbound_outbound
)

select
  *
from binning_into_groups
