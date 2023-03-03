with identify_latest_flow as (
    select
        *,
        row_number() over (
            partition by user_id order by dial_time asc
        ) as row_number
    from {{ ref("stg_unified_enrolled_user")}}
    where user_id is not null
      and dial_time is not null
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
    (date_diff(dial_time, scheduled_time, second))  / 60 as delay_in_minutes,
  from filter_out_row_number_1
)

-- binning_into_group_on_time as (
--     select
--         *,
--         case
--             when delay_in_minutes = 0 then "1. Call received on scheduled time"
--             when delay_in_minutes != 0 then "2. Did not receive call on scheduled time"
--             when scheduled_time is null then "3. Call is not scheduled"
--             else null
--             end as call_received_on_time_or_not
--     from time_difference_inbound_outbound
-- )
select
    delay_in_minutes
from time_difference_inbound_outbound
where scheduled_time is not null
-- group by 1