with program_sequence as (select * from {{ ref('stg_program_sequence') }}),
    content_version as (select * from {{ ref('stg_content_version') }}),
    registration as (select * from {{ ref('stg_registration') }}),
    call_records as (select * from {{ ref('stg_all_call_records') }}),
    content as (select * from {{ ref('stg_content') }}),
    joining_content_version as (
        select 
            content.*,
            content_version.content_status
        from content
        left join content_version using (content_id)
    ),
    joining_all_tables as (
        select
            call_records.all_call_record_uuid,
            call_records.data_source,
            program_sequence.module_id,
            program_sequence.content_id,
            call_records.user_id,
            joining_content_version.content_status,
            CASE 
                WHEN call_records.data_source = 'rp_ivr' and call_records.call_type = 'missed-call'
                THEN NULL
                WHEN call_records.data_source = 'admindashboard' AND call_records.missed_call_status = 'ignored'
                THEN NULL
                WHEN call_records.data_source = 'rp_ivr' AND call_records.telco_code IN ('16', '17', '19')
                THEN 'yes'
                WHEN call_records.data_source = 'admindashboard' AND call_records.call_status IN ('completed', 'failed_noresponse')
                THEN 'yes'
            ELSE 'no'
            END AS is_call_delivered,
            CASE 
                WHEN call_records.data_source = 'rp_ivr' and call_records.call_type = 'missed-call'
                THEN NULL
                WHEN call_records.data_source = 'admindashboard' AND call_records.missed_call_status = 'ignored'
                THEN NULL
                WHEN call_records.data_source = 'rp_ivr' AND call_records.telco_code = '16'
                THEN 'yes'
                WHEN call_records.data_source = 'admindashboard' AND call_records.call_status = 'completed'
                THEN 'yes'
            ELSE 'no'
            END AS is_call_answered
        from call_records
        left join program_sequence using (program_sequence_id, data_source)
        left join joining_content_version on joining_content_version.content_id = program_sequence.content_id 
                and joining_content_version.data_source = program_sequence.data_source
        left join registration on registration.user_id = call_records.user_id 
                and registration.data_source = call_records.data_source
        WHERE
        true
        -- and registration.signup_date <= '2021-06-21'
    ),
    response_rate_by_user_and_content as (
    select
      user_id,
      data_source,
      module_id,
      content_id,
      content_status,
      count(if(is_call_delivered = 'yes', all_call_record_uuid, null)) as calls_delivered,
      count(if(is_call_answered = 'yes', all_call_record_uuid, null)) as calls_answered,
    --   if
    --     (
    --     data_source = 'rp_ivr',
    --     null,
    --     safe_divide(
    --       count(if(is_call_answered = 'yes', all_call_record_uuid, null)),
    --       count(if(is_call_delivered = 'yes', all_call_record_uuid, null))
    --     )
    --   ) as response_rate
    from
      joining_all_tables
    where
      true
    group by
      1, 2, 3, 4, 5
  )

select
    *
--   module_id,
--   -- * except (user_id, calls_received, calls_answered)
--   avg(response_rate) as average_response_rate,
--   count(distinct user_id) as unique_users_receiving_calls
from
  response_rate_by_user_and_content
-- where data_source = 'admindashboard'
-- group by 1
