with program_sequence as (select * from {{ ref('stg_program_sequence') }}),
    content_version as (select * from {{ ref('stg_content_version') }}),
    registration as (select * from {{ ref('stg_registration') }}),
    call_records as (select * from {{ ref('stg_all_call_records') }}),
    content as (select * from {{ ref('stg_content') }}),
    by_module as (select * from {{ ref('stg_module') }}),
    topic as (select * from {{ ref('stg_module_topic') }}),
    joining_content_version as (
        select 
            content.*,
            content_version.content_status,
            content_version.content_duration
        from content
        left join content_version using (content_id)
    ),
    joining_all_tables as (
        SELECT 
            topic.topic_number as module_id,
            program_sequence.content_id,
            call_records.user_id,
            call_records.data_source,
            CAST(call_records.duration AS INTEGER) as call_duration,
            joining_content_version.content_duration as content_duration,
            joining_content_version.content_status,
            CASE 
                WHEN CAST(call_records.duration AS INTEGER) > joining_content_version.content_duration
                THEN joining_content_version.content_duration 
                ELSE CAST(call_records.duration AS INTEGER)
            END AS corrected_listen_seconds
        from call_records
        left join program_sequence using (program_sequence_id, data_source)
        left join joining_content_version on joining_content_version.content_id = program_sequence.content_id 
                and joining_content_version.data_source = program_sequence.data_source
        left join by_module on by_module.module_id = program_sequence.module_id
                            and by_module.data_source = program_sequence.data_source
        left join registration on registration.user_id = call_records.user_id 
                and registration.data_source = call_records.data_source
        left join topic on IF(by_module.module_name = 'INTRO_1-SIGNUP', topic.topic_number = 0,
            IF(substr(by_module.module_name, 6, 1)= '-', topic.topic_number = cast(substr(by_module.module_name, 5, 1) as int),
              topic.topic_number = cast(substr(by_module.module_name, 5, 2) as int)
            )
          )
          and by_module.data_source = 'rp_ivr'
        WHERE
            true
            and call_records.call_type = 'outbound-call'
            and call_records.call_status in ('answered', 'completed')
            and call_records.program_sequence_id is not null
            and registration.signup_date >= '2021-06-21'
    ),
    max_listen_seconds_per_user_content as (
        select
            * except (call_duration, corrected_listen_seconds),
            max(corrected_listen_seconds) as max_listen_seconds,
            count(module_id) as count_of_calls
        from joining_all_tables
        group by 1, 2, 3, 4, 5,6
  )

select 
    *
from max_listen_seconds_per_user_content
-- where data_source = 'admindashboard'
-- where module_id is not null