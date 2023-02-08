with
    call_records as (select * from {{ ref("stg_all_call_records") }}),
    content_version as (select * from {{ ref("stg_content_version") }}),
    language_used as (select * from {{ ref("stg_language") }}),
    users as (select * from {{ ref("stg_users") }}),
    partner as (select * from {{ ref("stg_partner") }}),
    program as (select * from {{ ref("stg_program")}}),
    program_sequence as (select * from {{ ref("stg_program_sequence")}}),
    module_stg as (select * from {{ ref("stg_module")}}),

    joining_all_tables as (

        select
            call_records.* except(program_id, created_on),
            date_trunc(call_records.created_on, day) created_on,
            content_version.content_duration as content_version_duration,
            language_used.language_name,
            -- language_used.language_id,
            partner.partner_name,
            program.program_name,
            module_stg.module_id
        from call_records
        left join content_version using (content_version_id, data_source)
        left join language_used using (language_id, data_source)
        left join users using (user_id, data_source)
        left join partner using (partner_id, data_source)
        left join program using (program_id, data_source)
        left join program_sequence using (program_sequence_id, data_source)
        left join module_stg on program_sequence.module_id = module_stg.module_id
                                and program_sequence.data_source = module_stg.data_source
        where module_stg.module_id <= 19
            and cast(call_records.created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
    )

select *
from joining_all_tables
