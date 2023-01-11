with
    ivr_prompt_response as (select * from {{ ref("stg_ivr_prompt_response") }}),
    all_call_records as (select * from {{ ref("stg_all_calls_records") }}),
    program_sequence as (select * from {{ ref("stg_program_sequence") }}),
    content_version as (select * from {{ ref("stg_content_version") }}),

    -- add deduplicated engagement level
    get_program_sequence_id as (
        select
            ivr_prompt_response.*,
            all_call_records.program_sequence_id,
            all_call_records.content_version_id,
            program_sequence.program_id,
            program_sequence.module_id,
            content_version.content_id
        from ivr_prompt_response
            left join all_call_records using (unified_call_id, data_source)
            left join program_sequence using (program_sequence_id, data_source)
            left join content_version using (content_version_id, data_source)
    )

select *
from get_program_sequence_id