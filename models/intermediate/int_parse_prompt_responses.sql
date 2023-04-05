with
    ivr_prompt_response as (select * from {{ ref("stg_ivr_prompt_response") }}),
    unified_call_records as (select * from {{ ref("int_unified_call_records") }}),
    program_sequence as (select * from {{ ref("stg_program_sequence") }}),
    content_version as (select * from {{ ref("stg_content_version") }}),
    content as (select * from {{ ref("stg_content") }}),
    program as (select * from {{ ref("stg_program") }}),
    q_r_p_map as (select * from {{ ref("int_question_response_placement_map") }}),

    -- add deduplicated engagement level
    parse_prompt_responses as (
        select
            ivr_prompt_response.*,
            unified_call_records.program_sequence_id,
            unified_call_records.content_version_id,
            unified_call_records.user_id,
            program_sequence.program_id,
            program_sequence.module_id,
            program.* except (program_id, data_source),
            content_version.content_id,
            content.* except (content_id, data_source, created_on),
            q_r_p_map.* except (keypress, data_source, program_sequence_id, content_id, webhook_response_value, unified_content_map_id)
        from ivr_prompt_response
        left join unified_call_records using (unified_call_id, data_source)
        left join program_sequence using (program_sequence_id, data_source)
        left join program using (program_id, data_source)
        left join content_version using (content_version_id, data_source)
        left join content on content.content_id = content_version.content_id and content.data_source = content_version.data_source
        left join
            q_r_p_map
                on
                true
                and q_r_p_map.unified_content_map_id = program_sequence.unified_content_map_id
                and q_r_p_map.data_source = program_sequence.data_source
                and q_r_p_map.keypress = ivr_prompt_response.keypress
                and ivr_prompt_response_created_on >= question_placement_activation_date
                and (
                    ivr_prompt_response_created_on < question_placement_deactivation_date
                    or question_placement_deactivation_date is null
                )
    )

select *
from parse_prompt_responses
