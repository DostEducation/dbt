with
    ivr_prompt_response as (select * from {{ ref("stg_ivr_prompt_response") }}),
    all_call_records as (select * from {{ ref("stg_all_calls_records") }}),
    program_sequence as (select * from {{ ref("stg_program_sequence") }}),
    content_version as (select * from {{ ref("stg_content_version") }}),
    q_r_p_map as (select * from {{ ref("int_question_response_placement_map") }}),

    -- add deduplicated engagement level
    get_program_sequence_id as (
        select
            ivr_prompt_response.*,
            all_call_records.program_sequence_id,
            all_call_records.content_version_id,
            program_sequence.program_id,
            program_sequence.module_id,
            content_version.content_id,
            q_r_p_map.* except (keypress, data_source, program_sequence_id, content_id, webhook_response_value)
        from ivr_prompt_response
        left join all_call_records using (unified_call_id, data_source)
        left join program_sequence using (program_sequence_id, data_source)
        left join content_version using (content_version_id, data_source)
        left join
            q_r_p_map
            on
            q_r_p_map.keypress = ivr_prompt_response.keypress
            and question_placement_status <> 'NotDeployed' -- to avoid duplicate records
            and ivr_prompt_response_created_on >= question_placement_activation_date
            and (
                ivr_prompt_response_created_on < question_placement_deactivation_date
                or question_placement_deactivation_date is null
            )
            and (
                (
                    q_r_p_map.program_sequence_id = program_sequence.program_sequence_id
                    and q_r_p_map.data_source = 'admindashboard'
                )
                or (
                    q_r_p_map.content_id = program_sequence.content_id
                    and q_r_p_map.data_source = 'rp_ivr'
                )
            )
    )

select *
from get_program_sequence_id
