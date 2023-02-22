with ivr_prompt_response as (select * from {{ ref("stg_ivr_prompt_response")}}),
     unified_call_records as (select * from {{ ref("int_unified_call_records") }}),
     content_version as (select * from {{ ref("stg_content_version") }}),
     language_program as (select * from {{ ref("stg_language")}}),
     program_sequence as (select * from {{ ref("stg_program_sequence") }}),
     module_program as (select * from {{ ref("stg_module")}}),
     program as (select * from {{ ref("stg_program") }}),
     
     ivr_prompt_resp_notresp as(
         select 
            ivr_prompt_response.*,
            program.program_name,
            module_program.module_id,
            language_program.language_name

         from ivr_prompt_response
         left join unified_call_records using (unified_call_id, data_source)
         left join program_sequence using (program_sequence_id, data_source)
         left join program using (program_id, data_source)
         left join content_version using (content_version_id, data_source)
         left join language_program using (language_id, data_source)
         left join module_program using (module_id, data_source)
            
     ) 

select 
    data_source,
    count(data_source)
from ivr_prompt_resp_notresp
where module_id <= 19
    and cast(created_on as DATETIME) >= CURRENT_DATE() - INTERVAL 7 DAY
group by 1