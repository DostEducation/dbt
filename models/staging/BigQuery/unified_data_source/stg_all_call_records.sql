with
    all_call_records as (
        select
            uuid as all_call_record_uuid,
            data_source,
            unified_call_id,
            program_sequence_id,
            content_version_id,
            user_id,
            duration,
            program_id,
            content_id,
            content_name,
            created_on,
            ist_created_on,
            migrated_on
            
        from {{ source("unified_data_source", "all_call_records") }}
    -- where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE
    )

select *
from all_call_records
