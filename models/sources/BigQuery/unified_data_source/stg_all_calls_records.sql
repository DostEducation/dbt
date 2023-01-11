with
    all_call_records as (
        select
            uuid as all_call_record_uuid,
            data_source,
            unified_call_id,
            program_sequence_id,
            content_version_id
        from {{ source("unified_data_source", "all_call_records") }}
    -- where migrated_on <= CURRENT_TIMESTAMP() - INTERVAL 100 MINUTE
    )

select *
from all_call_records
