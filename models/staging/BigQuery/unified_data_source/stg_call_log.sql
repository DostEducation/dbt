with
    call_log as (
        select
            uuid as call_log_uuid,
            data_source,
            id as call_log_id,
            program_sequence_id,
            content_version_id,
            user_id,
        from {{ source("unified_data_source", "call_log") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from call_log
