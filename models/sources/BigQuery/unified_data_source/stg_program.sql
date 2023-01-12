with
    programs as (
        select
            id as program_id,
            data_source,
            name as program_name,
            status as program_status,
            type as program_type
        from {{ source("unified_data_source", "program") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from programs
