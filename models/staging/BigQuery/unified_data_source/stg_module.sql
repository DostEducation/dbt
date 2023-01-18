with
    modules as (
        select
            id as module_id,
            data_source,
            name as module_name,
        from {{ source("unified_data_source", "module") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from modules
