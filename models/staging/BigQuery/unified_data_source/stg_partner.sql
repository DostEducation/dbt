with
    partner as (
        select
            id as partner_id,
            name as partner_name,
            channel_name,
            data_source
        from {{ source("unified_data_source", "partner") }}
    )

select *
from partner