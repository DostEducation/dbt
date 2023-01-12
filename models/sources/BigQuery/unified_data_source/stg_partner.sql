with
    partner as (
        select
            id as partner_id,
            name as partner_name,
            channel_name
        from {{ source("unified_data_source", "partner") }}
    )

select *
from partner