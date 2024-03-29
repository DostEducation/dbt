with users as (
    select
        uuid as user_uuid,
        id as user_id,
        name as user_name,
        partner_id,
        data_source,
        migrated_on
    from {{ source ("unified_data_source","users") }}
    where migrated_on <= current_timestamp() - interval 100 minute     
)

select 
    *
from users