with
    language_stg as (
        select
            uuid,
            id as language_id,
            name as language_name,
            data_source,
            migrated_on,
            created_on,
        from {{ source("unified_data_source", "language") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from language_stg