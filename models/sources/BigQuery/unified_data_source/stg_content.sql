with
    content as (
        select
            id as content_id,
            data_source,
            topic_id,
            name as content_name
        from {{ source("unified_data_source", "content") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from content
