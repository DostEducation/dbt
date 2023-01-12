with
    content_version as (
        select
            id as content_version_id,
            data_source,
            content_id,
            language_id,
            duration as content_duration,
            status as content_status,
            version as content_version,
        from {{ source("unified_data_source", "content_version") }}
        where migrated_on <= current_timestamp() - interval 100 minute
    )

select *
from content_version
