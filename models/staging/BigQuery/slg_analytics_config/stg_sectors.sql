with
    source as (select * from {{ source("slg_analytics_config", "src_sectors") }}),

    setup_columns as (
        select
            id as sector_id,
            sector_name,
            block_id,
            sector_code,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on
        from source
    )

select *
from setup_columns
