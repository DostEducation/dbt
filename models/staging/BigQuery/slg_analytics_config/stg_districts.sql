with
    source as (select * from {{ source("slg_analytics_config", "src_districts") }}),

    setup_columns as (
        select
            id as district_id,
            district_name,
            state_id,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on
        from source
    )

select *
from setup_columns
