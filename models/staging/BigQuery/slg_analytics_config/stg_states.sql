with
    source as (select * from {{ source("slg_analytics_config", "src_states") }}),

    setup_columns as (
        select
            id as state_id,
            state_name,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on
        from source
    )

select *
from setup_columns
