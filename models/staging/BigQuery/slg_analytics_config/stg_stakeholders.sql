with
    source as (select * from {{ source("slg_analytics_config", "src_stakeholders") }}),

    setup_columns as (
        select
            id as stakeholder_id,
            dost_team_id,
            name as stakeholder_name,
            designation,
            state_id,
            district_id,
            block_id,
            sector_id,
            status,
            relationship_level,
            notes,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on,
            updated_by
        from source
    )

select *
from setup_columns
