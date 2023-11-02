with
    source as (select * from {{ source("slg_analytics_config", "src_dost_team") }}),

    setup_columns as (
        select
            id as dost_team_id,
            name as dost_member_name,
            email as email_id,
            role,
            safe_cast(created_on as datetime) as created_on,
            safe_cast(updated_on as datetime) as updated_on
        from source
    )

select *
from setup_columns
