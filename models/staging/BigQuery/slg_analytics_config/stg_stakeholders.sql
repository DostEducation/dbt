with stakeholder_table as (select * from {{ source('slg_analytics_config', 'src_stakeholders') }}),
    structuring_stakeholders as (
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
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on,
            updated_by
        from stakeholder_table
    )
select
    * 
from structuring_stakeholders