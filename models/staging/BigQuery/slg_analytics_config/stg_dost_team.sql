with team_member as (select * from {{ source('slg_analytics_config', 'src_dost_team') }}),
    structuring_team as (
        select
            id as dost_team_id,
            name as dost_member_name,
            email as email_id,
            role,
            cast(created_on as DATETIME) as created_on,
            cast(updated_on as DATETIME) as updated_on
        from team_member
    )
select  
    *
from structuring_team