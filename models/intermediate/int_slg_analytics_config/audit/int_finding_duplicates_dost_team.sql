with dost_team as (select * from {{ ref('stg_dost_team') }}),
    finding_duplicates as (
        select 
            dost_team_id,
            name,
            count(dost_team_id) as no_of_ids
        from dost_team
        group by 1, 2
    )
select
    *
from finding_duplicates