with dost_team as (select * from {{ ref('stg_dost_team') }}),
    finding_duplicates as (
        select 
            name,
            count(name) as no_of_ids
        from dost_team
        group by 1
    )
select
    *
from finding_duplicates