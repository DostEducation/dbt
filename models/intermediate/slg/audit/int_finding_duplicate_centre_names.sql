with centres as (select * from {{ ref('stg_centres') }}),
    finding_duplicates as (
        select 
            centre_name,
            count(centre_name) as no_of_centres
        from centres
        group by 1
    )
select 
    * 
from finding_duplicates