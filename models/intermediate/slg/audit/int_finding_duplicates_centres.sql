with centres as (select * from {{ ref('stg_centres') }}),
    finding_duplicates as (
        select 
            centre_id,
            centre_name,
            count(centre_id) as no_of_centres
        from centres
        group by 1, 2
    )
select 
    * 
from finding_duplicates