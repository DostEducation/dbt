with sectors as (select * from {{ ref('stg_sectors') }}),
    blending_sectors as (
        select 
            sector_name,
            count(sector_name) as no_of_sectors
        from sectors
        group by 1
    )
select 
    *
from blending_sectors