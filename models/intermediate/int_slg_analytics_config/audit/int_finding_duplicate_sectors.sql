with sectors as (select * from {{ ref('stg_sectors') }}),
    blending_sectors as (
        select 
            sector_id,
            sector_name,
            count(sector_id) as no_of_sectors
        from sectors
        group by 1, 2
    )
select  
    *
from blending_sectors
-- where no_of_sectors > 1