with sectors as (select * from {{ ref('stg_sectors') }}),
    finding_duplicates as(
        select
            sector_code,
            count(sector_code) as no_of_sector_codes
        from sectors
        group by 1
    )
select 
    *
from finding_duplicates
where sector_code is not null
