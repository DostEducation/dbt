with sector_geographies as (select * from {{ ref('int_sector_geographies') }}),
    centre as (select * from {{ ref('stg_centres') }}),
    centre_geographies as (
        select 
            centre.* except (created_on, updated_on, sector_id),
            sector_geographies.sector_id,
            sector_geographies.* except(sector_id)
        from sector_geographies
        left join centre using (sector_id)
    )
select
    *
from centre_geographies
-- where sector_id is not null