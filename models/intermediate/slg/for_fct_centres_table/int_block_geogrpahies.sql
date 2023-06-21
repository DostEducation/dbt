with district_geographies as (select * from {{ ref('int_district_geographies') }}),
    block as (select * from {{ ref('stg_blocks') }}),
    block_geographies as (
        select 
            block.* except (created_on, updated_on),
            district_name,
            state_id,
            state_name
        from district_geographies
        left join block using (district_id)
    )
select 
    * 
from block_geographies