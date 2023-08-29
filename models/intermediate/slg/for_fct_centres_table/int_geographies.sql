with
    states as (select * from {{ ref('stg_states') }}),
    districts as (select * from {{ ref('stg_districts') }}),
    blocks as (select * from {{ ref('stg_blocks') }}),
    sectors as (select * from {{ ref('stg_sectors') }}),
    centers as (select * from {{ ref('stg_centres') }}),

    state_level as (
        select
            state_id, state_name,
            cast(null as string) as district_id, cast(null as string) as district_name,
            cast(null as string) as block_id, cast(null as string) as block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as center_id, cast(null as string) as center_name,
            'State' as activity_level
        from states
    ),

    district_level as (
        select
            state_id, state_name,
            districts.district_id, districts.district_name,
            cast(null as string) as block_id, cast(null as string) as block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as center_id, cast(null as string) as center_name,
            'District' as activity_level
        from state_level
        inner join districts using (state_id)
    ),

    block_level as (
        select
            state_id, state_name,
            district_id, district_name,
            blocks.block_id, blocks.block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as center_id, cast(null as string) as center_name,
            'Block' as activity_level
        from district_level
        inner join blocks using (district_id)
    ),

    union_all_levels as (
        select * from state_level
        union all
        select * from district_level
        union all
        select * from block_level
    )

select * 
from union_all_levels