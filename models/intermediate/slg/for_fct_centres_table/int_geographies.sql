with
    states as (select * from {{ ref('stg_states') }}),
    districts as (select * from {{ ref('stg_districts') }}),
    blocks as (select * from {{ ref('stg_blocks') }}),
    sectors as (select * from {{ ref('stg_sectors') }}),
    centres as (select * from {{ ref('stg_centres') }}),

    state_level as (
        select
            state_id, state_name,
            cast(null as string) as district_id, cast(null as string) as district_name,
            cast(null as string) as block_id, cast(null as string) as block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as centre_id, cast(null as string) as centre_name,
            'State' as activity_level
        from states
    ),

    district_level as (
        select
            state_id, state_name,
            districts.district_id, districts.district_name,
            cast(null as string) as block_id, cast(null as string) as block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as centre_id, cast(null as string) as centre_name,
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
            cast(null as string) as centre_id, cast(null as string) as centre_name,
            'Block' as activity_level
        from district_level
        inner join blocks using (district_id)
    ),
    sector_level as (
        select
            state_id, state_name,
            district_id, district_name,
            block_id, block_name,
            sectors.sector_id, sectors.sector_name,
            cast(null as string) as centre_id, cast(null as string) as centre_name,
            'Sector' as activity_level
        from block_level
        inner join sectors using (block_id)
    ),
    centre_level as (
        select
            state_id, state_name,
            district_id, district_name,
            block_id, block_name,
            sector_id, sector_name,
            centres.centre_id, centres.centre_name,
            'Centre' as activity_level
        from sector_level
        inner join centres using (sector_id)
    ),

    union_all_levels as (
        select * from state_level
        union all
        select * from district_level
        union all
        select * from block_level
        union all
        select * from sector_level
        union all
        select * from centre_level
    ),

    add_center_beneficiaries as (
        select
            union_all_levels.*,
            total_beneficiaries,
            is_balvatika
        from union_all_levels
        left join centres using (centre_id)
    ),
    add_target_beneficiaries as (
        select
            add_center_beneficiaries.*,
            target_beneficiaries_slg
        from add_center_beneficiaries
        left join blocks using (block_id)
    )
select * 
from add_target_beneficiaries