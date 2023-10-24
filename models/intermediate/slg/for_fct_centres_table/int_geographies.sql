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
            'State' as activity_level,
            state_name as activity_level_label,
            state_id as activity_level_id,
            null as total_beneficiaries,
            cast(null as string) as is_balvatika
        from states
    ),

    district_level as (
        select
            state_id, state_name,
            districts.district_id, districts.district_name,
            cast(null as string) as block_id, cast(null as string) as block_name,
            cast(null as string) as sector_id, cast(null as string) as sector_name,
            cast(null as string) as centre_id, cast(null as string) as centre_name,
            'District' as activity_level,
            districts.district_name as activity_level_label,
            districts.district_id as activity_level_id,
            null as total_beneficiaries,
            cast(null as string) as is_balvatika
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
            'Block' as activity_level,
            blocks.block_name as activity_level_label,
            blocks.block_id as activity_level_id,
            null as total_beneficiaries,
            cast(null as string) as is_balvatika
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
            'Sector' as activity_level,
            sectors.sector_name as activity_level_label,
            sectors.sector_id as activity_level_id,
            null as total_beneficiaries,
            cast(null as string) as is_balvatika
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
            'Centre' as activity_level,
            centres.centre_name as activity_level_label,
            centres.centre_id as activity_level_id,
            centres.total_beneficiaries as total_beneficiaries,
            centres.is_balvatika as is_balvatika
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

    rollup_total_beneficiaries as (
        select
            * except (total_beneficiaries),
            case
                when activity_level = 'Centre' then total_beneficiaries
                when activity_level = 'Sector' then sum(total_beneficiaries) over (partition by sector_id)
                when activity_level = 'Block' then sum(total_beneficiaries) over (partition by block_id)
                when activity_level = 'District' then sum(total_beneficiaries) over (partition by district_id)
                when activity_level = 'State' then sum(total_beneficiaries) over (partition by state_id)
            end as total_beneficiaries
        from union_all_levels
    )

select
    *,
    -- sum(total_beneficiaries)
from rollup_total_beneficiaries
where
    true
    -- and activity_level = 'Sector'
    -- and state_name = 'UK'
    -- and district_name = 'DDN'
    -- and block_name = 'Vikashnagar'
    -- and sector_name = 'Dhakrani'
    -- and sector_id = '83e98315'
    -- and total_beneficiaries is not null