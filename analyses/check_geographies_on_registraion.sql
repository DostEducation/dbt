with
    registrations as (select * from {{ ref('stg_registration') }}),
    sectors as (select * from {{ ref('stg_sectors') }}),
    blocks as (select * from {{ ref('stg_blocks') }}),

    unique_blocks_on_registrations as (
        select distinct block_name as block_name_reg
        from registrations
    ),
    outer_join_blocks as (
        select
            block_name_reg,
            block_name,
            case
                when block_name_reg is null then block_name
                else block_name_reg
            end as consolidated_names
        from unique_blocks_on_registrations
        full outer join blocks on blocks.block_name = unique_blocks_on_registrations.block_name_reg
        where block_name is null or block_name_reg is null
        order by consolidated_names
    ),

    unique_sectors_on_registrations as (
        select distinct sector_name as sector_name_reg
        from registrations
    ),

    outer_join_sectors as (
        select
            sector_name_reg,
            sector_name,
            case
                when sector_name_reg is null then sector_name
                else sector_name_reg
            end as consolidated_names
        from unique_sectors_on_registrations
        full outer join sectors on trim(sectors.sector_name) = trim(unique_sectors_on_registrations.sector_name_reg)
        where sector_name is null or sector_name_reg is null
        order by consolidated_names
    )

select *
from outer_join_sectors