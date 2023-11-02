with    dates as (select * from {{ ref("int_dates") }}),
    geographies as (select * from {{ ref("int_geographies") }}),
    registrations as (select * from {{ ref("stg_registration") }}),

    cross_join_dates as (select * from dates cross join geographies),

    calculate_sectorwise_registrations_on_database as (
        select
            sector_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01' and sector_name is not null
        group by 1, 2
    ),
    calculate_blockwise_registrations_on_database as (
        select
            block_name as block_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01' and sector_name is null
        group by 1, 2
    ),
    join_registrations_on_database_sectorwise as (
        select
            * 
        from cross_join_dates
        left join calculate_sectorwise_registrations_on_database using (date,sector_name)
        where activity_level = 'Sector'
    ),
    join_registrations_on_database_blockwise as (
        select
            * 
        from cross_join_dates
        left join calculate_blockwise_registrations_on_database using (date,block_name)
        where activity_level = 'Block'
    ),
    append_sector_block_registration as (
        select
            *
        from join_registrations_on_database_sectorwise
        union all
        select
            *
        from join_registrations_on_database_blockwise
    ),
    add_appended_data_to_cross_join_table as (
        select
            cross_join_dates.*,
            append_sector_block_registration.registrations_on_database
        from cross_join_dates
        left join append_sector_block_registration using (activity_level, activity_level_id,date)
    ),
    rollup_registrations_on_database as (
        select
            * except (registrations_on_database),
            case
                when activity_level  = 'Centre' then null
                when activity_level = 'Sector' then registrations_on_database
                when activity_level = 'Block' then sum(registrations_on_database) over (partition by state_id, district_id, block_id, date)
                when activity_level = 'District' then sum(registrations_on_database) over (partition by state_id, district_id, date)
                when activity_level = 'State' then sum(registrations_on_database) over (partition by state_id, date)
            end as registrations_on_database
        from add_appended_data_to_cross_join_table
    )
select 
    *
from rollup_registrations_on_database