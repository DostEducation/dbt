with
    dates as (select * from {{ ref("int_dates") }}),
    geographies as (select * from {{ ref("int_geographies") }}),
    registrations as (select * from {{ ref("stg_registration") }}),
    activities as (select * from {{ ref("fct_activities") }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),

    cross_join_dates as (select * from dates cross join geographies),

    calculate_registrations_on_database_sectorwise as (
        select
            sector_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01' and sector_name is not null
        group by 1, 2
    ),
    calculate_registrations_on_database as (
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
        left join calculate_registrations_on_database_sectorwise using (date,sector_name)
        where activity_level = 'Sector'
    ),
    join_registrations_on_database_blockwise as (
        select
            * 
        from cross_join_dates
        left join calculate_registrations_on_database using (date,block_name)
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
    ),

    calculate_registrations_on_app as (
        select
            activity_level,
            activity_level_id,
            cast(activities.created_on as date) as date,
            sum(
                registration_on_app
            ) as registration_on_app
        from activities
        group by 1, 2, 3
    ),

    join_registrations_on_app as(
        select
            * except (registration_on_app),
            case
                when activity_level = 'Centre' then sum(registration_on_app) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(registration_on_app) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(registration_on_app) over (partition by block_id, date)
                when activity_level = 'District' then sum(registration_on_app) over (partition by district_id, date)
                when activity_level = 'State' then sum(registration_on_app) over (partition by state_id, date)                
            end as registration_on_app
        from rollup_registrations_on_database
        left join calculate_registrations_on_app using (activity_level, activity_level_id, date)
    ),
    add_centre_visit as (
        select 
            cast(date_of_meeting as date) as date,
            centre_name,
            count(distinct centre_id) as centre_visited
        from activities
        where activity_type = 'Centre Visit Onboarding'
        group by 1,2
    ),
    join_centre_visit_with_main_table as (
        select
            *
        from cross_join_dates
        left join add_centre_visit using (date,centre_name)
    ),
    rolling_up_centre_visited as (
        select
            join_registrations_on_app.*,
            case
                when activity_level = 'Centre' then sum(centre_visited) over (partition by join_registrations_on_app.centre_id, date)
                when activity_level = 'Sector' then sum(centre_visited) over (partition by join_registrations_on_app.sector_id, date)
                when activity_level = 'Block' then sum(centre_visited) over (partition by join_registrations_on_app.block_id, date)
                when activity_level = 'District' then sum(centre_visited) over (partition by join_registrations_on_app.district_id, date)
                when activity_level = 'State' then sum(centre_visited) over (partition by join_registrations_on_app.state_id, date)                
            end as centre_visited
        from join_registrations_on_app
        left join join_centre_visit_with_main_table using (activity_level, activity_level_id, date)
    ),
    add_dost_info as (
        select
            rolling_up_centre_visited.*,
            dost_member_name
        from rolling_up_centre_visited
        left join dost_team on rolling_up_centre_visited.sectors_assigned_to_id = dost_team.dost_team_id
    )
select *, ((coalesce(registration_on_app,0) - coalesce(registrations_on_database,0)) / registrations_on_database) as percent_over_reported
from add_dost_info
where registration_on_app is not null
-- where dost_member_name is not null
-- where registrations_on_database is not null
-- where sectors_assigned_to_id is not null
-- where activity_level = 'Block'
-- where
    -- true
    -- and activity_level = 'State'
    -- and state_name = 'UK'
    -- and district_name = 'DDN'
    -- and block_name = 'Vikashnagar'
    -- and sector_name = 'Dhakrani'
    -- and sector_id = '83e98315'
    -- and registrations_on_database is not null
    -- and date >= '2023-10-01'
-- order by date



