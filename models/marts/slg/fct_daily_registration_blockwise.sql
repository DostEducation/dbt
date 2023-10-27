with
    dates as (select * from {{ ref("int_dates") }}),
    geographies as (select * from {{ ref("int_geographies") }}),
    registrations as (select * from {{ ref("stg_registration") }}),
    activities as (select * from {{ ref("fct_activities") }}),
    dost_team as (select * from {{ ref('stg_dost_team') }}),

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
    ),

    calculate_registrations_on_app as (
        select
            activity_level,
            activity_level_id,
            cast(activities.date_of_meeting as date) as date,
            sum(
                registration_on_app
            ) as registration_on_app
        from activities
        group by 1, 2, 3
    ),
    join_registrations_on_app as(
        select
            *
        from rollup_registrations_on_database
        left join calculate_registrations_on_app using (activity_level, activity_level_id, date)
    ),
    rollup_registrations_on_app as(
        select
            * except (registration_on_app),
            case
                when activity_level = 'Centre' then sum(registration_on_app) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(registration_on_app) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(registration_on_app) over (partition by block_id, date)
                when activity_level = 'District' then sum(registration_on_app) over (partition by district_id, date)
                when activity_level = 'State' then sum(registration_on_app) over (partition by state_id, date)                
            end as registration_on_app
        from join_registrations_on_app
    ),

    calculate_centres_visited as (
        select 
            'Centre' as activity_level,
            centre_id as activity_level_id,
            cast(date_of_meeting as date) as date,
            1 as centres_visited
        from activities
        where activity_type = 'Centre Visit Onboarding' and activity_level = 'Centre'
        group by 1, 2, 3
    ),
    join_centres_visited as (
        select
            *
        from rollup_registrations_on_app
        left join calculate_centres_visited using (activity_level, activity_level_id, date)
    ),
    roll_up_centres_visited as (
        select
            * except (centres_visited),
            case
                when activity_level = 'Centre' then sum(centres_visited) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(centres_visited) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(centres_visited) over (partition by block_id, date)
                when activity_level = 'District' then sum(centres_visited) over (partition by district_id, date)
                when activity_level = 'State' then sum(centres_visited) over (partition by state_id, date)                
            end as centres_visited
        from join_centres_visited
    ),
    calculate_registrations_over_reported as (
        select
            *,
            coalesce(registration_on_app, 0) - coalesce(registrations_on_database, 0) as registrations_over_reported
        from roll_up_centres_visited
    ),

    calculate_centers_available as (
        select
            *,
            case
                when activity_level = 'Centre' then 1 
                when activity_level = 'Sector' then count(distinct centre_id) over (partition by sector_id, date)
                when activity_level = 'Block' then count(distinct centre_id) over (partition by block_id, date)
                when activity_level = 'District' then count(distinct centre_id) over (partition by district_id, date)
                when activity_level = 'State' then count(distinct centre_id) over (partition by state_id, date)                
            end as centers_available
        from calculate_registrations_over_reported
    ),
    get_sector_assigned_to_info as (
        select
            calculate_centers_available.*,
            dost_member_name as sector_assigned_to_name,
            role as sector_assigned_to_role,
        from calculate_centers_available
        left join dost_team on sector_assigned_to_id = dost_team_id
    )

select *
from get_sector_assigned_to_info




