with
    dates as (select * from {{ ref("int_dates") }}),
    -- blocks as (select * from {{ ref("stg_blocks") }}),
    geographies as (select * from {{ ref("int_geographies") }}),
    registrations as (select * from {{ ref("stg_registration") }}),
    activities as (select * from {{ ref("fct_activities") }}),

    cross_join_dates as (select * from dates cross join geographies),

    calculate_blockwise_registrations as (
        select
            block_name as block_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01'
        group by 1, 2
    ),

    rollup_registrations_on_database as (
        select
            * except (registrations_on_database),
            case
                when activity_level in ('Center', 'Sector') then null
                when activity_level = 'Block' then registrations_on_database
                when activity_level = 'District' then sum(registrations_on_database) over (partition by district_id, date)
                when activity_level = 'State' then sum(registrations_on_database) over (partition by state_id, date)
            end as registrations_on_database
        from cross_join_dates
        left join calculate_blockwise_registrations using (date, block_name)
    ),

    calculate_daily_registrations_on_app as (
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

    join_daily_registrations_on_app as(
        select
            * except (registration_on_app),
            case
                when activity_level = 'Center' then sum(registration_on_app) over (partition by centre_id, date)
                when activity_level = 'Sector' then sum(registration_on_app) over (partition by sector_id, date)
                when activity_level = 'Block' then sum(registration_on_app) over (partition by block_id, date)
                when activity_level = 'District' then sum(registration_on_app) over (partition by district_id, date)
                when activity_level = 'State' then sum(registration_on_app) over (partition by state_id, date)                
            end as registration_on_app
        from rollup_registrations_on_database
        left join calculate_daily_registrations_on_app using (activity_level, activity_level_id, date)
    )

select *
from join_daily_registrations_on_app
where
    true
    -- and registration_on_app is not null
    -- and block_name = 'Doiwala'
    -- and district_name = 'DDN'
    -- and state_name = 'UK'
    -- and date = '2023-10-11'
    -- and activity_level in ('Block', 'District', 'State')
