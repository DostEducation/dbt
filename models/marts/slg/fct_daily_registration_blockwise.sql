with 
    blocks as (select * from {{ ref('stg_blocks') }}),
    registration as (select * from {{ ref('stg_registration') }}),
    activities as (select * from {{ ref('fct_activities') }}),
    date_spine as ({{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="current_date()"
    )
        }}
    ),
    

    join_block_date as (
        select
            blocks.block_name,
            cast(date_spine.date_day as date) as date_day
        from blocks
        cross join date_spine
    ),
    registrations_on_database as (
        select
        state_name as state_name_database,
        district_name as district_name_database,
        block_name as block_name,
        cast(user_created_on as date) as user_created_on_database,
        count(distinct user_phone) as registration_on_database
        from registration
        where
        true 
        and user_created_on >= '2022-12-31'
        group by
        1,2,3,4
    ),
    registrations_on_app as (
        SELECT
        state_name,
        district_name,
        blocks.block_name,
        cast(activities.created_on as date) as created_on,
        sum(coalesce(cast(centre_onboarding as int), 0) + coalesce(cast(home_onboarding as int), 0) + coalesce(cast(community_engagement_onboarded as int),0)) as registration_on_app
        FROM
        blocks
        left join activities using (block_name)  
        GROUP BY
        1,2,3,4
    )
    select
        join_block_date.*,
        registrations_on_database.* except(block_name),
        registrations_on_app.* except(block_name)
    from join_block_date
    left join registrations_on_database on registrations_on_database.block_name = join_block_date.block_name and join_block_date.date_day = registrations_on_database.user_created_on_database
    left join registrations_on_app on registrations_on_app.block_name = join_block_date.block_name and join_block_date.date_day = registrations_on_app.created_on