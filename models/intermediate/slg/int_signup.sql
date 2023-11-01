WITH  dates as (select * from {{ ref('int_dates') }}),
    geographies as (select * from {{ ref('int_geographies') }}),
    cross_join_dates as (select * from dates cross join geographies),
    user_details AS (
        SELECT 
        RIGHT(reg.user_phone, 10) AS user_phone,
        reg.user_id,
        DATETIME(reg.user_created_on, 'Asia/Kolkata') as user_registered_on,
        reg.state_name,
        reg.district_name,
        reg.block_name,
        reg.sector_name,
        DATETIME(prompt_response.prompt_timestamp, 'Asia/Kolkata') AS user_signed_up_to_uk_program_on,
        reg.data_source
        FROM {{ ref('stg_registration') }} AS reg
        LEFT JOIN {{ ref('stg_ivr_prompt_response') }} as prompt_response
            ON reg.data_source = 'admindashboard'
            AND prompt_response.data_source= reg.data_source
            AND RIGHT(prompt_response.user_phone, 10) = RIGHT(reg.user_phone, 10)
            AND prompt_response.response LIKE '%PROGRAM-OPTIN%'
            AND (prompt_response.response LIKE '%B-3%'
            OR prompt_response.response LIKE '%T-6%')
    ), partitioned_users AS (
        SELECT *, ROW_NUMBER() 
        OVER(PARTITION BY user_phone order by user_signed_up_to_uk_program_on) AS row_number
        FROM user_details
    ),
    select_latest_record as(
        SELECT * FROM partitioned_users 
        WHERE row_number = 1
    ),
    calculate_signup_sectorwise as (
        select
            sector_name,
            cast(user_registered_on as date) as date,
            count(distinct user_phone) as signup
        from select_latest_record
        where user_signed_up_to_uk_program_on is not null and user_registered_on >= '2021-06-01' and sector_name is not null
        group by 1,2
    ),
    calculate_signup_blockwise as (
        select
            block_name,
            cast(user_registered_on as date) as date,
            count(distinct user_phone) as signup
        from select_latest_record
        where user_signed_up_to_uk_program_on is not null and user_registered_on >= '2021-06-01' and sector_name is null
        group by 1,2
    ),
    join_signup_sectorwise as (
        select
            * 
        from cross_join_dates
        left join calculate_signup_sectorwise using (date,sector_name)
        where activity_level = 'Sector'
    ),
    join_signup_blockwise as (
        select
            * 
        from cross_join_dates
        left join calculate_signup_blockwise using (date,block_name)
        where activity_level = 'Block'
    ),
    append_signup as (
        select
            *
        from join_signup_sectorwise
        union all
        select
            *
        from join_signup_blockwise
    ),
    add_appended_data_to_cross_join_table as (
        select
            cross_join_dates.*,
            append_signup.signup
        from cross_join_dates
        left join append_signup using (activity_level, activity_level_id,date)
    ),
    rollup_signups as (
        select
            * except (signup),
            case
                when activity_level  = 'Centre' then null
                when activity_level = 'Sector' then signup
                when activity_level = 'Block' then sum(signup) over (partition by block_id, date)
                when activity_level = 'District' then sum(signup) over (partition by state_id, district_id, date)
                when activity_level = 'State' then sum(signup) over (partition by state_id, date)
            end as signup
        from add_appended_data_to_cross_join_table
    )
select
    *
from rollup_signups
where 
    true
    -- state_name = 'UK' 
    -- and activity_level = 'Block'
