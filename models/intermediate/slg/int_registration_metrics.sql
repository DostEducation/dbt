with    
    registrations as (select * from {{ ref("stg_registration") }}),
    prompt_response as (select * from {{ ref('stg_ivr_prompt_response') }}),
    cross_join_dates as (select * from {{ ref('int_date_geographies') }}),

    -- Registrations
    calculate_sectorwise_registrations_on_database as (
        select
            block_name,
            sector_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01' and sector_name is not null
        group by 1, 2,3
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
    calculate_districtwise_registrations_on_database as (
        select
            district_name,
            cast(user_created_on as date) as date,
            count(distinct user_phone) as registrations_on_database
        from registrations
        where user_created_on >= '2021-06-01' and sector_name is null and block_name = 'N/A'
        group by 1, 2
    ),
    join_sectorwise_registrations_on_database as (
        select
            * 
        from cross_join_dates
        left join calculate_sectorwise_registrations_on_database using (date,sector_name, block_name)
        where activity_level = 'Sector'
    ),
    join_blockwise_registrations_on_database as (
        select
            * 
        from cross_join_dates
        left join calculate_blockwise_registrations_on_database using (date,block_name)
        where activity_level = 'Block'
    ),
    join_districtwise_registrations_on_database as (
        select
            * 
        from cross_join_dates
        left join calculate_districtwise_registrations_on_database using (date,district_name)
        where activity_level = 'District'
    ),
    append_sector_block_registration as (
        select
            *
        from join_sectorwise_registrations_on_database
        union all
        select
            *
        from join_blockwise_registrations_on_database
        union all
        select
            *
        from join_districtwise_registrations_on_database
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
                when activity_level = 'Centre' then null
                when activity_level = 'Sector' then registrations_on_database
                when activity_level = 'Block' then sum(registrations_on_database) over (partition by state_id, district_id, block_id, date)
                when activity_level = 'District' then sum(registrations_on_database) over (partition by state_id, district_id, date)
                when activity_level = 'State' then sum(registrations_on_database) over (partition by state_id, date)
            end as registrations_on_database
        from add_appended_data_to_cross_join_table
    ),

    -- Signups
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
        FROM registrations AS reg
        LEFT JOIN prompt_response
            ON reg.data_source = 'admindashboard'
            AND prompt_response.data_source= reg.data_source
            AND RIGHT(prompt_response.user_phone, 10) = RIGHT(reg.user_phone, 10)
            AND prompt_response.response LIKE '%PROGRAM-OPTIN%'
            AND (prompt_response.response LIKE '%B-3%'
            OR prompt_response.response LIKE '%T-6%')
    ),
    partitioned_users AS (
        SELECT *, ROW_NUMBER() 
        OVER(PARTITION BY user_phone order by user_signed_up_to_uk_program_on) AS row_number
        FROM user_details
    ),
    select_latest_record as(
        SELECT * FROM partitioned_users 
        WHERE row_number = 1
    ),
    calculate_signups_sectorwise as (
        select
            sector_name,
            cast(user_registered_on as date) as date,
            count(distinct user_phone) as signups
        from select_latest_record
        where user_signed_up_to_uk_program_on is not null and user_registered_on >= '2021-06-01' and sector_name is not null
        group by 1,2
    ),
    calculate_signups_blockwise as (
        select
            block_name,
            cast(user_registered_on as date) as date,
            count(distinct user_phone) as signups
        from select_latest_record
        where user_signed_up_to_uk_program_on is not null and user_registered_on >= '2021-06-01' and sector_name is null
        group by 1,2
    ),
    join_signups_sectorwise as (
        select
            * 
        from cross_join_dates
        left join calculate_signups_sectorwise using (date,sector_name)
        where activity_level = 'Sector'
    ),
    join_signups_blockwise as (
        select
            * 
        from cross_join_dates
        left join calculate_signups_blockwise using (date,block_name)
        where activity_level = 'Block'
    ),
    append_signups as (
        select
            *
        from join_signups_sectorwise
        union all
        select
            *
        from join_signups_blockwise
    ),
    add_signups_to_cross_join_table as (
        select
            cross_join_dates.*,
            signups
        from cross_join_dates
        left join append_signups using (activity_level, activity_level_id,date)
    ),
    rollup_signups as (
        select
            * except (signups),
            case
                when activity_level = 'Centre' then null
                when activity_level = 'Sector' then signups
                when activity_level = 'Block' then sum(signups) over (partition by block_id, date)
                when activity_level = 'District' then sum(signups) over (partition by state_id, district_id, date)
                when activity_level = 'State' then sum(signups) over (partition by state_id, date)
            end as signups
        from add_signups_to_cross_join_table
    ),
    join_signup_with_main_table as(
        select
            rollup_registrations_on_database.*,
            signups
        from rollup_registrations_on_database
        left join rollup_signups using (activity_level, activity_level_id, date)
    )

select *
from join_signup_with_main_table
