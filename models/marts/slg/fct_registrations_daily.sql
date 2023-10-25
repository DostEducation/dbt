with
    activities as (select * from {{ ref("fct_activities") }}),
    user_details as (
        select
            right(reg.user_phone, 10) as user_phone,
            reg.user_id,
            datetime(reg.user_created_on, 'Asia/Kolkata') as user_registered_on,
            partner.partner_name as partner_name,
            datetime(
                prompt_response.prompt_timestamp, 'Asia/Kolkata'
            ) as user_signed_up_to_uk_program_on
        from {{ ref("stg_registration") }} as reg
        left join
            {{ ref("stg_partner") }} as partner
            on reg.partner_id = partner.partner_id
            and reg.data_source = 'admindashboard'
            and reg.data_source = partner.data_source
        left join
            {{ ref("stg_ivr_prompt_response") }} as prompt_response
            on reg.data_source = 'admindashboard'
            and prompt_response.data_source = reg.data_source
            and right(prompt_response.user_phone, 10) = right(reg.user_phone, 10)
            and prompt_response.response like '%PROGRAM-OPTIN%'
            and (
                prompt_response.response like '%B-3%'
                or prompt_response.response like '%T-6%'
            )
    ),
    partitioned_users as (
        select
            *,
            row_number() over (
                partition by user_phone order by user_signed_up_to_uk_program_on
            ) as row_number
        from user_details
    ),
    counting_user_phone as (
        select
            cast(user_registered_on as date) as user_registered_on,
            count(distinct user_phone) as actual_registration
        from partitioned_users
        where row_number = 1
        group by 1
        order by user_registered_on desc
    ),
    counting_reported_registration as (
        select
            cast(created_on as date) as created_on,
            sum(
                coalesce(cast(centre_onboarding as int), 0)
                + coalesce(cast(home_onboarding as int), 0)
                + coalesce(cast(community_engagement_onboarded as int), 0)
            ) as reported_registration
        from activities
        group by 1
        order by cast(created_on as date) desc
    )
select counting_user_phone.*, counting_reported_registration.reported_registration
from counting_user_phone
left join
    counting_reported_registration
    on counting_reported_registration.created_on
    = counting_user_phone.user_registered_on
order by user_registered_on desc
