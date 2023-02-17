with source as (

    select * from {{ source('unified_data_source', 'program_wise_engagement') }}
    where
        cast(user_created_on as timestamp)
        <= timestamp_sub(current_timestamp(), interval 100 minute)
),

renamed as (

    select
        user_program_id,
        user_program_status,
        program_name,
        program_start_date,
        program_end_date,
        program_length,
        user_phone_number,
        is_content_call,
        total_records_count,
        scheduled_calls_count,
        delivered_calls_count,
        answered_calls_count,
        unique_days_call_scheduled,
        unique_days_call_delivered,
        unique_days_call_answered,
        total_unique_content_delivered,
        total_unique_content_answered,
        time_for_one_content,
        total_content_duration,
        total_listen_seconds,
        total_corrected_listen_seconds,
        average_listen_seconds,
        average_listen_rate,
        response_rate,
        is_call_delivered,
        is_call_scheduled,
        is_call_answered,
        engagement_level as overall_engagement_level,
        prompts_delivered,
        prompts_responded,
        prompt_response_rate,
        data_source,
        program_on_registration,
        user_age,
        user_id,
        user_created_on,
        user_status,
        user_signup_status,
        is_active_user,
        state,
        district,
        centre,
        block,
        sector,
        parent_type,
        has_smartphone,
        is_child_between_0_3,
        is_child_between_3_6,
        is_child_above_6,
        has_no_child,
        gender_of_child,
        occupation,
        education_level,
        number_of_eligible_kids,
        partner_name,
        channel_name,
        first_user_week_engagement_level,
        first_user_month_engagement_level

    from source

)

select * from renamed



